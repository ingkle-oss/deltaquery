use crate::compute::{create_compute_using_factory, DQCompute, DQComputeSession};
use crate::configs::{DQConfig, DQTableConfig};
use crate::table::{create_table_using_factory, DQTable};
use anyhow::Error;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub type DQStateRef = Arc<Mutex<DQState>>;
pub type DQComputeRef = Arc<Mutex<Box<dyn DQCompute>>>;
pub type DQComputeSessionRef = Box<dyn DQComputeSession>;
pub type DQTableRef = Arc<Mutex<Box<dyn DQTable>>>;

pub struct DQState {
    config: DQConfig,

    compute: Option<DQComputeRef>,

    tables: HashMap<String, DQTableRef>,

    pool: Option<Pool<Postgres>>,
}

impl DQState {
    pub async fn new(config: DQConfig) -> Self {
        let pool = match config.metastore.as_ref() {
            Some(metastore) => {
                let pool = PgPoolOptions::new()
                    .max_connections(5)
                    .connect(metastore.url.as_str())
                    .await
                    .unwrap();

                Some(pool)
            }
            None => None,
        };

        DQState {
            config,
            compute: None,
            tables: HashMap::new(),
            pool,
        }
    }

    pub async fn get_compute(state: DQStateRef) -> Option<DQComputeRef> {
        let mut state = state.lock().await;

        if state.compute.is_none() {
            let config = &state.config.compute;

            if let Some(compute) = create_compute_using_factory(&config.r#type, Some(config)).await
            {
                state.compute = Some(Arc::new(Mutex::new(compute)));
            }
        }

        if let Some(compute) = state.compute.as_ref() {
            Some(compute.clone())
        } else {
            None
        }
    }

    pub async fn get_compute_session(state: DQStateRef) -> Result<DQComputeSessionRef, Error> {
        let mut state = state.lock().await;

        if state.compute.is_none() {
            let config = &state.config.compute;

            if let Some(compute) = create_compute_using_factory(&config.r#type, Some(config)).await
            {
                state.compute = Some(Arc::new(Mutex::new(compute)));
            }
        }

        let compute = state.compute.clone().expect("could not get compute engine");
        let compute = compute.lock().await;

        compute.prepare().await
    }

    pub async fn get_table(state: DQStateRef, target: &String) -> Option<DQTableRef> {
        let mut state = state.lock().await;

        if !state.tables.contains_key(target) {
            if let Some(table_config) = state.config.tables.iter().find(|c| &c.name == target) {
                let storage_config = match &table_config.storage {
                    Some(name) => state.config.storages.iter().find(|c| &c.name == name),
                    None => None,
                };
                let filesystem_config = if let Some(filesystem) = &table_config.filesystem {
                    state
                        .config
                        .filesystems
                        .iter()
                        .find(|c| &c.name == filesystem)
                } else {
                    None
                };

                if let Some(mut table) = create_table_using_factory(
                    storage_config.map_or("delta", |config| &config.r#type),
                    table_config,
                    storage_config,
                    filesystem_config,
                )
                .await
                {
                    if let Err(err) = table.update().await {
                        log::error!("failed to update table: {:?}", err);
                    }

                    state
                        .tables
                        .insert(target.clone(), Arc::new(Mutex::new(table)));
                }
            }
        }

        if let Some(table) = state.tables.get(target) {
            Some(table.clone())
        } else {
            None
        }
    }

    pub async fn update_tables(state: DQStateRef) {
        let state = state.lock().await;

        for (_, table) in state.tables.iter() {
            let mut table = table.lock().await;

            if let Err(err) = table.update().await {
                log::error!("failed to update table: {:?}", err);
            }
        }
    }

    pub async fn rebuild_tables(state: DQStateRef) {
        let mut state = state.lock().await;

        if let (Some(pool), Some(metastore)) =
            (state.pool.as_ref(), state.config.metastore.as_ref())
        {
            match sqlx::query_as::<_, DQTableConfig>(
                format!(
                    "SELECT {} FROM {}",
                    DQTableConfig::FIELD_NAMES_AS_ARRAY.join(","),
                    metastore.tables
                )
                .as_str(),
            )
            .fetch_all(pool)
            .await
            {
                Ok(tables) => {
                    let indices0: Vec<usize> = (0..state.config.tables.len()).rev().collect();
                    let mut indices1 = Vec::new();

                    for table in tables.into_iter() {
                        if let Some(index) = state
                            .config
                            .tables
                            .iter()
                            .position(|c| c.name == table.name)
                        {
                            let table0 = &state.config.tables[index];
                            if table0 != &table {
                                log::info!("update={:#?}", table);

                                state.tables.remove(&table.name);
                                state.config.tables[index] = table;
                            }

                            indices1.push(index);
                        } else {
                            log::info!("insert={:#?}", table);

                            state.config.tables.push(table);
                        }
                    }

                    for index in indices0.into_iter() {
                        if !indices1.contains(&index) {
                            let table = state.config.tables.remove(index);

                            log::info!("delete={:#?}", table);

                            state.tables.remove(&table.name);
                        }
                    }
                }
                Err(err) => panic!("could not query tables: {:?}", err),
            }
        }
    }
}
