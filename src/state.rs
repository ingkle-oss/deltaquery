use crate::compute::{create_compute_using_factory, DQCompute, DQComputeError, DQComputeSession};
use crate::configs::{DQAppConfig, DQConfig};
use crate::metastore::DQMetastore;
use crate::table::{create_table_using_factory, DQTable};
use anyhow::{anyhow, Error};
use std::collections::{HashMap, HashSet};
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

    metastore: Option<DQMetastore>,
}

impl DQState {
    pub async fn try_new(config: DQConfig) -> Result<Self, Error> {
        let metastore = match config.metastore.as_ref() {
            Some(metastore) => Some(DQMetastore::try_new(metastore).await?),
            None => None,
        };

        Ok(DQState {
            config,
            compute: None,
            tables: HashMap::new(),
            metastore,
        })
    }

    pub async fn get_compute(state: DQStateRef) -> Result<DQComputeRef, Error> {
        let mut state = state.lock().await;

        if state.compute.is_none() {
            let config = &state.config.compute;

            let compute = create_compute_using_factory(&config.r#type, Some(config)).await?;
            state.compute = Some(Arc::new(Mutex::new(compute)));
        }

        let compute = state.compute.as_ref().expect("could not get compute");
        Ok(compute.clone())
    }

    pub async fn get_compute_session(state: DQStateRef) -> Result<DQComputeSessionRef, Error> {
        let mut state = state.lock().await;

        if state.compute.is_none() {
            let config = &state.config.compute;

            let compute = create_compute_using_factory(&config.r#type, Some(config)).await?;
            state.compute = Some(Arc::new(Mutex::new(compute)));
        }

        let compute = state.compute.clone().expect("could not get compute engine");
        let compute = compute.lock().await;

        compute.prepare().await
    }

    pub async fn get_table(state: DQStateRef, target: &String) -> Result<DQTableRef, Error> {
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

                let mut table = create_table_using_factory(
                    storage_config.map_or("delta", |config| &config.r#type),
                    table_config,
                    storage_config,
                    filesystem_config,
                )
                .await?;
                if let Err(err) = table.update().await {
                    log::error!("failed to update table: {:?}", err);
                }

                state
                    .tables
                    .insert(target.clone(), Arc::new(Mutex::new(table)));
            }
        }

        match state.tables.get(target) {
            Some(table) => Ok(table.clone()),
            None => Err(anyhow!(DQComputeError::NoTable {
                message: target.clone()
            })),
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

        if let Some(metastore) = &state.metastore {
            match metastore.get_tables().await {
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

    pub async fn get_databases(state: DQStateRef) -> Result<HashSet<String>, Error> {
        let state = state.lock().await;

        let mut items = HashSet::new();

        for table in state.config.tables.iter() {
            let fields = table.name.split(".").collect::<Vec<_>>();

            if let Some(field) = fields.get(0) {
                items.insert(field.to_string());
            }
        }

        Ok(items)
    }

    pub async fn get_schemas(
        state: DQStateRef,
        scope: Option<String>,
    ) -> Result<HashSet<String>, Error> {
        let state = state.lock().await;

        let mut items = HashSet::new();

        if let Some(scope) = &scope {
            for table in state.config.tables.iter() {
                if table.name.starts_with(scope) {
                    let fields = table.name.split(".").collect::<Vec<_>>();

                    if let Some(field) = fields.get(1) {
                        items.insert(field.to_string());
                    }
                }
            }
        } else {
            for table in state.config.tables.iter() {
                let fields = table.name.split(".").collect::<Vec<_>>();

                if let Some(field) = fields.get(1) {
                    items.insert(field.to_string());
                }
            }
        }

        Ok(items)
    }

    pub async fn get_tables(
        state: DQStateRef,
        scope: Option<String>,
    ) -> Result<HashSet<String>, Error> {
        let state = state.lock().await;

        let mut items = HashSet::new();

        if let Some(scope) = &scope {
            for table in state.config.tables.iter() {
                if table.name.starts_with(scope) {
                    let fields = table.name.split(".").collect::<Vec<_>>();

                    if let Some(field) = fields.get(2) {
                        items.insert(field.to_string());
                    }
                }
            }
        } else {
            for table in state.config.tables.iter() {
                let fields = table.name.split(".").collect::<Vec<_>>();

                if let Some(field) = fields.get(2) {
                    items.insert(field.to_string());
                }
            }
        }

        Ok(items)
    }

    pub async fn get_app(state: DQStateRef, name: &str) -> Result<Option<DQAppConfig>, Error> {
        let state = state.lock().await;

        if let Some(metastore) = &state.metastore {
            let app = metastore.get_app(name).await?;

            Ok(Some(app))
        } else {
            Ok(None)
        }
    }
}
