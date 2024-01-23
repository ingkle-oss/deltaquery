use crate::compute::{create_compute_using_factory, DQCompute, DQComputeSession};
use crate::configs::{DQConfig, DQTableConfig};
use crate::table::{create_table_using_factory, DQTable};
use anyhow::Error;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use std::collections::hash_map::IterMut;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct DQState {
    config: DQConfig,

    compute: Option<Box<dyn DQCompute>>,

    tables: HashMap<String, Box<dyn DQTable>>,

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

    pub async fn get_compute(&mut self) -> Option<&mut Box<dyn DQCompute>> {
        if self.compute.is_none() {
            let config = &self.config.compute;

            self.compute = create_compute_using_factory(&config.r#type, Some(config)).await;
        }

        if let Some(compute) = self.compute.as_mut() {
            Some(compute)
        } else {
            None
        }
    }

    pub async fn get_table(&mut self, target: &String) -> Option<&mut Box<dyn DQTable>> {
        if !self.tables.contains_key(target) {
            if let Some(table_config) = self.config.tables.iter().find(|c| &c.name == target) {
                let storage_config = match &table_config.storage {
                    Some(name) => self.config.storages.iter().find(|c| &c.name == name),
                    None => None,
                };
                let filesystem_config = if let Some(filesystem) = &table_config.filesystem {
                    self.config
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
                    let _ = table.update().await;

                    self.tables.insert(target.clone(), table);
                }
            }
        }

        if let Some(table) = self.tables.get_mut(target) {
            Some(table)
        } else {
            None
        }
    }

    pub fn get_tables(&mut self) -> IterMut<'_, String, Box<dyn DQTable>> {
        self.tables.iter_mut()
    }

    pub async fn update_tables(&mut self) {
        if let (Some(pool), Some(metastore)) = (self.pool.as_ref(), self.config.metastore.as_ref())
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
                    let indices0: Vec<usize> = (0..self.config.tables.len()).rev().collect();
                    let mut indices1 = Vec::new();

                    for table in tables.into_iter() {
                        if let Some(index) =
                            self.config.tables.iter().position(|c| c.name == table.name)
                        {
                            let table0 = &self.config.tables[index];
                            if table0 != &table {
                                log::info!("update={:#?}", table);

                                self.tables.remove(&table.name);
                                self.config.tables[index] = table;
                            }

                            indices1.push(index);
                        } else {
                            log::info!("insert={:#?}", table);

                            self.config.tables.push(table);
                        }
                    }

                    for index in indices0.into_iter() {
                        if !indices1.contains(&index) {
                            let table = &self.config.tables[index];

                            log::info!("delete={:#?}", table);

                            self.tables.remove(&table.name);
                            self.config.tables.remove(index);
                        }
                    }
                }
                Err(err) => panic!("could not query tables: {:?}", err),
            }
        }
    }

    pub async fn prepare_compute_session(
        state: Arc<Mutex<DQState>>,
    ) -> Result<Box<dyn DQComputeSession>, Error> {
        let mut state = state.lock().await;

        let compute = state
            .get_compute()
            .await
            .expect("could not get compute engine");

        compute.prepare().await
    }
}
