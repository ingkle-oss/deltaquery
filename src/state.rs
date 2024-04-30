use crate::compute::{create_compute_using_factory, DQCompute, DQComputeError, DQComputeSession};
use crate::configs::{DQAppConfig, DQConfig};
use crate::identity::{create_identity_using_factory, DQIdentity};
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
pub type DQIdentityRef = Arc<Mutex<Box<dyn DQIdentity>>>;

pub struct DQState {
    config: DQConfig,

    identity: Option<DQIdentityRef>,

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
            identity: None,
            compute: None,
            tables: HashMap::new(),
            metastore,
        })
    }

    pub async fn get_identity(state: DQStateRef) -> Result<Option<DQIdentityRef>, Error> {
        let mut state = state.lock().await;

        if state.identity.is_none() {
            if let Some(config) = &state.config.identity {
                let identity =
                    create_identity_using_factory(&config.r#type, config.options.clone()).await?;
                state.identity = Some(Arc::new(Mutex::new(identity)));
            }
        }

        if let Some(identity) = state.identity.as_ref() {
            Ok(Some(identity.clone()))
        } else {
            Ok(None)
        }
    }

    pub async fn get_compute(state: DQStateRef) -> Result<DQComputeRef, Error> {
        let mut state = state.lock().await;

        if state.compute.is_none() {
            let config = &state.config.compute;

            let compute =
                create_compute_using_factory(&config.r#type, config.options.clone()).await?;
            state.compute = Some(Arc::new(Mutex::new(compute)));
        }

        let compute = state.compute.as_ref().expect("could not get compute");
        Ok(compute.clone())
    }

    pub async fn get_compute_session(state: DQStateRef) -> Result<DQComputeSessionRef, Error> {
        let mut state = state.lock().await;

        if state.compute.is_none() {
            let config = &state.config.compute;

            let compute =
                create_compute_using_factory(&config.r#type, config.options.clone()).await?;
            state.compute = Some(Arc::new(Mutex::new(compute)));
        }

        let compute = state.compute.clone().expect("could not get compute engine");
        let compute = compute.lock().await;

        compute.prepare().await
    }

    pub async fn get_table(state: DQStateRef, target: &String) -> Result<DQTableRef, Error> {
        let mut state = state.lock().await;

        if !state.tables.contains_key(target) {
            if let Some(config) = state.config.tables.iter().find(|c| &c.name == target) {
                let mut table = create_table_using_factory(&config.storage, config).await?;
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

                    if let Some(field) = fields.last() {
                        items.insert(field.to_string());
                    }
                }
            }
        } else {
            for table in state.config.tables.iter() {
                let fields = table.name.split(".").collect::<Vec<_>>();

                if let Some(field) = fields.last() {
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

    pub async fn has_identity(state: DQStateRef) -> bool {
        let state = state.lock().await;

        if state.metastore.is_some() {
            true
        } else if state.config.identity.is_some() {
            true
        } else {
            false
        }
    }
}
