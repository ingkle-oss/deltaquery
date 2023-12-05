use crate::configs::{DQEngineConfig, DQFilesystemConfig, DQTableConfig};
use crate::error::DQError;
use crate::metadata::DQMetadataMap;
use crate::state::DQState;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use once_cell::sync::Lazy;
use sqlparser::ast::Statement;
use std::collections::HashMap;
use tokio::sync::Mutex;

static TABLE_FACTORIES: Lazy<Mutex<HashMap<String, Box<dyn DQTableFactory>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[async_trait]
pub trait DQTable: Send + Sync {
    async fn update(&mut self) -> Result<(), DQError>;
    async fn select(
        &mut self,
        statement: &Statement,
        metadata: &DQMetadataMap,
    ) -> Result<Vec<RecordBatch>, DQError>;
}

#[async_trait]
pub trait DQTableFactory: Send + Sync {
    async fn create(
        &self,
        table_config: &DQTableConfig,
        engine_config: &DQEngineConfig,
        filesystem_config: Option<&DQFilesystemConfig>,
        state: &DQState,
    ) -> Box<dyn DQTable>;
}

pub async fn register_table_factory(name: &str, factory: Box<dyn DQTableFactory>) {
    let mut factories = TABLE_FACTORIES.lock().await;
    factories.insert(name.to_string(), factory);
}

pub async fn create_table_using_factory(
    name: &str,
    table_config: &DQTableConfig,
    engine_config: &DQEngineConfig,
    filesystem_config: Option<&DQFilesystemConfig>,
    state: &DQState,
) -> Option<Box<dyn DQTable>> {
    let factories = TABLE_FACTORIES.lock().await;
    if let Some(factory) = factories.get(name) {
        let table: Box<dyn DQTable> = factory
            .create(table_config, engine_config, filesystem_config, state)
            .await;
        Some(table)
    } else {
        None
    }
}
