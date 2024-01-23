use crate::configs::{DQFilesystemConfig, DQStorageConfig, DQTableConfig};
use anyhow::Error;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use once_cell::sync::Lazy;
use sqlparser::ast::Statement;
use std::collections::HashMap;
use tokio::sync::Mutex;

static TABLE_FACTORIES: Lazy<Mutex<HashMap<String, Box<dyn DQTableFactory>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[async_trait]
pub trait DQTable: Send + Sync {
    async fn update(&mut self) -> Result<(), Error>;
    async fn select(&mut self, statement: &Statement) -> Result<Vec<String>, Error>;
    async fn insert(&mut self, statement: &Statement) -> Result<(), Error>;

    fn schema(&self) -> Option<SchemaRef>;

    fn partition_columns(&self) -> Option<&Vec<String>>;
    fn filesystem_options(&self) -> &HashMap<String, String>;
}

#[async_trait]
pub trait DQTableFactory: Send + Sync {
    async fn create(
        &self,
        table_config: &DQTableConfig,
        storage_config: Option<&DQStorageConfig>,
        filesystem_config: Option<&DQFilesystemConfig>,
    ) -> Box<dyn DQTable>;
}

pub async fn register_table_factory(name: &str, factory: Box<dyn DQTableFactory>) {
    let mut factories = TABLE_FACTORIES.lock().await;
    factories.insert(name.to_string(), factory);
}

pub async fn create_table_using_factory(
    name: &str,
    table_config: &DQTableConfig,
    storage_config: Option<&DQStorageConfig>,
    filesystem_config: Option<&DQFilesystemConfig>,
) -> Option<Box<dyn DQTable>> {
    let factories = TABLE_FACTORIES.lock().await;
    if let Some(factory) = factories.get(name) {
        let table: Box<dyn DQTable> = factory
            .create(table_config, storage_config, filesystem_config)
            .await;
        Some(table)
    } else {
        None
    }
}
