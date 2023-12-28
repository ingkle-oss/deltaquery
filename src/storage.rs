use crate::configs::{DQFilesystemConfig, DQStorageConfig, DQTableConfig};
use crate::error::DQError;
use arrow::datatypes::Schema;
use async_trait::async_trait;
use once_cell::sync::Lazy;
use sqlparser::ast::Statement;
use std::collections::HashMap;
use tokio::sync::Mutex;

static STORAGE_FACTORIES: Lazy<Mutex<HashMap<String, Box<dyn DQStorageFactory>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[async_trait]
pub trait DQStorage: Send + Sync {
    async fn update(&mut self) -> Result<(), DQError>;
    async fn execute(&mut self, statement: &Statement) -> Result<Vec<String>, DQError>;

    fn schema(&self) -> Option<Schema>;
}

#[async_trait]
pub trait DQStorageFactory: Send + Sync {
    async fn create(
        &self,
        table_config: &DQTableConfig,
        storage_config: Option<&DQStorageConfig>,
        filesystem_config: Option<&DQFilesystemConfig>,
    ) -> Box<dyn DQStorage>;
}

pub async fn register_storage_factory(name: &str, factory: Box<dyn DQStorageFactory>) {
    let mut factories = STORAGE_FACTORIES.lock().await;
    factories.insert(name.to_string(), factory);
}

pub async fn create_storage_using_factory(
    name: &str,
    table_config: &DQTableConfig,
    storage_config: Option<&DQStorageConfig>,
    filesystem_config: Option<&DQFilesystemConfig>,
) -> Option<Box<dyn DQStorage>> {
    let factories = STORAGE_FACTORIES.lock().await;
    if let Some(factory) = factories.get(name) {
        let storage: Box<dyn DQStorage> = factory
            .create(table_config, storage_config, filesystem_config)
            .await;
        Some(storage)
    } else {
        None
    }
}
