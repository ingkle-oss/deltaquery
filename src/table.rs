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

    async fn sign(&self, files: &Vec<String>) -> Result<Vec<String>, Error>;

    fn name(&self) -> &String;

    fn schema(&self) -> Option<SchemaRef>;
    fn partition_columns(&self) -> &Vec<String>;

    fn location(&self) -> &String;
    fn filesystem_options(&self) -> &HashMap<String, String>;
    fn table_options(&self) -> &HashMap<String, String>;
    fn data_format(&self) -> &String;
}

#[async_trait]
pub trait DQTableFactory: Send + Sync {
    async fn create(
        &self,
        table_config: &DQTableConfig,
        storage_config: Option<&DQStorageConfig>,
        filesystem_config: Option<&DQFilesystemConfig>,
    ) -> Result<Box<dyn DQTable>, Error>;
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
) -> Result<Box<dyn DQTable>, Error> {
    let factories = TABLE_FACTORIES.lock().await;
    let factory = factories.get(name).expect("could not get table factory");
    let table: Box<dyn DQTable> = factory
        .create(table_config, storage_config, filesystem_config)
        .await?;
    Ok(table)
}
