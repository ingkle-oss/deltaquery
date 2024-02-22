use crate::configs::DQComputeConfig;
use crate::state::{DQComputeSessionRef, DQStateRef};
use anyhow::Error;
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, SchemaRef};
use async_trait::async_trait;
use once_cell::sync::Lazy;
use sqlparser::ast::Statement;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[allow(missing_docs)]
#[derive(thiserror::Error, Debug)]
pub enum DQComputeError {
    #[error("no table: {message}")]
    NoTable { message: String },

    #[error("invalid sql: {message}")]
    InvalidSql { message: String },

    #[error("not supported yet: {message}")]
    NotSupportedYet { message: String },
}

static COMPUTE_FACTORIES: Lazy<Mutex<HashMap<String, Box<dyn DQComputeFactory>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[async_trait]
pub trait DQCompute: Send + Sync {
    async fn prepare(&self) -> Result<DQComputeSessionRef, Error>;

    fn get_function_return_type(
        &self,
        _func: &String,
        _args: &Vec<String>,
        _schema: SchemaRef,
    ) -> Option<DataType> {
        None
    }
}

#[async_trait]
pub trait DQComputeSession: Send + Sync {
    async fn execute(
        &self,
        statement: &Statement,
        state: DQStateRef,
    ) -> Result<Arc<Vec<RecordBatch>>, Error>;
}

#[async_trait]
pub trait DQComputeFactory: Send + Sync {
    async fn create(
        &self,
        compute_config: Option<&DQComputeConfig>,
    ) -> Result<Box<dyn DQCompute>, Error>;
}

pub async fn register_compute_factory(name: &str, factory: Box<dyn DQComputeFactory>) {
    let mut factories = COMPUTE_FACTORIES.lock().await;
    factories.insert(name.to_string(), factory);
}

pub async fn create_compute_using_factory(
    name: &str,
    compute_config: Option<&DQComputeConfig>,
) -> Result<Box<dyn DQCompute>, Error> {
    let factories = COMPUTE_FACTORIES.lock().await;
    let factory = factories.get(name).expect("could not get compute factory");
    let compute: Box<dyn DQCompute> = factory.create(compute_config).await?;
    Ok(compute)
}
