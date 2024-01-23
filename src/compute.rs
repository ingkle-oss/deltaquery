use crate::configs::DQComputeConfig;
use crate::state::DQState;
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
    #[error("no table")]
    NoTable,

    #[error("invalid sql")]
    InvalidSql,

    #[error("not supported yet")]
    NotSupportedYet,
}

static COMPUTE_FACTORIES: Lazy<Mutex<HashMap<String, Box<dyn DQComputeFactory>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[async_trait]
pub trait DQCompute: Send + Sync {
    async fn execute(
        &mut self,
        statement: &Statement,
        state: Arc<Mutex<DQState>>,
    ) -> Result<Vec<RecordBatch>, Error>;

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
pub trait DQComputeFactory: Send + Sync {
    async fn create(&self, compute_config: Option<&DQComputeConfig>) -> Box<dyn DQCompute>;
}

pub async fn register_compute_factory(name: &str, factory: Box<dyn DQComputeFactory>) {
    let mut factories = COMPUTE_FACTORIES.lock().await;
    factories.insert(name.to_string(), factory);
}

pub async fn create_compute_using_factory(
    name: &str,
    compute_config: Option<&DQComputeConfig>,
) -> Option<Box<dyn DQCompute>> {
    let factories = COMPUTE_FACTORIES.lock().await;
    if let Some(factory) = factories.get(name) {
        let compute: Box<dyn DQCompute> = factory.create(compute_config).await;
        Some(compute)
    } else {
        None
    }
}
