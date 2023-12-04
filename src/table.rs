use crate::configs::{DQFilesystemConfig, DQTableConfig};
use crate::error::DQError;
use crate::metadata::DQMetadataMap;
use crate::state::DQState;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use sqlparser::ast::Statement;

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
        filesystem_config: Option<&DQFilesystemConfig>,
        state: &DQState,
    ) -> Box<dyn DQTable>;
}
