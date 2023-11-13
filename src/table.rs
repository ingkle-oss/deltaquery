use crate::error::DQError;
use crate::metadata::DQMetadataMap;
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
