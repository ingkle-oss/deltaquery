use crate::compute::DQCompute;
use crate::error::DQError;
use crate::storage::DQStorage;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use sqlparser::ast::Statement;

pub struct DQTable {
    pub storage: Box<dyn DQStorage>,
    pub compute: Box<dyn DQCompute>,
}

impl DQTable {
    pub fn new(storage: Box<dyn DQStorage>, compute: Box<dyn DQCompute>) -> Self {
        DQTable { storage, compute }
    }

    pub fn schema(&self) -> Option<SchemaRef> {
        self.storage.schema()
    }

    pub async fn update(&mut self) -> Result<(), DQError> {
        self.storage.update().await?;

        Ok(())
    }

    pub async fn execute(&mut self, statement: &Statement) -> Result<Vec<RecordBatch>, DQError> {
        let files = self.storage.execute(statement).await?;
        let schema = self.storage.schema();
        let batches = self.compute.execute(statement, schema, files).await?;

        Ok(batches)
    }
}
