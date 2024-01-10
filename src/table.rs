use crate::compute::DQCompute;
use crate::error::DQError;
use crate::storage::DQStorage;
use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use sqlparser::ast::{SetExpr, Statement};
use std::sync::Arc;

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
        match statement {
            Statement::Query(query) => {
                if let SetExpr::Select(_) = query.body.as_ref() {
                    let files = self.storage.select(statement).await?;
                    let schema = self.storage.schema();
                    let batches = self.compute.select(statement, schema, files).await?;

                    Ok(batches)
                } else {
                    unimplemented!()
                }
            }
            Statement::Insert { .. } => {
                self.storage.insert(statement).await?;

                let batches = vec![RecordBatch::new_empty(Arc::new(Schema::empty()))];

                Ok(batches)
            }
            _ => unimplemented!(),
        }
    }
}
