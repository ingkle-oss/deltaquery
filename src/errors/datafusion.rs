use crate::error::DQError;

impl From<deltalake::datafusion::common::DataFusionError> for DQError {
    fn from(err: deltalake::datafusion::common::DataFusionError) -> DQError {
        DQError::Common {
            message: err.to_string(),
        }
    }
}
