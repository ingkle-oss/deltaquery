use crate::error::DQError;

impl From<datafusion_common::DataFusionError> for DQError {
    fn from(err: datafusion_common::DataFusionError) -> DQError {
        DQError::Common {
            message: err.to_string(),
        }
    }
}
