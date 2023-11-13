use crate::error::DQError;
use arrow_schema::ArrowError;

impl From<ArrowError> for DQError {
    fn from(err: ArrowError) -> DQError {
        DQError::Common {
            message: err.to_string(),
        }
    }
}
