use crate::error::DQError;
use duckdb::Error;

impl From<Error> for DQError {
    fn from(err: Error) -> DQError {
        DQError::Common {
            message: err.to_string(),
        }
    }
}
