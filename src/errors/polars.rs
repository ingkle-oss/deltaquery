use crate::error::DQError;
use polars::prelude::*;

impl From<PolarsError> for DQError {
    fn from(err: PolarsError) -> DQError {
        DQError::Common {
            message: err.to_string(),
        }
    }
}
