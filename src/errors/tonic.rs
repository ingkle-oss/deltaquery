use crate::error::DQError;
use tonic::metadata::errors::ToStrError;
use tonic::Status;

impl From<DQError> for Status {
    fn from(err: DQError) -> Status {
        Status::internal(err.to_string())
    }
}

impl From<ToStrError> for DQError {
    fn from(err: ToStrError) -> DQError {
        DQError::Common {
            message: err.to_string(),
        }
    }
}
