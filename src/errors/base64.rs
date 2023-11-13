use crate::error::DQError;
use base64::DecodeError;

impl From<DecodeError> for DQError {
    fn from(err: DecodeError) -> DQError {
        DQError::Common {
            message: err.to_string(),
        }
    }
}
