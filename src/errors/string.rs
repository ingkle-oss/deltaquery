use crate::error::DQError;
use std::string::FromUtf8Error;

impl From<FromUtf8Error> for DQError {
    fn from(err: FromUtf8Error) -> DQError {
        DQError::Common {
            message: err.to_string(),
        }
    }
}
