use crate::error::DQError;

impl From<tera::Error> for DQError {
    fn from(err: tera::Error) -> DQError {
        DQError::Common {
            message: err.to_string(),
        }
    }
}
