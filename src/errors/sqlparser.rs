use crate::error::DQError;
use sqlparser::parser::ParserError;

impl From<ParserError> for DQError {
    fn from(err: ParserError) -> DQError {
        DQError::Common {
            message: err.to_string(),
        }
    }
}
