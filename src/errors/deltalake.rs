use crate::error::DQError;

impl From<deltalake::kernel::Error> for DQError {
    fn from(err: deltalake::kernel::Error) -> DQError {
        DQError::Common {
            message: err.to_string(),
        }
    }
}

impl From<deltalake::protocol::ProtocolError> for DQError {
    fn from(err: deltalake::protocol::ProtocolError) -> DQError {
        DQError::Common {
            message: err.to_string(),
        }
    }
}

impl From<deltalake::DeltaTableError> for DQError {
    fn from(err: deltalake::DeltaTableError) -> DQError {
        DQError::Common {
            message: err.to_string(),
        }
    }
}

impl From<deltalake::ObjectStoreError> for DQError {
    fn from(err: deltalake::ObjectStoreError) -> DQError {
        DQError::Common {
            message: err.to_string(),
        }
    }
}
