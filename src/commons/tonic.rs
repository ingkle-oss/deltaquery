use anyhow::Error;
use tonic::Status;

pub fn to_tonic_error(e: Error) -> Status {
    Status::internal(format!("{e:?}"))
}
