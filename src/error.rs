#[allow(missing_docs)]
#[derive(thiserror::Error, Debug)]
pub enum DQError {
    #[error("{message}")]
    Common { message: String },
}
