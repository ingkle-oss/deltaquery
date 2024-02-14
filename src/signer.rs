use anyhow::Error;
use async_trait::async_trait;
use std::time::Duration;

pub type DQSignerRef = Box<dyn DQSigner>;

#[async_trait]
pub trait DQSigner: Send + Sync {
    async fn sign(&self, bucket: &str, object: &str, expires: Duration) -> Result<String, Error>;
}
