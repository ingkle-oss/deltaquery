use crate::signer::DQSigner;
use anyhow::Error;
use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::{BehaviorVersion, Region};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::Client;
use std::collections::HashMap;
use std::time::Duration;

pub struct DQS3Signer {
    client: Client,
}

impl DQS3Signer {
    pub async fn try_new(options: &HashMap<String, String>) -> Result<Self, Error> {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());

        if let Some(endpoint) = options.get("AWS_ENDPOINT_URL") {
            loader = loader.endpoint_url(endpoint);
        }
        if let (Some(accesskey), Some(secretkey)) = (
            options.get("AWS_ACCESS_KEY_ID"),
            options.get("AWS_SECRET_ACCESS_KEY"),
        ) {
            loader = loader.credentials_provider(Credentials::new(
                accesskey,
                secretkey,
                None,
                None,
                "deltaquery",
            ));
        }
        if let Some(region) = options.get("AWS_REGION") {
            let provider = RegionProviderChain::first_try(Region::new(region.clone()))
                .or_default_provider()
                .or_else(Region::new("ap-northeast-2"));
            loader = loader.region(provider);
        } else {
            let provider = RegionProviderChain::default_provider();
            loader = loader.region(provider);
        }

        let config = loader.load().await;

        let client = Client::new(&config);

        Ok(DQS3Signer { client })
    }
}

#[async_trait]
impl DQSigner for DQS3Signer {
    async fn sign(&self, bucket: &str, object: &str, expires: Duration) -> Result<String, Error> {
        let presigned_request = self
            .client
            .get_object()
            .bucket(bucket)
            .key(if object.starts_with("/") {
                &object[1..]
            } else {
                object
            })
            .presigned(PresigningConfig::expires_in(expires).unwrap())
            .await?;

        Ok(presigned_request.uri().to_string())
    }
}
