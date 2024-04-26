use crate::state::{DQState, DQStateRef};
use anyhow::{anyhow, Error};
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use tokio::sync::Mutex;

#[allow(missing_docs)]
#[derive(thiserror::Error, Debug)]
pub enum DQIdentityError {
    #[error("unauthorized: {message}")]
    UNAUTHORIZED { message: String },
}

static IDENTITY_FACTORIES: Lazy<Mutex<HashMap<String, Box<dyn DQIdentityFactory>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[async_trait]
pub trait DQIdentity: Send + Sync {
    async fn validate(&self, token: &str) -> Result<(), Error>;
}

#[async_trait]
pub trait DQIdentityFactory: Send + Sync {
    async fn create(&self, options: serde_yaml::Value) -> Result<Box<dyn DQIdentity>, Error>;
}

pub async fn register_identity_factory(name: &str, factory: Box<dyn DQIdentityFactory>) {
    let mut factories = IDENTITY_FACTORIES.lock().await;
    factories.insert(name.to_string(), factory);
}

pub async fn create_identity_using_factory(
    name: &str,
    options: serde_yaml::Value,
) -> Result<Box<dyn DQIdentity>, Error> {
    let factories = IDENTITY_FACTORIES.lock().await;
    let factory = factories.get(name).expect("could not get identity factory");
    let identity: Box<dyn DQIdentity> = factory.create(options).await?;
    Ok(identity)
}

static BASIC_IDENTITYORIZATION_PREFIX: &str = "Basic ";
static BEARER_IDENTITYORIZATION_PREFIX: &str = "Bearer ";

pub async fn check_token(state: DQStateRef, authorization: &str) -> Result<(), Error> {
    if authorization.starts_with(BASIC_IDENTITYORIZATION_PREFIX) {
        let payload =
            BASE64_STANDARD.decode(&authorization[BASIC_IDENTITYORIZATION_PREFIX.len()..])?;
        let payload = String::from_utf8(payload)?;
        let tokens: Vec<_> = payload.split(':').collect();
        let (username, password) = match tokens.as_slice() {
            [username, password] => (username, password),
            _ => (&"none", &"none"),
        };

        match DQState::get_app(state.clone(), username).await {
            Ok(Some(app)) => match app.password.as_deref() {
                Some(hashed) => {
                    if bcrypt::verify(password, hashed)? {
                        Ok(())
                    } else {
                        Err(anyhow!(DQIdentityError::UNAUTHORIZED {
                            message: "wrong password".into()
                        }))
                    }
                }
                None => Err(anyhow!(DQIdentityError::UNAUTHORIZED {
                    message: "invalid password".into()
                })),
            },
            Ok(None) => Ok(()),
            Err(err) => Err(anyhow!(DQIdentityError::UNAUTHORIZED {
                message: err.to_string()
            })),
        }
    } else if authorization.starts_with(BEARER_IDENTITYORIZATION_PREFIX) {
        match DQState::get_identity(state.clone()).await? {
            Some(identity) => {
                let identity = identity.lock().await;

                let token = &authorization[BEARER_IDENTITYORIZATION_PREFIX.len()..];

                match identity.validate(token).await {
                    Ok(()) => Ok(()),
                    Err(err) => Err(anyhow!(DQIdentityError::UNAUTHORIZED {
                        message: err.to_string()
                    })),
                }
            }
            None => Ok(()),
        }
    } else {
        if DQState::has_identity(state).await {
            Err(anyhow!(DQIdentityError::UNAUTHORIZED {
                message: "no authorization header".into()
            }))
        } else {
            Ok(())
        }
    }
}
