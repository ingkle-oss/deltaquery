use crate::state::{DQState, DQStateRef};
use anyhow::{anyhow, Error};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;

#[allow(missing_docs)]
#[derive(thiserror::Error, Debug)]
pub enum DQAuthError {
    #[error("unauthorized: {message}")]
    UNAUTHORIZED { message: String },
}

static BASIC_AUTHORIZATION_PREFIX: &str = "Basic ";
static BEARER_AUTHORIZATION_PREFIX: &str = "Bearer ";

pub async fn check_token(state: DQStateRef, authorization: &str) -> Result<(), Error> {
    if authorization.starts_with(BASIC_AUTHORIZATION_PREFIX) {
        let payload = BASE64_STANDARD.decode(&authorization[BASIC_AUTHORIZATION_PREFIX.len()..])?;
        let payload = String::from_utf8(payload)?;
        let tokens: Vec<_> = payload.split(':').collect();
        let (username, password) = match tokens.as_slice() {
            [username, password] => (username, password),
            _ => (&"none", &"none"),
        };

        match DQState::get_app(state.clone(), username).await? {
            Some(app) => match app.password.as_deref() {
                Some(password0) => {
                    if password0 == *password {
                        Ok(())
                    } else {
                        Err(anyhow!(DQAuthError::UNAUTHORIZED {
                            message: "wrong password".into()
                        }))
                    }
                }
                None => Err(anyhow!(DQAuthError::UNAUTHORIZED {
                    message: "invalid password".into()
                })),
            },
            None => Err(anyhow!(DQAuthError::UNAUTHORIZED {
                message: "no app".into()
            })),
        }
    } else if authorization.starts_with(BEARER_AUTHORIZATION_PREFIX) {
        Err(anyhow!(DQAuthError::UNAUTHORIZED {
            message: "not supported yet".into()
        }))
    } else {
        Err(anyhow!(DQAuthError::UNAUTHORIZED {
            message: "invalid authorization header".into()
        }))
    }
}
