use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DQComputeConfig {
    pub r#type: String,

    #[serde(default)]
    pub options: serde_yaml::Value,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DQIdentityConfig {
    pub r#type: String,

    #[serde(default)]
    pub options: serde_yaml::Value,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DQTableConfig {
    pub name: String,
    pub r#type: String,
    pub storage: String,
    pub location: String,

    pub partitions: Option<Vec<String>>,
    pub options: Option<HashMap<String, String>>,

    pub created_at: Option<NaiveDateTime>,
    pub updated_at: Option<NaiveDateTime>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DQAppConfig {
    pub name: String,
    pub password: Option<String>,
    pub created_at: Option<NaiveDateTime>,
    pub updated_at: Option<NaiveDateTime>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DQMetastoreConfig {
    pub url: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DQTlsConfig {
    pub server_cert: String,
    pub server_key: String,
    pub client_cert: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DQRuntimeConfig {
    pub worker_threads: Option<usize>,
    pub blocking_threads: Option<usize>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DQConfig {
    pub version: i32,
    pub server: String,
    pub listen: String,

    pub runtime: Option<DQRuntimeConfig>,

    pub metastore: Option<DQMetastoreConfig>,
    pub tls: Option<DQTlsConfig>,

    pub identity: Option<DQIdentityConfig>,

    pub compute: DQComputeConfig,

    #[serde(default)]
    pub tables: Vec<DQTableConfig>,
}
