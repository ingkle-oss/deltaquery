use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DQStorageConfig {
    pub name: String,
    pub r#type: String,

    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DQComputeConfig {
    pub r#type: String,

    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DQFilesystemConfig {
    pub name: String,

    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, sqlx::FromRow)]
pub struct DQTableConfig {
    pub name: String,
    pub r#type: Option<String>,
    pub storage: Option<String>,
    pub filesystem: Option<String>,
    pub location: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
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

    pub compute: DQComputeConfig,

    #[serde(default)]
    pub storages: Vec<DQStorageConfig>,

    #[serde(default)]
    pub filesystems: Vec<DQFilesystemConfig>,

    #[serde(default)]
    pub tables: Vec<DQTableConfig>,
}
