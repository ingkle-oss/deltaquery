use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use struct_field_names_as_array::FieldNamesAsArray;

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

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, sqlx::FromRow, FieldNamesAsArray)]
#[field_names_as_array(visibility = "pub(super)")]
pub struct DQTableConfig {
    pub name: String,
    pub storage: Option<String>,
    pub filesystem: Option<String>,
    pub location: Option<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DQMetastoreConfig {
    pub url: String,

    pub tables: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DQTlsConfig {
    pub server_cert: String,
    pub server_key: String,
    pub client_cert: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DQConfig {
    pub version: i32,
    pub server: String,
    pub listen: String,

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
