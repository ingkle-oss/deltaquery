use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use struct_field_names_as_array::FieldNamesAsArray;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DQEngineConfig {
    pub name: String,
    pub r#type: String,
    pub configs: HashMap<String, String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DQFilesystemConfig {
    pub name: String,
    pub configs: HashMap<String, String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, sqlx::FromRow, FieldNamesAsArray)]
#[field_names_as_array(visibility = "pub(super)")]
#[serde(rename_all = "camelCase")]
pub struct DQTableConfig {
    pub name: String,
    pub engine: String,
    pub filesystem: Option<String>,
    pub location: String,
    pub predicates: Option<String>,
    pub use_versioning: Option<bool>,
    pub use_record_caching: Option<bool>,
    pub use_parquet_caching: Option<bool>,
    pub caching_location: Option<String>,
    pub caching_retention: Option<String>,
    pub caching_codec: Option<String>,
    pub caching_with_partitions: Option<bool>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DQMetastoreConfig {
    pub url: String,

    pub tables: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DQTlsConfig {
    pub server_cert: String,
    pub server_key: String,
    pub client_cert: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DQConfig {
    pub version: i32,
    pub server: String,
    pub listen: String,

    pub metastore: Option<DQMetastoreConfig>,
    pub tls: Option<DQTlsConfig>,

    #[serde(default)]
    pub engines: Vec<DQEngineConfig>,

    #[serde(default)]
    pub filesystems: Vec<DQFilesystemConfig>,

    #[serde(default)]
    pub tables: Vec<DQTableConfig>,
}
