use crate::commons::sql;
use crate::compute::{DQCompute, DQComputeFactory};
use crate::configs::{DQComputeConfig, DQFilesystemConfig, DQTableConfig};
use crate::error::DQError;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use duckdb::{params, Connection};
use sqlparser::ast::Statement;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use url::Url;

pub struct DQDuckDBCompute {
    engine: Arc<Mutex<Connection>>,
}

impl DQDuckDBCompute {
    pub async fn new(
        _table_config: &DQTableConfig,
        compute_config: Option<&DQComputeConfig>,
        filesystem_config: Option<&DQFilesystemConfig>,
    ) -> Self {
        let compute_options =
            compute_config.map_or(HashMap::new(), |config| config.options.clone());
        let filesystem_options =
            filesystem_config.map_or(HashMap::new(), |config| config.options.clone());

        let engine = Connection::open_in_memory().unwrap();
        setup_duckdb(&engine, &compute_options, &filesystem_options).unwrap();

        DQDuckDBCompute {
            engine: Arc::new(Mutex::new(engine)),
        }
    }
}

#[async_trait]
impl DQCompute for DQDuckDBCompute {
    async fn execute(
        &mut self,
        statement: &Statement,
        _schema: Option<SchemaRef>,
        files: Vec<String>,
    ) -> Result<Vec<RecordBatch>, DQError> {
        let mut batches = Vec::new();

        if let Some(from) = sql::get_table(statement) {
            log::info!("files={:#?}, {}", files, files.len());

            if files.len() > 0 {
                let engine = self.engine.lock().await;

                let files = files
                    .iter()
                    .map(|file| format!("'{}'", file))
                    .collect::<Vec<String>>()
                    .join(",");

                let mut stmt = engine.prepare(&statement.to_string().replace(
                    &from,
                    &format!("read_parquet([{}], union_by_name=true)", files),
                ))?;

                batches.extend(stmt.query_arrow([])?.collect::<Vec<RecordBatch>>());
            }
        }

        Ok(batches)
    }
}

pub struct DQDuckDBComputeFactory {}

impl DQDuckDBComputeFactory {
    pub fn new() -> Self {
        DQDuckDBComputeFactory {}
    }
}

#[async_trait]
impl DQComputeFactory for DQDuckDBComputeFactory {
    async fn create(
        &self,
        table_config: &DQTableConfig,
        compute_config: Option<&DQComputeConfig>,
        filesystem_config: Option<&DQFilesystemConfig>,
    ) -> Box<dyn DQCompute> {
        Box::new(DQDuckDBCompute::new(table_config, compute_config, filesystem_config).await)
    }
}

fn setup_duckdb(
    engine: &Connection,
    compute_options: &HashMap<String, String>,
    filesystem_options: &HashMap<String, String>,
) -> Result<(), DQError> {
    engine.execute("PRAGMA enable_object_cache", params![])?;

    if let Some(memory_limit) = compute_options.get("memory_limit") {
        engine.execute(&format!("SET memory_limit='{}'", memory_limit), params![])?;
    }

    if let Some(http_keep_alive) = compute_options.get("http_keep_alive") {
        engine.execute(
            &format!("SET http_keep_alive={}", http_keep_alive),
            params![],
        )?;
    }
    if let Some(http_retries) = compute_options.get("http_retries") {
        engine.execute(&format!("SET http_retries={}", http_retries), params![])?;
    }
    if let Some(http_retry_backoff) = compute_options.get("http_retry_backoff") {
        engine.execute(
            &format!("SET http_retry_backoff={}", http_retry_backoff),
            params![],
        )?;
    }
    if let Some(http_retry_wait_ms) = compute_options.get("http_retry_wait_ms") {
        engine.execute(
            &format!("SET http_retry_wait_ms={}", http_retry_wait_ms),
            params![],
        )?;
    }
    if let Some(http_timeout) = compute_options.get("http_timeout") {
        engine.execute(&format!("SET http_timeout={}", http_timeout), params![])?;
    }

    if let (Some(s3_access_key_id), Some(s3_secret_access_key)) = (
        filesystem_options.get("AWS_ACCESS_KEY_ID"),
        filesystem_options.get("AWS_SECRET_ACCESS_KEY"),
    ) {
        engine.execute(
            &format!("SET s3_access_key_id='{}'", s3_access_key_id),
            params![],
        )?;
        engine.execute(
            &format!("SET s3_secret_access_key='{}'", s3_secret_access_key),
            params![],
        )?;

        engine.execute("SET s3_url_style='path'", params![])?;

        if let Some(s3_endpoint) = filesystem_options.get("AWS_ENDPOINT_URL") {
            let url: Url = s3_endpoint.parse().unwrap();

            match (url.host_str(), url.port()) {
                (Some(host), Some(port)) => {
                    engine.execute(&format!("SET s3_endpoint='{}:{}'", host, port), params![])?;
                }
                (Some(host), None) => {
                    engine.execute(&format!("SET s3_endpoint='{}'", host), params![])?;
                }
                _ => unimplemented!(),
            }
        }
        if let Some(s3_region) = filesystem_options.get("AWS_REGION") {
            engine.execute(&format!("SET s3_region='{}'", s3_region), params![])?;
        }
        if let Some(s3_allow_http) = filesystem_options.get("AWS_ALLOW_HTTP") {
            if s3_allow_http == "true" {
                engine.execute("SET s3_use_ssl=false", params![])?;
            } else {
                engine.execute("SET s3_use_ssl=true", params![])?;
            }
        }
    }

    Ok(())
}
