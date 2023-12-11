use crate::compute::{DQCompute, DQComputeFactory};
use crate::configs::{DQComputeConfig, DQFilesystemConfig};
use crate::error::DQError;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use duckdb::{params, Connection};
use sqlparser::ast::{SetExpr, Statement, TableFactor};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use url::Url;

pub struct DQDuckDBCompute {
    engine: Arc<Mutex<Connection>>,
}

impl DQDuckDBCompute {
    pub async fn new(
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
        files: Vec<String>,
    ) -> Result<Vec<RecordBatch>, DQError> {
        let mut batches = Vec::new();

        match statement {
            Statement::Query(query) => match query.body.as_ref() {
                SetExpr::Select(select) => {
                    let mut from = String::default();

                    for table in &select.from {
                        match &table.relation {
                            TableFactor::Table { name, .. } => {
                                let target = name
                                    .0
                                    .iter()
                                    .map(|o| o.value.clone())
                                    .collect::<Vec<String>>();

                                from = target.join(".");
                            }
                            _ => unimplemented!(),
                        }
                    }

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
                _ => unreachable!(),
            },
            _ => unreachable!(),
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
        compute_config: Option<&DQComputeConfig>,
        filesystem_config: Option<&DQFilesystemConfig>,
    ) -> Box<dyn DQCompute> {
        Box::new(DQDuckDBCompute::new(compute_config, filesystem_config).await)
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

            engine.execute(
                &format!(
                    "SET s3_endpoint='{}:{}'",
                    url.host_str().unwrap(),
                    url.port().unwrap()
                ),
                params![],
            )?;
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
