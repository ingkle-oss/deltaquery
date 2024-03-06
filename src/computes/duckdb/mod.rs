use crate::compute::{DQCompute, DQComputeError, DQComputeFactory, DQComputeSession};
use crate::state::{DQComputeSessionRef, DQState, DQStateRef};
use anyhow::{anyhow, Error};
use arrow::array::RecordBatch;
use async_trait::async_trait;
use duckdb::{params, Connection};
use sqlparser::ast::{SetExpr, Statement, TableFactor};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

pub struct DQDuckDBCompute {
    compute_options: HashMap<String, String>,
}

impl DQDuckDBCompute {
    pub async fn try_new(compute_options: serde_yaml::Value) -> Result<Self, Error> {
        let compute_options: HashMap<String, String> =
            serde_yaml::from_value(compute_options).expect("could not get compute options");

        Ok(DQDuckDBCompute { compute_options })
    }
}

#[async_trait]
impl DQCompute for DQDuckDBCompute {
    async fn prepare(&self) -> Result<DQComputeSessionRef, Error> {
        let session: DQComputeSessionRef =
            Box::new(DQDuckDBComputeSession::try_new(&self.compute_options).await?);

        Ok(session)
    }
}

pub struct DQDuckDBComputeSession {
    compute_options: HashMap<String, String>,
}

impl DQDuckDBComputeSession {
    pub async fn try_new(compute_options: &HashMap<String, String>) -> Result<Self, Error> {
        Ok(DQDuckDBComputeSession {
            compute_options: compute_options.clone(),
        })
    }
}

#[async_trait]
impl DQComputeSession for DQDuckDBComputeSession {
    async fn execute(
        &self,
        statement: &Statement,
        state: DQStateRef,
    ) -> Result<Arc<Vec<RecordBatch>>, Error> {
        match statement {
            Statement::Query(query) => {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    for table in &select.from {
                        match &table.relation {
                            TableFactor::Table { name, .. } => {
                                let target = name
                                    .0
                                    .iter()
                                    .map(|o| o.value.clone())
                                    .collect::<Vec<String>>();
                                let target = target.join(".");

                                let table = DQState::get_table(state.clone(), &target).await?;
                                let mut table = table.lock().await;
                                let files = table.select(statement).await?;

                                log::info!("files={:#?}, {}", files, files.len());

                                if files.len() > 0 {
                                    let engine = Connection::open_in_memory()?;
                                    setup_duckdb(
                                        &engine,
                                        &self.compute_options,
                                        table.filesystem_options(),
                                    )?;

                                    let files = files
                                        .iter()
                                        .map(|file| format!("'{}'", file))
                                        .collect::<Vec<String>>()
                                        .join(",");

                                    let mut stmt =
                                        engine.prepare(&statement.to_string().replace(
                                            &target,
                                            &format!(
                                                "read_parquet([{}], union_by_name=true)",
                                                files
                                            ),
                                        ))?;

                                    let batches =
                                        stmt.query_arrow([])?.collect::<Vec<RecordBatch>>();

                                    return Ok(Arc::new(batches));
                                }
                            }
                            _ => {
                                return Err(anyhow!(DQComputeError::NotSupportedYet {
                                    message: table.relation.to_string()
                                }))
                            }
                        }
                    }
                }
            }
            _ => {
                return Err(anyhow!(DQComputeError::NotSupportedYet {
                    message: statement.to_string()
                }))
            }
        }

        Err(anyhow!(DQComputeError::InvalidSql {
            message: statement.to_string()
        }))
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
        compute_options: serde_yaml::Value,
    ) -> Result<Box<dyn DQCompute>, Error> {
        Ok(Box::new(DQDuckDBCompute::try_new(compute_options).await?))
    }
}

fn setup_duckdb(
    engine: &Connection,
    compute_options: &HashMap<String, String>,
    filesystem_options: &HashMap<String, String>,
) -> Result<(), Error> {
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
