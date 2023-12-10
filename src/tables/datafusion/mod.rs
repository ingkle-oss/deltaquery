use crate::commons::delta;
use crate::configs::{DQEngineConfig, DQFilesystemConfig, DQTableConfig};
use crate::error::DQError;
use crate::metadata::DQMetadataMap;
use crate::state::DQState;
use crate::table::{DQTable, DQTableFactory};
use arrow_array::cast::AsArray;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use datafusion_common::cast::as_string_array;
use deltalake::arrow::datatypes::Schema;
use deltalake::datafusion::common::ToDFSchema;
use deltalake::datafusion::physical_expr::create_physical_expr;
use deltalake::datafusion::physical_expr::execution_props::ExecutionProps;
use deltalake::kernel::{Action, Add, Metadata, Protocol};
use deltalake::logstore::default_logstore::DefaultLogStore;
use deltalake::logstore::LogStoreRef;
use deltalake::ObjectStoreError;
use duckdb::{params, Connection};
use sqlparser::ast::{SetExpr, Statement, TableFactor};
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use tokio::sync::Mutex;
use url::Url;

mod predicates;
mod statistics;

#[allow(dead_code)]
#[derive(Debug)]
struct DQDatafusionFile {
    add: Add,

    from: i64,
    to: i64,
}

pub struct DQDatafusionTable {
    store: LogStoreRef,

    location: String,
    predicates: Option<Vec<String>>,

    version: i64,
    schema: Schema,

    use_versioning: bool,

    protocol: Protocol,
    metadata: Metadata,
    files: HashMap<String, DQDatafusionFile>,

    stats: Vec<RecordBatch>,

    engine: Arc<Mutex<Connection>>,

    engine_options: HashMap<String, String>,
    filesystem_options: HashMap<String, String>,
}

impl DQDatafusionTable {
    pub async fn new(
        table_config: &DQTableConfig,
        engine_config: &DQEngineConfig,
        filesystem_config: Option<&DQFilesystemConfig>,
        _state: &DQState,
    ) -> Self {
        let mut engine_options = HashMap::<String, String>::new();
        for (k, v) in engine_config.configs.iter() {
            engine_options.insert(k.clone(), v.clone());
        }

        let mut filesystem_options = HashMap::<String, String>::new();
        if let Some(filesystem_config) = filesystem_config {
            for (k, v) in filesystem_config.configs.iter() {
                filesystem_options.insert(k.clone(), v.clone());
            }
        }

        let location = table_config.location.trim_end_matches("/").to_string();

        let url: Url = location.parse().unwrap();
        let location = if url.scheme() == "file" {
            url.path().to_string()
        } else {
            location
        };
        let store = Arc::new(DefaultLogStore::try_new(url, filesystem_options.clone()).unwrap());

        let predicates = match table_config.predicates.as_ref() {
            Some(predicates) => {
                if predicates.is_empty() {
                    None
                } else {
                    let predicates = predicates
                        .split(",")
                        .map(|field| field.to_string())
                        .collect::<Vec<String>>();

                    if predicates.len() > 0 {
                        Some(predicates)
                    } else {
                        None
                    }
                }
            }
            None => None,
        };

        if let Some(caching_location) = &table_config.caching_location {
            let _ = fs::create_dir_all(caching_location);
        }

        let engine = Connection::open_in_memory().unwrap();
        setup_duckdb(&engine, &engine_options, &filesystem_options).unwrap();

        DQDatafusionTable {
            store,
            location,
            version: -1,
            predicates: predicates,
            use_versioning: table_config.use_versioning.unwrap_or(false),
            protocol: Protocol::default(),
            metadata: Metadata::default(),
            schema: Schema::empty(),
            files: HashMap::new(),
            stats: Vec::new(),
            engine: Arc::new(Mutex::new(engine)),
            engine_options,
            filesystem_options,
        }
    }

    fn update_actions(&mut self, actions: &Vec<Action>, version: i64) -> Result<(), DQError> {
        for action in actions {
            if let Action::Add(add) = action {
                self.files.insert(
                    add.path.clone(),
                    DQDatafusionFile {
                        add: add.clone(),
                        from: version,
                        to: i64::MAX,
                    },
                );
            } else if let Action::Remove(remove) = action {
                if self.use_versioning {
                    if let Some(file) = self.files.get_mut(remove.path.as_str()) {
                        file.to = version;
                    }
                } else {
                    self.files.remove(remove.path.as_str());
                }
            } else if let Action::Protocol(protocol) = action {
                self.protocol = protocol.clone();
            } else if let Action::Metadata(metadata) = action {
                self.metadata = metadata.clone();
                self.schema = Schema::try_from(&metadata.schema().unwrap()).unwrap();
            }
        }

        Ok(())
    }

    async fn update_commits(&mut self) -> Result<(), DQError> {
        let mut actions = Vec::new();

        let mut version = self.version;

        loop {
            match delta::peek_commit(&self.store, version + 1).await {
                Ok(__actions) => {
                    self.update_actions(&__actions, version + 1)?;

                    actions.extend(__actions);

                    version = version + 1;
                }
                Err(ObjectStoreError::NotFound { .. }) => {
                    break;
                }
                Err(err) => panic!("could not peek commit: {:?}", err),
            }
        }

        if actions.len() > 0 {
            let stats = statistics::get_record_batch_from_actions(
                &actions,
                &self.schema,
                self.predicates.as_ref(),
            )?;
            self.stats.push(stats);

            self.version = version;

            log::info!("update={:?} version", self.version);
        }

        Ok(())
    }
}

#[async_trait]
impl DQTable for DQDatafusionTable {
    async fn update(&mut self) -> Result<(), DQError> {
        if self.version >= 0 {
            self.update_commits().await?;
        } else {
            if self.use_versioning {
                self.update_commits().await?;
            } else if let Ok(checkpoint) = delta::get_last_checkpoint(&self.store).await {
                let actions = delta::peek_checkpoint(&self.store, &checkpoint).await;
                self.update_actions(&actions, 0)?;

                let stats = statistics::get_record_batch_from_actions(
                    &actions,
                    &self.schema,
                    self.predicates.as_ref(),
                )?;
                self.stats.push(stats);

                self.version = checkpoint.version;

                log::info!("checkpoint={:?} version", self.version);

                self.update_commits().await?;
            } else {
                self.update_commits().await?;
            }
        }

        Ok(())
    }

    async fn select(
        &mut self,
        statement: &Statement,
        _metadata: &DQMetadataMap,
    ) -> Result<Vec<RecordBatch>, DQError> {
        let mut batches = Vec::new();

        match statement {
            Statement::Query(query) => match query.body.as_ref() {
                SetExpr::Select(select) => {
                    let mut from = String::default();
                    let mut files: Vec<String> = Vec::new();

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

                    if let (Some(selection), Some(batch0)) = (&select.selection, self.stats.first())
                    {
                        let expressions = predicates::parse_expression(
                            &selection,
                            &self
                                .schema
                                .fields
                                .iter()
                                .map(|field| field.name().as_str())
                                .collect(),
                            false,
                        );

                        log::info!("filters={:#?}", expressions);

                        let schema = batch0.schema();
                        let predicates = create_physical_expr(
                            &expressions,
                            &schema.clone().to_dfschema().unwrap(),
                            &schema,
                            &ExecutionProps::new(),
                        )?;

                        for batch in self.stats.iter() {
                            let results = predicates
                                .evaluate(&batch)
                                .ok()
                                .unwrap()
                                .into_array(batch.num_rows())
                                .unwrap();
                            let paths = as_string_array(
                                batch
                                    .column_by_name(statistics::STATS_TABLE_ADD_PATH)
                                    .unwrap(),
                            )?;
                            for (result, path) in results.as_boolean().iter().zip(paths) {
                                if let (Some(result), Some(path)) = (result, path) {
                                    if result {
                                        files.push(format!(
                                            "{}/{}",
                                            self.location,
                                            path.to_string()
                                        ));
                                    }
                                }
                            }
                        }
                    } else {
                        files.extend(
                            self.files
                                .keys()
                                .map(|path| format!("{}/{}", self.location, path))
                                .collect::<Vec<_>>(),
                        );
                    }

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

pub struct DQDatafusionTableFactory {}

impl DQDatafusionTableFactory {
    pub fn new() -> Self {
        DQDatafusionTableFactory {}
    }
}

#[async_trait]
impl DQTableFactory for DQDatafusionTableFactory {
    async fn create(
        &self,
        table_config: &DQTableConfig,
        engine_config: &DQEngineConfig,
        filesystem_config: Option<&DQFilesystemConfig>,
        state: &DQState,
    ) -> Box<dyn DQTable> {
        Box::new(
            DQDatafusionTable::new(table_config, engine_config, filesystem_config, state).await,
        )
    }
}

fn setup_duckdb(
    engine: &Connection,
    engine_options: &HashMap<String, String>,
    filesystem_options: &HashMap<String, String>,
) -> Result<(), DQError> {
    engine.execute("PRAGMA enable_object_cache", params![])?;

    if let Some(memory_limit) = engine_options.get("memory_limit") {
        engine.execute(&format!("SET memory_limit='{}'", memory_limit), params![])?;
    }

    if let Some(http_keep_alive) = engine_options.get("http_keep_alive") {
        engine.execute(
            &format!("SET http_keep_alive={}", http_keep_alive),
            params![],
        )?;
    }
    if let Some(http_retries) = engine_options.get("http_retries") {
        engine.execute(&format!("SET http_retries={}", http_retries), params![])?;
    }
    if let Some(http_retry_backoff) = engine_options.get("http_retry_backoff") {
        engine.execute(
            &format!("SET http_retry_backoff={}", http_retry_backoff),
            params![],
        )?;
    }
    if let Some(http_retry_wait_ms) = engine_options.get("http_retry_wait_ms") {
        engine.execute(
            &format!("SET http_retry_wait_ms={}", http_retry_wait_ms),
            params![],
        )?;
    }
    if let Some(http_timeout) = engine_options.get("http_timeout") {
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
