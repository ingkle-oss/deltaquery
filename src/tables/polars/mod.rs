use crate::commons::delta;
use crate::configs::{DQFilesystemConfig, DQTableConfig};
use crate::error::DQError;
use crate::metadata::DQMetadataMap;
use crate::state::DQState;
use crate::table::DQTable;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use deltalake::arrow::datatypes::{DataType, FieldRef, Schema};
use deltalake::kernel::{Action, Add, Metadata, Protocol};
use deltalake::logstore::default_logstore::DefaultLogStore;
use deltalake::logstore::LogStoreRef;
use deltalake::ObjectStoreError;
use duckdb::{params, Connection};
use polars::prelude::*;
use serde_json::Value;
use sqlparser::ast::{SetExpr, Statement, TableFactor};
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use tokio::sync::Mutex;
use url::Url;

mod predicates;

const STATS_TABLE_ADD_PATH: &str = "__path__";
const STATS_TABLE_CHUNKS_MAX: usize = 16;

#[allow(dead_code)]
#[derive(Debug)]
struct DQPolarsFile {
    add: Add,

    from: i64,
    to: i64,
}

pub struct DQPolarsTable {
    store: LogStoreRef,

    location: String,
    predicates: Option<Vec<String>>,

    version: i64,
    schema: Schema,
    stats: DataFrame,

    use_versioning: bool,

    protocol: Protocol,
    metadata: Metadata,
    files: HashMap<String, DQPolarsFile>,

    engine: Arc<Mutex<Connection>>,
}

impl DQPolarsTable {
    pub async fn new(
        table_config: &DQTableConfig,
        filesystem_config: Option<&DQFilesystemConfig>,
        _state: &DQState,
    ) -> Self {
        let mut options = HashMap::<String, String>::new();
        if let Some(filesystem_config) = filesystem_config {
            for (k, v) in filesystem_config.configs.iter() {
                options.insert(k.clone(), v.clone());
            }
        }

        let location = table_config.location.trim_end_matches("/").to_string();

        let url: Url = location.parse().unwrap();
        let location = if url.scheme() == "file" {
            url.path().to_string()
        } else {
            location
        };
        let store = Arc::new(DefaultLogStore::try_new(url, options.clone()).unwrap());

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
        setup_duckdb(&engine, &options).unwrap();

        DQPolarsTable {
            store,
            location,
            version: -1,
            predicates: predicates,
            use_versioning: table_config.use_versioning.unwrap_or(false),
            protocol: Protocol::default(),
            metadata: Metadata::default(),
            schema: Schema::empty(),
            files: HashMap::new(),
            stats: DataFrame::default(),
            engine: Arc::new(Mutex::new(engine)),
        }
    }

    fn update_actions(&mut self, actions: &Vec<Action>, version: i64) {
        for action in actions {
            if let Action::Add(add) = action {
                self.files.insert(
                    add.path.clone(),
                    DQPolarsFile {
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
    }

    async fn update_commits(&mut self) {
        let mut actions = Vec::new();

        let mut version = self.version;

        loop {
            match delta::peek_commit(&self.store, version + 1).await {
                Ok(__actions) => {
                    self.update_actions(&__actions, version + 1);

                    actions.extend(__actions);

                    version = version + 1;
                }
                Err(DQError::ObjectStore {
                    source: ObjectStoreError::NotFound { .. },
                }) => {
                    break;
                }
                Err(err) => panic!("could not peek commit: {:?}", err),
            }
        }

        if actions.len() > 0 {
            let stats = get_statistics(&actions, &self.schema, self.predicates.as_ref());
            self.stats = concat(
                [self.stats.clone().lazy(), stats.lazy()],
                UnionArgs {
                    parallel: true,
                    rechunk: if self.stats.n_chunks() >= STATS_TABLE_CHUNKS_MAX {
                        true
                    } else {
                        false
                    },
                    to_supertypes: false,
                },
            )
            .unwrap()
            .collect()
            .unwrap();

            self.version = version;

            log::info!("update={:?} version", self.version);
        }
    }
}

#[async_trait]
impl DQTable for DQPolarsTable {
    async fn update(&mut self) -> Result<(), DQError> {
        if self.version >= 0 {
            self.update_commits().await;
        } else {
            if self.use_versioning {
                self.update_commits().await;
            } else if let Ok(checkpoint) = delta::get_last_checkpoint(&self.store).await {
                let actions = delta::peek_checkpoint(&self.store, &checkpoint).await;
                self.update_actions(&actions, 0);

                self.stats = get_statistics(&actions, &self.schema, self.predicates.as_ref());

                self.version = checkpoint.version;

                log::info!("checkpoint={:?} version", self.version);

                self.update_commits().await;
            } else {
                self.update_commits().await;
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
                    let mut files = Vec::new();

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

                    let stats = if let Some(selection) = &select.selection {
                        let filters = predicates::parse_expression(
                            &selection,
                            &self.stats.get_column_names(),
                            false,
                        );

                        log::info!("filters={:#?}", filters);

                        self.stats.clone().lazy().filter(filters).collect()?
                    } else {
                        self.stats.clone()
                    };

                    unsafe {
                        for chunk in stats[STATS_TABLE_ADD_PATH].chunks() {
                            for index in 0..chunk.len() {
                                if let AnyValue::Utf8(path) = chunk.get_unchecked(index) {
                                    if let Some(DQPolarsFile { add, .. }) = self.files.get(path) {
                                        files.push(format!(
                                            "{}/{}",
                                            self.location,
                                            add.path.clone()
                                        ));
                                    }
                                }
                            }
                        }
                    }

                    log::info!("files={:#?}", files);

                    if files.len() > 0 {
                        let engine = self.engine.lock().await;

                        let mut stmt = engine.prepare(&statement.to_string().replace(
                            &from,
                            &format!(
                                        "read_parquet([{}])",
                                        files
                                            .iter()
                                            .map(|file| format!("'{}'", file))
                                            .collect::<Vec<String>>()
                                            .join(",")
                                    ),
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

fn setup_duckdb(engine: &Connection, options: &HashMap<String, String>) -> Result<(), DQError> {
    engine.execute("PRAGMA enable_object_cache", params![])?;

    if let (Some(s3_access_key_id), Some(s3_secret_access_key)) = (
        options.get("AWS_ACCESS_KEY_ID"),
        options.get("AWS_SECRET_ACCESS_KEY"),
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

        if let Some(s3_endpoint) = options.get("AWS_ENDPOINT_URL") {
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
        if let Some(s3_region) = options.get("AWS_REGION") {
            engine.execute(&format!("SET s3_region='{}'", s3_region), params![])?;
        }
        if let Some(s3_allow_http) = options.get("AWS_ALLOW_HTTP") {
            if s3_allow_http == "true" {
                engine.execute("SET s3_use_ssl=false", params![])?;
            } else {
                engine.execute("SET s3_use_ssl=true", params![])?;
            }
        }
    }

    Ok(())
}

fn get_statistics(
    actions: &Vec<Action>,
    schema: &Schema,
    predicates: Option<&Vec<String>>,
) -> DataFrame {
    let fields = match predicates {
        Some(predicates) => schema
            .fields()
            .iter()
            .filter(|field| predicates.contains(field.name()))
            .collect::<Vec<&FieldRef>>(),
        None => schema.fields().iter().collect::<Vec<&FieldRef>>(),
    };
    let mut columns = HashMap::<String, Vec<Value>>::new();

    let mut paths = Vec::new();

    for action in actions {
        if let Action::Add(add) = action {
            let partitions = &add.partition_values;
            let stats = add.get_stats().unwrap();
            for field in &fields {
                if partitions.contains_key(field.name()) {
                    let value = match partitions.get(field.name()).unwrap() {
                        Some(value) => Value::String(value.to_string()),
                        None => Value::Null,
                    };

                    match columns.get_mut(field.name()) {
                        Some(rows) => {
                            rows.push(value);
                        }
                        None => {
                            let mut rows = Vec::new();
                            rows.push(value);

                            columns.insert(field.name().clone(), rows);
                        }
                    }
                } else if let Some(stats) = stats.as_ref() {
                    if stats.min_values.contains_key(field.name()) {
                        let name = field.name();
                        let value = stats
                            .min_values
                            .get(field.name())
                            .unwrap()
                            .as_value()
                            .unwrap();

                        match columns.get_mut(name) {
                            Some(rows) => {
                                rows.push(value.clone());
                            }
                            None => {
                                let mut rows = Vec::new();
                                rows.push(value.clone());

                                columns.insert(name.clone(), rows);
                            }
                        }
                    }
                    if stats.max_values.contains_key(field.name()) {
                        let name = [field.name(), "max"].join(".");
                        let value = stats
                            .max_values
                            .get(field.name())
                            .unwrap()
                            .as_value()
                            .unwrap();

                        match columns.get_mut(&name) {
                            Some(rows) => {
                                rows.push(value.clone());
                            }
                            None => {
                                let mut rows = Vec::new();
                                rows.push(value.clone());

                                columns.insert(name.clone(), rows);
                            }
                        }
                    }
                }
            }

            paths.push(add.path.clone());
        }
    }

    let mut series = Vec::new();

    for field in &fields {
        let name = field.name();
        if let Some(rows) = columns.get(name) {
            series.push(match field.data_type() {
                DataType::Utf8 => Series::new(
                    name,
                    rows.iter()
                        .map(|value| value.as_str().unwrap())
                        .collect::<Vec<&str>>(),
                ),
                DataType::Int32 => Series::new(
                    name,
                    rows.iter()
                        .map(|value| value.as_i64().unwrap() as i32)
                        .collect::<Vec<i32>>(),
                ),
                DataType::Int64 => Series::new(
                    name,
                    rows.iter()
                        .map(|value| value.as_i64().unwrap())
                        .collect::<Vec<i64>>(),
                ),
                DataType::Date32 => Series::new(
                    name,
                    rows.iter()
                        .map(|value| value.as_str().unwrap())
                        .collect::<Vec<&str>>(),
                ),
                _ => unimplemented!(),
            })
        }
    }

    for field in &fields {
        let name = [field.name(), "max"].join(".");
        if let Some(rows) = columns.get(&name) {
            series.push(match field.data_type() {
                DataType::Utf8 => Series::new(
                    &name,
                    rows.iter()
                        .map(|value| value.as_str().unwrap())
                        .collect::<Vec<&str>>(),
                ),
                DataType::Int32 => Series::new(
                    &name,
                    rows.iter()
                        .map(|value| value.as_i64().unwrap() as i32)
                        .collect::<Vec<i32>>(),
                ),
                DataType::Int64 => Series::new(
                    &name,
                    rows.iter()
                        .map(|value| value.as_i64().unwrap())
                        .collect::<Vec<i64>>(),
                ),
                DataType::Date32 => Series::new(
                    &name,
                    rows.iter()
                        .map(|value| value.as_str().unwrap())
                        .collect::<Vec<&str>>(),
                ),
                _ => unimplemented!(),
            })
        }
    }

    series.push(Series::new(STATS_TABLE_ADD_PATH, paths));

    DataFrame::new(series).unwrap()
}
