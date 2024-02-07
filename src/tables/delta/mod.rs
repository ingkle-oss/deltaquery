use crate::commons::delta;
use crate::configs::{DQFilesystemConfig, DQStorageConfig, DQTableConfig};
use crate::table::{DQTable, DQTableFactory};
use anyhow::Error;
use arrow::array::cast::AsArray;
use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use chrono::Duration;
use datafusion::common::cast::as_string_array;
use datafusion::common::ToDFSchema;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;
use deltalake::kernel::{Action, Add, Metadata, Protocol};
use deltalake::logstore::logstore_for;
use deltalake::logstore::LogStoreRef;
use deltalake::table::builder::ensure_table_uri;
use deltalake::ObjectStoreError;
use sqlparser::ast::{SetExpr, Statement};
use std::collections::HashMap;
use std::sync::Arc;

mod predicates;
mod statistics;

#[allow(dead_code)]
#[derive(Debug)]
struct DQDeltaFile {
    add: Add,

    from: i64,
    to: i64,

    rank: usize,
}

pub struct DQDeltaTable {
    store: LogStoreRef,

    location: String,

    version: i64,
    schema: Option<SchemaRef>,
    partitions: Option<Vec<String>>,

    protocol: Protocol,
    metadata: Metadata,
    files: HashMap<String, DQDeltaFile>,

    stats: Vec<RecordBatch>,
    max_stats_batches: usize,
    use_versioning: bool,

    timestamp_field: Option<String>,
    timestamp_template: String,
    timestamp_duration: Duration,

    filesystem_options: HashMap<String, String>,
}

impl DQDeltaTable {
    pub async fn try_new(
        table_config: &DQTableConfig,
        storage_config: Option<&DQStorageConfig>,
        filesystem_config: Option<&DQFilesystemConfig>,
    ) -> Result<Self, Error> {
        let storage_options =
            storage_config.map_or(HashMap::new(), |config| config.options.clone());
        let filesystem_options =
            filesystem_config.map_or(HashMap::new(), |config| config.options.clone());

        let location = match &table_config.location {
            Some(location) => location.trim_end_matches("/").to_string(),
            None => "memory://".into(),
        };

        let store = logstore_for(ensure_table_uri(&location)?, filesystem_options.clone())?;

        Ok(DQDeltaTable {
            store,
            location,
            version: -1,
            protocol: Protocol::default(),
            metadata: Metadata::default(),
            schema: None,
            partitions: None,
            files: HashMap::new(),
            stats: Vec::new(),
            use_versioning: storage_options.get("use_versioning").map_or(false, |v| {
                v.parse().expect("could not parse use_versioning")
            }),
            max_stats_batches: storage_options.get("max_stats_batches").map_or(32, |v| {
                v.parse().expect("could not parse max_stats_batches")
            }),
            timestamp_field: storage_options.get("timestamp_field").cloned(),
            timestamp_template: storage_options
                .get("timestamp_template")
                .map_or(String::from("{{ date }} {{ hour }}:00:00 +00:00"), |v| {
                    v.clone()
                }),
            timestamp_duration: storage_options
                .get("timestamp_duration")
                .map_or(Duration::hours(1), |v| {
                    duration_str::parse_chrono(v).expect("could not parse timestamp_duration")
                }),
            filesystem_options,
        })
    }

    pub fn with_store(mut self, store: LogStoreRef) -> Self {
        self.store = store;
        self
    }

    fn update_actions(&mut self, actions: &Vec<Action>, version: i64) -> Result<(), Error> {
        for action in actions {
            if let Action::Add(add) = action {
                self.files.insert(
                    add.path.clone(),
                    DQDeltaFile {
                        add: add.clone(),
                        from: version,
                        to: i64::MAX,
                        rank: self.files.len(),
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
                self.schema = Some(Arc::new(Schema::try_from(&metadata.schema()?)?));
                self.partitions = Some(metadata.partition_columns.clone());
            }
        }

        Ok(())
    }

    fn update_stats(&mut self, actions: &Vec<Action>) -> Result<(), Error> {
        if let (Some(schema), Some(partitions)) = (&self.schema, &self.partitions) {
            let batch = statistics::get_record_batch_from_actions(
                &actions,
                schema,
                partitions,
                self.timestamp_field.as_ref(),
                &self.timestamp_template,
                &self.timestamp_duration,
            )?;
            self.stats.push(batch);

            if self.stats.len() > self.max_stats_batches {
                self.stats = vec![arrow::compute::concat_batches(
                    &self.stats.first().unwrap().schema(),
                    &self.stats,
                )?];
            }
        }

        Ok(())
    }

    async fn update_commits(&mut self) -> Result<(), Error> {
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
            self.update_stats(&actions)?;

            self.version = version;

            log::info!("update={:?} version", self.version);
        }

        Ok(())
    }
}

#[async_trait]
impl DQTable for DQDeltaTable {
    async fn update(&mut self) -> Result<(), Error> {
        if self.version >= 0 {
            self.update_commits().await?;
        } else {
            if self.use_versioning {
                self.update_commits().await?;
            } else if let Ok(checkpoint) = delta::get_last_checkpoint(&self.store).await {
                let actions = delta::peek_checkpoint(&self.store, &checkpoint).await;
                self.update_actions(&actions, 0)?;
                self.update_stats(&actions)?;

                self.version = checkpoint.version;

                log::info!("checkpoint={:?} version", self.version);

                self.update_commits().await?;
            } else {
                self.update_commits().await?;
            }
        }

        Ok(())
    }

    async fn select(&mut self, statement: &Statement) -> Result<Vec<String>, Error> {
        let mut files: Vec<&DQDeltaFile> = Vec::new();

        match statement {
            Statement::Query(query) => {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    let mut has_filters = false;

                    if let (Some(selection), Some(batch0)) = (&select.selection, self.stats.first())
                    {
                        if let Some(expressions) = predicates::parse_expression(
                            &selection,
                            &batch0.schema().fields(),
                            false,
                            None,
                        ) {
                            log::info!("filters={:#?}", expressions);

                            let schema = batch0.schema();
                            let predicates = create_physical_expr(
                                &expressions,
                                &schema.clone().to_dfschema()?,
                                &schema,
                                &ExecutionProps::new(),
                            )?;

                            for batch in self.stats.iter() {
                                match predicates.evaluate(&batch) {
                                    Ok(results) => {
                                        let results = results.into_array(batch.num_rows())?;
                                        let paths = as_string_array(
                                            batch
                                                .column_by_name(statistics::STATS_TABLE_ADD_PATH)
                                                .expect("could not get path column"),
                                        )?;
                                        for (result, path) in results.as_boolean().iter().zip(paths)
                                        {
                                            if let (Some(result), Some(path)) = (result, path) {
                                                if result {
                                                    if let Some(file) = self.files.get(path) {
                                                        files.push(file);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        log::error!("could not evaluate predicates: {:?}", err);
                                    }
                                }
                            }

                            has_filters = true;
                        }
                    }

                    if !has_filters {
                        files.extend(self.files.values());
                    }
                }
            }
            _ => unreachable!(),
        }

        files.sort_by(|a, b| a.rank.cmp(&b.rank));

        Ok(files
            .iter()
            .map(|file| format!("{}/{}", self.location, file.add.path))
            .collect())
    }

    async fn insert(&mut self, _statement: &Statement) -> Result<(), Error> {
        Ok(())
    }

    fn schema(&self) -> Option<SchemaRef> {
        self.schema.clone()
    }

    fn partition_columns(&self) -> Option<&Vec<String>> {
        self.partitions.as_ref()
    }

    fn location(&self) -> &String {
        &self.location
    }

    fn filesystem_options(&self) -> &HashMap<String, String> {
        &self.filesystem_options
    }
}

pub struct DQDeltaTableFactory {}

impl DQDeltaTableFactory {
    pub fn new() -> Self {
        deltalake::aws::register_handlers(None);

        DQDeltaTableFactory {}
    }
}

#[async_trait]
impl DQTableFactory for DQDeltaTableFactory {
    async fn create(
        &self,
        table_config: &DQTableConfig,
        storage_config: Option<&DQStorageConfig>,
        filesystem_config: Option<&DQFilesystemConfig>,
    ) -> Result<Box<dyn DQTable>, Error> {
        Ok(Box::new(
            DQDeltaTable::try_new(table_config, storage_config, filesystem_config).await?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::commons::tests;
    use crate::configs::DQTableConfig;
    use crate::table::DQTable;
    use crate::tables::delta::DQDeltaTable;
    use deltalake::logstore::default_logstore;
    use deltalake::ObjectStore;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::{collections::HashMap, sync::Arc};
    use url::Url;

    #[tokio::test]
    async fn test_predicates_pushdown() {
        let store = Arc::new(InMemory::new());
        let log_store = default_logstore(
            store.clone(),
            &Url::parse("mem://test0").unwrap(),
            &HashMap::new().into(),
        );

        let tmp_path = Path::from("_delta_log/tmp");

        let protocol = tests::create_protocol_action(None, None);
        let metadata = tests::create_metadata_action(None, None);
        let commit0 = tests::get_commit_bytes(&vec![protocol, metadata]).unwrap();
        store.put(&tmp_path, commit0).await.unwrap();
        log_store.write_commit_entry(0, &tmp_path).await.unwrap();

        let add0 = tests::create_add_action("file0", true, Some("{\"numRecords\":10,\"minValues\":{\"value\":1},\"maxValues\":{\"value\":10},\"nullCount\":{\"value\":0}}".into()));
        let add1 = tests::create_add_action("file1", true, Some("{\"numRecords\":10,\"minValues\":{\"value\":1},\"maxValues\":{\"value\":100},\"nullCount\":{\"value\":0}}".into()));
        let add2 = tests::create_add_action("file2", true, Some("{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":3},\"nullCount\":{\"value\":0}}".into()));
        let commit1 = tests::get_commit_bytes(&vec![add0, add1, add2]).unwrap();
        store.put(&tmp_path, commit1).await.unwrap();
        log_store.write_commit_entry(1, &tmp_path).await.unwrap();

        let remove1 = tests::create_remove_action("file1", true);
        let commit2 = tests::get_commit_bytes(&vec![remove1]).unwrap();
        store.put(&tmp_path, commit2).await.unwrap();
        log_store.write_commit_entry(2, &tmp_path).await.unwrap();

        let table_config = DQTableConfig {
            name: "test0".into(),
            storage: None,
            filesystem: None,
            location: None,
        };

        let mut storage = DQDeltaTable::try_new(&table_config, None, None)
            .await
            .unwrap();
        storage = storage.with_store(log_store.clone());
        storage.update().await.unwrap();

        let dialect = GenericDialect {};

        let statements = Parser::parse_sql(&dialect, "select * from test0").unwrap();
        let files = storage.select(statements.first().unwrap()).await.unwrap();
        assert_eq!(files.len(), 2);

        let statements =
            Parser::parse_sql(&dialect, "select * from test0 where value >= 0").unwrap();
        let files = storage.select(statements.first().unwrap()).await.unwrap();
        assert_eq!(files.len(), 2);
    }

    #[tokio::test]
    async fn test_remove_action_stats() {
        let store = Arc::new(InMemory::new());
        let log_store = default_logstore(
            store.clone(),
            &Url::parse("mem://test0").unwrap(),
            &HashMap::new().into(),
        );

        let tmp_path = Path::from("_delta_log/tmp");

        let protocol = tests::create_protocol_action(None, None);
        let metadata = tests::create_metadata_action(None, None);
        let commit0 = tests::get_commit_bytes(&vec![protocol, metadata]).unwrap();
        store.put(&tmp_path, commit0).await.unwrap();
        log_store.write_commit_entry(0, &tmp_path).await.unwrap();

        let table_config = DQTableConfig {
            name: "test0".into(),
            storage: None,
            filesystem: None,
            location: None,
        };
        let mut storage = DQDeltaTable::try_new(&table_config, None, None)
            .await
            .unwrap();
        storage = storage.with_store(log_store.clone());

        let add0 = tests::create_add_action("file0", true, Some("{\"numRecords\":10,\"minValues\":{\"value\":1},\"maxValues\":{\"value\":10},\"nullCount\":{\"value\":0}}".into()));
        let add1 = tests::create_add_action("file1", true, Some("{\"numRecords\":10,\"minValues\":{\"value\":1},\"maxValues\":{\"value\":100},\"nullCount\":{\"value\":0}}".into()));
        let add2 = tests::create_add_action("file2", true, Some("{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":3},\"nullCount\":{\"value\":0}}".into()));
        let commit1 = tests::get_commit_bytes(&vec![add0, add1, add2]).unwrap();
        store.put(&tmp_path, commit1).await.unwrap();
        log_store.write_commit_entry(1, &tmp_path).await.unwrap();

        storage.update().await.unwrap();

        let remove1 = tests::create_remove_action("file1", true);
        let commit2 = tests::get_commit_bytes(&vec![remove1]).unwrap();
        store.put(&tmp_path, commit2).await.unwrap();
        log_store.write_commit_entry(2, &tmp_path).await.unwrap();

        storage.update().await.unwrap();

        let dialect = GenericDialect {};

        let statements =
            Parser::parse_sql(&dialect, "select * from test0 where value >= 0").unwrap();
        let files = storage.select(statements.first().unwrap()).await.unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(
            storage
                .stats
                .iter()
                .map(|batch| batch.num_rows())
                .reduce(|a, b| a + b)
                .unwrap_or(0),
            2
        );
    }
}
