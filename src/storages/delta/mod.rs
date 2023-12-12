use crate::commons::delta;
use crate::configs::{DQFilesystemConfig, DQStorageConfig, DQTableConfig};
use crate::error::DQError;
use crate::storage::{DQStorage, DQStorageFactory};
use arrow::array::cast::AsArray;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use async_trait::async_trait;
use deltalake::datafusion::common::cast::as_string_array;
use deltalake::datafusion::common::ToDFSchema;
use deltalake::datafusion::physical_expr::create_physical_expr;
use deltalake::datafusion::physical_expr::execution_props::ExecutionProps;
use deltalake::kernel::{Action, Add, Metadata, Protocol};
use deltalake::logstore::LogStoreRef;
use deltalake::storage::config::configure_log_store;
use deltalake::ObjectStoreError;
use sqlparser::ast::{SetExpr, Statement};
use std::collections::HashMap;
use url::Url;

mod predicates;
mod statistics;

#[allow(dead_code)]
#[derive(Debug)]
struct DQDeltaFile {
    add: Add,

    from: i64,
    to: i64,
}

pub struct DQDeltaStorage {
    store: LogStoreRef,

    location: String,
    predicates: Option<Vec<String>>,

    version: i64,
    schema: Schema,

    use_versioning: bool,

    protocol: Protocol,
    metadata: Metadata,
    files: HashMap<String, DQDeltaFile>,

    stats: Vec<RecordBatch>,
    max_stats_batches: usize,
}

impl DQDeltaStorage {
    pub async fn new(
        table_config: &DQTableConfig,
        storage_config: Option<&DQStorageConfig>,
        filesystem_config: Option<&DQFilesystemConfig>,
    ) -> Self {
        let storage_options =
            storage_config.map_or(HashMap::new(), |config| config.options.clone());
        let filesystem_options =
            filesystem_config.map_or(HashMap::new(), |config| config.options.clone());

        let location = table_config.location.trim_end_matches("/").to_string();

        let url: Url = location.parse().unwrap();
        let location = if url.scheme() == "file" {
            url.path().to_string()
        } else {
            location
        };
        let store = configure_log_store(url.as_str(), filesystem_options, None).unwrap();

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

        DQDeltaStorage {
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
            max_stats_batches: storage_options
                .get("max_stats_batches")
                .map_or(32, |v| v.parse().unwrap()),
        }
    }

    pub fn with_store(mut self, store: LogStoreRef) -> Self {
        self.store = store;
        self
    }

    fn update_actions(&mut self, actions: &Vec<Action>, version: i64) -> Result<(), DQError> {
        for action in actions {
            if let Action::Add(add) = action {
                self.files.insert(
                    add.path.clone(),
                    DQDeltaFile {
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

    fn update_stats(&mut self, actions: &Vec<Action>) -> Result<(), DQError> {
        let batch = statistics::get_record_batch_from_actions(
            &actions,
            &self.schema,
            self.predicates.as_ref(),
        )?;
        self.stats.push(batch);

        if self.stats.len() > self.max_stats_batches {
            self.stats = vec![arrow::compute::concat_batches(
                &self.stats.first().unwrap().schema(),
                &self.stats,
            )?];
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
            self.update_stats(&actions)?;

            self.version = version;

            log::info!("update={:?} version", self.version);
        }

        Ok(())
    }
}

#[async_trait]
impl DQStorage for DQDeltaStorage {
    async fn update(&mut self) -> Result<(), DQError> {
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

    async fn execute(&mut self, statement: &Statement) -> Result<Vec<String>, DQError> {
        let mut files: Vec<String> = Vec::new();

        match statement {
            Statement::Query(query) => match query.body.as_ref() {
                SetExpr::Select(select) => {
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
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }

        Ok(files)
    }
}

pub struct DQDeltaStorageFactory {}

impl DQDeltaStorageFactory {
    pub fn new() -> Self {
        DQDeltaStorageFactory {}
    }
}

#[async_trait]
impl DQStorageFactory for DQDeltaStorageFactory {
    async fn create(
        &self,
        table_config: &DQTableConfig,
        storage_config: Option<&DQStorageConfig>,
        filesystem_config: Option<&DQFilesystemConfig>,
    ) -> Box<dyn DQStorage> {
        Box::new(DQDeltaStorage::new(table_config, storage_config, filesystem_config).await)
    }
}

#[cfg(test)]
mod tests {
    use crate::commons::tests;
    use crate::configs::DQTableConfig;
    use crate::storage::DQStorage;
    use crate::storages::delta::DQDeltaStorage;
    use deltalake::logstore::default_logstore::DefaultLogStore;
    use deltalake::logstore::{LogStore, LogStoreConfig};
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
        let log_store = DefaultLogStore::new(
            store.clone(),
            LogStoreConfig {
                location: Url::parse("mem://test0").unwrap(),
                options: HashMap::new().into(),
            },
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
            storage: "delta".into(),
            compute: "duckdb".into(),
            filesystem: None,
            location: "memory://".into(),
            predicates: None,
            use_versioning: None,
        };

        let mut storage = DQDeltaStorage::new(&table_config, None, None).await;
        storage = storage.with_store(Arc::new(log_store));
        storage.update().await.unwrap();

        let dialect = GenericDialect {};

        let statements = Parser::parse_sql(&dialect, "select * from test0").unwrap();
        let files = storage.execute(statements.first().unwrap()).await.unwrap();
        assert_eq!(files.len(), 2);

        let statements =
            Parser::parse_sql(&dialect, "select * from test0 where value >= 0").unwrap();
        let files = storage.execute(statements.first().unwrap()).await.unwrap();
        assert_eq!(files.len(), 2);
    }
}
