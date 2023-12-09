use deltalake::kernel::Action;
use deltalake::logstore::LogStoreRef;
use deltalake::parquet::file::reader::FileReader;
use deltalake::parquet::file::reader::SerializedFileReader;
use deltalake::ObjectStoreError;
use deltalake::Path;
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, Cursor};

#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy)]
pub struct DQDeltaCheckpoint {
    pub version: i64,
    pub size: i64,
    pub parts: Option<u32>,
    pub size_in_bytes: Option<i64>,
    pub num_of_add_files: Option<i64>,
}

pub async fn get_last_checkpoint(
    store: &LogStoreRef,
) -> Result<DQDeltaCheckpoint, ObjectStoreError> {
    let last_checkpoint_path = Path::from_iter(["_delta_log", "_last_checkpoint"]);
    let data = store.object_store().get(&last_checkpoint_path).await?;
    Ok(serde_json::from_slice(&data.bytes().await.unwrap()).unwrap())
}

fn get_checkpoint_paths(store: &LogStoreRef, checkpoint: &DQDeltaCheckpoint) -> Vec<Path> {
    let checkpoint_prefix = format!("{:020}", checkpoint.version);
    let log_path = store.log_path();
    let mut checkpoint_data_paths = Vec::new();

    match checkpoint.parts {
        None => {
            let path = log_path.child(&*format!("{checkpoint_prefix}.checkpoint.parquet"));
            checkpoint_data_paths.push(path);
        }
        Some(parts) => {
            for i in 0..parts {
                let path = log_path.child(&*format!(
                    "{}.checkpoint.{:010}.{:010}.parquet",
                    checkpoint_prefix,
                    i + 1,
                    parts
                ));
                checkpoint_data_paths.push(path);
            }
        }
    }

    checkpoint_data_paths
}

pub async fn peek_checkpoint(store: &LogStoreRef, checkpoint: &DQDeltaCheckpoint) -> Vec<Action> {
    let mut actions = Vec::new();

    let checkpoint_paths = get_checkpoint_paths(&store, &checkpoint);

    for path in checkpoint_paths.iter() {
        let data = store
            .object_store()
            .get(path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        let preader = SerializedFileReader::new(data).unwrap();
        let schema = preader.metadata().file_metadata().schema();
        if !schema.is_group() {
            unimplemented!();
        }
        for record in preader.get_row_iter(None).unwrap() {
            match record {
                Ok(row) => {
                    let action = Action::from_parquet_record(schema, &row).unwrap();

                    actions.push(action);
                }
                Err(_) => unimplemented!(),
            }
        }
    }

    actions
}

fn get_commit_path(version: i64) -> Path {
    let log_path = Path::from("_delta_log");
    let version = format!("{version:020}.json");
    log_path.child(version.as_str())
}

pub async fn peek_commit(
    store: &LogStoreRef,
    version: i64,
) -> Result<Vec<Action>, ObjectStoreError> {
    let commit_path = get_commit_path(version);
    let data = store.object_store().get(&commit_path).await?;
    let reader = BufReader::new(Cursor::new(data.bytes().await.unwrap()));

    let mut actions = Vec::new();

    for line in reader.lines() {
        let action: Action = serde_json::from_str(line.unwrap().as_str()).unwrap();
        actions.push(action);
    }

    Ok(actions)
}
