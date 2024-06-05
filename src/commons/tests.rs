use deltalake::kernel::{
    Action, Add, DataType, Metadata, PrimitiveType, Protocol, Remove, StructField, StructType,
};
use deltalake::operations::transaction::{TransactionError, PROTOCOL};
use std::collections::HashMap;

pub fn create_add_action(
    path: impl Into<String>,
    data_change: bool,
    stats: Option<String>,
) -> Action {
    Action::Add(Add {
        path: path.into(),
        size: 100,
        data_change,
        stats,
        modification_time: -1,
        partition_values: Default::default(),
        stats_parsed: None,
        base_row_id: None,
        default_row_commit_version: None,
        tags: None,
        deletion_vector: None,
        clustering_provider: None,
    })
}

pub fn create_remove_action(path: impl Into<String>, data_change: bool) -> Action {
    Action::Remove(Remove {
        path: path.into(),
        data_change,
        size: None,
        deletion_timestamp: None,
        deletion_vector: None,
        partition_values: Default::default(),
        extended_file_metadata: None,
        base_row_id: None,
        default_row_commit_version: None,
        tags: None,
    })
}

pub fn create_protocol_action(max_reader: Option<i32>, max_writer: Option<i32>) -> Action {
    let protocol = Protocol {
        min_reader_version: max_reader.unwrap_or(PROTOCOL.default_reader_version()),
        min_writer_version: max_writer.unwrap_or(PROTOCOL.default_writer_version()),
        writer_features: None,
        reader_features: None,
    };
    Action::Protocol(protocol)
}

pub fn create_metadata_action(
    columns: Vec<String>,
    parttiton_columns: Option<Vec<String>>,
    configuration: Option<HashMap<String, Option<String>>>,
) -> Action {
    let table_schema = StructType::new(
        columns
            .iter()
            .map(|column| StructField::new(column, DataType::Primitive(PrimitiveType::Long), true))
            .collect::<Vec<_>>(),
    );
    Action::Metadata(
        Metadata::try_new(
            table_schema,
            parttiton_columns.unwrap_or_default(),
            configuration.unwrap_or_default(),
        )
        .unwrap(),
    )
}

fn log_entry_from_actions<'a>(
    actions: impl IntoIterator<Item = &'a Action>,
) -> Result<String, TransactionError> {
    let mut jsons = Vec::<String>::new();
    for action in actions {
        let json = serde_json::to_string(action)
            .map_err(|e| TransactionError::SerializeLogJson { json_err: e })?;
        jsons.push(json);
    }
    Ok(jsons.join("\n"))
}

pub fn get_commit_bytes(actions: &Vec<Action>) -> Result<bytes::Bytes, TransactionError> {
    Ok(bytes::Bytes::from(log_entry_from_actions(actions)?))
}
