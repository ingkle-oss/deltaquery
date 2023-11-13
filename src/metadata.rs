use std::collections::HashMap;

pub type DQMetadataMap = HashMap<DQMetadataKey, DQMetadataValue>;

#[derive(Debug, Clone)]
pub enum DQMetadataValue {
    String(String),
    Int32(i32),
    Int64(i64),
    Uint32(u32),
    Uint64(u64),
    StringArray(Vec<String>),
    Int32Array(Vec<i32>),
    Int64Array(Vec<i64>),
    Uint32Array(Vec<u32>),
    Uint64Array(Vec<u64>),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum DQMetadataKey {}
