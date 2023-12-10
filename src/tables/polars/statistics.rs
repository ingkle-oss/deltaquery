use deltalake::arrow::datatypes::{DataType, FieldRef, Schema};
use deltalake::kernel::Action;
use polars::prelude::*;
use serde_json::Value;
use std::collections::HashMap;

pub const STATS_TABLE_ADD_PATH: &str = "__path__";
pub const STATS_TABLE_CHUNKS_MAX: usize = 16;

pub fn get_dataframe_from_actions(
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
