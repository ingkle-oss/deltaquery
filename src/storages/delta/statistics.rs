use crate::error::DQError;
use arrow::array::RecordBatch;
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use deltalake::datafusion::common::scalar::ScalarValue;
use deltalake::datafusion::common::DataFusionError;
use deltalake::kernel::Action;
use deltalake::DeltaTableError;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

pub const STATS_TABLE_ADD_PATH: &str = "__path__";

fn get_scalar_value_for_null(datatype: &DataType) -> Result<ScalarValue, DeltaTableError> {
    match datatype {
        DataType::Null => Ok(ScalarValue::Null),
        DataType::Boolean => Ok(ScalarValue::Boolean(None)),
        DataType::Int8 => Ok(ScalarValue::Int8(None)),
        DataType::Int16 => Ok(ScalarValue::Int16(None)),
        DataType::Int32 => Ok(ScalarValue::Int32(None)),
        DataType::Int64 => Ok(ScalarValue::Int64(None)),
        DataType::UInt8 => Ok(ScalarValue::UInt8(None)),
        DataType::UInt16 => Ok(ScalarValue::UInt16(None)),
        DataType::UInt32 => Ok(ScalarValue::UInt32(None)),
        DataType::UInt64 => Ok(ScalarValue::UInt64(None)),
        DataType::Float32 => Ok(ScalarValue::Float32(None)),
        DataType::Float64 => Ok(ScalarValue::Float64(None)),
        DataType::Date32 => Ok(ScalarValue::Date32(None)),
        DataType::Date64 => Ok(ScalarValue::Date64(None)),
        DataType::Binary => Ok(ScalarValue::Binary(None)),
        DataType::FixedSizeBinary(size) => Ok(ScalarValue::FixedSizeBinary(size.to_owned(), None)),
        DataType::LargeBinary => Ok(ScalarValue::LargeBinary(None)),
        DataType::Utf8 => Ok(ScalarValue::Utf8(None)),
        DataType::LargeUtf8 => Ok(ScalarValue::LargeUtf8(None)),
        DataType::Decimal128(precision, scale) => Ok(ScalarValue::Decimal128(
            None,
            precision.to_owned(),
            scale.to_owned(),
        )),
        DataType::Timestamp(unit, tz) => {
            let tz = tz.to_owned();
            Ok(match unit {
                TimeUnit::Second => ScalarValue::TimestampSecond(None, tz),
                TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(None, tz),
                TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(None, tz),
                TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(None, tz),
            })
        }
        DataType::Dictionary(k, v) => Ok(ScalarValue::Dictionary(
            k.clone(),
            Box::new(get_scalar_value_for_null(v).unwrap()),
        )),
        DataType::Float16
        | DataType::Decimal256(_, _)
        | DataType::Union(_, _)
        | DataType::LargeList(_)
        | DataType::Struct(_)
        | DataType::List(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::RunEndEncoded(_, _)
        | DataType::Map(_, _) => Err(DeltaTableError::Generic(format!(
            "unsupported data type for deltalake {}",
            datatype
        ))),
    }
}

fn get_scalar_value_for_timestamp(
    value: &serde_json::Value,
    datatype: &DataType,
) -> Result<ScalarValue, DataFusionError> {
    let string = match value {
        serde_json::Value::String(s) => s.to_owned(),
        _ => value.to_string(),
    };

    let timestamp =
        ScalarValue::try_from_string(string, &DataType::Timestamp(TimeUnit::Microsecond, None))?;
    let cast_array = cast_with_options(
        &timestamp.to_array()?,
        datatype,
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )?;

    ScalarValue::try_from_array(&cast_array, 0)
}

fn get_scalar_value(
    value: &serde_json::Value,
    datatype: &DataType,
) -> Result<Option<ScalarValue>, DQError> {
    match value {
        serde_json::Value::Array(_) => Ok(None),
        serde_json::Value::Object(_) => Ok(None),
        serde_json::Value::Null => Ok(Some(get_scalar_value_for_null(datatype)?)),
        serde_json::Value::String(string_val) => match datatype {
            DataType::Timestamp(_, _) => Ok(Some(get_scalar_value_for_timestamp(value, datatype)?)),
            _ => Ok(Some(ScalarValue::try_from_string(
                string_val.to_owned(),
                datatype,
            )?)),
        },
        other => match datatype {
            DataType::Timestamp(_, _) => Ok(Some(get_scalar_value_for_timestamp(value, datatype)?)),
            _ => Ok(Some(ScalarValue::try_from_string(
                other.to_string(),
                datatype,
            )?)),
        },
    }
}

pub fn get_record_batch_from_actions(
    actions: &Vec<Action>,
    schema: &SchemaRef,
    predicates: Option<&Vec<String>>,
) -> Result<RecordBatch, DQError> {
    let fields = match predicates {
        Some(predicates) => schema
            .fields()
            .iter()
            .filter(|field| predicates.contains(field.name()))
            .collect::<Vec<&FieldRef>>(),
        None => schema.fields().iter().collect::<Vec<&FieldRef>>(),
    };
    let mut columns = HashMap::<String, Vec<ScalarValue>>::new();

    for action in actions {
        if let Action::Add(add) = action {
            let partitions = &add.partition_values;
            let stats = add.get_stats().unwrap();
            for field in &fields {
                let data_type = field.data_type();

                if partitions.contains_key(field.name()) {
                    let value = match partitions.get(field.name()).unwrap() {
                        Some(value) => Value::String(value.to_string()),
                        None => Value::Null,
                    };

                    let value = get_scalar_value(&value, data_type)
                        .ok()
                        .flatten()
                        .unwrap_or(
                            get_scalar_value_for_null(data_type)
                                .expect("could not determine null type"),
                        );

                    match columns.get_mut(field.name()) {
                        Some(values) => {
                            values.push(value);
                        }
                        None => {
                            let mut values = Vec::new();
                            values.push(value);

                            columns.insert(field.name().clone(), values);
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

                        let value = get_scalar_value(&value, data_type)
                            .ok()
                            .flatten()
                            .unwrap_or(
                                get_scalar_value_for_null(data_type)
                                    .expect("could not determine null type"),
                            );

                        match columns.get_mut(name) {
                            Some(values) => {
                                values.push(value.clone());
                            }
                            None => {
                                let mut values = Vec::new();
                                values.push(value.clone());

                                columns.insert(name.clone(), values);
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

                        let value = get_scalar_value(&value, data_type)
                            .ok()
                            .flatten()
                            .unwrap_or(
                                get_scalar_value_for_null(data_type)
                                    .expect("could not determine null type"),
                            );

                        match columns.get_mut(&name) {
                            Some(values) => {
                                values.push(value.clone());
                            }
                            None => {
                                let mut values = Vec::new();
                                values.push(value.clone());

                                columns.insert(name.clone(), values);
                            }
                        }
                    }
                }
            }

            match columns.get_mut(STATS_TABLE_ADD_PATH) {
                Some(values) => {
                    values.push(ScalarValue::Utf8(Some(add.path.to_string())));
                }
                None => {
                    let mut values = Vec::new();
                    values.push(ScalarValue::Utf8(Some(add.path.to_string())));

                    columns.insert(STATS_TABLE_ADD_PATH.to_string(), values);
                }
            }
        }
    }

    let mut fields = fields
        .into_iter()
        .map(|field| field.clone())
        .collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        STATS_TABLE_ADD_PATH,
        DataType::Utf8,
        false,
    )));

    if columns.is_empty() {
        Ok(RecordBatch::new_empty(Arc::new(Schema::new(fields))))
    } else {
        let mut arrays = Vec::new();
        for field in fields.iter() {
            if let Some(values) = columns.remove(field.name()) {
                if let Some(array) = ScalarValue::iter_to_array(values).ok() {
                    arrays.push(array);
                }
            }
        }

        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
    }
}
