use anyhow::Error;
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, ListArray, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::scalar::ScalarValue;
use datafusion::common::DataFusionError;
use deltalake::kernel::Action;
use deltalake::protocol::Stats;
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
        | DataType::BinaryView
        | DataType::Utf8View
        | DataType::LargeListView(_)
        | DataType::ListView(_)
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
) -> Result<Option<ScalarValue>, Error> {
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
    partitions: &Vec<String>,
) -> Result<Option<RecordBatch>, Error> {
    let mut columns = HashMap::<String, Vec<ScalarValue>>::new();
    let mut fields = Vec::new();

    for field in schema.fields().iter() {
        fields.push(Arc::new(Field::new(
            field.name(),
            field.data_type().clone(),
            true,
        )));

        if !partitions.contains(field.name()) {
            fields.push(Arc::new(Field::new(
                &[field.name(), "max"].join("."),
                field.data_type().clone(),
                true,
            )));
        }
    }

    fields.push(Arc::new(Field::new(
        STATS_TABLE_ADD_PATH,
        DataType::Utf8,
        true,
    )));

    let mut rows = 0;

    for action in actions {
        if let Action::Add(add) = action {
            let parts = &add.partition_values;
            let stats: Option<Stats> = match &add.stats {
                Some(stats) => match serde_json::from_str(stats) {
                    Ok(stats) => Some(stats),
                    Err(_) => None,
                },
                None => None,
            };
            for field in schema.fields().iter() {
                let data_type = field.data_type();

                if partitions.contains(field.name()) {
                    let name = field.name().clone();

                    let value = match parts.get(field.name()).unwrap() {
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

                    match columns.get_mut(&name) {
                        Some(values) => {
                            values.push(value);
                        }
                        None => {
                            let mut values = Vec::new();
                            values.push(value);

                            columns.insert(name, values);
                        }
                    }
                } else if let Some(stats) = stats.as_ref() {
                    let name_min = field.name().clone();
                    let name_max = [field.name(), "max"].join(".");

                    let value_min = if stats.min_values.contains_key(field.name()) {
                        stats
                            .min_values
                            .get(field.name())
                            .unwrap()
                            .as_value()
                            .unwrap()
                            .clone()
                    } else {
                        Value::Null
                    };
                    let value_max = if stats.max_values.contains_key(field.name()) {
                        stats
                            .max_values
                            .get(field.name())
                            .unwrap()
                            .as_value()
                            .unwrap()
                            .clone()
                    } else {
                        Value::Null
                    };

                    let value_min = get_scalar_value(&value_min, data_type)
                        .ok()
                        .flatten()
                        .unwrap_or(
                            get_scalar_value_for_null(data_type)
                                .expect("could not determine null type"),
                        );

                    match columns.get_mut(&name_min) {
                        Some(values) => {
                            values.push(value_min);
                        }
                        None => {
                            let mut values = Vec::new();
                            values.push(value_min);

                            columns.insert(name_min, values);
                        }
                    }

                    let value_max = get_scalar_value(&value_max, data_type)
                        .ok()
                        .flatten()
                        .unwrap_or(
                            get_scalar_value_for_null(data_type)
                                .expect("could not determine null type"),
                        );

                    match columns.get_mut(&name_max) {
                        Some(values) => {
                            values.push(value_max);
                        }
                        None => {
                            let mut values = Vec::new();
                            values.push(value_max);

                            columns.insert(name_max, values);
                        }
                    }
                };
            }

            match columns.get_mut(STATS_TABLE_ADD_PATH) {
                Some(values) => {
                    values.push(ScalarValue::Utf8(Some(add.path.to_string())));
                }
                None => {
                    let mut values = Vec::new();
                    values.push(ScalarValue::Utf8(Some(add.path.to_string())));

                    columns.insert(STATS_TABLE_ADD_PATH.into(), values);
                }
            }

            rows = rows + 1;
        }
    }

    if rows == 0 {
        Ok(None)
    } else {
        let mut arrays = Vec::new();

        for field in fields.iter() {
            if let Some(values) = columns.remove(field.name()) {
                let array = ScalarValue::iter_to_array(values).expect("could not convert array");
                arrays.push(array);
            } else {
                let array = match field.data_type() {
                    DataType::Boolean => Arc::new(BooleanArray::new_null(rows)) as ArrayRef,
                    DataType::Int64 => Arc::new(Int64Array::new_null(rows)) as ArrayRef,
                    DataType::Int32 => Arc::new(Int32Array::new_null(rows)) as ArrayRef,
                    DataType::Int16 => Arc::new(Int16Array::new_null(rows)) as ArrayRef,
                    DataType::Int8 => Arc::new(Int8Array::new_null(rows)) as ArrayRef,
                    DataType::UInt64 => Arc::new(UInt64Array::new_null(rows)) as ArrayRef,
                    DataType::UInt32 => Arc::new(UInt32Array::new_null(rows)) as ArrayRef,
                    DataType::UInt16 => Arc::new(UInt16Array::new_null(rows)) as ArrayRef,
                    DataType::UInt8 => Arc::new(UInt8Array::new_null(rows)) as ArrayRef,
                    DataType::Float64 => Arc::new(Float64Array::new_null(rows)) as ArrayRef,
                    DataType::Float32 => Arc::new(Float32Array::new_null(rows)) as ArrayRef,
                    DataType::Date64 => Arc::new(Date64Array::new_null(rows)) as ArrayRef,
                    DataType::Date32 => Arc::new(Date32Array::new_null(rows)) as ArrayRef,
                    DataType::Utf8 => Arc::new(StringArray::new_null(rows)) as ArrayRef,
                    DataType::LargeUtf8 => Arc::new(StringArray::new_null(rows)) as ArrayRef,
                    DataType::Timestamp(unit, _) => match unit {
                        TimeUnit::Second => {
                            Arc::new(TimestampSecondArray::new_null(rows)) as ArrayRef
                        }
                        TimeUnit::Millisecond => {
                            Arc::new(TimestampMillisecondArray::new_null(rows)) as ArrayRef
                        }
                        TimeUnit::Microsecond => {
                            Arc::new(TimestampMicrosecondArray::new_null(rows)) as ArrayRef
                        }
                        TimeUnit::Nanosecond => {
                            Arc::new(TimestampNanosecondArray::new_null(rows)) as ArrayRef
                        }
                    },
                    DataType::List(field) => {
                        Arc::new(ListArray::new_null(field.clone(), rows)) as ArrayRef
                    }
                    DataType::Struct(fields) => {
                        Arc::new(StructArray::new_null(fields.clone(), rows)) as ArrayRef
                    }
                    _ => unimplemented!(),
                };

                arrays.push(array);
            }
        }

        Ok(Some(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            arrays,
        )?))
    }
}
