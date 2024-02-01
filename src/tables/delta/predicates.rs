use arrow::datatypes::{DataType, Fields, TimeUnit};
use chrono::NaiveDateTime;
use datafusion::common::scalar::ScalarValue;
use datafusion::common::Column;
use datafusion::logical_expr::{Cast, Expr};

pub fn parse_expression(
    predicates: &sqlparser::ast::Expr,
    fields: &Fields,
    use_max: bool,
    other: Option<&Expr>,
) -> Option<Expr> {
    match predicates {
        sqlparser::ast::Expr::BinaryOp { left, op, right } => match op {
            sqlparser::ast::BinaryOperator::And => {
                let left = parse_expression(left, fields, use_max, None);
                let right = parse_expression(right, fields, use_max, left.as_ref());

                match (left, right) {
                    (Some(left), Some(right)) => Some(left.and(right)),
                    (Some(left), None) => Some(left),
                    (None, Some(right)) => Some(right),
                    _ => None,
                }
            }
            sqlparser::ast::BinaryOperator::Or => {
                let left = parse_expression(left, fields, use_max, None);
                let right = parse_expression(right, fields, use_max, left.as_ref());

                match (left, right) {
                    (Some(left), Some(right)) => Some(left.or(right)),
                    _ => None,
                }
            }
            sqlparser::ast::BinaryOperator::Eq => {
                let min = parse_expression(left, fields, false, None);
                let max = parse_expression(left, fields, true, None);
                let right = parse_expression(right, fields, false, min.as_ref());

                if let (Some(min), Some(max), Some(right)) = (min, max, right) {
                    if min == max {
                        Some(min.eq(right))
                    } else {
                        Some(min.lt_eq(right.clone()).and(max.gt_eq(right)))
                    }
                } else {
                    None
                }
            }
            sqlparser::ast::BinaryOperator::Lt => {
                let left = parse_expression(left, fields, use_max, None);
                let right = parse_expression(right, fields, use_max, left.as_ref());

                if let (Some(left), Some(right)) = (left, right) {
                    Some(left.lt(right))
                } else {
                    None
                }
            }
            sqlparser::ast::BinaryOperator::LtEq => {
                let left = parse_expression(left, fields, use_max, None);
                let right = parse_expression(right, fields, use_max, left.as_ref());

                if let (Some(left), Some(right)) = (left, right) {
                    Some(left.lt_eq(right))
                } else {
                    None
                }
            }
            sqlparser::ast::BinaryOperator::Gt => {
                let left = parse_expression(left, fields, use_max, None);
                let right = parse_expression(right, fields, use_max, left.as_ref());

                if let (Some(left), Some(right)) = (left, right) {
                    Some(left.gt(right))
                } else {
                    None
                }
            }
            sqlparser::ast::BinaryOperator::GtEq => {
                let left = parse_expression(left, fields, use_max, None);
                let right = parse_expression(right, fields, use_max, left.as_ref());

                if let (Some(left), Some(right)) = (left, right) {
                    Some(left.gt_eq(right))
                } else {
                    None
                }
            }
            _ => unimplemented!(),
        },
        sqlparser::ast::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let min = parse_expression(expr, fields, false, None);
            let max = parse_expression(expr, fields, true, None);
            let low = parse_expression(low, fields, use_max, min.as_ref());
            let high = parse_expression(high, fields, use_max, max.as_ref());

            if let (Some(min), Some(max), Some(low), Some(high)) = (min, max, low, high) {
                if *negated {
                    Some(min.gt(high).or(max.lt(low)))
                } else {
                    Some(min.lt_eq(high).and(max.gt_eq(low)))
                }
            } else {
                None
            }
        }
        sqlparser::ast::Expr::Identifier(ident) => {
            let name = ident.value.clone();
            let column = if use_max {
                let name_max = [&name, "max"].join(".");
                if fields
                    .iter()
                    .any(|field| field.name() == &name_max.as_str())
                {
                    name_max
                } else {
                    name
                }
            } else {
                name
            };

            if let Some((_, field)) = fields.find(&column) {
                let expr = match field.data_type() {
                    DataType::Date32 | DataType::Date64 => Expr::Cast(Cast::new(
                        Box::new(Expr::Column(Column::from_name(column))),
                        DataType::Utf8,
                    )),
                    _ => Expr::Column(Column::from_name(column)),
                };

                Some(expr)
            } else {
                None
            }
        }
        sqlparser::ast::Expr::Value(value) => match value {
            sqlparser::ast::Value::Number(n, l) => {
                if *l {
                    Some(Expr::Literal(ScalarValue::Int64(Some(
                        n.parse::<i64>().expect("could not parse int64"),
                    ))))
                } else {
                    Some(Expr::Literal(ScalarValue::Int32(Some(
                        n.parse::<i32>().expect("could not parse int32"),
                    ))))
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s) => {
                if let Some(other) = other {
                    if let Some((_, field)) = fields.find(&other.to_string()) {
                        match field.data_type() {
                            DataType::Timestamp(unit, None) => {
                                let mut literal = None;

                                let formats = vec![
                                    "%Y-%m-%dT%H:%M:%S%.fZ",
                                    "%Y-%m-%dT%H:%M:%S%.f",
                                    "%Y-%m-%dT%H:%M:%S%z",
                                    "%Y-%m-%dT%H:%M:%S",
                                    "%Y-%m-%d %H:%M:%S%.fZ",
                                    "%Y-%m-%d %H:%M:%S%.f",
                                    "%Y-%m-%d %H:%M:%S%z",
                                    "%Y-%m-%d %H:%M:%S",
                                    "%Y-%m-%d %H:%M",
                                ];

                                for format in formats {
                                    if let Ok(datetime) = NaiveDateTime::parse_from_str(s, format) {
                                        literal = match unit {
                                            TimeUnit::Nanosecond => Some(Expr::Literal(
                                                ScalarValue::TimestampMicrosecond(
                                                    datetime.timestamp_nanos_opt(),
                                                    None,
                                                ),
                                            )),
                                            TimeUnit::Microsecond => Some(Expr::Literal(
                                                ScalarValue::TimestampMicrosecond(
                                                    Some(datetime.timestamp_micros()),
                                                    None,
                                                ),
                                            )),
                                            TimeUnit::Millisecond => Some(Expr::Literal(
                                                ScalarValue::TimestampMillisecond(
                                                    Some(datetime.timestamp_millis()),
                                                    None,
                                                ),
                                            )),
                                            TimeUnit::Second => {
                                                Some(Expr::Literal(ScalarValue::TimestampSecond(
                                                    Some(datetime.timestamp()),
                                                    None,
                                                )))
                                            }
                                        };

                                        break;
                                    }
                                }

                                literal
                            }
                            _ => Some(Expr::Literal(ScalarValue::Utf8(Some(s.clone())))),
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            _ => None,
        },
        _ => None,
    }
}
