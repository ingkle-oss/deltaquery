use arrow::datatypes::{DataType, Fields, TimeUnit};
use datafusion::common::scalar::ScalarValue;
use datafusion::common::Column;
use datafusion::logical_expr::{Cast, Expr};

pub fn parse_expression(
    predicates: &sqlparser::ast::Expr,
    fields: &Fields,
    use_max: bool,
) -> Option<Expr> {
    match predicates {
        sqlparser::ast::Expr::BinaryOp { left, op, right } => match op {
            sqlparser::ast::BinaryOperator::And => {
                let left = parse_expression(left, fields, use_max);
                let right = parse_expression(right, fields, use_max);

                match (left, right) {
                    (Some(left), Some(right)) => Some(left.and(right)),
                    (Some(left), None) => Some(left),
                    (None, Some(right)) => Some(right),
                    _ => None,
                }
            }
            sqlparser::ast::BinaryOperator::Or => {
                let left = parse_expression(left, fields, use_max);
                let right = parse_expression(right, fields, use_max);

                match (left, right) {
                    (Some(left), Some(right)) => Some(left.or(right)),
                    _ => None,
                }
            }
            sqlparser::ast::BinaryOperator::Eq => {
                let min = parse_expression(left, fields, false);
                let max = parse_expression(left, fields, true);
                let right = parse_expression(right, fields, false);

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
                let left = parse_expression(left, fields, use_max);
                let right = parse_expression(right, fields, use_max);

                if let (Some(left), Some(right)) = (left, right) {
                    Some(left.lt(right))
                } else {
                    None
                }
            }
            sqlparser::ast::BinaryOperator::LtEq => {
                let left = parse_expression(left, fields, use_max);
                let right = parse_expression(right, fields, use_max);

                if let (Some(left), Some(right)) = (left, right) {
                    Some(left.lt_eq(right))
                } else {
                    None
                }
            }
            sqlparser::ast::BinaryOperator::Gt => {
                let left = parse_expression(left, fields, use_max);
                let right = parse_expression(right, fields, use_max);

                if let (Some(left), Some(right)) = (left, right) {
                    Some(left.gt(right))
                } else {
                    None
                }
            }
            sqlparser::ast::BinaryOperator::GtEq => {
                let left = parse_expression(left, fields, use_max);
                let right = parse_expression(right, fields, use_max);

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
            let min = parse_expression(expr, fields, false);
            let max = parse_expression(expr, fields, true);
            let low = parse_expression(low, fields, use_max);
            let high = parse_expression(high, fields, use_max);

            if let (Some(min), Some(max), Some(low), Some(high)) = (min, max, low, high) {
                if *negated {
                    Some(
                        min.not_between(low.clone(), high.clone())
                            .and(max.not_between(low, high)),
                    )
                } else {
                    Some(
                        min.between(low.clone(), high.clone())
                            .or(max.between(low, high)),
                    )
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
                    DataType::Timestamp(TimeUnit::Microsecond, None) => Expr::Cast(Cast::new(
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
                Some(Expr::Literal(ScalarValue::Utf8(Some(s.clone()))))
            }
            _ => unimplemented!(),
        },
        _ => unimplemented!(),
    }
}
