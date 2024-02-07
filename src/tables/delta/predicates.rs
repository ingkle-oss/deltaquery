use arrow::datatypes::{DataType, Fields};
use datafusion::common::scalar::ScalarValue;
use datafusion::common::Column;
use datafusion::logical_expr::{Cast, Expr};

fn get_column_name_from_expression(expr: &Expr) -> &String {
    match expr {
        Expr::Column(column) => &column.name,
        Expr::Cast(cast) => match cast.expr.as_ref() {
            Expr::Column(column) => &column.name,
            _ => unimplemented!(),
        },
        _ => unimplemented!(),
    }
}

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
                let right = parse_expression(right, fields, use_max, None);

                match (left, right) {
                    (Some(left), Some(right)) => Some(left.and(right)),
                    (Some(left), None) => Some(left),
                    (None, Some(right)) => Some(right),
                    _ => None,
                }
            }
            sqlparser::ast::BinaryOperator::Or => {
                let left = parse_expression(left, fields, use_max, None);
                let right = parse_expression(right, fields, use_max, None);

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
                let left = parse_expression(left, fields, false, None);
                let right = parse_expression(right, fields, false, left.as_ref());

                if let (Some(left), Some(right)) = (left, right) {
                    Some(left.lt(right))
                } else {
                    None
                }
            }
            sqlparser::ast::BinaryOperator::LtEq => {
                let left = parse_expression(left, fields, false, None);
                let right = parse_expression(right, fields, false, left.as_ref());

                if let (Some(left), Some(right)) = (left, right) {
                    Some(left.lt_eq(right))
                } else {
                    None
                }
            }
            sqlparser::ast::BinaryOperator::Gt => {
                let left = parse_expression(left, fields, true, None);
                let right = parse_expression(right, fields, true, left.as_ref());

                if let (Some(left), Some(right)) = (left, right) {
                    Some(left.gt(right))
                } else {
                    None
                }
            }
            sqlparser::ast::BinaryOperator::GtEq => {
                let left = parse_expression(left, fields, true, None);
                let right = parse_expression(right, fields, true, left.as_ref());

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

            if let Some((_, _)) = fields.find(&column) {
                let expr = Expr::Column(Column::from_name(column));

                Some(expr)
            } else {
                None
            }
        }
        sqlparser::ast::Expr::Value(value) => match value {
            sqlparser::ast::Value::Number(n, _) => {
                if let Ok(l) = n.parse::<i64>() {
                    Some(Expr::Literal(ScalarValue::Int64(Some(l))))
                } else if let Ok(l) = n.parse::<u64>() {
                    Some(Expr::Literal(ScalarValue::UInt64(Some(l))))
                } else if let Ok(l) = n.parse::<f64>() {
                    Some(Expr::Literal(ScalarValue::Float64(Some(l))))
                } else {
                    unimplemented!()
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s) => {
                if let Some(other) = other {
                    if let Some((_, field)) = fields.find(get_column_name_from_expression(other)) {
                        match field.data_type() {
                            DataType::Utf8 => {
                                Some(Expr::Literal(ScalarValue::Utf8(Some(s.clone()))))
                            }
                            other => Some(Expr::Cast(Cast::new(
                                Box::new(Expr::Literal(ScalarValue::Utf8(Some(s.clone())))),
                                other.clone(),
                            ))),
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
