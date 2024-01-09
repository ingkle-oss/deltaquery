use arrow::datatypes::{DataType, Fields};
use deltalake::datafusion::common::scalar::ScalarValue;
use deltalake::datafusion::common::Column;
use deltalake::datafusion::logical_expr::{Cast, Expr};

fn get_binary_expression_with_minmax(
    predicates: &sqlparser::ast::Expr,
    fields: &Fields,
) -> (Expr, Expr, Expr) {
    match predicates {
        sqlparser::ast::Expr::BinaryOp { left, right, .. } => {
            let min = parse_expression(left, fields, false);
            let max = parse_expression(left, fields, true);
            let right = parse_expression(right, fields, false);

            (min, max, right)
        }
        _ => unimplemented!(),
    }
}

pub fn parse_expression(predicates: &sqlparser::ast::Expr, fields: &Fields, use_max: bool) -> Expr {
    match predicates {
        sqlparser::ast::Expr::BinaryOp { left, op, right } => match op {
            sqlparser::ast::BinaryOperator::And => {
                let left = parse_expression(left, fields, use_max);
                let right = parse_expression(right, fields, use_max);

                left.and(right)
            }
            sqlparser::ast::BinaryOperator::Or => {
                let left = parse_expression(left, fields, use_max);
                let right = parse_expression(right, fields, use_max);

                left.or(right)
            }
            sqlparser::ast::BinaryOperator::Eq => {
                let (min, max, right) = get_binary_expression_with_minmax(predicates, fields);

                if min == max {
                    min.eq(right)
                } else {
                    min.lt_eq(right.clone()).and(max.gt_eq(right))
                }
            }
            sqlparser::ast::BinaryOperator::Lt => {
                let left = parse_expression(left, fields, use_max);
                let right = parse_expression(right, fields, use_max);

                left.lt(right)
            }
            sqlparser::ast::BinaryOperator::LtEq => {
                let left = parse_expression(left, fields, use_max);
                let right = parse_expression(right, fields, use_max);

                left.lt_eq(right)
            }
            sqlparser::ast::BinaryOperator::Gt => {
                let left = parse_expression(left, fields, use_max);
                let right = parse_expression(right, fields, use_max);

                left.gt(right)
            }
            sqlparser::ast::BinaryOperator::GtEq => {
                let left = parse_expression(left, fields, use_max);
                let right = parse_expression(right, fields, use_max);

                left.gt_eq(right)
            }
            _ => unimplemented!(),
        },
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
                if field.data_type() != &DataType::Utf8 {
                    Expr::Cast(Cast::new(
                        Box::new(Expr::Column(Column::from_name(column))),
                        DataType::Utf8,
                    ))
                } else {
                    Expr::Column(Column::from_name(column))
                }
            } else {
                Expr::Column(Column::from_name(column))
            }
        }
        sqlparser::ast::Expr::Value(value) => match value {
            sqlparser::ast::Value::Number(n, l) => {
                if *l {
                    Expr::Literal(ScalarValue::Int64(Some(n.parse::<i64>().unwrap())))
                } else {
                    Expr::Literal(ScalarValue::Int32(Some(n.parse::<i32>().unwrap())))
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s) => {
                Expr::Literal(ScalarValue::Utf8(Some(s.clone())))
            }
            _ => unimplemented!(),
        },
        _ => unimplemented!(),
    }
}
