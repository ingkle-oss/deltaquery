use deltalake::datafusion::common::scalar::ScalarValue;
use deltalake::datafusion::common::Column;
use deltalake::datafusion::logical_expr::Expr;

fn get_binary_expression_with_minmax(
    predicates: &sqlparser::ast::Expr,
    columns: &Vec<&str>,
) -> (Expr, Expr, Expr) {
    match predicates {
        sqlparser::ast::Expr::BinaryOp { left, right, .. } => {
            let min = parse_expression(left, columns, false);
            let max = parse_expression(left, columns, true);
            let right = parse_expression(right, columns, false);

            (min, max, right)
        }
        _ => unimplemented!(),
    }
}

pub fn parse_expression(
    predicates: &sqlparser::ast::Expr,
    columns: &Vec<&str>,
    use_max: bool,
) -> Expr {
    match predicates {
        sqlparser::ast::Expr::BinaryOp { left, op, right } => match op {
            sqlparser::ast::BinaryOperator::And => {
                let left = parse_expression(left, columns, use_max);
                let right = parse_expression(right, columns, use_max);

                left.and(right)
            }
            sqlparser::ast::BinaryOperator::Or => {
                let left = parse_expression(left, columns, use_max);
                let right = parse_expression(right, columns, use_max);

                left.or(right)
            }
            sqlparser::ast::BinaryOperator::Eq => {
                let (min, max, right) = get_binary_expression_with_minmax(predicates, columns);

                if min == max {
                    min.eq(right)
                } else {
                    min.lt_eq(right.clone()).and(max.gt_eq(right))
                }
            }
            sqlparser::ast::BinaryOperator::Lt => {
                let left = parse_expression(left, columns, use_max);
                let right = parse_expression(right, columns, use_max);

                left.lt(right)
            }
            sqlparser::ast::BinaryOperator::LtEq => {
                let left = parse_expression(left, columns, use_max);
                let right = parse_expression(right, columns, use_max);

                left.lt_eq(right)
            }
            sqlparser::ast::BinaryOperator::Gt => {
                let left = parse_expression(left, columns, use_max);
                let right = parse_expression(right, columns, use_max);

                left.gt(right)
            }
            sqlparser::ast::BinaryOperator::GtEq => {
                let left = parse_expression(left, columns, use_max);
                let right = parse_expression(right, columns, use_max);

                left.gt_eq(right)
            }
            _ => unimplemented!(),
        },
        sqlparser::ast::Expr::Identifier(ident) => {
            let name = ident.value.as_str();
            if use_max {
                let name_max = [name, "max"].join(".");
                if columns.contains(&name_max.as_str()) {
                    Expr::Column(Column::from_name(name))
                } else {
                    Expr::Column(Column::from_name(name))
                }
            } else {
                Expr::Column(Column::from_name(name))
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
