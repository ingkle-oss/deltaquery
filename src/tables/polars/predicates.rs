use polars::prelude::*;
use sqlparser::ast::BinaryOperator::{And, Eq, Gt, GtEq, Lt, LtEq, Or};
use sqlparser::ast::{Expr, Value};

fn get_binary_expression_with_minmax(
    predicates: &Expr,
    columns: &Vec<&str>,
) -> (
    polars::prelude::Expr,
    polars::prelude::Expr,
    polars::prelude::Expr,
) {
    match predicates {
        Expr::BinaryOp { left, right, .. } => {
            let min = parse_expression(left, columns, false);
            let max = parse_expression(left, columns, true);
            let right = parse_expression(right, columns, false);

            (min, max, right)
        }
        _ => unimplemented!(),
    }
}

pub fn parse_expression(
    predicates: &Expr,
    columns: &Vec<&str>,
    use_max: bool,
) -> polars::prelude::Expr {
    match predicates {
        Expr::BinaryOp { left, op, right } => match op {
            And => {
                let left = parse_expression(left, columns, use_max);
                let right = parse_expression(right, columns, use_max);

                left.and(right)
            }
            Or => {
                let left = parse_expression(left, columns, use_max);
                let right = parse_expression(right, columns, use_max);

                left.or(right)
            }
            Eq => {
                let (min, max, right) = get_binary_expression_with_minmax(predicates, columns);

                if min == max {
                    min.eq(right)
                } else {
                    min.lt_eq(right.clone()).and(max.gt_eq(right))
                }
            }
            Lt => {
                let left = parse_expression(left, columns, use_max);
                let right = parse_expression(right, columns, use_max);

                left.lt(right)
            }
            LtEq => {
                let left = parse_expression(left, columns, use_max);
                let right = parse_expression(right, columns, use_max);

                left.lt_eq(right)
            }
            Gt => {
                let left = parse_expression(left, columns, use_max);
                let right = parse_expression(right, columns, use_max);

                left.gt(right)
            }
            GtEq => {
                let left = parse_expression(left, columns, use_max);
                let right = parse_expression(right, columns, use_max);

                left.gt_eq(right)
            }
            _ => unimplemented!(),
        },
        Expr::Identifier(ident) => {
            let name = ident.value.as_str();
            if use_max {
                let name_max = [name, "max"].join(".");
                if columns.contains(&name_max.as_str()) {
                    col(&name_max)
                } else {
                    col(name)
                }
            } else {
                col(name)
            }
        }
        Expr::Value(value) => match value {
            Value::Number(n, l) => {
                if *l {
                    lit(n.parse::<i64>().unwrap())
                } else {
                    lit(n.parse::<i32>().unwrap())
                }
            }
            Value::SingleQuotedString(s) => lit(s.clone()),
            _ => unimplemented!(),
        },
        _ => unimplemented!(),
    }
}
