use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, GroupByExpr, Ident, Query, Select, SelectItem, SetExpr,
    Statement, TableFactor,
};
use std::str::FromStr;

#[derive(Hash, Eq, PartialEq, Debug)]
pub enum DQSqlSelectPolicy {
    ExecuteAndBypass,
    ProjectionAndExecute,
    ExecuteAndMerge,
    NotSupported,
}

pub fn get_table(statement: &Statement) -> Option<String> {
    match statement {
        Statement::Query(query) => match query.body.as_ref() {
            SetExpr::Select(select) => {
                for table in &select.from {
                    match &table.relation {
                        TableFactor::Table { name, .. } => {
                            let target = name
                                .0
                                .iter()
                                .map(|o| o.value.clone())
                                .collect::<Vec<String>>();

                            return Some(target.join("."));
                        }
                        _ => unimplemented!(),
                    }
                }
            }
            _ => unimplemented!(),
        },
        _ => unimplemented!(),
    }

    None
}

pub fn get_projection(statement: &Statement) -> Vec<String> {
    let mut projection = Vec::new();

    match statement {
        Statement::Query(query) => match query.body.as_ref() {
            SetExpr::Select(select) => {
                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) => match expr {
                            Expr::Identifier(ident) => {
                                projection.push(ident.value.clone());
                            }
                            _ => unimplemented!(),
                        },
                        _ => unimplemented!(),
                    }
                }
            }
            _ => unimplemented!(),
        },
        _ => unimplemented!(),
    }

    projection
}

pub fn get_select_policy(statement: &Statement) -> DQSqlSelectPolicy {
    match statement {
        Statement::Query(query) => match query.body.as_ref() {
            SetExpr::Select(select) => {
                if select.from.iter().any(|table| table.joins.len() > 0) {
                    return DQSqlSelectPolicy::NotSupported;
                }

                for item in select.projection.iter() {
                    match item {
                        SelectItem::UnnamedExpr(expr) => match expr {
                            Expr::Function(func) => {
                                for ident in func.name.0.iter() {
                                    if ident.value == "avg" {
                                        return DQSqlSelectPolicy::ProjectionAndExecute;
                                    }
                                }
                            }
                            _ => {}
                        },
                        SelectItem::ExprWithAlias { expr, .. } => match expr {
                            Expr::Function(func) => {
                                for ident in func.name.0.iter() {
                                    if ident.value == "avg" {
                                        return DQSqlSelectPolicy::ProjectionAndExecute;
                                    }
                                }
                            }
                            _ => {}
                        },
                        _ => {}
                    }
                }

                match &select.group_by {
                    GroupByExpr::All => DQSqlSelectPolicy::ExecuteAndMerge,
                    GroupByExpr::Expressions(expressions) => {
                        if expressions.len() > 0 {
                            DQSqlSelectPolicy::ExecuteAndMerge
                        } else {
                            DQSqlSelectPolicy::ExecuteAndBypass
                        }
                    }
                }
            }
            _ => DQSqlSelectPolicy::ExecuteAndBypass,
        },
        _ => DQSqlSelectPolicy::ExecuteAndBypass,
    }
}

pub fn build_statement_for_projection(statement: &Statement) -> Option<Statement> {
    let mut projection = Vec::new();
    let mut from = Vec::new();

    match statement {
        Statement::Query(query) => match query.body.as_ref() {
            SetExpr::Select(select) => {
                from = select.from.clone();

                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) => match expr {
                            Expr::Identifier(ident) => {
                                projection.push(ident.value.clone());
                            }
                            _ => {}
                        },
                        SelectItem::ExprWithAlias { expr, .. } => match expr {
                            Expr::Identifier(ident) => {
                                projection.push(ident.value.clone());
                            }
                            Expr::Function(func) => {
                                for arg in func.args.iter() {
                                    match arg {
                                        FunctionArg::Unnamed(expr) => match expr {
                                            FunctionArgExpr::Expr(expr) => match expr {
                                                Expr::Identifier(ident) => {
                                                    projection.push(ident.value.clone());
                                                }
                                                _ => {}
                                            },
                                            _ => {}
                                        },
                                        _ => {}
                                    }
                                }
                            }
                            _ => {}
                        },
                        _ => {}
                    }
                }
            }
            _ => {}
        },
        _ => {}
    }

    Some(Statement::Query(Box::new(Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            distinct: None,
            top: None,
            projection: projection
                .iter()
                .map(|column| {
                    SelectItem::UnnamedExpr(Expr::Identifier(Ident {
                        value: column.into(),
                        quote_style: None,
                    }))
                })
                .collect(),
            into: None,
            from: from,
            lateral_views: vec![],
            selection: None,
            group_by: GroupByExpr::Expressions(vec![]),
            having: None,
            named_window: vec![],
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            qualify: None,
        }))),
        order_by: vec![],
        limit: None,
        limit_by: vec![],
        offset: None,
        fetch: None,
        locks: vec![],
    })))
}

pub fn build_statement_for_merge(statement: &Statement) -> Option<Statement> {
    let mut statement = statement.clone();

    match &mut statement {
        Statement::Query(query) => match query.body.as_mut() {
            SetExpr::Select(select) => {
                for item in select.projection.iter_mut() {
                    match item {
                        SelectItem::UnnamedExpr(expr) => match expr {
                            Expr::Function(func) => {
                                for ident in func.name.0.iter_mut() {
                                    if ident.value == "count" {
                                        ident.value = String::from_str("sum").unwrap();
                                    }
                                }
                            }
                            _ => {}
                        },
                        SelectItem::ExprWithAlias { expr, .. } => match expr {
                            Expr::Function(func) => {
                                for ident in func.name.0.iter_mut() {
                                    if ident.value == "count" {
                                        ident.value = String::from_str("sum").unwrap();
                                    }
                                }
                            }
                            _ => {}
                        },
                        _ => {}
                    }
                }
            }
            _ => {}
        },
        _ => {}
    }

    Some(statement)
}
