use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, SelectItem, SetExpr, Statement, TableFactor,
};

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
                        _ => {}
                    }
                }
            }
            _ => {}
        },
        Statement::Insert { table_name, .. } => {
            let target = table_name
                .0
                .iter()
                .map(|o| o.value.clone())
                .collect::<Vec<String>>();

            return Some(target.join("."));
        }
        _ => {}
    }

    None
}

pub fn get_columns(statement: &Statement) -> Vec<String> {
    let mut items = Vec::new();

    match statement {
        Statement::Query(query) => match query.body.as_ref() {
            SetExpr::Select(select) => {
                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) => match expr {
                            Expr::Identifier(ident) => {
                                items.push(ident.value.clone());
                            }
                            Expr::Function(func) => {
                                for arg in &func.args {
                                    match arg {
                                        FunctionArg::Unnamed(expr) => match expr {
                                            FunctionArgExpr::Expr(expr) => match expr {
                                                Expr::Identifier(ident) => {
                                                    items.push(ident.value.clone());
                                                }
                                                _ => unimplemented!(),
                                            },
                                            _ => unimplemented!(),
                                        },
                                        _ => unimplemented!(),
                                    }
                                }
                            }
                            Expr::Value(value) => {
                                items.push(value.to_string());
                            }
                            _ => unimplemented!(),
                        },
                        _ => unimplemented!(),
                    }
                }
            }
            _ => unimplemented!(),
        },
        Statement::Insert { columns, .. } => {
            for column in columns {
                items.push(column.value.clone());
            }
        }
        _ => unimplemented!(),
    }

    items
}

pub fn get_functions(statement: &Statement) -> Vec<String> {
    let mut items = Vec::new();

    match statement {
        Statement::Query(query) => match query.body.as_ref() {
            SetExpr::Select(select) => {
                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) => match expr {
                            Expr::Function(func) => {
                                let target = func
                                    .name
                                    .0
                                    .iter()
                                    .map(|o| o.value.clone())
                                    .collect::<Vec<String>>();

                                items.push(target.join("."));
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

    items
}
