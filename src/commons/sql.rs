use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement, TableFactor};

pub fn get_table(statement: &Statement) -> Option<String> {
    match statement {
        Statement::Query(query) => {
            if let SetExpr::Select(select) = query.body.as_ref() {
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
        }
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

pub fn get_functions(statement: &Statement) -> Vec<String> {
    let mut items = Vec::new();

    match statement {
        Statement::Query(query) => {
            if let SetExpr::Select(select) = query.body.as_ref() {
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
                        SelectItem::ExprWithAlias { expr, .. } => match expr {
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
        }
        _ => {}
    }

    items
}
