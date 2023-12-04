use crate::configs::DQConfig;
use crate::table::DQTable;
use crate::tables::polars::DQPolarsTable;
use std::collections::hash_map::IterMut;
use std::collections::HashMap;

pub struct DQState {
    config: DQConfig,

    tables: HashMap<String, Box<dyn DQTable>>,
}

impl DQState {
    pub async fn new(config: DQConfig) -> Self {
        DQState {
            config,
            tables: HashMap::new(),
        }
    }

    pub async fn get_table(&mut self, target: &String) -> Option<&mut Box<dyn DQTable>> {
        if !self.tables.contains_key(target) {
            if let Some(table_config) = self.config.tables.iter().find(|c| &c.name == target) {
                let filesystem_config = if let Some(filesystem) = &table_config.filesystem {
                    self.config
                        .filesystems
                        .iter()
                        .find(|c| &c.name == filesystem)
                } else {
                    None
                };

                let mut table: Box<dyn DQTable> = match table_config.repository.as_str() {
                    _ => Box::new(DQPolarsTable::new(table_config, filesystem_config, self).await),
                };
                let _ = table.update().await;

                self.tables.insert(target.clone(), table);
            }
        }

        if let Some(table) = self.tables.get_mut(target) {
            Some(table)
        } else {
            None
        }
    }

    pub fn get_tables(&mut self) -> IterMut<'_, String, Box<dyn DQTable>> {
        self.tables.iter_mut()
    }
}
