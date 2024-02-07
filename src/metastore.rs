use crate::configs::{DQMetastoreConfig, DQTableConfig};
use anyhow::Error;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};

pub struct DQMetastore {
    pool: Pool<Postgres>,
}

impl DQMetastore {
    pub async fn try_new(config: &DQMetastoreConfig) -> Result<Self, Error> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(config.url.as_str())
            .await?;

        Ok(DQMetastore { pool })
    }

    pub async fn get_tables(&self) -> Result<Vec<DQTableConfig>, Error> {
        sqlx::query_as::<_, DQTableConfig>(
            format!(
                "SELECT {} FROM {}",
                DQTableConfig::FIELD_NAMES_AS_ARRAY.join(","),
                "tables"
            )
            .as_str(),
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.into())
    }
}
