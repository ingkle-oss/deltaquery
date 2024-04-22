use crate::configs::{DQAppConfig, DQMetastoreConfig, DQTableConfig};
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
                [
                    "name",
                    "type",
                    "storage",
                    "location",
                    "partitions",
                    "options",
                    "created_at",
                    "updated_at"
                ]
                .join(","),
                "tables"
            )
            .as_str(),
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.into())
    }

    pub async fn get_app(&self, name: &str) -> Result<DQAppConfig, Error> {
        sqlx::query_as::<_, DQAppConfig>(
            format!(
                "SELECT {} FROM {} WHERE name='{}'",
                ["name", "password", "created_at", "updated_at"].join(","),
                "apps",
                name
            )
            .as_str(),
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| e.into())
    }
}
