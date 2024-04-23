use crate::configs::{DQAppConfig, DQMetastoreConfig, DQTableConfig};
use anyhow::Error;
use sqlx::postgres::PgPoolOptions;
use sqlx::{postgres::PgRow, FromRow, Pool, Postgres, Row};

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

impl FromRow<'_, PgRow> for DQTableConfig {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            name: row.try_get("name")?,
            r#type: row.try_get("type")?,
            storage: row.try_get("storage")?,
            location: row.try_get("location")?,
            partitions: row
                .try_get::<serde_json::Value, _>("partitions")
                .ok()
                .map_or(None, |value| serde_json::from_value(value).ok()),
            options: row
                .try_get::<serde_json::Value, _>("options")
                .ok()
                .map_or(None, |value| serde_json::from_value(value).ok()),
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

impl FromRow<'_, PgRow> for DQAppConfig {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            name: row.try_get("name")?,
            password: row.try_get("password")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}
