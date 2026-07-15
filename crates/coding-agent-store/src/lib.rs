//! Persistence responsibilities for coding-agent domain data.

mod migrate;
mod repositories;

use std::path::Path;
use std::time::Duration;

use coding_agent_domain::DomainError;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};

pub use repositories::RegisterRepositoryOutcome;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("stored repository ID is invalid: {0}")]
    InvalidRepositoryId(#[from] uuid::Error),
}

#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
}

impl Store {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        let in_memory = path == Path::new(":memory:");
        let options = SqliteConnectOptions::new()
            .filename(path)
            .in_memory(in_memory)
            .shared_cache(in_memory)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(if in_memory { 1 } else { 5 })
            .connect_with(options)
            .await?;

        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<(), StoreError> {
        migrate::run(&self.pool).await
    }

    #[doc(hidden)]
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}
