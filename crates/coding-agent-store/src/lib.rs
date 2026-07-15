//! Persistence responsibilities for coding-agent domain data.

mod migrate;
mod projection;
mod repositories;
mod tasks;

use std::path::Path;
use std::time::Duration;

use coding_agent_domain::{DomainError, TaskStatus};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};

pub use projection::{BootstrapSnapshot, EventPage, TaskDetail};
pub use repositories::RegisterRepositoryOutcome;
pub use tasks::{
    AppendEventOutcome, CreateTaskOutcome, RecoveryOutcome, RetryTaskOutcome, TaskTransition,
    TransitionOutcome,
};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("stored repository ID is invalid: {0}")]
    InvalidRepositoryId(#[from] uuid::Error),
    #[error("stored task ID is invalid: {0}")]
    InvalidTaskId(uuid::Error),
    #[error("stored client request ID is invalid: {0}")]
    InvalidClientRequestId(uuid::Error),
    #[error("stored task status is invalid: {0}")]
    InvalidTaskStatus(String),
    #[error("stored task event kind is invalid: {0}")]
    InvalidEventKind(String),
    #[error("stored task event schema version is invalid: {0}")]
    InvalidEventSchemaVersion(i64),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("illegal task transition from {from:?} to {to:?}")]
    IllegalTransition { from: TaskStatus, to: TaskStatus },
    #[error("the client request ID belongs to different task input")]
    IdempotencyConflict,
    #[error("task was not found")]
    TaskNotFound,
    #[error("task is not terminal and cannot be retried")]
    TaskNotRetryable,
    #[error("only non-lifecycle panel events may be appended to a running task")]
    InvalidRunningEvent,
    #[error("stored task attempt exceeds the supported range")]
    TaskAttemptOverflow,
    #[error("store invariant failed: {0}")]
    InvariantViolation(&'static str),
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
