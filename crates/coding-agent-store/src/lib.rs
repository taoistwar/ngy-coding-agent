//! Persistence responsibilities for coding-agent domain data.

mod artifacts;
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
    #[error("stored attempt artifact state is invalid: {0}")]
    InvalidArtifactState(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("illegal task transition from {from:?} to {to:?}")]
    IllegalTransition { from: TaskStatus, to: TaskStatus },
    #[error("the client request ID belongs to different task input")]
    IdempotencyConflict,
    #[error("task was not found")]
    TaskNotFound,
    #[error("attempt artifact input is invalid")]
    InvalidArtifactInput,
    #[error("attempt artifact identity conflicts with durable state")]
    ArtifactIdentityConflict,
    #[error("attempt artifact was not found")]
    ArtifactNotFound,
    #[error("attempt artifact state transition conflicts with durable state")]
    ArtifactStateConflict,
    #[error("task is not terminal and cannot be retried")]
    TaskNotRetryable,
    #[error("only non-lifecycle panel events may be appended to a running task")]
    InvalidRunningEvent,
    #[error("stored task attempt exceeds the supported range")]
    TaskAttemptOverflow,
    #[error("store invariant failed: {0}")]
    InvariantViolation(&'static str),
    #[error(
        "SQLite WAL checkpoint did not complete (busy={busy}, log_frames={log_frames}, checkpointed_frames={checkpointed_frames})"
    )]
    WalCheckpointIncomplete {
        busy: i64,
        log_frames: i64,
        checkpointed_frames: i64,
    },
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

    /// Checkpoints the WAL and then closes every handle sharing this store's pool.
    ///
    /// Pool closure is unconditional: callers receive the checkpoint error only after
    /// SQLx has closed the shared pool, so a failed checkpoint cannot leave SQLite
    /// handles live during process shutdown.
    pub async fn checkpoint_and_close(&self) -> Result<(), StoreError> {
        let checkpoint = async {
            let (busy, log_frames, checkpointed_frames): (i64, i64, i64) =
                sqlx::query_as("PRAGMA wal_checkpoint(TRUNCATE)")
                    .fetch_one(&self.pool)
                    .await?;
            validate_wal_checkpoint(busy, log_frames, checkpointed_frames)
        }
        .await;

        self.pool.close().await;
        checkpoint
    }

    #[doc(hidden)]
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

fn validate_wal_checkpoint(
    busy: i64,
    log_frames: i64,
    checkpointed_frames: i64,
) -> Result<(), StoreError> {
    let no_wal = log_frames == -1 && checkpointed_frames == -1;
    let complete_wal =
        log_frames >= 0 && checkpointed_frames >= 0 && log_frames == checkpointed_frames;
    if busy == 0 && (no_wal || complete_wal) {
        Ok(())
    } else {
        Err(StoreError::WalCheckpointIncomplete {
            busy,
            log_frames,
            checkpointed_frames,
        })
    }
}
pub use artifacts::{
    AttemptArtifactIdentity, AttemptArtifactState, ReserveAttemptArtifact,
    ReserveAttemptArtifactOutcome, TaskAttemptArtifact, UpdateAttemptArtifactOutcome,
};
