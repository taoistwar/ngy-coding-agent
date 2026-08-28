//! Persistence responsibilities for coding-agent domain data.

mod artifacts;
mod claims;
pub mod delivery;
mod migrate;
mod projection;
mod recovery;
mod repositories;
mod reviews;
mod stop_intents;
mod tasks;

use std::path::Path;
use std::time::Duration;

use coding_agent_domain::{DomainError, TaskStatus};
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Connection as _, SqlitePool};

pub use delivery::{
    AcceptMergeCommandRequest, AcceptMergeOutcome, AcceptedDeliverySourceState,
    AdvanceDeliverySourceObjectRequest, ArtifactDispositionRecord, BeginMergeAbortRequest,
    BindMergePreflightInputsOutcome, BindMergePreflightInputsRequest,
    BranchCleanupKnownNotAppliedReason, BranchDisposition, CleanupAcceptanceOutcome, CleanupKind,
    CleanupOperationAnchor, CleanupOperationRecord, CleanupOperationState,
    CleanupReconciliationReason, CleanupState, CleanupTargetHeadObservationRecord,
    CleanupTransition, CleanupTransitionOutcome, CleanupTransitionReceipt,
    CommitDeliverySourceRequest, CompleteBranchCleanupRequest, CompleteMergeAbortRequest,
    CompleteMergeRequest, CompleteWorktreeCleanupRequest, CreateDeliverySourceOutcome,
    CreateDeliverySourceRequest, CreatePreflightOutcome, CreatePreflightRequest,
    DELIVERY_COMMAND_REQUEST_HASH_ALGORITHM, DELIVERY_COMMAND_REQUEST_HASH_DOMAIN,
    DELIVERY_COMMAND_REQUEST_HASH_VERSION, DIRECTORY_IDENTITY_ALGORITHM_V1,
    DeleteBranchCommandRequest, DeliveryAcceptedOperationState, DeliveryArtifactProvenance,
    DeliveryCommand, DeliveryCommandId, DeliveryCommandKind, DeliveryCommandLookup,
    DeliveryCommandReceipt, DeliveryCommitMetadata, DeliveryEligibilitySnapshot, DeliveryError,
    DeliveryIdentity, DeliveryMutationEntity, DeliveryMutationEntityId, DeliveryMutationEntityKind,
    DeliveryMutationKey, DeliveryMutationKind, DeliveryMutationReceiptIdentity,
    DeliveryMutationRequest, DeliveryOperationId, DeliveryOperationSnapshot,
    DeliveryOwnershipSnapshot, DeliveryRecoveryAction, DeliveryRecoveryBatch,
    DeliveryRecoveryCursor, DeliveryRecoveryDisposition, DeliveryRecoveryEntry,
    DeliveryRecoveryQuery, DeliveryRecoveryQueryError, DeliveryResponseDiscriminator,
    DeliverySourceAnchor, DeliverySourceAppliedProof, DeliverySourceObjectProof,
    DeliverySourceReconciliationReason, DeliverySourceRecord, DeliverySourceRetryReason,
    DeliverySourceState, DeliverySourceTransitionOutcome, DeliverySourceTransitionReceipt,
    DeliveryState, DeliveryTimestamp, DeliveryVersion, DirectoryIdentity,
    EVIDENCE_IDENTITY_ALGORITHM_V1, EnterMergePendingRequest, EnterWorktreeRemovePendingRequest,
    EvidenceIdentityV1, FailUnboundMergePreflightOutcome, FailUnboundMergePreflightRequest,
    FailureCode, GitBranchRef, GitCommitOid, GitObjectAlgorithm, GitOid, GitTreeOid,
    MAX_DELIVERY_RECOVERY_BATCH, MarkPreflightStaleOutcome, MarkPreflightStaleRequest,
    MergeAbortAppliedProof, MergeAbortProof, MergeAppliedProof, MergeAutostashObservation,
    MergeCommitObjectProof, MergeConflictPathEncoding, MergeConflictPaths, MergeConflictRecord,
    MergeKnownNotAppliedReason, MergeOperationRecord, MergeOperationState, MergePreflightResult,
    MergeReconciliationReason, MergeTransitionOutcome, MergeTransitionReceipt,
    OtherGitOperationObservation, PersistentEligibilityBlocker, PreflightCommandRequest,
    PreflightRejectedReason, PreflightStaleReason, PreparedMergePreflightInputs,
    ReconcileBranchCleanupRequest, ReconcileDeliverySourceOutcome, ReconcileDeliverySourceReceipt,
    ReconcileDeliverySourceRequest, ReconcileMergeRequest, ReconcileWorktreeCleanupRequest,
    RecordBranchCleanupFailureRequest, RecordDeliverySourceRetryRequest,
    RecordMergeKnownFailureRequest, RecordMergePreflightResultRequest,
    RecordWorktreeCleanupFailureRequest, RecordWorktreeUnlockedRequest,
    RefreshBranchCleanupTargetRequest, RemoveWorktreeCommandRequest, Sha256Digest,
    SourceWorktreeProof, StartupDeliveryOwnership, StateTransition, UnboundMergePreflightFailure,
    WorktreeCleanupKnownNotAppliedReason, WorktreeDisposition, validate_cleanup_state,
    validate_cleanup_transition, validate_merge_source_state,
};
pub use projection::{
    BootstrapSnapshot, EventPage, QueueCapacity, SchedulerBootstrapSnapshot, TaskDetail,
};
pub use recovery::RecoveryReceipt;
pub use repositories::{RegisterRepositoryOutcome, RepositoryIdentityLookup};
pub use reviews::{FinalizeReviewedTaskOutcome, RecordReviewOutcome};
pub use stop_intents::{
    FinalizeStoppedTaskOutcome, FinalizeStoppedTaskReceipt, FinalizeStoppedTaskRequest,
    MAX_STOP_INTENT_BATCH, PersistStopIntentOutcome, StopIntentBatchItem, StopIntentBatchReceipt,
    StopIntentKind, StopIntentReceipt, StopIntentRequest,
};
pub use tasks::{
    AppendEventOutcome, CreateTaskOutcome, FinalizeUnreviewedTaskOutcome,
    FinalizeUnreviewedTaskRequest, QueueLimitedCreateTaskOutcome, QueueLimitedRetryTaskOutcome,
    RecoveryOutcome, RetryTaskOutcome, TaskTransition, TransitionOutcome,
};

pub const DATABASE_SCHEMA_UNSUPPORTED: &str = "DATABASE_SCHEMA_UNSUPPORTED";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Delivery(#[from] DeliveryError),
    #[error("stored repository ID is invalid: {0}")]
    InvalidRepositoryId(#[from] uuid::Error),
    #[error("stored task ID is invalid: {0}")]
    InvalidTaskId(uuid::Error),
    #[error("stored client request ID is invalid: {0}")]
    InvalidClientRequestId(uuid::Error),
    #[error("stored task status is invalid: {0}")]
    InvalidTaskStatus(String),
    #[error("stored task delivery readiness is invalid: {0}")]
    InvalidDeliveryReadiness(String),
    #[error("stored task event kind is invalid: {0}")]
    InvalidEventKind(String),
    #[error("stored task event schema version is invalid: {0}")]
    InvalidEventSchemaVersion(i64),
    #[error("stored attempt artifact state is invalid: {0}")]
    InvalidArtifactState(String),
    #[error("DATABASE_SCHEMA_UNSUPPORTED")]
    DatabaseSchemaUnsupported,
    #[error("database migration failed")]
    DatabaseMigration(#[source] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("illegal task transition from {from:?} to {to:?}")]
    IllegalTransition { from: TaskStatus, to: TaskStatus },
    #[error("IDEMPOTENCY_CONFLICT")]
    IdempotencyConflict,
    #[error("task was not found")]
    TaskNotFound,
    #[error("TASK_NOT_MERGE_ELIGIBLE")]
    TaskNotMergeEligible,
    #[error("DELIVERY_OPERATION_IN_PROGRESS")]
    DeliveryOperationInProgress,
    #[error("DELIVERY_RECONCILIATION_REQUIRED")]
    DeliveryReconciliationRequired,
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

    /// Seals and drains every handle sharing this store's pool, then checkpoints the WAL.
    ///
    /// A dedicated connection is opened after pool closure so the strict truncating
    /// checkpoint runs only after every pooled reader has stopped. The dedicated
    /// connection is closed unconditionally, including when the checkpoint fails.
    pub async fn checkpoint_and_close(&self) -> Result<(), StoreError> {
        let checkpoint_options = self
            .pool
            .connect_options()
            .as_ref()
            .clone()
            .create_if_missing(false);
        self.pool.close().await;

        let mut checkpoint_connection = SqliteConnection::connect_with(&checkpoint_options).await?;
        let checkpoint = checkpoint_wal(&mut checkpoint_connection).await;
        let close = checkpoint_connection
            .close()
            .await
            .map_err(StoreError::from);
        checkpoint.and(close)
    }

    /// Closes every handle sharing this store's pool without exposing raw SQL access.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    /// Raw database access for integration fixtures and explicit test-support builds.
    ///
    /// Production release builds intentionally omit this escape hatch so reviewed
    /// task finalization cannot be bypassed outside the store's typed API.
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

async fn checkpoint_wal(connection: &mut SqliteConnection) -> Result<(), StoreError> {
    let (busy, log_frames, checkpointed_frames): (i64, i64, i64) =
        sqlx::query_as("PRAGMA wal_checkpoint(TRUNCATE)")
            .fetch_one(connection)
            .await?;
    validate_wal_checkpoint(busy, log_frames, checkpointed_frames)
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
pub use claims::{
    ClaimTaskOutcome, ClaimTaskReceipt, ClaimTaskReconciliationOutcome, ClaimTaskRequest,
};
