use std::collections::HashSet;
use std::num::{NonZeroU32, NonZeroU64};

use coding_agent_domain::{ClientRequestId, DomainError, EventId, NewTask, TaskId, TaskStatus};
use coding_agent_store::{
    ClaimTaskOutcome, ClaimTaskRequest, FinalizeReviewedTaskOutcome, FinalizeStoppedTaskOutcome,
    FinalizeStoppedTaskRequest, FinalizeUnreviewedTaskOutcome, MAX_STOP_INTENT_BATCH,
    PersistStopIntentOutcome, QueueLimitedCreateTaskOutcome, QueueLimitedRetryTaskOutcome,
    RecordReviewOutcome, StopIntentBatchReceipt, StopIntentRequest,
};

use crate::store_writer::{
    FinalizeReviewedTaskRequest, FinalizeUnreviewedTaskRequest, RecordReviewRequest,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MutationSequence(NonZeroU64);

impl MutationSequence {
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DurableOperationKind {
    CreateTask,
    RetryTask,
    ClaimTask,
    PersistStopIntent,
    FinalizeStoppedTask,
    RecordReview,
    FinalizeReviewedTask,
    FinalizeUnreviewedTask,
    AppendRunningEvent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskMutationIdentity {
    pub task_id: TaskId,
    pub sequence: MutationSequence,
    pub kind: DurableOperationKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DurableOperationIdentity {
    CreateTask { client_request_id: ClientRequestId },
    RetryTask { source_task_id: TaskId },
    TaskMutation(TaskMutationIdentity),
    StopIntentBatch { items: Vec<TaskMutationIdentity> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StopIntentBatchIdentityError {
    #[error("a stop-intent batch must contain one to four task mutations")]
    InvalidSize,
    #[error("a stop-intent batch contains duplicate task identities")]
    DuplicateTask,
    #[error("a stop-intent batch identity must contain only stop-intent mutations")]
    WrongOperationKind,
}

impl DurableOperationIdentity {
    pub fn stop_intent_batch(
        items: Vec<TaskMutationIdentity>,
    ) -> Result<Self, StopIntentBatchIdentityError> {
        if items.is_empty() || items.len() > MAX_STOP_INTENT_BATCH {
            return Err(StopIntentBatchIdentityError::InvalidSize);
        }
        if items
            .iter()
            .any(|item| item.kind != DurableOperationKind::PersistStopIntent)
        {
            return Err(StopIntentBatchIdentityError::WrongOperationKind);
        }
        let mut task_ids = HashSet::with_capacity(items.len());
        if items.iter().any(|item| !task_ids.insert(item.task_id)) {
            return Err(StopIntentBatchIdentityError::DuplicateTask);
        }
        Ok(Self::StopIntentBatch { items })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingDurableResult {
    QueueLimitedCreate {
        identity: DurableOperationIdentity,
        input: NewTask,
        max_queued_tasks: NonZeroU32,
    },
    QueueLimitedRetry {
        identity: DurableOperationIdentity,
        source_task_id: TaskId,
        max_queued_tasks: NonZeroU32,
    },
    ClaimTask {
        identity: TaskMutationIdentity,
        request: ClaimTaskRequest,
    },
    PersistStopIntentBatch {
        identity: DurableOperationIdentity,
        requests: Vec<StopIntentRequest>,
    },
    FinalizeStoppedTask {
        identity: TaskMutationIdentity,
        request: FinalizeStoppedTaskRequest,
    },
    RecordReview {
        identity: TaskMutationIdentity,
        request: RecordReviewRequest,
    },
    FinalizeReviewedTask {
        identity: TaskMutationIdentity,
        request: FinalizeReviewedTaskRequest,
    },
    FinalizeUnreviewedTask {
        identity: TaskMutationIdentity,
        request: FinalizeUnreviewedTaskRequest,
    },
}

impl PendingDurableResult {
    pub fn identity(&self) -> DurableOperationIdentity {
        match self {
            Self::QueueLimitedCreate { identity, .. }
            | Self::QueueLimitedRetry { identity, .. }
            | Self::PersistStopIntentBatch { identity, .. } => identity.clone(),
            Self::ClaimTask { identity, .. }
            | Self::FinalizeStoppedTask { identity, .. }
            | Self::RecordReview { identity, .. }
            | Self::FinalizeReviewedTask { identity, .. }
            | Self::FinalizeUnreviewedTask { identity, .. } => {
                DurableOperationIdentity::TaskMutation(*identity)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnownNotAppliedReason {
    DeadlineBeforeStart,
    IngressClosed,
    IngressFull,
    BusyRolledBack,
    KnownRollback,
    ExactReconciliation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KnownNotAppliedError {
    Domain(DomainError),
    InvalidRepositoryId(String),
    InvalidTaskId(String),
    InvalidClientRequestId(String),
    InvalidTaskStatus(String),
    InvalidDeliveryReadiness(String),
    InvalidEventKind(String),
    InvalidEventSchemaVersion(i64),
    InvalidArtifactState(String),
    DatabaseSchemaUnsupported,
    DatabaseMigration(String),
    Json(String),
    IllegalTransition {
        from: TaskStatus,
        to: TaskStatus,
    },
    IdempotencyConflict,
    TaskNotFound,
    InvalidArtifactInput,
    ArtifactIdentityConflict,
    ArtifactNotFound,
    ArtifactStateConflict,
    TaskNotRetryable,
    InvalidRunningEvent,
    TaskAttemptOverflow,
    WalCheckpointIncomplete {
        busy: i64,
        log_frames: i64,
        checkpointed_frames: i64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeUnknownReason {
    CommitStatusUnknown,
    CompletionChannelClosed,
    NonReplayableOperation,
    ReconciliationFailed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
// This public disposition preserves one stable, directly matchable payload shape
// across every durable operation. Boxing only the replay branch would change the
// public API and every state-machine match for a layout-only optimization.
#[allow(clippy::large_enum_variant)]
pub enum DurableDisposition<T> {
    Confirmed(T),
    KnownNotApplied {
        reason: KnownNotAppliedReason,
        outcome: Option<T>,
        error: Option<KnownNotAppliedError>,
    },
    OutcomeUnknown {
        reason: OutcomeUnknownReason,
        pending: Option<PendingDurableResult>,
    },
    InvariantConflict {
        message: &'static str,
        outcome: Option<T>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableCompletion<T> {
    pub identity: DurableOperationIdentity,
    pub sequence_disposition: MutationSequenceDisposition,
    pub disposition: DurableDisposition<T>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationSequenceDisposition {
    RetainSame,
    AdvanceNext,
    BlockUnknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
// Receipts are moved through the bounded StoreWriter completion channel and
// matched exactly once. Keeping their public payloads direct avoids an API-wide
// ownership change for a bounded, short-lived value.
#[allow(clippy::large_enum_variant)]
pub enum PendingReplayReceipt {
    QueueLimitedCreate(QueueLimitedCreateTaskOutcome),
    QueueLimitedRetry(QueueLimitedRetryTaskOutcome),
    ClaimTask(ClaimTaskOutcome),
    PersistStopIntentBatch(StopIntentBatchReceipt),
    FinalizeStoppedTask(FinalizeStoppedTaskOutcome),
    RecordReview(RecordReviewOutcome),
    FinalizeReviewedTask(FinalizeReviewedTaskOutcome),
    FinalizeUnreviewedTask(FinalizeUnreviewedTaskOutcome),
}

impl PendingReplayReceipt {
    pub fn event_id(&self) -> Option<EventId> {
        match self {
            Self::QueueLimitedCreate(outcome) => match outcome {
                QueueLimitedCreateTaskOutcome::Created { event_id, .. } => Some(*event_id),
                QueueLimitedCreateTaskOutcome::Existing { task } => Some(task.last_event_id),
                QueueLimitedCreateTaskOutcome::QueueFull { .. } => None,
            },
            Self::QueueLimitedRetry(outcome) => match outcome {
                QueueLimitedRetryTaskOutcome::Created { event_id, .. } => Some(*event_id),
                QueueLimitedRetryTaskOutcome::Existing { task } => Some(task.last_event_id),
                QueueLimitedRetryTaskOutcome::QueueFull { .. } => None,
            },
            Self::ClaimTask(outcome) => {
                match outcome {
                    ClaimTaskOutcome::Applied(receipt)
                    | ClaimTaskOutcome::ExistingApplied(receipt) => Some(receipt.started_event_id),
                    ClaimTaskOutcome::KnownNotApplied { .. }
                    | ClaimTaskOutcome::InvariantConflict => None,
                }
            }
            Self::PersistStopIntentBatch(receipt) => receipt
                .items
                .iter()
                .filter_map(|item| match &item.outcome {
                    PersistStopIntentOutcome::TerminalWon { current } => {
                        Some(current.last_event_id)
                    }
                    PersistStopIntentOutcome::Applied(_)
                    | PersistStopIntentOutcome::Existing(_)
                    | PersistStopIntentOutcome::IntentConflict { .. } => None,
                })
                .max(),
            Self::FinalizeStoppedTask(outcome) => match outcome {
                FinalizeStoppedTaskOutcome::Applied(receipt)
                | FinalizeStoppedTaskOutcome::Existing(receipt) => Some(receipt.terminal_event_id),
                FinalizeStoppedTaskOutcome::InvariantConflict => None,
            },
            Self::RecordReview(outcome) => match outcome {
                RecordReviewOutcome::Applied { event_id, .. }
                | RecordReviewOutcome::Existing { event_id, .. } => Some(*event_id),
            },
            Self::FinalizeReviewedTask(outcome) => match outcome {
                FinalizeReviewedTaskOutcome::Applied {
                    terminal_event_id, ..
                }
                | FinalizeReviewedTaskOutcome::Existing {
                    terminal_event_id, ..
                } => Some(*terminal_event_id),
            },
            Self::FinalizeUnreviewedTask(outcome) => match outcome {
                FinalizeUnreviewedTaskOutcome::Applied { event_id, .. }
                | FinalizeUnreviewedTaskOutcome::Existing { event_id, .. } => Some(*event_id),
                FinalizeUnreviewedTaskOutcome::InvariantConflict => None,
            },
        }
    }

    pub fn has_stop_intent_conflict(&self) -> bool {
        matches!(self, Self::PersistStopIntentBatch(receipt) if receipt.items.iter().any(
            |item| matches!(item.outcome, PersistStopIntentOutcome::IntentConflict { .. })
        ))
    }
}
