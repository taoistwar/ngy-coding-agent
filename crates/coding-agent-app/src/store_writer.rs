use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::num::NonZeroU32;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coding_agent_domain::{
    EventId, NewRepository, NewReviewEvidence, NewTask, RepositoryId, TaskEventPayload,
    TaskFailure, TaskId, TaskStatus, UtcTimestamp,
};
#[cfg(feature = "test-support")]
use coding_agent_store::PersistStopIntentOutcome;
use coding_agent_store::{
    AppendEventOutcome, AttemptArtifactIdentity, ClaimTaskOutcome, ClaimTaskReconciliationOutcome,
    ClaimTaskRequest, CreateTaskOutcome, FinalizeReviewedTaskOutcome, FinalizeStoppedTaskOutcome,
    FinalizeStoppedTaskRequest, QueueLimitedCreateTaskOutcome, QueueLimitedRetryTaskOutcome,
    RecordReviewOutcome, RecoveryOutcome, RecoveryReceipt, RegisterRepositoryOutcome,
    ReserveAttemptArtifact, ReserveAttemptArtifactOutcome, RetryTaskOutcome,
    StopIntentBatchReceipt, StopIntentKind, StopIntentRequest, Store, StoreError, TaskTransition,
    TransitionOutcome, UpdateAttemptArtifactOutcome,
};
pub use coding_agent_store::{FinalizeUnreviewedTaskOutcome, FinalizeUnreviewedTaskRequest};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, sleep_until};

use crate::pending_durable::{
    DurableCompletion, DurableDisposition, DurableOperationIdentity, DurableOperationKind,
    KnownNotAppliedError, KnownNotAppliedReason, MutationSequenceDisposition, OutcomeUnknownReason,
    PendingDurableResult, PendingReplayReceipt, TaskMutationIdentity,
};

mod command;

pub use command::delivery::{
    DeliveryCleanupWriteCommand, DeliveryCleanupWriteOutcome, DeliveryCompletion,
    DeliveryDisposition, DeliveryMergeWriteCommand, DeliveryMergeWriteOutcome,
    DeliverySourceWriteCommand, DeliverySourceWriteOutcome, DeliverySubmission,
    DeliverySubmissionIdentity, DeliveryWriteCommand, DeliveryWriteOutcome,
};

pub(crate) use command::execution::sqlite_code_is_retryable;
#[cfg(test)]
use command::execution::{StoreFailureClassification, classify_store_failure};
use command::{
    WriteCommand,
    execution::{
        claim_outcome_from_reconciliation, classify_store_error, completed_transition_bypass_error,
        receive,
    },
    run_writer,
};

const RETRY_DELAYS: [Duration; 5] = [
    Duration::from_millis(25),
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(200),
    Duration::from_millis(400),
];
const COMPLETED_TRANSITION_BYPASS: &str =
    "Completed tasks must be committed through finalize_reviewed_task";

pub trait EventWake: Send + Sync + 'static {
    fn wake(&self);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteReceipt<T> {
    pub value: T,
    pub event_id: Option<EventId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordReviewRequest {
    pub task_id: TaskId,
    pub expected_repository_id: RepositoryId,
    pub expected_attempt: u32,
    pub evidence: NewReviewEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeReviewedTaskRequest {
    pub task_id: TaskId,
    pub expected_repository_id: RepositoryId,
    pub expected_attempt: u32,
    pub evidence: NewReviewEvidence,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreWriterError {
    #[error("SQLite writer remained busy through its bounded retry window")]
    Busy,
    #[error("the caller's absolute StoreWriter deadline elapsed")]
    DeadlineElapsed,
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("store writer is closed")]
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StoreWriterSubmitError {
    #[error("the selected StoreWriter ingress is full")]
    Full,
    #[error("the StoreWriter ingress is closed")]
    Closed,
    #[error("the StoreWriter mutation identity does not match its typed request")]
    InvalidIdentity,
    #[error("a per-task mutation sequence contains a gap")]
    SequenceGap,
    #[error("a per-task mutation sequence is duplicated or reversed")]
    SequenceReversed,
}

pub struct StoreWriterSubmission<T> {
    identity: DurableOperationIdentity,
    pending: Option<PendingDurableResult>,
    completion_channel_closed_reason: OutcomeUnknownReason,
    receiver: oneshot::Receiver<DurableCompletion<T>>,
}

impl<T> StoreWriterSubmission<T> {
    pub async fn completion(self) -> DurableCompletion<T> {
        match self.receiver.await {
            Ok(completion) => completion,
            Err(_) => DurableCompletion {
                identity: self.identity,
                sequence_disposition: MutationSequenceDisposition::BlockUnknown,
                disposition: DurableDisposition::OutcomeUnknown {
                    reason: self.completion_channel_closed_reason,
                    pending: self.pending,
                },
            },
        }
    }
}

pub enum PendingDurableSubmission {
    QueueLimitedCreate(StoreWriterSubmission<QueueLimitedCreateTaskOutcome>),
    QueueLimitedRetry(StoreWriterSubmission<QueueLimitedRetryTaskOutcome>),
    ClaimTask(StoreWriterSubmission<ClaimTaskOutcome>),
    ReconcileClaimTask(StoreWriterSubmission<ClaimTaskReconciliationOutcome>),
    PersistStopIntentBatch(StoreWriterSubmission<StopIntentBatchReceipt>),
    FinalizeStoppedTask(StoreWriterSubmission<FinalizeStoppedTaskOutcome>),
    RecordReview(StoreWriterSubmission<RecordReviewOutcome>),
    FinalizeReviewedTask(StoreWriterSubmission<FinalizeReviewedTaskOutcome>),
    FinalizeUnreviewedTask(StoreWriterSubmission<FinalizeUnreviewedTaskOutcome>),
}

impl PendingDurableSubmission {
    pub async fn completion(self) -> DurableCompletion<PendingReplayReceipt> {
        match self {
            Self::QueueLimitedCreate(submission) => map_pending_completion(
                submission.completion().await,
                PendingReplayReceipt::QueueLimitedCreate,
            ),
            Self::QueueLimitedRetry(submission) => map_pending_completion(
                submission.completion().await,
                PendingReplayReceipt::QueueLimitedRetry,
            ),
            Self::ClaimTask(submission) => map_pending_completion(
                submission.completion().await,
                PendingReplayReceipt::ClaimTask,
            ),
            Self::ReconcileClaimTask(submission) => {
                map_pending_completion(submission.completion().await, |outcome| {
                    PendingReplayReceipt::ClaimTask(claim_outcome_from_reconciliation(outcome))
                })
            }
            Self::PersistStopIntentBatch(submission) => map_pending_completion(
                submission.completion().await,
                PendingReplayReceipt::PersistStopIntentBatch,
            ),
            Self::FinalizeStoppedTask(submission) => map_pending_completion(
                submission.completion().await,
                PendingReplayReceipt::FinalizeStoppedTask,
            ),
            Self::RecordReview(submission) => map_pending_completion(
                submission.completion().await,
                PendingReplayReceipt::RecordReview,
            ),
            Self::FinalizeReviewedTask(submission) => map_pending_completion(
                submission.completion().await,
                PendingReplayReceipt::FinalizeReviewedTask,
            ),
            Self::FinalizeUnreviewedTask(submission) => map_pending_completion(
                submission.completion().await,
                PendingReplayReceipt::FinalizeUnreviewedTask,
            ),
        }
    }
}

fn map_pending_completion<T>(
    completion: DurableCompletion<T>,
    receipt: impl Fn(T) -> PendingReplayReceipt,
) -> DurableCompletion<PendingReplayReceipt> {
    let disposition = match completion.disposition {
        DurableDisposition::Confirmed(outcome) => DurableDisposition::Confirmed(receipt(outcome)),
        DurableDisposition::KnownNotApplied {
            reason,
            outcome,
            error,
        } => DurableDisposition::KnownNotApplied {
            reason,
            outcome: outcome.map(receipt),
            error,
        },
        DurableDisposition::OutcomeUnknown { reason, pending } => {
            DurableDisposition::OutcomeUnknown { reason, pending }
        }
        DurableDisposition::InvariantConflict { message, outcome } => {
            DurableDisposition::InvariantConflict {
                message,
                outcome: outcome.map(receipt),
            }
        }
    };
    DurableCompletion {
        identity: completion.identity,
        sequence_disposition: completion.sequence_disposition,
        disposition,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreWriterPriority {
    Normal,
    Urgent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StoreWriterSchedulingError {
    #[cfg(feature = "test-support")]
    #[error("the StoreWriter ingress is closed")]
    IngressClosed,
    #[cfg(feature = "test-support")]
    #[error("the StoreWriter mutation identity is invalid")]
    InvalidIdentity,
    #[error("the normal StoreWriter ingress is full")]
    NormalIngressFull,
    #[error("the urgent StoreWriter ingress is full")]
    UrgentIngressFull,
    #[cfg(feature = "test-support")]
    #[error("a per-task mutation sequence contains a gap")]
    SequenceGap,
    #[cfg(feature = "test-support")]
    #[error("a per-task mutation sequence is duplicated or reversed")]
    SequenceReversed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MutationIngressStatus {
    Admitted,
    Unresolved,
    Completed,
}

#[derive(Debug, Clone, Default)]
struct TaskIngressState {
    last_reserved: u64,
    completed_through: u64,
    states: BTreeMap<u64, MutationIngressStatus>,
}

#[derive(Debug, Clone, Default)]
struct IngressSequenceLedger {
    tasks: HashMap<TaskId, TaskIngressState>,
}

impl IngressSequenceLedger {
    fn accept(&mut self, items: &[TaskMutationIdentity]) -> Result<(), StoreWriterSubmitError> {
        let mut proposed = self.clone();
        for item in items {
            let state = proposed.tasks.entry(item.task_id).or_default();
            if state
                .states
                .values()
                .any(|status| *status == MutationIngressStatus::Unresolved)
            {
                return Err(StoreWriterSubmitError::SequenceGap);
            }
            let expected = state
                .last_reserved
                .checked_add(1)
                .ok_or(StoreWriterSubmitError::SequenceGap)?;
            match item.sequence.get().cmp(&expected) {
                std::cmp::Ordering::Equal => {
                    state.last_reserved = item.sequence.get();
                    state
                        .states
                        .insert(item.sequence.get(), MutationIngressStatus::Admitted);
                }
                std::cmp::Ordering::Greater => return Err(StoreWriterSubmitError::SequenceGap),
                std::cmp::Ordering::Less => return Err(StoreWriterSubmitError::SequenceReversed),
            }
        }
        *self = proposed;
        Ok(())
    }

    fn accept_reconciliation(
        &mut self,
        items: &[TaskMutationIdentity],
    ) -> Result<(), StoreWriterSubmitError> {
        let mut proposed = self.clone();
        for item in items {
            let Some(state) = proposed.tasks.get_mut(&item.task_id) else {
                return Err(StoreWriterSubmitError::SequenceGap);
            };
            match state.states.get_mut(&item.sequence.get()) {
                Some(status @ MutationIngressStatus::Unresolved) => {
                    *status = MutationIngressStatus::Admitted;
                }
                Some(MutationIngressStatus::Admitted | MutationIngressStatus::Completed) => {
                    return Err(StoreWriterSubmitError::SequenceReversed);
                }
                None if item.sequence.get() > state.last_reserved => {
                    return Err(StoreWriterSubmitError::SequenceGap);
                }
                None => return Err(StoreWriterSubmitError::SequenceReversed),
            }
        }
        *self = proposed;
        Ok(())
    }

    fn mark_unresolved(&mut self, items: &[TaskMutationIdentity]) {
        for item in items {
            let state = self.tasks.entry(item.task_id).or_default();
            state.last_reserved = state.last_reserved.max(item.sequence.get());
            state
                .states
                .insert(item.sequence.get(), MutationIngressStatus::Unresolved);
        }
    }

    fn resolve(
        &mut self,
        items: &[TaskMutationIdentity],
        disposition: MutationSequenceDisposition,
    ) {
        for item in items {
            let state = self.tasks.entry(item.task_id).or_default();
            match disposition {
                MutationSequenceDisposition::AdvanceNext => {
                    state
                        .states
                        .insert(item.sequence.get(), MutationIngressStatus::Completed);
                    while let Some(next) = state.completed_through.checked_add(1) {
                        if state.states.get(&next) != Some(&MutationIngressStatus::Completed) {
                            break;
                        }
                        state.completed_through = next;
                        state.states.remove(&next);
                    }
                }
                MutationSequenceDisposition::RetainSame
                | MutationSequenceDisposition::BlockUnknown => {
                    state
                        .states
                        .insert(item.sequence.get(), MutationIngressStatus::Unresolved);
                }
            }
        }
    }
}

struct MutationSequenceGuard {
    ledger: Arc<Mutex<IngressSequenceLedger>>,
    identities: Vec<TaskMutationIdentity>,
    resolved: bool,
}

impl MutationSequenceGuard {
    fn new(
        ledger: Arc<Mutex<IngressSequenceLedger>>,
        identities: Vec<TaskMutationIdentity>,
    ) -> Self {
        Self {
            ledger,
            identities,
            resolved: false,
        }
    }

    fn resolve(&mut self, disposition: MutationSequenceDisposition) {
        self.ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .resolve(&self.identities, disposition);
        self.resolved = true;
    }
}

impl Drop for MutationSequenceGuard {
    fn drop(&mut self) {
        if !self.resolved {
            self.ledger
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .mark_unresolved(&self.identities);
        }
    }
}

struct ScheduledCommand<T> {
    #[cfg(feature = "test-support")]
    priority: StoreWriterPriority,
    identities: Vec<TaskMutationIdentity>,
    value: T,
}

struct PriorityScheduler<T> {
    normal_capacity: usize,
    urgent_capacity: usize,
    reconciliation_capacity: usize,
    normal: VecDeque<ScheduledCommand<T>>,
    urgent: VecDeque<ScheduledCommand<T>>,
    reconciliation: VecDeque<ScheduledCommand<T>>,
    completed: HashMap<TaskId, u64>,
    urgent_streak: usize,
}

impl<T> PriorityScheduler<T> {
    fn new(normal_capacity: usize, urgent_capacity: usize) -> Self {
        assert!(normal_capacity > 0);
        assert!(urgent_capacity > 0);
        Self {
            normal_capacity,
            urgent_capacity,
            reconciliation_capacity: 1,
            normal: VecDeque::with_capacity(normal_capacity),
            urgent: VecDeque::with_capacity(urgent_capacity),
            reconciliation: VecDeque::with_capacity(1),
            completed: HashMap::new(),
            urgent_streak: 0,
        }
    }

    fn has_normal_capacity(&self) -> bool {
        self.normal.len() < self.normal_capacity
    }

    fn has_urgent_capacity(&self) -> bool {
        self.urgent.len() < self.urgent_capacity
    }

    fn has_reconciliation_capacity(&self) -> bool {
        self.reconciliation.len() < self.reconciliation_capacity
    }

    fn enqueue(
        &mut self,
        priority: StoreWriterPriority,
        identities: Vec<TaskMutationIdentity>,
        value: T,
    ) -> Result<(), StoreWriterSchedulingError> {
        let queue = match priority {
            StoreWriterPriority::Normal => {
                if !self.has_normal_capacity() {
                    return Err(StoreWriterSchedulingError::NormalIngressFull);
                }
                &mut self.normal
            }
            StoreWriterPriority::Urgent => {
                if !self.has_urgent_capacity() {
                    return Err(StoreWriterSchedulingError::UrgentIngressFull);
                }
                &mut self.urgent
            }
        };
        queue.push_back(ScheduledCommand {
            #[cfg(feature = "test-support")]
            priority,
            identities,
            value,
        });
        Ok(())
    }

    fn enqueue_reconciliation(
        &mut self,
        identities: Vec<TaskMutationIdentity>,
        value: T,
    ) -> Result<(), StoreWriterSchedulingError> {
        if !self.has_reconciliation_capacity() {
            return Err(StoreWriterSchedulingError::NormalIngressFull);
        }
        self.reconciliation.push_back(ScheduledCommand {
            #[cfg(feature = "test-support")]
            priority: StoreWriterPriority::Normal,
            identities,
            value,
        });
        Ok(())
    }

    fn pop_next(&mut self) -> Option<ScheduledCommand<T>> {
        let completed = &self.completed;
        let eligible = |scheduled: &ScheduledCommand<T>| {
            scheduled.identities.iter().all(|identity| {
                completed
                    .get(&identity.task_id)
                    .copied()
                    .unwrap_or(0)
                    .checked_add(1)
                    == Some(identity.sequence.get())
            })
        };
        if let Some(reconciliation) = self.reconciliation.iter().position(&eligible) {
            return self.reconciliation.remove(reconciliation);
        }
        let urgent = self.urgent.iter().position(&eligible);
        let normal = self.normal.iter().position(eligible);
        let choose_urgent = urgent.is_some() && (self.urgent_streak < 4 || normal.is_none());
        if choose_urgent {
            self.urgent_streak += 1;
            self.urgent.remove(urgent.expect("eligible urgent index"))
        } else if let Some(normal) = normal {
            self.urgent_streak = 0;
            self.normal.remove(normal)
        } else {
            None
        }
    }

    fn complete(&mut self, identities: &[TaskMutationIdentity], advance: bool) {
        if !advance {
            return;
        }
        for identity in identities {
            self.completed
                .insert(identity.task_id, identity.sequence.get());
        }
    }

    fn is_empty(&self) -> bool {
        self.normal.is_empty() && self.urgent.is_empty() && self.reconciliation.is_empty()
    }
}

#[cfg(feature = "test-support")]
pub struct StoreWriterSchedulingHarness {
    ingress: IngressSequenceLedger,
    scheduler: PriorityScheduler<()>,
}

#[cfg(feature = "test-support")]
impl StoreWriterSchedulingHarness {
    pub fn new(normal_capacity: usize, urgent_capacity: usize) -> Self {
        Self {
            ingress: IngressSequenceLedger::default(),
            scheduler: PriorityScheduler::new(normal_capacity, urgent_capacity),
        }
    }

    pub fn try_enqueue_normal(
        &mut self,
        identity: TaskMutationIdentity,
    ) -> Result<(), StoreWriterSchedulingError> {
        if !self.scheduler.has_normal_capacity() {
            return Err(StoreWriterSchedulingError::NormalIngressFull);
        }
        self.ingress.accept(&[identity]).map_err(scheduling_error)?;
        self.scheduler
            .enqueue(StoreWriterPriority::Normal, vec![identity], ())
    }

    pub fn try_enqueue_urgent(
        &mut self,
        identity: DurableOperationIdentity,
    ) -> Result<(), StoreWriterSchedulingError> {
        if !self.scheduler.has_urgent_capacity() {
            return Err(StoreWriterSchedulingError::UrgentIngressFull);
        }
        let items = task_mutation_identities(&identity);
        self.ingress.accept(&items).map_err(scheduling_error)?;
        self.scheduler
            .enqueue(StoreWriterPriority::Urgent, items, ())
    }

    pub fn pop_next(&mut self) -> Option<StoreWriterPriority> {
        let command = self.scheduler.pop_next()?;
        let priority = command.priority;
        self.scheduler.complete(&command.identities, true);
        Some(priority)
    }
}

#[cfg(feature = "test-support")]
fn scheduling_error(error: StoreWriterSubmitError) -> StoreWriterSchedulingError {
    match error {
        StoreWriterSubmitError::SequenceGap => StoreWriterSchedulingError::SequenceGap,
        StoreWriterSubmitError::SequenceReversed => StoreWriterSchedulingError::SequenceReversed,
        StoreWriterSubmitError::Full => StoreWriterSchedulingError::NormalIngressFull,
        StoreWriterSubmitError::Closed => StoreWriterSchedulingError::IngressClosed,
        StoreWriterSubmitError::InvalidIdentity => StoreWriterSchedulingError::InvalidIdentity,
    }
}

fn task_mutation_identities(identity: &DurableOperationIdentity) -> Vec<TaskMutationIdentity> {
    match identity {
        DurableOperationIdentity::TaskMutation(identity) => vec![*identity],
        DurableOperationIdentity::StopIntentBatch { items } => items.clone(),
        DurableOperationIdentity::CreateTask { .. }
        | DurableOperationIdentity::RetryTask { .. } => Vec::new(),
    }
}

#[derive(Debug, Clone)]
enum StoreWriterOperation {
    Delivery(Box<DeliveryWriteCommand>),
    RegisterRepository(NewRepository),
    CreateTask(NewTask),
    RetryTask(TaskId),
    QueueLimitedCreate {
        input: NewTask,
        max_queued_tasks: NonZeroU32,
    },
    QueueLimitedRetry {
        source_task_id: TaskId,
        max_queued_tasks: NonZeroU32,
    },
    ClaimTask(ClaimTaskRequest),
    ReconcileClaimTask(ClaimTaskRequest),
    PersistStopIntentBatch(Vec<StopIntentRequest>),
    FinalizeStoppedTask(FinalizeStoppedTaskRequest),
    TransitionWithEvent {
        task_id: TaskId,
        expected: TaskStatus,
        transition: TaskTransition,
    },
    AppendRunningEvent {
        task_id: TaskId,
        payload: TaskEventPayload,
    },
    RecordReview(RecordReviewRequest),
    FinalizeReviewedTask(FinalizeReviewedTaskRequest),
    FinalizeUnreviewedTask(FinalizeUnreviewedTaskRequest),
    ReserveAttemptArtifact(ReserveAttemptArtifact),
    MarkAttemptArtifactReady(AttemptArtifactIdentity),
    MarkAttemptArtifactInconsistent {
        identity: AttemptArtifactIdentity,
        failure_code: String,
    },
    InterruptRemainingAfterStops(TaskFailure),
    RecoverIncomplete {
        now: UtcTimestamp,
        failure: TaskFailure,
    },
}

#[derive(Debug)]
enum StoreWriterOperationOutcome {
    Delivery(Box<DeliveryWriteOutcome>),
    RegisterRepository(RegisterRepositoryOutcome),
    CreateTask(CreateTaskOutcome),
    RetryTask(RetryTaskOutcome),
    QueueLimitedCreate(QueueLimitedCreateTaskOutcome),
    QueueLimitedRetry(QueueLimitedRetryTaskOutcome),
    ClaimTask(ClaimTaskOutcome),
    ReconcileClaimTask(ClaimTaskReconciliationOutcome),
    PersistStopIntentBatch(StopIntentBatchReceipt),
    FinalizeStoppedTask(FinalizeStoppedTaskOutcome),
    TransitionWithEvent(TransitionOutcome),
    AppendRunningEvent(AppendEventOutcome),
    RecordReview(Box<RecordReviewOutcome>),
    FinalizeReviewedTask(Box<FinalizeReviewedTaskOutcome>),
    FinalizeUnreviewedTask(FinalizeUnreviewedTaskOutcome),
    ReserveAttemptArtifact(ReserveAttemptArtifactOutcome),
    UpdateAttemptArtifact(UpdateAttemptArtifactOutcome),
    InterruptRemainingAfterStops(RecoveryReceipt),
    RecoverIncomplete(RecoveryOutcome),
}

#[cfg(feature = "test-support")]
impl StoreWriterOperation {
    fn test_kind(&self) -> StoreWriterOperationKind {
        match self {
            Self::Delivery(command) => command.test_kind(),
            Self::RegisterRepository(_) => StoreWriterOperationKind::RegisterRepository,
            Self::CreateTask(_) => StoreWriterOperationKind::CreateTask,
            Self::RetryTask(_) => StoreWriterOperationKind::RetryTask,
            Self::QueueLimitedCreate { .. } => StoreWriterOperationKind::CreateTask,
            Self::QueueLimitedRetry { .. } => StoreWriterOperationKind::RetryTask,
            Self::ClaimTask(_) => StoreWriterOperationKind::StartTask,
            Self::ReconcileClaimTask(_) => StoreWriterOperationKind::ReconcileClaimTask,
            Self::PersistStopIntentBatch(_) => StoreWriterOperationKind::PersistStopIntentBatch,
            Self::FinalizeStoppedTask(_) => StoreWriterOperationKind::FinalizeStoppedTask,
            Self::TransitionWithEvent { transition, .. } => match transition {
                TaskTransition::Running => StoreWriterOperationKind::StartTask,
                TaskTransition::Completed | TaskTransition::Failed(_) => {
                    StoreWriterOperationKind::FinishTask
                }
                TaskTransition::Cancelled => StoreWriterOperationKind::CancelTask,
                TaskTransition::Interrupted(_) => StoreWriterOperationKind::InterruptTask,
            },
            Self::AppendRunningEvent { .. } => StoreWriterOperationKind::AppendRunningEvent,
            Self::RecordReview(_) => StoreWriterOperationKind::RecordReview,
            Self::FinalizeReviewedTask(_) => StoreWriterOperationKind::FinalizeReviewedTask,
            Self::FinalizeUnreviewedTask(request) => match request.transition {
                TaskTransition::Failed(_) => StoreWriterOperationKind::FinishTask,
                TaskTransition::Cancelled => StoreWriterOperationKind::CancelTask,
                TaskTransition::Running
                | TaskTransition::Completed
                | TaskTransition::Interrupted(_) => StoreWriterOperationKind::FinishTask,
            },
            Self::ReserveAttemptArtifact(_) => StoreWriterOperationKind::ReserveAttemptArtifact,
            Self::MarkAttemptArtifactReady(_) => StoreWriterOperationKind::MarkAttemptArtifactReady,
            Self::MarkAttemptArtifactInconsistent { .. } => {
                StoreWriterOperationKind::MarkAttemptArtifactInconsistent
            }
            Self::InterruptRemainingAfterStops(_) => {
                StoreWriterOperationKind::InterruptRemainingAfterStops
            }
            Self::RecoverIncomplete { .. } => StoreWriterOperationKind::RecoverIncomplete,
        }
    }
}

#[cfg(feature = "test-support")]
impl StoreWriterOperationOutcome {
    fn committed_durable_state(&self) -> bool {
        match self {
            Self::Delivery(outcome) => outcome.committed_durable_state(),
            Self::PersistStopIntentBatch(receipt) => receipt
                .items
                .iter()
                .any(|item| matches!(item.outcome, PersistStopIntentOutcome::Applied(_))),
            _ => self.has_durable_event(),
        }
    }

    fn has_durable_event(&self) -> bool {
        match self {
            Self::Delivery(_) => false,
            Self::RegisterRepository(_) => false,
            Self::ReserveAttemptArtifact(_) | Self::UpdateAttemptArtifact(_) => false,
            Self::PersistStopIntentBatch(_) => false,
            Self::CreateTask(CreateTaskOutcome::Created { .. })
            | Self::RetryTask(RetryTaskOutcome::Created { .. })
            | Self::QueueLimitedCreate(QueueLimitedCreateTaskOutcome::Created { .. })
            | Self::QueueLimitedRetry(QueueLimitedRetryTaskOutcome::Created { .. })
            | Self::ClaimTask(
                ClaimTaskOutcome::Applied(_) | ClaimTaskOutcome::ExistingApplied(_),
            )
            | Self::ReconcileClaimTask(ClaimTaskReconciliationOutcome::ExistingApplied(_))
            | Self::FinalizeStoppedTask(
                FinalizeStoppedTaskOutcome::Applied(_) | FinalizeStoppedTaskOutcome::Existing(_),
            )
            | Self::TransitionWithEvent(TransitionOutcome::Applied { .. })
            | Self::AppendRunningEvent(AppendEventOutcome::Applied { .. })
            | Self::RecordReview(_)
            | Self::FinalizeReviewedTask(_)
            | Self::FinalizeUnreviewedTask(FinalizeUnreviewedTaskOutcome::Applied { .. })
            | Self::FinalizeUnreviewedTask(FinalizeUnreviewedTaskOutcome::Existing { .. }) => true,
            Self::CreateTask(CreateTaskOutcome::Existing { .. })
            | Self::RetryTask(RetryTaskOutcome::Existing { .. })
            | Self::QueueLimitedCreate(
                QueueLimitedCreateTaskOutcome::Existing { .. }
                | QueueLimitedCreateTaskOutcome::QueueFull { .. },
            )
            | Self::QueueLimitedRetry(
                QueueLimitedRetryTaskOutcome::Existing { .. }
                | QueueLimitedRetryTaskOutcome::QueueFull { .. },
            )
            | Self::ClaimTask(
                ClaimTaskOutcome::KnownNotApplied { .. } | ClaimTaskOutcome::InvariantConflict,
            )
            | Self::ReconcileClaimTask(
                ClaimTaskReconciliationOutcome::KnownNotApplied { .. }
                | ClaimTaskReconciliationOutcome::InvariantConflict,
            )
            | Self::FinalizeStoppedTask(FinalizeStoppedTaskOutcome::InvariantConflict)
            | Self::TransitionWithEvent(TransitionOutcome::Conflict { .. })
            | Self::FinalizeUnreviewedTask(FinalizeUnreviewedTaskOutcome::InvariantConflict)
            | Self::AppendRunningEvent(AppendEventOutcome::NotRunning { .. }) => false,
            Self::InterruptRemainingAfterStops(receipt) => receipt.last_event_id.is_some(),
            Self::RecoverIncomplete(outcome) => outcome.last_event_id.is_some(),
        }
    }
}

type StoreWriterBackendFuture<'a> = Pin<
    Box<dyn Future<Output = Result<StoreWriterOperationOutcome, StoreWriterError>> + Send + 'a>,
>;

trait StoreWriterBackend: Send + Sync + 'static {
    fn execute(&self, operation: StoreWriterOperation) -> StoreWriterBackendFuture<'_>;
}

#[cfg(feature = "test-support")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreWriterFaultPoint {
    FailBeforeExecute,
    FailUnknownBeforeExecute,
    FailAfterCommitBeforeReply,
    BusyBeforeExecute,
    PauseBeforeExecute,
    PauseAfterCommitBeforeWake,
    DropWakeAfterCommit,
}

#[cfg(feature = "test-support")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreWriterOperationKind {
    AcceptWorktreeCleanup,
    RecordWorktreeUnlocked,
    EnterWorktreeRemovePending,
    CompleteWorktreeCleanup,
    RecordWorktreeCleanupFailure,
    ReconcileWorktreeCleanup,
    AcceptBranchCleanup,
    RefreshBranchCleanupTarget,
    CompleteBranchCleanup,
    RecordBranchCleanupFailure,
    ReconcileBranchCleanup,
    CreateMergePreflight,
    BindMergePreflightInputs,
    FailUnboundMergePreflight,
    MarkMergePreflightStale,
    RecordMergePreflightResult,
    AcceptMerge,
    EnterMergePending,
    CompleteMerge,
    BeginMergeAbort,
    CompleteMergeAbort,
    RecordMergeKnownFailure,
    ReconcileMerge,
    CreateDeliverySource,
    AdvanceDeliverySourceObject,
    CommitDeliverySource,
    RecordDeliverySourceRetry,
    ReconcileDeliverySource,
    RegisterRepository,
    CreateTask,
    RetryTask,
    PersistStopIntentBatch,
    FinalizeStoppedTask,
    StartTask,
    ReconcileClaimTask,
    FinishTask,
    CancelTask,
    InterruptTask,
    AppendRunningEvent,
    RecordReview,
    FinalizeReviewedTask,
    ReserveAttemptArtifact,
    MarkAttemptArtifactReady,
    MarkAttemptArtifactInconsistent,
    InterruptRemainingAfterStops,
    RecoverIncomplete,
}

#[cfg(feature = "test-support")]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreWriterFaultSpec {
    pub point: StoreWriterFaultPoint,
    #[serde(default)]
    pub operation: Option<StoreWriterOperationKind>,
    pub count: u32,
}

#[cfg(feature = "test-support")]
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StoreWriterTestConfigError {
    #[error("StoreWriter fault counts must be positive")]
    ZeroCount,
}

#[cfg(feature = "test-support")]
pub struct StoreWriterTestController {
    state: std::sync::Mutex<StoreWriterTestState>,
    generation: tokio::sync::watch::Sender<u64>,
    dropped_wakes: std::sync::atomic::AtomicUsize,
}

#[cfg(feature = "test-support")]
struct StoreWriterTestState {
    scripts: Vec<StoreWriterFaultScript>,
    hit_counts: std::collections::HashMap<(StoreWriterFaultPoint, StoreWriterOperationKind), u32>,
    pause_gates: std::collections::HashMap<StoreWriterFaultPoint, Arc<tokio::sync::Semaphore>>,
}

#[cfg(feature = "test-support")]
struct StoreWriterFaultScript {
    spec: StoreWriterFaultSpec,
    remaining: u32,
}

#[cfg(feature = "test-support")]
struct ConsumedStoreWriterFault {
    pause_gate: Option<Arc<tokio::sync::Semaphore>>,
}

#[cfg(feature = "test-support")]
impl StoreWriterTestController {
    pub fn try_new(
        faults: impl IntoIterator<Item = StoreWriterFaultSpec>,
    ) -> Result<Self, StoreWriterTestConfigError> {
        let mut scripts = Vec::new();
        let mut pause_gates = std::collections::HashMap::new();
        for spec in faults {
            if spec.count == 0 {
                return Err(StoreWriterTestConfigError::ZeroCount);
            }
            if matches!(
                spec.point,
                StoreWriterFaultPoint::PauseBeforeExecute
                    | StoreWriterFaultPoint::PauseAfterCommitBeforeWake
            ) {
                pause_gates
                    .entry(spec.point)
                    .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(0)));
            }
            let remaining = spec.count;
            scripts.push(StoreWriterFaultScript { spec, remaining });
        }
        let (generation, _) = tokio::sync::watch::channel(0);
        Ok(Self {
            state: std::sync::Mutex::new(StoreWriterTestState {
                scripts,
                hit_counts: std::collections::HashMap::new(),
                pause_gates,
            }),
            generation,
            dropped_wakes: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    pub fn arm_fault(&self, spec: StoreWriterFaultSpec) -> Result<(), StoreWriterTestConfigError> {
        if spec.count == 0 {
            return Err(StoreWriterTestConfigError::ZeroCount);
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(
            spec.point,
            StoreWriterFaultPoint::PauseBeforeExecute
                | StoreWriterFaultPoint::PauseAfterCommitBeforeWake
        ) {
            state
                .pause_gates
                .entry(spec.point)
                .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(0)));
        }
        let remaining = spec.count;
        state
            .scripts
            .push(StoreWriterFaultScript { spec, remaining });
        Ok(())
    }

    pub fn hit_count(
        &self,
        point: StoreWriterFaultPoint,
        operation: StoreWriterOperationKind,
    ) -> u32 {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .hit_counts
            .get(&(point, operation))
            .copied()
            .unwrap_or(0)
    }

    pub async fn wait_until_reached(&self, point: StoreWriterFaultPoint, expected: u32) {
        if expected == 0 {
            return;
        }
        let mut generation = self.generation.subscribe();
        loop {
            let observed = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .hit_counts
                .iter()
                .filter_map(|((observed_point, _), count)| {
                    (*observed_point == point).then_some(*count)
                })
                .sum::<u32>();
            if observed >= expected {
                return;
            }
            generation
                .changed()
                .await
                .expect("StoreWriter test controller remains alive while waiting");
        }
    }

    pub fn release(&self, point: StoreWriterFaultPoint) -> usize {
        let gate = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pause_gates
            .get(&point)
            .cloned();
        if let Some(gate) = gate {
            gate.add_permits(1);
            1
        } else {
            0
        }
    }

    fn consume(
        &self,
        point: StoreWriterFaultPoint,
        operation: StoreWriterOperationKind,
    ) -> Option<ConsumedStoreWriterFault> {
        let gate = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let script = state.scripts.iter_mut().find(|script| {
                script.remaining > 0
                    && script.spec.point == point
                    && script
                        .spec
                        .operation
                        .is_none_or(|expected| expected == operation)
            })?;
            script.remaining -= 1;
            *state.hit_counts.entry((point, operation)).or_default() += 1;
            state.pause_gates.get(&point).cloned()
        };
        self.generation.send_modify(|value| {
            *value = value.wrapping_add(1);
        });
        Some(ConsumedStoreWriterFault { pause_gate: gate })
    }

    async fn pause_if_scripted(
        &self,
        point: StoreWriterFaultPoint,
        operation: StoreWriterOperationKind,
    ) {
        let Some(consumed) = self.consume(point, operation) else {
            return;
        };
        consumed
            .pause_gate
            .expect("pause fault points install a release gate")
            .acquire_owned()
            .await
            .expect("StoreWriter test pause semaphore remains open")
            .forget();
    }

    fn mark_dropped_wake(&self) {
        self.dropped_wakes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn take_dropped_wake(&self) -> bool {
        self.dropped_wakes
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |remaining| remaining.checked_sub(1),
            )
            .is_ok()
    }
}

#[cfg(feature = "test-support")]
struct TestStoreWriterBackend {
    inner: Store,
    controller: Arc<StoreWriterTestController>,
}

#[cfg(feature = "test-support")]
impl StoreWriterBackend for TestStoreWriterBackend {
    fn execute(&self, operation: StoreWriterOperation) -> StoreWriterBackendFuture<'_> {
        Box::pin(async move {
            let kind = operation.test_kind();
            self.controller
                .pause_if_scripted(StoreWriterFaultPoint::PauseBeforeExecute, kind)
                .await;
            if self
                .controller
                .consume(StoreWriterFaultPoint::FailBeforeExecute, kind)
                .is_some()
            {
                return Err(StoreWriterError::Store(StoreError::InvariantViolation(
                    "injected test-support StoreWriter failure",
                )));
            }
            if self
                .controller
                .consume(StoreWriterFaultPoint::FailUnknownBeforeExecute, kind)
                .is_some()
            {
                return Err(StoreWriterError::Closed);
            }
            if self
                .controller
                .consume(StoreWriterFaultPoint::BusyBeforeExecute, kind)
                .is_some()
            {
                return Err(StoreWriterError::Busy);
            }
            let outcome = StoreWriterBackend::execute(&self.inner, operation).await?;
            if outcome.committed_durable_state() {
                self.controller
                    .pause_if_scripted(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, kind)
                    .await;
            }
            if self
                .controller
                .consume(StoreWriterFaultPoint::FailAfterCommitBeforeReply, kind)
                .is_some()
            {
                return Err(StoreWriterError::Closed);
            }
            if outcome.has_durable_event()
                && self
                    .controller
                    .consume(StoreWriterFaultPoint::DropWakeAfterCommit, kind)
                    .is_some()
            {
                self.controller.mark_dropped_wake();
            }
            Ok(outcome)
        })
    }
}

#[cfg(feature = "test-support")]
struct TestEventWake {
    inner: Arc<dyn EventWake>,
    controller: Arc<StoreWriterTestController>,
}

#[cfg(feature = "test-support")]
impl EventWake for TestEventWake {
    fn wake(&self) {
        if !self.controller.take_dropped_wake() {
            self.inner.wake();
        }
    }
}

impl StoreWriterBackend for Store {
    fn execute(&self, operation: StoreWriterOperation) -> StoreWriterBackendFuture<'_> {
        Box::pin(async move {
            let result = match operation {
                StoreWriterOperation::Delivery(delivery_command) => {
                    command::delivery::execute_store(self, *delivery_command)
                        .await
                        .map(|outcome| StoreWriterOperationOutcome::Delivery(Box::new(outcome)))
                }
                StoreWriterOperation::RegisterRepository(input) => self
                    .register_repository(input)
                    .await
                    .map(StoreWriterOperationOutcome::RegisterRepository),
                StoreWriterOperation::CreateTask(input) => self
                    .create_task(input)
                    .await
                    .map(StoreWriterOperationOutcome::CreateTask),
                StoreWriterOperation::RetryTask(task_id) => self
                    .retry_task(task_id)
                    .await
                    .map(StoreWriterOperationOutcome::RetryTask),
                StoreWriterOperation::QueueLimitedCreate {
                    input,
                    max_queued_tasks,
                } => self
                    .create_task_with_queue_limit(input, max_queued_tasks)
                    .await
                    .map(StoreWriterOperationOutcome::QueueLimitedCreate),
                StoreWriterOperation::QueueLimitedRetry {
                    source_task_id,
                    max_queued_tasks,
                } => self
                    .retry_task_with_queue_limit(source_task_id, max_queued_tasks)
                    .await
                    .map(StoreWriterOperationOutcome::QueueLimitedRetry),
                StoreWriterOperation::ClaimTask(request) => self
                    .claim_task(request)
                    .await
                    .map(StoreWriterOperationOutcome::ClaimTask),
                StoreWriterOperation::ReconcileClaimTask(request) => self
                    .reconcile_task_claim(&request)
                    .await
                    .map(StoreWriterOperationOutcome::ReconcileClaimTask),
                StoreWriterOperation::PersistStopIntentBatch(requests) => self
                    .persist_stop_intent_batch(requests)
                    .await
                    .map(StoreWriterOperationOutcome::PersistStopIntentBatch),
                StoreWriterOperation::FinalizeStoppedTask(request) => self
                    .finalize_stopped_task(request)
                    .await
                    .map(StoreWriterOperationOutcome::FinalizeStoppedTask),
                StoreWriterOperation::TransitionWithEvent {
                    task_id,
                    expected,
                    transition,
                } => {
                    if matches!(transition, TaskTransition::Completed) {
                        Err(completed_transition_bypass_error())
                    } else {
                        self.transition_with_event(task_id, expected, transition)
                            .await
                            .map(StoreWriterOperationOutcome::TransitionWithEvent)
                    }
                }
                StoreWriterOperation::AppendRunningEvent { task_id, payload } => self
                    .append_running_event(task_id, payload)
                    .await
                    .map(StoreWriterOperationOutcome::AppendRunningEvent),
                StoreWriterOperation::RecordReview(request) => self
                    .record_review(
                        request.task_id,
                        request.expected_repository_id,
                        request.expected_attempt,
                        request.evidence,
                    )
                    .await
                    .map(|outcome| StoreWriterOperationOutcome::RecordReview(Box::new(outcome))),
                StoreWriterOperation::FinalizeReviewedTask(request) => self
                    .finalize_reviewed_task(
                        request.task_id,
                        request.expected_repository_id,
                        request.expected_attempt,
                        request.evidence,
                    )
                    .await
                    .map(|outcome| {
                        StoreWriterOperationOutcome::FinalizeReviewedTask(Box::new(outcome))
                    }),
                StoreWriterOperation::FinalizeUnreviewedTask(request) => self
                    .finalize_unreviewed_task(request)
                    .await
                    .map(StoreWriterOperationOutcome::FinalizeUnreviewedTask),
                StoreWriterOperation::ReserveAttemptArtifact(input) => self
                    .reserve_attempt_artifact(input)
                    .await
                    .map(StoreWriterOperationOutcome::ReserveAttemptArtifact),
                StoreWriterOperation::MarkAttemptArtifactReady(identity) => self
                    .mark_attempt_artifact_ready(identity)
                    .await
                    .map(StoreWriterOperationOutcome::UpdateAttemptArtifact),
                StoreWriterOperation::MarkAttemptArtifactInconsistent {
                    identity,
                    failure_code,
                } => self
                    .mark_attempt_artifact_inconsistent(identity, failure_code)
                    .await
                    .map(StoreWriterOperationOutcome::UpdateAttemptArtifact),
                StoreWriterOperation::InterruptRemainingAfterStops(failure) => self
                    .interrupt_remaining_after_stops(failure)
                    .await
                    .map(StoreWriterOperationOutcome::InterruptRemainingAfterStops),
                StoreWriterOperation::RecoverIncomplete { now, failure } => self
                    .recover_incomplete(now, failure)
                    .await
                    .map(StoreWriterOperationOutcome::RecoverIncomplete),
            };
            result.map_err(classify_store_error)
        })
    }
}

#[derive(Clone)]
pub struct StoreWriterHandle {
    sender: mpsc::Sender<WriteCommand>,
    urgent_sender: mpsc::Sender<WriteCommand>,
    reconciliation_sender: mpsc::Sender<WriteCommand>,
    ingress_sequences: Arc<Mutex<IngressSequenceLedger>>,
}

impl StoreWriterHandle {
    pub fn spawn(store: Store, wake: Arc<dyn EventWake>, capacity: usize) -> Self {
        Self::spawn_with_backend(Arc::new(store), wake, capacity)
    }

    #[cfg(feature = "test-support")]
    pub fn spawn_with_test_controller(
        store: Store,
        wake: Arc<dyn EventWake>,
        capacity: usize,
        controller: Arc<StoreWriterTestController>,
    ) -> Self {
        let backend = Arc::new(TestStoreWriterBackend {
            inner: store,
            controller: controller.clone(),
        });
        let wake = Arc::new(TestEventWake {
            inner: wake,
            controller,
        });
        Self::spawn_with_backend(backend, wake, capacity)
    }

    #[cfg(feature = "test-support")]
    pub fn closed_for_test() -> Self {
        let (sender, normal_receiver) = mpsc::channel(1);
        let (urgent_sender, urgent_receiver) = mpsc::channel(1);
        let (reconciliation_sender, reconciliation_receiver) = mpsc::channel(1);
        drop(normal_receiver);
        drop(urgent_receiver);
        drop(reconciliation_receiver);
        Self {
            sender,
            urgent_sender,
            reconciliation_sender,
            ingress_sequences: Arc::new(Mutex::new(IngressSequenceLedger::default())),
        }
    }

    fn spawn_with_backend(
        backend: Arc<dyn StoreWriterBackend>,
        wake: Arc<dyn EventWake>,
        capacity: usize,
    ) -> Self {
        assert!(
            capacity > 0,
            "store-writer channel capacity must be positive"
        );
        let (sender, normal_receiver) = mpsc::channel(capacity);
        let (urgent_sender, urgent_receiver) = mpsc::channel(capacity);
        let (reconciliation_sender, reconciliation_receiver) = mpsc::channel(1);
        tokio::spawn(run_writer(
            normal_receiver,
            urgent_receiver,
            reconciliation_receiver,
            backend,
            wake,
            capacity,
        ));
        Self {
            sender,
            urgent_sender,
            reconciliation_sender,
            ingress_sequences: Arc::new(Mutex::new(IngressSequenceLedger::default())),
        }
    }

    pub fn submit_delivery(
        &self,
        command: DeliveryWriteCommand,
        deadline: Instant,
    ) -> DeliverySubmission {
        self.submit_delivery_on(command, deadline, false)
    }

    /// Reconciles an unknown delivery write by submitting the same typed request
    /// to the Store's receipt/journal query-first transaction on the dedicated lane.
    pub fn reconcile_delivery(
        &self,
        command: DeliveryWriteCommand,
        deadline: Instant,
    ) -> DeliverySubmission {
        self.submit_delivery_on(command, deadline, true)
    }

    fn submit_delivery_on(
        &self,
        command: DeliveryWriteCommand,
        deadline: Instant,
        reconciliation_lane: bool,
    ) -> DeliverySubmission {
        let identity = DeliverySubmissionIdentity::for_command(&command);
        let (response, receiver) = oneshot::channel();
        match self.reserve_normal(&[], reconciliation_lane) {
            Ok(permit) => {
                permit.send(WriteCommand::Delivery {
                    identity: identity.clone(),
                    command: command.clone(),
                    deadline,
                    reconciliation_lane,
                    response,
                });
            }
            Err(error) => send_delivery_ingress_rejection(
                response,
                identity.clone(),
                command.clone(),
                error,
                reconciliation_lane,
            ),
        }
        DeliverySubmission {
            identity,
            pending_command: command,
            completion_channel_closed_reason: completion_channel_closed_reason(reconciliation_lane),
            receiver,
        }
    }

    pub fn submit_queue_limited_create(
        &self,
        input: NewTask,
        max_queued_tasks: NonZeroU32,
        deadline: Instant,
    ) -> Result<StoreWriterSubmission<QueueLimitedCreateTaskOutcome>, StoreWriterSubmitError> {
        self.submit_queue_limited_create_on(input, max_queued_tasks, deadline, false)
    }

    fn submit_queue_limited_create_on(
        &self,
        input: NewTask,
        max_queued_tasks: NonZeroU32,
        deadline: Instant,
        reconciliation_lane: bool,
    ) -> Result<StoreWriterSubmission<QueueLimitedCreateTaskOutcome>, StoreWriterSubmitError> {
        let identity = DurableOperationIdentity::CreateTask {
            client_request_id: input.client_request_id,
        };
        let pending = PendingDurableResult::QueueLimitedCreate {
            identity: identity.clone(),
            input,
            max_queued_tasks,
        };
        let (response, receiver) = oneshot::channel();
        match self.reserve_normal(&[], reconciliation_lane) {
            Ok(permit) => {
                permit.send(WriteCommand::QueueLimitedCreate {
                    identity: identity.clone(),
                    input: match &pending {
                        PendingDurableResult::QueueLimitedCreate { input, .. } => input.clone(),
                        _ => unreachable!("constructed queue-limited create pending"),
                    },
                    max_queued_tasks,
                    deadline,
                    pending: pending.clone(),
                    reconciliation_lane,
                    response,
                });
            }
            Err(error @ (StoreWriterSubmitError::Full | StoreWriterSubmitError::Closed)) => {
                send_ingress_rejection(
                    response,
                    identity.clone(),
                    &pending,
                    error,
                    reconciliation_lane,
                );
            }
            Err(error) => return Err(error),
        }
        Ok(StoreWriterSubmission {
            identity,
            pending: Some(pending),
            completion_channel_closed_reason: completion_channel_closed_reason(reconciliation_lane),
            receiver,
        })
    }

    pub fn submit_queue_limited_retry(
        &self,
        source_task_id: TaskId,
        max_queued_tasks: NonZeroU32,
        deadline: Instant,
    ) -> Result<StoreWriterSubmission<QueueLimitedRetryTaskOutcome>, StoreWriterSubmitError> {
        self.submit_queue_limited_retry_on(source_task_id, max_queued_tasks, deadline, false)
    }

    fn submit_queue_limited_retry_on(
        &self,
        source_task_id: TaskId,
        max_queued_tasks: NonZeroU32,
        deadline: Instant,
        reconciliation_lane: bool,
    ) -> Result<StoreWriterSubmission<QueueLimitedRetryTaskOutcome>, StoreWriterSubmitError> {
        let identity = DurableOperationIdentity::RetryTask { source_task_id };
        let pending = PendingDurableResult::QueueLimitedRetry {
            identity: identity.clone(),
            source_task_id,
            max_queued_tasks,
        };
        let (response, receiver) = oneshot::channel();
        match self.reserve_normal(&[], reconciliation_lane) {
            Ok(permit) => {
                permit.send(WriteCommand::QueueLimitedRetry {
                    identity: identity.clone(),
                    source_task_id,
                    max_queued_tasks,
                    deadline,
                    pending: pending.clone(),
                    reconciliation_lane,
                    response,
                });
            }
            Err(error @ (StoreWriterSubmitError::Full | StoreWriterSubmitError::Closed)) => {
                send_ingress_rejection(
                    response,
                    identity.clone(),
                    &pending,
                    error,
                    reconciliation_lane,
                );
            }
            Err(error) => return Err(error),
        }
        Ok(StoreWriterSubmission {
            identity,
            pending: Some(pending),
            completion_channel_closed_reason: completion_channel_closed_reason(reconciliation_lane),
            receiver,
        })
    }

    pub fn submit_claim_task(
        &self,
        identity: TaskMutationIdentity,
        request: ClaimTaskRequest,
        deadline: Instant,
    ) -> Result<StoreWriterSubmission<ClaimTaskOutcome>, StoreWriterSubmitError> {
        self.submit_claim_task_on(identity, request, deadline, false)
    }

    fn submit_claim_task_on(
        &self,
        identity: TaskMutationIdentity,
        request: ClaimTaskRequest,
        deadline: Instant,
        reconciliation_lane: bool,
    ) -> Result<StoreWriterSubmission<ClaimTaskOutcome>, StoreWriterSubmitError> {
        if identity.kind != DurableOperationKind::ClaimTask || identity.task_id != request.task_id {
            return Err(StoreWriterSubmitError::InvalidIdentity);
        }
        let operation = DurableOperationIdentity::TaskMutation(identity);
        let pending = PendingDurableResult::ClaimTask { identity, request };
        let (response, receiver) = oneshot::channel();
        match self.reserve_normal(&[identity], reconciliation_lane) {
            Ok(permit) => {
                permit.send(WriteCommand::ClaimTask {
                    identity: operation.clone(),
                    sequence_guard: self.sequence_guard(&[identity]),
                    request: match &pending {
                        PendingDurableResult::ClaimTask { request, .. } => request.clone(),
                        _ => unreachable!("constructed claim pending"),
                    },
                    deadline,
                    pending: pending.clone(),
                    reconciliation_lane,
                    response,
                });
            }
            Err(error @ (StoreWriterSubmitError::Full | StoreWriterSubmitError::Closed)) => {
                send_ingress_rejection(
                    response,
                    operation.clone(),
                    &pending,
                    error,
                    reconciliation_lane,
                );
            }
            Err(error) => return Err(error),
        }
        Ok(StoreWriterSubmission {
            identity: operation,
            pending: Some(pending),
            completion_channel_closed_reason: completion_channel_closed_reason(reconciliation_lane),
            receiver,
        })
    }

    /// Reconciles a previously admitted claim sequence without attempting the
    /// claim mutation again.
    ///
    /// This command is deliberately typed separately from `submit_claim_task`:
    /// an unknown claim may only be resolved by the store's read-only exact
    /// tuple query, so reconciliation can never newly transition a queued task
    /// to running.
    pub fn submit_reconcile_claim_task(
        &self,
        identity: TaskMutationIdentity,
        request: ClaimTaskRequest,
        deadline: Instant,
    ) -> Result<StoreWriterSubmission<ClaimTaskReconciliationOutcome>, StoreWriterSubmitError> {
        if identity.kind != DurableOperationKind::ClaimTask || identity.task_id != request.task_id {
            return Err(StoreWriterSubmitError::InvalidIdentity);
        }
        let operation = DurableOperationIdentity::TaskMutation(identity);
        let pending = PendingDurableResult::ClaimTask { identity, request };
        let (response, receiver) = oneshot::channel();
        match self.reserve_normal(&[identity], true) {
            Ok(permit) => {
                permit.send(WriteCommand::ReconcileClaimTask {
                    identity: operation.clone(),
                    sequence_guard: self.sequence_guard(&[identity]),
                    request: match &pending {
                        PendingDurableResult::ClaimTask { request, .. } => request.clone(),
                        _ => unreachable!("constructed claim reconciliation pending"),
                    },
                    deadline,
                    pending: pending.clone(),
                    response,
                });
            }
            Err(error @ (StoreWriterSubmitError::Full | StoreWriterSubmitError::Closed)) => {
                send_ingress_rejection(response, operation.clone(), &pending, error, true);
            }
            Err(error) => return Err(error),
        }
        Ok(StoreWriterSubmission {
            identity: operation,
            pending: Some(pending),
            completion_channel_closed_reason: OutcomeUnknownReason::ReconciliationFailed,
            receiver,
        })
    }

    pub fn submit_stop_intent_batch(
        &self,
        identity: DurableOperationIdentity,
        requests: Vec<StopIntentRequest>,
        deadline: Instant,
    ) -> Result<StoreWriterSubmission<StopIntentBatchReceipt>, StoreWriterSubmitError> {
        self.submit_stop_intent_batch_on(identity, requests, deadline, false, true)
    }

    pub fn submit_user_stop_intent(
        &self,
        identity: TaskMutationIdentity,
        request: StopIntentRequest,
        deadline: Instant,
    ) -> Result<StoreWriterSubmission<StopIntentBatchReceipt>, StoreWriterSubmitError> {
        if identity.kind != DurableOperationKind::PersistStopIntent
            || identity.task_id != request.task_id
            || request.kind != StopIntentKind::UserCancelled
        {
            return Err(StoreWriterSubmitError::InvalidIdentity);
        }
        let identity = DurableOperationIdentity::stop_intent_batch(vec![identity])
            .map_err(|_| StoreWriterSubmitError::InvalidIdentity)?;
        self.submit_stop_intent_batch_on(identity, vec![request], deadline, false, false)
    }

    fn submit_stop_intent_batch_on(
        &self,
        identity: DurableOperationIdentity,
        requests: Vec<StopIntentRequest>,
        deadline: Instant,
        reconciliation_lane: bool,
        urgent: bool,
    ) -> Result<StoreWriterSubmission<StopIntentBatchReceipt>, StoreWriterSubmitError> {
        validate_stop_batch_identity(&identity, &requests)?;
        let pending = PendingDurableResult::PersistStopIntentBatch {
            identity: identity.clone(),
            requests,
        };
        let (response, receiver) = oneshot::channel();
        let mutation_identities = task_mutation_identities(&identity);
        let reservation = if urgent {
            self.reserve_urgent(&mutation_identities, reconciliation_lane)
        } else {
            self.reserve_normal(&mutation_identities, reconciliation_lane)
        };
        match reservation {
            Ok(permit) => {
                permit.send(WriteCommand::PersistStopIntentBatch {
                    identity: identity.clone(),
                    sequence_guard: self.sequence_guard(&mutation_identities),
                    requests: match &pending {
                        PendingDurableResult::PersistStopIntentBatch { requests, .. } => {
                            requests.clone()
                        }
                        _ => unreachable!("constructed stop-intent pending"),
                    },
                    deadline,
                    pending: pending.clone(),
                    reconciliation_lane,
                    response,
                });
            }
            Err(error @ (StoreWriterSubmitError::Full | StoreWriterSubmitError::Closed)) => {
                send_ingress_rejection(
                    response,
                    identity.clone(),
                    &pending,
                    error,
                    reconciliation_lane,
                );
            }
            Err(error) => return Err(error),
        }
        Ok(StoreWriterSubmission {
            identity,
            pending: Some(pending),
            completion_channel_closed_reason: completion_channel_closed_reason(reconciliation_lane),
            receiver,
        })
    }

    pub fn submit_finalize_stopped_task(
        &self,
        identity: TaskMutationIdentity,
        request: FinalizeStoppedTaskRequest,
        deadline: Instant,
    ) -> Result<StoreWriterSubmission<FinalizeStoppedTaskOutcome>, StoreWriterSubmitError> {
        self.submit_finalize_stopped_task_on(identity, request, deadline, false)
    }

    fn submit_finalize_stopped_task_on(
        &self,
        identity: TaskMutationIdentity,
        request: FinalizeStoppedTaskRequest,
        deadline: Instant,
        reconciliation_lane: bool,
    ) -> Result<StoreWriterSubmission<FinalizeStoppedTaskOutcome>, StoreWriterSubmitError> {
        if identity.kind != DurableOperationKind::FinalizeStoppedTask
            || identity.task_id != request.task_id
        {
            return Err(StoreWriterSubmitError::InvalidIdentity);
        }
        let operation = DurableOperationIdentity::TaskMutation(identity);
        let pending = PendingDurableResult::FinalizeStoppedTask { identity, request };
        let (response, receiver) = oneshot::channel();
        match self.reserve_normal(&[identity], reconciliation_lane) {
            Ok(permit) => {
                permit.send(WriteCommand::FinalizeStoppedTask {
                    identity: operation.clone(),
                    sequence_guard: self.sequence_guard(&[identity]),
                    request,
                    deadline,
                    pending: pending.clone(),
                    reconciliation_lane,
                    response,
                });
            }
            Err(error @ (StoreWriterSubmitError::Full | StoreWriterSubmitError::Closed)) => {
                send_ingress_rejection(
                    response,
                    operation.clone(),
                    &pending,
                    error,
                    reconciliation_lane,
                );
            }
            Err(error) => return Err(error),
        }
        Ok(StoreWriterSubmission {
            identity: operation,
            pending: Some(pending),
            completion_channel_closed_reason: completion_channel_closed_reason(reconciliation_lane),
            receiver,
        })
    }

    pub fn submit_record_review(
        &self,
        identity: TaskMutationIdentity,
        request: RecordReviewRequest,
        deadline: Instant,
    ) -> Result<StoreWriterSubmission<RecordReviewOutcome>, StoreWriterSubmitError> {
        self.submit_record_review_on(identity, request, deadline, false)
    }

    fn submit_record_review_on(
        &self,
        identity: TaskMutationIdentity,
        request: RecordReviewRequest,
        deadline: Instant,
        reconciliation_lane: bool,
    ) -> Result<StoreWriterSubmission<RecordReviewOutcome>, StoreWriterSubmitError> {
        if identity.kind != DurableOperationKind::RecordReview
            || identity.task_id != request.task_id
        {
            return Err(StoreWriterSubmitError::InvalidIdentity);
        }
        let operation = DurableOperationIdentity::TaskMutation(identity);
        let pending = PendingDurableResult::RecordReview { identity, request };
        let (response, receiver) = oneshot::channel();
        match self.reserve_normal(&[identity], reconciliation_lane) {
            Ok(permit) => {
                permit.send(WriteCommand::TypedRecordReview {
                    identity: operation.clone(),
                    sequence_guard: self.sequence_guard(&[identity]),
                    request: match &pending {
                        PendingDurableResult::RecordReview { request, .. } => request.clone(),
                        _ => unreachable!("constructed review pending"),
                    },
                    deadline,
                    pending: pending.clone(),
                    reconciliation_lane,
                    response,
                });
            }
            Err(error @ (StoreWriterSubmitError::Full | StoreWriterSubmitError::Closed)) => {
                send_ingress_rejection(
                    response,
                    operation.clone(),
                    &pending,
                    error,
                    reconciliation_lane,
                );
            }
            Err(error) => return Err(error),
        }
        Ok(StoreWriterSubmission {
            identity: operation,
            pending: Some(pending),
            completion_channel_closed_reason: completion_channel_closed_reason(reconciliation_lane),
            receiver,
        })
    }

    pub fn submit_finalize_reviewed_task(
        &self,
        identity: TaskMutationIdentity,
        request: FinalizeReviewedTaskRequest,
        deadline: Instant,
    ) -> Result<StoreWriterSubmission<FinalizeReviewedTaskOutcome>, StoreWriterSubmitError> {
        self.submit_finalize_reviewed_task_on(identity, request, deadline, false)
    }

    fn submit_finalize_reviewed_task_on(
        &self,
        identity: TaskMutationIdentity,
        request: FinalizeReviewedTaskRequest,
        deadline: Instant,
        reconciliation_lane: bool,
    ) -> Result<StoreWriterSubmission<FinalizeReviewedTaskOutcome>, StoreWriterSubmitError> {
        if identity.kind != DurableOperationKind::FinalizeReviewedTask
            || identity.task_id != request.task_id
        {
            return Err(StoreWriterSubmitError::InvalidIdentity);
        }
        let operation = DurableOperationIdentity::TaskMutation(identity);
        let pending = PendingDurableResult::FinalizeReviewedTask { identity, request };
        let (response, receiver) = oneshot::channel();
        match self.reserve_normal(&[identity], reconciliation_lane) {
            Ok(permit) => {
                permit.send(WriteCommand::TypedFinalizeReviewedTask {
                    identity: operation.clone(),
                    sequence_guard: self.sequence_guard(&[identity]),
                    request: match &pending {
                        PendingDurableResult::FinalizeReviewedTask { request, .. } => {
                            request.clone()
                        }
                        _ => unreachable!("constructed finalization pending"),
                    },
                    deadline,
                    pending: pending.clone(),
                    reconciliation_lane,
                    response,
                });
            }
            Err(error @ (StoreWriterSubmitError::Full | StoreWriterSubmitError::Closed)) => {
                send_ingress_rejection(
                    response,
                    operation.clone(),
                    &pending,
                    error,
                    reconciliation_lane,
                );
            }
            Err(error) => return Err(error),
        }
        Ok(StoreWriterSubmission {
            identity: operation,
            pending: Some(pending),
            completion_channel_closed_reason: completion_channel_closed_reason(reconciliation_lane),
            receiver,
        })
    }

    pub fn submit_finalize_unreviewed_task(
        &self,
        identity: TaskMutationIdentity,
        request: FinalizeUnreviewedTaskRequest,
        deadline: Instant,
    ) -> Result<StoreWriterSubmission<FinalizeUnreviewedTaskOutcome>, StoreWriterSubmitError> {
        self.submit_finalize_unreviewed_task_on(identity, request, deadline, false)
    }

    fn submit_finalize_unreviewed_task_on(
        &self,
        identity: TaskMutationIdentity,
        request: FinalizeUnreviewedTaskRequest,
        deadline: Instant,
        reconciliation_lane: bool,
    ) -> Result<StoreWriterSubmission<FinalizeUnreviewedTaskOutcome>, StoreWriterSubmitError> {
        if identity.kind != DurableOperationKind::FinalizeUnreviewedTask
            || identity.task_id != request.task_id
            || !matches!(
                request.transition,
                TaskTransition::Failed(_) | TaskTransition::Cancelled
            )
        {
            return Err(StoreWriterSubmitError::InvalidIdentity);
        }
        let operation = DurableOperationIdentity::TaskMutation(identity);
        let pending = PendingDurableResult::FinalizeUnreviewedTask { identity, request };
        let (response, receiver) = oneshot::channel();
        match self.reserve_normal(&[identity], reconciliation_lane) {
            Ok(permit) => {
                permit.send(WriteCommand::TypedFinalizeUnreviewedTask {
                    identity: operation.clone(),
                    sequence_guard: self.sequence_guard(&[identity]),
                    request: match &pending {
                        PendingDurableResult::FinalizeUnreviewedTask { request, .. } => {
                            request.clone()
                        }
                        _ => unreachable!("constructed unreviewed finalization pending"),
                    },
                    deadline,
                    pending: pending.clone(),
                    reconciliation_lane,
                    response,
                });
            }
            Err(error @ (StoreWriterSubmitError::Full | StoreWriterSubmitError::Closed)) => {
                send_ingress_rejection(
                    response,
                    operation.clone(),
                    &pending,
                    error,
                    reconciliation_lane,
                );
            }
            Err(error) => return Err(error),
        }
        Ok(StoreWriterSubmission {
            identity: operation,
            pending: Some(pending),
            completion_channel_closed_reason: completion_channel_closed_reason(reconciliation_lane),
            receiver,
        })
    }

    pub fn submit_append_running_event(
        &self,
        identity: TaskMutationIdentity,
        payload: TaskEventPayload,
        deadline: Instant,
    ) -> Result<StoreWriterSubmission<AppendEventOutcome>, StoreWriterSubmitError> {
        if identity.kind != DurableOperationKind::AppendRunningEvent {
            return Err(StoreWriterSubmitError::InvalidIdentity);
        }
        let operation = DurableOperationIdentity::TaskMutation(identity);
        let (response, receiver) = oneshot::channel();
        match self.reserve_nonreplayable_normal(&[identity]) {
            Ok(permit) => {
                permit.send(WriteCommand::TypedAppendRunningEvent {
                    identity: operation.clone(),
                    sequence_guard: self.sequence_guard(&[identity]),
                    task_id: identity.task_id,
                    payload,
                    deadline,
                    response,
                });
            }
            Err(error @ (StoreWriterSubmitError::Full | StoreWriterSubmitError::Closed)) => {
                send_nonreplayable_ingress_rejection(response, operation.clone(), error);
            }
            Err(error) => return Err(error),
        }
        Ok(StoreWriterSubmission {
            identity: operation,
            pending: None,
            completion_channel_closed_reason: OutcomeUnknownReason::NonReplayableOperation,
            receiver,
        })
    }

    pub fn reconcile_pending(
        &self,
        pending: PendingDurableResult,
        deadline: Instant,
    ) -> Result<PendingDurableSubmission, StoreWriterSubmitError> {
        match pending {
            PendingDurableResult::QueueLimitedCreate {
                identity,
                input,
                max_queued_tasks,
            } => {
                let expected = DurableOperationIdentity::CreateTask {
                    client_request_id: input.client_request_id,
                };
                if identity != expected {
                    return Err(StoreWriterSubmitError::InvalidIdentity);
                }
                self.submit_queue_limited_create_on(input, max_queued_tasks, deadline, true)
                    .map(PendingDurableSubmission::QueueLimitedCreate)
            }
            PendingDurableResult::QueueLimitedRetry {
                identity,
                source_task_id,
                max_queued_tasks,
            } => {
                if identity != (DurableOperationIdentity::RetryTask { source_task_id }) {
                    return Err(StoreWriterSubmitError::InvalidIdentity);
                }
                self.submit_queue_limited_retry_on(source_task_id, max_queued_tasks, deadline, true)
                    .map(PendingDurableSubmission::QueueLimitedRetry)
            }
            PendingDurableResult::ClaimTask { identity, request } => self
                .submit_reconcile_claim_task(identity, request, deadline)
                .map(PendingDurableSubmission::ReconcileClaimTask),
            PendingDurableResult::PersistStopIntentBatch { identity, requests } => self
                .submit_stop_intent_batch_on(identity, requests, deadline, true, true)
                .map(PendingDurableSubmission::PersistStopIntentBatch),
            PendingDurableResult::FinalizeStoppedTask { identity, request } => self
                .submit_finalize_stopped_task_on(identity, request, deadline, true)
                .map(PendingDurableSubmission::FinalizeStoppedTask),
            PendingDurableResult::RecordReview { identity, request } => self
                .submit_record_review_on(identity, request, deadline, true)
                .map(PendingDurableSubmission::RecordReview),
            PendingDurableResult::FinalizeReviewedTask { identity, request } => self
                .submit_finalize_reviewed_task_on(identity, request, deadline, true)
                .map(PendingDurableSubmission::FinalizeReviewedTask),
            PendingDurableResult::FinalizeUnreviewedTask { identity, request } => self
                .submit_finalize_unreviewed_task_on(identity, request, deadline, true)
                .map(PendingDurableSubmission::FinalizeUnreviewedTask),
        }
    }

    pub async fn register_repository(
        &self,
        input: NewRepository,
        deadline: Instant,
    ) -> Result<WriteReceipt<RegisterRepositoryOutcome>, StoreWriterError> {
        let (response, receiver) = oneshot::channel();
        self.send(WriteCommand::RegisterRepository {
            input,
            deadline,
            response,
        })
        .await?;
        receive(receiver).await
    }

    pub async fn create_task(
        &self,
        input: NewTask,
        deadline: Instant,
    ) -> Result<WriteReceipt<CreateTaskOutcome>, StoreWriterError> {
        let (response, receiver) = oneshot::channel();
        self.send(WriteCommand::CreateTask {
            input,
            deadline,
            response,
        })
        .await?;
        receive(receiver).await
    }

    pub async fn retry_task(
        &self,
        task_id: TaskId,
        deadline: Instant,
    ) -> Result<WriteReceipt<RetryTaskOutcome>, StoreWriterError> {
        let (response, receiver) = oneshot::channel();
        self.send(WriteCommand::RetryTask {
            task_id,
            deadline,
            response,
        })
        .await?;
        receive(receiver).await
    }

    pub async fn start_task(
        &self,
        task_id: TaskId,
        deadline: Instant,
    ) -> Result<WriteReceipt<TransitionOutcome>, StoreWriterError> {
        self.transition_with_event(
            task_id,
            TaskStatus::Queued,
            TaskTransition::Running,
            deadline,
        )
        .await
    }

    pub async fn cancel_task(
        &self,
        task_id: TaskId,
        expected: TaskStatus,
        deadline: Instant,
    ) -> Result<WriteReceipt<TransitionOutcome>, StoreWriterError> {
        self.transition_with_event(task_id, expected, TaskTransition::Cancelled, deadline)
            .await
    }

    pub async fn fail_task(
        &self,
        task_id: TaskId,
        failure: TaskFailure,
        deadline: Instant,
    ) -> Result<WriteReceipt<TransitionOutcome>, StoreWriterError> {
        self.transition_with_event(
            task_id,
            TaskStatus::Running,
            TaskTransition::Failed(failure),
            deadline,
        )
        .await
    }

    async fn transition_with_event(
        &self,
        task_id: TaskId,
        expected: TaskStatus,
        transition: TaskTransition,
        deadline: Instant,
    ) -> Result<WriteReceipt<TransitionOutcome>, StoreWriterError> {
        if matches!(transition, TaskTransition::Completed) {
            return Err(completed_transition_bypass_error().into());
        }
        let (response, receiver) = oneshot::channel();
        self.send(WriteCommand::TransitionWithEvent {
            task_id,
            expected,
            transition,
            deadline,
            response,
        })
        .await?;
        receive(receiver).await
    }

    pub async fn record_review(
        &self,
        request: RecordReviewRequest,
        deadline: Instant,
    ) -> Result<WriteReceipt<RecordReviewOutcome>, StoreWriterError> {
        let (response, receiver) = oneshot::channel();
        self.send(WriteCommand::RecordReview {
            request,
            deadline,
            response,
        })
        .await?;
        receive(receiver).await
    }

    pub async fn finalize_reviewed_task(
        &self,
        request: FinalizeReviewedTaskRequest,
        deadline: Instant,
    ) -> Result<WriteReceipt<FinalizeReviewedTaskOutcome>, StoreWriterError> {
        let (response, receiver) = oneshot::channel();
        self.send(WriteCommand::FinalizeReviewedTask {
            request,
            deadline,
            response,
        })
        .await?;
        receive(receiver).await
    }

    pub async fn append_running_event(
        &self,
        task_id: TaskId,
        payload: TaskEventPayload,
        deadline: Instant,
    ) -> Result<WriteReceipt<AppendEventOutcome>, StoreWriterError> {
        let (response, receiver) = oneshot::channel();
        self.send(WriteCommand::AppendRunningEvent {
            task_id,
            payload,
            deadline,
            response,
        })
        .await?;
        receive(receiver).await
    }

    /// Atomically interrupts the remaining Queued and Running tasks after all
    /// durable stop intents have reached their exact terminal tuples.
    ///
    /// The caller remains responsible for proving that every affected process
    /// tree has exited before submitting this final shutdown/degraded step.
    pub async fn interrupt_remaining_after_stops(
        &self,
        failure: TaskFailure,
        deadline: Instant,
    ) -> Result<WriteReceipt<RecoveryReceipt>, StoreWriterError> {
        let (response, receiver) = oneshot::channel();
        self.send(WriteCommand::InterruptRemainingAfterStops {
            failure,
            deadline,
            response,
        })
        .await?;
        receive(receiver).await
    }

    pub async fn recover_incomplete(
        &self,
        now: UtcTimestamp,
        failure: TaskFailure,
        deadline: Instant,
    ) -> Result<WriteReceipt<RecoveryOutcome>, StoreWriterError> {
        let (response, receiver) = oneshot::channel();
        self.send(WriteCommand::RecoverIncomplete {
            now,
            failure,
            deadline,
            response,
        })
        .await?;
        receive(receiver).await
    }

    pub async fn reserve_attempt_artifact(
        &self,
        input: ReserveAttemptArtifact,
        deadline: Instant,
    ) -> Result<WriteReceipt<ReserveAttemptArtifactOutcome>, StoreWriterError> {
        let (response, receiver) = oneshot::channel();
        self.send(WriteCommand::ReserveAttemptArtifact {
            input,
            deadline,
            response,
        })
        .await?;
        receive(receiver).await
    }

    pub async fn mark_attempt_artifact_ready(
        &self,
        identity: AttemptArtifactIdentity,
        deadline: Instant,
    ) -> Result<WriteReceipt<UpdateAttemptArtifactOutcome>, StoreWriterError> {
        let (response, receiver) = oneshot::channel();
        self.send(WriteCommand::MarkAttemptArtifactReady {
            identity,
            deadline,
            response,
        })
        .await?;
        receive(receiver).await
    }

    pub async fn mark_attempt_artifact_inconsistent(
        &self,
        identity: AttemptArtifactIdentity,
        failure_code: impl Into<String>,
        deadline: Instant,
    ) -> Result<WriteReceipt<UpdateAttemptArtifactOutcome>, StoreWriterError> {
        let (response, receiver) = oneshot::channel();
        self.send(WriteCommand::MarkAttemptArtifactInconsistent {
            identity,
            failure_code: failure_code.into(),
            deadline,
            response,
        })
        .await?;
        receive(receiver).await
    }

    async fn send(&self, command: WriteCommand) -> Result<(), StoreWriterError> {
        self.sender
            .send(command)
            .await
            .map_err(|_| StoreWriterError::Closed)
    }

    fn reserve_normal(
        &self,
        identities: &[TaskMutationIdentity],
        reconciliation_lane: bool,
    ) -> Result<mpsc::Permit<'_, WriteCommand>, StoreWriterSubmitError> {
        let sender = if reconciliation_lane {
            &self.reconciliation_sender
        } else {
            &self.sender
        };
        self.reserve_on(sender, identities, reconciliation_lane)
    }

    fn reserve_urgent(
        &self,
        identities: &[TaskMutationIdentity],
        reconciliation_lane: bool,
    ) -> Result<mpsc::Permit<'_, WriteCommand>, StoreWriterSubmitError> {
        let sender = if reconciliation_lane {
            &self.reconciliation_sender
        } else {
            &self.urgent_sender
        };
        self.reserve_on(sender, identities, reconciliation_lane)
    }

    fn reserve_on<'a>(
        &'a self,
        sender: &'a mpsc::Sender<WriteCommand>,
        identities: &[TaskMutationIdentity],
        reconciliation_lane: bool,
    ) -> Result<mpsc::Permit<'a, WriteCommand>, StoreWriterSubmitError> {
        let mut ingress = self
            .ingress_sequences
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut proposed = ingress.clone();
        if reconciliation_lane {
            proposed.accept_reconciliation(identities)?;
        } else {
            proposed.accept(identities)?;
        }
        match sender.try_reserve() {
            Ok(permit) => {
                *ingress = proposed;
                Ok(permit)
            }
            Err(error) => {
                if !reconciliation_lane {
                    proposed.mark_unresolved(identities);
                    *ingress = proposed;
                }
                Err(match error {
                    mpsc::error::TrySendError::Full(()) => StoreWriterSubmitError::Full,
                    mpsc::error::TrySendError::Closed(()) => StoreWriterSubmitError::Closed,
                })
            }
        }
    }

    fn reserve_nonreplayable_normal(
        &self,
        identities: &[TaskMutationIdentity],
    ) -> Result<mpsc::Permit<'_, WriteCommand>, StoreWriterSubmitError> {
        self.reserve_on(&self.sender, identities, false)
    }

    fn sequence_guard(&self, identities: &[TaskMutationIdentity]) -> MutationSequenceGuard {
        MutationSequenceGuard::new(self.ingress_sequences.clone(), identities.to_vec())
    }

    #[cfg(test)]
    pub(crate) fn stage_unresolved_mutations_for_test(
        &self,
        identities: &[TaskMutationIdentity],
    ) -> Result<(), StoreWriterSubmitError> {
        let mut ingress = self
            .ingress_sequences
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ingress.accept(identities)?;
        ingress.mark_unresolved(identities);
        Ok(())
    }
}

fn completion_channel_closed_reason(reconciliation_lane: bool) -> OutcomeUnknownReason {
    if reconciliation_lane {
        OutcomeUnknownReason::ReconciliationFailed
    } else {
        OutcomeUnknownReason::CompletionChannelClosed
    }
}

fn send_delivery_ingress_rejection(
    response: oneshot::Sender<DeliveryCompletion>,
    identity: DeliverySubmissionIdentity,
    command: DeliveryWriteCommand,
    error: StoreWriterSubmitError,
    reconciliation_lane: bool,
) {
    let disposition = if reconciliation_lane {
        DeliveryDisposition::OutcomeUnknown {
            reason: OutcomeUnknownReason::ReconciliationFailed,
            command,
        }
    } else {
        match error {
            StoreWriterSubmitError::Full => DeliveryDisposition::KnownNotApplied {
                reason: KnownNotAppliedReason::IngressFull,
                outcome: None,
                error: None,
            },
            StoreWriterSubmitError::Closed => DeliveryDisposition::KnownNotApplied {
                reason: KnownNotAppliedReason::IngressClosed,
                outcome: None,
                error: None,
            },
            StoreWriterSubmitError::InvalidIdentity
            | StoreWriterSubmitError::SequenceGap
            | StoreWriterSubmitError::SequenceReversed => DeliveryDisposition::InvariantConflict {
                message: "delivery ingress rejected an identity-free typed request",
                outcome: None,
            },
        }
    };
    let _ = response.send(DeliveryCompletion {
        identity,
        disposition,
    });
}

fn send_ingress_rejection<T>(
    response: oneshot::Sender<DurableCompletion<T>>,
    identity: DurableOperationIdentity,
    pending: &PendingDurableResult,
    error: StoreWriterSubmitError,
    reconciliation_lane: bool,
) {
    let disposition = if reconciliation_lane {
        DurableDisposition::OutcomeUnknown {
            reason: OutcomeUnknownReason::ReconciliationFailed,
            pending: Some(pending.clone()),
        }
    } else {
        DurableDisposition::KnownNotApplied {
            reason: match error {
                StoreWriterSubmitError::Full => KnownNotAppliedReason::IngressFull,
                StoreWriterSubmitError::Closed => KnownNotAppliedReason::IngressClosed,
                StoreWriterSubmitError::InvalidIdentity
                | StoreWriterSubmitError::SequenceGap
                | StoreWriterSubmitError::SequenceReversed => {
                    unreachable!("only ingress availability is converted to a completion")
                }
            },
            outcome: None,
            error: None,
        }
    };
    let _ = response.send(DurableCompletion {
        identity,
        sequence_disposition: if reconciliation_lane {
            MutationSequenceDisposition::BlockUnknown
        } else {
            MutationSequenceDisposition::RetainSame
        },
        disposition,
    });
}

fn send_nonreplayable_ingress_rejection<T>(
    response: oneshot::Sender<DurableCompletion<T>>,
    identity: DurableOperationIdentity,
    error: StoreWriterSubmitError,
) {
    let reason = match error {
        StoreWriterSubmitError::Full => KnownNotAppliedReason::IngressFull,
        StoreWriterSubmitError::Closed => KnownNotAppliedReason::IngressClosed,
        StoreWriterSubmitError::InvalidIdentity
        | StoreWriterSubmitError::SequenceGap
        | StoreWriterSubmitError::SequenceReversed => {
            unreachable!("only ingress availability is converted to a completion")
        }
    };
    let _ = response.send(DurableCompletion {
        identity,
        sequence_disposition: MutationSequenceDisposition::RetainSame,
        disposition: DurableDisposition::KnownNotApplied {
            reason,
            outcome: None,
            error: None,
        },
    });
}

fn validate_stop_batch_identity(
    identity: &DurableOperationIdentity,
    requests: &[StopIntentRequest],
) -> Result<(), StoreWriterSubmitError> {
    let DurableOperationIdentity::StopIntentBatch { items } = identity else {
        return Err(StoreWriterSubmitError::InvalidIdentity);
    };
    if items.len() != requests.len()
        || items.iter().zip(requests).any(|(item, request)| {
            item.kind != DurableOperationKind::PersistStopIntent || item.task_id != request.task_id
        })
    {
        return Err(StoreWriterSubmitError::InvalidIdentity);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
