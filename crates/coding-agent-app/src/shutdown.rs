use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use coding_agent_domain::{
    DeliveryReadiness, EventCursor, EventId, NewReviewEvidence, ReviewEvidence, ReviewVerdict,
    Task, TaskFailure, TaskStatus,
};
use coding_agent_runtime::{ProcessLivenessScope, SealedProcessLivenessScope};
use coding_agent_store::{
    FinalizeReviewedTaskOutcome, FinalizeStoppedTaskOutcome, FinalizeUnreviewedTaskOutcome,
    RecordReviewOutcome, RecoveryOutcome, StopIntentKind, StopIntentReceipt, StopIntentRequest,
    Store,
};
use serde::Serialize;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Instant, timeout_at};
use uuid::Uuid;

#[cfg(test)]
use crate::StoreWriterError;
use crate::pending_durable::{
    DurableDisposition, DurableOperationIdentity, DurableOperationKind, KnownNotAppliedReason,
    MutationSequenceDisposition, PendingDurableResult, PendingReplayReceipt,
};
use crate::task_manager::{
    ShutdownProcessCleanupProof, TaskManagerMessage, terminal_task_is_structurally_valid,
};
use crate::{
    EventDispatcherHandle, FinalizeReviewedTaskRequest, FinalizeUnreviewedTaskRequest,
    MutationDrainOutcome, MutationGate, NativeMessageSink, PlatformPaths, PrivateFile,
    QuiesceResult, RecordReviewRequest, ServiceState, ServiceStateController, StoreWriterHandle,
    StoreWriterSubmitError, TaskManagerHandle, WallClock,
};

const RECOVERY_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const RECOVERY_WRITE_BUDGET: Duration = Duration::from_secs(5);
const SHUTDOWN_PERSISTENCE_BUDGET: Duration = Duration::from_secs(5);
const SHUTDOWN_TOTAL_BUDGET: Duration = Duration::from_secs(10);
const SHUTDOWN_MUTATION_CANCEL_GRACE: Duration = Duration::from_secs(1);
const SHUTDOWN_FINALIZE_RESERVE: Duration = Duration::from_secs(1);
const SHUTDOWN_FAILSAFE_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const SHUTDOWN_MARKER_ERROR_CODE: &str = "SHUTDOWN_PERSISTENCE_FAILED";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownOutcome {
    Clean,
    Degraded,
}

impl ShutdownOutcome {
    pub const fn exit_code(self) -> i32 {
        match self {
            Self::Clean => 0,
            Self::Degraded => 1,
        }
    }
}

#[async_trait::async_trait]
pub(crate) trait ShutdownCleanup: Send + Sync + 'static {
    async fn stop_http(&self, deadline: Instant);
    fn stop_http_now(&self);
    fn unpublish_descriptor(&self);
    fn finish_lock(&self, proof: ShutdownRuntimeCleanupProof, disposition: ShutdownLockDisposition);
}

pub(crate) struct ShutdownRuntimeCleanupProof {
    task_processes: ShutdownProcessCleanupProof,
    _instance_processes: ConfirmedInstanceProcessCleanup,
}

struct ConfirmedInstanceProcessCleanup {
    _sealed_scope: SealedProcessLivenessScope,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShutdownLockDisposition {
    ReleaseNow,
    RetainUntilProcessExit,
}

struct ShutdownPrerequisites {
    proof: ShutdownRuntimeCleanupProof,
    mutation_outcome_unknown: bool,
    process_cleanup_outlived_deadline: bool,
}

#[derive(Clone)]
pub struct ShutdownCoordinator {
    inner: Arc<ShutdownCoordinatorInner>,
}

struct ShutdownCoordinatorInner {
    started: AtomicBool,
    outcome: watch::Sender<Option<ShutdownOutcome>>,
    runtime: RuntimeShutdown,
}

struct RuntimeShutdown {
    mutation_gate: MutationGate,
    instance_process_scope: ProcessLivenessScope,
    task_manager: TaskManagerHandle,
    dispatcher: EventDispatcherHandle,
    store: Store,
    cleanup: Arc<dyn ShutdownCleanup>,
    marker_path: PathBuf,
    instance_id: Uuid,
    wall_clock: Arc<dyn WallClock>,
    messages: Arc<dyn NativeMessageSink>,
    marker_writer: Arc<dyn ShutdownMarkerWriter>,
}

impl ShutdownCoordinator {
    #[allow(clippy::too_many_arguments)]
    #[cfg_attr(feature = "test-support", allow(dead_code))]
    pub(crate) fn new(
        mutation_gate: MutationGate,
        instance_process_scope: ProcessLivenessScope,
        task_manager: TaskManagerHandle,
        dispatcher: EventDispatcherHandle,
        store: Store,
        cleanup: Arc<dyn ShutdownCleanup>,
        paths: &PlatformPaths,
        instance_id: Uuid,
        wall_clock: Arc<dyn WallClock>,
        messages: Arc<dyn NativeMessageSink>,
    ) -> Self {
        Self::new_with_marker_writer(
            mutation_gate,
            instance_process_scope,
            task_manager,
            dispatcher,
            store,
            cleanup,
            paths,
            instance_id,
            wall_clock,
            messages,
            Arc::new(FilesystemShutdownMarkerWriter),
        )
    }

    #[cfg(feature = "test-support")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_for_process_test(
        mutation_gate: MutationGate,
        instance_process_scope: ProcessLivenessScope,
        task_manager: TaskManagerHandle,
        dispatcher: EventDispatcherHandle,
        store: Store,
        cleanup: Arc<dyn ShutdownCleanup>,
        paths: &PlatformPaths,
        instance_id: Uuid,
        wall_clock: Arc<dyn WallClock>,
        messages: Arc<dyn NativeMessageSink>,
        marker_write_failure: bool,
    ) -> Self {
        let marker_writer: Arc<dyn ShutdownMarkerWriter> = if marker_write_failure {
            Arc::new(ProcessTestFailingShutdownMarkerWriter)
        } else {
            Arc::new(FilesystemShutdownMarkerWriter)
        };
        Self::new_with_marker_writer(
            mutation_gate,
            instance_process_scope,
            task_manager,
            dispatcher,
            store,
            cleanup,
            paths,
            instance_id,
            wall_clock,
            messages,
            marker_writer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_with_marker_writer(
        mutation_gate: MutationGate,
        instance_process_scope: ProcessLivenessScope,
        task_manager: TaskManagerHandle,
        dispatcher: EventDispatcherHandle,
        store: Store,
        cleanup: Arc<dyn ShutdownCleanup>,
        paths: &PlatformPaths,
        instance_id: Uuid,
        wall_clock: Arc<dyn WallClock>,
        messages: Arc<dyn NativeMessageSink>,
        marker_writer: Arc<dyn ShutdownMarkerWriter>,
    ) -> Self {
        let (outcome, _) = watch::channel(None);
        Self {
            inner: Arc::new(ShutdownCoordinatorInner {
                started: AtomicBool::new(false),
                outcome,
                runtime: RuntimeShutdown {
                    mutation_gate,
                    instance_process_scope,
                    task_manager,
                    dispatcher,
                    store,
                    cleanup,
                    marker_path: paths.unclean_shutdown.clone(),
                    instance_id,
                    wall_clock,
                    messages,
                    marker_writer,
                },
            }),
        }
    }

    pub async fn shutdown(&self) -> ShutdownOutcome {
        let mut outcome = self.inner.outcome.subscribe();
        if self
            .inner
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let inner = self.inner.clone();
            let started = Instant::now();
            tokio::spawn(async move {
                let result = inner.runtime.run_supervised(started, false).await;
                inner.outcome.send_replace(Some(result));
            });
        }

        loop {
            if let Some(outcome) = *outcome.borrow() {
                return outcome;
            }
            if outcome.changed().await.is_err() {
                return ShutdownOutcome::Degraded;
            }
        }
    }

    pub(crate) fn force_cleanup(&self) {
        if self
            .inner
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.inner.runtime.begin_emergency_cleanup_now();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let inner = self.inner.clone();
                let started = Instant::now();
                handle.spawn(async move {
                    let result = inner.runtime.run_supervised(started, true).await;
                    inner.outcome.send_replace(Some(result));
                });
            }
        }
    }
}

mod runtime;

#[derive(Serialize)]
struct ShutdownMarker {
    timestamp: String,
    instance_id: String,
    error_code: &'static str,
}

trait ShutdownMarkerWriter: Send + Sync + 'static {
    fn write(
        &self,
        path: &Path,
        instance_id: Uuid,
        timestamp: time::OffsetDateTime,
    ) -> io::Result<()>;
}

struct FilesystemShutdownMarkerWriter;

impl ShutdownMarkerWriter for FilesystemShutdownMarkerWriter {
    fn write(
        &self,
        path: &Path,
        instance_id: Uuid,
        timestamp: time::OffsetDateTime,
    ) -> io::Result<()> {
        let instance_path = shutdown_marker_instance_path(path, instance_id)?;
        write_shutdown_marker(&instance_path, instance_id, timestamp)
    }
}

#[cfg(feature = "test-support")]
struct ProcessTestFailingShutdownMarkerWriter;

#[cfg(feature = "test-support")]
impl ShutdownMarkerWriter for ProcessTestFailingShutdownMarkerWriter {
    fn write(
        &self,
        _path: &Path,
        _instance_id: Uuid,
        _timestamp: time::OffsetDateTime,
    ) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "injected process-test shutdown marker failure",
        ))
    }
}

#[derive(Debug, thiserror::Error)]
enum ShutdownMarkerWriteError {
    #[error("the shutdown marker deadline elapsed")]
    Deadline,
    #[error("the shutdown marker worker failed: {0}")]
    Worker(String),
    #[error(transparent)]
    Io(#[from] io::Error),
}

async fn write_shutdown_marker_until(
    writer: Arc<dyn ShutdownMarkerWriter>,
    path: PathBuf,
    instance_id: Uuid,
    timestamp: time::OffsetDateTime,
    deadline: Instant,
) -> Result<(), ShutdownMarkerWriteError> {
    let write = tokio::task::spawn_blocking(move || writer.write(&path, instance_id, timestamp));
    match timeout_at(deadline, write).await {
        Ok(Ok(result)) => result.map_err(ShutdownMarkerWriteError::from),
        Ok(Err(error)) => Err(ShutdownMarkerWriteError::Worker(error.to_string())),
        Err(_) => Err(ShutdownMarkerWriteError::Deadline),
    }
}

fn shutdown_marker_instance_path(path: &Path, instance_id: Uuid) -> io::Result<PathBuf> {
    let file_name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "shutdown marker path has no file name",
        )
    })?;
    let mut instance_name = file_name.to_os_string();
    instance_name.push(".");
    instance_name.push(instance_id.hyphenated().to_string());
    instance_name.push(".marker");
    Ok(path.with_file_name(instance_name))
}

async fn publish_degraded_message_until(
    messages: Arc<dyn NativeMessageSink>,
    deadline: Instant,
) -> io::Result<()> {
    let publish = tokio::task::spawn_blocking(move || messages.publish_degraded_shutdown());
    match timeout_at(deadline, publish).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(io::Error::other(error.to_string())),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "degraded shutdown warning handoff deadline elapsed",
        )),
    }
}

fn write_shutdown_marker(
    path: &Path,
    instance_id: Uuid,
    timestamp: time::OffsetDateTime,
) -> io::Result<()> {
    let mut file = PrivateFile::create_new(path)?;
    let marker = ShutdownMarker {
        timestamp: timestamp
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(io::Error::other)?,
        instance_id: instance_id.hyphenated().to_string(),
        error_code: SHUTDOWN_MARKER_ERROR_CODE,
    };
    serde_json::to_writer(&mut file, &marker).map_err(io::Error::other)?;
    file.flush()?;
    file.as_file().sync_all()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DegradedRecoveryResult {
    pub recovery: RecoveryOutcome,
    pub replayed_pending_count: usize,
    pub ready_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DegradedCoordinatorError {
    #[error("degraded recovery was superseded by application shutdown")]
    Quiescing,
    #[error("degraded recovery was superseded by a newer exact actor barrier")]
    Superseded,
    #[error("task manager closed before degraded recovery was finalized")]
    ManagerClosed,
    #[error("typed degraded replay conflicted with durable state")]
    TypedConflict,
}

#[derive(Clone)]
pub struct DegradedCoordinator {
    backend: Arc<dyn RecoveryBackend>,
    service_state: ServiceStateController,
    manager: mpsc::WeakSender<TaskManagerMessage>,
}

#[async_trait::async_trait]
trait RecoveryBackend: Send + Sync + 'static {
    async fn replay(&self, pending: &PendingDurableResult) -> Result<Option<EventId>, ReplayError>;
    async fn recover(&self) -> Result<RecoveryOutcome, String>;
    async fn flush(&self, high_watermark: EventCursor) -> Result<(), String>;
}

#[derive(Debug)]
enum ReplayError {
    Retryable(String),
    Conflict(String),
}

struct RuntimeRecoveryBackend {
    writer: StoreWriterHandle,
    dispatcher: EventDispatcherHandle,
}

#[async_trait::async_trait]
impl RecoveryBackend for RuntimeRecoveryBackend {
    async fn replay(&self, pending: &PendingDurableResult) -> Result<Option<EventId>, ReplayError> {
        let expected_identity = pending.identity();
        let submission = self
            .writer
            .reconcile_pending(pending.clone(), Instant::now() + RECOVERY_WRITE_BUDGET)
            .map_err(classify_replay_submit_error)?;
        let completion = submission.completion().await;
        if completion.identity != expected_identity {
            return Err(ReplayError::Conflict(
                "typed pending replay returned a different mutation identity".to_owned(),
            ));
        }
        match (completion.sequence_disposition, completion.disposition) {
            (MutationSequenceDisposition::AdvanceNext, DurableDisposition::Confirmed(receipt)) => {
                classify_pending_replay_receipt(pending, receipt)
            }
            (
                MutationSequenceDisposition::AdvanceNext,
                DurableDisposition::KnownNotApplied {
                    reason: KnownNotAppliedReason::ExactReconciliation,
                    outcome: Some(receipt),
                    error: None,
                },
            ) => classify_pending_replay_receipt(pending, receipt),
            (
                MutationSequenceDisposition::AdvanceNext,
                DurableDisposition::KnownNotApplied { error: Some(_), .. },
            ) => Err(ReplayError::Conflict(
                "typed pending replay was rejected by durable state".to_owned(),
            )),
            (_, DurableDisposition::KnownNotApplied { reason, error, .. }) => {
                Err(ReplayError::Retryable(format!(
                    "typed pending replay was not admitted ({reason:?}, {error:?})"
                )))
            }
            (
                MutationSequenceDisposition::BlockUnknown,
                DurableDisposition::OutcomeUnknown { reason, .. },
            ) => Err(ReplayError::Retryable(format!(
                "typed pending outcome remains unknown ({reason:?})"
            ))),
            (_, DurableDisposition::OutcomeUnknown { .. }) => Err(ReplayError::Conflict(
                "typed pending replay returned an inconsistent sequence disposition".to_owned(),
            )),
            (_, DurableDisposition::InvariantConflict { message, .. }) => {
                Err(ReplayError::Conflict(message.to_owned()))
            }
            (
                MutationSequenceDisposition::RetainSame | MutationSequenceDisposition::BlockUnknown,
                DurableDisposition::Confirmed(_),
            ) => Err(ReplayError::Conflict(
                "typed pending replay confirmed without advancing its sequence".to_owned(),
            )),
        }
    }

    async fn recover(&self) -> Result<RecoveryOutcome, String> {
        self.writer
            .interrupt_remaining_after_stops(
                degraded_recovery_failure(),
                Instant::now() + RECOVERY_WRITE_BUDGET,
            )
            .await
            .map(|receipt| {
                let generic = receipt.value;
                RecoveryOutcome {
                    interrupted_count: generic.interrupted_count,
                    first_event_id: generic.first_event_id,
                    last_event_id: generic.last_event_id,
                    high_watermark: generic.high_watermark,
                }
            })
            .map_err(|error| error.to_string())
    }

    async fn flush(&self, high_watermark: EventCursor) -> Result<(), String> {
        self.dispatcher
            .flush_to(high_watermark)
            .await
            .map_err(|error| error.to_string())
    }
}

fn classify_pending_replay_receipt(
    pending: &PendingDurableResult,
    receipt: PendingReplayReceipt,
) -> Result<Option<EventId>, ReplayError> {
    if !shutdown_replay_receipt_matches(pending, &receipt) {
        Err(ReplayError::Conflict(
            "typed pending replay receipt did not match the submitted request".to_owned(),
        ))
    } else if receipt.has_stop_intent_conflict() {
        Err(ReplayError::Conflict(
            "typed stop-intent replay conflicted with the existing durable intent".to_owned(),
        ))
    } else {
        Ok(receipt.event_id())
    }
}

fn shutdown_replay_receipt_matches(
    pending: &PendingDurableResult,
    receipt: &PendingReplayReceipt,
) -> bool {
    match (pending, receipt) {
        (
            PendingDurableResult::QueueLimitedCreate { .. },
            PendingReplayReceipt::QueueLimitedCreate(_),
        )
        | (
            PendingDurableResult::QueueLimitedRetry { .. },
            PendingReplayReceipt::QueueLimitedRetry(_),
        )
        | (PendingDurableResult::ClaimTask { .. }, PendingReplayReceipt::ClaimTask(_)) => true,
        (
            PendingDurableResult::RecordReview { identity, request },
            PendingReplayReceipt::RecordReview(outcome),
        ) => {
            identity.task_id == request.task_id
                && identity.kind == DurableOperationKind::RecordReview
                && shutdown_record_review_matches(request, outcome)
        }
        (
            PendingDurableResult::FinalizeReviewedTask { identity, request },
            PendingReplayReceipt::FinalizeReviewedTask(outcome),
        ) => {
            identity.task_id == request.task_id
                && identity.kind == DurableOperationKind::FinalizeReviewedTask
                && shutdown_reviewed_terminal_matches(request, outcome)
        }
        (
            PendingDurableResult::FinalizeUnreviewedTask { identity, request },
            PendingReplayReceipt::FinalizeUnreviewedTask(outcome),
        ) => {
            identity.task_id == request.task_id
                && identity.kind == DurableOperationKind::FinalizeUnreviewedTask
                && shutdown_unreviewed_terminal_matches(request, outcome)
        }
        (
            PendingDurableResult::PersistStopIntentBatch { identity, requests },
            PendingReplayReceipt::PersistStopIntentBatch(receipt),
        ) => {
            let DurableOperationIdentity::StopIntentBatch { items } = identity else {
                return false;
            };
            items.len() == requests.len()
                && receipt.items.len() == requests.len()
                && items
                    .iter()
                    .zip(requests)
                    .zip(&receipt.items)
                    .all(|((identity, request), item)| {
                        identity.task_id == request.task_id
                            && identity.kind == DurableOperationKind::PersistStopIntent
                            && item.request == *request
                            && match &item.outcome {
                                coding_agent_store::PersistStopIntentOutcome::Applied(receipt)
                                | coding_agent_store::PersistStopIntentOutcome::Existing(
                                    receipt,
                                ) => shutdown_stop_receipt_matches_request(*receipt, *request),
                                coding_agent_store::PersistStopIntentOutcome::TerminalWon {
                                    current,
                                } => {
                                    current.id == request.task_id
                                        && current.repository_id
                                            == request.expected_repository_id
                                        && current.attempt == request.expected_attempt
                                        && terminal_task_is_structurally_valid(current)
                                }
                                coding_agent_store::PersistStopIntentOutcome::IntentConflict {
                                    existing,
                                } => {
                                    existing.task_id == request.task_id
                                        && existing.repository_id
                                            == request.expected_repository_id
                                        && existing.attempt == request.expected_attempt
                                        && existing.kind != request.kind
                                }
                            }
                    })
        }
        (
            PendingDurableResult::FinalizeStoppedTask { identity, request },
            PendingReplayReceipt::FinalizeStoppedTask(outcome),
        ) => {
            identity.task_id == request.task_id
                && identity.kind == DurableOperationKind::FinalizeStoppedTask
                && match outcome {
                    FinalizeStoppedTaskOutcome::Applied(receipt)
                    | FinalizeStoppedTaskOutcome::Existing(receipt) => {
                        receipt.intent.task_id == request.task_id
                            && receipt.intent.repository_id == request.expected_repository_id
                            && receipt.intent.attempt == request.expected_attempt
                            && receipt.intent.kind == request.expected_intent
                            && receipt.task.id == request.task_id
                            && receipt.task.repository_id == request.expected_repository_id
                            && receipt.task.attempt == request.expected_attempt
                            && shutdown_stopped_terminal_matches(
                                &receipt.task,
                                request.expected_intent,
                            )
                            && receipt.task.last_event_id == receipt.terminal_event_id
                    }
                    FinalizeStoppedTaskOutcome::InvariantConflict => false,
                }
        }
        _ => false,
    }
}

fn shutdown_stop_receipt_matches_request(
    receipt: StopIntentReceipt,
    request: StopIntentRequest,
) -> bool {
    receipt.task_id == request.task_id
        && receipt.repository_id == request.expected_repository_id
        && receipt.attempt == request.expected_attempt
        && receipt.kind == request.kind
}

fn shutdown_stopped_terminal_matches(task: &Task, kind: StopIntentKind) -> bool {
    if !terminal_task_is_structurally_valid(task)
        || task.delivery_readiness != DeliveryReadiness::Unreviewed
    {
        return false;
    }
    match kind {
        StopIntentKind::UserCancelled => {
            task.status == TaskStatus::Cancelled && task.failure.is_none()
        }
        StopIntentKind::DiskPressureCritical => {
            task.status == TaskStatus::Failed
                && task.failure.as_ref().is_some_and(|failure| {
                    failure.code == "DISK_PRESSURE_CRITICAL"
                        && failure.message == "critical disk pressure stopped the task"
                        && failure.retryable
                })
        }
    }
}

fn shutdown_review_evidence_is_exact(
    review: &ReviewEvidence,
    expected: &NewReviewEvidence,
) -> bool {
    let Ok(mut stored_value) = serde_json::to_value(review) else {
        return false;
    };
    let Some(stored) = stored_value.as_object_mut() else {
        return false;
    };
    stored.remove("created_at");
    serde_json::to_value(expected).is_ok_and(|expected| expected == stored_value)
}

fn shutdown_record_review_matches(
    request: &RecordReviewRequest,
    outcome: &RecordReviewOutcome,
) -> bool {
    let review = match outcome {
        RecordReviewOutcome::Applied { review, .. }
        | RecordReviewOutcome::Existing { review, .. } => review,
    };
    shutdown_review_evidence_is_exact(review, &request.evidence)
}

fn shutdown_reviewed_terminal_matches(
    request: &FinalizeReviewedTaskRequest,
    outcome: &FinalizeReviewedTaskOutcome,
) -> bool {
    let (task, review, review_event_id, terminal_event_id) = match outcome {
        FinalizeReviewedTaskOutcome::Applied {
            task,
            review,
            review_event_id,
            terminal_event_id,
        }
        | FinalizeReviewedTaskOutcome::Existing {
            task,
            review,
            review_event_id,
            terminal_event_id,
        } => (task, review, *review_event_id, *terminal_event_id),
    };
    let (expected_status, expected_readiness, expected_failure) = match request.evidence.verdict() {
        ReviewVerdict::Approved => (
            TaskStatus::Completed,
            DeliveryReadiness::ReviewApproved,
            None,
        ),
        ReviewVerdict::ChangesRequested => (
            TaskStatus::Failed,
            DeliveryReadiness::ReviewRejected,
            Some(TaskFailure {
                code: "REVIEW_REJECTED".to_owned(),
                message: "review rejected after three rounds".to_owned(),
                retryable: true,
            }),
        ),
    };
    terminal_task_is_structurally_valid(task)
        && task.id == request.task_id
        && task.repository_id == request.expected_repository_id
        && task.attempt == request.expected_attempt
        && task.status == expected_status
        && task.delivery_readiness == expected_readiness
        && task.failure == expected_failure
        && task.finished_at == Some(review.created_at())
        && task.last_event_id == terminal_event_id
        && review_event_id.get().checked_add(1) == Some(terminal_event_id.get())
        && shutdown_review_evidence_is_exact(review, &request.evidence)
}

fn shutdown_unreviewed_terminal_matches(
    request: &FinalizeUnreviewedTaskRequest,
    outcome: &FinalizeUnreviewedTaskOutcome,
) -> bool {
    let (task, event_id) = match outcome {
        FinalizeUnreviewedTaskOutcome::Applied { task, event_id }
        | FinalizeUnreviewedTaskOutcome::Existing { task, event_id } => (task, *event_id),
        FinalizeUnreviewedTaskOutcome::InvariantConflict => return false,
    };
    terminal_task_is_structurally_valid(task)
        && task.id == request.task_id
        && task.repository_id == request.expected_repository_id
        && task.attempt == request.expected_attempt
        && task.status == request.transition.next()
        && task.delivery_readiness == DeliveryReadiness::Unreviewed
        && task.failure.as_ref() == request.transition.failure()
        && task.last_event_id == event_id
}

impl DegradedCoordinator {
    pub(crate) fn new(
        writer: StoreWriterHandle,
        dispatcher: EventDispatcherHandle,
        service_state: ServiceStateController,
        manager: mpsc::WeakSender<TaskManagerMessage>,
    ) -> Self {
        Self {
            backend: Arc::new(RuntimeRecoveryBackend { writer, dispatcher }),
            service_state,
            manager,
        }
    }

    #[cfg(test)]
    fn with_backend(
        backend: Arc<dyn RecoveryBackend>,
        service_state: ServiceStateController,
        manager: mpsc::WeakSender<TaskManagerMessage>,
    ) -> Self {
        Self {
            backend,
            service_state,
            manager,
        }
    }

    pub async fn run(
        &self,
        pending: Vec<PendingDurableResult>,
    ) -> Result<DegradedRecoveryResult, DegradedCoordinatorError> {
        let replayed_pending_count = pending.len();
        let replay_high_watermark = self.replay_pending(&pending).await?;
        self.run_after_replay(0, 0, replayed_pending_count, replay_high_watermark)
            .await
    }

    pub(crate) async fn run_after_replay(
        &self,
        attempt_id: u64,
        barrier_epoch: u64,
        replayed_pending_count: usize,
        replay_high_watermark: Option<EventId>,
    ) -> Result<DegradedRecoveryResult, DegradedCoordinatorError> {
        let recovery = self.recover_store().await?;
        let high_watermark =
            replay_high_watermark
                .map(EventId::get)
                .map_or(recovery.high_watermark, |replayed| {
                    EventCursor::new(replayed.max(recovery.high_watermark.get()))
                        .expect("replayed event IDs are positive")
                });
        self.flush_recovery(high_watermark).await?;
        self.finalize(
            attempt_id,
            barrier_epoch,
            recovery,
            replayed_pending_count,
            high_watermark,
        )
        .await
    }

    async fn replay_pending(
        &self,
        pending: &[PendingDurableResult],
    ) -> Result<Option<EventId>, DegradedCoordinatorError> {
        let mut high_watermark = None;
        for request in pending {
            loop {
                self.ensure_not_quiescing()?;
                match self.backend.replay(request).await {
                    Ok(event_id) => {
                        if let Some(event_id) = event_id {
                            high_watermark = Some(
                                high_watermark
                                    .map_or(event_id, |current: EventId| current.max(event_id)),
                            );
                        }
                        break;
                    }
                    Err(ReplayError::Retryable(error)) => {
                        tracing::warn!(error = %error, "degraded typed replay attempt failed");
                        self.wait_to_retry().await?;
                    }
                    Err(ReplayError::Conflict(error)) => {
                        // Application quiescing or manager closure wins a race with an
                        // in-flight replay error; shutdown must not be converted into a
                        // new degraded freeze after the replay future returns.
                        self.ensure_not_quiescing()?;
                        tracing::error!(error = %error, "degraded typed replay conflicted");
                        self.freeze_manager(pending.to_vec()).await?;
                        return Err(DegradedCoordinatorError::TypedConflict);
                    }
                }
            }
        }
        Ok(high_watermark)
    }

    async fn recover_store(&self) -> Result<RecoveryOutcome, DegradedCoordinatorError> {
        loop {
            self.ensure_not_quiescing()?;
            match self.backend.recover().await {
                Ok(recovery) => return Ok(recovery),
                Err(error) => {
                    tracing::warn!(error = %error, "degraded store recovery attempt failed");
                    self.wait_to_retry().await?;
                }
            }
        }
    }

    async fn flush_recovery(
        &self,
        high_watermark: coding_agent_domain::EventCursor,
    ) -> Result<(), DegradedCoordinatorError> {
        loop {
            self.ensure_not_quiescing()?;
            match self.backend.flush(high_watermark).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    tracing::warn!(error = %error, "degraded recovery event flush failed");
                    self.wait_to_retry().await?;
                }
            }
        }
    }

    async fn finalize(
        &self,
        attempt_id: u64,
        barrier_epoch: u64,
        recovery: RecoveryOutcome,
        replayed_pending_count: usize,
        high_watermark: EventCursor,
    ) -> Result<DegradedRecoveryResult, DegradedCoordinatorError> {
        self.ensure_not_quiescing()?;
        let (response, receiver) = oneshot::channel();
        self.manager
            .upgrade()
            .ok_or(DegradedCoordinatorError::ManagerClosed)?
            .send(TaskManagerMessage::FinalizeDegraded {
                attempt_id,
                barrier_epoch,
                recovery,
                replayed_pending_count,
                high_watermark,
                response,
            })
            .await
            .map_err(|_| DegradedCoordinatorError::ManagerClosed)?;
        receiver
            .await
            .map_err(|_| DegradedCoordinatorError::ManagerClosed)?
    }

    async fn freeze_manager(
        &self,
        pending: Vec<PendingDurableResult>,
    ) -> Result<(), DegradedCoordinatorError> {
        let (response, receiver) = oneshot::channel();
        self.manager
            .upgrade()
            .ok_or(DegradedCoordinatorError::ManagerClosed)?
            .send(TaskManagerMessage::FreezeDegraded { pending, response })
            .await
            .map_err(|_| DegradedCoordinatorError::ManagerClosed)?;
        receiver
            .await
            .map_err(|_| DegradedCoordinatorError::ManagerClosed)
    }

    async fn wait_to_retry(&self) -> Result<(), DegradedCoordinatorError> {
        let mut state = self.service_state.subscribe();
        let manager = self
            .manager
            .upgrade()
            .ok_or(DegradedCoordinatorError::ManagerClosed)?;
        if manager.is_closed() {
            return Err(DegradedCoordinatorError::ManagerClosed);
        }
        if state.borrow().state == ServiceState::Quiescing {
            return Err(DegradedCoordinatorError::Quiescing);
        }
        tokio::select! {
            () = tokio::time::sleep(RECOVERY_RETRY_INTERVAL) => Ok(()),
            () = manager.closed() => Err(DegradedCoordinatorError::ManagerClosed),
            result = state.changed() => {
                if result.is_err() || state.borrow().state == ServiceState::Quiescing {
                    Err(DegradedCoordinatorError::Quiescing)
                } else {
                    Ok(())
                }
            }
        }
    }

    fn ensure_not_quiescing(&self) -> Result<(), DegradedCoordinatorError> {
        let manager = self
            .manager
            .upgrade()
            .ok_or(DegradedCoordinatorError::ManagerClosed)?;
        if manager.is_closed() {
            return Err(DegradedCoordinatorError::ManagerClosed);
        }
        if self.service_state.current().state == ServiceState::Quiescing {
            Err(DegradedCoordinatorError::Quiescing)
        } else {
            Ok(())
        }
    }
}

fn degraded_recovery_failure() -> TaskFailure {
    TaskFailure {
        code: "STORE_WRITE_FAILED".to_owned(),
        message: "task was interrupted while recovering the task store".to_owned(),
        retryable: true,
    }
}

#[cfg(test)]
fn classify_replay_error(error: StoreWriterError) -> ReplayError {
    match error {
        StoreWriterError::Busy => ReplayError::Retryable("store writer remained busy".to_owned()),
        StoreWriterError::DeadlineElapsed => {
            ReplayError::Retryable("store writer deadline elapsed".to_owned())
        }
        StoreWriterError::Closed => ReplayError::Conflict("store writer is closed".to_owned()),
        StoreWriterError::Store(error) => ReplayError::Conflict(error.to_string()),
    }
}

fn classify_replay_submit_error(error: StoreWriterSubmitError) -> ReplayError {
    match error {
        StoreWriterSubmitError::Full | StoreWriterSubmitError::Closed => {
            ReplayError::Retryable("typed pending replay ingress is unavailable".to_owned())
        }
        StoreWriterSubmitError::InvalidIdentity
        | StoreWriterSubmitError::SequenceGap
        | StoreWriterSubmitError::SequenceReversed => {
            ReplayError::Conflict("typed pending replay identity is inconsistent".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::num::NonZeroU64;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};

    use coding_agent_domain::{
        ClientRequestId, DeliveryReadiness, EventCursor, EventId, RepositoryId, Task, TaskId,
        TaskStatus, UtcTimestamp,
    };
    use coding_agent_store::{
        PersistStopIntentOutcome, StopIntentBatchItem, StopIntentBatchReceipt, StopIntentKind,
        StopIntentReceipt, StopIntentRequest, StoreError,
    };
    use serde_json::Value;

    use super::*;

    struct PanicOnceCleanup {
        stop_calls: AtomicUsize,
        removed: AtomicBool,
    }

    #[test]
    fn stop_intent_replay_receipt_preserves_conflict_and_terminal_winner_semantics() {
        let task_id = TaskId::new();
        let repository_id = RepositoryId::new();
        let requested_at =
            UtcTimestamp::parse_rfc3339("2026-07-28T00:00:00Z").expect("valid timestamp");
        let request = StopIntentRequest {
            task_id,
            expected_repository_id: repository_id,
            expected_attempt: 1,
            kind: StopIntentKind::UserCancelled,
        };
        let identity = crate::TaskMutationIdentity {
            task_id,
            sequence: crate::MutationSequence::new(
                NonZeroU64::new(1).expect("positive mutation sequence"),
            ),
            kind: crate::DurableOperationKind::PersistStopIntent,
        };
        let pending = PendingDurableResult::PersistStopIntentBatch {
            identity: DurableOperationIdentity::stop_intent_batch(vec![identity])
                .expect("construct stop-intent batch identity"),
            requests: vec![request],
        };
        let conflicting = StopIntentReceipt {
            task_id,
            repository_id,
            attempt: 1,
            kind: StopIntentKind::DiskPressureCritical,
            requested_at,
        };
        let conflict =
            crate::PendingReplayReceipt::PersistStopIntentBatch(StopIntentBatchReceipt {
                items: vec![StopIntentBatchItem {
                    request,
                    outcome: PersistStopIntentOutcome::IntentConflict {
                        existing: conflicting,
                    },
                }],
            });
        assert!(matches!(
            classify_pending_replay_receipt(&pending, conflict),
            Err(ReplayError::Conflict(_))
        ));

        let terminal_event_id = EventId::new(7).expect("positive event id");
        let terminal_task = Task {
            id: task_id,
            client_request_id: ClientRequestId::new(),
            repository_id,
            prompt: "terminal winner".to_owned(),
            status: TaskStatus::Cancelled,
            delivery_readiness: DeliveryReadiness::Unreviewed,
            attempt: 1,
            retry_of: None,
            created_at: requested_at,
            started_at: Some(requested_at),
            finished_at: Some(requested_at),
            last_event_id: terminal_event_id,
            failure: None,
        };
        let terminal =
            crate::PendingReplayReceipt::PersistStopIntentBatch(StopIntentBatchReceipt {
                items: vec![StopIntentBatchItem {
                    request,
                    outcome: PersistStopIntentOutcome::TerminalWon {
                        current: terminal_task.clone(),
                    },
                }],
            });
        assert!(matches!(
            classify_pending_replay_receipt(&pending, terminal),
            Ok(Some(event_id)) if event_id == terminal_event_id
        ));
        let mut unstarted_terminal = terminal_task;
        unstarted_terminal.started_at = None;
        assert!(matches!(
            classify_pending_replay_receipt(
                &pending,
                crate::PendingReplayReceipt::PersistStopIntentBatch(StopIntentBatchReceipt {
                    items: vec![StopIntentBatchItem {
                        request,
                        outcome: PersistStopIntentOutcome::TerminalWon {
                            current: unstarted_terminal,
                        },
                    }],
                }),
            ),
            Err(ReplayError::Conflict(_))
        ));

        let existing = StopIntentReceipt {
            task_id,
            repository_id,
            attempt: 1,
            kind: StopIntentKind::UserCancelled,
            requested_at,
        };
        let idempotent =
            crate::PendingReplayReceipt::PersistStopIntentBatch(StopIntentBatchReceipt {
                items: vec![StopIntentBatchItem {
                    request,
                    outcome: PersistStopIntentOutcome::Existing(existing),
                }],
            });
        assert!(matches!(
            classify_pending_replay_receipt(&pending, idempotent),
            Ok(None)
        ));
    }

    #[test]
    fn quality_replay_receipts_require_exact_request_and_terminal_tuples() {
        let task_id = TaskId::new();
        let repository_id = RepositoryId::new();
        let created_at =
            UtcTimestamp::parse_rfc3339("2026-07-28T00:00:00Z").expect("valid timestamp");
        let evidence = crate::fake_runner::approved_evidence();
        let review_event_id = EventId::new(7).expect("positive review event id");
        let exact_review = ReviewEvidence::try_from_new(evidence.clone(), created_at)
            .expect("construct exact stored review");
        let review_identity = crate::TaskMutationIdentity {
            task_id,
            sequence: crate::MutationSequence::new(
                NonZeroU64::new(1).expect("positive mutation sequence"),
            ),
            kind: DurableOperationKind::RecordReview,
        };
        let review_request = RecordReviewRequest {
            task_id,
            expected_repository_id: repository_id,
            expected_attempt: 1,
            evidence: evidence.clone(),
        };
        let pending_review = PendingDurableResult::RecordReview {
            identity: review_identity,
            request: review_request,
        };
        assert!(matches!(
            classify_pending_replay_receipt(
                &pending_review,
                PendingReplayReceipt::RecordReview(RecordReviewOutcome::Existing {
                    review: exact_review.clone(),
                    event_id: review_event_id,
                }),
            ),
            Ok(Some(event_id)) if event_id == review_event_id
        ));

        let mut conflicting_value =
            serde_json::to_value(&evidence).expect("serialize conflicting evidence");
        conflicting_value
            .as_object_mut()
            .expect("review evidence is an object")
            .insert(
                "summary".to_owned(),
                Value::String("a different durable review".to_owned()),
            );
        let conflicting_evidence: NewReviewEvidence =
            serde_json::from_value(conflicting_value).expect("construct conflicting evidence");
        let conflicting_review = ReviewEvidence::try_from_new(conflicting_evidence, created_at)
            .expect("construct conflicting stored review");
        assert!(matches!(
            classify_pending_replay_receipt(
                &pending_review,
                PendingReplayReceipt::RecordReview(RecordReviewOutcome::Existing {
                    review: conflicting_review,
                    event_id: review_event_id,
                }),
            ),
            Err(ReplayError::Conflict(_))
        ));

        let terminal_event_id = EventId::new(8).expect("positive terminal event id");
        let terminal_task = Task {
            id: task_id,
            client_request_id: ClientRequestId::new(),
            repository_id,
            prompt: "strict quality replay".to_owned(),
            status: TaskStatus::Completed,
            delivery_readiness: DeliveryReadiness::ReviewApproved,
            attempt: 1,
            retry_of: None,
            created_at,
            started_at: Some(created_at),
            finished_at: Some(created_at),
            last_event_id: terminal_event_id,
            failure: None,
        };
        let final_identity = crate::TaskMutationIdentity {
            task_id,
            sequence: crate::MutationSequence::new(
                NonZeroU64::new(2).expect("positive mutation sequence"),
            ),
            kind: DurableOperationKind::FinalizeReviewedTask,
        };
        let pending_final = PendingDurableResult::FinalizeReviewedTask {
            identity: final_identity,
            request: FinalizeReviewedTaskRequest {
                task_id,
                expected_repository_id: repository_id,
                expected_attempt: 1,
                evidence,
            },
        };
        let exact_final =
            PendingReplayReceipt::FinalizeReviewedTask(FinalizeReviewedTaskOutcome::Existing {
                task: terminal_task.clone(),
                review: exact_review.clone(),
                review_event_id,
                terminal_event_id,
            });
        assert!(matches!(
            classify_pending_replay_receipt(&pending_final, exact_final),
            Ok(Some(event_id)) if event_id == terminal_event_id
        ));
        let mut mismatched_terminal = terminal_task;
        mismatched_terminal.last_event_id = review_event_id;
        assert!(matches!(
            classify_pending_replay_receipt(
                &pending_final,
                PendingReplayReceipt::FinalizeReviewedTask(FinalizeReviewedTaskOutcome::Existing {
                    task: mismatched_terminal,
                    review: exact_review,
                    review_event_id,
                    terminal_event_id,
                },),
            ),
            Err(ReplayError::Conflict(_))
        ));

        let interrupted_event_id = EventId::new(9).expect("positive interrupted event id");
        let interruption = TaskFailure {
            code: "APP_SHUTDOWN".to_owned(),
            message: "application shut down before the task finished".to_owned(),
            retryable: true,
        };
        let unreviewed_request = FinalizeUnreviewedTaskRequest {
            task_id,
            expected_repository_id: repository_id,
            expected_attempt: 1,
            transition: coding_agent_store::TaskTransition::Interrupted(interruption.clone()),
        };
        let pending_unreviewed = PendingDurableResult::FinalizeUnreviewedTask {
            identity: crate::TaskMutationIdentity {
                task_id,
                sequence: crate::MutationSequence::new(
                    NonZeroU64::new(3).expect("positive mutation sequence"),
                ),
                kind: DurableOperationKind::FinalizeUnreviewedTask,
            },
            request: unreviewed_request,
        };
        let interrupted_task = Task {
            id: task_id,
            client_request_id: ClientRequestId::new(),
            repository_id,
            prompt: "strict unreviewed replay".to_owned(),
            status: TaskStatus::Interrupted,
            delivery_readiness: DeliveryReadiness::Unreviewed,
            attempt: 1,
            retry_of: None,
            created_at,
            started_at: Some(created_at),
            finished_at: Some(created_at),
            last_event_id: interrupted_event_id,
            failure: Some(interruption),
        };
        assert!(matches!(
            classify_pending_replay_receipt(
                &pending_unreviewed,
                PendingReplayReceipt::FinalizeUnreviewedTask(
                    FinalizeUnreviewedTaskOutcome::Existing {
                        task: interrupted_task.clone(),
                        event_id: interrupted_event_id,
                    },
                ),
            ),
            Ok(Some(event_id)) if event_id == interrupted_event_id
        ));
        let mut mismatched_interrupted = interrupted_task;
        mismatched_interrupted.failure = None;
        assert!(matches!(
            classify_pending_replay_receipt(
                &pending_unreviewed,
                PendingReplayReceipt::FinalizeUnreviewedTask(
                    FinalizeUnreviewedTaskOutcome::Existing {
                        task: mismatched_interrupted,
                        event_id: interrupted_event_id,
                    },
                ),
            ),
            Err(ReplayError::Conflict(_))
        ));
    }

    #[async_trait::async_trait]
    impl ShutdownCleanup for PanicOnceCleanup {
        async fn stop_http(&self, deadline: Instant) {
            let panic_at = deadline
                .checked_sub(Duration::from_millis(100))
                .unwrap_or(deadline);
            tokio::time::sleep_until(panic_at).await;
            if self.stop_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("injected shutdown cleanup panic");
            }
        }

        fn stop_http_now(&self) {}

        fn unpublish_descriptor(&self) {
            self.removed.store(true, Ordering::SeqCst);
        }

        fn finish_lock(
            &self,
            _proof: ShutdownRuntimeCleanupProof,
            _disposition: ShutdownLockDisposition,
        ) {
            self.removed.store(true, Ordering::SeqCst);
        }
    }

    struct FixedWallClock;

    impl WallClock for FixedWallClock {
        fn now_utc(&self) -> time::OffsetDateTime {
            time::macros::datetime!(2026-07-15 00:00 UTC)
        }
    }

    struct SilentMessages;

    impl NativeMessageSink for SilentMessages {
        fn show_error(&self, _title: &'static str, _body: String) {}
    }

    struct BlockingMarkerWriter {
        entered: Mutex<Option<oneshot::Sender<()>>>,
        release: Arc<(Mutex<bool>, Condvar)>,
    }

    impl ShutdownMarkerWriter for BlockingMarkerWriter {
        fn write(
            &self,
            _path: &Path,
            _instance_id: Uuid,
            _timestamp: time::OffsetDateTime,
        ) -> io::Result<()> {
            if let Some(entered) = self
                .entered
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
            {
                let _ = entered.send(());
            }
            let (released, wake) = &*self.release;
            let mut released = released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while !*released {
                released = wake
                    .wait(released)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_marker_write_does_not_consume_the_cleanup_reserve() {
        let (entered, entered_rx) = oneshot::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let writer = Arc::new(BlockingMarkerWriter {
            entered: Mutex::new(Some(entered)),
            release: release.clone(),
        });
        let deadline = Instant::now() + Duration::from_secs(1);
        let write = tokio::spawn(write_shutdown_marker_until(
            writer,
            PathBuf::from("unused-marker"),
            Uuid::new_v4(),
            time::macros::datetime!(2026-07-15 00:00 UTC),
            deadline,
        ));
        entered_rx.await.expect("blocking marker writer starts");

        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(matches!(
            write.await.expect("marker deadline task"),
            Err(ShutdownMarkerWriteError::Deadline)
        ));

        let (released, wake) = &*release;
        *released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        wake.notify_one();
    }

    #[tokio::test]
    async fn a_successful_marker_write_uses_an_immutable_instance_path() {
        let directory = tempfile::tempdir().expect("create instance-marker directory");
        let canonical = directory.path().join("unclean-shutdown.json");
        let instance_id = Uuid::new_v4();

        write_shutdown_marker_until(
            Arc::new(FilesystemShutdownMarkerWriter),
            canonical.clone(),
            instance_id,
            time::macros::datetime!(2026-07-15 00:00 UTC),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect("write immutable instance marker");

        let instance_path = shutdown_marker_instance_path(&canonical, instance_id)
            .expect("construct immutable instance marker path");
        assert!(instance_path.is_file());
        assert!(
            !canonical.exists(),
            "a shutdown worker must never publish or replace a shared canonical path"
        );
    }

    #[cfg(feature = "test-support")]
    #[test]
    fn process_test_marker_failure_is_deterministic_and_creates_no_file() {
        let directory = tempfile::tempdir().expect("create marker-failure directory");
        let path = directory.path().join("unclean-shutdown.json");

        let error = ProcessTestFailingShutdownMarkerWriter
            .write(
                &path,
                Uuid::new_v4(),
                time::macros::datetime!(2026-07-15 00:00 UTC),
            )
            .expect_err("the process-test writer always rejects marker creation");

        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(
            std::fs::read_dir(directory.path())
                .expect("scan marker-failure directory")
                .next()
                .is_none(),
            "marker failure must not leave a partial file"
        );
    }

    #[tokio::test]
    async fn a_late_worker_panic_reuses_the_original_absolute_shutdown_deadline() {
        let directory = tempfile::tempdir().expect("create panic-supervision directory");
        let paths = PlatformPaths::new(directory.path().join("data"), directory.path().join("run"));
        paths.prepare().expect("prepare panic-supervision paths");
        let store = Store::open(&paths.database_path)
            .await
            .expect("open panic-supervision store");
        store
            .migrate()
            .await
            .expect("migrate panic-supervision store");
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 8)
            .await
            .expect("spawn panic-supervision dispatcher");
        let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
        let state = ServiceStateController::new(ServiceState::Ready);
        let manager = TaskManagerHandle::spawn(
            store.clone(),
            writer,
            dispatcher.clone(),
            state.clone(),
            Arc::new(crate::FakeTaskRunner::default()),
            crate::task_manager::test_task_manager_launch_resources(1, 1),
            8,
        );
        let cleanup = Arc::new(PanicOnceCleanup {
            stop_calls: AtomicUsize::new(0),
            removed: AtomicBool::new(false),
        });
        let mutation_gate = MutationGate::new(state.clone());
        let mutation_guard = mutation_gate
            .enter_data_mutation()
            .expect("hold a non-cooperative mutation until the late panic path completes");
        let instance_id = Uuid::new_v4();
        let instance_process_scope =
            coding_agent_runtime::ProcessLivenessDirectory::open(&paths.runtime_dir)
                .expect("open panic-supervision process-liveness directory")
                .instance_scope(*instance_id.as_bytes())
                .expect("create panic-supervision process-liveness scope");
        let coordinator = ShutdownCoordinator::new(
            mutation_gate,
            instance_process_scope,
            manager,
            dispatcher,
            store,
            cleanup.clone(),
            &paths,
            instance_id,
            Arc::new(FixedWallClock),
            Arc::new(SilentMessages),
        );

        tokio::time::pause();
        let shutdown = tokio::spawn(async move { coordinator.shutdown().await });
        settle().await;
        for _ in 0..100 {
            tokio::time::advance(Duration::from_millis(100)).await;
            settle().await;
        }
        assert!(
            shutdown.is_finished(),
            "panic fallback must not receive a fresh ten-second budget (stop calls: {}, descriptor removed: {})",
            cleanup.stop_calls.load(Ordering::SeqCst),
            cleanup.removed.load(Ordering::SeqCst),
        );
        let outcome = shutdown.await.expect("join supervised shutdown worker");

        assert_eq!(outcome, ShutdownOutcome::Degraded);
        assert!(cleanup.removed.load(Ordering::SeqCst));
        assert_eq!(cleanup.stop_calls.load(Ordering::SeqCst), 1);
        drop(mutation_guard);
    }

    #[test]
    fn shutdown_marker_contains_only_the_approved_diagnostic_fields() {
        let directory = tempfile::tempdir().expect("create shutdown marker directory");
        let path = directory.path().join("unclean-shutdown.json");
        let instance_id = Uuid::new_v4();

        write_shutdown_marker(
            &path,
            instance_id,
            time::macros::datetime!(2026-07-15 00:00 UTC),
        )
        .expect("write private shutdown marker");

        let document: Value =
            serde_json::from_slice(&std::fs::read(&path).expect("read private shutdown marker"))
                .expect("parse private shutdown marker");
        let object = document.as_object().expect("marker is an object");
        let mut fields = object.keys().map(String::as_str).collect::<Vec<_>>();
        fields.sort_unstable();
        assert_eq!(fields, ["error_code", "instance_id", "timestamp"]);
        assert_eq!(object["instance_id"], instance_id.hyphenated().to_string());
        assert_eq!(object["error_code"], SHUTDOWN_MARKER_ERROR_CODE);
        assert_eq!(object["timestamp"], "2026-07-15T00:00:00Z");
    }

    #[test]
    fn a_non_file_marker_collision_is_a_best_effort_write_failure() {
        let directory = tempfile::tempdir().expect("create shutdown marker directory");
        let path = directory.path().join("unclean-shutdown.json");
        std::fs::create_dir(&path).expect("create marker-path directory collision");

        write_shutdown_marker(
            &path,
            Uuid::new_v4(),
            time::macros::datetime!(2026-07-15 00:00 UTC),
        )
        .expect_err("a directory cannot be accepted as an existing marker");

        assert!(path.is_dir());
    }

    #[test]
    fn an_existing_regular_marker_is_not_silently_accepted_as_this_instances_write() {
        let directory = tempfile::tempdir().expect("create shutdown marker directory");
        let path = directory.path().join("unclean-shutdown.json");
        std::fs::write(&path, b"preexisting marker").expect("create regular marker collision");

        write_shutdown_marker(
            &path,
            Uuid::new_v4(),
            time::macros::datetime!(2026-07-15 00:00 UTC),
        )
        .expect_err("a preexisting marker must remain a best-effort write failure");

        assert_eq!(
            std::fs::read(&path).expect("read untouched preexisting marker"),
            b"preexisting marker"
        );
    }

    struct ScriptedBackend {
        replay: Mutex<VecDeque<Result<EventId, ReplayError>>>,
        replayed: Mutex<Vec<PendingDurableResult>>,
        call_order: Mutex<Vec<&'static str>>,
        recover: Mutex<VecDeque<Result<RecoveryOutcome, String>>>,
        flush: Mutex<VecDeque<Result<(), String>>>,
        recover_calls: AtomicUsize,
        flush_calls: AtomicUsize,
        first_recover_barrier: Option<Arc<RecoverBarrier>>,
    }

    #[derive(Default)]
    struct RecoverBarrier {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl RecoverBarrier {
        async fn wait_until_entered(&self) {
            self.entered.notified().await;
        }

        fn release(&self) {
            self.release.notify_one();
        }
    }

    #[async_trait::async_trait]
    impl RecoveryBackend for ScriptedBackend {
        async fn replay(
            &self,
            pending: &PendingDurableResult,
        ) -> Result<Option<EventId>, ReplayError> {
            self.call_order
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push("replay");
            self.replayed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(pending.clone());
            self.replay
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .expect("scripted replay result")
                .map(Some)
        }

        async fn recover(&self) -> Result<RecoveryOutcome, String> {
            self.call_order
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push("recover");
            let attempt = self.recover_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt == 1
                && let Some(barrier) = &self.first_recover_barrier
            {
                barrier.entered.notify_one();
                barrier.release.notified().await;
            }
            self.recover
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .expect("scripted recover result")
        }

        async fn flush(&self, _high_watermark: EventCursor) -> Result<(), String> {
            self.call_order
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push("flush");
            self.flush_calls.fetch_add(1, Ordering::SeqCst);
            self.flush
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .expect("scripted flush result")
        }
    }

    impl ScriptedBackend {
        fn new(
            recover: impl IntoIterator<Item = Result<RecoveryOutcome, String>>,
            flush: impl IntoIterator<Item = Result<(), String>>,
        ) -> Self {
            Self {
                replay: Mutex::new(VecDeque::new()),
                replayed: Mutex::new(Vec::new()),
                call_order: Mutex::new(Vec::new()),
                recover: Mutex::new(recover.into_iter().collect()),
                flush: Mutex::new(flush.into_iter().collect()),
                recover_calls: AtomicUsize::new(0),
                flush_calls: AtomicUsize::new(0),
                first_recover_barrier: None,
            }
        }

        fn with_first_recover_barrier(mut self, barrier: Arc<RecoverBarrier>) -> Self {
            self.first_recover_barrier = Some(barrier);
            self
        }

        fn with_replay(
            self,
            replay: impl IntoIterator<Item = Result<EventId, ReplayError>>,
        ) -> Self {
            *self
                .replay
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = replay.into_iter().collect();
            self
        }

        fn recover_calls(&self) -> usize {
            self.recover_calls.load(Ordering::SeqCst)
        }

        fn flush_calls(&self) -> usize {
            self.flush_calls.load(Ordering::SeqCst)
        }

        fn replayed(&self) -> Vec<PendingDurableResult> {
            self.replayed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        fn call_order(&self) -> Vec<&'static str> {
            self.call_order
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    #[tokio::test]
    async fn a_flush_failure_retries_only_flush_after_recovery_commits() {
        tokio::time::pause();
        let backend = Arc::new(ScriptedBackend::new(
            [Ok(recovery())],
            [Err("read failed".to_owned()), Ok(())],
        ));
        let state = degraded_state();
        let (manager, mut messages) = mpsc::channel(8);
        let coordinator =
            DegradedCoordinator::with_backend(backend.clone(), state.clone(), manager.downgrade());
        let run = tokio::spawn(async move { coordinator.run(Vec::new()).await });

        wait_for_calls(&backend.flush_calls, 1).await;
        settle().await;
        assert_eq!(backend.recover_calls(), 1);
        tokio::time::advance(RECOVERY_RETRY_INTERVAL + Duration::from_millis(1)).await;
        wait_for_calls(&backend.flush_calls, 2).await;
        complete_finalization(&mut messages, &state, 0).await;

        let result = run.await.unwrap().unwrap();
        assert_eq!(backend.recover_calls(), 1);
        assert_eq!(backend.flush_calls(), 2);
        assert_eq!(result.replayed_pending_count, 0);
        assert_eq!(result.ready_generation, 2);
    }

    #[tokio::test]
    async fn recover_failure_retries_recovery_without_flushing_early() {
        tokio::time::pause();
        let backend = Arc::new(ScriptedBackend::new(
            [Err("write failed".to_owned()), Ok(recovery())],
            [Ok(())],
        ));
        let state = degraded_state();
        let (manager, mut messages) = mpsc::channel(8);
        let coordinator =
            DegradedCoordinator::with_backend(backend.clone(), state.clone(), manager.downgrade());
        let run = tokio::spawn(async move { coordinator.run(Vec::new()).await });

        wait_for_calls(&backend.recover_calls, 1).await;
        settle().await;
        assert_eq!(backend.flush_calls(), 0);
        tokio::time::advance(RECOVERY_RETRY_INTERVAL + Duration::from_millis(1)).await;
        wait_for_calls(&backend.recover_calls, 2).await;
        wait_for_calls(&backend.flush_calls, 1).await;
        complete_finalization(&mut messages, &state, 0).await;

        run.await.unwrap().unwrap();
        assert_eq!(backend.recover_calls(), 2);
        assert_eq!(backend.flush_calls(), 1);
    }

    #[tokio::test]
    async fn typed_pending_replays_in_original_order_before_recovery() {
        let pending = vec![pending_finalize(), pending_finalize()];
        let backend = Arc::new(
            ScriptedBackend::new([Ok(recovery())], [Ok(())])
                .with_replay([Ok(EventId::new(2).unwrap()), Ok(EventId::new(3).unwrap())]),
        );
        let state = degraded_state();
        let (manager, mut messages) = mpsc::channel(8);
        let coordinator =
            DegradedCoordinator::with_backend(backend.clone(), state.clone(), manager.downgrade());
        let expected = pending.clone();
        let run = tokio::spawn(async move { coordinator.run(pending).await });

        wait_for_calls(&backend.recover_calls, 1).await;
        complete_finalization(&mut messages, &state, 2).await;

        let result = run.await.unwrap().unwrap();
        assert_eq!(result.replayed_pending_count, 2);
        assert_eq!(backend.replayed(), expected);
        assert_eq!(
            backend.call_order(),
            vec!["replay", "replay", "recover", "flush"]
        );
    }

    #[test]
    fn typed_replay_retries_only_store_writer_busy() {
        assert!(matches!(
            classify_replay_error(StoreWriterError::Busy),
            ReplayError::Retryable(_)
        ));
        assert!(matches!(
            classify_replay_error(StoreWriterError::Closed),
            ReplayError::Conflict(_)
        ));
        assert!(matches!(
            classify_replay_error(StoreWriterError::Store(StoreError::InvariantViolation(
                "permanent replay failure"
            ))),
            ReplayError::Conflict(_)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn busy_typed_replay_retries_then_succeeds_without_freezing() {
        let pending = vec![pending_finalize()];
        let expected = pending.clone();
        let backend = Arc::new(
            ScriptedBackend::new([Ok(recovery())], [Ok(())]).with_replay([
                Err(classify_replay_error(StoreWriterError::Busy)),
                Ok(EventId::new(2).unwrap()),
            ]),
        );
        let state = degraded_state();
        let (manager, mut messages) = mpsc::channel(8);
        let coordinator =
            DegradedCoordinator::with_backend(backend.clone(), state.clone(), manager.downgrade());
        let run = tokio::spawn(async move { coordinator.run(pending).await });

        wait_for_replays(&backend, 1).await;
        settle().await;
        assert!(matches!(
            messages.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        tokio::time::advance(RECOVERY_RETRY_INTERVAL + Duration::from_millis(1)).await;
        wait_for_calls(&backend.recover_calls, 1).await;
        complete_finalization(&mut messages, &state, 1).await;

        let result = run.await.unwrap().unwrap();
        assert_eq!(result.replayed_pending_count, 1);
        assert_eq!(
            backend.replayed(),
            vec![expected[0].clone(), expected[0].clone()]
        );
        assert_eq!(
            backend.call_order(),
            vec!["replay", "replay", "recover", "flush"]
        );
    }

    #[tokio::test]
    async fn non_busy_sqlite_replay_error_freezes_and_returns_exact_pending() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory SQLite database");
        sqlx::query(
            "CREATE TABLE replay_constraint (
                id INTEGER PRIMARY KEY,
                value TEXT NOT NULL UNIQUE
            )",
        )
        .execute(&pool)
        .await
        .expect("create replay constraint fixture");
        sqlx::query("INSERT INTO replay_constraint (id, value) VALUES (1, 'duplicate')")
            .execute(&pool)
            .await
            .expect("seed replay constraint fixture");
        let database_error =
            sqlx::query("INSERT INTO replay_constraint (id, value) VALUES (2, 'duplicate')")
                .execute(&pool)
                .await
                .expect_err("duplicate value must fail");
        let database_code = database_error
            .as_database_error()
            .and_then(|error| error.code())
            .map(|code| code.into_owned())
            .expect("SQLite constraint error has a code");
        assert!(
            !crate::store_writer::sqlite_code_is_retryable(&database_code),
            "constraint code {database_code} must not be classified as BUSY/LOCKED"
        );

        let replay_error = classify_replay_error(StoreWriterError::Store(StoreError::Database(
            database_error,
        )));
        assert!(matches!(
            &replay_error,
            ReplayError::Conflict(message) if message.contains("UNIQUE constraint failed")
        ));

        let pending = vec![pending_finalize()];
        let expected = pending.clone();
        let backend = Arc::new(
            ScriptedBackend::new(std::iter::empty(), std::iter::empty())
                .with_replay([Err(replay_error)]),
        );
        let state = degraded_state();
        let (manager, mut messages) = mpsc::channel(8);
        let coordinator =
            DegradedCoordinator::with_backend(backend.clone(), state.clone(), manager.downgrade());
        let run = tokio::spawn(async move { coordinator.run(pending).await });

        let TaskManagerMessage::FreezeDegraded {
            pending: retained,
            response,
        } = messages.recv().await.expect("freeze message")
        else {
            panic!("non-BUSY database error must freeze degraded recovery");
        };
        assert_eq!(retained, expected);
        response.send(()).unwrap();

        assert_eq!(
            run.await.unwrap(),
            Err(DegradedCoordinatorError::TypedConflict)
        );
        assert_eq!(backend.replayed(), expected);
        assert_eq!(backend.recover_calls(), 0);
        assert_eq!(backend.flush_calls(), 0);
        assert_eq!(backend.call_order(), vec!["replay"]);
        assert_eq!(state.current().state, ServiceState::StoreDegraded);
    }

    #[tokio::test(start_paused = true)]
    async fn quiescing_during_busy_typed_replay_backoff_never_freezes() {
        let backend = Arc::new(
            ScriptedBackend::new(std::iter::empty(), std::iter::empty())
                .with_replay([Err(classify_replay_error(StoreWriterError::Busy))]),
        );
        let state = degraded_state();
        let (manager, mut messages) = mpsc::channel(8);
        let coordinator =
            DegradedCoordinator::with_backend(backend.clone(), state.clone(), manager.downgrade());
        let run = tokio::spawn(async move { coordinator.run(vec![pending_finalize()]).await });

        wait_for_replays(&backend, 1).await;
        settle().await;
        state.set(ServiceState::Quiescing).unwrap();

        assert_eq!(run.await.unwrap(), Err(DegradedCoordinatorError::Quiescing));
        assert_eq!(backend.replayed().len(), 1);
        assert!(matches!(
            messages.try_recv(),
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
        ));
    }

    #[tokio::test]
    async fn quiescing_during_flush_retry_never_finalizes_ready() {
        tokio::time::pause();
        let backend = Arc::new(ScriptedBackend::new(
            [Ok(recovery())],
            [Err("read failed".to_owned())],
        ));
        let state = degraded_state();
        let (manager, mut messages) = mpsc::channel(8);
        let coordinator =
            DegradedCoordinator::with_backend(backend.clone(), state.clone(), manager.downgrade());
        let run = tokio::spawn(async move { coordinator.run(Vec::new()).await });

        wait_for_calls(&backend.flush_calls, 1).await;
        settle().await;
        state.set(ServiceState::Quiescing).unwrap();

        assert_eq!(run.await.unwrap(), Err(DegradedCoordinatorError::Quiescing));
        assert_eq!(backend.recover_calls(), 1);
        assert_eq!(backend.flush_calls(), 1);
        assert!(matches!(
            messages.try_recv(),
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
        ));
        assert_eq!(state.current().state, ServiceState::Quiescing);
    }

    #[tokio::test(start_paused = true)]
    async fn manager_closure_during_recovery_backoff_stops_without_retrying() {
        let recover_barrier = Arc::new(RecoverBarrier::default());
        let backend = Arc::new(
            ScriptedBackend::new([Err("write failed".to_owned())], std::iter::empty())
                .with_first_recover_barrier(recover_barrier.clone()),
        );
        let state = degraded_state();
        let (manager, messages) = mpsc::channel(8);
        let weak_manager = manager.downgrade();
        let coordinator =
            DegradedCoordinator::with_backend(backend.clone(), state, weak_manager.clone());
        let run = tokio::spawn(async move { coordinator.run(Vec::new()).await });

        recover_barrier.wait_until_entered().await;
        assert_eq!(backend.recover_calls(), 1);
        recover_barrier.release();
        wait_for_strong_senders(&weak_manager, 2).await;

        drop(messages);

        let result = tokio::time::timeout(Duration::from_millis(1), run)
            .await
            .expect("manager closure must wake the recovery backoff")
            .expect("join degraded coordinator");

        assert_eq!(result, Err(DegradedCoordinatorError::ManagerClosed));
        assert_eq!(backend.recover_calls(), 1);
        drop(manager);
    }

    fn degraded_state() -> ServiceStateController {
        let state = ServiceStateController::new(ServiceState::Ready);
        state.set(ServiceState::StoreDegraded).unwrap();
        state
    }

    fn recovery() -> RecoveryOutcome {
        let event_id = EventId::new(1).unwrap();
        RecoveryOutcome {
            interrupted_count: 1,
            first_event_id: Some(event_id),
            last_event_id: Some(event_id),
            high_watermark: EventCursor::new(event_id.get()).unwrap(),
        }
    }

    fn pending_finalize() -> PendingDurableResult {
        let task_id = TaskId::new();
        PendingDurableResult::FinalizeReviewedTask {
            identity: crate::TaskMutationIdentity {
                task_id,
                sequence: crate::MutationSequence::new(NonZeroU64::new(1).unwrap()),
                kind: crate::DurableOperationKind::FinalizeReviewedTask,
            },
            request: crate::FinalizeReviewedTaskRequest {
                task_id,
                expected_repository_id: RepositoryId::new(),
                expected_attempt: 1,
                evidence: crate::fake_runner::approved_evidence(),
            },
        }
    }

    async fn complete_finalization(
        messages: &mut mpsc::Receiver<TaskManagerMessage>,
        state: &ServiceStateController,
        expected_replayed_pending_count: usize,
    ) {
        let TaskManagerMessage::FinalizeDegraded {
            attempt_id: _,
            barrier_epoch: _,
            recovery,
            replayed_pending_count,
            high_watermark,
            response,
        } = messages.recv().await.expect("finalization message")
        else {
            panic!("unexpected task-manager message");
        };
        assert_eq!(replayed_pending_count, expected_replayed_pending_count);
        assert!(high_watermark >= recovery.high_watermark);
        let ready = state.set(ServiceState::Ready).unwrap();
        response
            .send(Ok(DegradedRecoveryResult {
                recovery,
                replayed_pending_count,
                ready_generation: ready.generation,
            }))
            .unwrap();
    }

    async fn wait_for_calls(calls: &AtomicUsize, expected: usize) {
        for _ in 0..100 {
            if calls.load(Ordering::SeqCst) == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("call count did not reach {expected}");
    }

    async fn wait_for_replays(backend: &ScriptedBackend, expected: usize) {
        for _ in 0..100 {
            if backend.replayed().len() == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("replay count did not reach {expected}");
    }

    async fn wait_for_strong_senders(
        sender: &mpsc::WeakSender<TaskManagerMessage>,
        expected: usize,
    ) {
        for _ in 0..100 {
            if sender.strong_count() == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("strong sender count did not reach {expected}");
    }

    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }
}
