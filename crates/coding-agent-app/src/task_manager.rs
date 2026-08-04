use std::collections::{HashMap, HashSet, VecDeque};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(all(test, feature = "test-support"))]
use coding_agent_domain::UtcTimestamp;
use coding_agent_domain::{
    ActivityEntry, DiffSnapshot, EventCursor, EventId, NewReviewEvidence, PlanSnapshot, Repository,
    RepositoryId, ReviewEvidence, ReviewVerdict, Task, TaskEventKind, TaskEventPayload,
    TaskFailure, TaskId, TaskStatus, TestSnapshot,
};
use coding_agent_runtime::{ProcessCleanupProof, ProcessLivenessScope};
use coding_agent_store::{
    AppendEventOutcome, ClaimTaskOutcome, ClaimTaskReceipt, ClaimTaskReconciliationOutcome,
    ClaimTaskRequest, FinalizeReviewedTaskOutcome, FinalizeStoppedTaskOutcome,
    FinalizeStoppedTaskRequest, FinalizeUnreviewedTaskOutcome, PersistStopIntentOutcome,
    RecordReviewOutcome, SchedulerBootstrapSnapshot, StopIntentBatchReceipt, StopIntentKind,
    StopIntentReceipt, StopIntentRequest, Store, StoreError, TaskTransition, TransitionOutcome,
};
use tokio::sync::{Notify, broadcast, mpsc, oneshot};
use tokio::time::{Instant, MissedTickBehavior, timeout_at};
use tokio_util::sync::CancellationToken;

mod active_ownership;
#[cfg(any(test, feature = "test-support"))]
mod actor_test_support;
mod cancel;
mod claim_admission;
mod claim_launch;
mod claim_persistence;
mod critical_stop;
mod degraded_finalization;
mod degraded_recovery;
mod degraded_state;
mod final_stop;
mod quality_failure;
mod quiesce;
mod recovery_barrier;
mod repository_control_recovery;
mod runner_completion;
mod running_mutation;
mod scan_admission;
mod scheduler_projection;
mod scheduler_refresh;
mod stop_intent;
mod stop_projection;
mod stop_submission;
mod storage_activity;
mod terminal_lifecycle;
mod terminal_projection;
mod terminal_release;
#[cfg(test)]
mod test_dispatch;

use repository_control_recovery::ActiveRepositoryControlRecovery;
use scheduler_projection::{SchedulerProjectionBridge, SchedulerProjectionPublishError};
#[cfg(test)]
use storage_activity::{StorageActivityCompletionPauseForTest, StorageActivitySyncSnapshotForTest};
use storage_activity::{StorageActivitySubmission, StorageActivitySynchronizer};
use terminal_projection::{
    TerminalProjectionAttempt, TerminalProjectionBarrier, TerminalProjectionCompletion,
    TerminalProjectionCompletionDisposition,
};
use terminal_release::{
    ProjectedTerminalReleaseRequest, RecoveryTerminalReleaseRequest, TerminalReleaseCommit,
    commit_projected_terminal_release, commit_recovery_terminal_release,
};

#[cfg(test)]
use crate::TerminalProcessCleanReleaseProof;
use crate::repository_control::{RepositoryControlRecoveryState, RepositoryControlRecoveryWitness};
use crate::scheduler::{SchedulerStateReader, SchedulerStoreState};
use crate::scheduler_api_projection::{SchedulerPublicLimits, SchedulerRuntimeProjection};
#[cfg(feature = "test-support")]
use crate::test_support::{ActorPauseController, ActorPausePoint};
use crate::{
    DegradedCoordinator, DegradedCoordinatorError, DegradedRecoveryResult, DurableCompletion,
    DurableDisposition, DurableOperationIdentity, DurableOperationKind, EventDispatcherError,
    EventDispatcherHandle, FinalizeReviewedTaskRequest, FinalizeUnreviewedTaskRequest,
    KnownNotAppliedReason, MonitoredStorageScope, MutationSequence, MutationSequenceDisposition,
    PendingDurableResult, PendingDurableSubmission, PendingReplayReceipt, PermitLedger,
    PermitLedgerError, QueuedTaskCandidate, RecordReviewRequest, RepositoryControlCoordinator,
    RepositoryControlError, RepositoryControlLease, RepositoryCoordinationKey, RunContext,
    SchedulerAdmissionGates, SchedulerConcurrencyLimits, SchedulerStorageNotification,
    SchedulerStorageNotificationSink, ServiceState, ServiceStateController, SharedPermitOwnership,
    StorageActivity, StorageCriticalNotification, StorageCriticalNotificationSink,
    StorageMonitorError, StorageMonitorHandle, StorageMonitorSnapshot, StorageState,
    StoreWriterError, StoreWriterHandle, StoreWriterSubmitError, TaskMutationIdentity,
    scan_queued_candidates,
};

const RECONCILE_INTERVAL: Duration = Duration::from_millis(100);
const BACKGROUND_WRITE_BUDGET: Duration = Duration::from_secs(5);
const CRITICAL_STOP_PERSISTENCE_BUDGET: Duration = Duration::from_secs(1);
const STOP_WRITE_RETRY_LIMIT: u8 = 1;
const PROCESS_CLEANUP_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const PENDING_REPLAY_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const TERMINAL_PROJECTION_RETRY_INTERVAL: Duration = Duration::from_millis(100);

fn classify_refreshed_storage(
    snapshot: &StorageMonitorSnapshot,
    repository_id: RepositoryId,
) -> RefreshedStorageAdmission {
    if snapshot.data_state() != Some(StorageState::Normal)
        || snapshot.runtime_state() != Some(StorageState::Normal)
    {
        RefreshedStorageAdmission::GlobalBlocked
    } else if snapshot.repository_state(repository_id) != Some(StorageState::Normal) {
        RefreshedStorageAdmission::RepositoryBlocked
    } else {
        RefreshedStorageAdmission::Ready
    }
}

fn storage_snapshot_allows_claimed_launch(
    snapshot: &StorageMonitorSnapshot,
    repository_id: RepositoryId,
) -> bool {
    storage_state_allows_claimed_launch(snapshot.data_state())
        && storage_state_allows_claimed_launch(snapshot.runtime_state())
        && storage_state_allows_claimed_launch(snapshot.repository_state(repository_id))
}

fn storage_state_allows_claimed_launch(state: Option<StorageState>) -> bool {
    matches!(
        state,
        Some(StorageState::Normal | StorageState::Pressure | StorageState::Unavailable)
    )
}

#[async_trait::async_trait]
pub trait TaskRunner: Send + Sync + 'static {
    async fn run(&self, context: RunContext, sink: RunnerEventSink) -> RunnerOutcome;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunnerOutcome {
    Approved(NewReviewEvidence),
    Rejected(NewReviewEvidence),
    Cancelled,
    Failed(TaskFailure),
    /// The runner has no final OS proof that every child process exited.
    /// This is an internal paused state, never a terminal task transition.
    ProcessCleanupUnproven,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelOutcome {
    Cancelled { task: Task },
    Finished { task: Task },
    Accepted { task: Task },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunnerEvent {
    PlanUpdated(PlanSnapshot),
    ActivityAppended(ActivityEntry),
    DiffUpdated(DiffSnapshot),
    TestUpdated(TestSnapshot),
}

impl RunnerEvent {
    fn into_payload(self) -> TaskEventPayload {
        match self {
            Self::PlanUpdated(plan) => TaskEventPayload::PlanUpdated { plan },
            Self::ActivityAppended(entry) => TaskEventPayload::ActivityAppended { entry },
            Self::DiffUpdated(diff) => TaskEventPayload::DiffUpdated { diff },
            Self::TestUpdated(tests) => TaskEventPayload::TestUpdated { tests },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RunnerEventError {
    #[error("task is not running")]
    TaskNotRunning,
    #[error("the store is degraded")]
    StoreDegraded,
    #[error("the task manager is closed")]
    ManagerClosed,
}

pub struct RunnerEventSink {
    task_id: TaskId,
    repository_id: RepositoryId,
    attempt: u32,
    sender: mpsc::Sender<TaskManagerMessage>,
}

impl RunnerEventSink {
    pub async fn append(&self, event: RunnerEvent) -> Result<EventId, RunnerEventError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(TaskManagerMessage::RunnerEvent {
                task_id: self.task_id,
                event,
                response,
            })
            .await
            .map_err(|_| RunnerEventError::ManagerClosed)?;
        receiver
            .await
            .map_err(|_| RunnerEventError::ManagerClosed)?
    }

    pub async fn record_review(
        &self,
        evidence: NewReviewEvidence,
    ) -> Result<EventId, RunnerEventError> {
        let request = RecordReviewRequest {
            task_id: self.task_id,
            expected_repository_id: self.repository_id,
            expected_attempt: self.attempt,
            evidence,
        };
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(TaskManagerMessage::RecordReview { request, response })
            .await
            .map_err(|_| RunnerEventError::ManagerClosed)?;
        receiver
            .await
            .map_err(|_| RunnerEventError::ManagerClosed)?
    }
}

pub struct RunnerShutdownHandle {
    pub task_id: TaskId,
    pub cancellation: CancellationToken,
    pub done: oneshot::Receiver<()>,
}

pub enum QuiesceResult {
    Durable {
        recovery: coding_agent_store::RecoveryOutcome,
        active: Vec<RunnerShutdownHandle>,
    },
    Frozen {
        active: Vec<RunnerShutdownHandle>,
        error: StoreWriterError,
    },
}

#[cfg(feature = "test-support")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskManagerSafetySnapshot {
    pub active_count: usize,
    pub recovery_release_ready_count: usize,
    pub available_permits: usize,
    pub degraded_recovery_running: bool,
    pub generic_recovery_attempt_id: Option<u64>,
    pub quiesce_recovery_running: bool,
}

#[cfg(feature = "test-support")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerProjectionTestSnapshot {
    pub generation: u64,
    pub as_of_event_id: EventCursor,
    pub service_paused: bool,
    pub tasks: Vec<(TaskId, TaskStatus)>,
}

/// Inseparable process-local admission capabilities shared by the scheduler
/// and the production runner.
#[derive(Clone)]
pub struct TaskManagerLaunchResources {
    limits: SchedulerConcurrencyLimits,
    max_queued_tasks: NonZeroU32,
    cargo_jobs_per_task: NonZeroU32,
    critical_stop_persistence_budget: Duration,
    repository_control: Arc<RepositoryControlCoordinator>,
    server_instance_id: uuid::Uuid,
    instance_process_scope: ProcessLivenessScope,
    storage_admission: TaskManagerStorageAdmission,
    storage_signals: TaskManagerStorageSignals,
}

#[derive(Clone)]
enum TaskManagerStorageAdmission {
    Monitor(StorageMonitorHandle),
    #[cfg(any(test, feature = "test-support"))]
    TrustedNormalForTest,
    #[cfg(test)]
    ControlledForTest(Arc<Mutex<Option<StorageState>>>),
    #[cfg(test)]
    RepositoryScopesForTest(Arc<Mutex<HashMap<RepositoryId, StorageState>>>),
    #[cfg(test)]
    PausedRefreshForTest(Arc<PausedStorageRefresh>),
}

#[cfg(test)]
#[derive(Default)]
struct PausedStorageRefresh {
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
impl PausedStorageRefresh {
    async fn wait_until_reached(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.reached.notified())
            .await
            .expect("storage admission refresh reaches the test gate");
    }

    fn resume(&self) {
        self.release.notify_one();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RefreshedStorageAdmission {
    Ready,
    RepositoryBlocked,
    GlobalBlocked,
}

impl TaskManagerStorageAdmission {
    fn activity_monitor(&self) -> Option<StorageMonitorHandle> {
        match self {
            Self::Monitor(monitor) => Some(monitor.clone()),
            #[cfg(any(test, feature = "test-support"))]
            Self::TrustedNormalForTest => None,
            #[cfg(test)]
            Self::ControlledForTest(_)
            | Self::RepositoryScopesForTest(_)
            | Self::PausedRefreshForTest(_) => None,
        }
    }

    async fn refresh_for_repository_admission(
        &self,
        active_task_count: u32,
        repository_id: RepositoryId,
    ) -> Result<RefreshedStorageAdmission, StorageMonitorError> {
        match self {
            Self::Monitor(monitor) => monitor
                .refresh_for_repository_admission(active_task_count, repository_id)
                .await
                .map(|snapshot| classify_refreshed_storage(&snapshot, repository_id)),
            #[cfg(any(test, feature = "test-support"))]
            Self::TrustedNormalForTest => Ok(RefreshedStorageAdmission::Ready),
            #[cfg(test)]
            Self::ControlledForTest(_) => Ok(RefreshedStorageAdmission::Ready),
            #[cfg(test)]
            Self::RepositoryScopesForTest(scopes) => scopes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&repository_id)
                .copied()
                .map(|state| {
                    if state == StorageState::Normal {
                        RefreshedStorageAdmission::Ready
                    } else {
                        RefreshedStorageAdmission::RepositoryBlocked
                    }
                })
                .ok_or(StorageMonitorError::UnknownRepositoryScope),
            #[cfg(test)]
            Self::PausedRefreshForTest(gate) => {
                gate.reached.notify_one();
                gate.release.notified().await;
                Ok(RefreshedStorageAdmission::Ready)
            }
        }
    }

    fn launch_allowed(&self, repository_id: RepositoryId) -> bool {
        match self {
            Self::Monitor(monitor) => {
                storage_snapshot_allows_claimed_launch(&monitor.current_snapshot(), repository_id)
            }
            #[cfg(any(test, feature = "test-support"))]
            Self::TrustedNormalForTest => true,
            #[cfg(test)]
            Self::ControlledForTest(state) => storage_state_allows_claimed_launch(
                *state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            ),
            #[cfg(test)]
            Self::RepositoryScopesForTest(scopes) => scopes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&repository_id)
                .copied()
                .is_some_and(|state| storage_state_allows_claimed_launch(Some(state))),
            #[cfg(test)]
            Self::PausedRefreshForTest(_) => true,
        }
    }
}

impl TaskManagerLaunchResources {
    pub fn new(
        limits: SchedulerConcurrencyLimits,
        repository_control: Arc<RepositoryControlCoordinator>,
        server_instance_id: uuid::Uuid,
        instance_process_scope: ProcessLivenessScope,
        storage_monitor: StorageMonitorHandle,
    ) -> Self {
        assert_server_instance_id_v4(server_instance_id);
        Self {
            limits,
            max_queued_tasks: NonZeroU32::new(256)
                .expect("the compatibility queue limit is nonzero"),
            cargo_jobs_per_task: NonZeroU32::MIN,
            critical_stop_persistence_budget: CRITICAL_STOP_PERSISTENCE_BUDGET,
            repository_control,
            server_instance_id,
            instance_process_scope,
            storage_admission: TaskManagerStorageAdmission::Monitor(storage_monitor),
            storage_signals: TaskManagerStorageSignals::new(),
        }
    }

    pub(crate) fn new_with_storage_signals(
        limits: SchedulerConcurrencyLimits,
        repository_control: Arc<RepositoryControlCoordinator>,
        server_instance_id: uuid::Uuid,
        instance_process_scope: ProcessLivenessScope,
        storage_monitor: StorageMonitorHandle,
        storage_signals: TaskManagerStorageSignals,
    ) -> Self {
        assert_server_instance_id_v4(server_instance_id);
        Self {
            limits,
            max_queued_tasks: NonZeroU32::new(256)
                .expect("the compatibility queue limit is nonzero"),
            cargo_jobs_per_task: NonZeroU32::MIN,
            critical_stop_persistence_budget: CRITICAL_STOP_PERSISTENCE_BUDGET,
            repository_control,
            server_instance_id,
            instance_process_scope,
            storage_admission: TaskManagerStorageAdmission::Monitor(storage_monitor),
            storage_signals,
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn new_for_test(
        limits: SchedulerConcurrencyLimits,
        repository_control: Arc<RepositoryControlCoordinator>,
        instance_process_scope: ProcessLivenessScope,
    ) -> Self {
        Self::new_for_test_with_instance_id(
            limits,
            repository_control,
            uuid::Uuid::new_v4(),
            instance_process_scope,
        )
    }

    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn new_for_test_with_instance_id(
        limits: SchedulerConcurrencyLimits,
        repository_control: Arc<RepositoryControlCoordinator>,
        server_instance_id: uuid::Uuid,
        instance_process_scope: ProcessLivenessScope,
    ) -> Self {
        assert_server_instance_id_v4(server_instance_id);
        Self {
            limits,
            max_queued_tasks: NonZeroU32::new(256)
                .expect("the compatibility queue limit is nonzero"),
            cargo_jobs_per_task: NonZeroU32::MIN,
            critical_stop_persistence_budget: CRITICAL_STOP_PERSISTENCE_BUDGET,
            repository_control,
            server_instance_id,
            instance_process_scope,
            storage_admission: TaskManagerStorageAdmission::TrustedNormalForTest,
            storage_signals: TaskManagerStorageSignals::new(),
        }
    }

    pub const fn limits(&self) -> SchedulerConcurrencyLimits {
        self.limits
    }

    /// Supplies the two non-concurrency limits exposed by Scheduler.
    ///
    /// Existing embedders keep conservative protocol-valid defaults. The
    /// production runner factory must replace them with the immutable runtime
    /// configuration captured for this primary instance.
    pub fn with_scheduler_projection_limits(
        mut self,
        max_queued_tasks: NonZeroU32,
        cargo_jobs_per_task: NonZeroU32,
    ) -> Self {
        assert!(
            max_queued_tasks.get() <= 256,
            "scheduler queue projection limit must not exceed 256"
        );
        assert!(
            cargo_jobs_per_task.get() <= 8,
            "scheduler Cargo jobs projection limit must not exceed 8"
        );
        self.max_queued_tasks = max_queued_tasks;
        self.cargo_jobs_per_task = cargo_jobs_per_task;
        self
    }

    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn with_critical_stop_persistence_budget_for_test(mut self, budget: Duration) -> Self {
        assert!(
            !budget.is_zero(),
            "critical stop persistence budget must be positive"
        );
        self.critical_stop_persistence_budget = budget;
        self
    }

    fn scheduler_public_limits(&self) -> SchedulerPublicLimits {
        SchedulerPublicLimits::try_new(
            self.limits,
            self.max_queued_tasks.get(),
            self.cargo_jobs_per_task.get(),
        )
        .expect("TaskManager launch resources contain validated Scheduler limits")
    }

    pub const fn server_instance_id(&self) -> uuid::Uuid {
        self.server_instance_id
    }

    pub fn repository_control(&self) -> Arc<RepositoryControlCoordinator> {
        Arc::clone(&self.repository_control)
    }

    pub fn instance_process_scope(&self) -> ProcessLivenessScope {
        self.instance_process_scope.clone()
    }

    #[cfg(test)]
    pub(crate) fn storage_monitor_for_test(&self) -> Option<StorageMonitorHandle> {
        match &self.storage_admission {
            TaskManagerStorageAdmission::Monitor(monitor) => Some(monitor.clone()),
            TaskManagerStorageAdmission::TrustedNormalForTest
            | TaskManagerStorageAdmission::ControlledForTest(_)
            | TaskManagerStorageAdmission::RepositoryScopesForTest(_)
            | TaskManagerStorageAdmission::PausedRefreshForTest(_) => None,
        }
    }
}

fn assert_server_instance_id_v4(server_instance_id: uuid::Uuid) {
    assert!(
        !server_instance_id.is_nil()
            && server_instance_id.get_version() == Some(uuid::Version::Random),
        "task-manager server instance ID must be a version-4 UUID"
    );
}

#[cfg(test)]
pub(crate) fn test_task_manager_launch_resources(
    global: u32,
    per_repository: u32,
) -> TaskManagerLaunchResources {
    static PROCESS_LIVENESS: std::sync::OnceLock<(
        tempfile::TempDir,
        coding_agent_runtime::ProcessLivenessDirectory,
    )> = std::sync::OnceLock::new();
    let (_, directory) = PROCESS_LIVENESS.get_or_init(|| {
        let runtime = tempfile::tempdir().expect("create task-manager test runtime");
        let directory = coding_agent_runtime::ProcessLivenessDirectory::open(runtime.path())
            .expect("open task-manager test process-liveness directory");
        (runtime, directory)
    });
    let instance_id = uuid::Uuid::new_v4();
    TaskManagerLaunchResources::new_for_test_with_instance_id(
        SchedulerConcurrencyLimits::try_new(global, per_repository)
            .expect("valid task-manager test concurrency"),
        Arc::new(RepositoryControlCoordinator::new()),
        instance_id,
        directory
            .instance_scope(*instance_id.as_bytes())
            .expect("derive task-manager test instance scope"),
    )
}

#[derive(Debug, thiserror::Error)]
pub enum TaskManagerError {
    #[error("task manager is closed")]
    Closed,
    #[error("the task manager operation deadline elapsed")]
    DeadlineElapsed,
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    StoreWriter(#[from] StoreWriterError),
    #[error("task was not found")]
    TaskNotFound,
    #[error("task is not cancellable in state {task:?}")]
    TaskNotCancellable { task: Task },
    #[error("another stop intent already won for task {task:?}")]
    StopAlreadyRequested {
        task: Task,
        existing: StopIntentKind,
    },
    #[error("task manager is frozen")]
    Frozen,
    #[error("the store is degraded")]
    StoreDegraded,
    #[error("task manager invariant failed: {0}")]
    Invariant(&'static str),
}

#[derive(Clone)]
pub struct TaskManagerHandle {
    sender: mpsc::Sender<TaskManagerMessage>,
    degraded_recoveries: broadcast::Sender<DegradedRecoveryResult>,
    shutdown: Arc<TaskManagerShutdownControl>,
    scheduler_state_reader: SchedulerStateReader<SchedulerStoreState>,
    #[cfg(feature = "test-support")]
    storage_signals: TaskManagerStorageSignals,
    #[cfg(feature = "test-support")]
    actor_pauses: Option<Arc<ActorPauseController>>,
}

struct TaskManagerShutdownControl {
    frozen: AtomicBool,
    cancellation: CancellationToken,
    launch_barrier: Arc<Mutex<()>>,
    process_cleanup: Arc<ShutdownProcessCleanupTracker>,
}

impl TaskManagerShutdownControl {
    fn new(launch_barrier: Arc<Mutex<()>>) -> Self {
        Self {
            frozen: AtomicBool::new(false),
            cancellation: CancellationToken::new(),
            launch_barrier,
            process_cleanup: Arc::new(ShutdownProcessCleanupTracker::default()),
        }
    }

    fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::Acquire)
    }

    fn try_freeze(&self) -> bool {
        let _barrier = self
            .launch_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.frozen
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn freeze_and_cancel(&self) {
        let _barrier = self
            .launch_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.frozen.store(true, Ordering::Release);
        self.cancellation.cancel();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TrackedRunnerProcess {
    task_id: TaskId,
    operation_nonce: u64,
}

#[derive(Default)]
struct ShutdownProcessCleanupState {
    outstanding: HashMap<u64, TrackedRunnerProcess>,
}

/// Mailbox-independent process cleanup tracking for runners that crossed the
/// final launch gate.
///
/// Registration is performed while the shared launch barrier is held. Once
/// shutdown freezes that barrier-protected launch set, every registered entry
/// must independently observe its exact sealed scope as clean. Confirmed exact
/// ownership is retired immediately, so resident state is bounded by the
/// outstanding launch set rather than runner history.
struct ShutdownProcessCleanupTracker {
    id: u64,
    state: Mutex<ShutdownProcessCleanupState>,
    changed: Notify,
}

static NEXT_SHUTDOWN_PROCESS_CLEANUP_TRACKER_ID: AtomicU64 = AtomicU64::new(1);

impl Default for ShutdownProcessCleanupTracker {
    fn default() -> Self {
        let id = NEXT_SHUTDOWN_PROCESS_CLEANUP_TRACKER_ID.fetch_add(1, Ordering::Relaxed);
        assert_ne!(id, 0, "shutdown process-cleanup tracker identity overflow");
        Self {
            id,
            state: Mutex::new(ShutdownProcessCleanupState::default()),
            changed: Notify::new(),
        }
    }
}

impl ShutdownProcessCleanupTracker {
    fn register_spawned_runner(&self, process_scope: &TaskProcessScopeOwnership) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = TrackedRunnerProcess {
            task_id: process_scope.task_id(),
            operation_nonce: process_scope.operation_nonce(),
        };
        match state.outstanding.entry(process_scope.owner_id()) {
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(entry);
                true
            }
            std::collections::hash_map::Entry::Occupied(existing) => *existing.get() == entry,
        }
    }

    fn runner_returned(self: &Arc<Self>, process_scope: TaskProcessScopeOwnership) {
        let tracker = Arc::clone(self);
        tokio::spawn(async move {
            tracker.confirm_process_cleanup(process_scope).await;
        });
    }

    async fn confirm_process_cleanup(&self, process_scope: TaskProcessScopeOwnership) {
        loop {
            if matches!(
                process_scope.seal_and_cleanup(),
                Ok(ProcessCleanupProof::Confirmed)
            ) {
                if self.mark_confirmed(&process_scope) {
                    self.changed.notify_waiters();
                }
                return;
            }
            tokio::time::sleep(PROCESS_CLEANUP_RETRY_INTERVAL).await;
        }
    }

    fn mark_confirmed(&self, process_scope: &TaskProcessScopeOwnership) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let std::collections::hash_map::Entry::Occupied(entry) =
            state.outstanding.entry(process_scope.owner_id())
        else {
            return false;
        };
        if entry.get().task_id != process_scope.task_id()
            || entry.get().operation_nonce != process_scope.operation_nonce()
        {
            return false;
        }
        entry.remove();
        true
    }

    fn all_registered_confirmed(&self) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.outstanding.is_empty()
    }

    #[cfg(test)]
    async fn wait_for_all_registered(&self) -> ShutdownProcessCleanupProof {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.all_registered_confirmed() {
                return ShutdownProcessCleanupProof {
                    tracker_id: self.id,
                };
            }
            changed.await;
        }
    }

    async fn wait_for_all_registered_until(
        &self,
        deadline: Instant,
    ) -> (ShutdownProcessCleanupProof, bool) {
        let mut cleanup_outlived_deadline = false;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.all_registered_confirmed() {
                return (
                    ShutdownProcessCleanupProof {
                        tracker_id: self.id,
                    },
                    cleanup_outlived_deadline,
                );
            }
            if cleanup_outlived_deadline {
                changed.as_mut().await;
                continue;
            }
            tokio::select! {
                () = changed.as_mut() => {}
                () = tokio::time::sleep_until(deadline) => {
                    cleanup_outlived_deadline = true;
                }
            }
        }
    }
}

/// Opaque evidence that every runner in the launch-barrier-frozen set returned
/// and its exact task process scope reached `ProcessCleanupProof::Confirmed`.
#[derive(Clone, Copy)]
pub(crate) struct ShutdownProcessCleanupProof {
    tracker_id: u64,
}

impl TaskManagerHandle {
    pub fn spawn(
        store: Store,
        writer: StoreWriterHandle,
        dispatcher: EventDispatcherHandle,
        service_state: ServiceStateController,
        runner: Arc<dyn TaskRunner>,
        launch_resources: TaskManagerLaunchResources,
        capacity: usize,
    ) -> Self {
        #[cfg(feature = "test-support")]
        {
            Self::spawn_inner(
                store,
                writer,
                dispatcher,
                service_state,
                runner,
                launch_resources,
                capacity,
                None,
            )
        }
        #[cfg(not(feature = "test-support"))]
        {
            Self::spawn_inner(
                store,
                writer,
                dispatcher,
                service_state,
                runner,
                launch_resources,
                capacity,
            )
        }
    }

    #[cfg(feature = "test-support")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn_with_process_test_pauses(
        store: Store,
        writer: StoreWriterHandle,
        dispatcher: EventDispatcherHandle,
        service_state: ServiceStateController,
        runner: Arc<dyn TaskRunner>,
        launch_resources: TaskManagerLaunchResources,
        capacity: usize,
        actor_pauses: Arc<ActorPauseController>,
    ) -> Self {
        Self::spawn_inner(
            store,
            writer,
            dispatcher,
            service_state,
            runner,
            launch_resources,
            capacity,
            Some(actor_pauses),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_inner(
        store: Store,
        writer: StoreWriterHandle,
        dispatcher: EventDispatcherHandle,
        service_state: ServiceStateController,
        runner: Arc<dyn TaskRunner>,
        launch_resources: TaskManagerLaunchResources,
        capacity: usize,
        #[cfg(feature = "test-support")] actor_pauses: Option<Arc<ActorPauseController>>,
    ) -> Self {
        assert!(
            capacity > 0,
            "task-manager channel capacity must be positive"
        );
        let (sender, receiver) = mpsc::channel(capacity);
        let (critical_sender, critical_receiver) = mpsc::channel(1);
        let storage_signals = launch_resources.storage_signals.clone();
        let scheduler_public_limits = launch_resources.scheduler_public_limits();
        storage_signals.bind(sender.downgrade(), critical_sender);
        let (completion_sender, completion_receiver) = mpsc::channel(16);
        let (preparation_sender, preparation_receiver) =
            mpsc::channel(usize::try_from(launch_resources.limits.global().get()).unwrap_or(4));
        let shutdown = Arc::new(TaskManagerShutdownControl::new(
            storage_signals.launch_barrier(),
        ));
        let coordinator = DegradedCoordinator::new(
            writer.clone(),
            dispatcher.clone(),
            service_state.clone(),
            sender.downgrade(),
        );
        let service_state_receiver = service_state.subscribe();
        let initial_service = service_state.current();
        let initial_service_paused = initial_service.state != ServiceState::Ready;
        let scheduler_projection = SchedulerProjectionBridge::new_complete(
            launch_resources.server_instance_id,
            initial_service.generation,
            scheduler_public_limits,
            initial_service_paused,
        );
        let scheduler_state_reader = scheduler_projection.reader();
        let scheduler_snapshot_read_gate = Arc::new(tokio::sync::Mutex::new(()));
        let (degraded_recoveries, _) = broadcast::channel(16);
        let actor = TaskManager {
            store,
            writer,
            dispatcher,
            service_state,
            service_state_receiver,
            runner,
            permit_ledger: PermitLedger::new(launch_resources.limits),
            repository_control: launch_resources.repository_control,
            instance_process_scope: launch_resources.instance_process_scope,
            storage_admission: launch_resources.storage_admission,
            critical_stop_persistence_budget: launch_resources.critical_stop_persistence_budget,
            storage_activity_sync: StorageActivitySynchronizer::new(),
            sender: sender.downgrade(),
            receiver,
            deferred_messages: VecDeque::new(),
            critical_receiver,
            critical_wake: storage_signals.critical_wake.clone(),
            completion_sender,
            completion_receiver,
            preparation_sender,
            preparation_receiver,
            active: HashMap::new(),
            mutation_sequences: HashMap::new(),
            safety_registry: storage_signals.safety_registry(),
            coordinator,
            degraded_recoveries: degraded_recoveries.clone(),
            pending_durable_results: Vec::new(),
            pending_replay_in_flight: None,
            resolved_pending_replays: HashMap::new(),
            staged_stop_intent_completions: VecDeque::new(),
            scan_requested: false,
            scan_in_flight: false,
            scan_generation: 0,
            scan_available: HashMap::new(),
            scan_gates: SchedulerAdmissionGates::default(),
            storage_admission_in_flight: None,
            pending_quiesce: None,
            detached_cancel_completions: 0,
            exact_barrier_epoch: 0,
            next_operation_nonce: 1,
            next_quiesce_id: 1,
            next_pending_replay_attempt_id: 1,
            next_typed_write_attempt_id: 1,
            next_generic_recovery_attempt_id: 1,
            next_terminal_projection_attempt_id: 1,
            scheduler_projection,
            scheduler_public_limits,
            scheduler_storage_signals: storage_signals.clone(),
            applied_scheduler_storage_generation: 0,
            scheduler_snapshot_read_gate,
            main_closed: false,
            degraded: false,
            generic_recovery_attempt: None,
            degraded_replayed_pending_count: 0,
            degraded_replay_high_watermark: None,
            frozen: false,
            shutdown: shutdown.clone(),
            #[cfg(feature = "test-support")]
            next_launch_ordinal: 0,
            #[cfg(feature = "test-support")]
            actor_pauses: actor_pauses.clone(),
            #[cfg(test)]
            claim_hooks: None,
            #[cfg(test)]
            exit_probe: None,
            #[cfg(test)]
            storage_activity_exit_pause: None,
            #[cfg(test)]
            degraded_finalization_pause: None,
            #[cfg(test)]
            quiesce_finalization_pause: None,
        };
        tokio::spawn(actor.run());
        Self {
            sender,
            degraded_recoveries,
            shutdown,
            scheduler_state_reader,
            #[cfg(feature = "test-support")]
            storage_signals,
            #[cfg(feature = "test-support")]
            actor_pauses,
        }
    }

    #[cfg(test)]
    fn spawn_with_claim_hooks(
        runtime: (
            Store,
            StoreWriterHandle,
            EventDispatcherHandle,
            ServiceStateController,
        ),
        runner: Arc<dyn TaskRunner>,
        launch_resources: TaskManagerLaunchResources,
        capacity: usize,
        claim_hooks: Arc<ClaimTestHooks>,
    ) -> Self {
        let (store, writer, dispatcher, service_state) = runtime;
        assert!(
            capacity > 0,
            "task-manager channel capacity must be positive"
        );
        let (sender, receiver) = mpsc::channel(capacity);
        let (critical_sender, critical_receiver) = mpsc::channel(1);
        let storage_signals = launch_resources.storage_signals.clone();
        let scheduler_public_limits = launch_resources.scheduler_public_limits();
        storage_signals.bind(sender.downgrade(), critical_sender);
        let (completion_sender, completion_receiver) = mpsc::channel(16);
        let (preparation_sender, preparation_receiver) =
            mpsc::channel(usize::try_from(launch_resources.limits.global().get()).unwrap_or(4));
        let shutdown = Arc::new(TaskManagerShutdownControl::new(
            storage_signals.launch_barrier(),
        ));
        let coordinator = DegradedCoordinator::new(
            writer.clone(),
            dispatcher.clone(),
            service_state.clone(),
            sender.downgrade(),
        );
        let service_state_receiver = service_state.subscribe();
        let initial_service = service_state.current();
        let initial_service_paused = initial_service.state != ServiceState::Ready;
        let scheduler_projection = SchedulerProjectionBridge::new_complete(
            launch_resources.server_instance_id,
            initial_service.generation,
            scheduler_public_limits,
            initial_service_paused,
        );
        let scheduler_state_reader = scheduler_projection.reader();
        let scheduler_snapshot_read_gate = Arc::new(tokio::sync::Mutex::new(()));
        let (degraded_recoveries, _) = broadcast::channel(16);
        let permit_ledger = PermitLedger::new(launch_resources.limits);
        claim_hooks.install_permit_ledger(permit_ledger.clone());
        let actor = TaskManager {
            store,
            writer,
            dispatcher,
            service_state,
            service_state_receiver,
            runner,
            permit_ledger,
            repository_control: launch_resources.repository_control,
            instance_process_scope: launch_resources.instance_process_scope,
            storage_admission: launch_resources.storage_admission,
            critical_stop_persistence_budget: launch_resources.critical_stop_persistence_budget,
            storage_activity_sync: StorageActivitySynchronizer::new(),
            sender: sender.downgrade(),
            receiver,
            deferred_messages: VecDeque::new(),
            critical_receiver,
            critical_wake: storage_signals.critical_wake.clone(),
            completion_sender,
            completion_receiver,
            preparation_sender,
            preparation_receiver,
            active: HashMap::new(),
            mutation_sequences: HashMap::new(),
            safety_registry: storage_signals.safety_registry(),
            coordinator,
            degraded_recoveries: degraded_recoveries.clone(),
            pending_durable_results: Vec::new(),
            pending_replay_in_flight: None,
            resolved_pending_replays: HashMap::new(),
            staged_stop_intent_completions: VecDeque::new(),
            scan_requested: false,
            scan_in_flight: false,
            scan_generation: 0,
            scan_available: HashMap::new(),
            scan_gates: SchedulerAdmissionGates::default(),
            storage_admission_in_flight: None,
            pending_quiesce: None,
            detached_cancel_completions: 0,
            exact_barrier_epoch: 0,
            next_operation_nonce: 1,
            next_quiesce_id: 1,
            next_pending_replay_attempt_id: 1,
            next_typed_write_attempt_id: 1,
            next_generic_recovery_attempt_id: 1,
            next_terminal_projection_attempt_id: 1,
            scheduler_projection,
            scheduler_public_limits,
            scheduler_storage_signals: storage_signals.clone(),
            applied_scheduler_storage_generation: 0,
            scheduler_snapshot_read_gate,
            main_closed: false,
            degraded: false,
            generic_recovery_attempt: None,
            degraded_replayed_pending_count: 0,
            degraded_replay_high_watermark: None,
            frozen: false,
            shutdown: shutdown.clone(),
            #[cfg(feature = "test-support")]
            next_launch_ordinal: 0,
            #[cfg(feature = "test-support")]
            actor_pauses: None,
            claim_hooks: Some(claim_hooks),
            exit_probe: None,
            storage_activity_exit_pause: None,
            degraded_finalization_pause: None,
            quiesce_finalization_pause: None,
        };
        tokio::spawn(actor.run());
        Self {
            sender,
            degraded_recoveries,
            shutdown,
            scheduler_state_reader,
            #[cfg(feature = "test-support")]
            storage_signals,
            #[cfg(feature = "test-support")]
            actor_pauses: None,
        }
    }

    pub fn subscribe_degraded_recovery(&self) -> broadcast::Receiver<DegradedRecoveryResult> {
        self.degraded_recoveries.subscribe()
    }

    pub(crate) fn scheduler_state_reader(&self) -> SchedulerStateReader<SchedulerStoreState> {
        self.scheduler_state_reader.clone()
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn scheduler_server_instance_id_for_test(&self) -> uuid::Uuid {
        self.scheduler_state_reader
            .current()
            .public_state()
            .server_instance_id()
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn scheduler_projection_for_test(&self) -> SchedulerProjectionTestSnapshot {
        let snapshot = self.scheduler_state_reader.current();
        SchedulerProjectionTestSnapshot {
            generation: snapshot.generation(),
            as_of_event_id: snapshot.as_of_event_id(),
            service_paused: snapshot.public_state().service_paused_for_test(),
            tasks: snapshot.public_state().tasks_for_test(),
        }
    }

    pub(crate) fn freeze_and_cancel(&self) {
        self.shutdown.freeze_and_cancel();
    }

    #[cfg(test)]
    pub(crate) async fn freeze_and_wait_for_process_cleanup(&self) -> ShutdownProcessCleanupProof {
        self.shutdown.freeze_and_cancel();
        self.shutdown
            .process_cleanup
            .wait_for_all_registered()
            .await
    }

    pub(crate) async fn freeze_and_wait_for_process_cleanup_until(
        &self,
        deadline: Instant,
    ) -> (ShutdownProcessCleanupProof, bool) {
        self.shutdown.freeze_and_cancel();
        self.shutdown
            .process_cleanup
            .wait_for_all_registered_until(deadline)
            .await
    }

    pub(crate) async fn finalize_shutdown_after_process_cleanup(
        &self,
        proof: &ShutdownProcessCleanupProof,
        deadline: Instant,
    ) -> Result<QuiesceResult, TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        timeout_at(deadline, async {
            self.send(TaskManagerMessage::FinalizeShutdownAfterProcessCleanup {
                proof: *proof,
                deadline,
                response,
            })
            .await?;
            receiver.await.map_err(|_| TaskManagerError::Closed)?
        })
        .await
        .map_err(|_| TaskManagerError::DeadlineElapsed)?
    }

    #[cfg(test)]
    async fn install_exit_probe(&self) -> oneshot::Receiver<()> {
        let (exited, receiver) = oneshot::channel();
        let (installed, installed_receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InstallExitProbe { exited, installed })
            .await
            .expect("install task-manager exit probe");
        installed_receiver
            .await
            .expect("task manager acknowledges exit probe");
        receiver
    }

    #[cfg(test)]
    async fn storage_activity_sync_snapshot_for_test(
        &self,
    ) -> Result<StorageActivitySyncSnapshotForTest, TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InspectStorageActivitySyncForTest { response })
            .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    #[cfg(test)]
    async fn pause_next_storage_idle_completion_for_test(
        &self,
    ) -> Result<StorageActivityCompletionPauseForTest, TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::PauseNextStorageIdleCompletionForTest { response })
            .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    pub async fn notify_queued(&self, task_id: TaskId) -> Result<(), TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::NotifyQueued {
            _task_id: task_id,
            response,
        })
        .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)?
    }

    pub(crate) async fn notify_admission_changed(&self) -> Result<(), TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::AdmissionChanged { response })
            .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)?
    }

    pub async fn cancel(&self, task_id: TaskId) -> Result<CancelOutcome, TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::Cancel { task_id, response })
            .await?;
        #[cfg(feature = "test-support")]
        if let Some(actor_pauses) = &self.actor_pauses {
            actor_pauses.pause(ActorPausePoint::CancelEnqueued).await;
        }
        receiver.await.map_err(|_| TaskManagerError::Closed)?
    }

    pub async fn quiesce_and_interrupt(
        &self,
        deadline: Instant,
    ) -> Result<QuiesceResult, TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::Quiesce { deadline, response })
            .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)?
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub async fn pending_durable_results_for_test(
        &self,
    ) -> Result<Vec<PendingDurableResult>, TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InspectPendingDurableResults { response })
            .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    #[cfg(test)]
    async fn inject_stop_intent_completion_for_test(
        &self,
        identity: DurableOperationIdentity,
        completion: DurableCompletion<StopIntentBatchReceipt>,
    ) -> Result<(), TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InjectStopIntentCompletion {
            identity,
            completion,
            response,
        })
        .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    #[cfg(test)]
    async fn active_stop_snapshot_for_test(
        &self,
        task_id: TaskId,
    ) -> Result<Option<ActiveStopSnapshotForTest>, TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InspectActiveStop { task_id, response })
            .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    #[cfg(test)]
    async fn exact_barrier_snapshot_for_test(
        &self,
    ) -> Result<ExactBarrierSnapshotForTest, TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InspectExactBarriers { response })
            .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    #[cfg(test)]
    async fn install_generic_recovery_lease_for_test(
        &self,
        attempt_id: u64,
        barrier_epoch: u64,
    ) -> Result<(), TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InstallGenericRecoveryLeaseForTest {
            attempt_id,
            barrier_epoch,
            response,
        })
        .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    #[cfg(test)]
    async fn pause_next_degraded_finalization_for_test(
        &self,
    ) -> Result<(oneshot::Receiver<()>, oneshot::Sender<()>), TaskManagerError> {
        let (reached, reached_receiver) = oneshot::channel();
        let (release, release_receiver) = oneshot::channel();
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::PauseNextDegradedFinalizationForTest {
            pause: RecoveryFinalizationPauseForTest {
                reached,
                release: release_receiver,
            },
            response,
        })
        .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)?;
        Ok((reached_receiver, release))
    }

    #[cfg(test)]
    async fn pause_next_quiesce_finalization_for_test(
        &self,
    ) -> Result<(oneshot::Receiver<()>, oneshot::Sender<()>), TaskManagerError> {
        let (reached, reached_receiver) = oneshot::channel();
        let (release, release_receiver) = oneshot::channel();
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::PauseNextQuiesceFinalizationForTest {
            pause: RecoveryFinalizationPauseForTest {
                reached,
                release: release_receiver,
            },
            response,
        })
        .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)?;
        Ok((reached_receiver, release))
    }

    #[cfg(all(test, feature = "test-support"))]
    fn safety_registry_snapshot_for_test(&self) -> SafetyRegistrySnapshotForTest {
        let state = self.storage_signals.safety_registry.lock();
        SafetyRegistrySnapshotForTest {
            entry_count: state.entries.len(),
            pending_critical_count: state.pending_critical.len(),
            safety_latched_count: state
                .entries
                .values()
                .filter(|entry| entry.stop.is_latched())
                .count(),
        }
    }

    #[cfg(all(test, feature = "test-support"))]
    fn corrupt_safety_registry_nonce_for_test(&self, task_id: TaskId) -> bool {
        let _launch_guard = self
            .shutdown
            .launch_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut state = self.storage_signals.safety_registry.lock();
        let Some(entry) = state.entries.get_mut(&task_id) else {
            return false;
        };
        let Some(corrupted) = entry.operation_nonce.checked_add(1) else {
            return false;
        };
        entry.operation_nonce = corrupted;
        true
    }

    #[cfg(test)]
    async fn install_canonical_pending_for_test(
        &self,
        pending: PendingDurableResult,
    ) -> Result<(), TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InstallCanonicalPendingForTest { pending, response })
            .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    #[cfg(test)]
    async fn install_staged_stop_completions_for_test(
        &self,
        entries: Vec<StagedStopCompletionForTest>,
    ) -> Result<(), TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InstallStagedStopCompletionsForTest { entries, response })
            .await?;
        match receiver.await.map_err(|_| TaskManagerError::Closed)? {
            true => Ok(()),
            false => Err(TaskManagerError::Invariant(
                "failed to install staged stop completion fixture",
            )),
        }
    }

    #[cfg(test)]
    async fn resolve_canonical_predecessor_for_test(
        &self,
        predecessor: PendingDurableResult,
    ) -> Result<(), TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::ResolveCanonicalPredecessorForTest {
            predecessor,
            response,
        })
        .await?;
        match receiver.await.map_err(|_| TaskManagerError::Closed)? {
            true => Ok(()),
            false => Err(TaskManagerError::Invariant(
                "failed to resolve canonical predecessor fixture",
            )),
        }
    }

    #[cfg(test)]
    async fn release_canonical_predecessor_without_progress_for_test(
        &self,
        predecessor: PendingDurableResult,
    ) -> Result<(), TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(
            TaskManagerMessage::ReleaseCanonicalPredecessorWithoutProgressForTest {
                predecessor,
                response,
            },
        )
        .await?;
        match receiver.await.map_err(|_| TaskManagerError::Closed)? {
            true => Ok(()),
            false => Err(TaskManagerError::Invariant(
                "failed to release canonical predecessor fixture",
            )),
        }
    }

    #[cfg(test)]
    async fn stage_historical_record_review_pair_for_test(
        &self,
        task_id: TaskId,
        requests: [RecordReviewRequest; 2],
    ) -> Result<HistoricalRecordReviewPairForTest, TaskManagerError> {
        let (first_response, first_receiver) = oneshot::channel();
        let (second_response, second_receiver) = oneshot::channel();
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::StageHistoricalRecordReviewPairForTest {
            task_id,
            requests,
            review_responses: [first_response, second_response],
            response,
        })
        .await?;
        let entries = receiver.await.map_err(|_| TaskManagerError::Closed)?;
        Ok(HistoricalRecordReviewPairForTest {
            entries,
            responses: [first_receiver, second_receiver],
        })
    }

    #[cfg(test)]
    async fn inject_finalize_degraded_for_test(
        &self,
        attempt_id: u64,
        barrier_epoch: u64,
        recovery: coding_agent_store::RecoveryOutcome,
    ) -> Result<Result<DegradedRecoveryResult, DegradedCoordinatorError>, TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InjectFinalizeDegradedForTest {
            attempt_id,
            barrier_epoch,
            recovery: recovery.clone(),
            replayed_pending_count: 0,
            high_watermark: recovery.high_watermark,
            response,
        })
        .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    #[cfg(test)]
    async fn active_pending_stop_write_for_test(
        &self,
        task_id: TaskId,
    ) -> Result<Option<PendingDurableResult>, TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InspectActivePendingStopWrite { task_id, response })
            .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    #[cfg(test)]
    async fn inject_record_review_completion_for_test(
        &self,
        identity: TaskMutationIdentity,
        request: RecordReviewRequest,
        completion: DurableCompletion<RecordReviewOutcome>,
    ) -> Result<Result<EventId, RunnerEventError>, TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InjectRecordReviewCompletion {
            identity,
            request,
            completion,
            response,
        })
        .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    #[cfg(test)]
    async fn inject_terminal_write_completion_for_test(
        &self,
        task_id: TaskId,
        attempt_id: u64,
        identity: TaskMutationIdentity,
        stage: TerminalWriteStage,
        completion: TerminalWriteCompletion,
    ) -> Result<(), TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InjectTerminalWriteCompletion {
            task_id,
            attempt_id,
            identity,
            stage,
            completion,
            response,
        })
        .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    #[cfg(test)]
    async fn inject_final_stop_completion_for_test(
        &self,
        identity: TaskMutationIdentity,
        request: FinalizeStoppedTaskRequest,
        completion: DurableCompletion<FinalizeStoppedTaskOutcome>,
    ) -> Result<(), TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InjectFinalStopCompletion {
            identity,
            request,
            completion,
            response,
        })
        .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    #[cfg(test)]
    async fn inject_pending_replay_completion_for_test(
        &self,
        attempt_id: u64,
        pending: PendingDurableResult,
        result: Result<DurableCompletion<PendingReplayReceipt>, StoreWriterSubmitError>,
    ) -> Result<(), TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InjectPendingReplayCompletion {
            attempt_id,
            pending,
            result,
            response,
        })
        .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    #[cfg(test)]
    async fn inject_pending_replay_retry_for_test(
        &self,
        attempt_id: u64,
    ) -> Result<(), TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InjectPendingReplayRetry {
            attempt_id,
            response,
        })
        .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    #[cfg(test)]
    async fn inject_generic_recovery_completion_for_test(
        &self,
        attempt_id: u64,
        result: Result<DegradedRecoveryResult, DegradedCoordinatorError>,
    ) -> Result<(), TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InjectGenericRecoveryCompletion {
            attempt_id,
            result,
            response,
        })
        .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    #[cfg(test)]
    async fn inject_running_user_cancel_after_lookup_for_test(
        &self,
        task_id: TaskId,
    ) -> Result<CancelOutcome, TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InjectRunningUserCancelAfterLookup { task_id, response })
            .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)?
    }

    #[cfg(test)]
    async fn inject_stale_cancel_task_loaded_for_test(
        &self,
        task: Task,
    ) -> Result<CancelOutcome, TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InjectStaleCancelTaskLoaded { task, response })
            .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)?
    }

    #[cfg(test)]
    async fn inject_terminal_projection_for_test(
        &self,
        completion: TerminalProjectionCompletion,
    ) -> Result<TerminalProjectionSnapshotForTest, TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InjectTerminalProjection {
            completion,
            response,
        })
        .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    #[cfg(test)]
    async fn freeze_degraded_for_test(&self) -> Result<(), TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::FreezeDegraded {
            pending: Vec::new(),
            response,
        })
        .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    #[cfg(test)]
    async fn freeze_degraded_preserving_pending_for_test(&self) -> Result<(), TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::FreezeDegradedPreservingPendingForTest { response })
            .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    #[cfg(test)]
    async fn handle_critical_wake_for_test(&self) -> Result<(), TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::HandleCriticalWakeForTest { response })
            .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub async fn safety_snapshot_for_test(
        &self,
    ) -> Result<TaskManagerSafetySnapshot, TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InspectRecoverySafety { response })
            .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)?
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn shutdown_latched_for_test(&self) -> bool {
        self.shutdown.is_frozen()
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn notify_storage_critical_for_test(&self, scopes: Vec<MonitoredStorageScope>) {
        self.storage_signals
            .notify_storage_critical(StorageCriticalNotification::new(scopes));
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn notify_scheduler_storage_for_test(&self, notification: SchedulerStorageNotification) {
        self.storage_signals
            .notify_storage_classification(notification);
    }

    #[cfg(all(test, feature = "test-support"))]
    fn notify_storage_critical_at_for_test(
        &self,
        scopes: Vec<MonitoredStorageScope>,
        observed_at: Instant,
    ) {
        self.storage_signals.notify_storage_critical_at_for_test(
            StorageCriticalNotification::new(scopes),
            observed_at,
        );
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn arm_storage_critical_on_next_publish_for_test(
        &self,
        scopes: Vec<MonitoredStorageScope>,
    ) {
        self.storage_signals
            .arm_critical_on_next_publish(StorageCriticalNotification::new(scopes));
    }

    async fn send(&self, message: TaskManagerMessage) -> Result<(), TaskManagerError> {
        self.sender
            .send(message)
            .await
            .map_err(|_| TaskManagerError::Closed)
    }
}

// Design: actor messages own exact durable receipts until the actor validates
// their lineage. Boxing only the test-only large variants would introduce a
// second ownership shape into the same state-machine boundary.
#[allow(clippy::large_enum_variant)]
pub(crate) enum TaskManagerMessage {
    NotifyQueued {
        _task_id: TaskId,
        response: oneshot::Sender<Result<(), TaskManagerError>>,
    },
    AdmissionChanged {
        response: oneshot::Sender<Result<(), TaskManagerError>>,
    },
    Cancel {
        task_id: TaskId,
        response: oneshot::Sender<Result<CancelOutcome, TaskManagerError>>,
    },
    RunnerEvent {
        task_id: TaskId,
        event: RunnerEvent,
        response: oneshot::Sender<Result<EventId, RunnerEventError>>,
    },
    RecordReview {
        request: RecordReviewRequest,
        response: oneshot::Sender<Result<EventId, RunnerEventError>>,
    },
    StorageChanged,
    FinalizeDegraded {
        attempt_id: u64,
        barrier_epoch: u64,
        recovery: coding_agent_store::RecoveryOutcome,
        replayed_pending_count: usize,
        high_watermark: EventCursor,
        response: oneshot::Sender<Result<DegradedRecoveryResult, DegradedCoordinatorError>>,
    },
    FreezeDegraded {
        pending: Vec<PendingDurableResult>,
        response: oneshot::Sender<()>,
    },
    Quiesce {
        deadline: Instant,
        response: oneshot::Sender<Result<QuiesceResult, TaskManagerError>>,
    },
    FinalizeShutdownAfterProcessCleanup {
        proof: ShutdownProcessCleanupProof,
        deadline: Instant,
        response: oneshot::Sender<Result<QuiesceResult, TaskManagerError>>,
    },
    ResumeLaunch {
        task_id: TaskId,
        operation_nonce: u64,
    },
    #[cfg(feature = "test-support")]
    InspectPendingDurableResults {
        response: oneshot::Sender<Vec<PendingDurableResult>>,
    },
    #[cfg(feature = "test-support")]
    InspectRecoverySafety {
        response: oneshot::Sender<Result<TaskManagerSafetySnapshot, TaskManagerError>>,
    },
    #[cfg(test)]
    InstallExitProbe {
        exited: oneshot::Sender<()>,
        installed: oneshot::Sender<()>,
    },
    #[cfg(test)]
    InspectStorageActivitySyncForTest {
        response: oneshot::Sender<StorageActivitySyncSnapshotForTest>,
    },
    #[cfg(test)]
    PauseNextStorageIdleCompletionForTest {
        response: oneshot::Sender<StorageActivityCompletionPauseForTest>,
    },
    #[cfg(test)]
    InjectStopIntentCompletion {
        identity: DurableOperationIdentity,
        completion: DurableCompletion<StopIntentBatchReceipt>,
        response: oneshot::Sender<()>,
    },
    #[cfg(test)]
    InspectActiveStop {
        task_id: TaskId,
        response: oneshot::Sender<Option<ActiveStopSnapshotForTest>>,
    },
    #[cfg(test)]
    InspectExactBarriers {
        response: oneshot::Sender<ExactBarrierSnapshotForTest>,
    },
    #[cfg(test)]
    InstallGenericRecoveryLeaseForTest {
        attempt_id: u64,
        barrier_epoch: u64,
        response: oneshot::Sender<()>,
    },
    #[cfg(test)]
    PauseNextDegradedFinalizationForTest {
        pause: RecoveryFinalizationPauseForTest,
        response: oneshot::Sender<()>,
    },
    #[cfg(test)]
    PauseNextQuiesceFinalizationForTest {
        pause: RecoveryFinalizationPauseForTest,
        response: oneshot::Sender<()>,
    },
    #[cfg(test)]
    InstallCanonicalPendingForTest {
        pending: PendingDurableResult,
        response: oneshot::Sender<()>,
    },
    #[cfg(test)]
    InstallStagedStopCompletionsForTest {
        entries: Vec<StagedStopCompletionForTest>,
        response: oneshot::Sender<bool>,
    },
    #[cfg(test)]
    ResolveCanonicalPredecessorForTest {
        predecessor: PendingDurableResult,
        response: oneshot::Sender<bool>,
    },
    #[cfg(test)]
    ReleaseCanonicalPredecessorWithoutProgressForTest {
        predecessor: PendingDurableResult,
        response: oneshot::Sender<bool>,
    },
    #[cfg(test)]
    FreezeDegradedPreservingPendingForTest {
        response: oneshot::Sender<()>,
    },
    #[cfg(test)]
    HandleCriticalWakeForTest {
        response: oneshot::Sender<()>,
    },
    #[cfg(test)]
    StageHistoricalRecordReviewPairForTest {
        task_id: TaskId,
        requests: [RecordReviewRequest; 2],
        review_responses: [oneshot::Sender<Result<EventId, RunnerEventError>>; 2],
        response: oneshot::Sender<[(TaskMutationIdentity, RecordReviewRequest); 2]>,
    },
    #[cfg(test)]
    InjectFinalizeDegradedForTest {
        attempt_id: u64,
        barrier_epoch: u64,
        recovery: coding_agent_store::RecoveryOutcome,
        replayed_pending_count: usize,
        high_watermark: EventCursor,
        response: oneshot::Sender<Result<DegradedRecoveryResult, DegradedCoordinatorError>>,
    },
    #[cfg(test)]
    InspectActivePendingStopWrite {
        task_id: TaskId,
        response: oneshot::Sender<Option<PendingDurableResult>>,
    },
    #[cfg(test)]
    InjectRecordReviewCompletion {
        identity: TaskMutationIdentity,
        request: RecordReviewRequest,
        completion: DurableCompletion<RecordReviewOutcome>,
        response: oneshot::Sender<Result<EventId, RunnerEventError>>,
    },
    #[cfg(test)]
    InjectTerminalWriteCompletion {
        task_id: TaskId,
        attempt_id: u64,
        identity: TaskMutationIdentity,
        stage: TerminalWriteStage,
        completion: TerminalWriteCompletion,
        response: oneshot::Sender<()>,
    },
    #[cfg(test)]
    InjectFinalStopCompletion {
        identity: TaskMutationIdentity,
        request: FinalizeStoppedTaskRequest,
        completion: DurableCompletion<FinalizeStoppedTaskOutcome>,
        response: oneshot::Sender<()>,
    },
    #[cfg(test)]
    InjectPendingReplayCompletion {
        attempt_id: u64,
        pending: PendingDurableResult,
        result: Result<DurableCompletion<PendingReplayReceipt>, StoreWriterSubmitError>,
        response: oneshot::Sender<()>,
    },
    #[cfg(test)]
    InjectPendingReplayRetry {
        attempt_id: u64,
        response: oneshot::Sender<()>,
    },
    #[cfg(test)]
    InjectGenericRecoveryCompletion {
        attempt_id: u64,
        result: Result<DegradedRecoveryResult, DegradedCoordinatorError>,
        response: oneshot::Sender<()>,
    },
    #[cfg(test)]
    InjectRunningUserCancelAfterLookup {
        task_id: TaskId,
        response: CancelResponse,
    },
    #[cfg(test)]
    InjectStaleCancelTaskLoaded {
        task: Task,
        response: CancelResponse,
    },
    #[cfg(test)]
    InjectTerminalProjection {
        completion: TerminalProjectionCompletion,
        response: oneshot::Sender<TerminalProjectionSnapshotForTest>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmissionPhase {
    ClaimPending,
    ClaimUnknown,
    LaunchGatePending,
    LaunchSuppressed,
    Preparing,
    Running,
    RunnerReturned,
    TerminalWritePending,
    ProjectionPending,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActiveStopStageForTest {
    NoWinner,
    IntentSubmissionDeferred,
    IntentWritePending,
    IntentDurable,
    FinalStopWritePending,
    StopTerminal,
    TerminalWon,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActiveStopSnapshotForTest {
    phase: AdmissionPhase,
    stage: ActiveStopStageForTest,
    active_count: usize,
    available_permits: usize,
    cleanup_confirmed: bool,
    cleanup_available: bool,
    permit_active: bool,
    done_receiver_owned: bool,
    in_flight_mutations: usize,
    durable_sequence_blocked: bool,
    pending_runner_event_write_count: usize,
    pending_runner_event_identity: Option<TaskMutationIdentity>,
    pending_record_review_replay_count: usize,
    pending_record_review_write_count: usize,
    pending_record_review_attempt_id: Option<u64>,
    pending_record_review_identity: Option<TaskMutationIdentity>,
    pending_record_review_deadline: Option<Instant>,
    pending_record_review_retry_available: Option<bool>,
    next_typed_write_attempt_id: u64,
    next_terminal_projection_attempt_id: u64,
    next_mutation_sequence: u64,
    applied_record_review_count: usize,
    pending_terminal_attempt_id: Option<u64>,
    pending_terminal_identity: Option<TaskMutationIdentity>,
    pending_terminal_stage: Option<TerminalWriteStage>,
    pending_terminal_deadline: Option<Instant>,
    pending_terminal_retry_available: Option<bool>,
    staged_stop_completion_count: usize,
    terminal_task_set: bool,
    terminal_projection_attempt: Option<TerminalProjectionAttempt>,
    registry_owned: bool,
    permit_process_owner_id: u64,
    process_scope_owner_id: u64,
    hard_frozen: bool,
    pending_replay_in_flight: bool,
    pending_replay_attempt_id: Option<u64>,
    pending_replay_deadline: Option<Instant>,
    generic_recovery_attempt_id: Option<u64>,
    quiesce_recovery_running: bool,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ExactBarrierSnapshotForTest {
    detached_cancel_completions: usize,
    staged_stop_completion_count: usize,
    pending_durable_result_count: usize,
    pending_replay_in_flight: bool,
    barrier_epoch: u64,
    generic_recovery_attempt_id: Option<u64>,
    generic_recovery_barrier_epoch: Option<u64>,
    quiesce_recovery_running: bool,
    hard_frozen: bool,
}

#[cfg(test)]
struct HistoricalRecordReviewPairForTest {
    entries: [(TaskMutationIdentity, RecordReviewRequest); 2],
    responses: [oneshot::Receiver<Result<EventId, RunnerEventError>>; 2],
}

#[cfg(test)]
pub(crate) struct StagedStopCompletionForTest {
    identity: TaskMutationIdentity,
    request: StopIntentRequest,
    predecessor: PendingDurableResult,
    completion: DurableCompletion<StopIntentBatchReceipt>,
}

#[cfg(all(test, feature = "test-support"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SafetyRegistrySnapshotForTest {
    entry_count: usize,
    pending_critical_count: usize,
    safety_latched_count: usize,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TerminalProjectionSnapshotForTest {
    active: bool,
    phase: Option<AdmissionPhase>,
    current_attempt: Option<TerminalProjectionAttempt>,
    next_attempt_id: u64,
    next_typed_write_attempt_id: u64,
    next_mutation_sequence: Option<u64>,
    cleanup_available: bool,
    permit_active: bool,
    registry_owned: bool,
    hard_frozen: bool,
}

type CancelResponse = oneshot::Sender<Result<CancelOutcome, TaskManagerError>>;

#[derive(Clone, Copy)]
enum CancelTaskLookupKind {
    MayPredateActiveRelease,
    ReloadedAfterActiveRelease,
}

enum ActiveStopState {
    NoWinner,
    IntentSubmissionDeferred {
        kind: StopIntentKind,
        identity: TaskMutationIdentity,
        request: StopIntentRequest,
        deadline: Instant,
        retries_remaining: u8,
    },
    IntentWritePending {
        kind: StopIntentKind,
        identity: TaskMutationIdentity,
        request: StopIntentRequest,
        deadline: Instant,
        retries_remaining: u8,
    },
    IntentDurable {
        identity: TaskMutationIdentity,
        receipt: StopIntentReceipt,
    },
    FinalStopWritePending {
        kind: StopIntentKind,
        receipt: Option<StopIntentReceipt>,
        identity: TaskMutationIdentity,
        request: FinalizeStoppedTaskRequest,
        deadline: Instant,
        retries_remaining: u8,
    },
    StopTerminal {
        receipt: StopIntentReceipt,
        task: Task,
        terminal_event_id: EventId,
    },
    TerminalWon {
        task: Task,
    },
}

#[derive(Debug, Clone)]
struct StopIntentLineage {
    identity: TaskMutationIdentity,
    request: StopIntentRequest,
    decision: StopIntentLineageDecision,
}

#[derive(Debug, Clone)]
enum StopIntentLineageDecision {
    Durable(StopIntentReceipt),
    TerminalWon(Task),
    IntentConflict(StopIntentReceipt),
}

struct PendingRecordReviewReplay {
    lineage_id: u64,
    attempt_id: u64,
    operation_nonce: u64,
    request: RecordReviewRequest,
    deadline: Instant,
    response: Option<oneshot::Sender<Result<EventId, RunnerEventError>>>,
    deferred_original: Option<RecordReviewOutcome>,
    deferred_observers: Vec<oneshot::Sender<Result<EventId, RunnerEventError>>>,
}

struct PendingRecordReviewWrite {
    stage: RecordReviewWriteStage,
    request: RecordReviewRequest,
    deadline: Instant,
    response: Option<oneshot::Sender<Result<EventId, RunnerEventError>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordReviewWriteStage {
    Deferred,
    Submitted {
        attempt_id: u64,
        identity: TaskMutationIdentity,
        retry_available: bool,
    },
}

struct PendingRunnerEventWrite {
    stage: RunnerEventWriteStage,
    deadline: Instant,
    response: Option<oneshot::Sender<Result<EventId, RunnerEventError>>>,
}

enum RunnerEventWriteStage {
    Deferred(RunnerEvent),
    Submitted { identity: TaskMutationIdentity },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypedWriteAttemptOwner {
    Terminal {
        task_id: TaskId,
        operation_nonce: u64,
        identity: TaskMutationIdentity,
        stage: TerminalWriteStage,
    },
    RecordReview {
        task_id: TaskId,
        operation_nonce: u64,
        lineage_id: u64,
        identity: TaskMutationIdentity,
    },
}

#[derive(Debug, Clone)]
struct AppliedRecordReview {
    request: RecordReviewRequest,
    event_id: EventId,
}

#[derive(Debug, Clone)]
struct AppliedFinalStop {
    identity: TaskMutationIdentity,
    request: FinalizeStoppedTaskRequest,
    outcome: FinalizeStoppedTaskOutcome,
}

#[derive(Debug, Clone)]
struct ResolvedPendingReplay {
    pending: PendingDurableResult,
    receipt: PendingReplayReceipt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CanonicalPendingState {
    Absent,
    Ready,
    Blocked,
}

#[derive(Debug, Clone)]
struct StagedStopIntentCompletion {
    identity: DurableOperationIdentity,
    completion: DurableCompletion<StopIntentBatchReceipt>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopCompletionOwnership {
    FullyAbsent,
    FullyOwned,
    Mixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopCompletionDrain {
    Continue,
    Stop,
}

impl ActiveStopState {
    const fn kind(&self) -> Option<StopIntentKind> {
        match self {
            Self::NoWinner | Self::TerminalWon { .. } => None,
            Self::IntentSubmissionDeferred { kind, .. } | Self::IntentWritePending { kind, .. } => {
                Some(*kind)
            }
            Self::IntentDurable { receipt, .. } | Self::StopTerminal { receipt, .. } => {
                Some(receipt.kind)
            }
            Self::FinalStopWritePending { kind, .. } => Some(*kind),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaunchStopState {
    Clear,
    CancellationRequested,
    SafetyLatched,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaunchSuppressionReason {
    UserCancelled,
    ShutdownOrDegraded,
    StorageBlocked,
    SafetyCritical,
}

fn classify_launch_suppression(
    manager_allows_launch: bool,
    storage_allows_launch: bool,
    stop_state: LaunchStopState,
    user_stop_accepted: bool,
) -> Result<Option<LaunchSuppressionReason>, ()> {
    if stop_state == LaunchStopState::SafetyLatched {
        return Ok(Some(LaunchSuppressionReason::SafetyCritical));
    }
    if !manager_allows_launch {
        return Ok(Some(LaunchSuppressionReason::ShutdownOrDegraded));
    }
    if !storage_allows_launch {
        return Ok(Some(LaunchSuppressionReason::StorageBlocked));
    }
    if user_stop_accepted {
        return Ok(Some(LaunchSuppressionReason::UserCancelled));
    }
    match stop_state {
        LaunchStopState::Clear => Ok(None),
        LaunchStopState::CancellationRequested => Err(()),
        LaunchStopState::SafetyLatched => unreachable!("handled above"),
    }
}

static NEXT_TASK_PROCESS_SCOPE_OWNER: AtomicU64 = AtomicU64::new(1);

/// Exact task-scoped process ownership derived by this TaskManager from its
/// startup instance scope. The constructor is private so a same-task scope
/// from another runtime directory cannot be substituted at cleanup time.
pub(crate) struct TaskProcessScopeOwnership {
    inner: Arc<TaskProcessScopeInner>,
}

struct TaskProcessScopeInner {
    task_id: TaskId,
    operation_nonce: u64,
    owner_id: u64,
    scope: ProcessLivenessScope,
    sealed: Mutex<Option<coding_agent_runtime::SealedProcessLivenessScope>>,
}

impl TaskProcessScopeOwnership {
    fn derive(
        instance_scope: &ProcessLivenessScope,
        task_id: TaskId,
        operation_nonce: u64,
    ) -> Result<Self, coding_agent_runtime::ProcessLivenessError> {
        if operation_nonce == 0 {
            return Err(coding_agent_runtime::ProcessLivenessError::InvalidIdentity);
        }
        let owner_id = NEXT_TASK_PROCESS_SCOPE_OWNER.fetch_add(1, Ordering::Relaxed);
        if owner_id == 0 {
            return Err(coding_agent_runtime::ProcessLivenessError::Unavailable);
        }
        let scope = instance_scope.task_scope(*task_id.as_uuid().as_bytes())?;
        Ok(Self {
            inner: Arc::new(TaskProcessScopeInner {
                task_id,
                operation_nonce,
                owner_id,
                scope,
                sealed: Mutex::new(None),
            }),
        })
    }

    pub(crate) fn task_id(&self) -> TaskId {
        self.inner.task_id
    }

    pub(crate) fn operation_nonce(&self) -> u64 {
        self.inner.operation_nonce
    }

    pub(crate) fn owner_id(&self) -> u64 {
        self.inner.owner_id
    }

    pub(crate) fn scope(&self) -> &ProcessLivenessScope {
        &self.inner.scope
    }

    fn seal_and_cleanup(
        &self,
    ) -> Result<ProcessCleanupProof, coding_agent_runtime::ProcessLivenessError> {
        let mut sealed = self
            .inner
            .sealed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if sealed.is_none() {
            *sealed = Some(
                self.inner
                    .scope
                    .seal_task_scope(*self.inner.task_id.as_uuid().as_bytes())?,
            );
        }
        sealed
            .as_ref()
            .expect("sealed task process scope was installed")
            .cleanup_proof()
    }
}

impl Clone for TaskProcessScopeOwnership {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl std::fmt::Debug for TaskProcessScopeOwnership {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TaskProcessScopeOwnership(<opaque>)")
    }
}

struct ActiveRunner {
    actor_liveness: Option<mpsc::Sender<TaskManagerMessage>>,
    cancellation: CancellationToken,
    phase: AdmissionPhase,
    operation_nonce: u64,
    permit: SharedPermitOwnership,
    control_lease: Option<RepositoryControlLease>,
    control_recovery: Option<ActiveRepositoryControlRecovery>,
    repository: Repository,
    claimed_task: Option<Task>,
    claim_identity: TaskMutationIdentity,
    claim_request: ClaimTaskRequest,
    process_scope: TaskProcessScopeOwnership,
    cleanup_confirmation: Option<TaskProcessCleanupConfirmation>,
    terminal_event: Option<(TaskEventKind, EventId)>,
    terminal_projection_barrier: Option<TerminalProjectionBarrier>,
    preparation_complete: bool,
    repository_id: RepositoryId,
    attempt: u32,
    stop_state: ActiveStopState,
    stop_intent_lineage: Option<StopIntentLineage>,
    applied_final_stop: Option<AppliedFinalStop>,
    pending_terminal_write: Option<PendingTerminalWrite>,
    pending_runner_event_writes: HashMap<u64, PendingRunnerEventWrite>,
    pending_record_review_writes: HashMap<u64, PendingRecordReviewWrite>,
    pending_record_review_replays: HashMap<TaskMutationIdentity, PendingRecordReviewReplay>,
    applied_record_reviews: HashMap<TaskMutationIdentity, AppliedRecordReview>,
    next_runner_mutation_id: u64,
    user_cancel_waiters: Vec<CancelResponse>,
    terminal_cancel_waiters: Vec<CancelResponse>,
    accepted_stop_task: Option<Task>,
    accepted_stop_task_load_in_flight: bool,
    next_mutation_sequence: u64,
    durable_sequence_blocked: bool,
    in_flight_mutations: usize,
    pending_runner_outcome: Option<RunnerOutcome>,
    runner_returned: Option<RunnerReturnedState>,
    cleanup_retry_scheduled: bool,
    launch_suppression: Option<LaunchSuppressionReason>,
    recovery_release_ready: bool,
    terminal_task: Option<Task>,
    done_sender: Option<oneshot::Sender<()>>,
    done_receiver: Option<oneshot::Receiver<()>>,
}

fn claim_receipt_is_exact(active: &ActiveRunner, receipt: &ClaimTaskReceipt) -> bool {
    receipt.task.id == active.claim_request.task_id
        && receipt.task.repository_id == active.claim_request.expected_repository_id
        && receipt.task.attempt == active.claim_request.expected_attempt
        && receipt.task.status == TaskStatus::Running
        && receipt.task.started_at.is_some()
        && receipt.task.finished_at.is_none()
        && receipt.task.failure.is_none()
        && receipt.started_event_id == receipt.task.last_event_id
}

fn terminal_event_kind(status: TaskStatus) -> Option<TaskEventKind> {
    match status {
        TaskStatus::Completed => Some(TaskEventKind::TaskCompleted),
        TaskStatus::Failed => Some(TaskEventKind::TaskFailed),
        TaskStatus::Cancelled => Some(TaskEventKind::TaskCancelled),
        TaskStatus::Interrupted => Some(TaskEventKind::TaskInterrupted),
        TaskStatus::Queued | TaskStatus::Running => None,
    }
}

fn terminal_tasks_from_scheduler_snapshot(
    snapshot: &SchedulerBootstrapSnapshot,
    task_ids: &[TaskId],
) -> Option<Vec<Task>> {
    let mut terminal_tasks = Vec::with_capacity(task_ids.len());
    for task_id in task_ids {
        let task = snapshot.tasks.iter().find(|task| task.id == *task_id)?;
        terminal_event_kind(task.status)?;
        terminal_tasks.push(task.clone());
    }
    (terminal_tasks.len() == task_ids.len()).then_some(terminal_tasks)
}

const fn task_status_is_terminal(status: TaskStatus) -> bool {
    matches!(
        status,
        TaskStatus::Completed
            | TaskStatus::Failed
            | TaskStatus::Cancelled
            | TaskStatus::Interrupted
    )
}

pub(crate) fn terminal_task_is_structurally_valid(task: &Task) -> bool {
    Task::try_from_stored(task.clone()).is_ok()
        && task_status_is_terminal(task.status)
        && task.started_at.is_some()
        && task.finished_at.is_some_and(|finished_at| {
            task.started_at
                .is_some_and(|started_at| finished_at >= started_at)
        })
}

fn stop_receipt_matches_request(receipt: StopIntentReceipt, request: StopIntentRequest) -> bool {
    receipt.task_id == request.task_id
        && receipt.repository_id == request.expected_repository_id
        && receipt.attempt == request.expected_attempt
        && receipt.kind == request.kind
}

fn stop_receipt_matches_final_request(
    receipt: StopIntentReceipt,
    request: FinalizeStoppedTaskRequest,
) -> bool {
    receipt.task_id == request.task_id
        && receipt.repository_id == request.expected_repository_id
        && receipt.attempt == request.expected_attempt
        && receipt.kind == request.expected_intent
}

fn stopped_terminal_matches_intent(task: &Task, intent: StopIntentReceipt) -> bool {
    if !terminal_task_is_structurally_valid(task)
        || task.id != intent.task_id
        || task.repository_id != intent.repository_id
        || task.attempt != intent.attempt
        || task.delivery_readiness != coding_agent_domain::DeliveryReadiness::Unreviewed
    {
        return false;
    }
    match intent.kind {
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

fn stop_intent_lineage_matches_state(active: &ActiveRunner, lineage: &StopIntentLineage) -> bool {
    match (&lineage.decision, &active.stop_state) {
        (
            StopIntentLineageDecision::Durable(expected_receipt),
            ActiveStopState::IntentDurable { identity, receipt },
        ) => *identity == lineage.identity && receipt == expected_receipt,
        (
            StopIntentLineageDecision::Durable(expected_receipt),
            ActiveStopState::FinalStopWritePending {
                kind,
                receipt: Some(receipt),
                request,
                ..
            },
        ) => {
            *kind == expected_receipt.kind
                && receipt == expected_receipt
                && stop_receipt_matches_final_request(*expected_receipt, *request)
        }
        (
            StopIntentLineageDecision::Durable(expected_receipt),
            ActiveStopState::StopTerminal {
                receipt,
                task,
                terminal_event_id,
            },
        ) => {
            receipt == expected_receipt
                && stopped_terminal_matches_active_intent(active, task, *expected_receipt)
                && task.last_event_id == *terminal_event_id
        }
        (
            StopIntentLineageDecision::Durable(expected_receipt),
            ActiveStopState::TerminalWon { task },
        ) => stopped_terminal_matches_active_intent(active, task, *expected_receipt),
        (
            StopIntentLineageDecision::TerminalWon(expected),
            ActiveStopState::TerminalWon { task },
        ) => {
            task == expected
                && terminal_event_kind(task.status).is_some_and(|event_kind| {
                    terminal_receipt_is_exact(Some(active), task, event_kind, task.last_event_id)
                })
        }
        (
            StopIntentLineageDecision::IntentConflict(_),
            ActiveStopState::IntentWritePending {
                identity, request, ..
            },
        ) => *identity == lineage.identity && *request == lineage.request,
        _ => false,
    }
}

fn stop_intent_outcome_matches_lineage(
    lineage: &StopIntentLineage,
    request: StopIntentRequest,
    outcome: &PersistStopIntentOutcome,
) -> bool {
    if lineage.request != request {
        return false;
    }
    match (&lineage.decision, outcome) {
        (
            StopIntentLineageDecision::Durable(expected),
            PersistStopIntentOutcome::Applied(receipt)
            | PersistStopIntentOutcome::Existing(receipt),
        ) => receipt == expected,
        (
            StopIntentLineageDecision::TerminalWon(expected),
            PersistStopIntentOutcome::TerminalWon { current },
        ) => current == expected,
        (
            StopIntentLineageDecision::IntentConflict(expected),
            PersistStopIntentOutcome::IntentConflict { existing },
        ) => existing == expected,
        _ => false,
    }
}

fn stop_request_for_completion(
    active: &ActiveRunner,
    expected_identity: TaskMutationIdentity,
) -> Option<StopIntentRequest> {
    if let Some(lineage) = &active.stop_intent_lineage {
        return (lineage.identity == expected_identity
            && stop_intent_lineage_matches_state(active, lineage))
        .then_some(lineage.request);
    }
    match &active.stop_state {
        ActiveStopState::IntentWritePending {
            identity, request, ..
        } if *identity == expected_identity => Some(*request),
        _ => None,
    }
}

fn pending_replay_receipt_matches(
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
                && record_review_outcome(request, outcome).is_some()
        }
        (
            PendingDurableResult::FinalizeReviewedTask { identity, request },
            PendingReplayReceipt::FinalizeReviewedTask(outcome),
        ) => {
            identity.task_id == request.task_id
                && identity.kind == DurableOperationKind::FinalizeReviewedTask
                && reviewed_terminal_outcome(request, outcome.clone()).is_some()
        }
        (
            PendingDurableResult::FinalizeUnreviewedTask { identity, request },
            PendingReplayReceipt::FinalizeUnreviewedTask(outcome),
        ) => {
            identity.task_id == request.task_id
                && identity.kind == DurableOperationKind::FinalizeUnreviewedTask
                && unreviewed_terminal_outcome(request, outcome.clone()).is_some()
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
                && items.iter().zip(requests).zip(&receipt.items).all(
                    |((identity, request), item)| {
                        identity.task_id == request.task_id
                            && identity.kind == DurableOperationKind::PersistStopIntent
                            && item.request == *request
                    },
                )
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
                        stop_receipt_matches_final_request(receipt.intent, *request)
                            && receipt.task.id == request.task_id
                            && receipt.task.repository_id == request.expected_repository_id
                            && receipt.task.attempt == request.expected_attempt
                            && stopped_terminal_matches_intent(&receipt.task, receipt.intent)
                            && receipt.terminal_event_id == receipt.task.last_event_id
                    }
                    FinalizeStoppedTaskOutcome::InvariantConflict => true,
                }
        }
        _ => false,
    }
}

fn pending_replay_receipts_are_equivalent(
    pending: &PendingDurableResult,
    expected: &PendingReplayReceipt,
    actual: &PendingReplayReceipt,
) -> bool {
    match (pending, expected, actual) {
        (
            PendingDurableResult::RecordReview { request, .. },
            PendingReplayReceipt::RecordReview(expected),
            PendingReplayReceipt::RecordReview(actual),
        ) => {
            let expected = record_review_outcome(request, expected);
            expected.is_some() && expected == record_review_outcome(request, actual)
        }
        (
            PendingDurableResult::PersistStopIntentBatch { requests, .. },
            PendingReplayReceipt::PersistStopIntentBatch(expected),
            PendingReplayReceipt::PersistStopIntentBatch(actual),
        ) => {
            expected.items.len() == requests.len()
                && actual.items.len() == requests.len()
                && expected.items.iter().zip(&actual.items).zip(requests).all(
                    |((expected, actual), request)| {
                        expected.request == *request
                            && actual.request == *request
                            && match (&expected.outcome, &actual.outcome) {
                                (
                                    PersistStopIntentOutcome::Applied(expected)
                                    | PersistStopIntentOutcome::Existing(expected),
                                    PersistStopIntentOutcome::Applied(actual)
                                    | PersistStopIntentOutcome::Existing(actual),
                                ) => expected == actual,
                                (
                                    PersistStopIntentOutcome::TerminalWon { current: expected },
                                    PersistStopIntentOutcome::TerminalWon { current: actual },
                                ) => expected == actual,
                                (
                                    PersistStopIntentOutcome::IntentConflict { existing: expected },
                                    PersistStopIntentOutcome::IntentConflict { existing: actual },
                                ) => expected == actual,
                                _ => false,
                            }
                    },
                )
        }
        (
            PendingDurableResult::FinalizeStoppedTask { .. },
            PendingReplayReceipt::FinalizeStoppedTask(expected),
            PendingReplayReceipt::FinalizeStoppedTask(actual),
        ) => match (expected, actual) {
            (
                FinalizeStoppedTaskOutcome::Applied(expected)
                | FinalizeStoppedTaskOutcome::Existing(expected),
                FinalizeStoppedTaskOutcome::Applied(actual)
                | FinalizeStoppedTaskOutcome::Existing(actual),
            ) => expected == actual,
            (
                FinalizeStoppedTaskOutcome::InvariantConflict,
                FinalizeStoppedTaskOutcome::InvariantConflict,
            ) => true,
            _ => false,
        },
        _ => expected == actual,
    }
}

fn durable_identity_contains_task(identity: &DurableOperationIdentity, task_id: TaskId) -> bool {
    match identity {
        DurableOperationIdentity::TaskMutation(identity) => identity.task_id == task_id,
        DurableOperationIdentity::StopIntentBatch { items } => {
            items.iter().any(|identity| identity.task_id == task_id)
        }
        DurableOperationIdentity::CreateTask { .. }
        | DurableOperationIdentity::RetryTask { .. } => false,
    }
}

fn send_terminal_cancel_response(response: CancelResponse, task: Task) {
    let result = if task.status == TaskStatus::Cancelled {
        Ok(CancelOutcome::Cancelled { task })
    } else {
        Ok(CancelOutcome::Finished { task })
    };
    let _ = response.send(result);
}

fn terminal_receipt_is_exact(
    active: Option<&ActiveRunner>,
    task: &Task,
    event_kind: TaskEventKind,
    event_id: EventId,
) -> bool {
    active.is_some_and(|active| {
        terminal_task_matches_claimed(active, task)
            && task.id == active.claim_request.task_id
            && task.repository_id == active.repository_id
            && task.attempt == active.attempt
            && terminal_event_kind(task.status) == Some(event_kind)
            && task.last_event_id == event_id
    })
}

fn terminal_task_matches_claimed(active: &ActiveRunner, task: &Task) -> bool {
    let Some(claimed) = active.claimed_task.as_ref() else {
        return false;
    };
    if Task::try_from_stored(claimed.clone()).is_err() || !terminal_task_is_structurally_valid(task)
    {
        return false;
    }
    task.id == claimed.id
        && task.client_request_id == claimed.client_request_id
        && task.repository_id == claimed.repository_id
        && task.prompt == claimed.prompt
        && task.attempt == claimed.attempt
        && task.retry_of == claimed.retry_of
        && task.created_at == claimed.created_at
        && task.started_at == claimed.started_at
        && claimed.status == TaskStatus::Running
        && claimed.started_at.is_some()
        && claimed.finished_at.is_none()
        && claimed.failure.is_none()
        && task.finished_at.is_some_and(|finished_at| {
            claimed
                .started_at
                .is_some_and(|started_at| finished_at >= started_at)
        })
        && task.last_event_id > claimed.last_event_id
}

fn stopped_terminal_matches_active_intent(
    active: &ActiveRunner,
    task: &Task,
    intent: StopIntentReceipt,
) -> bool {
    stopped_terminal_matches_intent(task, intent)
        && terminal_event_kind(task.status).is_some_and(|event_kind| {
            terminal_receipt_is_exact(Some(active), task, event_kind, task.last_event_id)
        })
}

fn unreviewed_terminal_outcome(
    request: &FinalizeUnreviewedTaskRequest,
    outcome: FinalizeUnreviewedTaskOutcome,
) -> Option<(Task, TaskEventKind, EventId)> {
    let (task, event_id) = match outcome {
        FinalizeUnreviewedTaskOutcome::Applied { task, event_id }
        | FinalizeUnreviewedTaskOutcome::Existing { task, event_id } => (task, event_id),
        FinalizeUnreviewedTaskOutcome::InvariantConflict => return None,
    };
    let event_kind = terminal_event_kind(task.status)?;
    if task.id != request.task_id
        || task.repository_id != request.expected_repository_id
        || task.attempt != request.expected_attempt
        || task.status != request.transition.next()
        || task.started_at.is_none()
        || task.finished_at.is_none()
        || task.failure.as_ref() != request.transition.failure()
        || task.last_event_id != event_id
    {
        return None;
    }
    Some((task, event_kind, event_id))
}

// Design: the applied variant transfers the validated task and receipt together;
// keeping them inline makes that atomic ownership hand-off explicit.
#[allow(clippy::large_enum_variant)]
enum UnreviewedTerminalCompletion {
    Applied {
        task: Task,
        event_kind: TaskEventKind,
        event_id: EventId,
        outcome: FinalizeUnreviewedTaskOutcome,
    },
    ReplaySame,
    RetryNext,
    Freeze,
}

fn classify_unreviewed_terminal_completion(
    identity: TaskMutationIdentity,
    request: &FinalizeUnreviewedTaskRequest,
    pending: &PendingDurableResult,
    completion: DurableCompletion<FinalizeUnreviewedTaskOutcome>,
) -> UnreviewedTerminalCompletion {
    if completion.identity != DurableOperationIdentity::TaskMutation(identity) {
        return UnreviewedTerminalCompletion::Freeze;
    }
    match (completion.sequence_disposition, completion.disposition) {
        (MutationSequenceDisposition::AdvanceNext, DurableDisposition::Confirmed(outcome)) => {
            match unreviewed_terminal_outcome(request, outcome.clone()) {
                Some((task, event_kind, event_id)) => UnreviewedTerminalCompletion::Applied {
                    task,
                    event_kind,
                    event_id,
                    outcome,
                },
                None => UnreviewedTerminalCompletion::Freeze,
            }
        }
        (
            MutationSequenceDisposition::RetainSame,
            DurableDisposition::KnownNotApplied {
                reason: KnownNotAppliedReason::IngressClosed | KnownNotAppliedReason::IngressFull,
                outcome: None,
                error: None,
            },
        ) => UnreviewedTerminalCompletion::ReplaySame,
        (
            MutationSequenceDisposition::BlockUnknown,
            DurableDisposition::OutcomeUnknown {
                pending: Some(returned),
                ..
            },
        ) if returned == *pending => UnreviewedTerminalCompletion::ReplaySame,
        (
            MutationSequenceDisposition::AdvanceNext,
            DurableDisposition::KnownNotApplied {
                reason:
                    KnownNotAppliedReason::BusyRolledBack | KnownNotAppliedReason::DeadlineBeforeStart,
                outcome: None,
                ..
            },
        ) => UnreviewedTerminalCompletion::RetryNext,
        _ => UnreviewedTerminalCompletion::Freeze,
    }
}

fn reviewed_evidence_is_exact(review: &ReviewEvidence, expected: &NewReviewEvidence) -> bool {
    let Ok(mut stored_value) = serde_json::to_value(review) else {
        return false;
    };
    let Some(stored) = stored_value.as_object_mut() else {
        return false;
    };
    stored.remove("created_at");
    serde_json::to_value(expected).is_ok_and(|expected| expected == stored_value)
}

fn record_review_outcome(
    request: &RecordReviewRequest,
    outcome: &RecordReviewOutcome,
) -> Option<EventId> {
    let (review, event_id) = match outcome {
        RecordReviewOutcome::Applied { review, event_id }
        | RecordReviewOutcome::Existing { review, event_id } => (review, *event_id),
    };
    reviewed_evidence_is_exact(review, &request.evidence).then_some(event_id)
}

fn reviewed_terminal_outcome(
    request: &FinalizeReviewedTaskRequest,
    outcome: FinalizeReviewedTaskOutcome,
) -> Option<(Task, TaskEventKind, EventId)> {
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
        } => (task, review, review_event_id, terminal_event_id),
    };
    let (expected_status, expected_readiness, expected_failure) = match request.evidence.verdict() {
        ReviewVerdict::Approved => (
            TaskStatus::Completed,
            coding_agent_domain::DeliveryReadiness::ReviewApproved,
            None,
        ),
        ReviewVerdict::ChangesRequested => (
            TaskStatus::Failed,
            coding_agent_domain::DeliveryReadiness::ReviewRejected,
            Some(TaskFailure {
                code: "REVIEW_REJECTED".to_owned(),
                message: "review rejected after three rounds".to_owned(),
                retryable: true,
            }),
        ),
    };
    let event_kind = terminal_event_kind(task.status)?;
    if task.id != request.task_id
        || task.repository_id != request.expected_repository_id
        || task.attempt != request.expected_attempt
        || task.status != expected_status
        || task.delivery_readiness != expected_readiness
        || task.failure != expected_failure
        || task.finished_at != Some(review.created_at())
        || task.last_event_id != terminal_event_id
        || review_event_id.get().checked_add(1) != Some(terminal_event_id.get())
        || !reviewed_evidence_is_exact(&review, &request.evidence)
    {
        return None;
    }
    Some((task, event_kind, terminal_event_id))
}

// Design: the applied variant transfers the validated task and review receipt
// together, so this short-lived classifier result intentionally owns both inline.
#[allow(clippy::large_enum_variant)]
enum ReviewedTerminalCompletion {
    Applied {
        task: Task,
        event_kind: TaskEventKind,
        event_id: EventId,
        outcome: FinalizeReviewedTaskOutcome,
    },
    ReplaySame,
    RetryNext,
    Freeze,
}

fn classify_reviewed_terminal_completion(
    identity: TaskMutationIdentity,
    request: &FinalizeReviewedTaskRequest,
    pending: &PendingDurableResult,
    completion: DurableCompletion<FinalizeReviewedTaskOutcome>,
) -> ReviewedTerminalCompletion {
    if completion.identity != DurableOperationIdentity::TaskMutation(identity) {
        return ReviewedTerminalCompletion::Freeze;
    }
    match (completion.sequence_disposition, completion.disposition) {
        (MutationSequenceDisposition::AdvanceNext, DurableDisposition::Confirmed(outcome)) => {
            match reviewed_terminal_outcome(request, outcome.clone()) {
                Some((task, event_kind, event_id)) => ReviewedTerminalCompletion::Applied {
                    task,
                    event_kind,
                    event_id,
                    outcome,
                },
                None => ReviewedTerminalCompletion::Freeze,
            }
        }
        (
            MutationSequenceDisposition::RetainSame,
            DurableDisposition::KnownNotApplied {
                reason: KnownNotAppliedReason::IngressClosed | KnownNotAppliedReason::IngressFull,
                outcome: None,
                error: None,
            },
        ) => ReviewedTerminalCompletion::ReplaySame,
        (
            MutationSequenceDisposition::BlockUnknown,
            DurableDisposition::OutcomeUnknown {
                pending: Some(returned),
                ..
            },
        ) if returned == *pending => ReviewedTerminalCompletion::ReplaySame,
        (
            MutationSequenceDisposition::AdvanceNext,
            DurableDisposition::KnownNotApplied {
                reason:
                    KnownNotAppliedReason::BusyRolledBack | KnownNotAppliedReason::DeadlineBeforeStart,
                outcome: None,
                ..
            },
        ) => ReviewedTerminalCompletion::RetryNext,
        _ => ReviewedTerminalCompletion::Freeze,
    }
}

#[derive(Clone)]
struct StorageAdmissionCandidate {
    scan_generation: u64,
    operation_nonce: u64,
    task: Task,
    repository: Repository,
    coordination_key: RepositoryCoordinationKey,
}

struct PendingQuiesce {
    quiesce_id: u64,
    deadline: Instant,
    response: oneshot::Sender<Result<QuiesceResult, TaskManagerError>>,
    recovery_started: bool,
    recovery_safety_generation: Option<u64>,
}

#[cfg(test)]
pub(crate) struct RecoveryFinalizationPauseForTest {
    reached: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

#[derive(Clone)]
struct PendingReplayAttempt {
    attempt_id: u64,
    pending: PendingDurableResult,
    deadline: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TerminalWriteStage {
    SubmitPending,
    ReconcileSamePending,
}

#[derive(Clone)]
enum PendingTerminalWriteKind {
    Reviewed(FinalizeReviewedTaskRequest),
    Unreviewed(FinalizeUnreviewedTaskRequest),
}

impl PendingTerminalWriteKind {
    fn pending(&self, identity: TaskMutationIdentity) -> PendingDurableResult {
        match self {
            Self::Reviewed(request) => PendingDurableResult::FinalizeReviewedTask {
                identity,
                request: request.clone(),
            },
            Self::Unreviewed(request) => PendingDurableResult::FinalizeUnreviewedTask {
                identity,
                request: request.clone(),
            },
        }
    }
}

#[derive(Clone)]
struct PendingTerminalWrite {
    attempt_id: u64,
    identity: TaskMutationIdentity,
    kind: PendingTerminalWriteKind,
    stage: TerminalWriteStage,
    deadline: Instant,
    retry_available: bool,
}

pub(crate) enum TerminalWriteCompletion {
    Reviewed(DurableCompletion<FinalizeReviewedTaskOutcome>),
    Unreviewed(DurableCompletion<FinalizeUnreviewedTaskOutcome>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GenericRecoveryAttempt {
    attempt_id: u64,
    barrier_epoch: u64,
    safety_generation: u64,
}

struct QuiesceRecoveryReceipt {
    recovery: coding_agent_store::RecoveryOutcome,
    terminal_tasks: Vec<Task>,
    projection: EventCursor,
    scheduler_snapshot: SchedulerBootstrapSnapshot,
}

struct DegradedFinalizationReceipt {
    recovery: coding_agent_store::RecoveryOutcome,
    replayed_pending_count: usize,
    terminal_tasks: Vec<Task>,
    projection: EventCursor,
    scheduler_snapshot: SchedulerBootstrapSnapshot,
}

#[derive(Debug, thiserror::Error)]
enum SchedulerProjectionRefreshError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Dispatcher(#[from] EventDispatcherError),
    #[error(transparent)]
    Publish(#[from] SchedulerProjectionPublishError),
}

enum StorageAdmissionResult {
    Ready,
    RepositoryBlocked,
    GlobalBlocked,
    Stale,
    Unavailable,
}

// Design: this internal channel is the ownership boundary for durable
// completions. Keeping receipts inline avoids adding boxed and unboxed ownership
// paths throughout the actor state machine.
#[allow(clippy::large_enum_variant)]
enum TaskManagerCompletion {
    CancelTaskLoaded {
        task_id: TaskId,
        lookup_kind: CancelTaskLookupKind,
        result: Result<Option<Task>, StoreError>,
        response: CancelResponse,
    },
    QueuedCancelCompleted {
        task_id: TaskId,
        result: Result<crate::WriteReceipt<TransitionOutcome>, StoreWriterError>,
        response: CancelResponse,
    },
    ScanLoaded {
        scan_generation: u64,
        result: Result<SchedulerBootstrapSnapshot, StoreError>,
    },
    StorageAdmissionCompleted {
        scan_generation: u64,
        task_id: TaskId,
        operation_nonce: u64,
        result: StorageAdmissionResult,
    },
    StorageActivitySynchronized {
        submission: StorageActivitySubmission,
        result: Result<(), StorageMonitorError>,
    },
    ClaimCompleted {
        task_id: TaskId,
        operation_nonce: u64,
        completion: DurableCompletion<ClaimTaskOutcome>,
    },
    ClaimReconciled {
        task_id: TaskId,
        operation_nonce: u64,
        completion: DurableCompletion<ClaimTaskReconciliationOutcome>,
    },
    ClaimReconciliationRetry {
        task_id: TaskId,
        operation_nonce: u64,
    },
    SuppressedLaunchReturned {
        task_id: TaskId,
        operation_nonce: u64,
    },
    #[cfg(any(test, feature = "test-support"))]
    LaunchGateReady {
        task_id: TaskId,
        operation_nonce: u64,
    },
    RunnerReturned {
        task_id: TaskId,
        operation_nonce: u64,
        outcome: RunnerOutcome,
    },
    #[cfg(feature = "test-support")]
    TerminalWriteReady {
        task_id: TaskId,
        operation_nonce: u64,
        attempt_id: u64,
    },
    ProcessCleanupRetry {
        task_id: TaskId,
        operation_nonce: u64,
    },
    StopIntentPersisted {
        identity: DurableOperationIdentity,
        completion: DurableCompletion<StopIntentBatchReceipt>,
    },
    CriticalStopDeadlineElapsed {
        entries: Vec<(TaskMutationIdentity, StopIntentRequest)>,
        deadline: Instant,
    },
    StopAcceptedTaskLoaded {
        task_id: TaskId,
        operation_nonce: u64,
        result: Result<Option<Task>, StoreError>,
    },
    FinalStopPersisted {
        task_id: TaskId,
        operation_nonce: u64,
        identity: TaskMutationIdentity,
        completion: DurableCompletion<FinalizeStoppedTaskOutcome>,
    },
    RunnerEventPersisted {
        task_id: TaskId,
        operation_nonce: u64,
        logical_id: u64,
        identity: TaskMutationIdentity,
        completion: DurableCompletion<AppendEventOutcome>,
    },
    RunnerReviewPersisted {
        task_id: TaskId,
        operation_nonce: u64,
        lineage_id: u64,
        attempt_id: u64,
        identity: TaskMutationIdentity,
        completion: DurableCompletion<RecordReviewOutcome>,
    },
    TerminalPersisted {
        task_id: TaskId,
        operation_nonce: u64,
        attempt_id: u64,
        identity: TaskMutationIdentity,
        stage: TerminalWriteStage,
        completion: TerminalWriteCompletion,
    },
    TerminalProjected {
        completion: TerminalProjectionCompletion,
    },
    PendingReplayCompleted {
        attempt_id: u64,
        pending: PendingDurableResult,
        result: Result<DurableCompletion<PendingReplayReceipt>, StoreWriterSubmitError>,
    },
    PendingReplayRetry {
        attempt_id: u64,
    },
    GenericRecoveryCompleted {
        attempt_id: u64,
        barrier_epoch: u64,
        result: Result<DegradedRecoveryResult, DegradedCoordinatorError>,
    },
    QuiesceDeadlineElapsed {
        quiesce_id: u64,
        deadline: Instant,
    },
    QuiesceRecovered {
        quiesce_id: u64,
        result: Result<QuiesceRecoveryReceipt, StoreWriterError>,
    },
    DegradedFinalizationLoaded {
        attempt_id: u64,
        barrier_epoch: u64,
        result: Result<DegradedFinalizationReceipt, StoreError>,
        response: oneshot::Sender<Result<DegradedRecoveryResult, DegradedCoordinatorError>>,
    },
}

#[derive(Clone)]
struct StopControl {
    safety_latch: Arc<AtomicBool>,
    cancellation: CancellationToken,
}

impl StopControl {
    fn new(cancellation: CancellationToken) -> Self {
        Self {
            safety_latch: Arc::new(AtomicBool::new(false)),
            cancellation,
        }
    }

    fn is_latched(&self) -> bool {
        self.safety_latch.load(Ordering::Acquire)
    }

    fn latch_and_cancel(&self) {
        self.safety_latch.store(true, Ordering::Release);
        self.cancellation.cancel();
    }
}

struct ActiveSafetyEntry {
    operation_nonce: u64,
    repository_id: RepositoryId,
    coordination_key: RepositoryCoordinationKey,
    stop: StopControl,
}

#[derive(Debug, Clone, Copy)]
struct CriticalStopFact {
    operation_nonce: u64,
    observed_at: Instant,
}

#[derive(Default)]
struct ActiveSafetyRegistryState {
    entries: HashMap<TaskId, ActiveSafetyEntry>,
    current_critical_scopes: Vec<MonitoredStorageScope>,
    critical_observed_at: HashMap<MonitoredStorageScope, Instant>,
    pending_critical: HashMap<TaskId, CriticalStopFact>,
    safety_generation: u64,
    safety_generation_overflowed: bool,
    #[cfg(feature = "test-support")]
    critical_on_next_publish: Option<StorageCriticalNotification>,
}

impl ActiveSafetyRegistryState {
    fn launch_stop_state(
        &self,
        task_id: TaskId,
        operation_nonce: u64,
        coordination_key: RepositoryCoordinationKey,
    ) -> Option<LaunchStopState> {
        let entry = self.entries.get(&task_id)?;
        if entry.operation_nonce != operation_nonce || entry.coordination_key != coordination_key {
            return None;
        }
        Some(if entry.stop.is_latched() {
            LaunchStopState::SafetyLatched
        } else if entry.stop.cancellation.is_cancelled() {
            LaunchStopState::CancellationRequested
        } else {
            LaunchStopState::Clear
        })
    }
}

#[derive(Clone, Default)]
struct ActiveSafetyRegistry {
    state: Arc<Mutex<ActiveSafetyRegistryState>>,
}

impl ActiveSafetyRegistry {
    // Design: callers must receive the concrete invariant error used by the
    // task-manager API; boxing it here would create a one-off error contract.
    #[allow(clippy::result_large_err)]
    fn publish(&self, task_id: TaskId, entry: ActiveSafetyEntry) -> Result<bool, TaskManagerError> {
        let mut state = self.lock();
        if state.entries.len() >= 4 || state.entries.contains_key(&task_id) {
            return Err(TaskManagerError::Invariant(
                "active safety registry rejected duplicate or excess ownership",
            ));
        }
        state.entries.insert(task_id, entry);
        #[cfg(feature = "test-support")]
        if let Some(notification) = state.critical_on_next_publish.take() {
            record_critical_scopes(&mut state, notification.scopes(), Instant::now(), false);
        }
        Ok(latch_critical_entries(&mut state))
    }

    fn launch_stop_state(
        &self,
        task_id: TaskId,
        operation_nonce: u64,
        coordination_key: RepositoryCoordinationKey,
    ) -> Option<LaunchStopState> {
        self.lock()
            .launch_stop_state(task_id, operation_nonce, coordination_key)
    }

    fn latch_storage_critical(
        &self,
        notification: &StorageCriticalNotification,
        observed_at: Instant,
    ) -> bool {
        let mut state = self.lock();
        record_critical_scopes(&mut state, notification.scopes(), observed_at, false);
        latch_critical_entries(&mut state)
    }

    fn update_storage_classification(
        &self,
        notification: &SchedulerStorageNotification,
        observed_at: Instant,
    ) -> bool {
        let scopes = critical_scopes(notification);
        let mut state = self.lock();
        record_critical_scopes(&mut state, &scopes, observed_at, true);
        latch_critical_entries(&mut state)
    }

    fn take_actionable_pending_critical(
        &self,
        retained_for_terminal_release: &HashSet<TaskId>,
    ) -> (Vec<(TaskId, CriticalStopFact)>, bool) {
        let mut state = self.lock();
        let mut task_ids = state
            .pending_critical
            .keys()
            .filter(|task_id| !retained_for_terminal_release.contains(task_id))
            .copied()
            .collect::<Vec<_>>();
        task_ids.sort_unstable_by_key(|task_id| task_id.as_uuid().as_u128());
        let mut pending = Vec::with_capacity(task_ids.len().min(4));
        for task_id in task_ids.into_iter().take(4) {
            let fact = state
                .pending_critical
                .remove(&task_id)
                .expect("a selected critical fact remains registry-owned");
            pending.push((task_id, fact));
        }
        let more_actionable = state
            .pending_critical
            .keys()
            .any(|task_id| !retained_for_terminal_release.contains(task_id));
        (pending, more_actionable)
    }

    #[cfg(feature = "test-support")]
    fn arm_critical_on_next_publish(&self, notification: StorageCriticalNotification) {
        self.lock().critical_on_next_publish = Some(notification);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ActiveSafetyRegistryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn latch_critical_entries(state: &mut ActiveSafetyRegistryState) -> bool {
    let mut affected = Vec::new();
    for (task_id, entry) in &state.entries {
        let observed_at = [
            state.critical_observed_at.get(&MonitoredStorageScope::Data),
            state
                .critical_observed_at
                .get(&MonitoredStorageScope::Runtime),
            state
                .critical_observed_at
                .get(&MonitoredStorageScope::RepositoryGit(entry.repository_id)),
        ]
        .into_iter()
        .flatten()
        .copied()
        .min();
        if let Some(observed_at) = observed_at {
            entry.stop.latch_and_cancel();
            affected.push((*task_id, entry.operation_nonce, observed_at));
        }
    }
    let mut pending_changed = false;
    for (task_id, operation_nonce, observed_at) in affected {
        match state.pending_critical.entry(task_id) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(CriticalStopFact {
                    operation_nonce,
                    observed_at,
                });
                pending_changed = true;
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let fact = entry.get_mut();
                if fact.operation_nonce != operation_nonce {
                    *fact = CriticalStopFact {
                        operation_nonce,
                        observed_at,
                    };
                    pending_changed = true;
                } else if observed_at < fact.observed_at {
                    fact.observed_at = observed_at;
                    pending_changed = true;
                }
            }
        }
    }
    if pending_changed {
        if let Some(next_generation) = state.safety_generation.checked_add(1) {
            state.safety_generation = next_generation;
        } else {
            state.safety_generation_overflowed = true;
        }
    }
    !state.pending_critical.is_empty()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoverySafetyGate {
    Exact(u64),
    CriticalPending,
    Conflict,
}

fn recovery_safety_gate(
    state: &ActiveSafetyRegistryState,
    active: &HashMap<TaskId, ActiveRunner>,
) -> RecoverySafetyGate {
    if state.safety_generation_overflowed || state.entries.len() != active.len() {
        return RecoverySafetyGate::Conflict;
    }
    let mut critical_pending = !state.pending_critical.is_empty();
    for (task_id, active) in active {
        match state.launch_stop_state(
            *task_id,
            active.operation_nonce,
            active.permit.coordination_key(),
        ) {
            Some(LaunchStopState::Clear | LaunchStopState::CancellationRequested) => {}
            Some(LaunchStopState::SafetyLatched) => critical_pending = true,
            None => return RecoverySafetyGate::Conflict,
        }
    }
    if critical_pending {
        RecoverySafetyGate::CriticalPending
    } else {
        RecoverySafetyGate::Exact(state.safety_generation)
    }
}

fn record_critical_scopes(
    state: &mut ActiveSafetyRegistryState,
    scopes: &[MonitoredStorageScope],
    observed_at: Instant,
    replace: bool,
) {
    if replace {
        state
            .critical_observed_at
            .retain(|scope, _| scopes.contains(scope));
        state.current_critical_scopes = scopes.to_vec();
    } else {
        state.current_critical_scopes.extend_from_slice(scopes);
        state.current_critical_scopes =
            StorageCriticalNotification::new(std::mem::take(&mut state.current_critical_scopes))
                .scopes()
                .to_vec();
    }
    for scope in scopes {
        state
            .critical_observed_at
            .entry(*scope)
            .and_modify(|current| {
                if observed_at < *current {
                    *current = observed_at;
                }
            })
            .or_insert(observed_at);
    }
}

fn critical_scopes(notification: &SchedulerStorageNotification) -> Vec<MonitoredStorageScope> {
    let mut scopes = Vec::new();
    if notification.data_state() == StorageState::Critical {
        scopes.push(MonitoredStorageScope::Data);
    }
    if notification.runtime_state() == StorageState::Critical {
        scopes.push(MonitoredStorageScope::Runtime);
    }
    scopes.extend(
        notification
            .repositories()
            .iter()
            .filter(|repository| repository.state() == StorageState::Critical)
            .map(|repository| MonitoredStorageScope::RepositoryGit(repository.repository_id())),
    );
    StorageCriticalNotification::new(scopes).scopes().to_vec()
}

#[derive(Clone, Default)]
struct CriticalWakeSignal {
    sender: Arc<Mutex<Option<mpsc::Sender<()>>>>,
}

impl CriticalWakeSignal {
    fn bind(&self, sender: mpsc::Sender<()>) {
        *self
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(sender);
    }

    fn notify(&self) {
        let sender = self
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(sender) = sender {
            let _ = sender.try_send(());
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct TaskManagerStorageSignals {
    safety_registry: ActiveSafetyRegistry,
    mailbox: Arc<Mutex<Option<mpsc::WeakSender<TaskManagerMessage>>>>,
    critical_wake: CriticalWakeSignal,
    launch_barrier: Arc<Mutex<()>>,
    scheduler_storage: Arc<Mutex<LatestSchedulerStorage>>,
}

#[derive(Default)]
struct LatestSchedulerStorage {
    generation: u64,
    notification: Option<SchedulerStorageNotification>,
}

impl TaskManagerStorageSignals {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn bind(
        &self,
        sender: mpsc::WeakSender<TaskManagerMessage>,
        critical_sender: mpsc::Sender<()>,
    ) {
        *self
            .mailbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(sender);
        self.critical_wake.bind(critical_sender);
        if self.latest_scheduler_storage().0 > 0 {
            self.try_send(TaskManagerMessage::StorageChanged);
        }
    }

    fn safety_registry(&self) -> ActiveSafetyRegistry {
        self.safety_registry.clone()
    }

    fn launch_barrier(&self) -> Arc<Mutex<()>> {
        Arc::clone(&self.launch_barrier)
    }

    fn latest_scheduler_storage(&self) -> (u64, Option<SchedulerStorageNotification>) {
        let latest = self
            .scheduler_storage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (latest.generation, latest.notification.clone())
    }

    fn update_scheduler_storage(&self, notification: SchedulerStorageNotification) -> bool {
        let mut latest = self
            .scheduler_storage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if latest.notification.as_ref() == Some(&notification) {
            return false;
        }
        latest.notification = Some(notification);
        latest.generation = latest.generation.saturating_add(1);
        true
    }

    #[cfg(feature = "test-support")]
    fn arm_critical_on_next_publish(&self, notification: StorageCriticalNotification) {
        self.safety_registry
            .arm_critical_on_next_publish(notification);
    }

    #[cfg(all(test, feature = "test-support"))]
    fn notify_storage_critical_at_for_test(
        &self,
        notification: StorageCriticalNotification,
        observed_at: Instant,
    ) {
        let launch_guard = self
            .launch_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let wake = self
            .safety_registry
            .latch_storage_critical(&notification, observed_at);
        drop(launch_guard);
        if wake {
            self.critical_wake.notify();
        }
    }

    fn try_send(&self, message: TaskManagerMessage) {
        let sender = self
            .mailbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(sender) = sender.and_then(|sender| sender.upgrade()) {
            let _ = sender.try_send(message);
        }
    }
}

impl SchedulerStorageNotificationSink for TaskManagerStorageSignals {
    fn notify_storage_classification(&self, notification: SchedulerStorageNotification) {
        let scheduler_changed = self.update_scheduler_storage(notification.clone());
        let launch_guard = self
            .launch_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let wake = self
            .safety_registry
            .update_storage_classification(&notification, Instant::now());
        drop(launch_guard);
        if wake {
            self.critical_wake.notify();
        }
        if scheduler_changed {
            self.try_send(TaskManagerMessage::StorageChanged);
        }
    }
}

impl StorageCriticalNotificationSink for TaskManagerStorageSignals {
    fn notify_storage_critical(&self, notification: StorageCriticalNotification) {
        let launch_guard = self
            .launch_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let wake = self
            .safety_registry
            .latch_storage_critical(&notification, Instant::now());
        drop(launch_guard);
        if wake {
            self.critical_wake.notify();
        }
    }
}

/// Actor-owned evidence that the runner future for one task has returned.
///
/// The constructor is intentionally private to this module: receiving or
/// fabricating a public enum value must never be enough to prove that the
/// runner no longer has code executing against its process scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RunnerReturnedState {
    task_id: TaskId,
    operation_nonce: u64,
    process_owner_id: u64,
}

impl RunnerReturnedState {
    fn new(process_scope: &TaskProcessScopeOwnership) -> Self {
        Self {
            task_id: process_scope.task_id(),
            operation_nonce: process_scope.operation_nonce(),
            process_owner_id: process_scope.owner_id(),
        }
    }
}

/// Opaque, task-bound confirmation used by the scheduler's terminal release
/// proof. Only this module can mint it, after the actor owns
/// `RunnerReturnedState` and observes the exact RunContext task scope clean.
pub(crate) struct TaskProcessCleanupConfirmation {
    task_id: TaskId,
    operation_nonce: u64,
    process_owner_id: u64,
    consumed: AtomicBool,
}

impl TaskProcessCleanupConfirmation {
    fn try_new(
        returned: RunnerReturnedState,
        process_scope: &TaskProcessScopeOwnership,
    ) -> Result<Self, crate::scheduler::TerminalReleaseProofError> {
        if returned.task_id != process_scope.task_id()
            || returned.operation_nonce != process_scope.operation_nonce()
            || returned.process_owner_id != process_scope.owner_id()
        {
            return Err(crate::scheduler::TerminalReleaseProofError::ProcessTreeNotClean);
        }
        match process_scope.seal_and_cleanup() {
            Ok(ProcessCleanupProof::Confirmed) => Ok(Self {
                task_id: returned.task_id,
                operation_nonce: returned.operation_nonce,
                process_owner_id: returned.process_owner_id,
                consumed: AtomicBool::new(false),
            }),
            Ok(ProcessCleanupProof::Held | ProcessCleanupProof::Unknown) | Err(_) => {
                Err(crate::scheduler::TerminalReleaseProofError::ProcessTreeNotClean)
            }
        }
    }

    pub(crate) const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub(crate) const fn operation_nonce(&self) -> u64 {
        self.operation_nonce
    }

    pub(crate) const fn process_owner_id(&self) -> u64 {
        self.process_owner_id
    }

    pub(crate) fn is_available_for_terminal_release(&self) -> bool {
        !self.consumed.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn consume_for_terminal_release(
        &self,
    ) -> Result<(), crate::scheduler::TerminalReleaseProofError> {
        self.consumed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| crate::scheduler::TerminalReleaseProofError::CleanupAlreadyConsumed)
    }
}

// Design: replay ownership remains inline so the caller can move the exact
// pending durable record back into the canonical recovery state without boxing.
#[allow(clippy::large_enum_variant)]
enum QualityWriteFailure {
    RetryNextSequence,
    Replay(PendingDurableResult),
    ColdRecovery,
    Freeze,
}

struct TaskManager {
    store: Store,
    writer: StoreWriterHandle,
    dispatcher: EventDispatcherHandle,
    service_state: ServiceStateController,
    service_state_receiver: tokio::sync::watch::Receiver<crate::ServiceStateSnapshot>,
    runner: Arc<dyn TaskRunner>,
    permit_ledger: PermitLedger,
    repository_control: Arc<RepositoryControlCoordinator>,
    instance_process_scope: ProcessLivenessScope,
    storage_admission: TaskManagerStorageAdmission,
    critical_stop_persistence_budget: Duration,
    storage_activity_sync: StorageActivitySynchronizer,
    sender: mpsc::WeakSender<TaskManagerMessage>,
    receiver: mpsc::Receiver<TaskManagerMessage>,
    deferred_messages: VecDeque<TaskManagerMessage>,
    critical_receiver: mpsc::Receiver<()>,
    critical_wake: CriticalWakeSignal,
    completion_sender: mpsc::Sender<TaskManagerCompletion>,
    completion_receiver: mpsc::Receiver<TaskManagerCompletion>,
    preparation_sender: mpsc::Sender<crate::run_context::PreparationCompleted>,
    preparation_receiver: mpsc::Receiver<crate::run_context::PreparationCompleted>,
    active: HashMap<TaskId, ActiveRunner>,
    mutation_sequences: HashMap<TaskId, u64>,
    safety_registry: ActiveSafetyRegistry,
    coordinator: DegradedCoordinator,
    degraded_recoveries: broadcast::Sender<DegradedRecoveryResult>,
    pending_durable_results: Vec<PendingDurableResult>,
    pending_replay_in_flight: Option<PendingReplayAttempt>,
    resolved_pending_replays: HashMap<DurableOperationIdentity, ResolvedPendingReplay>,
    staged_stop_intent_completions: VecDeque<StagedStopIntentCompletion>,
    scan_requested: bool,
    scan_in_flight: bool,
    scan_generation: u64,
    scan_available: HashMap<TaskId, (Task, Repository, RepositoryCoordinationKey)>,
    scan_gates: SchedulerAdmissionGates,
    storage_admission_in_flight: Option<StorageAdmissionCandidate>,
    pending_quiesce: Option<PendingQuiesce>,
    detached_cancel_completions: usize,
    exact_barrier_epoch: u64,
    next_operation_nonce: u64,
    next_quiesce_id: u64,
    next_pending_replay_attempt_id: u64,
    next_typed_write_attempt_id: u64,
    next_generic_recovery_attempt_id: u64,
    next_terminal_projection_attempt_id: u64,
    scheduler_projection: SchedulerProjectionBridge,
    scheduler_public_limits: SchedulerPublicLimits,
    scheduler_storage_signals: TaskManagerStorageSignals,
    applied_scheduler_storage_generation: u64,
    scheduler_snapshot_read_gate: Arc<tokio::sync::Mutex<()>>,
    main_closed: bool,
    degraded: bool,
    generic_recovery_attempt: Option<GenericRecoveryAttempt>,
    degraded_replayed_pending_count: usize,
    degraded_replay_high_watermark: Option<EventId>,
    frozen: bool,
    shutdown: Arc<TaskManagerShutdownControl>,
    #[cfg(feature = "test-support")]
    next_launch_ordinal: u64,
    #[cfg(feature = "test-support")]
    actor_pauses: Option<Arc<ActorPauseController>>,
    #[cfg(test)]
    claim_hooks: Option<Arc<ClaimTestHooks>>,
    #[cfg(test)]
    exit_probe: Option<oneshot::Sender<()>>,
    #[cfg(test)]
    storage_activity_exit_pause: Option<StorageActivityCompletionPauseForTest>,
    #[cfg(test)]
    degraded_finalization_pause: Option<RecoveryFinalizationPauseForTest>,
    #[cfg(test)]
    quiesce_finalization_pause: Option<RecoveryFinalizationPauseForTest>,
}

impl TaskManager {
    async fn run(mut self) {
        let mut reconcile = tokio::time::interval(RECONCILE_INTERVAL);
        reconcile.set_missed_tick_behavior(MissedTickBehavior::Skip);
        reconcile.tick().await;

        loop {
            self.expire_pending_quiesce(Instant::now());
            while self.critical_receiver.try_recv().is_ok() {
                self.handle_critical_wake();
            }
            if let Some(message) = self.deferred_messages.pop_front() {
                self.handle_message(message).await;
            } else {
                tokio::select! {
                    critical = self.critical_receiver.recv() => {
                        if critical.is_some() {
                            self.handle_critical_wake();
                        }
                    }
                    preparation = self.preparation_receiver.recv() => {
                        if let Some(preparation) = preparation {
                            self.handle_preparation_completed(preparation);
                        }
                    }
                    completion = self.completion_receiver.recv() => {
                        if let Some(completion) = completion {
                            self.handle_completion(completion).await;
                        }
                    }
                    service_changed = self.service_state_receiver.changed() => {
                        if service_changed.is_ok() {
                            self.refresh_scheduler_after_service_change().await;
                        }
                    }
                    message = self.receiver.recv(), if !self.main_closed => {
                        match message {
                            Some(message) => self.handle_message(message).await,
                            None => {
                                self.main_closed = true;
                                self.scan_requested = false;
                                self.finish_scan();
                            }
                        }
                    }
                    _ = reconcile.tick(), if !self.main_closed => {
                        if self.claims_allowed() && self.periodic_reconciliation_enabled() {
                            self.scan_requested = true;
                        }
                    }
                }
            }

            self.refresh_scheduler_after_storage_change().await;
            if !self.main_closed {
                self.start_scan_if_requested();
            }
            if self.main_closed
                && self.active.is_empty()
                && self.pending_quiesce.is_none()
                && self.detached_cancel_completions == 0
                && self.deferred_messages.is_empty()
            {
                if self.storage_activity_sync.has_in_flight() {
                    #[cfg(test)]
                    if let Some(pause) = self.storage_activity_exit_pause.take() {
                        pause.notify_actor_waiting_to_exit();
                    }
                } else {
                    break;
                }
            }
        }

        #[cfg(test)]
        if let Some(exited) = self.exit_probe.take() {
            let _ = exited.send(());
        }
    }

    #[cfg(test)]
    fn periodic_reconciliation_enabled(&self) -> bool {
        // Claim-hook tests trigger reconciliation explicitly so the actor cannot
        // enter a paused claim before the test receives its notification reply.
        self.claim_hooks.is_none()
    }

    #[cfg(not(test))]
    fn periodic_reconciliation_enabled(&self) -> bool {
        true
    }

    async fn handle_message(&mut self, message: TaskManagerMessage) {
        self.expire_pending_quiesce(Instant::now());
        self.expire_critical_stop_deadlines(Instant::now());
        #[cfg(test)]
        let Some(message) = self.handle_test_message(message).await else {
            return;
        };
        match message {
            TaskManagerMessage::NotifyQueued { response, .. } => {
                let result = if self.is_frozen() {
                    Err(TaskManagerError::Frozen)
                } else if self.degraded || self.service_state.current().state != ServiceState::Ready
                {
                    Err(TaskManagerError::StoreDegraded)
                } else {
                    self.scan_requested = true;
                    Ok(())
                };
                let _ = response.send(result);
            }
            TaskManagerMessage::AdmissionChanged { response } => {
                let result = if self.is_frozen() {
                    Err(TaskManagerError::Frozen)
                } else if self.degraded || self.service_state.current().state != ServiceState::Ready
                {
                    Err(TaskManagerError::StoreDegraded)
                } else {
                    self.scan_requested = true;
                    Ok(())
                };
                let _ = response.send(result);
            }
            TaskManagerMessage::Cancel { task_id, response } => {
                self.begin_cancel(task_id, response);
            }
            TaskManagerMessage::RunnerEvent {
                task_id,
                event,
                response,
            } => {
                self.submit_runner_event(task_id, event, response);
            }
            TaskManagerMessage::RecordReview { request, response } => {
                self.submit_runner_review(request, response);
            }
            TaskManagerMessage::StorageChanged => {
                self.refresh_scheduler_after_storage_change().await;
                if self.claims_allowed() {
                    self.scan_requested = true;
                }
            }
            TaskManagerMessage::FinalizeDegraded {
                attempt_id,
                barrier_epoch,
                recovery,
                replayed_pending_count,
                high_watermark,
                response,
            } => {
                self.start_finalize_degraded(
                    attempt_id,
                    barrier_epoch,
                    recovery,
                    replayed_pending_count,
                    high_watermark,
                    response,
                );
            }
            TaskManagerMessage::FreezeDegraded { pending, response } => {
                let introduces_pending = pending
                    .iter()
                    .any(|candidate| !self.pending_durable_results.contains(candidate));
                if introduces_pending && !self.advance_exact_barrier_epoch() {
                    let _ = response.send(());
                    return;
                }
                self.pending_durable_results = pending;
                self.freeze_degraded();
                let _ = response.send(());
            }
            TaskManagerMessage::Quiesce { deadline, response } => {
                self.begin_quiesce(deadline, response);
            }
            TaskManagerMessage::FinalizeShutdownAfterProcessCleanup {
                proof,
                deadline,
                response,
            } => {
                self.begin_shutdown_finalization(proof, deadline, response);
            }
            TaskManagerMessage::ResumeLaunch {
                task_id,
                operation_nonce,
            } => {
                self.finish_claim_launch(task_id, operation_nonce);
            }
            #[cfg(feature = "test-support")]
            TaskManagerMessage::InspectPendingDurableResults { response } => {
                let _ = response.send(self.pending_durable_results.clone());
            }
            #[cfg(feature = "test-support")]
            TaskManagerMessage::InspectRecoverySafety { response } => {
                let result = if self.is_frozen() {
                    Err(TaskManagerError::Frozen)
                } else {
                    Ok(TaskManagerSafetySnapshot {
                        active_count: self.active.len(),
                        recovery_release_ready_count: self
                            .active
                            .values()
                            .filter(|active| active.recovery_release_ready)
                            .count(),
                        available_permits: usize::try_from(
                            self.permit_ledger
                                .snapshot()
                                .limits()
                                .global()
                                .get()
                                .saturating_sub(self.permit_ledger.snapshot().global_owned()),
                        )
                        .unwrap_or(0),
                        degraded_recovery_running: self.pending_replay_in_flight.is_some()
                            || self.generic_recovery_attempt.is_some(),
                        generic_recovery_attempt_id: self
                            .generic_recovery_attempt
                            .map(|attempt| attempt.attempt_id),
                        quiesce_recovery_running: self
                            .pending_quiesce
                            .as_ref()
                            .is_some_and(|pending| pending.recovery_started),
                    })
                };
                let _ = response.send(result);
            }
            #[cfg(test)]
            _ => unreachable!("test-only TaskManager message escaped its dispatcher"),
        }
    }

    fn drain_buffered_messages(&mut self) -> bool {
        let queued = self.receiver.len();
        let mut drained = false;
        for _ in 0..queued {
            match self.receiver.try_recv() {
                Ok(message) => {
                    drained = true;
                    self.deferred_messages.push_back(message);
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    self.main_closed = true;
                    self.scan_requested = false;
                    self.finish_scan();
                    break;
                }
            }
        }
        drained
    }

    fn start_scan_if_requested(&mut self) {
        if !self.scan_requested || self.scan_in_flight || !self.claims_allowed() {
            return;
        }
        let Some(scan_generation) = self.scan_generation.checked_add(1) else {
            self.freeze_degraded();
            return;
        };
        self.scan_generation = scan_generation;
        self.scan_requested = false;
        self.scan_in_flight = true;
        let store = self.store.clone();
        let scheduler_snapshot_read_gate = Arc::clone(&self.scheduler_snapshot_read_gate);
        let completion = self.completion_sender.clone();
        tokio::spawn(async move {
            let read_guard = scheduler_snapshot_read_gate.lock().await;
            let result = store.scheduler_bootstrap_snapshot().await;
            drop(read_guard);
            let _ = completion
                .send(TaskManagerCompletion::ScanLoaded {
                    scan_generation,
                    result,
                })
                .await;
        });
    }

    async fn handle_completion(&mut self, completion: TaskManagerCompletion) {
        self.expire_pending_quiesce(Instant::now());
        self.expire_critical_stop_deadlines(Instant::now());
        match completion {
            TaskManagerCompletion::CancelTaskLoaded {
                task_id,
                lookup_kind,
                result,
                response,
            } => self.handle_cancel_task_loaded(task_id, lookup_kind, result, response),
            TaskManagerCompletion::QueuedCancelCompleted {
                task_id,
                result,
                response,
            } => {
                self.handle_queued_cancel_completed(task_id, result, response)
                    .await;
            }
            TaskManagerCompletion::ScanLoaded {
                scan_generation,
                result,
            } => {
                if !self.scan_in_flight || scan_generation != self.scan_generation {
                    return;
                }
                match result {
                    Ok(snapshot) => {
                        self.handle_scan_snapshot(scan_generation, snapshot).await;
                    }
                    Err(error) => {
                        self.finish_scan();
                        tracing::warn!(error = %error, "task reconciliation scan failed");
                        self.scan_requested = true;
                    }
                }
            }
            TaskManagerCompletion::StorageAdmissionCompleted {
                scan_generation,
                task_id,
                operation_nonce,
                result,
            } => {
                self.handle_storage_admission_completed(
                    scan_generation,
                    task_id,
                    operation_nonce,
                    result,
                )
                .await;
            }
            TaskManagerCompletion::StorageActivitySynchronized { submission, result } => {
                self.handle_storage_activity_synchronized(submission, result);
            }
            TaskManagerCompletion::ClaimCompleted {
                task_id,
                operation_nonce,
                completion,
            } => {
                self.handle_claim_completion(task_id, operation_nonce, completion)
                    .await;
            }
            TaskManagerCompletion::ClaimReconciled {
                task_id,
                operation_nonce,
                completion,
            } => {
                self.handle_claim_reconciliation(task_id, operation_nonce, completion)
                    .await;
            }
            TaskManagerCompletion::ClaimReconciliationRetry {
                task_id,
                operation_nonce,
            } => self.submit_claim_reconciliation(task_id, operation_nonce),
            TaskManagerCompletion::SuppressedLaunchReturned {
                task_id,
                operation_nonce,
            } => {
                self.handle_suppressed_launch_returned(task_id, operation_nonce)
                    .await;
            }
            #[cfg(any(test, feature = "test-support"))]
            TaskManagerCompletion::LaunchGateReady {
                task_id,
                operation_nonce,
            } => self.finish_claim_launch(task_id, operation_nonce),
            TaskManagerCompletion::RunnerReturned {
                task_id,
                operation_nonce,
                outcome,
            } => {
                self.handle_runner_returned(task_id, operation_nonce, outcome)
                    .await;
            }
            #[cfg(feature = "test-support")]
            TaskManagerCompletion::TerminalWriteReady {
                task_id,
                operation_nonce,
                attempt_id,
            } => {
                self.submit_terminal_write_after_process_pause(task_id, operation_nonce, attempt_id)
            }
            TaskManagerCompletion::ProcessCleanupRetry {
                task_id,
                operation_nonce,
            } => {
                self.handle_process_cleanup_retry(task_id, operation_nonce)
                    .await;
            }
            TaskManagerCompletion::StopIntentPersisted {
                identity,
                completion,
            } => {
                if self
                    .project_and_handle_stop_intent_persisted(identity, completion)
                    .await
                    == StopCompletionDrain::Continue
                {
                    self.kick_exact_barrier_progress();
                }
            }
            TaskManagerCompletion::CriticalStopDeadlineElapsed { entries, deadline } => {
                self.handle_critical_stop_deadline_elapsed(entries, deadline);
            }
            TaskManagerCompletion::StopAcceptedTaskLoaded {
                task_id,
                operation_nonce,
                result,
            } => self.handle_stop_accepted_task_loaded(task_id, operation_nonce, result),
            TaskManagerCompletion::FinalStopPersisted {
                task_id,
                operation_nonce,
                identity,
                completion,
            } => self.handle_final_stop_persisted(task_id, operation_nonce, identity, completion),
            TaskManagerCompletion::RunnerEventPersisted {
                task_id,
                operation_nonce,
                logical_id,
                identity,
                completion,
            } => self.handle_runner_event_persisted(
                task_id,
                operation_nonce,
                logical_id,
                identity,
                completion,
            ),
            TaskManagerCompletion::RunnerReviewPersisted {
                task_id,
                operation_nonce,
                lineage_id,
                attempt_id,
                identity,
                completion,
            } => self.handle_runner_review_persisted(
                task_id,
                operation_nonce,
                lineage_id,
                attempt_id,
                identity,
                completion,
                None,
            ),
            TaskManagerCompletion::TerminalPersisted {
                task_id,
                operation_nonce,
                attempt_id,
                identity,
                stage,
                completion,
            } => {
                self.handle_terminal_persisted(
                    task_id,
                    operation_nonce,
                    attempt_id,
                    identity,
                    stage,
                    completion,
                );
            }
            TaskManagerCompletion::TerminalProjected { completion } => {
                self.handle_terminal_projected(completion).await;
            }
            TaskManagerCompletion::PendingReplayCompleted {
                attempt_id,
                pending,
                result,
            } => {
                self.handle_pending_replay_completed(attempt_id, pending, result);
            }
            TaskManagerCompletion::PendingReplayRetry { attempt_id } => {
                self.handle_pending_replay_retry(attempt_id);
            }
            TaskManagerCompletion::GenericRecoveryCompleted {
                attempt_id,
                barrier_epoch,
                result,
            } => {
                self.handle_generic_recovery_completed(attempt_id, barrier_epoch, result);
            }
            TaskManagerCompletion::QuiesceDeadlineElapsed {
                quiesce_id,
                deadline,
            } => {
                self.handle_quiesce_deadline_elapsed(quiesce_id, deadline);
            }
            TaskManagerCompletion::QuiesceRecovered { quiesce_id, result } => {
                #[cfg(test)]
                if let Some(pause) = self.quiesce_finalization_pause.take() {
                    let _ = pause.reached.send(());
                    let _ = pause.release.await;
                }
                self.handle_quiesce_recovered(quiesce_id, result);
            }
            TaskManagerCompletion::DegradedFinalizationLoaded {
                attempt_id,
                barrier_epoch,
                result,
                response,
            } => {
                #[cfg(test)]
                if let Some(pause) = self.degraded_finalization_pause.take() {
                    let _ = pause.reached.send(());
                    let _ = pause.release.await;
                }
                let result = match result {
                    Ok(receipt) => self.finalize_degraded(attempt_id, barrier_epoch, receipt),
                    Err(error) => self.handle_degraded_finalization_load_error(
                        attempt_id,
                        barrier_epoch,
                        error,
                    ),
                };
                let _ = response.send(result);
            }
        }
    }

    fn handle_preparation_completed(
        &mut self,
        preparation: crate::run_context::PreparationCompleted,
    ) {
        let Some(active) = self.active.get_mut(&preparation.task_id()) else {
            return;
        };
        if active.operation_nonce != preparation.operation_nonce()
            || active.phase != AdmissionPhase::Preparing
            || active.permit.coordination_key() != preparation.coordination_key()
        {
            return;
        }
        active.preparation_complete = true;
        active.phase = AdmissionPhase::Running;
        preparation.acknowledge();
        if self.claims_allowed() {
            self.scan_requested = true;
        }
    }

    fn freeze_degraded_while_launch_barrier_held(&mut self) {
        if self.service_state.current().state == ServiceState::Ready {
            let _ = self.service_state.set(ServiceState::StoreDegraded);
        }
        self.frozen = true;
        self.degraded = true;
        self.generic_recovery_attempt = None;
        self.scan_requested = false;
        self.shutdown.frozen.store(true, Ordering::Release);
        self.shutdown.cancellation.cancel();
        for active in self.active.values() {
            active.cancellation.cancel();
        }
    }

    fn freeze_degraded(&mut self) {
        if self.service_state.current().state == ServiceState::Ready {
            let _ = self.service_state.set(ServiceState::StoreDegraded);
        }
        self.frozen = true;
        self.degraded = true;
        self.generic_recovery_attempt = None;
        self.scan_requested = false;
        self.shutdown.freeze_and_cancel();
        for active in self.active.values() {
            active.cancellation.cancel();
        }
    }

    fn claims_allowed(&self) -> bool {
        !self.main_closed
            && !self.degraded
            && !self.is_frozen()
            && self.service_state.current().state == ServiceState::Ready
            && !self.process_cleanup_pauses_scheduler()
            && !self.repository_control_recovery_pauses_admission()
    }

    fn is_frozen(&self) -> bool {
        self.frozen || self.shutdown.is_frozen()
    }

    fn current_persistence_deadline(&self) -> Instant {
        let background = background_deadline();
        self.pending_quiesce
            .as_ref()
            .map_or(background, |pending| background.min(pending.deadline))
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimPhase {
    PermitAcquired,
    HandleRegistered,
    ActorLivenessAcquired,
    ClaimRetainedForReconciliation,
    RunningCommitted,
    TerminalDispatched,
    PendingReplayBeforeActorDelivery,
}

#[cfg(test)]
struct ClaimTestHooks {
    phase: ClaimPhase,
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
    pause_consumed: AtomicBool,
    active_count: std::sync::atomic::AtomicUsize,
    permit_ledger: std::sync::Mutex<Option<PermitLedger>>,
}

#[cfg(test)]
impl ClaimTestHooks {
    fn new(phase: ClaimPhase) -> Self {
        Self {
            phase,
            reached: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            pause_consumed: AtomicBool::new(false),
            active_count: std::sync::atomic::AtomicUsize::new(0),
            permit_ledger: std::sync::Mutex::new(None),
        }
    }

    fn install_permit_ledger(&self, permit_ledger: PermitLedger) {
        let previous = self
            .permit_ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(permit_ledger);
        assert!(previous.is_none(), "claim permit ledger installed twice");
    }

    async fn pause(&self, phase: ClaimPhase) {
        if self.phase == phase
            && self
                .pause_consumed
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.reached.notify_one();
            self.release.notified().await;
        }
    }

    async fn wait_until_reached(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.reached.notified())
            .await
            .unwrap_or_else(|_| panic!("claim pause was not reached for phase {:?}", self.phase));
    }

    fn resume(&self) {
        self.release.notify_one();
    }

    fn active_count(&self) -> usize {
        self.active_count.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn available_permits(&self) -> usize {
        let snapshot = self.permit_snapshot();
        usize::try_from(
            snapshot
                .limits()
                .global()
                .get()
                .saturating_sub(snapshot.global_owned()),
        )
        .expect("test permit count fits usize")
    }

    fn permit_snapshot(&self) -> crate::PermitLedgerSnapshot {
        self.permit_ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .expect("claim permit ledger installed")
            .snapshot()
    }
}

fn background_deadline() -> Instant {
    Instant::now() + BACKGROUND_WRITE_BUDGET
}

fn classify_quality_write_failure<T>(
    completion: &DurableCompletion<T>,
    submitted: &PendingDurableResult,
    retry_available: bool,
) -> QualityWriteFailure {
    match (completion.sequence_disposition, &completion.disposition) {
        (
            MutationSequenceDisposition::RetainSame,
            DurableDisposition::KnownNotApplied {
                reason: KnownNotAppliedReason::IngressClosed | KnownNotAppliedReason::IngressFull,
                outcome: None,
                error: None,
            },
        ) => QualityWriteFailure::Replay(submitted.clone()),
        (
            MutationSequenceDisposition::AdvanceNext,
            DurableDisposition::KnownNotApplied {
                reason:
                    KnownNotAppliedReason::BusyRolledBack | KnownNotAppliedReason::DeadlineBeforeStart,
                outcome: None,
                error: None,
            },
        ) if retry_available => QualityWriteFailure::RetryNextSequence,
        (
            MutationSequenceDisposition::AdvanceNext,
            DurableDisposition::KnownNotApplied {
                reason:
                    KnownNotAppliedReason::BusyRolledBack | KnownNotAppliedReason::DeadlineBeforeStart,
                outcome: None,
                error: None,
            },
        ) => QualityWriteFailure::ColdRecovery,
        (
            MutationSequenceDisposition::BlockUnknown,
            DurableDisposition::OutcomeUnknown {
                pending: returned, ..
            },
        ) => QualityWriteFailure::Replay(returned.clone().unwrap_or_else(|| submitted.clone())),
        _ => QualityWriteFailure::Freeze,
    }
}

fn final_review_request(
    task_id: TaskId,
    repository_id: RepositoryId,
    attempt: u32,
    outcome: &RunnerOutcome,
) -> Option<FinalizeReviewedTaskRequest> {
    let evidence = match outcome {
        RunnerOutcome::Approved(evidence) if evidence.verdict() == ReviewVerdict::Approved => {
            evidence.clone()
        }
        RunnerOutcome::Rejected(evidence)
            if evidence.verdict() == ReviewVerdict::ChangesRequested && evidence.round() == 3 =>
        {
            evidence.clone()
        }
        _ => return None,
    };
    Some(FinalizeReviewedTaskRequest {
        task_id,
        expected_repository_id: repository_id,
        expected_attempt: attempt,
        evidence,
    })
}

fn quality_evidence_mismatch_failure() -> TaskFailure {
    TaskFailure {
        code: "QUALITY_EVIDENCE_MISMATCH".to_owned(),
        message: "the final quality evidence did not match the runner outcome".to_owned(),
        retryable: false,
    }
}

fn runner_panicked_failure() -> TaskFailure {
    TaskFailure {
        code: "RUNNER_PANICKED".to_owned(),
        message: "task runner panicked".to_owned(),
        retryable: false,
    }
}

fn process_tree_cleanup_unproven_failure() -> TaskFailure {
    TaskFailure {
        code: "PROCESS_TREE_CLEANUP_FAILED".to_owned(),
        message: "the runner returned without proving process-tree cleanup".to_owned(),
        retryable: true,
    }
}

fn shutdown_failure() -> TaskFailure {
    TaskFailure {
        code: "APP_SHUTDOWN".to_owned(),
        message: "application shut down before the task finished".to_owned(),
        retryable: true,
    }
}

#[cfg(test)]
mod tests;
