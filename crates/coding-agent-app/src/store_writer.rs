use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use coding_agent_domain::{
    EventId, NewRepository, NewTask, TaskEventPayload, TaskFailure, TaskId, TaskStatus,
    UtcTimestamp,
};
use coding_agent_store::{
    AppendEventOutcome, AttemptArtifactIdentity, CreateTaskOutcome, RecoveryOutcome,
    RegisterRepositoryOutcome, ReserveAttemptArtifact, ReserveAttemptArtifactOutcome,
    RetryTaskOutcome, Store, StoreError, TaskTransition, TransitionOutcome,
    UpdateAttemptArtifactOutcome,
};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, sleep_until};

const RETRY_DELAYS: [Duration; 5] = [
    Duration::from_millis(25),
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(200),
    Duration::from_millis(400),
];

pub trait EventWake: Send + Sync + 'static {
    fn wake(&self);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteReceipt<T> {
    pub value: T,
    pub event_id: Option<EventId>,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreWriterError {
    #[error("SQLite writer remained busy through its bounded retry window")]
    Busy,
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("store writer is closed")]
    Closed,
}

#[derive(Debug, Clone)]
enum StoreWriterOperation {
    RegisterRepository(NewRepository),
    CreateTask(NewTask),
    RetryTask(TaskId),
    TransitionWithEvent {
        task_id: TaskId,
        expected: TaskStatus,
        transition: TaskTransition,
    },
    AppendRunningEvent {
        task_id: TaskId,
        payload: TaskEventPayload,
    },
    ReserveAttemptArtifact(ReserveAttemptArtifact),
    MarkAttemptArtifactReady(AttemptArtifactIdentity),
    MarkAttemptArtifactInconsistent {
        identity: AttemptArtifactIdentity,
        failure_code: String,
    },
    RecoverIncomplete {
        now: UtcTimestamp,
        failure: TaskFailure,
    },
}

#[derive(Debug)]
enum StoreWriterOperationOutcome {
    RegisterRepository(RegisterRepositoryOutcome),
    CreateTask(CreateTaskOutcome),
    RetryTask(RetryTaskOutcome),
    TransitionWithEvent(TransitionOutcome),
    AppendRunningEvent(AppendEventOutcome),
    ReserveAttemptArtifact(ReserveAttemptArtifactOutcome),
    UpdateAttemptArtifact(UpdateAttemptArtifactOutcome),
    RecoverIncomplete(RecoveryOutcome),
}

#[cfg(feature = "test-support")]
impl StoreWriterOperation {
    fn test_kind(&self) -> StoreWriterOperationKind {
        match self {
            Self::RegisterRepository(_) => StoreWriterOperationKind::RegisterRepository,
            Self::CreateTask(_) => StoreWriterOperationKind::CreateTask,
            Self::RetryTask(_) => StoreWriterOperationKind::RetryTask,
            Self::TransitionWithEvent { transition, .. } => match transition {
                TaskTransition::Running => StoreWriterOperationKind::StartTask,
                TaskTransition::Completed | TaskTransition::Failed(_) => {
                    StoreWriterOperationKind::FinishTask
                }
                TaskTransition::Cancelled => StoreWriterOperationKind::CancelTask,
                TaskTransition::Interrupted(_) => StoreWriterOperationKind::InterruptTask,
            },
            Self::AppendRunningEvent { .. } => StoreWriterOperationKind::AppendRunningEvent,
            Self::ReserveAttemptArtifact(_) => StoreWriterOperationKind::ReserveAttemptArtifact,
            Self::MarkAttemptArtifactReady(_) => StoreWriterOperationKind::MarkAttemptArtifactReady,
            Self::MarkAttemptArtifactInconsistent { .. } => {
                StoreWriterOperationKind::MarkAttemptArtifactInconsistent
            }
            Self::RecoverIncomplete { .. } => StoreWriterOperationKind::RecoverIncomplete,
        }
    }
}

#[cfg(feature = "test-support")]
impl StoreWriterOperationOutcome {
    fn has_durable_event(&self) -> bool {
        match self {
            Self::RegisterRepository(_) => false,
            Self::ReserveAttemptArtifact(_) | Self::UpdateAttemptArtifact(_) => false,
            Self::CreateTask(CreateTaskOutcome::Created { .. })
            | Self::RetryTask(RetryTaskOutcome::Created { .. })
            | Self::TransitionWithEvent(TransitionOutcome::Applied { .. })
            | Self::AppendRunningEvent(AppendEventOutcome::Applied { .. }) => true,
            Self::CreateTask(CreateTaskOutcome::Existing { .. })
            | Self::RetryTask(RetryTaskOutcome::Existing { .. })
            | Self::TransitionWithEvent(TransitionOutcome::Conflict { .. })
            | Self::AppendRunningEvent(AppendEventOutcome::NotRunning { .. }) => false,
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
    BusyBeforeExecute,
    PauseBeforeExecute,
    PauseAfterCommitBeforeWake,
    DropWakeAfterCommit,
}

#[cfg(feature = "test-support")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreWriterOperationKind {
    RegisterRepository,
    CreateTask,
    RetryTask,
    StartTask,
    FinishTask,
    CancelTask,
    InterruptTask,
    AppendRunningEvent,
    ReserveAttemptArtifact,
    MarkAttemptArtifactReady,
    MarkAttemptArtifactInconsistent,
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
                .consume(StoreWriterFaultPoint::BusyBeforeExecute, kind)
                .is_some()
            {
                return Err(StoreWriterError::Busy);
            }
            let outcome = StoreWriterBackend::execute(&self.inner, operation).await?;
            if outcome.has_durable_event() {
                self.controller
                    .pause_if_scripted(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, kind)
                    .await;
                if self
                    .controller
                    .consume(StoreWriterFaultPoint::DropWakeAfterCommit, kind)
                    .is_some()
                {
                    self.controller.mark_dropped_wake();
                }
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
                StoreWriterOperation::TransitionWithEvent {
                    task_id,
                    expected,
                    transition,
                } => self
                    .transition_with_event(task_id, expected, transition)
                    .await
                    .map(StoreWriterOperationOutcome::TransitionWithEvent),
                StoreWriterOperation::AppendRunningEvent { task_id, payload } => self
                    .append_running_event(task_id, payload)
                    .await
                    .map(StoreWriterOperationOutcome::AppendRunningEvent),
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

    fn spawn_with_backend(
        backend: Arc<dyn StoreWriterBackend>,
        wake: Arc<dyn EventWake>,
        capacity: usize,
    ) -> Self {
        assert!(
            capacity > 0,
            "store-writer channel capacity must be positive"
        );
        let (sender, receiver) = mpsc::channel(capacity);
        tokio::spawn(run_writer(receiver, backend, wake));
        Self { sender }
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

    pub async fn transition_with_event(
        &self,
        task_id: TaskId,
        expected: TaskStatus,
        transition: TaskTransition,
        deadline: Instant,
    ) -> Result<WriteReceipt<TransitionOutcome>, StoreWriterError> {
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
}

enum WriteCommand {
    RegisterRepository {
        input: NewRepository,
        deadline: Instant,
        response:
            oneshot::Sender<Result<WriteReceipt<RegisterRepositoryOutcome>, StoreWriterError>>,
    },
    CreateTask {
        input: NewTask,
        deadline: Instant,
        response: oneshot::Sender<Result<WriteReceipt<CreateTaskOutcome>, StoreWriterError>>,
    },
    RetryTask {
        task_id: TaskId,
        deadline: Instant,
        response: oneshot::Sender<Result<WriteReceipt<RetryTaskOutcome>, StoreWriterError>>,
    },
    TransitionWithEvent {
        task_id: TaskId,
        expected: TaskStatus,
        transition: TaskTransition,
        deadline: Instant,
        response: oneshot::Sender<Result<WriteReceipt<TransitionOutcome>, StoreWriterError>>,
    },
    AppendRunningEvent {
        task_id: TaskId,
        payload: TaskEventPayload,
        deadline: Instant,
        response: oneshot::Sender<Result<WriteReceipt<AppendEventOutcome>, StoreWriterError>>,
    },
    ReserveAttemptArtifact {
        input: ReserveAttemptArtifact,
        deadline: Instant,
        response:
            oneshot::Sender<Result<WriteReceipt<ReserveAttemptArtifactOutcome>, StoreWriterError>>,
    },
    MarkAttemptArtifactReady {
        identity: AttemptArtifactIdentity,
        deadline: Instant,
        response:
            oneshot::Sender<Result<WriteReceipt<UpdateAttemptArtifactOutcome>, StoreWriterError>>,
    },
    MarkAttemptArtifactInconsistent {
        identity: AttemptArtifactIdentity,
        failure_code: String,
        deadline: Instant,
        response:
            oneshot::Sender<Result<WriteReceipt<UpdateAttemptArtifactOutcome>, StoreWriterError>>,
    },
    RecoverIncomplete {
        now: UtcTimestamp,
        failure: TaskFailure,
        deadline: Instant,
        response: oneshot::Sender<Result<WriteReceipt<RecoveryOutcome>, StoreWriterError>>,
    },
}

async fn run_writer(
    mut receiver: mpsc::Receiver<WriteCommand>,
    backend: Arc<dyn StoreWriterBackend>,
    wake: Arc<dyn EventWake>,
) {
    while let Some(command) = receiver.recv().await {
        match command {
            WriteCommand::RegisterRepository {
                input,
                deadline,
                response,
            } => {
                let result = execute(
                    &*backend,
                    StoreWriterOperation::RegisterRepository(input),
                    deadline,
                )
                .await
                .and_then(expect_repository)
                .map(|value| WriteReceipt {
                    value,
                    event_id: None,
                });
                let _ = response.send(result);
            }
            WriteCommand::CreateTask {
                input,
                deadline,
                response,
            } => {
                let result = execute(&*backend, StoreWriterOperation::CreateTask(input), deadline)
                    .await
                    .and_then(expect_create)
                    .map(|value| {
                        let event_id = match &value {
                            CreateTaskOutcome::Created { event_id, .. } => Some(*event_id),
                            CreateTaskOutcome::Existing { .. } => None,
                        };
                        receipt_and_wake(value, event_id, &*wake)
                    });
                let _ = response.send(result);
            }
            WriteCommand::RetryTask {
                task_id,
                deadline,
                response,
            } => {
                let result = execute(
                    &*backend,
                    StoreWriterOperation::RetryTask(task_id),
                    deadline,
                )
                .await
                .and_then(expect_retry)
                .map(|value| {
                    let event_id = match &value {
                        RetryTaskOutcome::Created { event_id, .. } => Some(*event_id),
                        RetryTaskOutcome::Existing { .. } => None,
                    };
                    receipt_and_wake(value, event_id, &*wake)
                });
                let _ = response.send(result);
            }
            WriteCommand::TransitionWithEvent {
                task_id,
                expected,
                transition,
                deadline,
                response,
            } => {
                let operation = StoreWriterOperation::TransitionWithEvent {
                    task_id,
                    expected,
                    transition,
                };
                let result = execute(&*backend, operation, deadline)
                    .await
                    .and_then(expect_transition)
                    .map(|value| {
                        let event_id = match &value {
                            TransitionOutcome::Applied { event_id, .. } => Some(*event_id),
                            TransitionOutcome::Conflict { .. } => None,
                        };
                        receipt_and_wake(value, event_id, &*wake)
                    });
                let _ = response.send(result);
            }
            WriteCommand::AppendRunningEvent {
                task_id,
                payload,
                deadline,
                response,
            } => {
                let operation = StoreWriterOperation::AppendRunningEvent { task_id, payload };
                let result = execute(&*backend, operation, deadline)
                    .await
                    .and_then(expect_append)
                    .map(|value| {
                        let event_id = match &value {
                            AppendEventOutcome::Applied { event_id } => Some(*event_id),
                            AppendEventOutcome::NotRunning { .. } => None,
                        };
                        receipt_and_wake(value, event_id, &*wake)
                    });
                let _ = response.send(result);
            }
            WriteCommand::RecoverIncomplete {
                now,
                failure,
                deadline,
                response,
            } => {
                let operation = StoreWriterOperation::RecoverIncomplete { now, failure };
                let result = execute(&*backend, operation, deadline)
                    .await
                    .and_then(expect_recovery)
                    .map(|value| {
                        let event_id = value.last_event_id;
                        receipt_and_wake(value, event_id, &*wake)
                    });
                let _ = response.send(result);
            }
            WriteCommand::ReserveAttemptArtifact {
                input,
                deadline,
                response,
            } => {
                let result = execute(
                    &*backend,
                    StoreWriterOperation::ReserveAttemptArtifact(input),
                    deadline,
                )
                .await
                .and_then(expect_reserve_artifact)
                .map(|value| WriteReceipt {
                    value,
                    event_id: None,
                });
                let _ = response.send(result);
            }
            WriteCommand::MarkAttemptArtifactReady {
                identity,
                deadline,
                response,
            } => {
                let result = execute(
                    &*backend,
                    StoreWriterOperation::MarkAttemptArtifactReady(identity),
                    deadline,
                )
                .await
                .and_then(expect_update_artifact)
                .map(|value| WriteReceipt {
                    value,
                    event_id: None,
                });
                let _ = response.send(result);
            }
            WriteCommand::MarkAttemptArtifactInconsistent {
                identity,
                failure_code,
                deadline,
                response,
            } => {
                let result = execute(
                    &*backend,
                    StoreWriterOperation::MarkAttemptArtifactInconsistent {
                        identity,
                        failure_code,
                    },
                    deadline,
                )
                .await
                .and_then(expect_update_artifact)
                .map(|value| WriteReceipt {
                    value,
                    event_id: None,
                });
                let _ = response.send(result);
            }
        }
    }
}

async fn execute(
    backend: &dyn StoreWriterBackend,
    operation: StoreWriterOperation,
    deadline: Instant,
) -> Result<StoreWriterOperationOutcome, StoreWriterError> {
    if Instant::now() >= deadline {
        return Err(StoreWriterError::Busy);
    }

    let mut retry = 0;
    loop {
        match backend.execute(operation.clone()).await {
            Err(StoreWriterError::Busy) if retry < RETRY_DELAYS.len() => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(StoreWriterError::Busy);
                }
                let retry_at = now + RETRY_DELAYS[retry];
                retry += 1;
                if retry_at >= deadline {
                    sleep_until(deadline).await;
                    return Err(StoreWriterError::Busy);
                }
                sleep_until(retry_at).await;
                if Instant::now() >= deadline {
                    return Err(StoreWriterError::Busy);
                }
            }
            result => return result,
        }
    }
}

async fn receive<T>(
    receiver: oneshot::Receiver<Result<WriteReceipt<T>, StoreWriterError>>,
) -> Result<WriteReceipt<T>, StoreWriterError> {
    receiver.await.map_err(|_| StoreWriterError::Closed)?
}

fn receipt_and_wake<T>(
    value: T,
    event_id: Option<EventId>,
    wake: &dyn EventWake,
) -> WriteReceipt<T> {
    if event_id.is_some() && catch_unwind(AssertUnwindSafe(|| wake.wake())).is_err() {
        tracing::warn!("event wake panicked after a durable store commit");
    }
    WriteReceipt { value, event_id }
}

fn classify_store_error(error: StoreError) -> StoreWriterError {
    if let StoreError::Database(database) = &error
        && let Some(code) = database.as_database_error().and_then(|error| error.code())
        && sqlite_code_is_retryable(&code)
    {
        return StoreWriterError::Busy;
    }
    StoreWriterError::Store(error)
}

pub(crate) fn sqlite_code_is_retryable(code: &str) -> bool {
    code.parse::<i32>()
        .is_ok_and(|code| matches!(code & 0xff, 5 | 6))
}

fn unexpected_outcome() -> StoreWriterError {
    StoreWriterError::Store(StoreError::InvariantViolation(
        "store writer backend returned a mismatched outcome",
    ))
}

fn expect_repository(
    outcome: StoreWriterOperationOutcome,
) -> Result<RegisterRepositoryOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::RegisterRepository(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

fn expect_create(
    outcome: StoreWriterOperationOutcome,
) -> Result<CreateTaskOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::CreateTask(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

fn expect_retry(
    outcome: StoreWriterOperationOutcome,
) -> Result<RetryTaskOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::RetryTask(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

fn expect_transition(
    outcome: StoreWriterOperationOutcome,
) -> Result<TransitionOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::TransitionWithEvent(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

fn expect_append(
    outcome: StoreWriterOperationOutcome,
) -> Result<AppendEventOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::AppendRunningEvent(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

fn expect_recovery(
    outcome: StoreWriterOperationOutcome,
) -> Result<RecoveryOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::RecoverIncomplete(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

fn expect_reserve_artifact(
    outcome: StoreWriterOperationOutcome,
) -> Result<ReserveAttemptArtifactOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::ReserveAttemptArtifact(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

fn expect_update_artifact(
    outcome: StoreWriterOperationOutcome,
) -> Result<UpdateAttemptArtifactOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::UpdateAttemptArtifact(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use coding_agent_domain::{CanonicalPath, ClientRequestId, Repository};
    use tokio::sync::Notify;

    use super::*;

    struct UnitFixture {
        store: Store,
        repository: Repository,
        _temp_dir: tempfile::TempDir,
    }

    async fn unit_fixture() -> UnitFixture {
        let temp_dir = tempfile::tempdir().expect("create writer unit-test directory");
        let store = Store::open(temp_dir.path().join("store.sqlite3"))
            .await
            .expect("open writer unit-test store");
        store
            .migrate()
            .await
            .expect("migrate writer unit-test store");
        let repository = match store
            .register_repository(NewRepository {
                selected_path: canonical(temp_dir.path().join("selected")),
                display_name: "unit repository".to_owned(),
                git_root: canonical(temp_dir.path().join("git")),
                cargo_workspace_root: canonical(temp_dir.path().join("workspace")),
            })
            .await
            .expect("register writer unit-test repository")
        {
            RegisterRepositoryOutcome::Created(repository)
            | RegisterRepositoryOutcome::Existing(repository) => repository,
        };
        UnitFixture {
            store,
            repository,
            _temp_dir: temp_dir,
        }
    }

    fn canonical(path: PathBuf) -> CanonicalPath {
        CanonicalPath::try_from_canonical(path).expect("construct unit-test canonical path")
    }

    fn new_task(repository: &Repository, prompt: &str) -> NewTask {
        NewTask::try_new(ClientRequestId::new(), repository.id, prompt)
            .expect("construct writer unit-test task")
    }

    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(10)
    }

    #[derive(Default)]
    struct CountingWake(AtomicUsize);

    impl CountingWake {
        fn count(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    impl EventWake for CountingWake {
        fn wake(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum InjectedAttempt {
        KnownUncommittedBusy,
        TerminalRollback,
    }

    struct FaultControlledBackend {
        inner: Store,
        attempts: AtomicUsize,
        injected: Mutex<VecDeque<InjectedAttempt>>,
        pause: Option<Arc<PausePoint>>,
    }

    impl FaultControlledBackend {
        fn new(inner: Store, injected: impl IntoIterator<Item = InjectedAttempt>) -> Self {
            Self {
                inner,
                attempts: AtomicUsize::new(0),
                injected: Mutex::new(injected.into_iter().collect()),
                pause: None,
            }
        }

        fn paused(inner: Store, pause: Arc<PausePoint>) -> Self {
            Self {
                inner,
                attempts: AtomicUsize::new(0),
                injected: Mutex::new(VecDeque::new()),
                pause: Some(pause),
            }
        }

        fn attempts(&self) -> usize {
            self.attempts.load(Ordering::SeqCst)
        }
    }

    impl StoreWriterBackend for FaultControlledBackend {
        fn execute(&self, operation: StoreWriterOperation) -> StoreWriterBackendFuture<'_> {
            Box::pin(async move {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                if let Some(pause) = &self.pause {
                    pause.started.notify_one();
                    pause.release.notified().await;
                }
                let injected = self
                    .injected
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .pop_front();
                match injected {
                    Some(InjectedAttempt::KnownUncommittedBusy) => Err(StoreWriterError::Busy),
                    Some(InjectedAttempt::TerminalRollback) => Err(StoreWriterError::Store(
                        StoreError::InvariantViolation("injected rolled-back attempt"),
                    )),
                    None => StoreWriterBackend::execute(&self.inner, operation).await,
                }
            })
        }
    }

    #[derive(Default)]
    struct PausePoint {
        started: Notify,
        release: Notify,
    }

    #[test]
    fn sqlite_retry_classification_is_limited_to_busy_and_locked_families() {
        for code in ["5", "6", "261", "517", "262"] {
            assert!(sqlite_code_is_retryable(code), "retry SQLite code {code}");
        }
        for code in ["4", "7", "260", "516", "787", "not-a-code"] {
            assert!(
                !sqlite_code_is_retryable(code),
                "do not retry SQLite code {code}"
            );
        }
    }

    #[tokio::test]
    async fn retries_two_known_uncommitted_busy_attempts_then_commits_once() {
        let fixture = unit_fixture().await;
        let backend = Arc::new(FaultControlledBackend::new(
            fixture.store.clone(),
            [
                InjectedAttempt::KnownUncommittedBusy,
                InjectedAttempt::KnownUncommittedBusy,
            ],
        ));
        let wake = Arc::new(CountingWake::default());
        let writer = StoreWriterHandle::spawn_with_backend(backend.clone(), wake.clone(), 4);

        let receipt = writer
            .create_task(
                new_task(&fixture.repository, "retry transient busy"),
                deadline(),
            )
            .await
            .expect("third attempt commits");

        assert!(matches!(receipt.value, CreateTaskOutcome::Created { .. }));
        assert_eq!(backend.attempts(), 3);
        assert_eq!(
            fixture
                .store
                .bootstrap_snapshot()
                .await
                .unwrap()
                .tasks
                .len(),
            1
        );
        assert_eq!(wake.count(), 1);
    }

    #[tokio::test]
    async fn retry_schedule_is_exact_and_bounded() {
        let fixture = unit_fixture().await;
        tokio::time::pause();
        let backend = Arc::new(FaultControlledBackend::new(
            fixture.store.clone(),
            [InjectedAttempt::KnownUncommittedBusy; 6],
        ));
        let writer = StoreWriterHandle::spawn_with_backend(
            backend.clone(),
            Arc::new(CountingWake::default()),
            4,
        );
        let request = tokio::spawn({
            let writer = writer.clone();
            let input = new_task(&fixture.repository, "bounded retries");
            async move { writer.create_task(input, deadline()).await }
        });

        wait_for_attempts(&backend, 1).await;
        for (index, delay_ms) in [25_u64, 50, 100, 200, 400].into_iter().enumerate() {
            tokio::time::advance(Duration::from_millis(delay_ms - 1)).await;
            tokio::task::yield_now().await;
            assert_eq!(backend.attempts(), index + 1);
            tokio::time::advance(Duration::from_millis(2)).await;
            wait_for_attempts(&backend, index + 2).await;
        }

        assert!(matches!(
            request.await.unwrap(),
            Err(StoreWriterError::Busy)
        ));
        assert_eq!(backend.attempts(), 6);
    }

    #[tokio::test]
    async fn deadline_expiring_during_backoff_prevents_the_next_attempt() {
        let fixture = unit_fixture().await;
        tokio::time::pause();
        let backend = Arc::new(FaultControlledBackend::new(
            fixture.store.clone(),
            [InjectedAttempt::KnownUncommittedBusy],
        ));
        let wake = Arc::new(CountingWake::default());
        let writer = StoreWriterHandle::spawn_with_backend(backend.clone(), wake.clone(), 4);

        let result = writer
            .create_task(
                new_task(&fixture.repository, "deadline during backoff"),
                Instant::now() + Duration::from_millis(10),
            )
            .await;
        // The retry deadline above deliberately uses paused Tokio time. Restore real time before
        // asking SQLx for a pooled connection so its acquire timeout cannot auto-advance first.
        tokio::time::resume();

        assert!(matches!(result, Err(StoreWriterError::Busy)));
        assert_eq!(backend.attempts(), 1);
        assert!(
            fixture
                .store
                .bootstrap_snapshot()
                .await
                .unwrap()
                .tasks
                .is_empty()
        );
        assert_eq!(wake.count(), 0);
    }

    #[tokio::test]
    async fn terminal_rolled_back_attempt_is_not_retried_or_woken() {
        let fixture = unit_fixture().await;
        let backend = Arc::new(FaultControlledBackend::new(
            fixture.store.clone(),
            [InjectedAttempt::TerminalRollback],
        ));
        let wake = Arc::new(CountingWake::default());
        let writer = StoreWriterHandle::spawn_with_backend(backend.clone(), wake.clone(), 4);

        let result = writer
            .create_task(new_task(&fixture.repository, "rolled back"), deadline())
            .await;

        assert!(matches!(result, Err(StoreWriterError::Store(_))));
        assert_eq!(backend.attempts(), 1);
        assert!(
            fixture
                .store
                .bootstrap_snapshot()
                .await
                .unwrap()
                .tasks
                .is_empty()
        );
        assert_eq!(wake.count(), 0);
    }

    #[tokio::test]
    async fn dropping_request_future_does_not_cancel_a_started_attempt() {
        let fixture = unit_fixture().await;
        let pause = Arc::new(PausePoint::default());
        let backend = Arc::new(FaultControlledBackend::paused(
            fixture.store.clone(),
            pause.clone(),
        ));
        let writer =
            StoreWriterHandle::spawn_with_backend(backend, Arc::new(CountingWake::default()), 4);
        let request = tokio::spawn({
            let writer = writer.clone();
            let input = new_task(&fixture.repository, "detached request");
            async move { writer.create_task(input, deadline()).await }
        });
        pause.started.notified().await;
        request.abort();
        pause.release.notify_one();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if !fixture
                    .store
                    .bootstrap_snapshot()
                    .await
                    .unwrap()
                    .tasks
                    .is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actor completes an already-started transaction");
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn scripted_failures_are_operation_scoped_and_counted_exactly() {
        let fixture = unit_fixture().await;
        let controller = Arc::new(
            StoreWriterTestController::try_new([StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailBeforeExecute,
                operation: Some(StoreWriterOperationKind::CreateTask),
                count: 2,
            }])
            .expect("valid writer fault script"),
        );
        let writer = StoreWriterHandle::spawn_with_test_controller(
            fixture.store.clone(),
            Arc::new(CountingWake::default()),
            4,
            controller.clone(),
        );

        for prompt in ["first injected failure", "second injected failure"] {
            assert!(matches!(
                writer
                    .create_task(new_task(&fixture.repository, prompt), deadline())
                    .await,
                Err(StoreWriterError::Store(StoreError::InvariantViolation(
                    "injected test-support StoreWriter failure"
                )))
            ));
        }
        writer
            .create_task(
                new_task(&fixture.repository, "fault budget exhausted"),
                deadline(),
            )
            .await
            .expect("third matching operation commits");

        assert_eq!(
            controller.hit_count(
                StoreWriterFaultPoint::FailBeforeExecute,
                StoreWriterOperationKind::CreateTask,
            ),
            2
        );
    }

    #[cfg(feature = "test-support")]
    #[test]
    fn fault_specs_have_a_closed_schema_and_positive_counts() {
        let parsed = serde_json::from_str::<StoreWriterFaultSpec>(
            r#"{
                "point": "pause_before_execute",
                "operation": "finish_task",
                "count": 3
            }"#,
        )
        .expect("deserialize a closed fault spec");
        assert_eq!(parsed.point, StoreWriterFaultPoint::PauseBeforeExecute);
        assert_eq!(parsed.operation, Some(StoreWriterOperationKind::FinishTask));
        assert_eq!(parsed.count, 3);

        assert!(
            serde_json::from_str::<StoreWriterFaultSpec>(
                r#"{
                    "point": "fail_before_execute",
                    "count": 1,
                    "prompt_contains": "magic"
                }"#,
            )
            .is_err()
        );
        assert_eq!(
            StoreWriterTestController::try_new([StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailBeforeExecute,
                operation: None,
                count: 0,
            }])
            .err(),
            Some(StoreWriterTestConfigError::ZeroCount)
        );
    }

    #[cfg(feature = "test-support")]
    #[test]
    fn transition_fault_filters_distinguish_start_finish_cancel_and_interrupt() {
        let task_id = TaskId::new();
        let transition = |expected, transition| StoreWriterOperation::TransitionWithEvent {
            task_id,
            expected,
            transition,
        };
        assert_eq!(
            transition(TaskStatus::Queued, TaskTransition::Running).test_kind(),
            StoreWriterOperationKind::StartTask
        );
        assert_eq!(
            transition(TaskStatus::Running, TaskTransition::Completed).test_kind(),
            StoreWriterOperationKind::FinishTask
        );
        assert_eq!(
            transition(
                TaskStatus::Running,
                TaskTransition::Failed(failure("FAILED"))
            )
            .test_kind(),
            StoreWriterOperationKind::FinishTask
        );
        assert_eq!(
            transition(TaskStatus::Queued, TaskTransition::Cancelled).test_kind(),
            StoreWriterOperationKind::CancelTask
        );
        assert_eq!(
            transition(
                TaskStatus::Running,
                TaskTransition::Interrupted(failure("INTERRUPTED")),
            )
            .test_kind(),
            StoreWriterOperationKind::InterruptTask
        );
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn before_execute_pause_is_releasable_and_precedes_the_commit() {
        let fixture = unit_fixture().await;
        let controller = Arc::new(
            StoreWriterTestController::try_new([StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::CreateTask),
                count: 1,
            }])
            .expect("valid writer pause script"),
        );
        let wake = Arc::new(CountingWake::default());
        let writer = StoreWriterHandle::spawn_with_test_controller(
            fixture.store.clone(),
            wake.clone(),
            4,
            controller.clone(),
        );
        let request = tokio::spawn({
            let writer = writer.clone();
            let input = new_task(&fixture.repository, "before execute");
            async move { writer.create_task(input, deadline()).await }
        });

        controller
            .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
            .await;
        assert!(
            fixture
                .store
                .bootstrap_snapshot()
                .await
                .expect("read store before release")
                .tasks
                .is_empty()
        );
        assert_eq!(wake.count(), 0);
        assert!(!request.is_finished());

        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
        request
            .await
            .expect("join paused request")
            .expect("released request succeeds");
        assert_eq!(wake.count(), 1);
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn scripted_busy_attempts_use_the_normal_bounded_retry_path() {
        let fixture = unit_fixture().await;
        let controller = Arc::new(
            StoreWriterTestController::try_new([StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::BusyBeforeExecute,
                operation: Some(StoreWriterOperationKind::CreateTask),
                count: 2,
            }])
            .expect("valid writer busy script"),
        );
        let writer = StoreWriterHandle::spawn_with_test_controller(
            fixture.store.clone(),
            Arc::new(CountingWake::default()),
            4,
            controller.clone(),
        );

        writer
            .create_task(new_task(&fixture.repository, "busy retries"), deadline())
            .await
            .expect("third attempt commits");
        assert_eq!(
            controller.hit_count(
                StoreWriterFaultPoint::BusyBeforeExecute,
                StoreWriterOperationKind::CreateTask,
            ),
            2
        );
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn commit_before_wake_pause_is_releasable_without_hiding_the_commit() {
        let fixture = unit_fixture().await;
        let controller = Arc::new(
            StoreWriterTestController::try_new([StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
                operation: Some(StoreWriterOperationKind::CreateTask),
                count: 1,
            }])
            .expect("valid writer pause script"),
        );
        let wake = Arc::new(CountingWake::default());
        let writer = StoreWriterHandle::spawn_with_test_controller(
            fixture.store.clone(),
            wake.clone(),
            4,
            controller.clone(),
        );
        let request = tokio::spawn({
            let writer = writer.clone();
            let input = new_task(&fixture.repository, "commit before wake");
            async move { writer.create_task(input, deadline()).await }
        });

        controller
            .wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1)
            .await;
        assert_eq!(
            fixture
                .store
                .bootstrap_snapshot()
                .await
                .expect("read committed task")
                .tasks
                .len(),
            1
        );
        assert_eq!(wake.count(), 0);
        assert!(!request.is_finished());

        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
            1
        );
        request
            .await
            .expect("join paused request")
            .expect("released request succeeds");
        assert_eq!(wake.count(), 1);
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn commit_before_wake_pause_skips_existing_without_consuming_its_budget() {
        let fixture = unit_fixture().await;
        let input = new_task(&fixture.repository, "existing outcome");
        fixture
            .store
            .create_task(input.clone())
            .await
            .expect("seed the existing task");
        let controller = Arc::new(
            StoreWriterTestController::try_new([StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
                operation: Some(StoreWriterOperationKind::CreateTask),
                count: 1,
            }])
            .expect("valid writer pause script"),
        );
        let writer = StoreWriterHandle::spawn_with_test_controller(
            fixture.store.clone(),
            Arc::new(CountingWake::default()),
            4,
            controller.clone(),
        );

        let existing = tokio::time::timeout(
            Duration::from_secs(1),
            writer.create_task(input, deadline()),
        )
        .await
        .expect("Existing does not pause")
        .expect("Existing succeeds");
        assert!(matches!(existing.value, CreateTaskOutcome::Existing { .. }));
        assert_eq!(
            controller.hit_count(
                StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
                StoreWriterOperationKind::CreateTask,
            ),
            0
        );

        let request = tokio::spawn({
            let writer = writer.clone();
            let input = new_task(&fixture.repository, "durable after existing");
            async move { writer.create_task(input, deadline()).await }
        });
        tokio::time::timeout(
            Duration::from_secs(1),
            controller.wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1),
        )
        .await
        .expect("the next durable create consumes the preserved pause budget");
        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
            1
        );
        request
            .await
            .expect("join durable create")
            .expect("released durable create succeeds");
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn commit_before_wake_pause_skips_conflict_without_consuming_its_budget() {
        let fixture = unit_fixture().await;
        let conflict_task = fixture
            .store
            .create_task(new_task(&fixture.repository, "conflict outcome"))
            .await
            .expect("seed conflict task")
            .task()
            .clone();
        fixture
            .store
            .transition_with_event(
                conflict_task.id,
                TaskStatus::Queued,
                TaskTransition::Cancelled,
            )
            .await
            .expect("move conflict task out of queued");
        let durable_task = fixture
            .store
            .create_task(new_task(&fixture.repository, "durable after conflict"))
            .await
            .expect("seed durable transition task")
            .task()
            .clone();
        let controller = Arc::new(
            StoreWriterTestController::try_new([StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
                operation: Some(StoreWriterOperationKind::StartTask),
                count: 1,
            }])
            .expect("valid writer pause script"),
        );
        let writer = StoreWriterHandle::spawn_with_test_controller(
            fixture.store.clone(),
            Arc::new(CountingWake::default()),
            4,
            controller.clone(),
        );

        let conflict = tokio::time::timeout(
            Duration::from_secs(1),
            writer.transition_with_event(
                conflict_task.id,
                TaskStatus::Queued,
                TaskTransition::Running,
                deadline(),
            ),
        )
        .await
        .expect("Conflict does not pause")
        .expect("Conflict is a successful outcome");
        assert!(matches!(conflict.value, TransitionOutcome::Conflict { .. }));
        assert_eq!(
            controller.hit_count(
                StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
                StoreWriterOperationKind::StartTask,
            ),
            0
        );

        let request = tokio::spawn({
            let writer = writer.clone();
            async move {
                writer
                    .transition_with_event(
                        durable_task.id,
                        TaskStatus::Queued,
                        TaskTransition::Running,
                        deadline(),
                    )
                    .await
            }
        });
        tokio::time::timeout(
            Duration::from_secs(1),
            controller.wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1),
        )
        .await
        .expect("the next durable transition consumes the preserved pause budget");
        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
            1
        );
        request
            .await
            .expect("join durable transition")
            .expect("released durable transition succeeds");
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn commit_before_wake_pause_skips_not_running_without_consuming_its_budget() {
        let fixture = unit_fixture().await;
        let task = fixture
            .store
            .create_task(new_task(&fixture.repository, "not-running outcome"))
            .await
            .expect("seed not-running task")
            .task()
            .clone();
        let controller = Arc::new(
            StoreWriterTestController::try_new([StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
                operation: Some(StoreWriterOperationKind::AppendRunningEvent),
                count: 1,
            }])
            .expect("valid writer pause script"),
        );
        let writer = StoreWriterHandle::spawn_with_test_controller(
            fixture.store.clone(),
            Arc::new(CountingWake::default()),
            4,
            controller.clone(),
        );

        let not_running = tokio::time::timeout(
            Duration::from_secs(1),
            writer.append_running_event(task.id, plan_payload(1), deadline()),
        )
        .await
        .expect("NotRunning does not pause")
        .expect("NotRunning is a successful outcome");
        assert!(matches!(
            not_running.value,
            AppendEventOutcome::NotRunning { .. }
        ));
        assert_eq!(
            controller.hit_count(
                StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
                StoreWriterOperationKind::AppendRunningEvent,
            ),
            0
        );
        fixture
            .store
            .transition_with_event(task.id, TaskStatus::Queued, TaskTransition::Running)
            .await
            .expect("move task to running");

        let request = tokio::spawn({
            let writer = writer.clone();
            async move {
                writer
                    .append_running_event(task.id, plan_payload(2), deadline())
                    .await
            }
        });
        tokio::time::timeout(
            Duration::from_secs(1),
            controller.wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1),
        )
        .await
        .expect("the next durable append consumes the preserved pause budget");
        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
            1
        );
        request
            .await
            .expect("join durable append")
            .expect("released durable append succeeds");
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn dropped_wake_budget_does_not_drop_the_next_notification() {
        let fixture = unit_fixture().await;
        let controller = Arc::new(
            StoreWriterTestController::try_new([StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::DropWakeAfterCommit,
                operation: Some(StoreWriterOperationKind::CreateTask),
                count: 1,
            }])
            .expect("valid writer drop-wake script"),
        );
        let wake = Arc::new(CountingWake::default());
        let writer = StoreWriterHandle::spawn_with_test_controller(
            fixture.store.clone(),
            wake.clone(),
            4,
            controller,
        );

        writer
            .create_task(new_task(&fixture.repository, "dropped wake"), deadline())
            .await
            .expect("first commit succeeds");
        assert_eq!(wake.count(), 0);
        writer
            .create_task(new_task(&fixture.repository, "delivered wake"), deadline())
            .await
            .expect("second commit succeeds");
        assert_eq!(wake.count(), 1);
    }

    async fn wait_for_attempts(backend: &FaultControlledBackend, expected: usize) {
        for _ in 0..100 {
            if backend.attempts() == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!(
            "store attempts did not reach {expected}; observed {}",
            backend.attempts()
        );
    }

    #[cfg(feature = "test-support")]
    fn failure(code: &str) -> TaskFailure {
        TaskFailure {
            code: code.to_owned(),
            message: "unit-test failure".to_owned(),
            retryable: true,
        }
    }

    #[cfg(feature = "test-support")]
    fn plan_payload(revision: u64) -> TaskEventPayload {
        TaskEventPayload::PlanUpdated {
            plan: coding_agent_domain::PlanSnapshot {
                revision,
                items: Vec::new(),
            },
        }
    }
}
