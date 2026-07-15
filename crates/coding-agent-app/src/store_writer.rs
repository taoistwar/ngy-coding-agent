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
    AppendEventOutcome, CreateTaskOutcome, RecoveryOutcome, RegisterRepositoryOutcome,
    RetryTaskOutcome, Store, StoreError, TaskTransition, TransitionOutcome,
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
    RecoverIncomplete(RecoveryOutcome),
}

type StoreWriterBackendFuture<'a> = Pin<
    Box<dyn Future<Output = Result<StoreWriterOperationOutcome, StoreWriterError>> + Send + 'a>,
>;

trait StoreWriterBackend: Send + Sync + 'static {
    fn execute(&self, operation: StoreWriterOperation) -> StoreWriterBackendFuture<'_>;
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
}
