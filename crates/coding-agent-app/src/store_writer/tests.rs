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

#[test]
fn deterministic_store_failures_keep_their_exact_clone_safe_variant() {
    let cases = [
        (
            StoreError::DatabaseSchemaUnsupported,
            KnownNotAppliedError::DatabaseSchemaUnsupported,
        ),
        (
            StoreError::InvalidTaskStatus("corrupt-status".to_owned()),
            KnownNotAppliedError::InvalidTaskStatus("corrupt-status".to_owned()),
        ),
        (
            StoreError::IllegalTransition {
                from: TaskStatus::Queued,
                to: TaskStatus::Completed,
            },
            KnownNotAppliedError::IllegalTransition {
                from: TaskStatus::Queued,
                to: TaskStatus::Completed,
            },
        ),
        (
            StoreError::TaskAttemptOverflow,
            KnownNotAppliedError::TaskAttemptOverflow,
        ),
        (
            StoreError::WalCheckpointIncomplete {
                busy: 1,
                log_frames: 7,
                checkpointed_frames: 3,
            },
            KnownNotAppliedError::WalCheckpointIncomplete {
                busy: 1,
                log_frames: 7,
                checkpointed_frames: 3,
            },
        ),
    ];

    for (error, expected) in cases {
        assert_eq!(
            classify_store_failure(error),
            StoreFailureClassification::Known(expected)
        );
    }
    assert_eq!(
        classify_store_failure(StoreError::InvariantViolation("exact invariant")),
        StoreFailureClassification::Invariant("exact invariant")
    );
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
async fn completed_transition_is_rejected_by_backend_and_internal_command_without_side_effects() {
    let fixture = unit_fixture().await;
    let queued = fixture
        .store
        .create_task(new_task(&fixture.repository, "review finalization only"))
        .await
        .expect("create task")
        .task()
        .clone();
    let running = match fixture
        .store
        .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .expect("start task")
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("fixture transition must apply"),
    };
    let before = fixture
        .store
        .bootstrap_snapshot()
        .await
        .expect("load before snapshot");

    let backend_error = StoreWriterBackend::execute(
        &fixture.store,
        StoreWriterOperation::TransitionWithEvent {
            task_id: running.id,
            expected: TaskStatus::Running,
            transition: TaskTransition::Completed,
        },
    )
    .await
    .expect_err("backend must reject generic Completed");
    assert!(matches!(
        backend_error,
        StoreWriterError::Store(StoreError::InvariantViolation(message))
            if message == COMPLETED_TRANSITION_BYPASS
    ));

    let wake = Arc::new(CountingWake::default());
    let writer = StoreWriterHandle::spawn(fixture.store.clone(), wake.clone(), 4);
    let (response, receiver) = oneshot::channel();
    writer
        .send(WriteCommand::TransitionWithEvent {
            task_id: running.id,
            expected: TaskStatus::Running,
            transition: TaskTransition::Completed,
            deadline: deadline(),
            response,
        })
        .await
        .expect("send internal command");
    let command_error = receive::<TransitionOutcome>(receiver)
        .await
        .expect_err("internal command must reject generic Completed");
    assert!(matches!(
        command_error,
        StoreWriterError::Store(StoreError::InvariantViolation(message))
            if message == COMPLETED_TRANSITION_BYPASS
    ));

    let after = fixture
        .store
        .bootstrap_snapshot()
        .await
        .expect("load after snapshot");
    assert_eq!(after.latest_event_id, before.latest_event_id);
    assert_eq!(
        after
            .tasks
            .iter()
            .find(|task| task.id == running.id)
            .expect("running task remains")
            .status,
        TaskStatus::Running
    );
    assert_eq!(wake.count(), 0);
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
        plan: coding_agent_domain::PlanSnapshot::legacy(revision, Vec::new()),
    }
}
