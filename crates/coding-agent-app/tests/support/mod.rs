#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::ffi::{OsStr, OsString};
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use coding_agent_app::{
    CancelOutcome, CommandRunner, DegradedRecoveryResult, EventDispatcherHandle, EventWake,
    FakeRunnerConfig, FakeTaskRunner, LaunchToken, RunContext, RunnerEvent, RunnerEventError,
    RunnerEventSink, RunnerOutcome, SecurityClock, SecurityManager, SecuritySeed, ServiceState,
    ServiceStateController, StoreWriterHandle, TaskManagerHandle, TaskRunner,
};
#[cfg(feature = "test-support")]
use coding_agent_app::{FakeScenario, ScriptedFakeRunner};
use coding_agent_domain::{
    ActivityEntry, ActivityLevel, CanonicalPath, ClientRequestId, EventCursor, EventId,
    NewRepository, NewTask, PlanSnapshot, Repository, RepositoryId, Task, TaskEventKind,
    TaskEventPayload, TaskFailure, TaskId, TaskStatus, TestSnapshot, TestStatus, UtcTimestamp,
};
use coding_agent_store::{
    AppendEventOutcome, CreateTaskOutcome, RegisterRepositoryOutcome, Store, TaskTransition,
    TransitionOutcome,
};
use sqlx::pool::PoolConnection;
use sqlx::{Sqlite, Transaction};
use tempfile::TempDir;
use tokio::sync::{Mutex as AsyncMutex, Notify, oneshot};
use tokio::time::{Duration, Instant};

#[derive(Clone)]
pub struct FakeSecurityClock {
    now: Arc<Mutex<std::time::Instant>>,
}

impl FakeSecurityClock {
    pub fn new() -> Self {
        Self {
            now: Arc::new(Mutex::new(std::time::Instant::now())),
        }
    }

    pub fn advance(&self, duration: std::time::Duration) {
        let mut now = self.now.lock().expect("lock fake security clock");
        *now += duration;
    }
}

impl SecurityClock for FakeSecurityClock {
    fn now(&self) -> std::time::Instant {
        *self.now.lock().expect("lock fake security clock")
    }
}

pub struct SecurityFixture {
    pub manager: SecurityManager,
    pub initial_launch_token: LaunchToken,
    pub clock: Arc<FakeSecurityClock>,
    pub public_origin: String,
    pub expected_host: String,
}

impl SecurityFixture {
    pub fn production() -> Self {
        let public_origin = "http://127.0.0.1:43121".to_owned();
        let expected_host = "127.0.0.1:43121".to_owned();
        let clock = Arc::new(FakeSecurityClock::new());
        let seed = SecuritySeed::generate().expect("generate security seed");
        let initial_launch_token = seed.initial_launch_token().clone();
        let manager = SecurityManager::from_seed(seed, public_origin.clone(), clock.clone())
            .expect("construct production security manager");
        Self {
            manager,
            initial_launch_token,
            clock,
            public_origin,
            expected_host,
        }
    }
}

pub struct StoreFixture {
    pub store: Store,
    pub repository: Repository,
    root: PathBuf,
    _temp_dir: TempDir,
}

pub struct WriterFixture {
    pub store: Store,
    pub repository: Repository,
    pub writer: StoreWriterHandle,
    pub wake: Arc<CountingWake>,
    root: PathBuf,
    _temp_dir: TempDir,
}

pub struct DispatcherFixture {
    pub store: Store,
    pub dispatcher: EventDispatcherHandle,
    pub startup_cursor: EventCursor,
    running_task: Task,
    _temp_dir: TempDir,
}

pub struct TaskManagerFixture {
    pub store: Store,
    pub repository: Repository,
    pub writer: StoreWriterHandle,
    pub manager: TaskManagerHandle,
    pub runner: Arc<ControlledRunner>,
    pub state: ServiceStateController,
    busy_lock: AsyncMutex<Option<BusyLock>>,
    _temp_dir: TempDir,
}

pub struct DegradedFixture {
    pub store: Store,
    pub writer: StoreWriterHandle,
    pub dispatcher: EventDispatcherHandle,
    pub manager: TaskManagerHandle,
    pub runner: Arc<ControlledRunner>,
    pub state: ServiceStateController,
    repository: Repository,
    completion_gates: AsyncMutex<HashMap<TaskId, CompletionGate>>,
    recovery_results: AsyncMutex<tokio::sync::broadcast::Receiver<DegradedRecoveryResult>>,
    busy_lock: AsyncMutex<Option<BusyLock>>,
    database_path: PathBuf,
    _temp_dir: TempDir,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandCall {
    pub program: String,
    pub args: Vec<OsString>,
    pub current_dir: PathBuf,
}

#[derive(Default)]
pub struct RecordingRunner {
    results: Mutex<VecDeque<Result<Vec<u8>, io::ErrorKind>>>,
    calls: Mutex<Vec<CommandCall>>,
}

impl RecordingRunner {
    pub fn scripted(results: impl IntoIterator<Item = Result<Vec<u8>, io::ErrorKind>>) -> Self {
        Self {
            results: Mutex::new(results.into_iter().collect()),
            calls: Mutex::new(Vec::new()),
        }
    }

    pub fn calls(&self) -> Vec<CommandCall> {
        self.calls.lock().expect("lock command calls").clone()
    }
}

#[async_trait::async_trait]
impl CommandRunner for RecordingRunner {
    async fn run(
        &self,
        program: &str,
        args: &[OsString],
        current_dir: &Path,
    ) -> io::Result<Vec<u8>> {
        self.calls
            .lock()
            .expect("lock command calls")
            .push(CommandCall {
                program: program.to_owned(),
                args: args.to_vec(),
                current_dir: current_dir.to_path_buf(),
            });
        self.results
            .lock()
            .expect("lock command results")
            .pop_front()
            .expect("scripted command result")
            .map_err(io::Error::from)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum LockState {
    Missing,
    Stale,
    Dirty,
}

pub struct RealRepositoryFixture {
    _temp: TempDir,
    pub git_root: PathBuf,
    pub workspace: PathBuf,
    pub selected: PathBuf,
    pub runtime: PathBuf,
}

impl RealRepositoryFixture {
    pub fn new(lock_state: LockState) -> Self {
        let temp = tempfile::tempdir().expect("create real discovery fixture");
        let git_root = temp.path().join("repository");
        let workspace = git_root.join("workspace");
        let selected = workspace.join("selected").join("deep");
        let runtime = temp.path().join("neutral-runtime");
        std::fs::create_dir_all(&selected).expect("create selected directory");
        std::fs::create_dir(&runtime).expect("create neutral runtime directory");
        std::fs::write(
            workspace.join("Cargo.toml"),
            b"[workspace]\nmembers = []\nresolver = \"3\"\n",
        )
        .expect("write workspace manifest");
        std::fs::write(
            git_root.join("rust-toolchain.toml"),
            b"[toolchain]\nchannel = \"coding-agent-intentionally-unavailable\"\n",
        )
        .expect("write unavailable toolchain override");
        let lock = workspace.join("Cargo.lock");
        if !matches!(lock_state, LockState::Missing) {
            std::fs::write(&lock, b"# deliberately stale lock bytes\n")
                .expect("write initial lock bytes");
        }
        fixture_command(&git_root, "git", ["init", "--quiet"]);
        fixture_command(
            &git_root,
            "git",
            ["config", "user.email", "fixture@example.invalid"],
        );
        fixture_command(&git_root, "git", ["config", "user.name", "Fixture"]);
        fixture_command(&git_root, "git", ["add", "."]);
        fixture_command(&git_root, "git", ["commit", "--quiet", "-m", "fixture"]);
        if matches!(lock_state, LockState::Dirty) {
            std::fs::write(&lock, b"dirty lock bytes that must survive\n")
                .expect("dirty tracked lock");
        }
        Self {
            _temp: temp,
            git_root,
            workspace,
            selected,
            runtime,
        }
    }

    pub fn fingerprint(&self) -> RepositoryFingerprint {
        let mut files = Vec::new();
        collect_fixture_files(&self.git_root, &self.git_root, &mut files);
        files.sort();
        let mut locks = files
            .iter()
            .filter(|path| path.file_name() == Some(OsStr::new("Cargo.lock")))
            .map(|path| {
                (
                    path.clone(),
                    std::fs::read(self.git_root.join(path)).expect("read lock bytes"),
                )
            })
            .collect::<Vec<_>>();
        locks.sort_by(|left, right| left.0.cmp(&right.0));
        let status = fixture_output(&self.git_root, "git", ["status", "--porcelain=v1"]);
        RepositoryFingerprint {
            files,
            locks,
            status,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RepositoryFingerprint {
    files: Vec<PathBuf>,
    locks: Vec<(PathBuf, Vec<u8>)>,
    status: Vec<u8>,
}

fn collect_fixture_files(root: &Path, directory: &Path, files: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(directory).expect("read fixture directory") {
        let entry = entry.expect("read fixture entry");
        let path = entry.path();
        let relative = path.strip_prefix(root).expect("relative fixture path");
        if relative.components().next() == Some(std::path::Component::Normal(OsStr::new(".git"))) {
            continue;
        }
        if entry.file_type().expect("read fixture file type").is_dir() {
            collect_fixture_files(root, &path, files);
        } else {
            files.push(relative.to_path_buf());
        }
    }
}

fn fixture_command<I, S>(current_dir: &Path, program: &str, args: I)
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let status = Command::new(program)
        .args(args)
        .current_dir(current_dir)
        .status()
        .expect("run fixture command");
    assert!(status.success(), "fixture command failed: {program}");
}

fn fixture_output<I, S>(current_dir: &Path, program: &str, args: I) -> Vec<u8>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new(program)
        .args(args)
        .current_dir(current_dir)
        .output()
        .expect("run fixture command");
    assert!(output.status.success(), "fixture command failed: {program}");
    output.stdout
}

pub struct FakeRunnerFixture {
    core: RunnerFixtureCore,
}

#[cfg(feature = "test-support")]
pub struct ScriptedFakeRunnerFixture {
    core: RunnerFixtureCore,
    pub runner: Arc<ScriptedFakeRunner>,
    reverse_poll: Option<Arc<ReversePollingRunner>>,
}

#[cfg(feature = "test-support")]
struct ReversePollingRunner {
    inner: Arc<ScriptedFakeRunner>,
    first_task_id: Mutex<Option<TaskId>>,
    later_entered: Notify,
}

struct RunnerFixtureCore {
    store: Store,
    repository: Repository,
    writer: StoreWriterHandle,
    manager: TaskManagerHandle,
    _temp_dir: TempDir,
}

struct BusyLock {
    transaction: Transaction<'static, Sqlite>,
    legacy_connections: Vec<PoolConnection<Sqlite>>,
}

static DEGRADED_TEST_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

pub async fn degraded_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    DEGRADED_TEST_LOCK.lock().await
}

pub async fn store_fixture() -> StoreFixture {
    let temp_dir = tempfile::tempdir().expect("create app fixture directory");
    let database_path = temp_dir.path().join("store.sqlite3");
    let store = Store::open(database_path)
        .await
        .expect("open fixture store");
    store.migrate().await.expect("migrate fixture store");
    let root = temp_dir.path().to_path_buf();
    let repository = match store
        .register_repository(repository_input_at(&root, "seed"))
        .await
        .expect("register seed repository")
    {
        RegisterRepositoryOutcome::Created(repository)
        | RegisterRepositoryOutcome::Existing(repository) => repository,
    };

    StoreFixture {
        store,
        repository,
        root,
        _temp_dir: temp_dir,
    }
}

pub async fn writer_fixture() -> WriterFixture {
    let fixture = store_fixture().await;
    let wake = Arc::new(CountingWake::default());
    let writer = StoreWriterHandle::spawn(fixture.store.clone(), wake.clone(), 8);
    WriterFixture {
        store: fixture.store,
        repository: fixture.repository,
        writer,
        wake,
        root: fixture.root,
        _temp_dir: fixture._temp_dir,
    }
}

pub async fn dispatcher_fixture() -> DispatcherFixture {
    let fixture = store_fixture().await;
    let queued = match fixture
        .store
        .create_task(new_task(fixture.repository.id, "dispatcher fixture"))
        .await
        .expect("create dispatcher fixture task")
    {
        CreateTaskOutcome::Created { task, .. } | CreateTaskOutcome::Existing { task } => task,
    };
    let running_task = match fixture
        .store
        .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .expect("start dispatcher fixture task")
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("dispatcher fixture task must start"),
    };
    let startup_cursor = fixture
        .store
        .latest_event_id()
        .await
        .expect("read dispatcher startup cursor");
    let dispatcher = EventDispatcherHandle::spawn(fixture.store.clone(), 1_024)
        .await
        .expect("spawn event dispatcher");

    DispatcherFixture {
        store: fixture.store,
        dispatcher,
        startup_cursor,
        running_task,
        _temp_dir: fixture._temp_dir,
    }
}

pub async fn task_manager_fixture(concurrency: usize) -> TaskManagerFixture {
    let fixture = store_fixture().await;
    let dispatcher = EventDispatcherHandle::spawn(fixture.store.clone(), 1_024)
        .await
        .expect("spawn task-manager fixture dispatcher");
    let writer = StoreWriterHandle::spawn(fixture.store.clone(), Arc::new(dispatcher.clone()), 64);
    let state = ServiceStateController::new(ServiceState::Ready);
    let runner = Arc::new(ControlledRunner::default());
    let manager = TaskManagerHandle::spawn(
        fixture.store.clone(),
        writer.clone(),
        dispatcher,
        state.clone(),
        runner.clone(),
        concurrency,
        64,
    );
    TaskManagerFixture {
        store: fixture.store,
        repository: fixture.repository,
        writer,
        manager,
        runner,
        state,
        busy_lock: AsyncMutex::new(None),
        _temp_dir: fixture._temp_dir,
    }
}

pub async fn degraded_fixture_with_concurrency(concurrency: usize) -> DegradedFixture {
    let fixture = store_fixture().await;
    let database_path = fixture.root.join("store.sqlite3");
    let dispatcher = EventDispatcherHandle::spawn(fixture.store.clone(), 1_024)
        .await
        .expect("spawn degraded fixture dispatcher");
    let writer = StoreWriterHandle::spawn(fixture.store.clone(), Arc::new(dispatcher.clone()), 64);
    let state = ServiceStateController::new(ServiceState::Ready);
    let runner = Arc::new(ControlledRunner::default());
    let manager = TaskManagerHandle::spawn(
        fixture.store.clone(),
        writer.clone(),
        dispatcher.clone(),
        state.clone(),
        runner.clone(),
        concurrency,
        64,
    );
    let recovery_results = manager.subscribe_degraded_recovery();
    DegradedFixture {
        store: fixture.store,
        writer,
        dispatcher,
        manager,
        runner,
        state,
        repository: fixture.repository,
        completion_gates: AsyncMutex::new(HashMap::new()),
        recovery_results: AsyncMutex::new(recovery_results),
        busy_lock: AsyncMutex::new(None),
        database_path,
        _temp_dir: fixture._temp_dir,
    }
}

pub async fn fake_runner_fixture() -> FakeRunnerFixture {
    fake_runner_fixture_with_config(FakeRunnerConfig::default()).await
}

pub async fn fake_runner_fixture_with_config(config: FakeRunnerConfig) -> FakeRunnerFixture {
    let runner = Arc::new(FakeTaskRunner::new(config));
    FakeRunnerFixture {
        core: runner_fixture(runner, 4).await,
    }
}

#[cfg(feature = "test-support")]
pub async fn scripted_fake_runner_fixture(
    scenarios: impl IntoIterator<Item = FakeScenario>,
    concurrency: usize,
) -> ScriptedFakeRunnerFixture {
    let runner = Arc::new(ScriptedFakeRunner::new(
        FakeRunnerConfig::default(),
        scenarios,
    ));
    ScriptedFakeRunnerFixture {
        core: runner_fixture(runner.clone(), concurrency).await,
        runner,
        reverse_poll: None,
    }
}

#[cfg(feature = "test-support")]
pub async fn reverse_polled_scripted_fake_runner_fixture(
    scenarios: impl IntoIterator<Item = FakeScenario>,
    concurrency: usize,
) -> ScriptedFakeRunnerFixture {
    let runner = Arc::new(ScriptedFakeRunner::new(
        FakeRunnerConfig::default(),
        scenarios,
    ));
    let reverse_poll = Arc::new(ReversePollingRunner {
        inner: runner.clone(),
        first_task_id: Mutex::new(None),
        later_entered: Notify::new(),
    });
    ScriptedFakeRunnerFixture {
        core: runner_fixture(reverse_poll.clone(), concurrency).await,
        runner,
        reverse_poll: Some(reverse_poll),
    }
}

#[cfg(feature = "test-support")]
#[async_trait::async_trait]
impl TaskRunner for ReversePollingRunner {
    async fn run(&self, context: RunContext, sink: RunnerEventSink) -> RunnerOutcome {
        let is_first = self
            .first_task_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some_and(|task_id| task_id == context.task.id);
        if is_first {
            self.later_entered.notified().await;
        } else {
            self.later_entered.notify_one();
        }
        self.inner.run(context, sink).await
    }
}

async fn runner_fixture(runner: Arc<dyn TaskRunner>, concurrency: usize) -> RunnerFixtureCore {
    let fixture = store_fixture().await;
    let dispatcher = EventDispatcherHandle::spawn(fixture.store.clone(), 1_024)
        .await
        .expect("spawn runner fixture dispatcher");
    let writer = StoreWriterHandle::spawn(fixture.store.clone(), Arc::new(dispatcher.clone()), 64);
    let manager = TaskManagerHandle::spawn(
        fixture.store.clone(),
        writer.clone(),
        dispatcher,
        ServiceStateController::new(ServiceState::Ready),
        runner,
        concurrency,
        64,
    );
    RunnerFixtureCore {
        store: fixture.store,
        repository: fixture.repository,
        writer,
        manager,
        _temp_dir: fixture._temp_dir,
    }
}

impl RunnerFixtureCore {
    async fn create_tasks(&self, prompts: &[&str]) -> Vec<Task> {
        let mut tasks = Vec::with_capacity(prompts.len());
        for prompt in prompts {
            let receipt = self
                .writer
                .create_task(new_task(self.repository.id, prompt), deadline())
                .await
                .expect("create runner fixture task");
            tasks.push(receipt.value.task().clone());
        }
        tasks
    }

    async fn notify_tasks(&self, tasks: &[Task]) {
        for task in tasks {
            self.manager
                .notify_queued(task.id)
                .await
                .expect("notify manager of runner fixture task");
        }
    }

    async fn enqueue(&self, prompts: &[&str]) -> Vec<Task> {
        let tasks = self.create_tasks(prompts).await;
        self.notify_tasks(&tasks).await;
        tasks
    }

    async fn load(&self, task_id: TaskId) -> Task {
        self.detail(task_id).await.task
    }

    async fn detail(&self, task_id: TaskId) -> coding_agent_store::TaskDetail {
        self.store
            .task_detail(task_id)
            .await
            .expect("load runner fixture task")
            .expect("runner fixture task exists")
    }

    async fn event_kinds(&self, task_id: TaskId) -> Vec<TaskEventKind> {
        self.events(task_id)
            .await
            .into_iter()
            .map(|event| event.payload.kind())
            .collect()
    }

    async fn events(&self, task_id: TaskId) -> Vec<coding_agent_domain::TaskEvent> {
        self.store
            .task_events_after(task_id, EventCursor::ZERO, usize::MAX)
            .await
            .expect("load runner fixture events")
            .events
    }

    async fn wait_for_status(&self, task_id: TaskId, expected: TaskStatus) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self.load(task_id).await.status == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("task {task_id} did not reach {expected:?}"));
    }

    async fn wait_for_terminal(&self, task_id: TaskId) {
        for _ in 0..20_000 {
            if matches!(
                self.load(task_id).await.status,
                TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
        let status = self.load(task_id).await.status;
        let kinds = self.event_kinds(task_id).await;
        panic!("task {task_id} did not become terminal: status={status:?}, events={kinds:?}");
    }
}

impl FakeRunnerFixture {
    pub async fn start(&self) -> Task {
        let task = self
            .core
            .enqueue(&["run the deterministic fake task"])
            .await
            .remove(0);
        self.core
            .wait_for_status(task.id, TaskStatus::Running)
            .await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self.core.detail(task.id).await.plan.is_some() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fake runner emits its first snapshot");
        task
    }

    pub async fn cancel(&self, task_id: TaskId) -> CancelOutcome {
        self.core
            .manager
            .cancel(task_id)
            .await
            .expect("cancel fake runner task")
    }

    pub async fn load(&self, task_id: TaskId) -> Task {
        self.core.load(task_id).await
    }

    pub async fn detail(&self, task_id: TaskId) -> coding_agent_store::TaskDetail {
        self.core.detail(task_id).await
    }

    pub async fn event_kinds(&self, task_id: TaskId) -> Vec<TaskEventKind> {
        self.core.event_kinds(task_id).await
    }

    pub async fn test_statuses(&self, task_id: TaskId) -> Vec<TestStatus> {
        self.core
            .events(task_id)
            .await
            .into_iter()
            .filter_map(|event| match event.payload {
                TaskEventPayload::TestUpdated { tests } => Some(tests.status),
                _ => None,
            })
            .collect()
    }

    pub async fn test_snapshots(&self, task_id: TaskId) -> Vec<TestSnapshot> {
        self.core
            .events(task_id)
            .await
            .into_iter()
            .filter_map(|event| match event.payload {
                TaskEventPayload::TestUpdated { tests } => Some(tests),
                _ => None,
            })
            .collect()
    }

    pub async fn wait_for_terminal(&self, task_id: TaskId) {
        self.core.wait_for_terminal(task_id).await;
    }
}

#[cfg(feature = "test-support")]
impl ScriptedFakeRunnerFixture {
    pub async fn enqueue(&self, prompts: &[&str]) -> Vec<Task> {
        let tasks = self.core.create_tasks(prompts).await;
        if let Some(reverse_poll) = &self.reverse_poll {
            *reverse_poll
                .first_task_id
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                tasks.first().map(|task| task.id);
        }
        self.core.notify_tasks(&tasks).await;
        tasks
    }

    pub async fn start(&self, prompt: &str) -> Task {
        let task = self.core.enqueue(&[prompt]).await.remove(0);
        self.core
            .wait_for_status(task.id, TaskStatus::Running)
            .await;
        self.wait_for_runner_start(task.id).await;
        task
    }

    pub async fn cancel(&self, task_id: TaskId) -> CancelOutcome {
        self.core
            .manager
            .cancel(task_id)
            .await
            .expect("cancel scripted fake task")
    }

    pub async fn load(&self, task_id: TaskId) -> Task {
        self.core.load(task_id).await
    }

    pub async fn event_kinds(&self, task_id: TaskId) -> Vec<TaskEventKind> {
        self.core.event_kinds(task_id).await
    }

    pub async fn wait_for_status(&self, task_id: TaskId, expected: TaskStatus) {
        self.core.wait_for_status(task_id, expected).await;
    }

    pub async fn wait_for_runner_start(&self, task_id: TaskId) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !self.runner.started_task_ids().contains(&task_id) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("scripted runner did not start task {task_id}"));
    }

    pub async fn wait_for_one_failed(&self, task_ids: &[TaskId]) -> TaskId {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                for task_id in task_ids {
                    if self.core.load(*task_id).await.status == TaskStatus::Failed {
                        return *task_id;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("one scripted task becomes failed")
    }
}

impl WriterFixture {
    pub fn repository_input(&self, name: &str) -> NewRepository {
        repository_input_at(&self.root, name)
    }
}

impl DispatcherFixture {
    pub async fn commit_events_without_wake(&self, messages: &[String]) -> Vec<EventId> {
        let mut event_ids = Vec::with_capacity(messages.len());
        for (index, message) in messages.iter().enumerate() {
            let outcome = self
                .store
                .append_running_event(
                    self.running_task.id,
                    TaskEventPayload::ActivityAppended {
                        entry: ActivityEntry {
                            id: format!("activity-{index}-{message}"),
                            level: ActivityLevel::Info,
                            message: message.clone(),
                            created_at: timestamp(),
                        },
                    },
                )
                .await
                .expect("append dispatcher fixture event");
            let event_id = match outcome {
                AppendEventOutcome::Applied { event_id } => event_id,
                AppendEventOutcome::NotRunning { .. } => {
                    panic!("dispatcher fixture task must remain running")
                }
            };
            event_ids.push(event_id);
        }
        event_ids
    }
}

impl TaskManagerFixture {
    pub async fn enqueue_tasks(&self, count: usize, notify: bool) -> Vec<Task> {
        let mut tasks = Vec::with_capacity(count);
        for index in 0..count {
            let receipt = self
                .writer
                .create_task(
                    new_task(self.repository.id, &format!("manager task {index}")),
                    deadline(),
                )
                .await
                .expect("create manager fixture task");
            let task = receipt.value.task().clone();
            if notify {
                self.manager
                    .notify_queued(task.id)
                    .await
                    .expect("notify manager of queued task");
            }
            tasks.push(task);
        }
        tasks
    }

    pub async fn load(&self, task_id: TaskId) -> Task {
        self.load_detail(task_id).await.task
    }

    pub async fn load_detail(&self, task_id: TaskId) -> coding_agent_store::TaskDetail {
        self.store
            .task_detail(task_id)
            .await
            .expect("load manager fixture task")
            .expect("manager fixture task exists")
    }

    pub async fn wait_for_status(&self, task_id: TaskId, expected: TaskStatus) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self.load(task_id).await.status == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("task {task_id} did not reach {expected:?} before the timeout"));
    }

    pub async fn wait_for_running(&self, expected: usize) {
        for _ in 0..5_000 {
            let running = self
                .store
                .bootstrap_snapshot()
                .await
                .expect("read running task count")
                .tasks
                .into_iter()
                .filter(|task| task.status == TaskStatus::Running)
                .count();
            if running == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("running task count did not reach {expected}");
    }

    pub async fn reconcile(&self) {
        self.manager
            .notify_queued(TaskId::new())
            .await
            .expect("request reconciliation scan");
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    pub async fn set_created_at(&self, task_id: TaskId, timestamp: &str) {
        sqlx::query("UPDATE tasks SET created_at = ? WHERE id = ?")
            .bind(timestamp)
            .bind(task_id.to_string())
            .execute(self.store.pool())
            .await
            .expect("override task creation time");
    }

    pub async fn fail_started_event_inserts(&self, enabled: bool) {
        let statement = if enabled {
            "CREATE TRIGGER fail_task_started BEFORE INSERT ON task_events \
             WHEN NEW.kind = 'task.started' BEGIN SELECT RAISE(ABORT, 'injected'); END"
        } else {
            "DROP TRIGGER IF EXISTS fail_task_started"
        };
        sqlx::query(statement)
            .execute(self.store.pool())
            .await
            .expect("toggle task-start failure trigger");
    }

    pub async fn fail_started_event_for(&self, task_id: Option<TaskId>) {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS test_claim_failures (task_id TEXT PRIMARY KEY NOT NULL)",
        )
        .execute(self.store.pool())
        .await
        .expect("create FIFO-head failure selector");
        sqlx::query("DELETE FROM test_claim_failures")
            .execute(self.store.pool())
            .await
            .expect("clear FIFO-head failure selector");
        sqlx::query("DROP TRIGGER IF EXISTS fail_fifo_head_started")
            .execute(self.store.pool())
            .await
            .expect("remove prior FIFO-head failure trigger");
        if let Some(task_id) = task_id {
            sqlx::query("INSERT INTO test_claim_failures (task_id) VALUES (?)")
                .bind(task_id.to_string())
                .execute(self.store.pool())
                .await
                .expect("select FIFO-head task for injected failure");
            sqlx::query(
                "CREATE TRIGGER fail_fifo_head_started BEFORE INSERT ON task_events \
                 WHEN NEW.kind = 'task.started' AND EXISTS (\
                     SELECT 1 FROM test_claim_failures WHERE task_id = NEW.task_id\
                 ) BEGIN SELECT RAISE(ABORT, 'injected'); END",
            )
            .execute(self.store.pool())
            .await
            .expect("install FIFO-head failure trigger");
        }
    }

    pub async fn force_claim_busy(&self, enabled: bool) {
        let mut held = self.busy_lock.lock().await;
        if enabled {
            assert!(held.is_none(), "busy lock is already held");
            let options = self
                .store
                .pool()
                .connect_options()
                .as_ref()
                .clone()
                .busy_timeout(Duration::ZERO);
            self.store.pool().set_connect_options(options);
            let existing = self.store.pool().size();
            let mut legacy_connections = Vec::with_capacity(existing as usize);
            for _ in 0..existing {
                legacy_connections.push(
                    self.store
                        .pool()
                        .acquire()
                        .await
                        .expect("reserve connection with old busy timeout"),
                );
            }
            let transaction = self
                .store
                .pool()
                .begin_with("BEGIN IMMEDIATE")
                .await
                .expect("hold SQLite writer lock");
            *held = Some(BusyLock {
                transaction,
                legacy_connections,
            });
        } else if let Some(lock) = held.take() {
            lock.transaction
                .rollback()
                .await
                .expect("release SQLite writer lock");
            drop(lock.legacy_connections);
        }
    }
}

impl DegradedFixture {
    pub async fn start_success_task(&self) -> TaskId {
        let gate = self.runner.push_completion_gate();
        let task = self.create_task("degraded running task").await;
        self.completion_gates.lock().await.insert(task.id, gate);
        self.manager
            .notify_queued(task.id)
            .await
            .expect("notify degraded fixture running task");
        self.wait_for_status(task.id, TaskStatus::Running).await;
        task.id
    }

    pub async fn start_event_task(&self, event: RunnerEvent) -> (TaskId, EventGate) {
        let gate = self.runner.push_event_gate(event);
        let task = self.create_task("degraded event task").await;
        self.manager
            .notify_queued(task.id)
            .await
            .expect("notify degraded fixture event task");
        self.wait_for_status(task.id, TaskStatus::Running).await;
        (task.id, gate)
    }

    pub async fn enqueue_task(&self) -> TaskId {
        let task = self.create_task("degraded queued task").await;
        self.manager
            .notify_queued(task.id)
            .await
            .expect("notify degraded fixture queued task");
        task.id
    }

    pub async fn finish_runner(&self, task_id: TaskId) {
        self.completion_gates
            .lock()
            .await
            .remove(&task_id)
            .expect("completion gate exists")
            .release
            .notify_one();
    }

    pub async fn fail_all_background_writes(&self) {
        let mut held = self.busy_lock.lock().await;
        assert!(held.is_none(), "background-write lock is already held");
        let options = self
            .store
            .pool()
            .connect_options()
            .as_ref()
            .clone()
            .busy_timeout(Duration::ZERO);
        self.store.pool().set_connect_options(options);
        let existing = self.store.pool().size();
        let mut connections = Vec::with_capacity(existing as usize);
        for _ in 0..existing {
            connections.push(
                self.store
                    .pool()
                    .acquire()
                    .await
                    .expect("reserve connection with old busy timeout"),
            );
        }
        for connection in &mut connections {
            sqlx::query("PRAGMA busy_timeout = 0")
                .execute(&mut **connection)
                .await
                .expect("set zero busy timeout on an existing fixture connection");
        }
        drop(connections);
        let transaction = self
            .store
            .pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .expect("hold degraded fixture SQLite writer lock");
        *held = Some(BusyLock {
            transaction,
            legacy_connections: Vec::new(),
        });
    }

    pub async fn restore_writes(&self) {
        let lock = self
            .busy_lock
            .lock()
            .await
            .take()
            .expect("background-write lock is held");
        lock.transaction
            .rollback()
            .await
            .expect("release degraded fixture SQLite writer lock");
        drop(lock.legacy_connections);
    }

    pub async fn install_non_transient_event_failure(&self) {
        sqlx::query(
            "CREATE TABLE degraded_failure_switch (enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)))",
        )
        .execute(self.store.pool())
        .await
        .expect("create non-transient degraded failure switch");
        sqlx::query("INSERT INTO degraded_failure_switch (enabled) VALUES (1)")
            .execute(self.store.pool())
            .await
            .expect("enable non-transient degraded failure");
        sqlx::query(
            "CREATE TRIGGER fail_degraded_events BEFORE INSERT ON task_events \
             WHEN NEW.kind IN ('task.completed', 'task.cancelled', 'task.failed', 'task.interrupted') \
             AND EXISTS (SELECT 1 FROM degraded_failure_switch WHERE enabled = 1) \
             BEGIN SELECT RAISE(ABORT, 'injected non-transient failure'); END",
        )
        .execute(self.store.pool())
        .await
        .expect("install non-transient degraded failure");
    }

    pub async fn remove_non_transient_event_failure(&self) {
        sqlx::query("UPDATE degraded_failure_switch SET enabled = 0")
            .execute(self.store.pool())
            .await
            .expect("disable non-transient degraded failure");
    }

    pub async fn wait_for_state(&self, expected: ServiceState) {
        let mut state = self.state.subscribe();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if state.borrow().state == expected {
                    return;
                }
                state
                    .changed()
                    .await
                    .expect("service-state sender remains open");
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "service state did not reach {expected:?}; current={:?}",
                self.state.current()
            )
        });
    }

    pub async fn load(&self, task_id: TaskId) -> Task {
        self.store
            .task_detail(task_id)
            .await
            .expect("load degraded fixture task")
            .expect("degraded fixture task exists")
            .task
    }

    pub async fn next_recovery(&self) -> DegradedRecoveryResult {
        self.recovery_results
            .lock()
            .await
            .recv()
            .await
            .expect("degraded recovery result is published")
    }

    pub fn database_path(&self) -> &std::path::Path {
        &self.database_path
    }

    async fn create_task(&self, prompt: &str) -> Task {
        self.writer
            .create_task(new_task(self.repository.id, prompt), deadline())
            .await
            .expect("create degraded fixture task")
            .value
            .task()
            .clone()
    }

    async fn wait_for_status(&self, task_id: TaskId, expected: TaskStatus) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self.load(task_id).await.status == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("task {task_id} did not reach {expected:?}"));
    }
}

#[derive(Default)]
pub struct ControlledRunner {
    scenarios: Mutex<VecDeque<RunnerScenario>>,
    started: Mutex<HashMap<TaskId, usize>>,
    releases: Mutex<HashMap<TaskId, Arc<Notify>>>,
}

type EventAppendResult = Result<coding_agent_domain::EventId, RunnerEventError>;
type SharedEventResultReceiver = Arc<AsyncMutex<Option<oneshot::Receiver<EventAppendResult>>>>;

enum RunnerScenario {
    Blocking(Arc<Notify>),
    Panic,
    LateEvent {
        release: Arc<Notify>,
        result: oneshot::Sender<EventAppendResult>,
    },
    Events {
        events: Vec<RunnerEvent>,
        result: oneshot::Sender<Vec<coding_agent_domain::EventId>>,
    },
    CompletionGate(Arc<Notify>),
    EventGate {
        release: Arc<Notify>,
        event: RunnerEvent,
        result: oneshot::Sender<EventAppendResult>,
    },
}

pub struct LateEventGate {
    pub release: Arc<Notify>,
    pub result: SharedEventResultReceiver,
}

pub struct CompletionGate {
    pub release: Arc<Notify>,
}

pub struct EventGate {
    pub release: Arc<Notify>,
    pub result: oneshot::Receiver<EventAppendResult>,
}

pub struct EventResults(oneshot::Receiver<Vec<coding_agent_domain::EventId>>);

impl EventResults {
    pub async fn receive(self) -> Vec<coding_agent_domain::EventId> {
        self.0.await.expect("runner sends event IDs")
    }
}

impl ControlledRunner {
    pub fn push_blocking(&self, count: usize) {
        let mut scenarios = self
            .scenarios
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        scenarios.extend((0..count).map(|_| RunnerScenario::Blocking(Arc::new(Notify::new()))));
    }

    pub fn push_panic(&self) {
        self.push(RunnerScenario::Panic);
    }

    pub fn push_late_event(&self) -> LateEventGate {
        let release = Arc::new(Notify::new());
        let (result, receiver) = oneshot::channel();
        self.push(RunnerScenario::LateEvent {
            release: release.clone(),
            result,
        });
        LateEventGate {
            release,
            result: Arc::new(AsyncMutex::new(Some(receiver))),
        }
    }

    pub fn push_events(&self, events: Vec<RunnerEvent>) -> EventResults {
        let (result, receiver) = oneshot::channel();
        self.push(RunnerScenario::Events { events, result });
        EventResults(receiver)
    }

    pub fn push_completion_gate(&self) -> CompletionGate {
        let release = Arc::new(Notify::new());
        self.push(RunnerScenario::CompletionGate(release.clone()));
        CompletionGate { release }
    }

    pub fn push_event_gate(&self, event: RunnerEvent) -> EventGate {
        let release = Arc::new(Notify::new());
        let (result, receiver) = oneshot::channel();
        self.push(RunnerScenario::EventGate {
            release: release.clone(),
            event,
            result,
        });
        EventGate {
            release,
            result: receiver,
        }
    }

    pub fn release(&self, task_id: TaskId) {
        let release = self
            .releases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&task_id)
            .cloned()
            .expect("blocking runner has started");
        release.notify_one();
    }

    pub fn started_count(&self, task_id: TaskId) -> usize {
        self.started
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&task_id)
            .copied()
            .unwrap_or(0)
    }

    fn push(&self, scenario: RunnerScenario) {
        self.scenarios
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_back(scenario);
    }
}

#[async_trait::async_trait]
impl TaskRunner for ControlledRunner {
    async fn run(&self, context: RunContext, sink: RunnerEventSink) -> RunnerOutcome {
        *self
            .started
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(context.task.id)
            .or_insert(0) += 1;
        let scenario = self
            .scenarios
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
            .expect("a runner scenario is queued for every started task");
        match scenario {
            RunnerScenario::Blocking(release) => {
                self.releases
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(context.task.id, release.clone());
                tokio::select! {
                    () = context.cancellation.cancelled() => RunnerOutcome::Cancelled,
                    () = release.notified() => RunnerOutcome::Succeeded,
                }
            }
            RunnerScenario::Panic => panic!("injected runner panic"),
            RunnerScenario::LateEvent { release, result } => {
                tokio::spawn(async move {
                    release.notified().await;
                    let _ = result.send(
                        sink.append(RunnerEvent::PlanUpdated(PlanSnapshot {
                            revision: 99,
                            items: Vec::new(),
                        }))
                        .await,
                    );
                });
                RunnerOutcome::Succeeded
            }
            RunnerScenario::Events { events, result } => {
                let mut ids = Vec::with_capacity(events.len());
                for event in events {
                    ids.push(sink.append(event).await.expect("append running event"));
                }
                let _ = result.send(ids);
                RunnerOutcome::Succeeded
            }
            RunnerScenario::CompletionGate(release) => {
                release.notified().await;
                RunnerOutcome::Succeeded
            }
            RunnerScenario::EventGate {
                release,
                event,
                result,
            } => {
                release.notified().await;
                let _ = result.send(sink.append(event).await);
                RunnerOutcome::Succeeded
            }
        }
    }
}

pub fn new_task(repository_id: RepositoryId, prompt: &str) -> NewTask {
    NewTask::try_new(ClientRequestId::new(), repository_id, prompt).expect("construct fixture task")
}

pub fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

pub fn timestamp() -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339("2026-07-15T00:00:00Z").expect("construct fixture timestamp")
}

pub fn failure(code: &str) -> TaskFailure {
    TaskFailure {
        code: code.to_owned(),
        message: format!("safe message for {code}"),
        retryable: true,
    }
}

fn repository_input_at(root: &std::path::Path, name: &str) -> NewRepository {
    NewRepository {
        selected_path: canonical(root.join(format!("{name}-selected"))),
        display_name: name.to_owned(),
        git_root: canonical(root.join(format!("{name}-git"))),
        cargo_workspace_root: canonical(root.join(format!("{name}-workspace"))),
    }
}

fn canonical(path: PathBuf) -> CanonicalPath {
    CanonicalPath::try_from_canonical(path).expect("construct canonical fixture path")
}

#[derive(Default)]
pub struct CountingWake {
    count: AtomicUsize,
}

impl CountingWake {
    pub fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

impl EventWake for CountingWake {
    fn wake(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }
}

pub struct PanickingWake;

impl EventWake for PanickingWake {
    fn wake(&self) {
        panic!("injected wake panic");
    }
}
