#![allow(dead_code)]

#[cfg(feature = "test-support")]
pub mod concurrent_e2e;
#[cfg(feature = "test-support")]
pub mod delivery;

use std::collections::{HashMap, VecDeque};
use std::ffi::{OsStr, OsString};
use std::io;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use coding_agent_app::{
    AvailableParallelismProbe, BrowserLaunchError, BrowserLauncher, BrowserOpener, CancelOutcome,
    CommandRunner, DegradedRecoveryResult, EventDispatcherHandle, EventWake, FakeRunnerConfig,
    FakeTaskRunner, FilesystemRepositoryIdentityResolver, FixedStartupRunnerFactory, LaunchToken,
    ListenerFactory, NativeMessageSink, PlatformPaths, RepositoryControlCoordinator,
    RepositoryIdentityResolver, RunContext, RunnerEvent, RunnerEventError, RunnerEventSink,
    RunnerOutcome, SchedulerConcurrencyLimits, SecurityClock, SecurityManager, SecuritySeed,
    ServiceState, ServiceStateController, StartupDependencies, StartupPaths, StoreFactory,
    StoreWriterHandle, TaskManagerHandle, TaskManagerLaunchResources, TaskRunner, WallClock,
};
#[cfg(feature = "test-support")]
use coding_agent_app::{
    FakeScenario, InstanceLock, PrimaryRuntime, PrimaryRuntimeTestHandles, ScriptedFakeRunner,
    StartupOutcome, StoreWriterFaultPoint, StoreWriterFaultSpec, StoreWriterOperationKind,
    StoreWriterTestController, launch,
};
use coding_agent_domain::{
    ActivityEntry, ActivityLevel, CanonicalPath, CheckActor, CheckEvidence, CheckEvidenceStatus,
    ClientRequestId, EventCursor, EventId, FindingSeverity, NewRepository, NewReviewEvidence,
    NewTask, PlanItem, PlanItemStatus, PlanSnapshot, Repository, RepositoryId, RequiredCheck,
    ReviewCoverageEvidence, ReviewDecisionSource, ReviewFinding, ReviewVerdict, Task,
    TaskEventKind, TaskEventPayload, TaskFailure, TaskId, TaskStatus, TestSnapshot, TestStatus,
    UtcTimestamp, WorkspaceDigest,
};
use coding_agent_runtime::{ProcessLivenessDirectory, ProcessLivenessScope};
use coding_agent_store::{
    AppendEventOutcome, CreateTaskOutcome, RegisterRepositoryOutcome, Store, TaskTransition,
    TransitionOutcome,
};
use sqlx::pool::PoolConnection;
use sqlx::{Sqlite, Transaction};
use tempfile::TempDir;
#[cfg(feature = "test-support")]
use tokio::sync::watch;
use tokio::sync::{Mutex as AsyncMutex, Notify, oneshot};
use tokio::time::{Duration, Instant};

pub fn instance_process_scope(runtime_directory: &Path) -> ProcessLivenessScope {
    let liveness_runtime = private_liveness_runtime(runtime_directory);
    let mut instance_id = [0x15; 16];
    instance_id[6] = 0x45;
    instance_id[8] = 0x95;
    ProcessLivenessDirectory::open(&liveness_runtime)
        .expect("open process-liveness test directory")
        .instance_scope(instance_id)
        .expect("create process-liveness test instance scope")
}

fn private_liveness_runtime(runtime_directory: &Path) -> PathBuf {
    let liveness_runtime = runtime_directory.join(".process-liveness-test-runtime");
    PlatformPaths::new(&liveness_runtime, &liveness_runtime)
        .prepare()
        .expect("create private process-liveness test runtime");
    harden_private_liveness_runtime(&liveness_runtime)
        .expect("harden private process-liveness test runtime");
    liveness_runtime
        .canonicalize()
        .expect("canonicalize private process-liveness test runtime")
}

#[cfg(unix)]
fn harden_private_liveness_runtime(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
}

#[cfg(windows)]
fn harden_private_liveness_runtime(path: &Path) -> io::Result<()> {
    use std::ffi::c_void;
    use std::fs::OpenOptions;
    use std::mem::size_of;
    use std::os::windows::fs::{MetadataExt as _, OpenOptionsExt as _};
    use std::os::windows::io::AsRawHandle as _;
    use std::ptr::{null, null_mut};

    use windows_sys::Win32::Foundation::{CloseHandle, ERROR_SUCCESS, HANDLE, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        BuildTrusteeWithSidW, EXPLICIT_ACCESS_W, SE_FILE_OBJECT, SET_ACCESS, SetEntriesInAclW,
        SetSecurityInfo, TRUSTEE_W,
    };
    use windows_sys::Win32::Security::{
        DACL_SECURITY_INFORMATION, GetTokenInformation, PROTECTED_DACL_SECURITY_INFORMATION,
        SUB_CONTAINERS_AND_OBJECTS_INHERIT, TOKEN_QUERY, TOKEN_USER, TokenUser,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ALL_ACCESS, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS,
        FILE_FLAG_OPEN_REPARSE_POINT, READ_CONTROL, WRITE_DAC,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    let mut options = OpenOptions::new();
    options
        .read(true)
        .access_mode(READ_CONTROL | WRITE_DAC)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT);
    let directory = options.open(path)?;
    let metadata = directory.metadata()?;
    if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "process-liveness test runtime is not a plain directory",
        ));
    }

    let mut token: HANDLE = null_mut();
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let result = (|| {
        let mut required = 0u32;
        unsafe {
            GetTokenInformation(token, TokenUser, null_mut(), 0, &mut required);
        }
        if required == 0 {
            return Err(io::Error::last_os_error());
        }
        let words = (required as usize).div_ceil(size_of::<usize>());
        let mut buffer = vec![0usize; words];
        if unsafe {
            GetTokenInformation(
                token,
                TokenUser,
                buffer.as_mut_ptr().cast::<c_void>(),
                required,
                &mut required,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let user = unsafe { &*buffer.as_ptr().cast::<TOKEN_USER>() };
        let mut trustee = TRUSTEE_W::default();
        unsafe { BuildTrusteeWithSidW(&mut trustee, user.User.Sid) };
        let access = EXPLICIT_ACCESS_W {
            grfAccessPermissions: FILE_ALL_ACCESS,
            grfAccessMode: SET_ACCESS,
            grfInheritance: SUB_CONTAINERS_AND_OBJECTS_INHERIT,
            Trustee: trustee,
        };
        let mut acl = null_mut();
        let acl_status = unsafe { SetEntriesInAclW(1, &access, null(), &mut acl) };
        if acl_status != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(acl_status as i32));
        }
        let status = unsafe {
            SetSecurityInfo(
                directory.as_raw_handle() as HANDLE,
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                acl,
                null(),
            )
        };
        unsafe {
            LocalFree(acl.cast());
        }
        if status == ERROR_SUCCESS {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(status as i32))
        }
    })();
    unsafe {
        CloseHandle(token);
    }
    result
}

pub fn task_process_scope(runtime_directory: &Path) -> ProcessLivenessScope {
    let mut task_id = [0x25; 16];
    task_id[6] = 0x45;
    task_id[8] = 0xa5;
    instance_process_scope(runtime_directory)
        .task_scope(task_id)
        .expect("create process-liveness test task scope")
}

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

#[derive(Debug, Clone, Copy, Default)]
pub struct StartupBehavior {
    pub prepare_error: Option<io::ErrorKind>,
    pub panic_on_store_open: bool,
    pub browser_fails: bool,
    pub listener_failures: usize,
}

#[derive(Default)]
pub struct StartupCalls {
    store_opens: AtomicUsize,
    listener_binds: AtomicUsize,
    parallelism_probes: AtomicUsize,
    browser_urls: Mutex<Vec<String>>,
    messages: Mutex<Vec<(String, String)>>,
}

impl StartupCalls {
    pub fn store_opens(&self) -> usize {
        self.store_opens.load(Ordering::SeqCst)
    }

    pub fn listener_binds(&self) -> usize {
        self.listener_binds.load(Ordering::SeqCst)
    }

    pub fn parallelism_probes(&self) -> usize {
        self.parallelism_probes.load(Ordering::SeqCst)
    }

    pub fn browser_urls(&self) -> Vec<String> {
        self.browser_urls
            .lock()
            .expect("lock startup browser URLs")
            .clone()
    }

    pub fn messages(&self) -> Vec<(String, String)> {
        self.messages.lock().expect("lock startup messages").clone()
    }
}

struct CountingAvailableParallelismProbe {
    calls: Arc<StartupCalls>,
}

impl AvailableParallelismProbe for CountingAvailableParallelismProbe {
    fn available_parallelism(&self) -> Option<NonZeroUsize> {
        self.calls.parallelism_probes.fetch_add(1, Ordering::SeqCst);
        NonZeroUsize::new(8)
    }
}

pub struct StartupFixture {
    pub paths: PlatformPaths,
    pub calls: Arc<StartupCalls>,
    _temp: Arc<TempDir>,
}

#[cfg(feature = "test-support")]
pub struct ShutdownFixture {
    pub primary: Box<PrimaryRuntime>,
    pub startup: StartupFixture,
    pub handles: PrimaryRuntimeTestHandles,
    pub runner: Arc<ScriptedFakeRunner>,
    pub repository: Repository,
}

impl StartupFixture {
    pub fn new() -> Self {
        let temp = Arc::new(tempfile::tempdir().expect("create startup fixture"));
        let root = temp.path().canonicalize().unwrap();
        let paths = PlatformPaths::new(root.join("data"), root.join("runtime"));
        Self {
            paths,
            calls: Arc::new(StartupCalls::default()),
            _temp: temp,
        }
    }

    pub fn prepare(&self) {
        self.paths.prepare().expect("prepare startup fixture paths");
    }

    pub fn dependencies(&self, behavior: StartupBehavior) -> StartupDependencies {
        let mut dependencies = StartupDependencies::production(None);
        dependencies.paths = Arc::new(FixedStartupPaths {
            paths: self.paths.clone(),
            prepare_error: behavior.prepare_error,
        });
        dependencies.stores = Arc::new(CountingStoreFactory {
            calls: self.calls.clone(),
            panic_on_open: behavior.panic_on_store_open,
        });
        dependencies.listeners = Arc::new(CountingListenerFactory {
            calls: self.calls.clone(),
            failures_remaining: Mutex::new(behavior.listener_failures),
        });
        dependencies.browser = Arc::new(RecordingBrowser {
            calls: self.calls.clone(),
            fails: behavior.browser_fails,
        });
        dependencies.messages = Arc::new(RecordingMessages {
            calls: self.calls.clone(),
        });
        dependencies.wall_clock = Arc::new(FixedWallClock);
        dependencies.security_clock = Arc::new(FakeSecurityClock::new());
        dependencies.available_parallelism = Arc::new(CountingAvailableParallelismProbe {
            calls: self.calls.clone(),
        });
        dependencies.runner_factory = Arc::new(FixedStartupRunnerFactory::new(
            Arc::new(FakeTaskRunner::default()),
            NonZeroU32::new(4).expect("test concurrency is nonzero"),
        ));
        dependencies
    }
}

impl Default for StartupFixture {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "test-support")]
impl ShutdownFixture {
    pub async fn start_task(&self, prompt: &str) -> Task {
        let receipt = self
            .handles
            .writer
            .create_task(new_task(self.repository.id, prompt), deadline())
            .await
            .expect("create shutdown fixture task");
        let task = receipt.value.task().clone();
        self.handles
            .task_manager
            .notify_queued(task.id)
            .await
            .expect("notify shutdown fixture task");
        self.wait_for_status(task.id, TaskStatus::Running).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while !self.runner.started_task_ids().contains(&task.id) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown fixture runner starts");
        task
    }

    pub fn hold_task_process_tree(
        &self,
        task_id: TaskId,
    ) -> coding_agent_runtime::HeldProcessLivenessTreeForTest {
        self.handles
            .hold_task_process_tree_for_test(task_id)
            .expect("hold the exact shutdown task process tree")
    }

    pub async fn wait_for_status(&self, task_id: TaskId, expected: TaskStatus) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let status = self
                    .handles
                    .store
                    .task_detail(task_id)
                    .await
                    .expect("load shutdown fixture task")
                    .expect("shutdown fixture task exists")
                    .task
                    .status;
                if status == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("task {task_id} did not reach {expected:?}"));
    }

    pub async fn event_kinds(&self, task_id: TaskId) -> Vec<TaskEventKind> {
        self.handles
            .store
            .task_events_after(task_id, EventCursor::ZERO, usize::MAX)
            .await
            .expect("load shutdown fixture task events")
            .events
            .into_iter()
            .map(|event| event.payload.kind())
            .collect()
    }

    pub async fn install_interrupted_event_failure(&self) {
        sqlx::query(
            "CREATE TRIGGER fail_shutdown_interrupted BEFORE INSERT ON task_events \
             WHEN NEW.kind = 'task.interrupted' \
             BEGIN SELECT RAISE(ABORT, 'injected shutdown persistence failure'); END",
        )
        .execute(self.handles.store.pool())
        .await
        .expect("install shutdown interruption failure trigger");
    }

    pub fn make_marker_creation_fail(&self) -> PathBuf {
        let instance_path = self.instance_shutdown_marker_path();
        std::fs::create_dir(&instance_path).expect("occupy instance marker path with a directory");
        instance_path
    }

    pub fn instance_shutdown_marker_path(&self) -> PathBuf {
        let file_name = self
            .startup
            .paths
            .unclean_shutdown
            .file_name()
            .expect("shutdown marker has a file name");
        let mut instance_name = file_name.to_os_string();
        instance_name.push(".");
        instance_name.push(self.primary.instance_id().hyphenated().to_string());
        instance_name.push(".marker");
        self.startup
            .paths
            .unclean_shutdown
            .with_file_name(instance_name)
    }

    pub async fn reopen_task(&self, task_id: TaskId) -> Task {
        let store = Store::open(&self.startup.paths.database_path)
            .await
            .expect("reopen shutdown fixture store");
        let task = store
            .task_detail(task_id)
            .await
            .expect("load reopened shutdown fixture task")
            .expect("reopened shutdown fixture task exists")
            .task;
        store.pool().close().await;
        task
    }

    pub async fn reopen_event_kinds(&self, task_id: TaskId) -> Vec<TaskEventKind> {
        let store = Store::open(&self.startup.paths.database_path)
            .await
            .expect("reopen shutdown fixture store for task events");
        let event_kinds = store
            .task_events_after(task_id, EventCursor::ZERO, usize::MAX)
            .await
            .expect("load reopened shutdown fixture task events")
            .events
            .into_iter()
            .map(|event| event.payload.kind())
            .collect();
        store.close().await;
        event_kinds
    }

    pub async fn assert_runtime_cleanup(&self) {
        assert!(
            !self.startup.paths.instance_descriptor.exists(),
            "shutdown must remove the runtime descriptor"
        );
        let reopened = InstanceLock::try_acquire(&self.startup.paths.instance_lock)
            .expect("reopen shutdown fixture instance lock");
        assert!(
            reopened.is_some(),
            "shutdown must release the permanent instance lock"
        );
        drop(reopened);
        assert!(
            tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, self.primary.port(),))
                .await
                .is_err(),
            "shutdown must close the loopback listener"
        );
    }

    pub async fn wait_for_messages(&self, expected: usize) -> Vec<(String, String)> {
        for _ in 0..20_000 {
            let messages = self.startup.calls.messages();
            if messages.len() >= expected {
                return messages;
            }
            std::thread::yield_now();
            tokio::task::yield_now().await;
        }
        panic!(
            "expected at least {expected} native messages, got {:?}",
            self.startup.calls.messages()
        );
    }
}

struct FixedStartupPaths {
    paths: PlatformPaths,
    prepare_error: Option<io::ErrorKind>,
}

impl StartupPaths for FixedStartupPaths {
    fn discover(&self) -> io::Result<PlatformPaths> {
        Ok(self.paths.clone())
    }

    fn prepare(&self, paths: &PlatformPaths) -> io::Result<()> {
        if let Some(kind) = self.prepare_error {
            return Err(io::Error::new(kind, "injected path preparation failure"));
        }
        paths.prepare()
    }
}

struct CountingStoreFactory {
    calls: Arc<StartupCalls>,
    panic_on_open: bool,
}

#[async_trait::async_trait]
impl StoreFactory for CountingStoreFactory {
    async fn open(&self, path: &Path) -> Result<Store, coding_agent_store::StoreError> {
        self.calls.store_opens.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.panic_on_open,
            "secondary startup must never construct a Store"
        );
        Store::open(path).await
    }
}

struct CountingListenerFactory {
    calls: Arc<StartupCalls>,
    failures_remaining: Mutex<usize>,
}

#[async_trait::async_trait]
impl ListenerFactory for CountingListenerFactory {
    async fn bind(&self, address: std::net::SocketAddrV4) -> io::Result<tokio::net::TcpListener> {
        self.calls.listener_binds.fetch_add(1, Ordering::SeqCst);
        let should_fail = {
            let mut remaining = self
                .failures_remaining
                .lock()
                .expect("lock listener failure script");
            if *remaining == 0 {
                false
            } else {
                *remaining -= 1;
                true
            }
        };
        if should_fail {
            Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                "injected listener bind failure",
            ))
        } else {
            tokio::net::TcpListener::bind(address).await
        }
    }
}

struct RecordingBrowser {
    calls: Arc<StartupCalls>,
    fails: bool,
}

impl BrowserOpener for RecordingBrowser {
    fn open(&self, port: u16, token: &str) -> Result<(), BrowserLaunchError> {
        let url = BrowserLauncher::url(port, token);
        self.calls
            .browser_urls
            .lock()
            .expect("lock startup browser URLs")
            .push(url.clone());
        if self.fails {
            Err(BrowserLaunchError::for_url(url))
        } else {
            Ok(())
        }
    }
}

struct RecordingMessages {
    calls: Arc<StartupCalls>,
}

impl NativeMessageSink for RecordingMessages {
    fn show_error(&self, title: &'static str, body: String) {
        self.calls
            .messages
            .lock()
            .expect("lock startup messages")
            .push((title.to_owned(), body));
    }
}

pub struct FixedWallClock;

impl WallClock for FixedWallClock {
    fn now_utc(&self) -> time::OffsetDateTime {
        time::macros::datetime!(2026-07-15 00:00 UTC)
    }
}

pub struct StoreFixture {
    pub store: Store,
    pub repository: Repository,
    root: PathBuf,
    _temp_dir: StoreFixtureRoot,
}

struct StoreFixtureRoot {
    temp_dir: Option<TempDir>,
    retain_on_unexpected_drop: bool,
}

impl StoreFixtureRoot {
    fn new(temp_dir: TempDir) -> Self {
        Self {
            temp_dir: Some(temp_dir),
            retain_on_unexpected_drop: false,
        }
    }

    fn arm_delivery_root_for_explicit_close(&mut self) {
        self.retain_on_unexpected_drop = true;
    }

    fn path(&self) -> &Path {
        self.temp_dir
            .as_ref()
            .expect("store fixture root remains owned")
            .path()
    }

    fn take_for_explicit_close(&mut self) -> TempDir {
        self.retain_on_unexpected_drop = false;
        self.temp_dir
            .take()
            .expect("store fixture root closes exactly once")
    }
}

impl Drop for StoreFixtureRoot {
    fn drop(&mut self) {
        if !self.retain_on_unexpected_drop {
            return;
        }
        let Some(temp_dir) = self.temp_dir.take() else {
            return;
        };
        let retained_root = temp_dir.keep();
        eprintln!(
            "delivery fixture retained after unexpected drop: {}",
            retained_root.display()
        );
    }
}

impl StoreFixture {
    pub fn instance_process_scope(&self) -> ProcessLivenessScope {
        let runtime_directory = self.root.join("runtime");
        PlatformPaths::new(self.root.join("data"), &runtime_directory)
            .prepare()
            .expect("create store-fixture private runtime directory");
        instance_process_scope(&runtime_directory)
    }

    pub fn arm_delivery_root_for_explicit_close(&mut self) {
        self._temp_dir.arm_delivery_root_for_explicit_close();
    }

    pub async fn close(self) -> Result<(), String> {
        let Self {
            store,
            repository,
            root: _,
            _temp_dir: mut root_guard,
        } = self;
        let mut failures = Vec::new();
        let store_close_timed_out = tokio::time::timeout(Duration::from_secs(10), store.close())
            .await
            .is_err();
        if store_close_timed_out {
            failures.push("store close timed out".to_owned());
        }
        drop(store);
        drop(repository);

        let temporary_root = root_guard.path().to_path_buf();
        if store_close_timed_out {
            drop(root_guard);
            return Err(failures.join("; "));
        }
        let temp_dir = root_guard.take_for_explicit_close();
        drop(root_guard);
        if let Err(error) = temp_dir.close() {
            failures.push(format!("temporary directory close failed: {error}"));
        }
        if temporary_root.exists() {
            failures.push(format!(
                "temporary directory leaked: {}",
                temporary_root.display()
            ));
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }
}

pub struct WriterFixture {
    pub store: Store,
    pub repository: Repository,
    pub writer: StoreWriterHandle,
    pub wake: Arc<CountingWake>,
    root: PathBuf,
    _temp_dir: StoreFixtureRoot,
}

pub struct DispatcherFixture {
    pub store: Store,
    pub dispatcher: EventDispatcherHandle,
    pub startup_cursor: EventCursor,
    running_task: Task,
    _temp_dir: StoreFixtureRoot,
}

pub struct TaskManagerFixture {
    pub store: Store,
    pub repository: Repository,
    pub writer: StoreWriterHandle,
    pub dispatcher: EventDispatcherHandle,
    pub manager: TaskManagerHandle,
    pub runner: Arc<ControlledRunner>,
    pub state: ServiceStateController,
    busy_lock: AsyncMutex<Option<BusyLock>>,
    _temp_dir: StoreFixtureRoot,
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
    _temp_dir: StoreFixtureRoot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandCall {
    pub program: String,
    pub args: Vec<OsString>,
    pub current_dir: PathBuf,
    pub deadline: tokio::time::Instant,
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
        deadline: tokio::time::Instant,
    ) -> io::Result<Vec<u8>> {
        self.calls
            .lock()
            .expect("lock command calls")
            .push(CommandCall {
                program: program.to_owned(),
                args: args.to_vec(),
                current_dir: current_dir.to_path_buf(),
                deadline,
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
    let status = command_status(Command::new(program).args(args).current_dir(current_dir))
        .expect("run fixture command");
    assert!(status.success(), "fixture command failed: {program}");
}

fn fixture_output<I, S>(current_dir: &Path, program: &str, args: I) -> Vec<u8>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = command_output(Command::new(program).args(args).current_dir(current_dir))
        .expect("run fixture command");
    assert!(output.status.success(), "fixture command failed: {program}");
    output.stdout
}

pub fn command_output(command: &mut Command) -> io::Result<Output> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = {
        let _spawn_guard = coding_agent_runtime::acquire_process_spawn_lock();
        command.spawn()?
    };
    child.wait_with_output()
}

pub fn command_status(command: &mut Command) -> io::Result<ExitStatus> {
    let mut child = {
        let _spawn_guard = coding_agent_runtime::acquire_process_spawn_lock();
        command.spawn()?
    };
    child.wait()
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
    registered_pair: watch::Sender<Option<ReversePollingPair>>,
}

#[cfg(feature = "test-support")]
#[derive(Clone)]
struct ReversePollingPair {
    first_task_id: TaskId,
    later_task_id: TaskId,
    later_entered: Arc<Notify>,
}

struct RunnerFixtureCore {
    store: Store,
    repository: Repository,
    writer: StoreWriterHandle,
    manager: TaskManagerHandle,
    _temp_dir: StoreFixtureRoot,
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
    let root = temp_dir.path().canonicalize().unwrap();
    let database_path = root.join("store.sqlite3");
    let store = Store::open(database_path)
        .await
        .expect("open fixture store");
    store.migrate().await.expect("migrate fixture store");
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
        _temp_dir: StoreFixtureRoot::new(temp_dir),
    }
}

pub async fn repository_control_fixture(
    store: &Store,
) -> (
    Arc<RepositoryControlCoordinator>,
    Arc<dyn RepositoryIdentityResolver>,
) {
    let lookups = store
        .list_repository_identity_lookups()
        .await
        .expect("load repository identity lookups");
    for lookup in &lookups {
        let common_git = lookup.git_root.as_path().join(".git");
        if !common_git.exists() {
            std::fs::create_dir_all(&common_git)
                .expect("create fixture common Git identity directory");
        }
    }
    let resolver: Arc<dyn RepositoryIdentityResolver> =
        Arc::new(FilesystemRepositoryIdentityResolver);
    let coordinator = Arc::new(RepositoryControlCoordinator::new());
    coordinator
        .register_aliases(lookups, resolver.as_ref())
        .expect("register fixture repository identities");
    (coordinator, resolver)
}

pub async fn task_manager_launch_resources(
    fixture: &StoreFixture,
    global: usize,
    per_repository: usize,
) -> TaskManagerLaunchResources {
    let (repository_control, _) = repository_control_fixture(&fixture.store).await;
    TaskManagerLaunchResources::new_for_test(
        SchedulerConcurrencyLimits::try_new(
            u32::try_from(global).expect("test global concurrency fits u32"),
            u32::try_from(per_repository).expect("test repository concurrency fits u32"),
        )
        .expect("valid task-manager fixture concurrency"),
        repository_control,
        fixture.instance_process_scope(),
    )
    .with_critical_stop_persistence_budget_for_test(Duration::from_secs(30))
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
    let launch_resources = task_manager_launch_resources(&fixture, concurrency, concurrency).await;
    let dispatcher = EventDispatcherHandle::spawn(fixture.store.clone(), 1_024)
        .await
        .expect("spawn task-manager fixture dispatcher");
    let writer = StoreWriterHandle::spawn(fixture.store.clone(), Arc::new(dispatcher.clone()), 64);
    let state = ServiceStateController::new(ServiceState::Ready);
    let runner = Arc::new(ControlledRunner::default());
    let manager = TaskManagerHandle::spawn(
        fixture.store.clone(),
        writer.clone(),
        dispatcher.clone(),
        state.clone(),
        runner.clone(),
        launch_resources,
        64,
    );
    TaskManagerFixture {
        store: fixture.store,
        repository: fixture.repository,
        writer,
        dispatcher,
        manager,
        runner,
        state,
        busy_lock: AsyncMutex::new(None),
        _temp_dir: fixture._temp_dir,
    }
}

#[cfg(feature = "test-support")]
pub async fn gated_two_repository_task_manager_fixture(
    global: usize,
    per_repository: usize,
) -> (TaskManagerFixture, Repository) {
    let fixture = store_fixture().await;
    let second_repository = match fixture
        .store
        .register_repository(repository_input_at(&fixture.root, "second"))
        .await
        .expect("register second task-manager fixture repository")
    {
        RegisterRepositoryOutcome::Created(repository)
        | RegisterRepositoryOutcome::Existing(repository) => repository,
    };
    let launch_resources = task_manager_launch_resources(&fixture, global, per_repository).await;
    let dispatcher = EventDispatcherHandle::spawn(fixture.store.clone(), 1_024)
        .await
        .expect("spawn gated task-manager fixture dispatcher");
    let writer = StoreWriterHandle::spawn(fixture.store.clone(), Arc::new(dispatcher.clone()), 64);
    let state = ServiceStateController::new(ServiceState::StoreDegraded);
    let runner = Arc::new(ControlledRunner::default());
    let manager = TaskManagerHandle::spawn(
        fixture.store.clone(),
        writer.clone(),
        dispatcher.clone(),
        state.clone(),
        runner.clone(),
        launch_resources,
        64,
    );
    (
        TaskManagerFixture {
            store: fixture.store,
            repository: fixture.repository,
            writer,
            dispatcher,
            manager,
            runner,
            state,
            busy_lock: AsyncMutex::new(None),
            _temp_dir: fixture._temp_dir,
        },
        second_repository,
    )
}

#[cfg(feature = "test-support")]
pub async fn task_manager_fixture_with_writer_faults(
    global: usize,
    per_repository: usize,
    faults: impl IntoIterator<Item = StoreWriterFaultSpec>,
) -> (TaskManagerFixture, Arc<StoreWriterTestController>) {
    let fixture = store_fixture().await;
    let launch_resources = task_manager_launch_resources(&fixture, global, per_repository).await;
    let dispatcher = EventDispatcherHandle::spawn(fixture.store.clone(), 1_024)
        .await
        .expect("spawn faulted task-manager fixture dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new(faults)
            .expect("construct task-manager StoreWriter controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store.clone(),
        Arc::new(dispatcher.clone()),
        64,
        controller.clone(),
    );
    let state = ServiceStateController::new(ServiceState::Ready);
    let runner = Arc::new(ControlledRunner::default());
    let manager = TaskManagerHandle::spawn(
        fixture.store.clone(),
        writer.clone(),
        dispatcher.clone(),
        state.clone(),
        runner.clone(),
        launch_resources,
        64,
    );
    (
        TaskManagerFixture {
            store: fixture.store,
            repository: fixture.repository,
            writer,
            dispatcher,
            manager,
            runner,
            state,
            busy_lock: AsyncMutex::new(None),
            _temp_dir: fixture._temp_dir,
        },
        controller,
    )
}

#[cfg(feature = "test-support")]
pub async fn paused_finalize_task_manager_fixture()
-> (TaskManagerFixture, Arc<StoreWriterTestController>) {
    let fixture = store_fixture().await;
    let launch_resources = task_manager_launch_resources(&fixture, 1, 1).await;
    let dispatcher = EventDispatcherHandle::spawn(fixture.store.clone(), 1_024)
        .await
        .expect("spawn paused-finalize fixture dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
            operation: Some(StoreWriterOperationKind::FinalizeReviewedTask),
            count: 1,
        }])
        .expect("construct paused-finalize controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store.clone(),
        Arc::new(dispatcher.clone()),
        64,
        controller.clone(),
    );
    let state = ServiceStateController::new(ServiceState::Ready);
    let runner = Arc::new(ControlledRunner::default());
    let manager = TaskManagerHandle::spawn(
        fixture.store.clone(),
        writer.clone(),
        dispatcher.clone(),
        state.clone(),
        runner.clone(),
        launch_resources,
        64,
    );
    (
        TaskManagerFixture {
            store: fixture.store,
            repository: fixture.repository,
            writer,
            dispatcher,
            manager,
            runner,
            state,
            busy_lock: AsyncMutex::new(None),
            _temp_dir: fixture._temp_dir,
        },
        controller,
    )
}

pub async fn degraded_fixture_with_concurrency(concurrency: usize) -> DegradedFixture {
    let fixture = store_fixture().await;
    let launch_resources = task_manager_launch_resources(&fixture, concurrency, concurrency).await;
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
        launch_resources,
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

#[cfg(feature = "test-support")]
pub async fn degraded_fixture_with_writer_faults(
    concurrency: usize,
    faults: impl IntoIterator<Item = StoreWriterFaultSpec>,
) -> (DegradedFixture, Arc<StoreWriterTestController>) {
    let fixture = store_fixture().await;
    let launch_resources = task_manager_launch_resources(&fixture, concurrency, concurrency).await;
    let database_path = fixture.root.join("store.sqlite3");
    let dispatcher = EventDispatcherHandle::spawn(fixture.store.clone(), 1_024)
        .await
        .expect("spawn degraded fixture dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new(faults)
            .expect("construct degraded fixture StoreWriter controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store.clone(),
        Arc::new(dispatcher.clone()),
        64,
        controller.clone(),
    );
    let state = ServiceStateController::new(ServiceState::Ready);
    let runner = Arc::new(ControlledRunner::default());
    let manager = TaskManagerHandle::spawn(
        fixture.store.clone(),
        writer.clone(),
        dispatcher.clone(),
        state.clone(),
        runner.clone(),
        launch_resources,
        64,
    );
    let recovery_results = manager.subscribe_degraded_recovery();
    (
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
        },
        controller,
    )
}

#[cfg(feature = "test-support")]
pub async fn shutdown_fixture(
    scenarios: impl IntoIterator<Item = FakeScenario>,
) -> ShutdownFixture {
    let startup = StartupFixture::new();
    let runner = Arc::new(ScriptedFakeRunner::new(
        FakeRunnerConfig::default(),
        scenarios,
    ));
    let mut dependencies = startup.dependencies(StartupBehavior::default());
    dependencies.runner_factory = Arc::new(FixedStartupRunnerFactory::new(
        runner.clone(),
        NonZeroU32::new(4).expect("test concurrency is nonzero"),
    ));
    let primary = match launch(dependencies).await.expect("launch shutdown fixture") {
        StartupOutcome::Primary(primary) => primary,
        StartupOutcome::Secondary(_) => panic!("shutdown fixture must own the primary lock"),
    };
    let handles = primary.test_handles();
    let repository_root = startup
        ._temp
        .path()
        .canonicalize()
        .expect("canonicalize shutdown fixture repository root");
    let repository_input = repository_input_at(&repository_root, "shutdown");
    std::fs::create_dir_all(repository_input.git_root.as_path().join(".git"))
        .expect("create authenticated shutdown repository identity");
    let registration_deadline = deadline();
    let repository = match handles
        .writer
        .register_repository(repository_input, registration_deadline)
        .await
        .expect("register shutdown fixture repository")
        .value
    {
        RegisterRepositoryOutcome::Created(repository)
        | RegisterRepositoryOutcome::Existing(repository) => repository,
    };
    handles
        .attach_repository_runtime_for_test(&repository, registration_deadline)
        .await
        .expect("attach shutdown fixture repository runtime");

    ShutdownFixture {
        primary,
        startup,
        handles,
        runner,
        repository,
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
    let (registered_pair, _) = watch::channel(None);
    let reverse_poll = Arc::new(ReversePollingRunner {
        inner: runner.clone(),
        registered_pair,
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
    async fn run(&self, mut context: RunContext, sink: RunnerEventSink) -> RunnerOutcome {
        context.complete_preparation_for_test().await;
        let (is_first, later_entered) = self.registered_pair_for(context.task.id).await;
        if is_first {
            later_entered.notified().await;
        } else {
            later_entered.notify_one();
        }
        self.inner
            .run_after_preparation_for_test(context, sink)
            .await
    }
}

#[cfg(feature = "test-support")]
impl ReversePollingRunner {
    fn register_pair(&self, first_task_id: TaskId, later_task_id: TaskId) {
        self.registered_pair.send_replace(Some(ReversePollingPair {
            first_task_id,
            later_task_id,
            later_entered: Arc::new(Notify::new()),
        }));
    }

    async fn registered_pair_for(&self, task_id: TaskId) -> (bool, Arc<Notify>) {
        let mut registered_pair = self.registered_pair.subscribe();
        loop {
            let pair = registered_pair.borrow_and_update().clone();
            if let Some(pair) = pair {
                if pair.first_task_id == task_id {
                    return (true, pair.later_entered);
                }
                if pair.later_task_id == task_id {
                    return (false, pair.later_entered);
                }
            }
            registered_pair
                .changed()
                .await
                .expect("reverse-poll pair registration remains alive");
        }
    }
}

async fn runner_fixture(runner: Arc<dyn TaskRunner>, concurrency: usize) -> RunnerFixtureCore {
    let fixture = store_fixture().await;
    let launch_resources = task_manager_launch_resources(&fixture, concurrency, concurrency).await;
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
        launch_resources,
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
        self.wait_for_status_with_timeout(task_id, expected, Duration::from_secs(5))
            .await;
    }

    async fn wait_for_status_with_timeout(
        &self,
        task_id: TaskId,
        expected: TaskStatus,
        timeout: Duration,
    ) {
        tokio::time::timeout(timeout, async {
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
            let [first, later] = tasks.as_slice() else {
                panic!("reverse-poll fixture requires exactly two tasks per registered pair");
            };
            reverse_poll.register_pair(first.id, later.id);
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

    pub async fn detail(&self, task_id: TaskId) -> coding_agent_store::TaskDetail {
        self.core.detail(task_id).await
    }

    pub async fn event_kinds(&self, task_id: TaskId) -> Vec<TaskEventKind> {
        self.core.event_kinds(task_id).await
    }

    pub async fn wait_for_status(&self, task_id: TaskId, expected: TaskStatus) {
        self.core.wait_for_status(task_id, expected).await;
    }

    pub async fn wait_for_status_with_timeout(
        &self,
        task_id: TaskId,
        expected: TaskStatus,
        timeout: Duration,
    ) {
        self.core
            .wait_for_status_with_timeout(task_id, expected, timeout)
            .await;
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
                        entry: ActivityEntry::legacy(
                            format!("activity-{index}-{message}"),
                            ActivityLevel::Info,
                            message.clone(),
                            timestamp(),
                        ),
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

    pub async fn enqueue_tasks_for_repository(
        &self,
        repository_id: RepositoryId,
        prompt_prefix: &str,
        count: usize,
    ) -> Vec<Task> {
        let mut tasks = Vec::with_capacity(count);
        for index in 0..count {
            let receipt = self
                .writer
                .create_task(
                    new_task(repository_id, &format!("{prompt_prefix} {index}")),
                    deadline(),
                )
                .await
                .expect("create task for explicit fixture repository");
            tasks.push(receipt.value.task().clone());
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

    pub async fn event_kinds(&self, task_id: TaskId) -> Vec<TaskEventKind> {
        self.store
            .task_events_after(task_id, EventCursor::ZERO, usize::MAX)
            .await
            .expect("load manager fixture task events")
            .events
            .into_iter()
            .map(|event| event.payload.kind())
            .collect()
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

    pub async fn wait_for_runner_start(&self, task_id: TaskId) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.runner.started_count(task_id) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("runner did not start task {task_id} before the timeout"));
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
        let mut transaction = self
            .store
            .pool()
            .begin()
            .await
            .expect("begin task creation-time override");
        let task = sqlx::query("UPDATE tasks SET created_at = ? WHERE id = ?")
            .bind(timestamp)
            .bind(task_id.to_string())
            .execute(&mut *transaction)
            .await
            .expect("override task creation time");
        let queued = sqlx::query(
            "UPDATE task_events \
             SET created_at = ?, \
                 payload_json = json_set(payload_json, '$.task.created_at', ?) \
             WHERE task_id = ? AND kind = 'task.queued'",
        )
        .bind(timestamp)
        .bind(timestamp)
        .bind(task_id.to_string())
        .execute(&mut *transaction)
        .await
        .expect("override queued-event creation time");
        assert_eq!(task.rows_affected(), 1, "fixture task exists exactly once");
        assert_eq!(
            queued.rows_affected(),
            1,
            "fixture queued event exists exactly once"
        );
        transaction
            .commit()
            .await
            .expect("commit task creation-time override");
        let expected = UtcTimestamp::parse_rfc3339(timestamp)
            .expect("fixture override uses a valid RFC 3339 timestamp");
        let detail = self
            .store
            .task_detail(task_id)
            .await
            .expect("validate creation-time override aggregate")
            .expect("creation-time override task still exists");
        assert_eq!(detail.task.created_at, expected);
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

    pub async fn fail_fifo_head_started_event_inserts(&self, enabled: bool) {
        let statement = if enabled {
            "CREATE TRIGGER fail_fifo_head_started BEFORE INSERT ON task_events \
             WHEN NEW.kind = 'task.started' AND NEW.task_id = (\
                 SELECT id FROM tasks ORDER BY created_at, id LIMIT 1\
             ) BEGIN SELECT RAISE(ABORT, 'injected'); END"
        } else {
            "DROP TRIGGER IF EXISTS fail_fifo_head_started"
        };
        sqlx::query(statement)
            .execute(self.store.pool())
            .await
            .expect("toggle FIFO-head task-start failure trigger");
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
            for connection in &mut legacy_connections {
                sqlx::query("PRAGMA busy_timeout = 0")
                    .execute(&mut **connection)
                    .await
                    .expect("set zero busy timeout on an existing fixture connection");
            }
            drop(legacy_connections);
            let transaction = self
                .store
                .pool()
                .begin_with("BEGIN IMMEDIATE")
                .await
                .expect("hold SQLite writer lock");
            *held = Some(BusyLock {
                transaction,
                legacy_connections: Vec::new(),
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
    pub async fn start_cleanup_unproven_task(&self) -> (TaskId, CleanupUnprovenGate) {
        let gate = self.runner.push_cleanup_unproven_gate();
        let task = self.create_task("degraded cleanup-unproven task").await;
        self.manager
            .notify_queued(task.id)
            .await
            .expect("notify cleanup-unproven task");
        self.wait_for_status(task.id, TaskStatus::Running).await;
        (task.id, gate)
    }

    pub async fn start_success_task(&self) -> TaskId {
        let gate = self.runner.push_completion_gate();
        let task = self.create_task("degraded running task").await;
        self.completion_gates.lock().await.insert(task.id, gate);
        self.manager
            .notify_queued(task.id)
            .await
            .expect("notify degraded fixture running task");
        self.wait_for_status(task.id, TaskStatus::Running).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self
                    .store
                    .task_detail(task.id)
                    .await
                    .expect("load degraded fixture plan")
                    .expect("degraded fixture task exists")
                    .plan
                    .is_some()
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("degraded fixture runner persists its review plan");
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

    pub async fn start_review_task(&self, evidence: NewReviewEvidence) -> (TaskId, EventGate) {
        let gate = self.runner.push_review_gate(evidence);
        let task = self.create_task("degraded review task").await;
        self.manager
            .notify_queued(task.id)
            .await
            .expect("notify degraded fixture review task");
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

    pub async fn event_kinds(&self, task_id: TaskId) -> Vec<TaskEventKind> {
        self.store
            .task_events_after(task_id, EventCursor::ZERO, usize::MAX)
            .await
            .expect("load degraded fixture events")
            .events
            .into_iter()
            .map(|event| event.payload.kind())
            .collect()
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
    started_order: Mutex<Vec<TaskId>>,
    releases: Mutex<HashMap<TaskId, Arc<Notify>>>,
    held_process_trees:
        Mutex<HashMap<TaskId, coding_agent_runtime::HeldProcessLivenessTreeForTest>>,
}

type EventAppendResult = Result<coding_agent_domain::EventId, RunnerEventError>;
type SharedEventResultReceiver = Arc<AsyncMutex<Option<oneshot::Receiver<EventAppendResult>>>>;

enum RunnerScenario {
    Blocking(Arc<Notify>),
    CleanupUnprovenGate(Arc<Notify>),
    Failure(TaskFailure),
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
    ReviewGate {
        release: Arc<Notify>,
        evidence: NewReviewEvidence,
        result: oneshot::Sender<EventAppendResult>,
    },
    DurableThenLateEvent {
        durable: RunnerEvent,
        release: Arc<Notify>,
        late: RunnerEvent,
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

pub struct CleanupUnprovenGate {
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

    pub fn push_failure(&self, failure: TaskFailure) {
        self.push(RunnerScenario::Failure(failure));
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

    pub fn push_cleanup_unproven_gate(&self) -> CleanupUnprovenGate {
        let release = Arc::new(Notify::new());
        self.push(RunnerScenario::CleanupUnprovenGate(release.clone()));
        CleanupUnprovenGate { release }
    }

    pub fn cleanup_tree_is_held(&self, task_id: TaskId) -> bool {
        self.held_process_trees
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&task_id)
    }

    pub fn release_cleanup_tree(&self, task_id: TaskId) -> bool {
        self.held_process_trees
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&task_id)
            .is_some()
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

    pub fn push_review_gate(&self, evidence: NewReviewEvidence) -> EventGate {
        let release = Arc::new(Notify::new());
        let (result, receiver) = oneshot::channel();
        self.push(RunnerScenario::ReviewGate {
            release: release.clone(),
            evidence,
            result,
        });
        EventGate {
            release,
            result: receiver,
        }
    }

    pub fn push_durable_then_late_event(
        &self,
        durable: RunnerEvent,
        late: RunnerEvent,
    ) -> EventGate {
        let release = Arc::new(Notify::new());
        let (result, receiver) = oneshot::channel();
        self.push(RunnerScenario::DurableThenLateEvent {
            durable,
            release: release.clone(),
            late,
            result,
        });
        EventGate {
            release,
            result: receiver,
        }
    }

    pub async fn release(&self, task_id: TaskId) {
        let release = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let release = self
                    .releases
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&task_id)
                    .cloned();
                if let Some(release) = release {
                    return release;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("blocking runner did not start task {task_id}"));
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

    pub async fn wait_for_started_task(&self, index: usize) -> TaskId {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let task_id = self
                    .started_order
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(index)
                    .copied();
                if let Some(task_id) = task_id {
                    return task_id;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("runner did not publish start at index {index}"))
    }

    pub fn started_task_ids(&self) -> Vec<TaskId> {
        self.started_order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
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
    async fn run(&self, mut context: RunContext, sink: RunnerEventSink) -> RunnerOutcome {
        context.complete_preparation_for_test().await;
        let scenario = self
            .scenarios
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
            .expect("a runner scenario is queued for every started task");
        if let RunnerScenario::Blocking(release) = &scenario {
            self.releases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(context.task.id, release.clone());
        }
        *self
            .started
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(context.task.id)
            .or_insert(0) += 1;
        self.started_order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(context.task.id);
        match scenario {
            RunnerScenario::Blocking(release) => tokio::select! {
                () = context.cancellation.cancelled() => RunnerOutcome::Cancelled,
                () = release.notified() => approved_outcome(&sink).await,
            },
            RunnerScenario::CleanupUnprovenGate(release) => {
                let held = context
                    .process_liveness_scope()
                    .hold_tree_for_test()
                    .expect("hold the exact cleanup-unproven task process tree");
                assert!(
                    self.held_process_trees
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(context.task.id, held)
                        .is_none(),
                    "a cleanup-unproven task owns one held process tree"
                );
                release.notified().await;
                RunnerOutcome::ProcessCleanupUnproven
            }
            RunnerScenario::Failure(failure) => RunnerOutcome::Failed(failure),
            RunnerScenario::Panic => panic!("injected runner panic"),
            RunnerScenario::LateEvent { release, result } => {
                if let Err(outcome) = persist_review_plan(&sink).await {
                    return outcome;
                }
                tokio::spawn(async move {
                    release.notified().await;
                    let _ = result.send(
                        sink.append(RunnerEvent::PlanUpdated(PlanSnapshot::legacy(
                            99,
                            Vec::new(),
                        )))
                        .await,
                    );
                });
                RunnerOutcome::Approved(approved_review())
            }
            RunnerScenario::Events { events, result } => {
                let mut ids = Vec::with_capacity(events.len());
                for event in events {
                    ids.push(sink.append(event).await.expect("append running event"));
                }
                let _ = result.send(ids);
                approved_outcome(&sink).await
            }
            RunnerScenario::CompletionGate(release) => {
                if let Err(outcome) = persist_review_plan(&sink).await {
                    return outcome;
                }
                release.notified().await;
                RunnerOutcome::Approved(approved_review())
            }
            RunnerScenario::EventGate {
                release,
                event,
                result,
            } => {
                release.notified().await;
                let append = sink.append(event).await;
                let rejected = append.is_err();
                let _ = result.send(append);
                if rejected {
                    RunnerOutcome::Failed(failure("RUNNER_EVENT_REJECTED"))
                } else {
                    approved_outcome(&sink).await
                }
            }
            RunnerScenario::ReviewGate {
                release,
                evidence,
                result,
            } => {
                if let Err(outcome) = persist_review_plan(&sink).await {
                    return outcome;
                }
                release.notified().await;
                let review = sink.record_review(evidence).await;
                let rejected = review.is_err();
                let _ = result.send(review);
                if rejected {
                    RunnerOutcome::Failed(failure("RUNNER_REVIEW_REJECTED"))
                } else {
                    RunnerOutcome::Cancelled
                }
            }
            RunnerScenario::DurableThenLateEvent {
                durable,
                release,
                late,
                result,
            } => {
                sink.append(durable)
                    .await
                    .expect("append durable runner event before the gate");
                tokio::select! {
                    biased;
                    () = context.cancellation.cancelled() => {}
                    () = release.notified() => {}
                }
                let append = sink.append(late).await;
                let rejected = append.is_err();
                let _ = result.send(append);
                if rejected {
                    RunnerOutcome::Failed(failure("RUNNER_EVENT_REJECTED"))
                } else {
                    approved_outcome(&sink).await
                }
            }
        }
    }
}

async fn approved_outcome(sink: &RunnerEventSink) -> RunnerOutcome {
    match persist_review_plan(sink).await {
        Ok(()) => RunnerOutcome::Approved(approved_review()),
        Err(outcome) => outcome,
    }
}

async fn persist_review_plan(sink: &RunnerEventSink) -> Result<(), RunnerOutcome> {
    sink.append(RunnerEvent::PlanUpdated(fixture_review_plan()))
        .await
        .map(|_| ())
        .map_err(|_| RunnerOutcome::Failed(failure("RUNNER_EVENT_REJECTED")))
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

pub fn approved_review() -> NewReviewEvidence {
    approved_review_round(1)
}

pub fn approved_review_round(round: u8) -> NewReviewEvidence {
    approved_review_round_with_summary(round, format!("fixture round {round} approved"))
}

pub fn approved_review_round_with_summary(
    round: u8,
    summary: impl Into<String>,
) -> NewReviewEvidence {
    let generation = u64::from(round);
    let digit = char::from(b'a' + round - 1);
    let digest = WorkspaceDigest::try_new(digit.to_string().repeat(64))
        .expect("construct fixture workspace digest");
    let check = fixture_required_check();
    let evidence = CheckEvidence::try_for_check(
        &check,
        CheckActor::Executor,
        u32::from(round),
        generation,
        digest.clone(),
        CheckEvidenceStatus::Passed,
        10,
        "fixture check passed",
        false,
    )
    .expect("construct fixture check evidence");
    let coverage =
        ReviewCoverageEvidence::try_new(generation, digest.clone(), "f".repeat(64), vec![0], 1)
            .expect("construct fixture coverage");
    NewReviewEvidence::try_new(
        round,
        ReviewDecisionSource::Reviewer,
        generation,
        digest,
        ReviewVerdict::Approved,
        summary,
        Vec::new(),
        Vec::new(),
        vec![check],
        vec![evidence],
        Some(coverage),
    )
    .expect("construct fixture approved review")
}

pub fn changes_requested_review(round: u8) -> NewReviewEvidence {
    changes_requested_review_with_summary(round, format!("fixture round {round} changes requested"))
}

pub fn changes_requested_review_with_summary(
    round: u8,
    summary: impl Into<String>,
) -> NewReviewEvidence {
    let generation = u64::from(round);
    let digit = char::from(b'a' + round - 1);
    let digest = WorkspaceDigest::try_new(digit.to_string().repeat(64))
        .expect("construct fixture workspace digest");
    let check = fixture_required_check();
    let evidence = CheckEvidence::try_for_check(
        &check,
        CheckActor::Executor,
        u32::from(round),
        generation,
        digest.clone(),
        CheckEvidenceStatus::Passed,
        10,
        "fixture check passed",
        false,
    )
    .expect("construct fixture check evidence");
    NewReviewEvidence::try_new(
        round,
        ReviewDecisionSource::Reviewer,
        generation,
        digest,
        ReviewVerdict::ChangesRequested,
        summary,
        vec![
            ReviewFinding::try_for_review(
                round,
                1,
                FindingSeverity::Blocking,
                "fixture blocking finding",
                Some("src/lib.rs".to_owned()),
                Some(1),
            )
            .expect("construct fixture finding"),
        ],
        Vec::new(),
        vec![check],
        vec![evidence],
        None,
    )
    .expect("construct fixture changes-requested review")
}

fn fixture_required_check() -> RequiredCheck {
    RequiredCheck::try_cargo_test("fixture-cargo-test", None, None)
        .expect("construct fixture required check")
}

pub fn fixture_review_plan() -> PlanSnapshot {
    PlanSnapshot::try_structured(
        1,
        "Fixture review plan",
        vec![
            PlanItem::try_structured(
                "fixture-plan",
                "Complete fixture work",
                "Complete the deterministic fixture task",
                vec!["The fixture required check passes".to_owned()],
                PlanItemStatus::Completed,
            )
            .expect("construct fixture plan item"),
        ],
        vec![fixture_required_check()],
    )
    .expect("construct fixture structured plan")
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
