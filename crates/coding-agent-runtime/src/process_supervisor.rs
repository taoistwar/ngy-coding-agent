use std::collections::{BTreeMap, VecDeque};
use std::ffi::OsString;
use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::{self, Instant as TokioInstant};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::command_policy::{CommandPolicyError, ValidatedCommand};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedStream {
    pub head: Vec<u8>,
    pub tail: Vec<u8>,
    pub observed_bytes: u64,
    pub omitted_observed_bytes: u64,
    pub truncated: bool,
    pub complete: bool,
}

impl CapturedStream {
    fn empty() -> Self {
        Self {
            head: Vec::new(),
            tail: Vec::new(),
            observed_bytes: 0,
            omitted_observed_bytes: 0,
            truncated: false,
            complete: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandResult {
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub timed_out: bool,
    pub cancelled: bool,
    pub stdout: CapturedStream,
    pub stderr: CapturedStream,
    pub truncated: bool,
    pub duration_ms: u64,
}

impl CommandResult {
    fn cancelled_before_spawn() -> Self {
        Self {
            exit_code: None,
            signal: None,
            timed_out: false,
            cancelled: true,
            stdout: CapturedStream::empty(),
            stderr: CapturedStream::empty(),
            truncated: false,
            duration_ms: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessLimits {
    stdout_bytes: usize,
    stderr_bytes: usize,
    max_command_timeout: Duration,
    cleanup_timeout: Duration,
}

impl ProcessLimits {
    pub fn try_new(
        stdout_bytes: usize,
        stderr_bytes: usize,
        max_command_timeout: Duration,
        cleanup_timeout: Duration,
    ) -> Result<Self, ProcessLimitsError> {
        if stdout_bytes == 0 || stderr_bytes == 0 {
            return Err(ProcessLimitsError::ZeroOutputLimit);
        }
        if max_command_timeout.is_zero() || cleanup_timeout.is_zero() {
            return Err(ProcessLimitsError::ZeroDuration);
        }
        Ok(Self {
            stdout_bytes,
            stderr_bytes,
            max_command_timeout,
            cleanup_timeout,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProcessLimitsError {
    #[error("process output limits must be non-zero")]
    ZeroOutputLimit,
    #[error("process timeout limits must be non-zero")]
    ZeroDuration,
}

/// Opaque guard used to serialize process creation across application crates.
///
/// Darwin needs this coordination because its portable pipe fallback applies
/// `FD_CLOEXEC` immediately after creation rather than atomically.
pub struct ProcessSpawnGuard {
    _guard: std::sync::MutexGuard<'static, ()>,
}

pub fn acquire_process_spawn_lock() -> ProcessSpawnGuard {
    let guard = process_spawn_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    ProcessSpawnGuard { _guard: guard }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ToolchainEnvironmentError {
    #[error("toolchain environment directories must be existing absolute directories")]
    Directory,
    #[error("the pinned Rust compiler must be an existing absolute file")]
    Compiler,
    #[cfg(all(windows, target_env = "msvc"))]
    #[error("the pinned MSVC linker must be an existing absolute file")]
    Linker,
    #[error("the explicit tool search path is invalid")]
    SearchPath,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum PlatformEnvironmentError {
    #[error("the process temporary directory must be an existing absolute directory")]
    TempDirectory,
    #[error("the Windows system root must be an existing absolute directory")]
    SystemRoot,
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error("validated process command is invalid")]
    InvalidCommand,
    #[error("validated process command identity or policy check failed")]
    CommandPolicy(#[source] CommandPolicyError),
    #[error("requested command timeout exceeds the configured maximum")]
    TimeoutOutsideLimit,
    #[error("process spawn failed")]
    SpawnFailed(#[source] io::Error),
    #[error("process tree setup failed before user code could run")]
    TreeSetupFailed(#[source] io::Error),
    #[error("spawned process did not expose both output pipes")]
    MissingOutputPipe,
    #[error("waiting for the process failed")]
    WaitFailed(#[source] io::Error),
    #[error("process tree termination failed")]
    TreeCleanupFailed(#[source] io::Error),
    #[error("bounded process tree cleanup timed out")]
    CleanupTimedOut,
    #[error("draining process output failed")]
    OutputDrainFailed(#[source] io::Error),
    #[error("process supervisor worker failed")]
    WorkerFailed,
}

impl ProcessError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidCommand | Self::TimeoutOutsideLimit => "COMMAND_NOT_ALLOWED",
            Self::CommandPolicy(error) => error.code(),
            Self::SpawnFailed(_) | Self::TreeSetupFailed(_) => "COMMAND_SPAWN_FAILED",
            Self::MissingOutputPipe | Self::OutputDrainFailed(_) => "COMMAND_OUTPUT_FAILED",
            Self::WaitFailed(_) | Self::WorkerFailed => "COMMAND_WAIT_FAILED",
            Self::TreeCleanupFailed(_) | Self::CleanupTimedOut => "PROCESS_TREE_CLEANUP_FAILED",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ChildEnvironment {
    entries: BTreeMap<OsString, OsString>,
}

#[derive(Debug, Clone)]
pub(crate) struct PlatformEnvironment {
    temp_directory: PathBuf,
    system_root: Option<PathBuf>,
}

impl PlatformEnvironment {
    pub(crate) fn try_new(
        temp_directory: PathBuf,
        system_root: Option<PathBuf>,
    ) -> Result<Self, PlatformEnvironmentError> {
        if !is_existing_absolute_directory(&temp_directory) {
            return Err(PlatformEnvironmentError::TempDirectory);
        }
        #[cfg(windows)]
        if system_root
            .as_ref()
            .is_none_or(|path| !is_existing_absolute_directory(path))
        {
            return Err(PlatformEnvironmentError::SystemRoot);
        }
        #[cfg(unix)]
        if system_root.is_some() {
            return Err(PlatformEnvironmentError::SystemRoot);
        }
        Ok(Self {
            temp_directory,
            system_root,
        })
    }

    fn from_current_process() -> Result<Self, PlatformEnvironmentError> {
        #[cfg(windows)]
        let system_root = std::env::var_os("SYSTEMROOT")
            .or_else(|| std::env::var_os("WINDIR"))
            .map(PathBuf::from);
        #[cfg(unix)]
        let system_root = None;
        Self::try_new(std::env::temp_dir(), system_root)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RustToolchainEnvironment {
    search_path: OsString,
    cargo_home: PathBuf,
    rustup_home: Option<PathBuf>,
    rustc: PathBuf,
    rustdoc: PathBuf,
    #[cfg(all(windows, target_env = "msvc"))]
    windows_msvc: Option<WindowsMsvcEnvironment>,
}

#[cfg(all(windows, target_env = "msvc"))]
#[derive(Debug, Clone)]
pub(crate) struct WindowsMsvcEnvironment {
    linker_environment_key: &'static str,
    linker: PathBuf,
    library_path: OsString,
    include_path: OsString,
}

#[cfg(all(windows, target_env = "msvc"))]
impl WindowsMsvcEnvironment {
    pub(crate) fn try_new(
        linker: PathBuf,
        library_directories: Vec<PathBuf>,
        include_directories: Vec<PathBuf>,
    ) -> Result<Self, ToolchainEnvironmentError> {
        if !linker.is_absolute() || !linker.is_file() {
            return Err(ToolchainEnvironmentError::Linker);
        }
        let library_path = join_toolchain_directories(library_directories)?;
        let include_path = join_toolchain_directories(include_directories)?;
        let linker_environment_key =
            cargo_linker_environment_key().ok_or(ToolchainEnvironmentError::Linker)?;
        Ok(Self {
            linker_environment_key,
            linker,
            library_path,
            include_path,
        })
    }
}

impl RustToolchainEnvironment {
    pub(crate) fn try_new(
        search_directories: Vec<PathBuf>,
        cargo_home: PathBuf,
        rustup_home: Option<PathBuf>,
        rustc: PathBuf,
        rustdoc: PathBuf,
    ) -> Result<Self, ToolchainEnvironmentError> {
        if search_directories.is_empty()
            || search_directories
                .iter()
                .any(|directory| !is_existing_absolute_directory(directory))
            || !is_existing_absolute_directory(&cargo_home)
            || rustup_home
                .as_ref()
                .is_some_and(|directory| !is_existing_absolute_directory(directory))
        {
            return Err(ToolchainEnvironmentError::Directory);
        }
        if !rustc.is_absolute() || !rustc.is_file() || !rustdoc.is_absolute() || !rustdoc.is_file()
        {
            return Err(ToolchainEnvironmentError::Compiler);
        }
        if search_directories
            .iter()
            .any(|directory| contains_path_list_separator(directory.as_os_str()))
        {
            return Err(ToolchainEnvironmentError::SearchPath);
        }
        let search_path = std::env::join_paths(search_directories)
            .map_err(|_| ToolchainEnvironmentError::SearchPath)?;
        Ok(Self {
            search_path,
            cargo_home,
            rustup_home,
            rustc,
            rustdoc,
            #[cfg(all(windows, target_env = "msvc"))]
            windows_msvc: None,
        })
    }

    #[cfg(all(windows, target_env = "msvc"))]
    pub(crate) fn with_windows_msvc(mut self, windows_msvc: WindowsMsvcEnvironment) -> Self {
        self.windows_msvc = Some(windows_msvc);
        self
    }
}

#[cfg(all(windows, target_env = "msvc"))]
fn join_toolchain_directories(
    directories: Vec<PathBuf>,
) -> Result<OsString, ToolchainEnvironmentError> {
    if directories.is_empty()
        || directories
            .iter()
            .any(|directory| !is_existing_absolute_directory(directory))
        || directories
            .iter()
            .any(|directory| contains_path_list_separator(directory.as_os_str()))
    {
        return Err(ToolchainEnvironmentError::Directory);
    }
    std::env::join_paths(directories).map_err(|_| ToolchainEnvironmentError::SearchPath)
}

#[cfg(all(windows, target_env = "msvc"))]
fn cargo_linker_environment_key() -> Option<&'static str> {
    if cfg!(target_arch = "x86_64") {
        Some("CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_LINKER")
    } else if cfg!(target_arch = "aarch64") {
        Some("CARGO_TARGET_AARCH64_PC_WINDOWS_MSVC_LINKER")
    } else if cfg!(target_arch = "x86") {
        Some("CARGO_TARGET_I686_PC_WINDOWS_MSVC_LINKER")
    } else {
        None
    }
}

fn is_existing_absolute_directory(path: &std::path::Path) -> bool {
    path.is_absolute() && path.is_dir()
}

#[cfg(unix)]
fn contains_path_list_separator(value: &std::ffi::OsStr) -> bool {
    use std::os::unix::ffi::OsStrExt;
    value.as_bytes().contains(&b':')
}

#[cfg(windows)]
fn contains_path_list_separator(value: &std::ffi::OsStr) -> bool {
    use std::os::windows::ffi::OsStrExt;
    value.encode_wide().any(|unit| unit == u16::from(b';'))
}

impl ChildEnvironment {
    pub(crate) fn from_entries(entries: impl IntoIterator<Item = (OsString, OsString)>) -> Self {
        Self {
            entries: entries.into_iter().collect(),
        }
    }

    pub(crate) fn for_platform(platform: &PlatformEnvironment) -> Self {
        let mut entries = BTreeMap::new();
        #[cfg(windows)]
        {
            let system_root = platform
                .system_root
                .as_ref()
                .expect("validated Windows platform environment has a system root")
                .as_os_str()
                .to_owned();
            entries.insert(OsString::from("SYSTEMROOT"), system_root.clone());
            entries.insert(OsString::from("WINDIR"), system_root);
            let temp = platform.temp_directory.as_os_str().to_owned();
            entries.insert(OsString::from("TEMP"), temp.clone());
            entries.insert(OsString::from("TMP"), temp);
        }
        #[cfg(unix)]
        entries.insert(
            OsString::from("TMPDIR"),
            platform.temp_directory.as_os_str().to_owned(),
        );
        entries.insert(OsString::from("CARGO_NET_OFFLINE"), OsString::from("true"));
        entries.insert(OsString::from("CARGO_TERM_COLOR"), OsString::from("never"));
        entries.insert(OsString::from("RUST_BACKTRACE"), OsString::from("0"));
        entries.insert(OsString::from("LC_ALL"), OsString::from("C"));
        entries.insert(OsString::from("LANG"), OsString::from("C"));
        Self { entries }
    }

    pub(crate) fn for_git(platform: &PlatformEnvironment) -> Self {
        let mut environment = Self::for_platform(platform);
        for (key, value) in [
            ("GIT_CONFIG_NOSYSTEM", "1"),
            ("GIT_ATTR_NOSYSTEM", "1"),
            ("GIT_OPTIONAL_LOCKS", "0"),
            ("GIT_TERMINAL_PROMPT", "0"),
            ("GCM_INTERACTIVE", "Never"),
            ("GIT_LFS_SKIP_SMUDGE", "1"),
        ] {
            environment
                .entries
                .insert(OsString::from(key), OsString::from(value));
        }
        environment
    }

    pub(crate) fn for_rust_toolchain(
        platform: &PlatformEnvironment,
        toolchain: &RustToolchainEnvironment,
    ) -> Self {
        let mut environment = Self::for_platform(platform);
        environment
            .entries
            .insert(OsString::from("PATH"), toolchain.search_path.to_owned());
        environment.entries.insert(
            OsString::from("CARGO_HOME"),
            toolchain.cargo_home.as_os_str().to_owned(),
        );
        environment.entries.insert(
            OsString::from("RUSTC"),
            toolchain.rustc.as_os_str().to_owned(),
        );
        environment.entries.insert(
            OsString::from("RUSTDOC"),
            toolchain.rustdoc.as_os_str().to_owned(),
        );
        if let Some(rustup_home) = &toolchain.rustup_home {
            environment.entries.insert(
                OsString::from("RUSTUP_HOME"),
                rustup_home.as_os_str().to_owned(),
            );
        }
        #[cfg(all(windows, target_env = "msvc"))]
        if let Some(msvc) = &toolchain.windows_msvc {
            environment.entries.insert(
                OsString::from(msvc.linker_environment_key),
                msvc.linker.as_os_str().to_owned(),
            );
            environment
                .entries
                .insert(OsString::from("LIB"), msvc.library_path.to_owned());
            environment
                .entries
                .insert(OsString::from("INCLUDE"), msvc.include_path.to_owned());
        }
        environment
    }

    pub(crate) fn set_cargo_target_directory(&mut self, directory: &std::path::Path) {
        self.entries.insert(
            OsString::from("CARGO_TARGET_DIR"),
            directory.as_os_str().to_owned(),
        );
    }

    fn from_current_process() -> Result<Self, PlatformEnvironmentError> {
        PlatformEnvironment::from_current_process().map(|platform| Self::for_platform(&platform))
    }

    #[cfg(test)]
    fn insert_test_value(&mut self, key: impl Into<OsString>, value: impl Into<OsString>) {
        self.entries.insert(key.into(), value.into());
    }

    pub(crate) fn entries(&self) -> &BTreeMap<OsString, OsString> {
        &self.entries
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ProcessSupervisor {
    limits: ProcessLimits,
    tasks: TaskTracker,
}

impl ProcessSupervisor {
    pub(crate) fn new(limits: ProcessLimits) -> Self {
        Self {
            limits,
            tasks: TaskTracker::new(),
        }
    }

    pub(crate) async fn run(
        &self,
        command: ValidatedCommand,
        cancellation: CancellationToken,
    ) -> Result<CommandResult, ProcessError> {
        if cancellation.is_cancelled() {
            return Ok(CommandResult::cancelled_before_spawn());
        }
        if command.timeout() > self.limits.max_command_timeout {
            return Err(ProcessError::TimeoutOutsideLimit);
        }

        let execution = self.start(command, cancellation).await?;
        execution.wait().await
    }

    async fn start(
        &self,
        command: ValidatedCommand,
        cancellation: CancellationToken,
    ) -> Result<ProcessExecution, ProcessError> {
        let started = Instant::now();
        // This is process-global because Darwin's pipe()+fcntl fallback cannot
        // atomically create CLOEXEC descriptors. Runtime-owned spawns are
        // serialized across prepare -> spawn so they cannot inherit another
        // command's transient sentinel descriptors.
        let spawn_guard = acquire_process_spawn_lock();
        command
            .executable()
            .revalidate()
            .map_err(ProcessError::CommandPolicy)?;
        command
            .working_directory()
            .revalidate()
            .map_err(ProcessError::CommandPolicy)?;
        for executable in command.dependent_executables() {
            executable
                .revalidate()
                .map_err(ProcessError::CommandPolicy)?;
        }
        for directory in command.dependent_directories() {
            directory
                .revalidate()
                .map_err(ProcessError::CommandPolicy)?;
        }
        let executable_file = command
            .executable()
            .cloned_file()
            .map_err(ProcessError::CommandPolicy)?;
        let working_directory = command
            .working_directory()
            .cloned_directory()
            .map_err(ProcessError::CommandPolicy)?;
        let dependent_directories = command
            .dependent_directories()
            .iter()
            .map(|directory| directory.cloned_directory())
            .collect::<Result<Vec<_>, _>>()
            .map_err(ProcessError::CommandPolicy)?;
        let executable = platform::Executable::new(command.executable().path(), executable_file)
            .map_err(ProcessError::TreeSetupFailed)?;
        let mut process = Command::new(executable.program());
        process
            .args(command.arguments())
            .env_clear()
            .envs(command.environment().entries())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        if let Some(argv0) = command.unix_argv0() {
            use std::os::unix::process::CommandExt as _;
            process.as_std_mut().arg0(argv0);
        }
        #[cfg(windows)]
        process.current_dir(command.working_directory().path());

        #[cfg(windows)]
        let working_directory_path_leases = command
            .working_directory()
            .acquire_spawn_path_leases()
            .map_err(ProcessError::CommandPolicy)?;
        #[cfg(windows)]
        let dependent_directory_path_leases = command
            .dependent_directories()
            .iter()
            .map(|directory| directory.acquire_spawn_path_leases())
            .collect::<Result<Vec<_>, _>>()
            .map_err(ProcessError::CommandPolicy)?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        #[cfg(unix)]
        let prepared = platform::prepare(
            &mut process,
            executable,
            working_directory,
            dependent_directories,
        )
        .map_err(ProcessError::TreeSetupFailed)?;
        #[cfg(windows)]
        let prepared = platform::prepare(
            &mut process,
            executable,
            working_directory,
            working_directory_path_leases,
            dependent_directories,
            dependent_directory_path_leases,
        )
        .map_err(ProcessError::TreeSetupFailed)?;
        let mut child = process.spawn().map_err(ProcessError::SpawnFailed)?;
        drop(spawn_guard);
        let (Some(stdout), Some(stderr)) = (child.stdout.take(), child.stderr.take()) else {
            cleanup_failed_spawn(child, self.limits.cleanup_timeout, self.tasks.clone()).await?;
            return Err(ProcessError::MissingOutputPipe);
        };

        let tree = match prepared.attach_and_resume(&child) {
            Ok(attached) => attached,
            Err(error) => {
                drop(stdout);
                drop(stderr);
                cleanup_failed_spawn(child, self.limits.cleanup_timeout, self.tasks.clone())
                    .await?;
                return Err(ProcessError::TreeSetupFailed(error));
            }
        };
        let stdout_task = tokio::spawn(drain_stream(stdout, self.limits.stdout_bytes));
        let stderr_task = tokio::spawn(drain_stream(stderr, self.limits.stderr_bytes));
        let external_tree = tree.0.clone();
        let abandonment = CancellationToken::new();
        let limits = self.limits;
        let timeout = command.timeout();
        let worker_tasks = self.tasks.clone();
        let worker_abandonment = abandonment.clone();
        let worker = self.tasks.spawn(async move {
            supervise_child(
                child,
                stdout_task,
                stderr_task,
                tree.0,
                tree.1,
                limits,
                timeout,
                cancellation,
                worker_abandonment,
                worker_tasks,
                started,
            )
            .await
        });

        Ok(ProcessExecution {
            worker: Some(worker),
            tree: external_tree,
            abandonment,
        })
    }

    pub(crate) async fn shutdown(&self) {
        self.tasks.close();
        self.tasks.wait().await;
    }
}

struct ProcessExecution {
    worker: Option<JoinHandle<Result<CommandResult, ProcessError>>>,
    tree: TreeKillHandle,
    abandonment: CancellationToken,
}

impl ProcessExecution {
    async fn wait(mut self) -> Result<CommandResult, ProcessError> {
        let worker = self.worker.take().expect("process worker is present");
        worker.await.map_err(|_| ProcessError::WorkerFailed)?
    }
}

impl Drop for ProcessExecution {
    fn drop(&mut self) {
        self.abandonment.cancel();
        let _ = self.tree.kill_now();
    }
}

enum ObservedTermination {
    Exited(Option<ExitStatus>),
    Cancelled,
    TimedOut,
    AnchorLost(io::Error),
    WaitFailed(io::Error),
}

#[cfg(target_os = "macos")]
fn should_use_exited_tree_kill(observed: &ObservedTermination) -> bool {
    matches!(observed, ObservedTermination::Exited(_))
}

#[cfg(target_os = "macos")]
fn reconcile_exited_tree_kill(
    kill_result: io::Result<()>,
    liveness_probe: impl FnOnce() -> io::Result<bool>,
) -> io::Result<()> {
    match kill_result {
        Err(kill_error) if kill_error.raw_os_error() == Some(libc::EPERM) => {
            match liveness_probe() {
                Ok(true) => Ok(()),
                Ok(false) => Err(kill_error),
                Err(probe_error) => Err(probe_error),
            }
        }
        result => result,
    }
}

#[allow(clippy::too_many_arguments)]
async fn supervise_child(
    mut child: Child,
    stdout_task: JoinHandle<io::Result<CapturedStream>>,
    stderr_task: JoinHandle<io::Result<CapturedStream>>,
    tree: TreeKillHandle,
    mut leader_exit: platform::LeaderExit,
    limits: ProcessLimits,
    timeout: Duration,
    cancellation: CancellationToken,
    abandonment: CancellationToken,
    tasks: TaskTracker,
    started: Instant,
) -> Result<CommandResult, ProcessError> {
    let _worker_guard = TreeWorkerGuard(tree.clone());
    let deadline = TokioInstant::now() + timeout;
    let observed = tokio::select! {
        biased;

        _ = cancellation.cancelled() => ObservedTermination::Cancelled,
        _ = abandonment.cancelled() => ObservedTermination::Cancelled,
        status = leader_exit.wait(&mut child) => match status {
            Ok(_status) if cancellation.is_cancelled() => ObservedTermination::Cancelled,
            Ok(status) => ObservedTermination::Exited(status),
            Err(error) if platform::leader_anchor_lost(&error) => {
                ObservedTermination::AnchorLost(error)
            }
            Err(error) => ObservedTermination::WaitFailed(error),
        },
        _ = time::sleep_until(deadline) => deadline_observation(&cancellation),
    };

    let observed = match observed {
        ObservedTermination::AnchorLost(error) => {
            // A wildcard/external waiter consumed the Unix leader. The PGID is
            // no longer identity-bound, so fail without sending a group signal
            // that could target a reused identifier.
            tree.disarm_without_kill();
            let _ = child.start_kill();
            abort_and_join_drains(stdout_task, stderr_task).await;
            return Err(ProcessError::WaitFailed(error));
        }
        observed => observed,
    };

    // Unix observes exit with waitid(WNOWAIT), so the group leader remains a
    // non-reusable PID anchor until this process-group kill has completed. XNU
    // filters SZOMB members while resolving a negative-PID kill, so a group with
    // only the waitable leader left can report EPERM. In that exact macOS case,
    // an EOF sentinel proves that every protocol-participating process exited.
    #[cfg(target_os = "macos")]
    let kill_error = if should_use_exited_tree_kill(&observed) {
        tree.kill_now_after_observed_exit(&leader_exit).err()
    } else {
        tree.kill_now().err()
    };
    #[cfg(not(target_os = "macos"))]
    let kill_error = tree.kill_now().err();
    if kill_error.is_some() {
        let _ = child.start_kill();
    }
    let cleanup_deadline = TokioInstant::now() + limits.cleanup_timeout;
    if kill_error.is_none() {
        match time::timeout_at(cleanup_deadline, leader_exit.wait_tree_before_reap()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                abort_and_join_drains(stdout_task, stderr_task).await;
                handoff_tree_reap(tasks, child, tree.clone(), leader_exit);
                return Err(ProcessError::TreeCleanupFailed(error));
            }
            Err(_) => {
                abort_and_join_drains(stdout_task, stderr_task).await;
                handoff_tree_reap(tasks, child, tree.clone(), leader_exit);
                return Err(ProcessError::CleanupTimedOut);
            }
        }
    }
    let status = match &observed {
        ObservedTermination::Exited(Some(status)) => status.to_owned(),
        _ => match time::timeout_at(cleanup_deadline, child.wait()).await {
            Ok(Ok(status)) => status,
            Ok(Err(error)) => {
                abort_and_join_drains(stdout_task, stderr_task).await;
                handoff_child_reap(tasks, child, tree.clone());
                return Err(ProcessError::WaitFailed(error));
            }
            Err(_) => {
                abort_and_join_drains(stdout_task, stderr_task).await;
                handoff_child_reap(tasks, child, tree.clone());
                return Err(ProcessError::CleanupTimedOut);
            }
        },
    };

    if kill_error.is_none() {
        match time::timeout_at(cleanup_deadline, leader_exit.wait_tree_after_reap()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                abort_and_join_drains(stdout_task, stderr_task).await;
                return Err(ProcessError::TreeCleanupFailed(error));
            }
            Err(_) => {
                abort_and_join_drains(stdout_task, stderr_task).await;
                return Err(ProcessError::CleanupTimedOut);
            }
        }
    }

    let (stdout, stderr) = collect_drains_until(cleanup_deadline, stdout_task, stderr_task).await?;

    if let Some(error) = kill_error {
        return Err(ProcessError::TreeCleanupFailed(error));
    }
    if let ObservedTermination::WaitFailed(error) = observed {
        return Err(ProcessError::WaitFailed(error));
    }

    let cancelled = matches!(observed, ObservedTermination::Cancelled);
    let timed_out = matches!(observed, ObservedTermination::TimedOut);
    let truncated = stdout.truncated || stderr.truncated;
    Ok(CommandResult {
        exit_code: status.code(),
        signal: exit_signal(&status),
        timed_out,
        cancelled,
        stdout,
        stderr,
        truncated,
        duration_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
    })
}

fn deadline_observation(cancellation: &CancellationToken) -> ObservedTermination {
    if cancellation.is_cancelled() {
        ObservedTermination::Cancelled
    } else {
        ObservedTermination::TimedOut
    }
}

async fn collect_drains_until(
    deadline: TokioInstant,
    mut stdout_task: JoinHandle<io::Result<CapturedStream>>,
    mut stderr_task: JoinHandle<io::Result<CapturedStream>>,
) -> Result<(CapturedStream, CapturedStream), ProcessError> {
    match time::timeout_at(deadline, async {
        let (stdout, stderr) = tokio::join!(&mut stdout_task, &mut stderr_task);
        Ok::<_, ProcessError>((join_captured(stdout)?, join_captured(stderr)?))
    })
    .await
    {
        Ok(result) => result,
        Err(_) => {
            abort_and_join_drains(stdout_task, stderr_task).await;
            Err(ProcessError::CleanupTimedOut)
        }
    }
}

async fn abort_and_join_drains(
    stdout_task: JoinHandle<io::Result<CapturedStream>>,
    stderr_task: JoinHandle<io::Result<CapturedStream>>,
) {
    stdout_task.abort();
    stderr_task.abort();
    let _ = tokio::join!(stdout_task, stderr_task);
}

fn handoff_child_reap(tasks: TaskTracker, mut child: Child, tree: TreeKillHandle) {
    tasks.spawn(async move {
        let _guard = TreeWorkerGuard(tree);
        let _ = child.start_kill();
        let _ = child.wait().await;
    });
}

fn handoff_tree_reap(
    tasks: TaskTracker,
    mut child: Child,
    tree: TreeKillHandle,
    mut leader_exit: platform::LeaderExit,
) {
    tasks.spawn(async move {
        let _guard = TreeWorkerGuard(tree);
        let _ = leader_exit.wait_tree_before_reap().await;
        let _ = child.start_kill();
        let _ = child.wait().await;
    });
}

fn process_spawn_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

async fn cleanup_failed_spawn(
    mut child: Child,
    cleanup_timeout: Duration,
    tasks: TaskTracker,
) -> Result<(), ProcessError> {
    let _ = child.start_kill();
    match time::timeout(cleanup_timeout, child.wait()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(ProcessError::WaitFailed(error)),
        Err(_) => {
            tasks.spawn(async move {
                let _ = child.start_kill();
                let _ = child.wait().await;
            });
            Err(ProcessError::CleanupTimedOut)
        }
    }
}

fn join_captured(
    result: Result<io::Result<CapturedStream>, JoinError>,
) -> Result<CapturedStream, ProcessError> {
    match result {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(error)) => Err(ProcessError::OutputDrainFailed(error)),
        Err(_) => Err(ProcessError::WorkerFailed),
    }
}

async fn drain_stream(
    mut stream: impl AsyncRead + Unpin,
    limit: usize,
) -> io::Result<CapturedStream> {
    let mut capture = HeadTailCapture::new(limit);
    let mut buffer = [0u8; 8 * 1_024];
    loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Ok(capture.finish());
        }
        capture.push(&buffer[..read]);
    }
}

struct HeadTailCapture {
    head: Vec<u8>,
    tail: VecDeque<u8>,
    head_capacity: usize,
    tail_capacity: usize,
    observed_bytes: u64,
}

impl HeadTailCapture {
    fn new(limit: usize) -> Self {
        let head_capacity = if limit == 1 { 1 } else { limit / 2 };
        Self {
            head: Vec::with_capacity(head_capacity),
            tail: VecDeque::with_capacity(limit - head_capacity),
            head_capacity,
            tail_capacity: limit - head_capacity,
            observed_bytes: 0,
        }
    }

    fn push(&mut self, bytes: &[u8]) {
        self.observed_bytes = self.observed_bytes.saturating_add(bytes.len() as u64);
        for byte in bytes {
            if self.head.len() < self.head_capacity {
                self.head.push(*byte);
            } else if self.tail_capacity != 0 {
                if self.tail.len() == self.tail_capacity {
                    self.tail.pop_front();
                }
                self.tail.push_back(*byte);
            }
        }
    }

    fn finish(mut self) -> CapturedStream {
        let retained = self.head.len().saturating_add(self.tail.len());
        let truncated = self.observed_bytes > retained as u64;
        if !truncated {
            self.head.extend(self.tail.drain(..));
        }
        CapturedStream {
            head: self.head,
            tail: self.tail.into_iter().collect(),
            observed_bytes: self.observed_bytes,
            omitted_observed_bytes: self.observed_bytes.saturating_sub(retained as u64),
            truncated,
            complete: true,
        }
    }
}

#[derive(Clone)]
struct TreeKillHandle {
    inner: Arc<TreeKillInner>,
}

struct TreeKillInner {
    platform: platform::ProcessTree,
    outcome: OnceLock<Result<(), KillFailure>>,
}

impl TreeKillHandle {
    fn new(platform: platform::ProcessTree) -> Self {
        Self {
            inner: Arc::new(TreeKillInner {
                platform,
                outcome: OnceLock::new(),
            }),
        }
    }

    fn kill_now(&self) -> io::Result<()> {
        self.inner
            .outcome
            .get_or_init(|| self.inner.platform.kill().map_err(KillFailure::from))
            .clone()
            .map_err(KillFailure::into_io_error)
    }

    #[cfg(target_os = "macos")]
    fn kill_now_after_observed_exit(&self, leader_exit: &platform::LeaderExit) -> io::Result<()> {
        self.inner
            .outcome
            .get_or_init(|| {
                reconcile_exited_tree_kill(self.inner.platform.kill(), || {
                    leader_exit.liveness_pipe_has_no_writers_now()
                })
                .map_err(KillFailure::from)
            })
            .clone()
            .map_err(KillFailure::into_io_error)
    }

    fn disarm_without_kill(&self) {
        let _ = self.inner.outcome.set(Ok(()));
    }

    #[cfg(all(test, windows))]
    fn active_processes_for_test(&self) -> io::Result<u32> {
        self.inner.platform.active_processes()
    }
}

#[derive(Debug, Clone)]
struct KillFailure {
    kind: io::ErrorKind,
    raw_os_error: Option<i32>,
}

impl From<io::Error> for KillFailure {
    fn from(error: io::Error) -> Self {
        Self {
            kind: error.kind(),
            raw_os_error: error.raw_os_error(),
        }
    }
}

impl KillFailure {
    fn into_io_error(self) -> io::Error {
        self.raw_os_error
            .map(io::Error::from_raw_os_error)
            .unwrap_or_else(|| io::Error::from(self.kind))
    }
}

struct TreeWorkerGuard(TreeKillHandle);

impl Drop for TreeWorkerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill_now();
    }
}

#[cfg(unix)]
fn exit_signal(status: &ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    status.signal()
}

#[cfg(windows)]
fn exit_signal(_: &ExitStatus) -> Option<i32> {
    None
}

#[cfg(unix)]
mod platform {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    use super::*;
    use tokio::signal::unix::{Signal, SignalKind};

    pub(super) struct Executable {
        file: File,
        program: PathBuf,
    }

    impl Executable {
        pub(super) fn new(path: &std::path::Path, file: File) -> io::Result<Self> {
            let file = normalize_inherited_file(file)?;
            #[cfg(target_os = "macos")]
            let program = path.to_owned();
            #[cfg(not(target_os = "macos"))]
            let program = {
                let _ = path;
                #[cfg(any(target_os = "linux", target_os = "android"))]
                let descriptor_root = std::path::Path::new("/proc/self/fd");
                #[cfg(not(any(target_os = "linux", target_os = "android")))]
                let descriptor_root = std::path::Path::new("/dev/fd");
                if !descriptor_root.is_dir() {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "descriptor-backed executable namespace is unavailable",
                    ));
                }
                descriptor_root.join(file.as_raw_fd().to_string())
            };
            Ok(Self { file, program })
        }

        pub(super) fn program(&self) -> &std::path::Path {
            &self.program
        }
    }

    pub(super) struct Prepared {
        sigchld: Signal,
        liveness_read: OwnedFd,
        liveness_write: OwnedFd,
    }

    pub(super) struct LeaderExit {
        process_id: libc::id_t,
        sigchld: Signal,
        liveness_read: OwnedFd,
    }

    pub(super) struct ProcessTree {
        process_group: i32,
    }

    pub(super) fn prepare(
        command: &mut Command,
        executable: Executable,
        working_directory: File,
        dependent_directories: Vec<File>,
    ) -> io::Result<Prepared> {
        let sigchld = tokio::signal::unix::signal(SignalKind::child())?;
        let (liveness_read, liveness_write) = create_liveness_pipe()?;
        let inherited_write = liveness_write.as_raw_fd();
        #[cfg(not(target_os = "macos"))]
        let executable_descriptor = executable.file.as_raw_fd();
        let working_directory = normalize_inherited_file(working_directory)?;
        let working_directory_descriptor = working_directory.as_raw_fd();
        unsafe {
            command.pre_exec(move || {
                // Keep both owned descriptors captured until this closure has
                // run in the child. The executable descriptor must survive the
                // following exec long enough for descriptor-backed Unix paths
                // to resolve it. macOS retains the same revalidated handle until
                // path-based exec and leaves it CLOEXEC so it is not exposed to
                // the launched tool. The cwd is selected from its retained
                // capability.
                let _executable = &executable.file;
                let _working_directory = &working_directory;
                let _dependent_directories = &dependent_directories;
                if libc::fchdir(working_directory_descriptor) != 0 {
                    return Err(io::Error::last_os_error());
                }
                #[cfg(not(target_os = "macos"))]
                clear_close_on_exec(executable_descriptor)?;
                clear_close_on_exec(inherited_write)
            });
        }
        command.process_group(0);
        Ok(Prepared {
            sigchld,
            liveness_read,
            liveness_write,
        })
    }

    pub(super) fn leader_anchor_lost(error: &io::Error) -> bool {
        error.raw_os_error() == Some(libc::ECHILD)
    }

    impl Prepared {
        pub(super) fn attach_and_resume(
            self,
            child: &Child,
        ) -> io::Result<(TreeKillHandle, LeaderExit)> {
            let Self {
                sigchld,
                liveness_read,
                liveness_write,
            } = self;
            let process_group = child
                .id()
                .and_then(|id| i32::try_from(id).ok())
                .ok_or_else(|| io::Error::other("spawned process has no valid process group"))?;
            let attached = (
                TreeKillHandle::new(ProcessTree { process_group }),
                LeaderExit {
                    process_id: process_group as libc::id_t,
                    sigchld,
                    liveness_read,
                },
            );
            drop(liveness_write);
            Ok(attached)
        }
    }

    impl LeaderExit {
        pub(super) async fn wait(&mut self, _child: &mut Child) -> io::Result<Option<ExitStatus>> {
            loop {
                if exit_is_waitable(self.process_id)? {
                    return Ok(None);
                }
                self.sigchld
                    .recv()
                    .await
                    .ok_or_else(|| io::Error::other("SIGCHLD listener closed"))?;
            }
        }

        pub(super) async fn wait_tree_before_reap(&mut self) -> io::Result<()> {
            loop {
                if liveness_pipe_has_no_writers(&self.liveness_read)? {
                    return Ok(());
                }
                time::sleep(Duration::from_millis(2)).await;
            }
        }

        #[cfg(target_os = "macos")]
        pub(super) fn liveness_pipe_has_no_writers_now(&self) -> io::Result<bool> {
            liveness_pipe_has_no_writers(&self.liveness_read)
        }

        pub(super) async fn wait_tree_after_reap(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    pub(super) fn create_liveness_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
        let mut descriptors = [-1; 2];
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let result =
            unsafe { libc::pipe2(descriptors.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        let result = unsafe { libc::pipe(descriptors.as_mut_ptr()) };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        let read = match normalize_sentinel_descriptor(descriptors[0]) {
            Ok(read) => read,
            Err(error) => {
                let _ = unsafe { libc::close(descriptors[1]) };
                return Err(error);
            }
        };
        let write = normalize_sentinel_descriptor(descriptors[1])?;
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        {
            set_close_on_exec(read.as_raw_fd())?;
            set_close_on_exec(write.as_raw_fd())?;
            set_nonblocking(read.as_raw_fd())?;
        }
        Ok((read, write))
    }

    fn normalize_sentinel_descriptor(descriptor: i32) -> io::Result<OwnedFd> {
        let original = unsafe { OwnedFd::from_raw_fd(descriptor) };
        if descriptor > libc::STDERR_FILENO {
            return Ok(original);
        }
        let duplicate =
            unsafe { libc::fcntl(descriptor, libc::F_DUPFD_CLOEXEC, libc::STDERR_FILENO + 1) };
        if duplicate == -1 {
            return Err(io::Error::last_os_error());
        }
        drop(original);
        Ok(unsafe { OwnedFd::from_raw_fd(duplicate) })
    }

    fn normalize_inherited_file(file: File) -> io::Result<File> {
        let descriptor = file.as_raw_fd();
        if descriptor > libc::STDERR_FILENO {
            return Ok(file);
        }
        let duplicate =
            unsafe { libc::fcntl(descriptor, libc::F_DUPFD_CLOEXEC, libc::STDERR_FILENO + 1) };
        if duplicate == -1 {
            return Err(io::Error::last_os_error());
        }
        drop(file);
        Ok(unsafe { File::from_raw_fd(duplicate) })
    }

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    fn set_close_on_exec(descriptor: i32) -> io::Result<()> {
        update_descriptor_flags(descriptor, |flags| flags | libc::FD_CLOEXEC)
    }

    fn clear_close_on_exec(descriptor: i32) -> io::Result<()> {
        update_descriptor_flags(descriptor, |flags| flags & !libc::FD_CLOEXEC)
    }

    fn update_descriptor_flags(descriptor: i32, update: impl FnOnce(i32) -> i32) -> io::Result<()> {
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        if flags == -1 {
            return Err(io::Error::last_os_error());
        }
        if unsafe { libc::fcntl(descriptor, libc::F_SETFD, update(flags)) } == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    fn set_nonblocking(descriptor: i32) -> io::Result<()> {
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
        if flags == -1 {
            return Err(io::Error::last_os_error());
        }
        if unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub(super) fn liveness_pipe_has_no_writers(read: &OwnedFd) -> io::Result<bool> {
        let mut byte = 0u8;
        loop {
            let read_count = unsafe {
                libc::read(
                    read.as_raw_fd(),
                    (&mut byte as *mut u8).cast::<libc::c_void>(),
                    1,
                )
            };
            if read_count == 0 {
                return Ok(true);
            }
            if read_count > 0 {
                continue;
            }
            let error = io::Error::last_os_error();
            return match error.kind() {
                io::ErrorKind::Interrupted => continue,
                io::ErrorKind::WouldBlock => Ok(false),
                _ => Err(error),
            };
        }
    }

    fn exit_is_waitable(process_id: libc::id_t) -> io::Result<bool> {
        loop {
            let mut information = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
            let result = unsafe {
                libc::waitid(
                    libc::P_PID,
                    process_id,
                    information.as_mut_ptr(),
                    libc::WEXITED | libc::WNOWAIT | libc::WNOHANG,
                )
            };
            if result == 0 {
                let information = unsafe { information.assume_init() };
                return Ok(unsafe { information.si_pid() } != 0);
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }

    impl ProcessTree {
        pub(super) fn kill(&self) -> io::Result<()> {
            let result = unsafe { libc::kill(-self.process_group, libc::SIGKILL) };
            if result == 0 {
                Ok(())
            } else {
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::ESRCH) {
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }
}

#[cfg(windows)]
mod platform {
    use std::ffi::c_void;
    use std::mem::size_of;
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
    use std::ptr::null;

    use windows_sys::Win32::Foundation::{HANDLE, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
    };
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
        QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
    };
    use windows_sys::Win32::System::Threading::{
        CREATE_NO_WINDOW, CREATE_SUSPENDED, OpenThread, ResumeThread, THREAD_SUSPEND_RESUME,
    };

    use super::*;

    pub(super) struct Executable {
        file: File,
        program: PathBuf,
    }

    impl Executable {
        pub(super) fn new(path: &std::path::Path, file: File) -> io::Result<Self> {
            Ok(Self {
                file,
                program: path.to_owned(),
            })
        }

        pub(super) fn program(&self) -> &std::path::Path {
            &self.program
        }
    }

    pub(super) struct Prepared {
        job: Arc<OwnedHandle>,
        executable_lease: File,
        working_directory_lease: File,
        working_directory_path_leases: Vec<File>,
        dependent_directory_leases: Vec<File>,
        dependent_directory_path_leases: Vec<File>,
    }

    pub(super) struct LeaderExit {
        job: Arc<OwnedHandle>,
        _dependent_directory_leases: Vec<File>,
        _dependent_directory_path_leases: Vec<File>,
    }

    pub(super) struct ProcessTree {
        job: Arc<OwnedHandle>,
    }

    pub(super) fn prepare(
        command: &mut Command,
        executable: Executable,
        working_directory: File,
        working_directory_path_leases: Vec<File>,
        dependent_directory_leases: Vec<File>,
        dependent_directory_path_leases: Vec<File>,
    ) -> io::Result<Prepared> {
        let job = unsafe { CreateJobObjectW(null(), null()) };
        if job.is_null() {
            return Err(io::Error::last_os_error());
        }
        let job = Arc::new(unsafe { OwnedHandle::from_raw_handle(job) });
        let mut information = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        information.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let succeeded = unsafe {
            SetInformationJobObject(
                job.as_raw_handle() as HANDLE,
                JobObjectExtendedLimitInformation,
                (&information as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast::<c_void>(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        if succeeded == 0 {
            return Err(io::Error::last_os_error());
        }
        command.creation_flags(CREATE_SUSPENDED | CREATE_NO_WINDOW);
        Ok(Prepared {
            job,
            executable_lease: executable.file,
            working_directory_lease: working_directory,
            working_directory_path_leases,
            dependent_directory_leases,
            dependent_directory_path_leases,
        })
    }

    pub(super) fn leader_anchor_lost(_: &io::Error) -> bool {
        false
    }

    impl Prepared {
        pub(super) fn attach_and_resume(
            self,
            child: &Child,
        ) -> io::Result<(TreeKillHandle, LeaderExit)> {
            let Self {
                job,
                executable_lease,
                working_directory_lease,
                working_directory_path_leases,
                dependent_directory_leases,
                dependent_directory_path_leases,
            } = self;
            let process = child
                .raw_handle()
                .ok_or_else(|| io::Error::other("spawned process has no process handle"))?
                as HANDLE;
            if unsafe { AssignProcessToJobObject(job.as_raw_handle() as HANDLE, process) } == 0 {
                return Err(io::Error::last_os_error());
            }
            let process_id = child
                .id()
                .ok_or_else(|| io::Error::other("spawned process has no process id"))?;
            resume_only_thread(process_id)?;
            drop(executable_lease);
            drop(working_directory_lease);
            drop(working_directory_path_leases);
            Ok((
                TreeKillHandle::new(ProcessTree { job: job.clone() }),
                LeaderExit {
                    job,
                    _dependent_directory_leases: dependent_directory_leases,
                    _dependent_directory_path_leases: dependent_directory_path_leases,
                },
            ))
        }
    }

    impl LeaderExit {
        pub(super) async fn wait(&mut self, child: &mut Child) -> io::Result<Option<ExitStatus>> {
            child.wait().await.map(Some)
        }

        pub(super) async fn wait_tree_before_reap(&mut self) -> io::Result<()> {
            Ok(())
        }

        pub(super) async fn wait_tree_after_reap(&mut self) -> io::Result<()> {
            loop {
                if active_processes(&self.job)? == 0 {
                    return Ok(());
                }
                time::sleep(Duration::from_millis(2)).await;
            }
        }
    }

    fn active_processes(job: &OwnedHandle) -> io::Result<u32> {
        let mut information = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
        let succeeded = unsafe {
            QueryInformationJobObject(
                job.as_raw_handle() as HANDLE,
                JobObjectBasicAccountingInformation,
                (&mut information as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast::<c_void>(),
                size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                std::ptr::null_mut(),
            )
        };
        if succeeded == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(information.ActiveProcesses)
        }
    }

    fn resume_only_thread(process_id: u32) -> io::Result<()> {
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if snapshot == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        let snapshot = unsafe { OwnedHandle::from_raw_handle(snapshot) };
        let mut entry = THREADENTRY32 {
            dwSize: size_of::<THREADENTRY32>() as u32,
            ..THREADENTRY32::default()
        };
        let mut succeeded =
            unsafe { Thread32First(snapshot.as_raw_handle() as HANDLE, &mut entry) };
        while succeeded != 0 {
            if entry.th32OwnerProcessID == process_id {
                let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
                if thread.is_null() {
                    return Err(io::Error::last_os_error());
                }
                let thread = unsafe { OwnedHandle::from_raw_handle(thread) };
                let previous = unsafe { ResumeThread(thread.as_raw_handle() as HANDLE) };
                if previous == u32::MAX {
                    return Err(io::Error::last_os_error());
                }
                if previous != 1 {
                    return Err(io::Error::other(
                        "suspended primary thread had an unexpected suspend count",
                    ));
                }
                return Ok(());
            }
            succeeded = unsafe { Thread32Next(snapshot.as_raw_handle() as HANDLE, &mut entry) };
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "suspended primary thread was not found",
        ))
    }

    impl ProcessTree {
        #[cfg(test)]
        pub(super) fn active_processes(&self) -> io::Result<u32> {
            active_processes(&self.job)
        }

        pub(super) fn kill(&self) -> io::Result<()> {
            if unsafe { TerminateJobObject(self.job.as_raw_handle() as HANDLE, 1) } == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::path::Path;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    use tempfile::TempDir;
    use tokio::io::ReadBuf;

    use super::*;

    const HELPER_ENV: &str = "CODING_AGENT_PROCESS_HELPER";
    const HELPER_PID_FILE: &str = "CODING_AGENT_PROCESS_HELPER_PID_FILE";
    const HELPER_TEST: &str = "process_supervisor::tests::process_helper_entrypoint";

    #[test]
    fn process_helper_entrypoint() {
        let Some(mode) = std::env::var_os(HELPER_ENV) else {
            return;
        };
        match mode.to_string_lossy().as_ref() {
            "split" => {
                print!("stdout-marker");
                eprint!("stderr-marker");
                flush_standard_streams();
                std::process::exit(0);
            }
            "exit-7" => std::process::exit(7),
            "flood" => {
                let stdout = std::thread::spawn(|| {
                    let mut output = std::io::stdout().lock();
                    output.write_all(b"stdout-head|").unwrap();
                    output.write_all(&vec![b'o'; 256 * 1_024]).unwrap();
                    output.write_all(b"|stdout-tail").unwrap();
                    output.flush().unwrap();
                });
                let stderr = std::thread::spawn(|| {
                    let mut output = std::io::stderr().lock();
                    output.write_all(b"stderr-head|").unwrap();
                    output.write_all(&vec![b'e'; 256 * 1_024]).unwrap();
                    output.write_all(b"|stderr-tail").unwrap();
                    output.flush().unwrap();
                });
                stdout.join().unwrap();
                stderr.join().unwrap();
                std::process::exit(0);
            }
            "binary" => {
                std::io::stdout()
                    .write_all(&[0xff, 0x00, 0xfe, 0x7f])
                    .unwrap();
                std::io::stderr().write_all(&[0x80, 0x81, 0x00]).unwrap();
                flush_standard_streams();
                std::process::exit(0);
            }
            "sleep" => {
                write_helper_pid();
                std::thread::sleep(Duration::from_secs(60));
                std::process::exit(0);
            }
            "leader" | "leader-closed-pipe" | "leader-sleep" => {
                let mut child = std::process::Command::new(std::env::current_exe().unwrap());
                child
                    .args(["--exact", HELPER_TEST, "--nocapture"])
                    .env(HELPER_ENV, "grandchild");
                if mode == "leader-closed-pipe" {
                    child.stdout(Stdio::null()).stderr(Stdio::null());
                }
                child.spawn().unwrap();
                wait_for_helper_pid_sync();
                if mode == "leader-sleep" {
                    std::thread::sleep(Duration::from_secs(60));
                }
                std::process::exit(0);
            }
            "grandchild" => {
                write_helper_pid();
                println!("grandchild-ready");
                std::io::stdout().flush().unwrap();
                std::thread::sleep(Duration::from_secs(60));
                std::process::exit(0);
            }
            "environment" => {
                for key in [
                    "CODING_AGENT_SENTINEL_SECRET",
                    "OPENAI_API_KEY",
                    "HTTP_PROXY",
                    "HTTPS_PROXY",
                    "ALL_PROXY",
                    "NO_PROXY",
                    "SSH_AUTH_SOCK",
                    "GITHUB_TOKEN",
                    "CI_JOB_TOKEN",
                    "AWS_SECRET_ACCESS_KEY",
                    "AZURE_CLIENT_SECRET",
                    "GOOGLE_APPLICATION_CREDENTIALS",
                    "GIT_ASKPASS",
                    "CARGO_REGISTRY_TOKEN",
                    "RUSTC_WRAPPER",
                    "RUSTFLAGS",
                    "LD_PRELOAD",
                    "DYLD_INSERT_LIBRARIES",
                ] {
                    println!("{key}={}", usize::from(std::env::var_os(key).is_some()));
                }
                println!(
                    "CARGO_NET_OFFLINE={}",
                    std::env::var("CARGO_NET_OFFLINE").unwrap_or_default()
                );
                flush_standard_streams();
                std::process::exit(0);
            }
            unexpected => panic!("unknown helper mode {unexpected}"),
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_uses_the_validated_executable_path_instead_of_dev_fd() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("pinned-tool");
        std::fs::write(&path, b"deterministic test image").unwrap();
        let path = std::fs::canonicalize(path).unwrap();
        let file = File::open(&path).unwrap();

        let executable = platform::Executable::new(&path, file).unwrap();

        assert_eq!(executable.program(), path);
        assert!(!executable.program().starts_with("/dev/fd"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_only_uses_the_exited_tree_kill_path_after_observed_leader_exit() {
        assert!(should_use_exited_tree_kill(&ObservedTermination::Exited(
            None
        )));
        assert!(!should_use_exited_tree_kill(
            &ObservedTermination::Cancelled
        ));
        assert!(!should_use_exited_tree_kill(&ObservedTermination::TimedOut));
        assert!(!should_use_exited_tree_kill(
            &ObservedTermination::WaitFailed(io::Error::other("wait failed"))
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_only_accepts_eof_after_an_exited_tree_kill_returns_eperm() {
        assert!(
            reconcile_exited_tree_kill(Err(io::Error::from_raw_os_error(libc::EPERM)), || Ok(true))
                .is_ok()
        );

        let writers_remain =
            reconcile_exited_tree_kill(Err(io::Error::from_raw_os_error(libc::EPERM)), || {
                Ok(false)
            })
            .unwrap_err();
        assert_eq!(writers_remain.raw_os_error(), Some(libc::EPERM));

        let probe_failed =
            reconcile_exited_tree_kill(Err(io::Error::from_raw_os_error(libc::EPERM)), || {
                Err(io::Error::from_raw_os_error(libc::EIO))
            })
            .unwrap_err();
        assert_eq!(probe_failed.raw_os_error(), Some(libc::EIO));

        let non_eperm =
            reconcile_exited_tree_kill(Err(io::Error::from_raw_os_error(libc::EINVAL)), || {
                panic!("non-EPERM failures must not probe liveness")
            })
            .unwrap_err();
        assert_eq!(non_eperm.raw_os_error(), Some(libc::EINVAL));

        reconcile_exited_tree_kill(Ok(()), || {
            panic!("successful kills must not probe liveness")
        })
        .unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_liveness_probe_distinguishes_live_writers_from_eof() {
        let (read, write) = platform::create_liveness_pipe().unwrap();

        assert!(!platform::liveness_pipe_has_no_writers(&read).unwrap());
        drop(write);
        assert!(platform::liveness_pipe_has_no_writers(&read).unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn separates_streams_reports_nonzero_and_drains_dual_pipe_floods() {
        let temp = tempfile::tempdir().unwrap();
        let supervisor = supervisor(512, Duration::from_secs(5));

        let split = supervisor
            .run(
                helper_command("split", &temp, Duration::from_secs(2)),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(split.exit_code, Some(0));
        assert!(contains(&split.stdout.head, b"stdout-marker"));
        assert!(!contains(&split.stdout.head, b"stderr-marker"));
        assert!(contains(&split.stderr.head, b"stderr-marker"));

        let nonzero = supervisor
            .run(
                helper_command("exit-7", &temp, Duration::from_secs(2)),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(nonzero.exit_code, Some(7));
        assert!(!nonzero.timed_out);
        assert!(!nonzero.cancelled);

        let flood = supervisor
            .run(
                helper_command("flood", &temp, Duration::from_secs(5)),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert!(flood.truncated);
        assert!(flood.stdout.observed_bytes > 256 * 1_024);
        assert!(flood.stderr.observed_bytes > 256 * 1_024);
        assert!(contains(&flood.stdout.head, b"stdout-head|"));
        assert!(flood.stdout.tail.ends_with(b"|stdout-tail"));
        assert!(contains(&flood.stderr.head, b"stderr-head|"));
        assert!(flood.stderr.tail.ends_with(b"|stderr-tail"));

        let binary = supervisor
            .run(
                helper_command("binary", &temp, Duration::from_secs(2)),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert!(contains(&binary.stdout.head, &[0xff, 0x00, 0xfe, 0x7f]));
        assert!(contains(&binary.stderr.head, &[0x80, 0x81, 0x00]));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pre_cancel_running_cancel_and_timeout_are_bounded_and_cancel_wins() {
        let temp = tempfile::tempdir().unwrap();
        let supervisor = supervisor(1_024, Duration::from_secs(5));
        let pid_file = temp.path().join("pid");

        let pre_cancelled = CancellationToken::new();
        pre_cancelled.cancel();
        let command = helper_command_with_pid("sleep", &temp, &pid_file, Duration::from_millis(1));
        let result = supervisor.run(command, pre_cancelled).await.unwrap();
        assert!(result.cancelled);
        assert!(!result.timed_out);
        assert!(!pid_file.exists());

        let cancellation = CancellationToken::new();
        let running_supervisor = supervisor.clone();
        let running = tokio::spawn({
            let cancellation = cancellation.clone();
            let command =
                helper_command_with_pid("sleep", &temp, &pid_file, Duration::from_secs(5));
            async move { running_supervisor.run(command, cancellation).await }
        });
        let process_id = wait_for_helper_pid(&pid_file).await;
        cancellation.cancel();
        let result = running.await.unwrap().unwrap();
        assert!(result.cancelled);
        assert!(!result.timed_out);
        wait_until_process_gone(process_id).await;

        let timeout_pid = temp.path().join("timeout-pid");
        let result = supervisor
            .run(
                helper_command_with_pid("sleep", &temp, &timeout_pid, Duration::from_millis(50)),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert!(result.timed_out);
        assert!(!result.cancelled);
        wait_until_process_gone(wait_for_helper_pid(&timeout_pid).await).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn leader_exit_and_aborted_supervisor_both_kill_the_entire_tree() {
        let temp = tempfile::tempdir().unwrap();
        let supervisor = supervisor(1_024, Duration::from_secs(5));

        for mode in ["leader", "leader-closed-pipe"] {
            let pid_file = temp.path().join(format!("{mode}-pid"));
            let execution = supervisor
                .start(
                    helper_command_with_pid(mode, &temp, &pid_file, Duration::from_secs(5)),
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            #[cfg(windows)]
            let tree = execution.tree.clone();
            let result = execution.wait().await.unwrap();
            assert_eq!(result.exit_code, Some(0));
            let process_id = wait_for_helper_pid(&pid_file).await;
            if mode == "leader-closed-pipe" {
                #[cfg(unix)]
                assert!(
                    !process_is_running(process_id),
                    "supervisor returned before the closed-pipe descendant terminated"
                );
                #[cfg(windows)]
                assert_eq!(tree.active_processes_for_test().unwrap(), 0);
            }
            wait_until_process_gone(process_id).await;
        }

        let abort_pid = temp.path().join("abort-pid");
        let running_supervisor = supervisor.clone();
        let execution = tokio::spawn({
            let command =
                helper_command_with_pid("leader-sleep", &temp, &abort_pid, Duration::from_secs(5));
            async move {
                running_supervisor
                    .run(command, CancellationToken::new())
                    .await
            }
        });
        let process_id = wait_for_helper_pid(&abort_pid).await;
        execution.abort();
        let _ = execution.await;
        wait_until_process_gone(process_id).await;
        supervisor.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn child_environment_is_allowlist_built_and_clears_sensitive_variables() {
        let temp = tempfile::tempdir().unwrap();
        let environment = ChildEnvironment::for_platform(&platform_environment(&temp));
        let sensitive_keys = [
            "CODING_AGENT_SENTINEL_SECRET",
            "OPENAI_API_KEY",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "NO_PROXY",
            "SSH_AUTH_SOCK",
            "GITHUB_TOKEN",
            "CI_JOB_TOKEN",
            "AWS_SECRET_ACCESS_KEY",
            "AZURE_CLIENT_SECRET",
            "GOOGLE_APPLICATION_CREDENTIALS",
            "GIT_ASKPASS",
            "CARGO_REGISTRY_TOKEN",
            "RUSTC_WRAPPER",
            "RUSTFLAGS",
            "LD_PRELOAD",
            "DYLD_INSERT_LIBRARIES",
        ];
        for key in sensitive_keys {
            assert!(!environment.entries.contains_key(&OsString::from(key)));
        }
        let command = helper_command_with_environment(
            "environment",
            &temp,
            Duration::from_secs(2),
            environment,
            None,
        );

        let result = supervisor(4_096, Duration::from_secs(5))
            .run(command, CancellationToken::new())
            .await
            .unwrap();
        let stdout = String::from_utf8(result.stdout.head).unwrap();
        for key in sensitive_keys {
            assert!(stdout.contains(&format!("{key}=0")), "{stdout}");
        }
        assert!(stdout.contains("CARGO_NET_OFFLINE=true"));
    }

    #[test]
    fn rust_toolchain_environment_has_an_exact_typed_allowlist() {
        let temp = tempfile::tempdir().unwrap();
        let compiler = std::env::current_exe().unwrap();
        let toolchain = RustToolchainEnvironment::try_new(
            vec![temp.path().to_path_buf()],
            temp.path().to_path_buf(),
            Some(temp.path().to_path_buf()),
            compiler.clone(),
            compiler.clone(),
        )
        .unwrap();
        let platform = platform_environment(&temp);
        let environment = ChildEnvironment::for_rust_toolchain(&platform, &toolchain);
        let mut actual = environment
            .entries
            .keys()
            .map(|key| key.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let mut expected = vec![
            "CARGO_HOME",
            "CARGO_NET_OFFLINE",
            "CARGO_TERM_COLOR",
            "LANG",
            "LC_ALL",
            "PATH",
            "RUSTC",
            "RUSTDOC",
            "RUSTUP_HOME",
            "RUST_BACKTRACE",
        ];
        #[cfg(windows)]
        expected.extend(["SYSTEMROOT", "TEMP", "TMP", "WINDIR"]);
        #[cfg(unix)]
        expected.push("TMPDIR");
        actual.sort();
        expected.sort_unstable();
        assert_eq!(actual, expected);
        assert_eq!(environment.entries[&OsString::from("RUSTC")], compiler);
        assert_eq!(environment.entries[&OsString::from("RUSTDOC")], compiler);
        assert!(
            !environment
                .entries
                .contains_key(&OsString::from("CODING_AGENT_SENTINEL_SECRET"))
        );
        assert!(matches!(
            PlatformEnvironment::try_new(PathBuf::from("relative-temp"), None),
            Err(PlatformEnvironmentError::TempDirectory)
        ));
        #[cfg(windows)]
        assert!(matches!(
            PlatformEnvironment::try_new(temp.path().to_path_buf(), None),
            Err(PlatformEnvironmentError::SystemRoot)
        ));
        #[cfg(unix)]
        assert!(matches!(
            PlatformEnvironment::try_new(
                temp.path().to_path_buf(),
                Some(temp.path().to_path_buf())
            ),
            Err(PlatformEnvironmentError::SystemRoot)
        ));

        assert!(matches!(
            RustToolchainEnvironment::try_new(
                vec![PathBuf::from("relative")],
                temp.path().to_path_buf(),
                None,
                std::env::current_exe().unwrap(),
                std::env::current_exe().unwrap(),
            ),
            Err(ToolchainEnvironmentError::Directory)
        ));
        assert!(matches!(
            RustToolchainEnvironment::try_new(
                vec![temp.path().to_path_buf()],
                temp.path().to_path_buf(),
                None,
                temp.path().join("missing-rustc"),
                std::env::current_exe().unwrap(),
            ),
            Err(ToolchainEnvironmentError::Compiler)
        ));

        let separator = if cfg!(windows) { ';' } else { ':' };
        let invalid_path_entry = temp.path().join(format!("invalid{separator}entry"));
        std::fs::create_dir(&invalid_path_entry).unwrap();
        assert!(matches!(
            RustToolchainEnvironment::try_new(
                vec![invalid_path_entry],
                temp.path().to_path_buf(),
                None,
                std::env::current_exe().unwrap(),
                std::env::current_exe().unwrap(),
            ),
            Err(ToolchainEnvironmentError::SearchPath)
        ));
    }

    #[cfg(all(windows, target_env = "msvc"))]
    #[test]
    fn windows_msvc_environment_adds_only_pinned_toolchain_entries() {
        let temp = tempfile::tempdir().unwrap();
        let linker_directory = temp.path().join("msvc-bin");
        let library_directory = temp.path().join("msvc-lib");
        let include_directory = temp.path().join("msvc-include");
        for directory in [&linker_directory, &library_directory, &include_directory] {
            std::fs::create_dir(directory).unwrap();
        }
        let linker = linker_directory.join("link.exe");
        std::fs::copy(std::env::current_exe().unwrap(), &linker).unwrap();
        let compiler = std::env::current_exe().unwrap();
        let toolchain = RustToolchainEnvironment::try_new(
            vec![linker_directory.clone(), temp.path().to_path_buf()],
            temp.path().to_path_buf(),
            None,
            compiler.clone(),
            compiler,
        )
        .unwrap()
        .with_windows_msvc(
            WindowsMsvcEnvironment::try_new(
                linker.clone(),
                vec![library_directory.clone()],
                vec![include_directory.clone()],
            )
            .unwrap(),
        );

        let environment =
            ChildEnvironment::for_rust_toolchain(&platform_environment(&temp), &toolchain);
        let linker_key = OsString::from(cargo_linker_environment_key().unwrap());
        assert_eq!(environment.entries[&linker_key], linker);
        assert_eq!(
            std::env::split_paths(&environment.entries[&OsString::from("LIB")]).collect::<Vec<_>>(),
            vec![library_directory]
        );
        assert_eq!(
            std::env::split_paths(&environment.entries[&OsString::from("INCLUDE")])
                .collect::<Vec<_>>(),
            vec![include_directory]
        );
        assert_eq!(
            std::env::split_paths(&environment.entries[&OsString::from("PATH")])
                .collect::<Vec<_>>(),
            vec![linker_directory, temp.path().to_path_buf()]
        );

        let mut actual = environment
            .entries
            .keys()
            .map(|key| key.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let mut expected = vec![
            "CARGO_HOME",
            "CARGO_NET_OFFLINE",
            "CARGO_TERM_COLOR",
            cargo_linker_environment_key().unwrap(),
            "INCLUDE",
            "LANG",
            "LC_ALL",
            "LIB",
            "PATH",
            "RUSTC",
            "RUSTDOC",
            "RUST_BACKTRACE",
            "SYSTEMROOT",
            "TEMP",
            "TMP",
            "WINDIR",
        ];
        actual.sort();
        expected.sort_unstable();
        assert_eq!(actual, expected);
        assert!(
            !environment
                .entries
                .contains_key(&OsString::from("CODING_AGENT_SENTINEL_SECRET"))
        );
    }

    #[cfg(all(windows, target_env = "msvc"))]
    #[test]
    fn windows_msvc_environment_fails_closed_for_untrusted_paths() {
        let temp = tempfile::tempdir().unwrap();
        let library_directory = temp.path().join("lib");
        let include_directory = temp.path().join("include");
        std::fs::create_dir(&library_directory).unwrap();
        std::fs::create_dir(&include_directory).unwrap();
        let missing_linker = temp.path().join("missing-link.exe");

        assert!(matches!(
            WindowsMsvcEnvironment::try_new(
                PathBuf::from("relative-link.exe"),
                vec![library_directory.clone()],
                vec![include_directory.clone()],
            ),
            Err(ToolchainEnvironmentError::Linker)
        ));
        assert!(matches!(
            WindowsMsvcEnvironment::try_new(
                missing_linker,
                vec![library_directory.clone()],
                vec![include_directory.clone()],
            ),
            Err(ToolchainEnvironmentError::Linker)
        ));

        let linker = temp.path().join("link.exe");
        std::fs::copy(std::env::current_exe().unwrap(), &linker).unwrap();
        for library_directories in [
            Vec::new(),
            vec![PathBuf::from("relative-lib")],
            vec![temp.path().join("missing-lib")],
        ] {
            assert!(matches!(
                WindowsMsvcEnvironment::try_new(
                    linker.clone(),
                    library_directories,
                    vec![include_directory.clone()],
                ),
                Err(ToolchainEnvironmentError::Directory)
            ));
        }
        assert!(matches!(
            WindowsMsvcEnvironment::try_new(
                linker,
                vec![library_directory],
                vec![PathBuf::from("relative-include")],
            ),
            Err(ToolchainEnvironmentError::Directory)
        ));
    }

    #[tokio::test]
    async fn bounded_drain_cleanup_aborts_and_joins_every_pipe_task() {
        let active = Arc::new(AtomicUsize::new(2));
        let stdout = tokio::spawn(drain_stream(NeverReadyReader::new(active.clone()), 64));
        let stderr = tokio::spawn(drain_stream(NeverReadyReader::new(active.clone()), 64));

        let result = collect_drains_until(
            TokioInstant::now() + Duration::from_millis(20),
            stdout,
            stderr,
        )
        .await;

        assert!(matches!(result, Err(ProcessError::CleanupTimedOut)));
        assert_eq!(active.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn head_tail_capture_has_exact_boundaries() {
        for (limit, input, expected_head, expected_tail, truncated) in [
            (1, b"a".as_slice(), b"a".as_slice(), b"".as_slice(), false),
            (1, b"ab".as_slice(), b"a".as_slice(), b"".as_slice(), true),
            (
                4,
                b"abcd".as_slice(),
                b"abcd".as_slice(),
                b"".as_slice(),
                false,
            ),
            (
                4,
                b"abcde".as_slice(),
                b"ab".as_slice(),
                b"de".as_slice(),
                true,
            ),
        ] {
            let mut capture = HeadTailCapture::new(limit);
            capture.push(input);
            let output = capture.finish();
            assert_eq!(output.head, expected_head);
            assert_eq!(output.tail, expected_tail);
            assert_eq!(output.truncated, truncated);
            assert_eq!(output.observed_bytes, input.len() as u64);
        }
    }

    #[test]
    fn cancellation_wins_when_deadline_and_token_are_both_ready() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        assert!(matches!(
            deadline_observation(&cancellation),
            ObservedTermination::Cancelled
        ));
    }

    struct NeverReadyReader {
        active: Arc<AtomicUsize>,
    }

    impl NeverReadyReader {
        fn new(active: Arc<AtomicUsize>) -> Self {
            Self { active }
        }
    }

    impl AsyncRead for NeverReadyReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl Drop for NeverReadyReader {
        fn drop(&mut self) {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn supervisor(output_bytes: usize, max_timeout: Duration) -> ProcessSupervisor {
        ProcessSupervisor::new(
            ProcessLimits::try_new(
                output_bytes,
                output_bytes,
                max_timeout,
                Duration::from_secs(2),
            )
            .unwrap(),
        )
    }

    fn helper_command(mode: &str, temp: &TempDir, timeout: Duration) -> ValidatedCommand {
        helper_command_with_environment(
            mode,
            temp,
            timeout,
            ChildEnvironment::from_current_process().unwrap(),
            None,
        )
    }

    fn helper_command_with_pid(
        mode: &str,
        temp: &TempDir,
        pid_file: &Path,
        timeout: Duration,
    ) -> ValidatedCommand {
        helper_command_with_environment(
            mode,
            temp,
            timeout,
            ChildEnvironment::from_current_process().unwrap(),
            Some(pid_file),
        )
    }

    fn helper_command_with_environment(
        mode: &str,
        temp: &TempDir,
        timeout: Duration,
        mut environment: ChildEnvironment,
        pid_file: Option<&Path>,
    ) -> ValidatedCommand {
        environment.insert_test_value(HELPER_ENV, mode);
        if let Some(pid_file) = pid_file {
            environment.insert_test_value(HELPER_PID_FILE, pid_file.as_os_str());
        }
        ValidatedCommand::for_test(
            std::env::current_exe().unwrap(),
            ["--exact", HELPER_TEST, "--nocapture"]
                .into_iter()
                .map(OsString::from)
                .collect(),
            temp.path().to_path_buf(),
            environment,
            timeout,
        )
        .unwrap()
    }

    fn platform_environment(temp: &TempDir) -> PlatformEnvironment {
        #[cfg(windows)]
        let system_root = std::env::var_os("SYSTEMROOT")
            .or_else(|| std::env::var_os("WINDIR"))
            .map(PathBuf::from);
        #[cfg(unix)]
        let system_root = None;
        PlatformEnvironment::try_new(temp.path().to_path_buf(), system_root).unwrap()
    }

    fn flush_standard_streams() {
        std::io::stdout().flush().unwrap();
        std::io::stderr().flush().unwrap();
    }

    fn write_helper_pid() {
        let path = std::env::var_os(HELPER_PID_FILE).expect("helper pid file is configured");
        std::fs::write(path, std::process::id().to_string()).unwrap();
    }

    fn wait_for_helper_pid_sync() {
        let path = PathBuf::from(
            std::env::var_os(HELPER_PID_FILE).expect("helper pid file is configured"),
        );
        let deadline = Instant::now() + Duration::from_secs(2);
        while !path.exists() {
            assert!(
                Instant::now() < deadline,
                "grandchild did not publish its pid"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    async fn wait_for_helper_pid(path: &Path) -> u32 {
        let deadline = TokioInstant::now() + Duration::from_secs(3);
        loop {
            if let Ok(value) = std::fs::read_to_string(path) {
                return value.parse().unwrap();
            }
            assert!(
                TokioInstant::now() < deadline,
                "helper did not publish its pid"
            );
            time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn wait_until_process_gone(process_id: u32) {
        let deadline = TokioInstant::now() + Duration::from_secs(3);
        while process_exists(process_id) {
            assert!(
                TokioInstant::now() < deadline,
                "process {process_id} survived cleanup"
            );
            time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }

    #[cfg(target_os = "linux")]
    fn process_is_running(process_id: u32) -> bool {
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{process_id}/stat")) else {
            return false;
        };
        stat.rsplit_once(") ")
            .and_then(|(_, fields)| fields.chars().next())
            .is_some_and(|state| state != 'Z')
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    fn process_is_running(process_id: u32) -> bool {
        process_exists(process_id)
    }

    #[cfg(windows)]
    fn process_is_running(process_id: u32) -> bool {
        process_exists(process_id)
    }

    #[cfg(unix)]
    fn process_exists(process_id: u32) -> bool {
        let Ok(process_id) = i32::try_from(process_id) else {
            return false;
        };
        if unsafe { libc::kill(process_id, 0) } == 0 {
            true
        } else {
            io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
        }
    }

    #[cfg(windows)]
    fn process_exists(process_id: u32) -> bool {
        use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};

        use windows_sys::Win32::Foundation::{HANDLE, WAIT_TIMEOUT};
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE,
            WaitForSingleObject,
        };

        let process = unsafe {
            OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                0,
                process_id,
            )
        };
        if process.is_null() {
            return false;
        }
        let process = unsafe { OwnedHandle::from_raw_handle(process) };
        unsafe { WaitForSingleObject(process.as_raw_handle() as HANDLE, 0) == WAIT_TIMEOUT }
    }
}
