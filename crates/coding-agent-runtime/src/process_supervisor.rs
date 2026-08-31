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

#[cfg(unix)]
use crate::command_policy::UnixDeliveryDirectoryRole;
use crate::command_policy::{CommandPolicyError, ValidatedCommand};
use crate::process_liveness::{
    ProcessCleanupProof, ProcessLivenessError, ProcessLivenessScope, ProcessLivenessSentinel,
};

#[cfg(any(test, feature = "test-support"))]
mod faults;
mod input;
mod output;
mod start;
mod supervision;
mod tree_control;

#[cfg(test)]
use output::HeadTailCapture;
use output::{drain_stream, join_captured};
#[cfg(target_os = "macos")]
use supervision::reconcile_exited_tree_kill;
#[cfg(all(test, target_os = "macos"))]
use supervision::should_use_exited_tree_kill;
#[cfg(test)]
use supervision::{ObservedTermination, collect_drains_until, deadline_observation};
use supervision::{
    ProcessExecution, SpawnedLivenessOwner, cleanup_attached_spawn_failure, supervise_child,
};
use tree_control::{TreeKillHandle, TreeWorkerGuard, exit_signal};

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use faults::{
    ProcessFault, ProcessFaultController, ProcessFaultControllerError, ProcessFaultEvent,
    ProcessFaultEventKind, ProcessFaultZeroLiveProof,
};

pub(crate) use input::ExactChildInput;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use input::{
    MAX_EXACT_CHILD_INPUT_BYTES_FOR_TEST, ProcessStdinTestObservation, ProcessStdinTestOutcome,
    ProcessStdinTestScenario, exercise_process_stdin_for_test,
};

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

    pub(crate) const fn max_command_timeout(self) -> Duration {
        self.max_command_timeout
    }

    pub(crate) const fn cleanup_timeout(self) -> Duration {
        self.cleanup_timeout
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
    #[error("process-liveness setup failed before user code could run")]
    LivenessSetupFailed(#[source] ProcessLivenessError),
    #[error("spawned process did not expose both output pipes")]
    MissingOutputPipe,
    #[error("spawned process did not expose its requested input pipe")]
    MissingInputPipe,
    #[error("the child closed exact input before it was fully written and closed")]
    InputClosedEarly,
    #[error("writing exact child input failed")]
    InputWriteFailed(#[source] io::Error),
    #[error("closing exact child input failed")]
    InputCloseFailed(#[source] io::Error),
    #[error("exact child input completion is unknown")]
    InputCompletionUnknown,
    #[error("waiting for the process failed")]
    WaitFailed(#[source] io::Error),
    #[error("process tree identity was lost before cleanup could be proven")]
    TreeControlLost(#[source] io::Error),
    #[error("process tree termination failed")]
    TreeCleanupFailed(#[source] io::Error),
    #[error("bounded process tree cleanup timed out")]
    CleanupTimedOut,
    #[error("process-liveness cleanup could not be proven")]
    LivenessCleanupUnproven,
    #[error("process-liveness cleanup proof failed")]
    LivenessCleanupFailed(#[source] ProcessLivenessError),
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
            Self::SpawnFailed(_) | Self::TreeSetupFailed(_) | Self::LivenessSetupFailed(_) => {
                "COMMAND_SPAWN_FAILED"
            }
            Self::MissingOutputPipe | Self::OutputDrainFailed(_) => "COMMAND_OUTPUT_FAILED",
            Self::MissingInputPipe
            | Self::InputClosedEarly
            | Self::InputWriteFailed(_)
            | Self::InputCloseFailed(_) => "COMMAND_INPUT_FAILED",
            Self::InputCompletionUnknown => "COMMAND_INPUT_UNKNOWN",
            Self::WaitFailed(_) => "COMMAND_WAIT_FAILED",
            Self::TreeControlLost(_)
            | Self::TreeCleanupFailed(_)
            | Self::CleanupTimedOut
            | Self::LivenessCleanupUnproven
            | Self::LivenessCleanupFailed(_)
            | Self::WorkerFailed => "PROCESS_TREE_CLEANUP_FAILED",
        }
    }

    pub const fn process_cleanup_is_unproven(&self) -> bool {
        matches!(
            self,
            Self::TreeControlLost(_)
                | Self::TreeCleanupFailed(_)
                | Self::CleanupTimedOut
                | Self::LivenessCleanupUnproven
                | Self::LivenessCleanupFailed(_)
                | Self::WorkerFailed
        )
    }

    /// True only when the supervisor failed before `process.spawn()` could
    /// create a child. Callers may use this narrow fact to distinguish a
    /// proven zero-effect mutation attempt from an outcome that needs
    /// reconciliation.
    pub(crate) const fn child_could_not_have_started(&self) -> bool {
        matches!(
            self,
            Self::InvalidCommand
                | Self::CommandPolicy(_)
                | Self::TimeoutOutsideLimit
                | Self::SpawnFailed(_)
                | Self::TreeSetupFailed(_)
                | Self::LivenessSetupFailed(_)
        )
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
    liveness_scope: ProcessLivenessScope,
    tasks: TaskTracker,
    #[cfg(test)]
    supervision_gate: Option<Arc<tokio::sync::Notify>>,
    #[cfg(test)]
    proof_continuation_started: Option<Arc<tokio::sync::Notify>>,
    #[cfg(test)]
    supervision_fault: Option<SupervisionFault>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SupervisionFault {
    AttachAndResume,
    AnchorLost,
    KillNow,
    AttachedCleanupWaitAfterReap,
}

#[cfg(test)]
fn injected_supervision_error(fault: SupervisionFault) -> io::Error {
    io::Error::other(format!("injected process-supervisor fault: {fault:?}"))
}

impl ProcessSupervisor {
    pub(crate) fn new(limits: ProcessLimits, liveness_scope: ProcessLivenessScope) -> Self {
        Self {
            limits,
            liveness_scope,
            tasks: TaskTracker::new(),
            #[cfg(test)]
            supervision_gate: None,
            #[cfg(test)]
            proof_continuation_started: None,
            #[cfg(test)]
            supervision_fault: None,
        }
    }

    #[cfg(test)]
    fn new_paused_for_test(
        limits: ProcessLimits,
        liveness_scope: ProcessLivenessScope,
    ) -> (Self, Arc<tokio::sync::Notify>) {
        let gate = Arc::new(tokio::sync::Notify::new());
        let mut supervisor = Self::new(limits, liveness_scope);
        supervisor.supervision_gate = Some(gate.clone());
        (supervisor, gate)
    }

    #[cfg(test)]
    fn new_faulted_for_test(
        limits: ProcessLimits,
        liveness_scope: ProcessLivenessScope,
        fault: SupervisionFault,
    ) -> Self {
        let mut supervisor = Self::new(limits, liveness_scope);
        supervisor.supervision_fault = Some(fault);
        supervisor
    }

    #[cfg(test)]
    fn new_faulted_paused_for_test(
        limits: ProcessLimits,
        liveness_scope: ProcessLivenessScope,
        fault: SupervisionFault,
    ) -> (Self, Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
        let gate = Arc::new(tokio::sync::Notify::new());
        let proof_continuation_started = Arc::new(tokio::sync::Notify::new());
        let mut supervisor = Self::new_faulted_for_test(limits, liveness_scope, fault);
        supervisor.supervision_gate = Some(gate.clone());
        supervisor.proof_continuation_started = Some(proof_continuation_started.clone());
        (supervisor, gate, proof_continuation_started)
    }

    pub(crate) async fn run(
        &self,
        mut command: ValidatedCommand,
        cancellation: CancellationToken,
    ) -> Result<CommandResult, ProcessError> {
        if cancellation.is_cancelled() {
            return Ok(CommandResult::cancelled_before_spawn());
        }
        if command.timeout() > self.limits.max_command_timeout {
            return Err(ProcessError::TimeoutOutsideLimit);
        }

        let exact_input = command.take_exact_input();
        let execution = self.start(command, exact_input, cancellation).await?;
        execution.wait().await
    }

    pub(crate) async fn shutdown(&self) {
        self.tasks.close();
        self.tasks.wait().await;
    }
}

fn process_spawn_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

#[cfg(unix)]
#[path = "process_supervisor/platform_unix.rs"]
mod platform;
#[cfg(windows)]
#[path = "process_supervisor/platform_windows.rs"]
mod platform;
#[cfg(test)]
mod tests;
