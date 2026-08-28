use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::{NonZeroU16, NonZeroU32, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use coding_agent_domain::UtcTimestamp;
#[cfg(feature = "test-support")]
use coding_agent_domain::{Repository, TaskId};
use coding_agent_runtime::{
    ProcessCleanupProof, ProcessLivenessDirectory, ProcessLivenessError, ProcessLivenessScope,
    SealedProcessLivenessScope,
};
use coding_agent_store::{Store, StoreError};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[cfg(test)]
use crate::SecuritySeed;
use crate::platform::{
    PrivateFile, harden_private_file, validate_private_file, validate_private_file_snapshot,
};
#[cfg(feature = "test-support")]
use crate::repository_service::RepositoryRuntimeRegistrar;
use crate::runtime_config::RuntimeConfigLoadError;
use crate::security::{LauncherSecret, SecurityClock, SecurityError, SystemSecurityClock};
use crate::shutdown::{ShutdownCleanup, ShutdownLockDisposition, ShutdownRuntimeCleanupProof};
#[cfg(feature = "test-support")]
use crate::test_support::ProcessTestWatchers;
use crate::{
    BrowserLaunchError, BrowserLauncher, EventDispatcherError, NativeDialogService, PlatformPaths,
    ProductionStartupRunnerFactory, ShutdownCoordinator, ShutdownOutcome, StartupRunnerFactory,
    SystemWallClock, WallClock,
};
#[cfg(feature = "test-support")]
use crate::{
    DeliveryManagerHandle, EventDispatcherHandle, MutationGate, StoreWriterHandle,
    TaskManagerHandle,
};

mod start_primary;

use start_primary::start_primary;

const MAX_DESCRIPTOR_BYTES: u64 = 4 * 1024;
const LAUNCHER_SECRET_BYTES: usize = 32;
const SECONDARY_DEADLINE: Duration = Duration::from_secs(10);
const SECONDARY_INITIAL_DELAY: Duration = Duration::from_millis(25);
const SECONDARY_MAX_DELAY: Duration = Duration::from_secs(1);
const LISTENER_BIND_ATTEMPTS: usize = 3;
const EVENT_BROADCAST_CAPACITY: usize = 1_024;
const ACTOR_QUEUE_CAPACITY: usize = 64;
const PROCESS_CLEANUP_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const DEGRADED_SHUTDOWN_WARNING_ARGUMENT: &str =
    "--coding-agent-internal-degraded-shutdown-warning";
const DEGRADED_SHUTDOWN_TITLE: &str = "Coding Agent did not shut down cleanly";
const DEGRADED_SHUTDOWN_MESSAGE: &str = "Some terminal task states could not be persisted. They will be recovered the next time Coding Agent starts.";

/// Handles the private child-process mode used to keep the degraded-shutdown
/// warning alive after the primary process exits.
pub fn run_degraded_shutdown_warning_if_requested() -> bool {
    if !is_degraded_shutdown_warning_invocation(std::env::args_os()) {
        return false;
    }

    let _ = io::stdout().write_all(b"R");
    let _ = io::stdout().flush();
    let _ = rfd::MessageDialog::new()
        .set_level(rfd::MessageLevel::Error)
        .set_title(DEGRADED_SHUTDOWN_TITLE)
        .set_description(DEGRADED_SHUTDOWN_MESSAGE)
        .show();
    true
}

fn is_degraded_shutdown_warning_invocation<I, S>(arguments: I) -> bool
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut arguments = arguments.into_iter();
    let Some(_executable) = arguments.next() else {
        return false;
    };
    arguments
        .next()
        .is_some_and(|argument| argument.as_ref() == OsStr::new(DEGRADED_SHUTDOWN_WARNING_ARGUMENT))
        && arguments.next().is_none()
}

/// Owns the operating-system lock on the permanent `instance.lock` file.
///
/// The file itself is never removed or replaced. Dropping this value closes the
/// descriptor and releases the lock, allowing a later process to acquire the
/// same file.
pub struct InstanceLock {
    lease: Arc<LockLease>,
}

struct LockLease {
    _file: File,
}

impl fmt::Debug for InstanceLock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstanceLock")
            .finish_non_exhaustive()
    }
}

impl InstanceLock {
    pub fn try_acquire(path: impl AsRef<Path>) -> io::Result<Option<Self>> {
        let path = path.as_ref();
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "instance lock path is a symlink",
                ));
            }
            Ok(metadata) if !metadata.is_file() => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "instance lock path is not a regular file",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }

        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt as _;
            use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;

            options
                .access_mode(crate::platform::windows_private_file_access_mode())
                .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
        }
        let file = options.open(path)?;

        match file.try_lock() {
            Ok(()) => {
                harden_private_file(&file)?;
                Ok(Some(Self {
                    lease: Arc::new(LockLease { _file: file }),
                }))
            }
            Err(TryLockError::WouldBlock) => {
                validate_private_file(&file)?;
                Ok(None)
            }
            Err(TryLockError::Error(error)) => Err(error),
        }
    }

    fn keepalive(&self) -> Arc<LockLease> {
        self.lease.clone()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeDescriptorError {
    #[error("runtime descriptor I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("runtime descriptor JSON is invalid: {0}")]
    Json(#[from] serde_json::Error),
    #[error("runtime descriptor is invalid: {0}")]
    Invalid(&'static str),
}

#[derive(Clone, PartialEq, Eq)]
pub struct RuntimeDescriptor {
    instance_id: Uuid,
    pid: NonZeroU32,
    port: NonZeroU16,
    started_at: UtcTimestamp,
    launcher_secret: String,
}

impl RuntimeDescriptor {
    pub fn new(
        instance_id: Uuid,
        pid: NonZeroU32,
        port: NonZeroU16,
        started_at: UtcTimestamp,
        launcher_secret: LauncherSecret,
    ) -> Result<Self, RuntimeDescriptorError> {
        validate_instance_id(instance_id)?;
        validate_pid(pid)?;
        validate_launcher_secret(launcher_secret.as_str())?;
        Ok(Self {
            instance_id,
            pid,
            port,
            started_at,
            launcher_secret: launcher_secret.as_str().to_owned(),
        })
    }

    pub fn instance_id(&self) -> Uuid {
        self.instance_id
    }

    pub fn pid(&self) -> NonZeroU32 {
        self.pid
    }

    pub fn port(&self) -> NonZeroU16 {
        self.port
    }

    pub fn started_at(&self) -> UtcTimestamp {
        self.started_at
    }

    pub fn launcher_secret(&self) -> &str {
        &self.launcher_secret
    }

    pub fn publish(&self, path: impl AsRef<Path>) -> Result<(), RuntimeDescriptorError> {
        let path = path.as_ref();
        let encoded = serde_json::to_vec(&RuntimeDescriptorWire::from(self))?;
        if encoded.len() as u64 > MAX_DESCRIPTOR_BYTES {
            return Err(RuntimeDescriptorError::Invalid("document is too large"));
        }

        let temporary_path = temporary_descriptor_path(path)?;
        let publication = (|| {
            let mut temporary = PrivateFile::create_new(&temporary_path)?;
            temporary.write_all(&encoded)?;
            temporary.flush()?;
            temporary.as_file().sync_all()?;
            drop(temporary);
            atomic_replace(&temporary_path, path)
        })();
        if let Err(error) = publication {
            let _ = std::fs::remove_file(&temporary_path);
            return Err(RuntimeDescriptorError::Io(error));
        }
        sync_parent_directory(path)?;
        Ok(())
    }

    pub fn read(path: impl AsRef<Path>) -> Result<Self, RuntimeDescriptorError> {
        let path = path.as_ref();
        // Open a fresh handle for every call so atomic replacement is observed
        // on the next retry instead of retaining a stale descriptor inode. Final-component
        // no-follow flags and handle validation avoid a path-check/open race.
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt as _;
            use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;

            options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
        }
        let file = options.open(path)?;
        validate_private_file_snapshot(&file)?;
        if file.metadata()?.len() > MAX_DESCRIPTOR_BYTES {
            return Err(RuntimeDescriptorError::Invalid("document is too large"));
        }

        let mut encoded = Vec::new();
        file.take(MAX_DESCRIPTOR_BYTES + 1)
            .read_to_end(&mut encoded)?;
        if encoded.len() as u64 > MAX_DESCRIPTOR_BYTES {
            return Err(RuntimeDescriptorError::Invalid("document is too large"));
        }
        let wire: RuntimeDescriptorWire = serde_json::from_slice(&encoded)?;
        Self::try_from(wire)
    }
}

impl fmt::Debug for RuntimeDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeDescriptor")
            .field("instance_id", &self.instance_id)
            .field("pid", &self.pid)
            .field("port", &self.port)
            .field("started_at", &self.started_at)
            .field("launcher_secret", &"<redacted>")
            .finish()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeDescriptorWire {
    instance_id: String,
    pid: u64,
    port: u64,
    started_at: String,
    launcher_secret: String,
}

impl From<&RuntimeDescriptor> for RuntimeDescriptorWire {
    fn from(descriptor: &RuntimeDescriptor) -> Self {
        Self {
            instance_id: descriptor.instance_id.hyphenated().to_string(),
            pid: u64::from(descriptor.pid.get()),
            port: u64::from(descriptor.port.get()),
            started_at: descriptor.started_at.to_string(),
            launcher_secret: descriptor.launcher_secret.clone(),
        }
    }
}

impl TryFrom<RuntimeDescriptorWire> for RuntimeDescriptor {
    type Error = RuntimeDescriptorError;

    fn try_from(wire: RuntimeDescriptorWire) -> Result<Self, Self::Error> {
        let instance_id = Uuid::parse_str(&wire.instance_id)
            .map_err(|_| RuntimeDescriptorError::Invalid("instance ID is not a UUID"))?;
        validate_instance_id(instance_id)?;
        if wire.instance_id != instance_id.hyphenated().to_string() {
            return Err(RuntimeDescriptorError::Invalid(
                "instance ID is not canonical",
            ));
        }

        let pid = u32::try_from(wire.pid)
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or(RuntimeDescriptorError::Invalid(
                "process ID is outside the supported range",
            ))?;
        validate_pid(pid)?;
        let port = u16::try_from(wire.port)
            .ok()
            .and_then(NonZeroU16::new)
            .ok_or(RuntimeDescriptorError::Invalid(
                "loopback port is outside the supported range",
            ))?;
        let started_at = UtcTimestamp::parse_rfc3339(&wire.started_at)
            .map_err(|_| RuntimeDescriptorError::Invalid("start timestamp is invalid"))?;
        if wire.started_at != started_at.to_string() {
            return Err(RuntimeDescriptorError::Invalid(
                "start timestamp is not canonical UTC",
            ));
        }
        validate_launcher_secret(&wire.launcher_secret)?;

        Ok(Self {
            instance_id,
            pid,
            port,
            started_at,
            launcher_secret: wire.launcher_secret,
        })
    }
}

fn validate_instance_id(instance_id: Uuid) -> Result<(), RuntimeDescriptorError> {
    if instance_id.is_nil() || instance_id.get_version() != Some(uuid::Version::Random) {
        Err(RuntimeDescriptorError::Invalid(
            "instance ID is not a version-4 UUID",
        ))
    } else {
        Ok(())
    }
}

fn validate_launcher_secret(secret: &str) -> Result<(), RuntimeDescriptorError> {
    let decoded = URL_SAFE_NO_PAD
        .decode(secret)
        .map_err(|_| RuntimeDescriptorError::Invalid("launcher secret encoding is invalid"))?;
    if decoded.len() != LAUNCHER_SECRET_BYTES || URL_SAFE_NO_PAD.encode(decoded) != secret {
        return Err(RuntimeDescriptorError::Invalid(
            "launcher secret must encode exactly 32 bytes",
        ));
    }
    Ok(())
}

fn validate_pid(_pid: NonZeroU32) -> Result<(), RuntimeDescriptorError> {
    #[cfg(unix)]
    if _pid.get() > libc::pid_t::MAX as u32 {
        return Err(RuntimeDescriptorError::Invalid(
            "process ID is outside the platform range",
        ));
    }
    Ok(())
}

fn temporary_descriptor_path(path: &Path) -> Result<PathBuf, RuntimeDescriptorError> {
    let file_name = path.file_name().ok_or(RuntimeDescriptorError::Invalid(
        "descriptor path has no file name",
    ))?;
    let mut temporary_name = OsString::from(file_name);
    temporary_name.push(format!(".{}.tmp", Uuid::new_v4().hyphenated()));
    Ok(path.with_file_name(temporary_name))
}

#[cfg(windows)]
fn atomic_replace(source: &Path, destination: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt as _;
    use std::ptr::null;

    use windows_sys::Win32::Foundation::ERROR_FILE_NOT_FOUND;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW, ReplaceFileW,
    };

    let source = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let destination = destination
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    if unsafe {
        ReplaceFileW(
            destination.as_ptr(),
            source.as_ptr(),
            null(),
            0,
            null(),
            null(),
        )
    } != 0
    {
        return Ok(());
    }
    let replace_error = io::Error::last_os_error();
    if replace_error.raw_os_error() != Some(ERROR_FILE_NOT_FOUND as i32) {
        return Err(replace_error);
    }

    if unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    } != 0
    {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(windows))]
fn atomic_replace(source: &Path, destination: &Path) -> io::Result<()> {
    std::fs::rename(source, destination)
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "descriptor path has no parent directory",
        )
    })?;
    match File::open(parent)?.sync_all() {
        Ok(()) => Ok(()),
        Err(error)
            if error
                .raw_os_error()
                .is_some_and(|code| [libc::EINVAL, libc::ENOTSUP].contains(&code)) =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartupPhase {
    Starting,
    Ready,
}

#[derive(Debug, Clone)]
pub struct StartupPhaseController {
    phase: Arc<AtomicU8>,
}

impl StartupPhaseController {
    pub fn new() -> Self {
        Self {
            phase: Arc::new(AtomicU8::new(0)),
        }
    }

    pub fn current(&self) -> StartupPhase {
        match self.phase.load(Ordering::Acquire) {
            0 => StartupPhase::Starting,
            1 => StartupPhase::Ready,
            _ => unreachable!("startup phase has an internal invalid value"),
        }
    }

    pub fn mark_ready(&self) -> bool {
        self.phase
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

impl Default for StartupPhaseController {
    fn default() -> Self {
        Self::new()
    }
}

pub trait StartupPaths: Send + Sync + 'static {
    fn discover(&self) -> io::Result<PlatformPaths>;
    fn prepare_lock_parent(&self, paths: &PlatformPaths) -> io::Result<()>;
    fn prepare(&self, paths: &PlatformPaths) -> io::Result<()>;
}

#[async_trait::async_trait]
pub trait StoreFactory: Send + Sync + 'static {
    async fn open(&self, path: &Path) -> Result<Store, StoreError>;
}

#[async_trait::async_trait]
pub trait ListenerFactory: Send + Sync + 'static {
    async fn bind(&self, address: SocketAddrV4) -> io::Result<TcpListener>;
}

pub trait BrowserOpener: Send + Sync + 'static {
    fn open(&self, port: u16, token: &str) -> Result<(), BrowserLaunchError>;
}

pub trait NativeMessageSink: Send + Sync + 'static {
    fn show_error(&self, title: &'static str, body: String);

    fn publish_degraded_shutdown(&self) -> io::Result<()> {
        self.show_error(
            DEGRADED_SHUTDOWN_TITLE,
            DEGRADED_SHUTDOWN_MESSAGE.to_owned(),
        );
        Ok(())
    }
}

pub trait AvailableParallelismProbe: Send + Sync + 'static {
    fn available_parallelism(&self) -> Option<NonZeroUsize>;
}

#[derive(Debug, Default)]
struct SystemAvailableParallelismProbe;

impl AvailableParallelismProbe for SystemAvailableParallelismProbe {
    fn available_parallelism(&self) -> Option<NonZeroUsize> {
        std::thread::available_parallelism().ok()
    }
}

#[derive(Clone)]
pub struct StartupDependencies {
    pub paths: Arc<dyn StartupPaths>,
    pub stores: Arc<dyn StoreFactory>,
    pub listeners: Arc<dyn ListenerFactory>,
    pub browser: Arc<dyn BrowserOpener>,
    pub messages: Arc<dyn NativeMessageSink>,
    pub wall_clock: Arc<dyn WallClock>,
    pub security_clock: Arc<dyn SecurityClock>,
    pub available_parallelism: Arc<dyn AvailableParallelismProbe>,
    pub runner_factory: Arc<dyn StartupRunnerFactory>,
    pub dialog: Option<NativeDialogService>,
    #[cfg(feature = "test-support")]
    pub(crate) process_test_support: Option<Arc<crate::test_support::ProcessTestRuntime>>,
    public_origin: StartupPublicOrigin,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
enum StartupPublicOrigin {
    #[default]
    Listener,
    Development(String),
}

impl StartupPublicOrigin {
    fn browser_port(&self, listener_port: u16) -> Option<u16> {
        match self {
            Self::Listener => NonZeroU16::new(listener_port).map(NonZeroU16::get),
            Self::Development(public_origin) => canonical_loopback_origin_port(public_origin),
        }
    }
}

impl StartupDependencies {
    pub fn production(dialog: Option<NativeDialogService>) -> Self {
        Self {
            paths: Arc::new(SystemStartupPaths),
            stores: Arc::new(SystemStoreFactory),
            listeners: Arc::new(SystemListenerFactory),
            browser: Arc::new(SystemBrowserOpener),
            messages: Arc::new(SystemNativeMessageSink),
            wall_clock: Arc::new(SystemWallClock),
            security_clock: Arc::new(SystemSecurityClock),
            available_parallelism: Arc::new(SystemAvailableParallelismProbe),
            runner_factory: Arc::new(ProductionStartupRunnerFactory),
            dialog,
            #[cfg(feature = "test-support")]
            process_test_support: None,
            public_origin: StartupPublicOrigin::Listener,
        }
    }

    /// Routes browser requests through one exact development origin while Axum
    /// continues to require the dynamically-bound listener authority as Host.
    ///
    /// The value is validated by [`crate::SecurityManager`] when a primary starts; no
    /// localhost alias, wildcard origin, or authentication bypass is introduced.
    pub fn with_development_public_origin(mut self, public_origin: impl Into<String>) -> Self {
        self.public_origin = StartupPublicOrigin::Development(public_origin.into());
        self
    }
}

impl Default for StartupDependencies {
    fn default() -> Self {
        #[cfg(not(target_os = "macos"))]
        let dialog = Some(NativeDialogService::new());
        #[cfg(target_os = "macos")]
        let dialog = None;
        Self::production(dialog)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StartupError {
    #[error("application paths are unavailable: {0}")]
    Paths(#[source] io::Error),
    #[error("the single-instance lock is unavailable: {0}")]
    Lock(#[source] io::Error),
    #[error("process-liveness state is unavailable: {0}")]
    ProcessLiveness(#[from] ProcessLivenessError),
    #[error("cleanup of a previous process tree could not be proven")]
    ProcessCleanupUnproven,
    #[error(transparent)]
    Descriptor(#[from] RuntimeDescriptorError),
    #[error("the local store could not be started: {0}")]
    Store(#[from] StoreError),
    #[error(transparent)]
    RuntimeConfig(#[from] RuntimeConfigLoadError),
    #[error("startup security initialization failed: {0}")]
    Security(#[from] SecurityError),
    #[error("the event dispatcher could not be started: {0}")]
    Dispatcher(#[from] EventDispatcherError),
    #[error(transparent)]
    Runner(#[from] crate::StartupRunnerFactoryError),
    #[error("the loopback listener could not be bound: {0}")]
    Listener(#[source] io::Error),
    #[error("the listener factory returned a non-loopback or zero-port listener")]
    InvalidListener,
    #[error("the real loopback readiness probe failed")]
    SelfProbe,
    #[error("the locked primary could not be verified within ten seconds")]
    PrimaryUnverified,
    #[error("the current wall-clock time is outside the supported range")]
    Timestamp,
}

impl StartupError {
    fn native_title(&self) -> &'static str {
        match self {
            Self::PrimaryUnverified => "Coding Agent is already running",
            _ => "Coding Agent could not start",
        }
    }

    fn native_body(&self) -> String {
        match self {
            Self::Paths(_) => {
                "The private application data directory could not be prepared.".to_owned()
            }
            Self::Store(_) => {
                "The local database could not be opened, migrated, or recovered. No web server was published."
                    .to_owned()
            }
            Self::RuntimeConfig(error) => format!(
                "The runtime configuration is invalid.\n\nError code: {}",
                error.code()
            ),
            Self::Runner(error) => format!(
                "The coding task runner could not be started.\n\nError code: {}",
                error.code()
            ),
            Self::PrimaryUnverified => {
                "Another process owns the application lock, but it could not be verified. The live lock and runtime descriptor were left untouched."
                    .to_owned()
            }
            _ => format!("{self}"),
        }
    }
}

pub enum StartupOutcome {
    Primary(Box<PrimaryRuntime>),
    Secondary(SecondaryRuntime),
}

pub async fn launch(dependencies: StartupDependencies) -> Result<StartupOutcome, StartupError> {
    let messages = dependencies.messages.clone();
    let result = launch_inner(dependencies).await;
    if let Err(error) = &result {
        messages.show_error(error.native_title(), error.native_body());
    }
    result
}

async fn launch_inner(dependencies: StartupDependencies) -> Result<StartupOutcome, StartupError> {
    let paths = dependencies.paths.discover().map_err(StartupError::Paths)?;
    dependencies
        .paths
        .prepare_lock_parent(&paths)
        .map_err(StartupError::Paths)?;

    let lock = InstanceLock::try_acquire(&paths.instance_lock).map_err(StartupError::Lock)?;
    match lock {
        Some(lock) => {
            dependencies
                .paths
                .prepare(&paths)
                .map_err(StartupError::Paths)?;
            let instance_id = Uuid::new_v4();
            start_primary(paths, lock, instance_id, dependencies)
                .await
                .map(Box::new)
                .map(StartupOutcome::Primary)
        }
        None => start_secondary(paths, dependencies)
            .await
            .map(StartupOutcome::Secondary),
    }
}

pub struct PrimaryRuntime {
    descriptor: RuntimeDescriptor,
    _process_liveness_scope: ProcessLivenessScope,
    startup_phase: StartupPhaseController,
    shutdown: ShutdownCoordinator,
    quit_requested: Arc<Notify>,
    browser_opened: bool,
    #[cfg(feature = "test-support")]
    test_handles: PrimaryRuntimeTestHandles,
    #[cfg(feature = "test-support")]
    _test_signal_watchers: ProcessTestWatchers,
    #[cfg(feature = "test-support")]
    _process_test_support: Option<Arc<crate::test_support::ProcessTestRuntime>>,
}

#[cfg(feature = "test-support")]
#[derive(Clone)]
pub struct PrimaryRuntimeTestHandles {
    pub store: Store,
    pub writer: StoreWriterHandle,
    pub dispatcher: EventDispatcherHandle,
    pub task_manager: TaskManagerHandle,
    pub delivery_manager: DeliveryManagerHandle,
    pub mutation_gate: MutationGate,
    repository_registrar: RepositoryRuntimeRegistrar,
    process_liveness_scope: ProcessLivenessScope,
}

#[cfg(feature = "test-support")]
impl PrimaryRuntimeTestHandles {
    pub fn hold_instance_process_tree_for_test(
        &self,
    ) -> Result<coding_agent_runtime::HeldProcessLivenessTreeForTest, ProcessLivenessError> {
        self.process_liveness_scope.hold_tree_for_test()
    }

    pub fn hold_task_process_tree_for_test(
        &self,
        task_id: TaskId,
    ) -> Result<coding_agent_runtime::HeldProcessLivenessTreeForTest, ProcessLivenessError> {
        self.process_liveness_scope
            .task_scope(*task_id.as_uuid().as_bytes())?
            .hold_tree_for_test()
    }

    pub async fn attach_repository_runtime_for_test(
        &self,
        repository: &Repository,
        deadline: Instant,
    ) -> Result<(), &'static str> {
        self.repository_registrar
            .attach(repository, deadline)
            .await
            .map_err(|_| "repository runtime attachment failed")?;
        tokio::time::timeout_at(deadline, self.task_manager.notify_admission_changed())
            .await
            .map_err(|_| "repository admission notification deadline elapsed")?
            .map_err(|_| "repository admission notification failed")
    }
}

impl PrimaryRuntime {
    pub fn instance_id(&self) -> Uuid {
        self.descriptor.instance_id()
    }

    pub fn port(&self) -> u16 {
        self.descriptor.port().get()
    }

    pub fn startup_phase(&self) -> StartupPhase {
        self.startup_phase.current()
    }

    pub fn browser_opened(&self) -> bool {
        self.browser_opened
    }

    pub async fn wait_for_quit_request(&self) {
        self.quit_requested.notified().await;
    }

    pub fn shutdown_coordinator(&self) -> ShutdownCoordinator {
        self.shutdown.clone()
    }

    pub async fn shutdown(&self) -> ShutdownOutcome {
        let outcome = self.shutdown.shutdown().await;
        #[cfg(feature = "test-support")]
        {
            let watcher_shutdown = self._test_signal_watchers.shutdown_and_join().await;
            if let Err(error) = watcher_shutdown {
                tracing::warn!(
                    ?error,
                    error_code = "PROCESS_TEST_WATCHER_SHUTDOWN_INCOMPLETE",
                    "process-test signal watchers did not release their capabilities before shutdown"
                );
            }
            let signal_capability_close = self
                ._process_test_support
                .as_ref()
                .map(|support| support.close_signal_capability())
                .transpose();
            if let Err(error) = signal_capability_close {
                tracing::warn!(
                    ?error,
                    error_code = "PROCESS_TEST_SIGNAL_CAPABILITY_CLOSE_FAILED",
                    "process-test signal capability remained open after shutdown"
                );
            }
            if outcome == ShutdownOutcome::Clean
                && watcher_shutdown.is_ok()
                && signal_capability_close.is_ok()
            {
                ShutdownOutcome::Clean
            } else {
                ShutdownOutcome::Degraded
            }
        }
        #[cfg(not(feature = "test-support"))]
        outcome
    }

    #[cfg(feature = "test-support")]
    pub fn test_handles(&self) -> PrimaryRuntimeTestHandles {
        self.test_handles.clone()
    }
}

impl Drop for PrimaryRuntime {
    fn drop(&mut self) {
        self.shutdown.force_cleanup();
    }
}

struct StartupShutdownGuard {
    shutdown: Option<ShutdownCoordinator>,
}

impl StartupShutdownGuard {
    fn new(shutdown: ShutdownCoordinator) -> Self {
        Self {
            shutdown: Some(shutdown),
        }
    }

    fn disarm(mut self) -> ShutdownCoordinator {
        self.shutdown
            .take()
            .expect("startup shutdown guard is armed until primary construction")
    }
}

impl Drop for StartupShutdownGuard {
    fn drop(&mut self) {
        if let Some(shutdown) = &self.shutdown {
            shutdown.force_cleanup();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecondaryRuntime {
    instance_id: Uuid,
    browser_opened: bool,
}

impl SecondaryRuntime {
    pub fn instance_id(&self) -> Uuid {
        self.instance_id
    }

    pub fn browser_opened(&self) -> bool {
        self.browser_opened
    }
}

async fn await_previous_process_cleanup(process_liveness: &ProcessLivenessDirectory) {
    let mut last_observation = None;
    loop {
        let observation = process_liveness.probe_stale();
        match observation {
            Ok(ProcessCleanupProof::Confirmed) => return,
            Ok(proof @ (ProcessCleanupProof::Held | ProcessCleanupProof::Unknown)) => {
                if last_observation != Some(Ok(proof)) {
                    tracing::warn!(
                        error_code = "PROCESS_TREE_CLEANUP_PENDING",
                        ?proof,
                        "startup is retaining the primary lock while process cleanup remains unproven"
                    );
                    last_observation = Some(Ok(proof));
                }
            }
            Err(error) => {
                if last_observation != Some(Err(error)) {
                    tracing::warn!(
                        error_code = "PROCESS_TREE_CLEANUP_PROBE_UNAVAILABLE",
                        %error,
                        "startup is retaining the primary lock while the cleanup probe is unavailable"
                    );
                    last_observation = Some(Err(error));
                }
            }
        }
        tokio::time::sleep(PROCESS_CLEANUP_RETRY_INTERVAL).await;
    }
}

async fn await_process_liveness_directory(paths: &PlatformPaths) -> ProcessLivenessDirectory {
    let mut last_error = None;
    loop {
        match ProcessLivenessDirectory::open(&paths.runtime_dir) {
            Ok(directory) => return directory,
            Err(error) => {
                if last_error != Some(error) {
                    tracing::warn!(
                        error_code = "PROCESS_TREE_CLEANUP_PROBE_UNAVAILABLE",
                        %error,
                        "startup is retaining the primary lock while the process-liveness namespace is unavailable"
                    );
                    last_error = Some(error);
                }
            }
        }
        tokio::time::sleep(PROCESS_CLEANUP_RETRY_INTERVAL).await;
    }
}

fn remove_recovered_shutdown_marker(paths: &PlatformPaths) {
    if let Err(error) = std::fs::remove_file(&paths.unclean_shutdown)
        && error.kind() != io::ErrorKind::NotFound
    {
        tracing::warn!(
            error_code = "SHUTDOWN_MARKER_REMOVE_FAILED",
            "recovered shutdown marker could not be removed"
        );
    }

    let Some(parent) = paths.unclean_shutdown.parent() else {
        return;
    };
    let Some(file_name) = paths.unclean_shutdown.file_name().and_then(OsStr::to_str) else {
        return;
    };
    let prefix = format!("{file_name}.");
    let entries = match std::fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(error) => {
            tracing::warn!(
                error = %error,
                error_code = "SHUTDOWN_MARKER_STAGING_SCAN_FAILED",
                "staged shutdown markers could not be scanned"
            );
            return;
        }
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(&prefix)
            && (name.ends_with(".pending") || name.ends_with(".marker"))
            && let Err(error) = std::fs::remove_file(entry.path())
            && error.kind() != io::ErrorKind::NotFound
        {
            tracing::warn!(
                error = %error,
                error_code = "SHUTDOWN_MARKER_STAGING_REMOVE_FAILED",
                "a staged shutdown marker could not be removed"
            );
        }
    }
}

async fn start_secondary(
    paths: PlatformPaths,
    dependencies: StartupDependencies,
) -> Result<SecondaryRuntime, StartupError> {
    let deadline = Instant::now() + SECONDARY_DEADLINE;
    let mut delay = SECONDARY_INITIAL_DELAY;

    loop {
        if Instant::now() >= deadline {
            return Err(StartupError::PrimaryUnverified);
        }

        if let Ok(descriptor) = RuntimeDescriptor::read(&paths.instance_descriptor)
            && !process_is_dead(descriptor.pid())
            && let Ok((status, Some(probe))) =
                crate::local_client::probe_ready(&descriptor, deadline).await
            && status == http::StatusCode::OK
            && probe.instance_id == descriptor.instance_id()
            && probe.state == StartupPhase::Ready
            && let Ok((status, Some(grant))) =
                crate::local_client::request_reopen(&descriptor, deadline).await
            && status == http::StatusCode::OK
            && let Some(browser_port) = dependencies
                .public_origin
                .browser_port(descriptor.port().get())
            && let Some(token) =
                validate_reopen_grant(&grant, dependencies.wall_clock.now_utc(), browser_port)
        {
            let browser_opened = open_browser_or_report(
                &*dependencies.browser,
                &*dependencies.messages,
                browser_port,
                &token,
            );
            return Ok(SecondaryRuntime {
                instance_id: descriptor.instance_id(),
                browser_opened,
            });
        }

        let wake = (Instant::now() + delay).min(deadline);
        tokio::time::sleep_until(wake).await;
        delay = delay.saturating_mul(2).min(SECONDARY_MAX_DELAY);
    }
}

fn validate_reopen_grant(
    grant: &crate::local_client::ReopenGrant,
    now: time::OffsetDateTime,
    browser_port: u16,
) -> Option<String> {
    let prefix = BrowserLauncher::url(browser_port, "");
    let token = grant.url.strip_prefix(&prefix)?;
    validate_launcher_secret(token).ok()?;
    if BrowserLauncher::url(browser_port, token) != grant.url {
        return None;
    }

    let expires_at = grant.expires_at.as_offset_date_time();
    if expires_at < now.saturating_add(time::Duration::seconds(110))
        || expires_at > now.saturating_add(time::Duration::seconds(121))
    {
        return None;
    }
    Some(token.to_owned())
}

fn canonical_loopback_origin_port(public_origin: &str) -> Option<u16> {
    let uri = public_origin.parse::<http::Uri>().ok()?;
    let authority = uri.authority()?;
    let port = authority.port_u16().filter(|port| *port != 0)?;
    (uri.scheme_str() == Some("http")
        && authority.host() == "127.0.0.1"
        && public_origin == format!("http://127.0.0.1:{port}"))
    .then_some(port)
}

fn open_browser_or_report(
    browser: &dyn BrowserOpener,
    messages: &dyn NativeMessageSink,
    port: u16,
    token: &str,
) -> bool {
    let url = BrowserLauncher::url(port, token);
    if browser.open(port, token).is_ok() {
        true
    } else {
        messages.show_error(
            "Open Coding Agent",
            format!(
                "The browser could not be opened automatically. Copy and open this complete URL:\n\n{url}"
            ),
        );
        false
    }
}

async fn bind_loopback(factory: &dyn ListenerFactory) -> Result<TcpListener, StartupError> {
    let requested = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
    let mut last_error = None;
    for _ in 0..LISTENER_BIND_ATTEMPTS {
        match factory.bind(requested).await {
            Ok(listener) => match listener.local_addr() {
                Ok(SocketAddr::V4(address))
                    if *address.ip() == Ipv4Addr::LOCALHOST && address.port() != 0 =>
                {
                    return Ok(listener);
                }
                Ok(_) => return Err(StartupError::InvalidListener),
                Err(error) => last_error = Some(error),
            },
            Err(error) => last_error = Some(error),
        }
    }
    Err(StartupError::Listener(last_error.unwrap_or_else(|| {
        io::Error::other("listener factory exhausted without an attempt")
    })))
}

fn remove_stale_descriptors(paths: &PlatformPaths) -> Result<(), RuntimeDescriptorError> {
    if let Err(error) = std::fs::remove_file(&paths.instance_descriptor)
        && error.kind() != io::ErrorKind::NotFound
    {
        return Err(error.into());
    }

    let Some(file_name) = paths.instance_descriptor.file_name() else {
        return Err(RuntimeDescriptorError::Invalid(
            "descriptor path has no file name",
        ));
    };
    let prefix = format!("{}.", file_name.to_string_lossy());
    for entry in std::fs::read_dir(&paths.runtime_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(&prefix) && name.ends_with(".tmp") {
            match std::fs::remove_file(entry.path()) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
fn process_is_dead(pid: NonZeroU32) -> bool {
    let result = unsafe { libc::kill(pid.get() as libc::pid_t, 0) };
    result != 0 && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
}

#[cfg(windows)]
fn process_is_dead(pid: NonZeroU32) -> bool {
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_INVALID_PARAMETER, WAIT_OBJECT_0, WAIT_TIMEOUT,
    };
    use windows_sys::Win32::System::Threading::{
        OpenProcess, PROCESS_SYNCHRONIZE, WaitForSingleObject,
    };

    let process = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, pid.get()) };
    if process.is_null() {
        return io::Error::last_os_error().raw_os_error() == Some(ERROR_INVALID_PARAMETER as i32);
    }
    let status = unsafe { WaitForSingleObject(process, 0) };
    unsafe { CloseHandle(process) };
    match status {
        WAIT_OBJECT_0 => true,
        WAIT_TIMEOUT => false,
        _ => false,
    }
}

#[cfg(not(any(unix, windows)))]
fn process_is_dead(_pid: NonZeroU32) -> bool {
    false
}

struct ServerRuntime {
    shutdown: CancellationToken,
    task: Option<JoinHandle<io::Result<()>>>,
}

impl ServerRuntime {
    fn spawn(listener: TcpListener, router: Router, lock: Arc<LockLease>) -> Self {
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            let _lock = lock;
            axum::serve(listener, router)
                .with_graceful_shutdown(server_shutdown.cancelled_owned())
                .await
        });
        Self {
            shutdown,
            task: Some(task),
        }
    }

    async fn shutdown(mut self, deadline: Instant) {
        self.shutdown.cancel();
        if let Some(mut task) = self.task.take()
            && tokio::time::timeout_at(deadline, &mut task).await.is_err()
        {
            task.abort();
            let _ = task.await;
        }
    }

    fn stop(&mut self) {
        self.shutdown.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl Drop for ServerRuntime {
    fn drop(&mut self) {
        self.stop();
    }
}

struct PrimaryRuntimeCleanup {
    descriptor_path: PathBuf,
    instance_process_scope: ProcessLivenessScope,
    runtime_actors_installed: AtomicBool,
    resources: Mutex<PrimaryRuntimeResources>,
}

struct PrimaryRuntimeResources {
    server: Option<ServerRuntime>,
    lock: Option<InstanceLock>,
}

impl PrimaryRuntimeCleanup {
    fn new(
        lock: InstanceLock,
        descriptor_path: PathBuf,
        instance_process_scope: ProcessLivenessScope,
    ) -> Self {
        Self {
            descriptor_path,
            instance_process_scope,
            runtime_actors_installed: AtomicBool::new(false),
            resources: Mutex::new(PrimaryRuntimeResources {
                server: None,
                lock: Some(lock),
            }),
        }
    }

    fn mark_runtime_actors_installed(&self) {
        self.runtime_actors_installed.store(true, Ordering::Release);
    }

    fn lock_keepalive(&self) -> Arc<LockLease> {
        self.resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .lock
            .as_ref()
            .expect("a live primary cleanup owner retains the instance lock")
            .keepalive()
    }

    fn install_server(&self, server: ServerRuntime) {
        let mut resources = self
            .resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            resources.lock.is_some() && resources.server.is_none(),
            "primary runtime server can only be installed on a live owner"
        );
        resources.server = Some(server);
    }

    fn remove_descriptor(&self) {
        if let Err(error) = std::fs::remove_file(&self.descriptor_path)
            && error.kind() != io::ErrorKind::NotFound
        {
            tracing::warn!(
                error_code = "DESCRIPTOR_REMOVE_FAILED",
                "runtime descriptor cleanup failed"
            );
        }
    }

    fn stop_http_now(&self) {
        let mut resources = self
            .resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(mut server) = resources.server.take() {
            server.stop();
        }
    }

    fn release_lock(&self) {
        let lock = self
            .resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .lock
            .take();
        drop(lock);
    }

    fn retain_unreleased_runtime_until_process_exit(&self) {
        let mut resources = self
            .resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(lock) = resources.lock.take() else {
            return;
        };
        if let Some(mut server) = resources.server.take() {
            server.stop();
        }
        drop(resources);
        self.remove_descriptor();
        // If every asynchronous fail-safe has itself failed, leaking the
        // OS-backed lease is safer than allowing a replacement primary to
        // overlap process trees whose cleanup was never proven. The OS
        // releases the lease when this process exits.
        std::mem::forget(lock);
    }

    fn finish_abandoned_startup(&self) {
        let owns_lock = self
            .resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .lock
            .is_some();
        if !owns_lock {
            return;
        }
        self.stop_http_now();
        self.remove_descriptor();
        if self.runtime_actors_installed.load(Ordering::Acquire) {
            self.retain_unreleased_runtime_until_process_exit();
            return;
        }
        let sealed_scope = match self.instance_process_scope.seal_instance_scope() {
            Ok(scope) => scope,
            Err(_) => {
                self.retain_unreleased_runtime_until_process_exit();
                return;
            }
        };
        match sealed_scope.cleanup_proof() {
            Ok(ProcessCleanupProof::Confirmed) => self.release_lock(),
            Ok(ProcessCleanupProof::Held | ProcessCleanupProof::Unknown) | Err(_) => {
                self.retain_until_instance_cleanup(sealed_scope);
            }
        }
    }

    fn retain_until_instance_cleanup(&self, sealed_scope: SealedProcessLivenessScope) {
        let lock = self
            .resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .lock
            .take();
        let Some(lock) = lock else {
            return;
        };
        let retained_lock = FailClosedInstanceLock::new(lock);
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            drop(retained_lock);
            return;
        };
        handle.spawn(release_lock_after_instance_cleanup(
            retained_lock,
            sealed_scope,
        ));
    }
}

impl Drop for PrimaryRuntimeCleanup {
    fn drop(&mut self) {
        self.finish_abandoned_startup();
    }
}

#[async_trait::async_trait]
impl ShutdownCleanup for PrimaryRuntimeCleanup {
    async fn stop_http(&self, deadline: Instant) {
        let server = self
            .resources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .server
            .take();
        if let Some(server) = server {
            server.shutdown(deadline).await;
        }
    }

    fn stop_http_now(&self) {
        PrimaryRuntimeCleanup::stop_http_now(self);
    }

    fn unpublish_descriptor(&self) {
        self.remove_descriptor();
    }

    fn finish_lock(
        &self,
        _proof: ShutdownRuntimeCleanupProof,
        disposition: ShutdownLockDisposition,
    ) {
        match disposition {
            ShutdownLockDisposition::ReleaseNow => PrimaryRuntimeCleanup::release_lock(self),
            ShutdownLockDisposition::RetainUntilProcessExit => {
                self.retain_unreleased_runtime_until_process_exit();
            }
        }
    }
}

async fn release_lock_after_instance_cleanup(
    mut lock: FailClosedInstanceLock,
    sealed_scope: SealedProcessLivenessScope,
) {
    loop {
        if matches!(
            sealed_scope.cleanup_proof(),
            Ok(ProcessCleanupProof::Confirmed)
        ) {
            lock.release_after_proof();
            return;
        }
        tokio::time::sleep(PROCESS_CLEANUP_RETRY_INTERVAL).await;
    }
}

struct FailClosedInstanceLock {
    lock: Option<InstanceLock>,
}

impl FailClosedInstanceLock {
    fn new(lock: InstanceLock) -> Self {
        Self { lock: Some(lock) }
    }

    fn release_after_proof(&mut self) {
        drop(self.lock.take());
    }
}

impl Drop for FailClosedInstanceLock {
    fn drop(&mut self) {
        if let Some(lock) = self.lock.take() {
            std::mem::forget(lock);
        }
    }
}

struct SystemStartupPaths;

impl StartupPaths for SystemStartupPaths {
    fn discover(&self) -> io::Result<PlatformPaths> {
        PlatformPaths::discover()
    }

    fn prepare_lock_parent(&self, paths: &PlatformPaths) -> io::Result<()> {
        paths.prepare_runtime_directory()
    }

    fn prepare(&self, paths: &PlatformPaths) -> io::Result<()> {
        paths.prepare()
    }
}

struct SystemStoreFactory;

#[async_trait::async_trait]
impl StoreFactory for SystemStoreFactory {
    async fn open(&self, path: &Path) -> Result<Store, StoreError> {
        Store::open(path).await
    }
}

struct SystemListenerFactory;

#[async_trait::async_trait]
impl ListenerFactory for SystemListenerFactory {
    async fn bind(&self, address: SocketAddrV4) -> io::Result<TcpListener> {
        TcpListener::bind(address).await
    }
}

struct SystemBrowserOpener;

impl BrowserOpener for SystemBrowserOpener {
    fn open(&self, port: u16, token: &str) -> Result<(), BrowserLaunchError> {
        BrowserLauncher::open(port, token)
    }
}

struct SystemNativeMessageSink;

impl NativeMessageSink for SystemNativeMessageSink {
    fn show_error(&self, title: &'static str, body: String) {
        let _ = rfd::MessageDialog::new()
            .set_level(rfd::MessageLevel::Error)
            .set_title(title)
            .set_description(body)
            .show();
    }

    fn publish_degraded_shutdown(&self) -> io::Result<()> {
        let executable = std::env::current_exe()?;
        let mut command = Command::new(executable);
        command
            .arg(DEGRADED_SHUTDOWN_WARNING_ARGUMENT)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());

        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt as _;
            use windows_sys::Win32::System::Threading::{
                CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW,
            };

            command.creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
        }

        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt as _;

            command.process_group(0);
        }

        let mut child = {
            let _spawn_guard = coding_agent_runtime::acquire_process_spawn_lock();
            command.spawn()?
        };
        let mut ready = [0_u8; 1];
        child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("shutdown warning helper stdout was unavailable"))?
            .read_exact(&mut ready)?;
        if ready == *b"R" {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shutdown warning helper returned an invalid acknowledgement",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_cleanup_drop_preserves_a_replacement_descriptor() {
        let temp = tempfile::tempdir().expect("create cleanup fixture");
        let lock_path = temp.path().join("instance.lock");
        let descriptor_path = temp.path().join("runtime.json");
        let lock = InstanceLock::try_acquire(&lock_path)
            .expect("acquire cleanup fixture lock")
            .expect("cleanup fixture lock is available");
        let process_scope = ProcessLivenessDirectory::open(temp.path())
            .expect("open cleanup fixture process-liveness directory")
            .instance_scope(*Uuid::new_v4().as_bytes())
            .expect("create cleanup fixture process-liveness scope");
        std::fs::write(&descriptor_path, b"old descriptor")
            .expect("write owned descriptor fixture");

        let cleanup = Arc::new(PrimaryRuntimeCleanup::new(
            lock,
            descriptor_path.clone(),
            process_scope,
        ));
        cleanup.stop_http_now();
        cleanup.remove_descriptor();
        cleanup.release_lock();
        assert!(
            !descriptor_path.exists(),
            "the first cleanup must remove the descriptor it owns"
        );

        std::fs::write(&descriptor_path, b"replacement descriptor")
            .expect("publish replacement descriptor fixture");
        drop(cleanup);

        assert_eq!(
            std::fs::read(&descriptor_path).expect("read replacement descriptor"),
            b"replacement descriptor",
            "dropping an already-cleaned owner must not remove a later primary's descriptor"
        );
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn aborting_startup_cleanup_worker_keeps_the_lock_fail_closed() {
        let temp = tempfile::tempdir().expect("create fail-closed cleanup fixture");
        let lock_path = temp.path().join("instance.lock");
        let lock = InstanceLock::try_acquire(&lock_path)
            .expect("acquire fail-closed cleanup lock")
            .expect("fail-closed cleanup lock is available");
        let instance = ProcessLivenessDirectory::open(temp.path())
            .expect("open fail-closed process-liveness directory")
            .instance_scope(*Uuid::new_v4().as_bytes())
            .expect("create fail-closed process-liveness scope");
        let held_tree = instance
            .hold_tree_for_test()
            .expect("hold startup process tree");
        let sealed = instance
            .seal_instance_scope()
            .expect("seal abandoned startup instance");
        let worker = tokio::spawn(release_lock_after_instance_cleanup(
            FailClosedInstanceLock::new(lock),
            sealed,
        ));
        tokio::task::yield_now().await;

        worker.abort();
        let _ = worker.await;
        assert!(
            InstanceLock::try_acquire(&lock_path)
                .expect("probe fail-closed startup lock")
                .is_none(),
            "aborting the proof worker must retain the OS lease until process exit"
        );
        drop(held_tree);
        assert!(
            InstanceLock::try_acquire(&lock_path)
                .expect("probe leaked fail-closed startup lock")
                .is_none(),
            "a cancelled proof worker cannot later release the retained lease"
        );
        let _retained_fixture = temp.keep();
    }

    #[test]
    fn abandoning_cleanup_after_runtime_actors_exist_retains_the_lock_fail_closed() {
        let temp = tempfile::tempdir().expect("create actor-stage cleanup fixture");
        let lock_path = temp.path().join("instance.lock");
        let lock = InstanceLock::try_acquire(&lock_path)
            .expect("acquire actor-stage cleanup lock")
            .expect("actor-stage cleanup lock is available");
        let instance = ProcessLivenessDirectory::open(temp.path())
            .expect("open actor-stage process-liveness directory")
            .instance_scope(*Uuid::new_v4().as_bytes())
            .expect("create actor-stage process-liveness scope");
        let cleanup = PrimaryRuntimeCleanup::new(lock, temp.path().join("runtime.json"), instance);
        cleanup.mark_runtime_actors_installed();

        drop(cleanup);

        assert!(
            InstanceLock::try_acquire(&lock_path)
                .expect("probe actor-stage cleanup lock")
                .is_none(),
            "process cleanup alone cannot release the lease after in-process actors exist"
        );
        let _retained_fixture = temp.keep();
    }

    #[test]
    fn degraded_warning_helper_requires_the_exact_private_switch() {
        assert!(is_degraded_shutdown_warning_invocation([
            "coding-agent",
            DEGRADED_SHUTDOWN_WARNING_ARGUMENT,
        ]));
        assert!(!is_degraded_shutdown_warning_invocation(["coding-agent"]));
        assert!(!is_degraded_shutdown_warning_invocation([
            "coding-agent",
            DEGRADED_SHUTDOWN_WARNING_ARGUMENT,
            "unexpected",
        ]));
        assert!(!is_degraded_shutdown_warning_invocation([
            "coding-agent",
            "--not-the-private-warning-switch",
        ]));
    }

    #[test]
    fn reopen_grant_accepts_only_the_exact_fresh_loopback_fragment_url() {
        let now = time::macros::datetime!(2026-07-15 00:00 UTC);
        let seed = SecuritySeed::generate().expect("generate grant fixture secrets");
        let token = seed.initial_launch_token().as_str().to_owned();
        let descriptor = RuntimeDescriptor::new(
            Uuid::new_v4(),
            NonZeroU32::new(std::process::id()).expect("nonzero test PID"),
            NonZeroU16::new(43_121).expect("nonzero test port"),
            UtcTimestamp::new(now).expect("current timestamp"),
            seed.launcher_secret().clone(),
        )
        .expect("construct grant fixture descriptor");
        let expires_at = UtcTimestamp::new(now.saturating_add(time::Duration::seconds(120)))
            .expect("construct fresh expiration");
        let valid = crate::local_client::ReopenGrant {
            url: BrowserLauncher::url(descriptor.port().get(), &token),
            expires_at,
        };
        assert_eq!(
            validate_reopen_grant(&valid, now, descriptor.port().get()).as_deref(),
            Some(token.as_str())
        );

        let development = crate::local_client::ReopenGrant {
            url: BrowserLauncher::url(5_173, &token),
            expires_at,
        };
        assert_eq!(
            validate_reopen_grant(&development, now, 5_173).as_deref(),
            Some(token.as_str())
        );
        assert!(
            validate_reopen_grant(&valid, now, 5_173).is_none(),
            "a listener-origin grant must not be accepted in development"
        );

        for url in [
            format!("http://localhost:{}/#token={token}", descriptor.port()),
            format!("http://127.0.0.1:{}/?token={token}", descriptor.port()),
            format!("http://127.0.0.1:{}/#token={token}extra", descriptor.port()),
        ] {
            let invalid = crate::local_client::ReopenGrant { url, expires_at };
            assert!(validate_reopen_grant(&invalid, now, descriptor.port().get()).is_none());
        }

        let stale = crate::local_client::ReopenGrant {
            url: valid.url,
            expires_at: UtcTimestamp::new(now.saturating_add(time::Duration::seconds(5)))
                .expect("construct stale expiration"),
        };
        assert!(validate_reopen_grant(&stale, now, descriptor.port().get()).is_none());
    }
}
