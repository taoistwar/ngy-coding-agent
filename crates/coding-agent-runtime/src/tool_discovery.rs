use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::command_policy::{ExecutionDirectory, PinnedExecutable, ValidatedCommand};
use crate::process_supervisor::{ChildEnvironment, ProcessError, ProcessLimits, ProcessSupervisor};

const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(15);
const BOOTSTRAP_CLEANUP_TIMEOUT: Duration = Duration::from_secs(3);
const BOOTSTRAP_STREAM_LIMIT: usize = 16 * 1024;

/// Executables and environment roots fixed once during trusted application startup.
///
/// Executable handles are retained so typed command adapters can revalidate the
/// namespace entry before every spawn. Paths are deliberately omitted from the
/// `Debug` representation because startup diagnostics can be user-visible.
#[derive(Clone)]
pub struct ToolchainPaths {
    cargo: Arc<PinnedExecutable>,
    rustc: Arc<PinnedExecutable>,
    rustdoc: Arc<PinnedExecutable>,
    git: Arc<PinnedExecutable>,
    search_directories: Vec<PathBuf>,
    cargo_home: PathBuf,
}

impl ToolchainPaths {
    pub fn cargo(&self) -> Arc<PinnedExecutable> {
        Arc::clone(&self.cargo)
    }

    pub fn rustc(&self) -> Arc<PinnedExecutable> {
        Arc::clone(&self.rustc)
    }

    pub fn rustdoc(&self) -> Arc<PinnedExecutable> {
        Arc::clone(&self.rustdoc)
    }

    pub fn git(&self) -> Arc<PinnedExecutable> {
        Arc::clone(&self.git)
    }

    pub fn search_directories(&self) -> &[PathBuf] {
        &self.search_directories
    }

    pub fn cargo_home(&self) -> &Path {
        &self.cargo_home
    }
}

impl fmt::Debug for ToolchainPaths {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolchainPaths")
            .field("cargo", &"<pinned>")
            .field("rustc", &"<pinned>")
            .field("rustdoc", &"<pinned>")
            .field("git", &"<pinned>")
            .field("search_directory_count", &self.search_directories.len())
            .field("cargo_home", &"<validated>")
            .finish()
    }
}

/// Secret-safe failures from trusted startup tool discovery.
///
/// These variants intentionally carry neither environment values nor local
/// paths. Detailed OS errors must only be logged after the application's common
/// redaction layer has classified them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ToolDiscoveryError {
    #[error("the neutral runtime directory is invalid")]
    RuntimeDirectoryInvalid,
    #[error("the host executable search path is missing or invalid")]
    HostPathInvalid,
    #[error("the bootstrap Rust compiler was not found")]
    BootstrapRustcMissing,
    #[error("the bootstrap Rust compiler path is invalid")]
    BootstrapRustcInvalid,
    #[error("the bootstrap Git executable was not found")]
    BootstrapGitMissing,
    #[error("the bootstrap Git executable path is invalid")]
    BootstrapGitInvalid,
    #[error("the bootstrap Git executable could not be started")]
    BootstrapGitSpawnFailed,
    #[error("the bootstrap Git executable did not finish before its deadline")]
    BootstrapGitTimedOut,
    #[error("the bootstrap Git executable could not be monitored")]
    BootstrapGitWaitFailed,
    #[error("the bootstrap Git executable output could not be read")]
    BootstrapGitOutputFailed,
    #[error("the bootstrap Git executable produced too much output")]
    BootstrapGitOutputTooLarge,
    #[error("the bootstrap Git executable failed")]
    BootstrapGitFailed,
    #[error("the Git version response is malformed")]
    GitVersionMalformed,
    #[error("Git 2.45 or newer is required")]
    GitVersionUnsupported,
    #[error("the Cargo home directory is missing or invalid")]
    CargoHomeInvalid,
    #[error("the bootstrap process environment is invalid")]
    BootstrapEnvironmentInvalid,
    #[error("the bootstrap Rust compiler could not be started")]
    BootstrapSpawnFailed,
    #[error("the bootstrap Rust compiler did not finish before its deadline")]
    BootstrapTimedOut,
    #[error("the bootstrap Rust compiler could not be monitored")]
    BootstrapWaitFailed,
    #[error("the bootstrap Rust compiler output could not be read")]
    BootstrapOutputFailed,
    #[error("the bootstrap Rust compiler produced too much output")]
    BootstrapOutputTooLarge,
    #[error("the bootstrap Rust compiler failed")]
    BootstrapRustcFailed,
    #[error("the Rust sysroot response is malformed")]
    SysrootMalformed,
    #[error("the Rust sysroot directory is missing or invalid")]
    SysrootInvalid,
    #[error("the sysroot Cargo executable is missing or invalid")]
    CargoInvalid,
    #[error("the sysroot Rust compiler is missing or invalid")]
    RustcInvalid,
    #[error("the sysroot Rust documentation tool is missing or invalid")]
    RustdocInvalid,
}

impl ToolDiscoveryError {
    pub const fn code(self) -> &'static str {
        "TOOLCHAIN_DISCOVERY_FAILED"
    }
}

/// Discovers and pins the only Git and Rust executables available to typed tools.
///
/// `runtime_safe_cwd` must be an application-owned neutral directory, not a
/// repository. Optional bootstrap paths bypass host `PATH` lookup for that tool.
/// The host `PATH` is snapshotted at most once and is never forwarded to the
/// bootstrap child.
pub async fn discover(
    runtime_safe_cwd: impl AsRef<Path>,
    explicit_bootstrap_rustc: Option<&Path>,
    explicit_bootstrap_git: Option<&Path>,
) -> Result<ToolchainPaths, ToolDiscoveryError> {
    let runtime_safe_cwd = canonical_existing_directory(
        runtime_safe_cwd.as_ref(),
        ToolDiscoveryError::RuntimeDirectoryInvalid,
    )?;

    let needs_host_path = explicit_bootstrap_rustc.is_none() || explicit_bootstrap_git.is_none();
    let host_path = if needs_host_path {
        std::env::var_os("PATH")
            .ok_or(ToolDiscoveryError::HostPathInvalid)
            .and_then(|value| parse_host_path(&value))?
    } else {
        Vec::new()
    };

    let cargo_home = discover_cargo_home()?;
    let rustup_home = discover_optional_rustup_home()?;
    let bootstrap_rustc = resolve_bootstrap_executable(
        ToolRole::BootstrapRustc,
        explicit_bootstrap_rustc,
        &host_path,
        rustup_home.as_deref(),
    )?;
    let git = resolve_bootstrap_executable(
        ToolRole::Git,
        explicit_bootstrap_git,
        &host_path,
        rustup_home.as_deref(),
    )?;
    let bootstrap_environment =
        BootstrapEnvironment::from_host(&cargo_home, rustup_home.as_deref())?;
    let runtime_safe_cwd = Arc::new(
        ExecutionDirectory::open(&runtime_safe_cwd)
            .map_err(|_| ToolDiscoveryError::RuntimeDirectoryInvalid)?,
    );

    let git_version_output = query_git_version(
        &git,
        Arc::clone(&runtime_safe_cwd),
        bootstrap_environment.clone(),
    )
    .await?;
    require_supported_git_version(&git_version_output)?;

    let known_sysroot = bootstrap_rustc.known_sysroot.clone();
    let sysroot = if let Some(known_sysroot) = known_sysroot {
        known_sysroot
    } else {
        let sysroot_output =
            query_sysroot(bootstrap_rustc, runtime_safe_cwd, bootstrap_environment).await?;
        parse_sysroot_line(&sysroot_output)?
    };
    let sysroot = canonical_existing_directory(&sysroot, ToolDiscoveryError::SysrootInvalid)?;
    let bin =
        canonical_existing_directory(&sysroot.join("bin"), ToolDiscoveryError::SysrootInvalid)?;

    let cargo = pin_sysroot_tool(&bin, ToolRole::Cargo)?;
    let rustc = pin_sysroot_tool(&bin, ToolRole::Rustc)?;
    let rustdoc = pin_sysroot_tool(&bin, ToolRole::Rustdoc)?;

    let mut search_directories = vec![bin];
    if let Some(parent) = git.pinned.path().parent() {
        push_canonical_search_directory(&mut search_directories, parent);
    }
    for directory in trusted_system_search_directories() {
        push_canonical_search_directory(&mut search_directories, &directory);
    }

    Ok(ToolchainPaths {
        cargo: Arc::new(cargo),
        rustc: Arc::new(rustc),
        rustdoc: Arc::new(rustdoc),
        git: Arc::new(git.pinned),
        search_directories,
        cargo_home,
    })
}

#[derive(Debug, Clone, Copy)]
enum ToolRole {
    BootstrapRustc,
    Git,
    Cargo,
    Rustc,
    Rustdoc,
}

impl ToolRole {
    fn executable_name(self) -> &'static OsStr {
        #[cfg(windows)]
        match self {
            Self::BootstrapRustc | Self::Rustc => OsStr::new("rustc.exe"),
            Self::Git => OsStr::new("git.exe"),
            Self::Cargo => OsStr::new("cargo.exe"),
            Self::Rustdoc => OsStr::new("rustdoc.exe"),
        }
        #[cfg(unix)]
        match self {
            Self::BootstrapRustc | Self::Rustc => OsStr::new("rustc"),
            Self::Git => OsStr::new("git"),
            Self::Cargo => OsStr::new("cargo"),
            Self::Rustdoc => OsStr::new("rustdoc"),
        }
    }

    const fn missing_error(self) -> ToolDiscoveryError {
        match self {
            Self::BootstrapRustc => ToolDiscoveryError::BootstrapRustcMissing,
            Self::Git => ToolDiscoveryError::BootstrapGitMissing,
            Self::Cargo => ToolDiscoveryError::CargoInvalid,
            Self::Rustc => ToolDiscoveryError::RustcInvalid,
            Self::Rustdoc => ToolDiscoveryError::RustdocInvalid,
        }
    }

    const fn invalid_error(self) -> ToolDiscoveryError {
        match self {
            Self::BootstrapRustc => ToolDiscoveryError::BootstrapRustcInvalid,
            Self::Git => ToolDiscoveryError::BootstrapGitInvalid,
            Self::Cargo => ToolDiscoveryError::CargoInvalid,
            Self::Rustc => ToolDiscoveryError::RustcInvalid,
            Self::Rustdoc => ToolDiscoveryError::RustdocInvalid,
        }
    }
}

struct BootstrapExecutable {
    pinned: PinnedExecutable,
    known_sysroot: Option<PathBuf>,
}

#[derive(Debug, PartialEq, Eq)]
struct ConcreteRustupRustc {
    executable: PathBuf,
    sysroot: PathBuf,
}

fn resolve_bootstrap_executable(
    role: ToolRole,
    explicit_path: Option<&Path>,
    host_path: &[PathBuf],
    rustup_home: Option<&Path>,
) -> Result<BootstrapExecutable, ToolDiscoveryError> {
    let candidate = if let Some(path) = explicit_path {
        validate_unambiguous_absolute_path(path).map_err(|_| role.invalid_error())?;
        path.to_owned()
    } else {
        first_path_candidate(host_path, role.executable_name())
            .ok_or_else(|| role.missing_error())?
    };

    // Host PATH entries are bootstrap input only. Resolve a trusted shim once,
    // then pin and invoke the canonical object; child processes never search it.
    let mut canonical = std::fs::canonicalize(&candidate).map_err(|_| role.invalid_error())?;
    let mut known_sysroot = None;
    if is_rustup_rustc_proxy(role, &candidate, &canonical) {
        // A retained executable descriptor has a kernel-visible `/proc/self/fd`
        // or `/dev/fd` path on Unix, while canonicalization exposes `rustup.exe`
        // on Windows. Neither preserves rustup's proxy filename reliably. Read
        // only rustup's bounded, host-owned default setting and pin that already-
        // installed toolchain's concrete compiler. No repository override,
        // inherited working directory, or network lookup participates.
        let concrete =
            concrete_rustup_rustc(rustup_home.ok_or(ToolDiscoveryError::BootstrapRustcInvalid)?)?;
        canonical = concrete.executable;
        known_sysroot = Some(concrete.sysroot);
    } else if matches!(role, ToolRole::BootstrapRustc) {
        known_sysroot = matching_managed_rustup_sysroot(rustup_home, &canonical);
    }
    #[cfg(windows)]
    if matches!(role, ToolRole::Git) {
        canonical = concrete_windows_git(&candidate, canonical)?;
    }
    validate_unambiguous_absolute_path(&canonical).map_err(|_| role.invalid_error())?;
    let pinned = PinnedExecutable::open(&canonical).map_err(|_| role.invalid_error())?;

    Ok(BootstrapExecutable {
        pinned,
        known_sysroot,
    })
}

fn has_file_name(path: &Path, expected: &str) -> bool {
    let Some(actual) = path.file_name().and_then(OsStr::to_str) else {
        return false;
    };
    #[cfg(windows)]
    {
        actual.eq_ignore_ascii_case(expected)
    }
    #[cfg(not(windows))]
    {
        actual == expected
    }
}

fn is_rustup_rustc_proxy(role: ToolRole, candidate: &Path, canonical: &Path) -> bool {
    #[cfg(windows)]
    let (rustc_name, rustup_name) = ("rustc.exe", "rustup.exe");
    #[cfg(not(windows))]
    let (rustc_name, rustup_name) = ("rustc", "rustup");

    if !matches!(role, ToolRole::BootstrapRustc) || !has_file_name(candidate, rustc_name) {
        return false;
    }
    if has_file_name(canonical, rustup_name) {
        return true;
    }

    // rustup historically preferred hard-linked proxies and still permits
    // them as a fallback. Canonicalization cannot distinguish those names, so
    // compare the candidate with the fixed sibling `rustup` object on Unix.
    #[cfg(unix)]
    {
        candidate
            .parent()
            .is_some_and(|parent| same_unix_file(candidate, &parent.join(rustup_name)))
    }
    #[cfg(windows)]
    {
        false
    }
}

#[cfg(unix)]
fn same_unix_file(left: &Path, right: &Path) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    let (Ok(left), Ok(right)) = (std::fs::metadata(left), std::fs::metadata(right)) else {
        return false;
    };
    left.is_file() && right.is_file() && left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(windows)]
fn concrete_windows_git(
    bootstrap_candidate: &Path,
    canonical_bootstrap: PathBuf,
) -> Result<PathBuf, ToolDiscoveryError> {
    let Some(candidates) = windows_git_for_windows_layout_candidates(bootstrap_candidate) else {
        return Ok(canonical_bootstrap);
    };
    let installation_root = bootstrap_candidate
        .parent()
        .and_then(Path::parent)
        .ok_or(ToolDiscoveryError::BootstrapGitInvalid)?;
    let canonical_root =
        canonical_existing_directory(installation_root, ToolDiscoveryError::BootstrapGitInvalid)?;

    // Git for Windows puts a small native launcher on PATH under `cmd/`.
    // Under a supervised Windows job that launcher can detach its MSYS Git
    // child, defeating the process-tree lifetime guarantee. Prefer the real
    // architecture-specific Git image inside the same canonical installation.
    // No PATH search or child process is involved in this resolution.
    for candidate in candidates {
        match std::fs::symlink_metadata(&candidate) {
            Ok(_) => {
                let concrete = std::fs::canonicalize(candidate)
                    .map_err(|_| ToolDiscoveryError::BootstrapGitInvalid)?;
                if !concrete.starts_with(&canonical_root) {
                    return Err(ToolDiscoveryError::BootstrapGitInvalid);
                }
                return Ok(concrete);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return Err(ToolDiscoveryError::BootstrapGitInvalid),
        }
    }
    Ok(canonical_bootstrap)
}

#[cfg(windows)]
fn windows_git_for_windows_layout_candidates(candidate: &Path) -> Option<[PathBuf; 2]> {
    if !has_file_name(candidate, "git.exe")
        || !candidate
            .parent()
            .is_some_and(|parent| has_file_name(parent, "cmd"))
    {
        return None;
    }
    let root = candidate.parent()?.parent()?;
    #[cfg(target_pointer_width = "64")]
    let architectures = ["mingw64", "mingw32"];
    #[cfg(not(target_pointer_width = "64"))]
    let architectures = ["mingw32", "mingw64"];
    Some(architectures.map(|architecture| root.join(architecture).join("bin").join("git.exe")))
}

fn concrete_rustup_rustc(rustup_home: &Path) -> Result<ConcreteRustupRustc, ToolDiscoveryError> {
    const SETTINGS_LIMIT: u64 = 64 * 1024;

    let root = crate::RootCapability::open(rustup_home)
        .map_err(|_| ToolDiscoveryError::BootstrapRustcInvalid)?;
    let settings_path = crate::RelativePath::parse("settings.toml".to_owned())
        .map_err(|_| ToolDiscoveryError::BootstrapRustcInvalid)?;
    let mut settings_file = root
        .open_file_for_read(&settings_path)
        .map_err(|_| ToolDiscoveryError::BootstrapRustcInvalid)?;
    let metadata = settings_file
        .metadata()
        .map_err(|_| ToolDiscoveryError::BootstrapRustcInvalid)?;
    if !metadata.is_file() || metadata.len() > SETTINGS_LIMIT {
        return Err(ToolDiscoveryError::BootstrapRustcInvalid);
    }
    let mut encoded = Vec::with_capacity(metadata.len() as usize);
    (&mut settings_file)
        .take(SETTINGS_LIMIT + 1)
        .read_to_end(&mut encoded)
        .map_err(|_| ToolDiscoveryError::BootstrapRustcInvalid)?;
    if encoded.len() as u64 > SETTINGS_LIMIT {
        return Err(ToolDiscoveryError::BootstrapRustcInvalid);
    }
    let settings =
        std::str::from_utf8(&encoded).map_err(|_| ToolDiscoveryError::BootstrapRustcInvalid)?;
    let toolchain = parse_default_rustup_toolchain(settings)
        .ok_or(ToolDiscoveryError::BootstrapRustcInvalid)?;
    concrete_rustup_rustc_path(rustup_home, toolchain)
}

fn concrete_rustup_rustc_path(
    rustup_home: &Path,
    toolchain: &str,
) -> Result<ConcreteRustupRustc, ToolDiscoveryError> {
    if !is_safe_rustup_toolchain_name(toolchain) {
        return Err(ToolDiscoveryError::BootstrapRustcInvalid);
    }
    let rustup_home =
        canonical_existing_directory(rustup_home, ToolDiscoveryError::BootstrapRustcInvalid)?;
    let toolchains = canonical_existing_directory(
        &rustup_home.join("toolchains"),
        ToolDiscoveryError::BootstrapRustcInvalid,
    )?;
    // Accept only rustup's managed `<home>/toolchains/<default>/bin` layout.
    // Exact canonical parents reject linked custom toolchains, while the final
    // component containment check rejects a compiler symlink that escapes it.
    if toolchains.parent() != Some(rustup_home.as_path()) {
        return Err(ToolDiscoveryError::BootstrapRustcInvalid);
    }
    let toolchain_root = canonical_existing_directory(
        &toolchains.join(toolchain),
        ToolDiscoveryError::BootstrapRustcInvalid,
    )?;
    if toolchain_root.parent() != Some(toolchains.as_path()) {
        return Err(ToolDiscoveryError::BootstrapRustcInvalid);
    }
    let bin = canonical_existing_directory(
        &toolchain_root.join("bin"),
        ToolDiscoveryError::BootstrapRustcInvalid,
    )?;
    if bin.parent() != Some(toolchain_root.as_path()) {
        return Err(ToolDiscoveryError::BootstrapRustcInvalid);
    }
    let candidate = bin.join(ToolRole::Rustc.executable_name());
    let canonical =
        std::fs::canonicalize(candidate).map_err(|_| ToolDiscoveryError::BootstrapRustcInvalid)?;
    validate_unambiguous_absolute_path(&canonical)
        .map_err(|_| ToolDiscoveryError::BootstrapRustcInvalid)?;
    if !canonical.starts_with(&toolchain_root)
        || !std::fs::metadata(&canonical)
            .map_err(|_| ToolDiscoveryError::BootstrapRustcInvalid)?
            .is_file()
    {
        return Err(ToolDiscoveryError::BootstrapRustcInvalid);
    }
    Ok(ConcreteRustupRustc {
        executable: canonical,
        sysroot: toolchain_root,
    })
}

fn matching_managed_rustup_sysroot(
    rustup_home: Option<&Path>,
    canonical_rustc: &Path,
) -> Option<PathBuf> {
    let concrete = concrete_rustup_rustc(rustup_home?).ok()?;
    (concrete.executable == canonical_rustc).then_some(concrete.sysroot)
}

fn parse_default_rustup_toolchain(settings: &str) -> Option<&str> {
    let mut default = None;
    for line in settings.lines() {
        let assignment = line.split_once('#').map_or(line, |(value, _)| value).trim();
        if assignment.is_empty() {
            continue;
        }
        let Some((key, value)) = assignment.split_once('=') else {
            continue;
        };
        if key.trim() != "default_toolchain" {
            continue;
        }
        if default.is_some() {
            return None;
        }
        let value = value.trim();
        let value = value.strip_prefix('"')?.strip_suffix('"')?;
        if !is_safe_rustup_toolchain_name(value) {
            return None;
        }
        default = Some(value);
    }
    default
}

fn is_safe_rustup_toolchain_name(value: &str) -> bool {
    if value.is_empty()
        || value.len() > 128
        || value.ends_with('.')
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return false;
    }
    let mut components = Path::new(value).components();
    matches!(components.next(), Some(Component::Normal(name)) if name == OsStr::new(value))
        && components.next().is_none()
}

fn pin_sysroot_tool(
    sysroot_bin: &Path,
    role: ToolRole,
) -> Result<PinnedExecutable, ToolDiscoveryError> {
    let path = sysroot_bin.join(role.executable_name());
    PinnedExecutable::open(path).map_err(|_| role.invalid_error())
}

fn parse_host_path(value: &OsStr) -> Result<Vec<PathBuf>, ToolDiscoveryError> {
    if value.is_empty() {
        return Err(ToolDiscoveryError::HostPathInvalid);
    }

    let mut directories = Vec::new();
    for directory in std::env::split_paths(value) {
        validate_unambiguous_absolute_path(&directory)
            .map_err(|_| ToolDiscoveryError::HostPathInvalid)?;
        if !directories.contains(&directory) {
            directories.push(directory);
        }
    }
    if directories.is_empty() {
        return Err(ToolDiscoveryError::HostPathInvalid);
    }
    Ok(directories)
}

fn first_path_candidate(directories: &[PathBuf], executable_name: &OsStr) -> Option<PathBuf> {
    directories.iter().find_map(|directory| {
        let candidate = directory.join(executable_name);
        match std::fs::symlink_metadata(&candidate) {
            Ok(_) => Some(candidate),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(_) => Some(candidate),
        }
    })
}

fn validate_unambiguous_absolute_path(path: &Path) -> Result<(), ()> {
    if !path.is_absolute() || path.as_os_str().is_empty() {
        return Err(());
    }
    if path
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(());
    }
    Ok(())
}

fn canonical_existing_directory(
    path: &Path,
    error: ToolDiscoveryError,
) -> Result<PathBuf, ToolDiscoveryError> {
    validate_unambiguous_absolute_path(path).map_err(|_| error)?;
    let canonical = std::fs::canonicalize(path).map_err(|_| error)?;
    validate_unambiguous_absolute_path(&canonical).map_err(|_| error)?;
    canonical.is_dir().then_some(canonical).ok_or(error)
}

fn push_canonical_search_directory(directories: &mut Vec<PathBuf>, candidate: &Path) {
    let Ok(canonical) = std::fs::canonicalize(candidate) else {
        return;
    };
    if canonical.is_dir() && !directories.contains(&canonical) {
        directories.push(canonical);
    }
}

fn trusted_system_search_directories() -> Vec<PathBuf> {
    #[cfg(windows)]
    {
        let Some(system_root) = std::env::var_os("SYSTEMROOT")
            .or_else(|| std::env::var_os("WINDIR"))
            .map(PathBuf::from)
        else {
            return Vec::new();
        };
        vec![system_root.join("System32"), system_root]
    }
    #[cfg(unix)]
    {
        vec![PathBuf::from("/usr/bin"), PathBuf::from("/bin")]
    }
}

fn discover_cargo_home() -> Result<PathBuf, ToolDiscoveryError> {
    if let Some(configured) = std::env::var_os("CARGO_HOME") {
        return canonical_existing_directory(
            &PathBuf::from(configured),
            ToolDiscoveryError::CargoHomeInvalid,
        );
    }

    let home = platform_home_directory().ok_or(ToolDiscoveryError::CargoHomeInvalid)?;
    canonical_existing_directory(&home.join(".cargo"), ToolDiscoveryError::CargoHomeInvalid)
}

fn platform_home_directory() -> Option<PathBuf> {
    #[cfg(windows)]
    let value = std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME"));
    #[cfg(unix)]
    let value = std::env::var_os("HOME");
    value.map(PathBuf::from)
}

#[derive(Clone)]
struct BootstrapEnvironment {
    entries: Vec<(OsString, OsString)>,
}

impl BootstrapEnvironment {
    fn from_host(
        cargo_home: &Path,
        rustup_home: Option<&Path>,
    ) -> Result<Self, ToolDiscoveryError> {
        let temp = canonical_existing_directory(
            &std::env::temp_dir(),
            ToolDiscoveryError::BootstrapEnvironmentInvalid,
        )?;
        let mut entries = vec![
            (
                OsString::from("CARGO_HOME"),
                cargo_home.as_os_str().to_owned(),
            ),
            (OsString::from("CARGO_NET_OFFLINE"), OsString::from("true")),
            (OsString::from("RUST_BACKTRACE"), OsString::from("0")),
            (OsString::from("LC_ALL"), OsString::from("C")),
            (OsString::from("LANG"), OsString::from("C")),
        ];

        #[cfg(windows)]
        {
            let system_root = std::env::var_os("SYSTEMROOT")
                .or_else(|| std::env::var_os("WINDIR"))
                .map(PathBuf::from)
                .ok_or(ToolDiscoveryError::BootstrapEnvironmentInvalid)?;
            let system_root = canonical_existing_directory(
                &system_root,
                ToolDiscoveryError::BootstrapEnvironmentInvalid,
            )?;
            entries.extend([
                (
                    OsString::from("SYSTEMROOT"),
                    system_root.as_os_str().to_owned(),
                ),
                (OsString::from("WINDIR"), system_root.as_os_str().to_owned()),
                (OsString::from("TEMP"), temp.as_os_str().to_owned()),
                (OsString::from("TMP"), temp.as_os_str().to_owned()),
            ]);
        }
        #[cfg(unix)]
        entries.push((OsString::from("TMPDIR"), temp.as_os_str().to_owned()));

        if let Some(rustup_home) = rustup_home {
            entries.push((
                OsString::from("RUSTUP_HOME"),
                rustup_home.as_os_str().to_owned(),
            ));
        }
        Ok(Self { entries })
    }
}

fn discover_optional_rustup_home() -> Result<Option<PathBuf>, ToolDiscoveryError> {
    if let Some(configured) = std::env::var_os("RUSTUP_HOME") {
        return canonical_existing_directory(
            &PathBuf::from(configured),
            ToolDiscoveryError::BootstrapEnvironmentInvalid,
        )
        .map(Some);
    }
    let Some(home) = platform_home_directory() else {
        return Ok(None);
    };
    let candidate = home.join(".rustup");
    match std::fs::metadata(&candidate) {
        Ok(metadata) if metadata.is_dir() => canonical_existing_directory(
            &candidate,
            ToolDiscoveryError::BootstrapEnvironmentInvalid,
        )
        .map(Some),
        Ok(_) => Err(ToolDiscoveryError::BootstrapEnvironmentInvalid),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(ToolDiscoveryError::BootstrapEnvironmentInvalid),
    }
}

async fn query_sysroot(
    rustc: BootstrapExecutable,
    runtime_safe_cwd: Arc<ExecutionDirectory>,
    environment: BootstrapEnvironment,
) -> Result<Vec<u8>, ToolDiscoveryError> {
    let command = ValidatedCommand::rustc_sysroot(
        Arc::new(rustc.pinned),
        runtime_safe_cwd,
        ChildEnvironment::from_entries(environment.entries),
        BOOTSTRAP_TIMEOUT,
    )
    .map_err(|_| ToolDiscoveryError::BootstrapRustcInvalid)?;
    let limits = ProcessLimits::try_new(
        BOOTSTRAP_STREAM_LIMIT,
        BOOTSTRAP_STREAM_LIMIT,
        BOOTSTRAP_TIMEOUT,
        BOOTSTRAP_CLEANUP_TIMEOUT,
    )
    .map_err(|_| ToolDiscoveryError::BootstrapEnvironmentInvalid)?;
    let result = ProcessSupervisor::new(limits)
        .run(command, CancellationToken::new())
        .await
        .map_err(|error| {
            #[cfg(debug_assertions)]
            log_process_failure("bootstrap rustc", &error);
            map_bootstrap_process_error(error)
        })?;

    if result.timed_out {
        return Err(ToolDiscoveryError::BootstrapTimedOut);
    }
    if result.cancelled {
        return Err(ToolDiscoveryError::BootstrapWaitFailed);
    }
    if !result.stdout.complete || !result.stderr.complete {
        return Err(ToolDiscoveryError::BootstrapOutputFailed);
    }
    if result.stdout.truncated || result.stderr.truncated || result.truncated {
        return Err(ToolDiscoveryError::BootstrapOutputTooLarge);
    }
    if result.exit_code != Some(0) || result.signal.is_some() {
        return Err(ToolDiscoveryError::BootstrapRustcFailed);
    }

    let mut stdout = result.stdout.head;
    stdout.extend(result.stdout.tail);
    Ok(stdout)
}

async fn query_git_version(
    git: &BootstrapExecutable,
    runtime_safe_cwd: Arc<ExecutionDirectory>,
    environment: BootstrapEnvironment,
) -> Result<Vec<u8>, ToolDiscoveryError> {
    let command = ValidatedCommand::git_version(
        Arc::new(
            git.pinned
                .try_clone()
                .map_err(|_| ToolDiscoveryError::BootstrapGitInvalid)?,
        ),
        runtime_safe_cwd,
        ChildEnvironment::from_entries(environment.entries),
        BOOTSTRAP_TIMEOUT,
    )
    .map_err(|_| ToolDiscoveryError::BootstrapGitInvalid)?;
    let limits = ProcessLimits::try_new(
        BOOTSTRAP_STREAM_LIMIT,
        BOOTSTRAP_STREAM_LIMIT,
        BOOTSTRAP_TIMEOUT,
        BOOTSTRAP_CLEANUP_TIMEOUT,
    )
    .map_err(|_| ToolDiscoveryError::BootstrapEnvironmentInvalid)?;
    let result = ProcessSupervisor::new(limits)
        .run(command, CancellationToken::new())
        .await
        .map_err(|error| {
            #[cfg(debug_assertions)]
            log_process_failure("bootstrap Git", &error);
            map_git_process_error(error)
        })?;

    if result.timed_out {
        return Err(ToolDiscoveryError::BootstrapGitTimedOut);
    }
    if result.cancelled {
        return Err(ToolDiscoveryError::BootstrapGitWaitFailed);
    }
    if !result.stdout.complete || !result.stderr.complete {
        return Err(ToolDiscoveryError::BootstrapGitOutputFailed);
    }
    if result.stdout.truncated || result.stderr.truncated || result.truncated {
        return Err(ToolDiscoveryError::BootstrapGitOutputTooLarge);
    }
    if result.exit_code != Some(0) || result.signal.is_some() {
        return Err(ToolDiscoveryError::BootstrapGitFailed);
    }

    let mut stdout = result.stdout.head;
    stdout.extend(result.stdout.tail);
    Ok(stdout)
}

fn map_git_process_error(error: ProcessError) -> ToolDiscoveryError {
    match error {
        ProcessError::InvalidCommand
        | ProcessError::CommandPolicy(_)
        | ProcessError::TimeoutOutsideLimit => ToolDiscoveryError::BootstrapGitInvalid,
        ProcessError::SpawnFailed(_) | ProcessError::TreeSetupFailed(_) => {
            ToolDiscoveryError::BootstrapGitSpawnFailed
        }
        ProcessError::MissingOutputPipe | ProcessError::OutputDrainFailed(_) => {
            ToolDiscoveryError::BootstrapGitOutputFailed
        }
        ProcessError::WaitFailed(_)
        | ProcessError::TreeCleanupFailed(_)
        | ProcessError::CleanupTimedOut
        | ProcessError::WorkerFailed => ToolDiscoveryError::BootstrapGitWaitFailed,
    }
}

#[cfg(debug_assertions)]
fn log_process_failure(context: &str, error: &ProcessError) {
    let (variant, source) = match error {
        ProcessError::InvalidCommand => ("InvalidCommand", None),
        ProcessError::CommandPolicy(_) => ("CommandPolicy", None),
        ProcessError::TimeoutOutsideLimit => ("TimeoutOutsideLimit", None),
        ProcessError::SpawnFailed(source) => ("SpawnFailed", Some(source)),
        ProcessError::TreeSetupFailed(source) => ("TreeSetupFailed", Some(source)),
        ProcessError::MissingOutputPipe => ("MissingOutputPipe", None),
        ProcessError::WaitFailed(source) => ("WaitFailed", Some(source)),
        ProcessError::TreeCleanupFailed(source) => ("TreeCleanupFailed", Some(source)),
        ProcessError::CleanupTimedOut => ("CleanupTimedOut", None),
        ProcessError::OutputDrainFailed(source) => ("OutputDrainFailed", Some(source)),
        ProcessError::WorkerFailed => ("WorkerFailed", None),
    };
    if let Some(source) = source {
        eprintln!(
            "secret-safe {context} process failure: variant={variant} io_kind={:?} raw_os_error={:?}",
            source.kind(),
            source.raw_os_error()
        );
    } else {
        eprintln!("secret-safe {context} process failure: variant={variant}");
    }
}

fn require_supported_git_version(output: &[u8]) -> Result<(), ToolDiscoveryError> {
    if output.contains(&0) {
        return Err(ToolDiscoveryError::GitVersionMalformed);
    }
    let output =
        std::str::from_utf8(output).map_err(|_| ToolDiscoveryError::GitVersionMalformed)?;
    let line = output
        .strip_suffix("\r\n")
        .or_else(|| output.strip_suffix('\n'))
        .unwrap_or(output);
    if line.is_empty()
        || line.contains('\r')
        || line.contains('\n')
        || line.trim_matches(|character: char| character.is_ascii_whitespace()) != line
    {
        return Err(ToolDiscoveryError::GitVersionMalformed);
    }

    let version = line
        .strip_prefix("git version ")
        .ok_or(ToolDiscoveryError::GitVersionMalformed)?;
    let mut components = version.split('.');
    let major = components
        .next()
        .and_then(|value| value.parse::<u32>().ok())
        .ok_or(ToolDiscoveryError::GitVersionMalformed)?;
    let minor = components
        .next()
        .and_then(|value| value.parse::<u32>().ok())
        .ok_or(ToolDiscoveryError::GitVersionMalformed)?;
    if components.next().is_none() {
        return Err(ToolDiscoveryError::GitVersionMalformed);
    }

    if major > 2 || (major == 2 && minor >= 45) {
        Ok(())
    } else {
        Err(ToolDiscoveryError::GitVersionUnsupported)
    }
}

fn map_bootstrap_process_error(error: ProcessError) -> ToolDiscoveryError {
    match error {
        ProcessError::InvalidCommand
        | ProcessError::CommandPolicy(_)
        | ProcessError::TimeoutOutsideLimit => ToolDiscoveryError::BootstrapRustcInvalid,
        ProcessError::SpawnFailed(_) | ProcessError::TreeSetupFailed(_) => {
            ToolDiscoveryError::BootstrapSpawnFailed
        }
        ProcessError::MissingOutputPipe | ProcessError::OutputDrainFailed(_) => {
            ToolDiscoveryError::BootstrapOutputFailed
        }
        ProcessError::WaitFailed(_)
        | ProcessError::TreeCleanupFailed(_)
        | ProcessError::CleanupTimedOut
        | ProcessError::WorkerFailed => ToolDiscoveryError::BootstrapWaitFailed,
    }
}

fn parse_sysroot_line(output: &[u8]) -> Result<PathBuf, ToolDiscoveryError> {
    if output.contains(&0) {
        return Err(ToolDiscoveryError::SysrootMalformed);
    }
    let output = std::str::from_utf8(output).map_err(|_| ToolDiscoveryError::SysrootMalformed)?;
    let line = output
        .strip_suffix("\r\n")
        .or_else(|| output.strip_suffix('\n'))
        .unwrap_or(output);
    if line.is_empty()
        || line.contains('\r')
        || line.contains('\n')
        || line.trim_matches(|character: char| character.is_ascii_whitespace()) != line
    {
        return Err(ToolDiscoveryError::SysrootMalformed);
    }
    let path = PathBuf::from(line);
    validate_unambiguous_absolute_path(&path).map_err(|_| ToolDiscoveryError::SysrootMalformed)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_parser_preserves_absolute_candidate_order_and_deduplicates() {
        let first = absolute_test_path("tool-discovery-first");
        let second = absolute_test_path("tool-discovery-second");
        let encoded = std::env::join_paths([&first, &second, &first]).unwrap();

        let parsed = parse_host_path(&encoded).unwrap();

        assert_eq!(parsed, vec![first.clone(), second.clone()]);
        assert_eq!(
            parsed
                .iter()
                .map(|directory| directory.join(ToolRole::Git.executable_name()))
                .collect::<Vec<_>>(),
            vec![
                first.join(ToolRole::Git.executable_name()),
                second.join(ToolRole::Git.executable_name())
            ]
        );
    }

    #[test]
    fn path_parser_rejects_relative_and_empty_candidates() {
        let absolute = absolute_test_path("tool-discovery-absolute");
        let relative = PathBuf::from("relative-tool-directory");
        let with_relative = std::env::join_paths([&absolute, &relative]).unwrap();
        assert_eq!(
            parse_host_path(&with_relative),
            Err(ToolDiscoveryError::HostPathInvalid)
        );
        assert_eq!(
            parse_host_path(OsStr::new("")),
            Err(ToolDiscoveryError::HostPathInvalid)
        );
    }

    #[test]
    fn path_candidate_uses_the_first_existing_absolute_entry() {
        let temporary = tempfile::tempdir().unwrap();
        let first = temporary.path().join("first");
        let second = temporary.path().join("second");
        std::fs::create_dir(&first).unwrap();
        std::fs::create_dir(&second).unwrap();
        let executable_name = ToolRole::Git.executable_name();
        std::fs::write(first.join(executable_name), b"first").unwrap();
        std::fs::write(second.join(executable_name), b"second").unwrap();

        assert_eq!(
            first_path_candidate(&[first.clone(), second], executable_name),
            Some(first.join(executable_name))
        );
        assert_eq!(first_path_candidate(&[], executable_name), None);
    }

    #[test]
    fn sysroot_parser_accepts_one_absolute_line_with_platform_newline() {
        let sysroot = absolute_test_path("tool-discovery-sysroot");
        let output = format!("{}\r\n", sysroot.display());
        assert_eq!(parse_sysroot_line(output.as_bytes()).unwrap(), sysroot);
    }

    #[test]
    fn sysroot_parser_rejects_relative_extra_whitespace_multiline_and_binary() {
        let absolute = absolute_test_path("tool-discovery-sysroot");
        let multiline = format!("{}\nsecond-line\n", absolute.display());
        let padded = format!(" {}\n", absolute.display());

        for malformed in [
            b"relative\n".as_slice(),
            multiline.as_bytes(),
            padded.as_bytes(),
            b"\0invalid\n".as_slice(),
            b"\n".as_slice(),
        ] {
            assert_eq!(
                parse_sysroot_line(malformed),
                Err(ToolDiscoveryError::SysrootMalformed)
            );
        }
    }

    #[test]
    fn git_version_parser_requires_the_no_lazy_fetch_capability_floor() {
        for supported in [
            b"git version 2.45.0\n".as_slice(),
            b"git version 2.53.0.windows.1\r\n".as_slice(),
            b"git version 3.0.0\n".as_slice(),
        ] {
            assert_eq!(require_supported_git_version(supported), Ok(()));
        }

        for unsupported in [
            b"git version 2.44.9\n".as_slice(),
            b"git version 1.99.0\n".as_slice(),
        ] {
            assert_eq!(
                require_supported_git_version(unsupported),
                Err(ToolDiscoveryError::GitVersionUnsupported)
            );
        }
    }

    #[test]
    fn git_version_parser_rejects_ambiguous_or_malformed_output() {
        for malformed in [
            b"2.53.0\n".as_slice(),
            b"git version 2.53\n".as_slice(),
            b"git version two.53.0\n".as_slice(),
            b"git version 2.53.0\nsecond line\n".as_slice(),
            b" git version 2.53.0\n".as_slice(),
            b"git version 2.53.0\0\n".as_slice(),
        ] {
            assert_eq!(
                require_supported_git_version(malformed),
                Err(ToolDiscoveryError::GitVersionMalformed)
            );
        }
    }

    #[test]
    fn rustup_default_toolchain_parser_is_bounded_and_path_safe() {
        assert_eq!(
            parse_default_rustup_toolchain(
                "version = \"12\"\ndefault_toolchain = \"stable-x86_64-pc-windows-msvc\"\n"
            ),
            Some("stable-x86_64-pc-windows-msvc")
        );
        assert_eq!(
            parse_default_rustup_toolchain(
                "default_toolchain = \"1.97.0-x86_64-pc-windows-msvc\" # installed\n"
            ),
            Some("1.97.0-x86_64-pc-windows-msvc")
        );

        for malformed in [
            "default_toolchain = \".\"\n",
            "default_toolchain = \"..\"\n",
            "default_toolchain = \"stable.\"\n",
            "default_toolchain = \"../escape\"\n",
            "default_toolchain = \"nested/toolchain\"\n",
            "default_toolchain = \"\"\n",
            "default_toolchain = \"stable\"\ndefault_toolchain = \"nightly\"\n",
            "default_toolchain = 'stable'\n",
        ] {
            assert_eq!(parse_default_rustup_toolchain(malformed), None);
        }
    }

    #[test]
    fn rustup_proxy_detection_requires_the_rustc_role_and_exact_proxy_names() {
        #[cfg(windows)]
        let (rustc_name, rustup_name) = ("rustc.exe", "rustup.exe");
        #[cfg(not(windows))]
        let (rustc_name, rustup_name) = ("rustc", "rustup");
        let root = absolute_test_path("tool-discovery-rustup-proxy");
        let rustc = root.join(rustc_name);
        let rustup = root.join(rustup_name);

        assert!(is_rustup_rustc_proxy(
            ToolRole::BootstrapRustc,
            &rustc,
            &rustup
        ));
        assert!(!is_rustup_rustc_proxy(ToolRole::Git, &rustc, &rustup));
        assert!(!is_rustup_rustc_proxy(
            ToolRole::BootstrapRustc,
            &root.join("cargo"),
            &rustup
        ));
        assert!(!is_rustup_rustc_proxy(
            ToolRole::BootstrapRustc,
            &rustc,
            &root.join(rustc_name)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn rustup_proxy_detection_recognizes_hardlinked_unix_proxies() {
        let temporary = tempfile::tempdir().unwrap();
        let rustup = temporary.path().join("rustup");
        let rustc = temporary.path().join("rustc");
        std::fs::write(&rustup, b"rustup proxy image").unwrap();
        std::fs::hard_link(&rustup, &rustc).unwrap();

        let canonical_rustc = std::fs::canonicalize(&rustc).unwrap();
        assert!(has_file_name(&canonical_rustc, "rustc"));
        assert!(is_rustup_rustc_proxy(
            ToolRole::BootstrapRustc,
            &rustc,
            &canonical_rustc
        ));

        std::fs::remove_file(&rustc).unwrap();
        std::fs::write(&rustc, b"independent compiler image").unwrap();
        assert!(!is_rustup_rustc_proxy(
            ToolRole::BootstrapRustc,
            &rustc,
            &std::fs::canonicalize(&rustc).unwrap()
        ));
    }

    #[test]
    fn concrete_rustup_rustc_path_uses_only_the_configured_installed_layout() {
        let temporary = tempfile::tempdir().unwrap();
        let rustup_home = temporary.path();
        let toolchain = "1.97.0-test-host";
        let bin = rustup_home.join("toolchains").join(toolchain).join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        let rustc = bin.join(ToolRole::Rustc.executable_name());
        std::fs::write(&rustc, b"test compiler").unwrap();
        let concrete = concrete_rustup_rustc_path(rustup_home, toolchain).unwrap();
        assert_eq!(concrete.executable, std::fs::canonicalize(rustc).unwrap());
        assert_eq!(
            concrete.sysroot,
            std::fs::canonicalize(rustup_home.join("toolchains").join(toolchain)).unwrap()
        );
        assert_eq!(
            concrete_rustup_rustc_path(rustup_home, ".."),
            Err(ToolDiscoveryError::BootstrapRustcInvalid)
        );
    }

    #[cfg(unix)]
    #[test]
    fn concrete_rustup_rustc_rejects_a_toolchain_symlink_escape() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().unwrap();
        let rustup_home = temporary.path().join("rustup");
        let toolchains = rustup_home.join("toolchains");
        let outside = temporary.path().join("outside");
        std::fs::create_dir_all(outside.join("bin")).unwrap();
        std::fs::write(
            outside.join("bin").join(ToolRole::Rustc.executable_name()),
            b"outside compiler",
        )
        .unwrap();
        std::fs::create_dir_all(&toolchains).unwrap();
        symlink(&outside, toolchains.join("escaped")).unwrap();

        assert_eq!(
            concrete_rustup_rustc_path(&rustup_home, "escaped"),
            Err(ToolDiscoveryError::BootstrapRustcInvalid)
        );

        let linked_compiler = toolchains.join("linked-compiler");
        std::fs::create_dir_all(linked_compiler.join("bin")).unwrap();
        symlink(
            outside.join("bin").join(ToolRole::Rustc.executable_name()),
            linked_compiler
                .join("bin")
                .join(ToolRole::Rustc.executable_name()),
        )
        .unwrap();
        assert_eq!(
            concrete_rustup_rustc_path(&rustup_home, "linked-compiler"),
            Err(ToolDiscoveryError::BootstrapRustcInvalid)
        );
    }

    #[cfg(unix)]
    #[test]
    fn concrete_rustup_rustc_reads_the_bounded_default_setting() {
        use std::os::unix::fs::PermissionsExt as _;

        let temporary = tempfile::tempdir().unwrap();
        let rustup_home = temporary.path().canonicalize().unwrap();
        let toolchain = "1.97.0-test-host";
        let bin = rustup_home.join("toolchains").join(toolchain).join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        let rustc = bin.join(ToolRole::Rustc.executable_name());
        std::fs::write(&rustc, b"test compiler").unwrap();
        std::fs::set_permissions(&rustc, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::write(
            rustup_home.join("settings.toml"),
            format!("version = \"12\"\ndefault_toolchain = \"{toolchain}\"\n"),
        )
        .unwrap();

        let concrete = concrete_rustup_rustc(&rustup_home).unwrap();
        assert_eq!(concrete.executable, std::fs::canonicalize(&rustc).unwrap());
        assert_eq!(
            concrete.sysroot,
            std::fs::canonicalize(rustup_home.join("toolchains").join(toolchain)).unwrap()
        );
        assert_eq!(
            matching_managed_rustup_sysroot(Some(&rustup_home), &concrete.executable),
            Some(concrete.sysroot.clone())
        );
        assert_eq!(
            matching_managed_rustup_sysroot(
                Some(&rustup_home),
                &temporary.path().join("other-rustc")
            ),
            None
        );
        let resolved = resolve_bootstrap_executable(
            ToolRole::BootstrapRustc,
            Some(&rustc),
            &[],
            Some(&rustup_home),
        )
        .unwrap();
        assert_eq!(resolved.known_sysroot, Some(concrete.sysroot));
    }

    #[cfg(windows)]
    #[test]
    fn git_for_windows_cmd_launcher_maps_only_to_fixed_installation_candidates() {
        let launcher = PathBuf::from(r"C:\Program Files\Git\cmd\git.exe");
        let candidates = windows_git_for_windows_layout_candidates(&launcher).unwrap();
        let preferred = if cfg!(target_pointer_width = "64") {
            "mingw64"
        } else {
            "mingw32"
        };
        let fallback = if cfg!(target_pointer_width = "64") {
            "mingw32"
        } else {
            "mingw64"
        };
        assert_eq!(
            candidates,
            [
                PathBuf::from(r"C:\Program Files\Git")
                    .join(preferred)
                    .join(r"bin\git.exe"),
                PathBuf::from(r"C:\Program Files\Git")
                    .join(fallback)
                    .join(r"bin\git.exe"),
            ]
        );
        assert!(
            windows_git_for_windows_layout_candidates(Path::new(
                r"C:\Program Files\Git\mingw64\bin\git.exe"
            ))
            .is_none()
        );
        assert!(
            windows_git_for_windows_layout_candidates(Path::new(r"C:\other\cmd\tool.exe"))
                .is_none()
        );
    }

    #[test]
    fn supervisor_errors_are_collapsed_to_secret_safe_discovery_categories() {
        assert_eq!(
            map_bootstrap_process_error(ProcessError::SpawnFailed(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "sensitive executable path",
            ))),
            ToolDiscoveryError::BootstrapSpawnFailed
        );
        assert_eq!(
            map_bootstrap_process_error(ProcessError::OutputDrainFailed(io::Error::other(
                "sensitive pipe detail",
            ))),
            ToolDiscoveryError::BootstrapOutputFailed
        );
        assert_eq!(
            map_bootstrap_process_error(ProcessError::CleanupTimedOut),
            ToolDiscoveryError::BootstrapWaitFailed
        );
    }

    fn absolute_test_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(name)
    }
}
