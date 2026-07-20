use std::collections::{BTreeMap, BTreeSet};
#[cfg(all(windows, target_env = "msvc"))]
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[cfg(all(windows, target_env = "msvc"))]
use find_msvc_tools::{Env, EnvGetter, find_tool_with_env};

use crate::command_policy::{
    ExecutionDirectory, PinnedExecutable, ValidatedCommand, child_visible_path,
    is_safe_cargo_selector,
};
use crate::native_fs::{open_child_directory, open_child_file};
#[cfg(all(windows, target_env = "msvc"))]
use crate::process_supervisor::WindowsMsvcEnvironment;
use crate::process_supervisor::{
    ChildEnvironment, CommandResult, PlatformEnvironment, ProcessError, ProcessLimits,
    ProcessSupervisor, RustToolchainEnvironment,
};
use crate::root_capability::{ensure_plain_directory, ensure_plain_file};
use crate::tool_discovery::ToolchainPaths;
use crate::{CommandPolicyError, RelativePath};

/// Bounded Cargo metadata projected into the only package and integration-test
/// selectors that typed Cargo commands may accept.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CargoCatalog {
    packages: Vec<CargoPackage>,
}

impl CargoCatalog {
    pub fn packages(&self) -> &[CargoPackage] {
        &self.packages
    }

    /// Deterministic, bounded model context containing selector names only.
    /// Absolute paths and the raw Cargo metadata document are never retained.
    pub fn repository_context(&self) -> String {
        let mut context = String::from("Cargo workspace selectors:\n");
        for package in &self.packages {
            context.push_str("package=");
            context.push_str(&package.name);
            context.push_str("; integration_tests=");
            if package.integration_tests.is_empty() {
                context.push('-');
            } else {
                context.push_str(&package.integration_tests.join(","));
            }
            context.push('\n');
        }
        context
    }

    fn package(&self, name: &str) -> Option<&CargoPackage> {
        self.packages.iter().find(|package| package.name == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CargoPackage {
    name: String,
    integration_tests: Vec<String>,
}

impl CargoPackage {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn integration_tests(&self) -> &[String] {
        &self.integration_tests
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CargoRunStatus {
    Cancelled,
    TimedOut,
    Passed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CargoRunResult {
    pub status: CargoRunStatus,
    pub command: CommandResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CargoToolLimits {
    metadata_timeout: Duration,
    max_packages: usize,
    max_targets: usize,
    max_name_bytes: usize,
}

impl CargoToolLimits {
    pub fn try_new(
        metadata_timeout: Duration,
        max_packages: usize,
        max_targets: usize,
        max_name_bytes: usize,
    ) -> Result<Self, CargoToolError> {
        if metadata_timeout.is_zero()
            || max_packages == 0
            || max_targets == 0
            || max_name_bytes == 0
        {
            return Err(CargoToolError::InvalidLimits);
        }
        Ok(Self {
            metadata_timeout,
            max_packages,
            max_targets,
            max_name_bytes,
        })
    }
}

#[derive(Debug)]
pub struct CargoTools {
    supervisor: ProcessSupervisor,
    cargo: Arc<PinnedExecutable>,
    rustc: Arc<PinnedExecutable>,
    rustdoc: Arc<PinnedExecutable>,
    git: Arc<PinnedExecutable>,
    #[cfg(all(windows, target_env = "msvc"))]
    windows_linker: Arc<PinnedExecutable>,
    execution_directory: Arc<ExecutionDirectory>,
    target_directory: Arc<ExecutionDirectory>,
    environment: ChildEnvironment,
    redaction_paths: Vec<PathBuf>,
    limits: CargoToolLimits,
}

#[derive(Debug, Clone, Copy)]
enum MetadataAccess {
    Normal,
    ReadOnly,
}

impl CargoTools {
    /// Binds Cargo to a retained worktree and a pre-created target directory.
    /// Callers cannot supply a program, argv, manifest path, target triple, or
    /// Cargo configuration argument after this trusted composition step.
    pub fn from_trusted_capabilities(
        toolchain: &ToolchainPaths,
        execution_directory: Arc<ExecutionDirectory>,
        target_directory: Arc<ExecutionDirectory>,
        temporary_directory: impl AsRef<std::path::Path>,
        process_limits: ProcessLimits,
        limits: CargoToolLimits,
    ) -> Result<Self, CargoToolError> {
        execution_directory
            .revalidate()
            .map_err(CargoToolError::CommandPolicy)?;
        target_directory
            .revalidate()
            .map_err(CargoToolError::CommandPolicy)?;
        validate_bound_directory_path(
            &execution_directory,
            execution_directory.path(),
            target_directory.path(),
        )?;

        let platform = platform_environment(temporary_directory.as_ref())?;
        let rustc = toolchain.rustc();
        let rustdoc = toolchain.rustdoc();
        #[cfg(all(windows, target_env = "msvc"))]
        let windows_msvc = discover_windows_msvc()?;
        let search_directories = toolchain.search_directories().to_vec();
        #[cfg(all(windows, target_env = "msvc"))]
        let search_directories = {
            let mut combined = windows_msvc.search_directories.clone();
            combined.extend(search_directories);
            combined
        };
        let toolchain_environment = RustToolchainEnvironment::try_new(
            search_directories,
            toolchain.cargo_home().to_owned(),
            None,
            rustc.path().to_owned(),
            rustdoc.path().to_owned(),
        )
        .map_err(|_| CargoToolError::InvalidEnvironment)?;
        #[cfg(all(windows, target_env = "msvc"))]
        let toolchain_environment = toolchain_environment.with_windows_msvc(
            WindowsMsvcEnvironment::try_new(
                windows_msvc.linker.path().to_owned(),
                windows_msvc.library_directories.clone(),
                windows_msvc.include_directories.clone(),
            )
            .map_err(|_| CargoToolError::InvalidEnvironment)?,
        );
        #[cfg(all(windows, target_env = "msvc"))]
        let redaction_paths = windows_msvc.redaction_paths();
        #[cfg(not(all(windows, target_env = "msvc")))]
        let redaction_paths = Vec::new();
        let mut environment =
            ChildEnvironment::for_rust_toolchain(&platform, &toolchain_environment);
        environment.set_cargo_target_directory(&child_visible_path(target_directory.path()));

        Ok(Self {
            supervisor: ProcessSupervisor::new(process_limits),
            cargo: toolchain.cargo(),
            rustc,
            rustdoc,
            git: toolchain.git(),
            #[cfg(all(windows, target_env = "msvc"))]
            windows_linker: windows_msvc.linker,
            execution_directory,
            target_directory,
            environment,
            redaction_paths,
            limits,
        })
    }

    /// Refreshes the selector catalog with a fixed, offline Cargo metadata
    /// invocation. A truncated or otherwise incomplete stdout is never parsed.
    pub async fn catalog(
        &self,
        cancellation: CancellationToken,
    ) -> Result<CargoCatalog, CargoToolError> {
        self.catalog_with_timeout(
            cancellation,
            self.limits.metadata_timeout,
            MetadataAccess::Normal,
        )
        .await
    }

    /// Reads selector metadata without permitting Cargo to update workspace
    /// state. Recovery and observation paths use this variant so classifying
    /// an artifact cannot create or repair its lockfile.
    pub(crate) async fn catalog_read_only(
        &self,
        cancellation: CancellationToken,
    ) -> Result<CargoCatalog, CargoToolError> {
        self.catalog_with_timeout(
            cancellation,
            self.limits.metadata_timeout,
            MetadataAccess::ReadOnly,
        )
        .await
    }

    async fn catalog_with_timeout(
        &self,
        cancellation: CancellationToken,
        timeout: Duration,
        access: MetadataAccess,
    ) -> Result<CargoCatalog, CargoToolError> {
        let command = match access {
            MetadataAccess::Normal => ValidatedCommand::cargo_metadata(
                self.cargo.clone(),
                self.execution_directory.clone(),
                self.environment.clone(),
                timeout,
            ),
            MetadataAccess::ReadOnly => ValidatedCommand::cargo_metadata_read_only(
                self.cargo.clone(),
                self.execution_directory.clone(),
                self.environment.clone(),
                timeout,
            ),
        }
        .and_then(|command| command.with_dependent_executables(self.dependent_executables()))
        .map_err(CargoToolError::CommandPolicy)?;
        let result = self
            .supervisor
            .run(command, cancellation)
            .await
            .map_err(CargoToolError::Process)?;
        parse_metadata_result(
            result,
            &self.execution_directory,
            Some(&self.target_directory),
            self.limits,
        )
    }

    pub async fn check(
        &self,
        package: Option<&str>,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<CargoRunResult, CargoToolError> {
        if cancellation.is_cancelled() {
            return Err(CargoToolError::Cancelled);
        }
        if timeout.is_zero() {
            return Err(CargoToolError::CommandPolicy(
                CommandPolicyError::InvalidTimeout,
            ));
        }
        let started = Instant::now();
        let catalog = self
            .catalog_with_timeout(
                cancellation.clone(),
                self.limits.metadata_timeout.min(timeout),
                MetadataAccess::Normal,
            )
            .await?;
        if let Some(package) = package {
            validate_package(&catalog, package)?;
        }
        let remaining = remaining_timeout(started, timeout)?;
        let command = ValidatedCommand::cargo_check(
            self.cargo.clone(),
            self.execution_directory.clone(),
            self.environment.clone(),
            package,
            remaining,
        )
        .and_then(|command| command.with_dependent_executables(self.dependent_executables()))
        .map_err(CargoToolError::CommandPolicy)?;
        self.run(command, cancellation).await
    }

    pub async fn test(
        &self,
        package: Option<&str>,
        test: Option<&str>,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<CargoRunResult, CargoToolError> {
        if cancellation.is_cancelled() {
            return Err(CargoToolError::Cancelled);
        }
        if timeout.is_zero() {
            return Err(CargoToolError::CommandPolicy(
                CommandPolicyError::InvalidTimeout,
            ));
        }
        let package = package.ok_or(CargoToolError::PackageRequired)?;
        let started = Instant::now();
        let catalog = self
            .catalog_with_timeout(
                cancellation.clone(),
                self.limits.metadata_timeout.min(timeout),
                MetadataAccess::Normal,
            )
            .await?;
        validate_test_selection(&catalog, package, test)?;
        let remaining = remaining_timeout(started, timeout)?;
        let command = ValidatedCommand::cargo_test(
            self.cargo.clone(),
            self.execution_directory.clone(),
            self.environment.clone(),
            package,
            test,
            remaining,
        )
        .and_then(|command| command.with_dependent_executables(self.dependent_executables()))
        .map_err(CargoToolError::CommandPolicy)?;
        self.run(command, cancellation).await
    }

    async fn run(
        &self,
        command: ValidatedCommand,
        cancellation: CancellationToken,
    ) -> Result<CargoRunResult, CargoToolError> {
        let command = self
            .supervisor
            .run(command, cancellation)
            .await
            .map_err(CargoToolError::Process)?;
        cargo_run_result(command)
    }

    fn dependent_executables(&self) -> Vec<Arc<PinnedExecutable>> {
        let mut executables = Vec::with_capacity(4);
        executables.extend([self.rustc.clone(), self.rustdoc.clone(), self.git.clone()]);
        #[cfg(all(windows, target_env = "msvc"))]
        executables.push(self.windows_linker.clone());
        executables
    }

    pub(crate) fn redaction_paths(&self) -> &[PathBuf] {
        &self.redaction_paths
    }
}

fn remaining_timeout(started: Instant, total: Duration) -> Result<Duration, CargoToolError> {
    total
        .checked_sub(started.elapsed())
        .filter(|remaining| !remaining.is_zero())
        .ok_or(CargoToolError::TimedOut)
}

fn platform_environment(path: &std::path::Path) -> Result<PlatformEnvironment, CargoToolError> {
    #[cfg(windows)]
    let system_root = std::env::var_os("SYSTEMROOT")
        .or_else(|| std::env::var_os("WINDIR"))
        .map(PathBuf::from);
    #[cfg(unix)]
    let system_root = None;

    PlatformEnvironment::try_new(path.to_owned(), system_root)
        .map_err(|_| CargoToolError::InvalidEnvironment)
}

#[cfg(all(windows, target_env = "msvc"))]
struct NoHostMsvcEnvironment;

#[cfg(all(windows, target_env = "msvc"))]
impl EnvGetter for NoHostMsvcEnvironment {
    fn get_env(&self, _: &'static str) -> Option<Env> {
        None
    }
}

#[cfg(all(windows, target_env = "msvc"))]
struct DiscoveredWindowsMsvc {
    linker: Arc<PinnedExecutable>,
    search_directories: Vec<PathBuf>,
    library_directories: Vec<PathBuf>,
    include_directories: Vec<PathBuf>,
}

#[cfg(all(windows, target_env = "msvc"))]
impl DiscoveredWindowsMsvc {
    fn redaction_paths(&self) -> Vec<PathBuf> {
        let mut paths = vec![self.linker.path().to_owned()];
        for path in self
            .search_directories
            .iter()
            .chain(&self.library_directories)
            .chain(&self.include_directories)
        {
            if !paths.contains(path) {
                paths.push(path.clone());
            }
        }
        paths
    }
}

#[cfg(all(windows, target_env = "msvc"))]
fn discover_windows_msvc() -> Result<DiscoveredWindowsMsvc, CargoToolError> {
    // Deliberately deny find-msvc-tools access to the process environment. The
    // result therefore comes from Windows Setup Configuration and machine SDK
    // registry state, never caller-controlled PATH/LIB/INCLUDE overrides.
    let tool = find_tool_with_env(std::env::consts::ARCH, "link.exe", &NoHostMsvcEnvironment)
        .ok_or(CargoToolError::InvalidEnvironment)?;
    let environment = tool
        .env()
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect::<Vec<_>>();
    validate_windows_msvc(tool.path(), &environment)
}

#[cfg(all(windows, target_env = "msvc"))]
fn validate_windows_msvc(
    linker: &Path,
    environment: &[(OsString, OsString)],
) -> Result<DiscoveredWindowsMsvc, CargoToolError> {
    let mut variables = BTreeMap::new();
    for (key, value) in environment {
        if !matches!(key.to_str(), Some("PATH" | "LIB" | "INCLUDE"))
            || variables.insert(key.to_owned(), value.to_owned()).is_some()
        {
            return Err(CargoToolError::InvalidEnvironment);
        }
    }
    if variables.len() != 3 {
        return Err(CargoToolError::InvalidEnvironment);
    }

    let search_directories = canonical_msvc_path_list(
        variables
            .get(OsStr::new("PATH"))
            .ok_or(CargoToolError::InvalidEnvironment)?,
    )?;
    let library_directories = canonical_msvc_path_list(
        variables
            .get(OsStr::new("LIB"))
            .ok_or(CargoToolError::InvalidEnvironment)?,
    )?;
    let include_directories = canonical_msvc_path_list(
        variables
            .get(OsStr::new("INCLUDE"))
            .ok_or(CargoToolError::InvalidEnvironment)?,
    )?;

    if !linker.is_absolute()
        || linker
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(CargoToolError::InvalidEnvironment);
    }
    let linker = std::fs::canonicalize(linker).map_err(|_| CargoToolError::InvalidEnvironment)?;
    if !linker.is_file()
        || !linker
            .file_name()
            .and_then(OsStr::to_str)
            .is_some_and(|name| name.eq_ignore_ascii_case("link.exe"))
        || !linker.parent().is_some_and(|parent| {
            search_directories
                .iter()
                .any(|directory| directory == parent)
        })
    {
        return Err(CargoToolError::InvalidEnvironment);
    }
    let linker =
        Arc::new(PinnedExecutable::open(linker).map_err(|_| CargoToolError::InvalidEnvironment)?);

    Ok(DiscoveredWindowsMsvc {
        linker,
        search_directories,
        library_directories,
        include_directories,
    })
}

#[cfg(all(windows, target_env = "msvc"))]
fn canonical_msvc_path_list(value: &OsStr) -> Result<Vec<PathBuf>, CargoToolError> {
    let mut canonical_directories = Vec::new();
    for directory in std::env::split_paths(value) {
        // find-msvc-tools emits a trailing separator. Never preserve the
        // resulting empty component because Windows interprets it as the
        // child process's current directory.
        if directory.as_os_str().is_empty() {
            continue;
        }
        if !directory.is_absolute()
            || directory
                .components()
                .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            return Err(CargoToolError::InvalidEnvironment);
        }
        let canonical =
            std::fs::canonicalize(directory).map_err(|_| CargoToolError::InvalidEnvironment)?;
        if !canonical.is_dir() {
            return Err(CargoToolError::InvalidEnvironment);
        }
        if !canonical_directories.contains(&canonical) {
            canonical_directories.push(canonical);
        }
    }
    if canonical_directories.is_empty() {
        Err(CargoToolError::InvalidEnvironment)
    } else {
        Ok(canonical_directories)
    }
}

fn cargo_run_result(command: CommandResult) -> Result<CargoRunResult, CargoToolError> {
    // Cancellation and timeout are terminal supervisor outcomes, not
    // Cargo diagnostics. They therefore take precedence over any retained
    // stderr that happens to resemble an offline dependency error.
    if !command.cancelled
        && !command.timed_out
        && (command.exit_code != Some(0) || command.signal.is_some())
        && output_indicates_offline_dependency(&command)
    {
        return Err(CargoToolError::DependencyUnavailableOffline);
    }
    Ok(CargoRunResult {
        status: classify_run(&command),
        command,
    })
}

fn validate_package<'a>(
    catalog: &'a CargoCatalog,
    package: &str,
) -> Result<&'a CargoPackage, CargoToolError> {
    catalog
        .package(package)
        .ok_or(CargoToolError::UnknownPackage)
}

fn validate_test_selection(
    catalog: &CargoCatalog,
    package: &str,
    test: Option<&str>,
) -> Result<(), CargoToolError> {
    let package = validate_package(catalog, package)?;
    if let Some(test) = test
        && !package
            .integration_tests
            .iter()
            .any(|candidate| candidate == test)
    {
        return Err(CargoToolError::UnknownIntegrationTest);
    }
    Ok(())
}

fn classify_run(result: &CommandResult) -> CargoRunStatus {
    if result.cancelled {
        CargoRunStatus::Cancelled
    } else if result.timed_out {
        CargoRunStatus::TimedOut
    } else if result.exit_code == Some(0) && result.signal.is_none() {
        CargoRunStatus::Passed
    } else {
        CargoRunStatus::Failed
    }
}

fn parse_metadata_result(
    result: CommandResult,
    bound_directory: &ExecutionDirectory,
    expected_target_directory: Option<&ExecutionDirectory>,
    limits: CargoToolLimits,
) -> Result<CargoCatalog, CargoToolError> {
    if result.cancelled {
        return Err(CargoToolError::Cancelled);
    }
    if result.timed_out {
        return Err(CargoToolError::TimedOut);
    }
    if result.exit_code != Some(0) || result.signal.is_some() {
        return if output_indicates_offline_dependency(&result) {
            Err(CargoToolError::DependencyUnavailableOffline)
        } else {
            Err(CargoToolError::MetadataCommandFailed)
        };
    }
    let stdout = complete_output(&result)?;
    let metadata: MetadataDocument =
        serde_json::from_slice(&stdout).map_err(|_| CargoToolError::MetadataInvalid)?;
    if let Some(expected_target_directory) = expected_target_directory
        && !expected_target_directory
            .matches_path(&metadata.target_directory)
            .map_err(|_| CargoToolError::MetadataPathOutsideWorkspace)?
    {
        return Err(CargoToolError::MetadataPathOutsideWorkspace);
    }
    catalog_from_metadata(metadata, bound_directory, limits)
}

fn complete_output(result: &CommandResult) -> Result<Vec<u8>, CargoToolError> {
    let stdout = &result.stdout;
    let retained = stdout.head.len().saturating_add(stdout.tail.len());
    if !stdout.complete
        || stdout.truncated
        || stdout.omitted_observed_bytes != 0
        || stdout.observed_bytes != retained as u64
    {
        return Err(CargoToolError::MetadataOutputIncomplete);
    }
    let mut complete = Vec::with_capacity(retained);
    complete.extend_from_slice(&stdout.head);
    complete.extend_from_slice(&stdout.tail);
    Ok(complete)
}

fn output_indicates_offline_dependency(result: &CommandResult) -> bool {
    let mut output = Vec::new();
    for stream in [&result.stdout, &result.stderr] {
        output.extend_from_slice(&stream.head);
        output.extend_from_slice(&stream.tail);
    }
    let output = String::from_utf8_lossy(&output).to_ascii_lowercase();
    let offline = output.contains("offline mode")
        || output.contains("--offline was specified")
        || output.contains("using offline mode");
    let unavailable_dependency = output.contains("no matching package named")
        || output.contains("failed to download")
        || output.contains("failed to get")
        || output.contains("attempting to make an http request")
        || output.contains("could not find package")
        || output.contains("failed to select a version");
    offline && unavailable_dependency
}

fn catalog_from_metadata(
    metadata: MetadataDocument,
    bound_directory: &ExecutionDirectory,
    limits: CargoToolLimits,
) -> Result<CargoCatalog, CargoToolError> {
    if metadata.version != 1 {
        return Err(CargoToolError::MetadataInvalid);
    }
    if !bound_directory
        .matches_path(&metadata.workspace_root)
        .map_err(|_| CargoToolError::WorkspaceRootMismatch)?
    {
        return Err(CargoToolError::WorkspaceRootMismatch);
    }
    validate_bound_directory_path(
        bound_directory,
        &metadata.workspace_root,
        &metadata.target_directory,
    )?;
    if metadata.workspace_members.is_empty() {
        return Err(CargoToolError::MetadataInvalid);
    }
    if metadata.workspace_members.len() > limits.max_packages {
        return Err(CargoToolError::CatalogTooLarge);
    }

    let workspace_member_count = metadata.workspace_members.len();
    let member_ids = metadata
        .workspace_members
        .into_iter()
        .collect::<BTreeSet<_>>();
    if member_ids.len() != workspace_member_count {
        return Err(CargoToolError::MetadataInvalid);
    }
    if member_ids.len() > limits.max_packages {
        return Err(CargoToolError::CatalogTooLarge);
    }

    let mut members = BTreeMap::new();
    for package in metadata.packages {
        if member_ids.contains(&package.id) && members.insert(package.id.clone(), package).is_some()
        {
            return Err(CargoToolError::MetadataInvalid);
        }
    }
    if members.len() != member_ids.len() {
        return Err(CargoToolError::MetadataInvalid);
    }

    let mut package_names = BTreeSet::new();
    let mut total_targets = 0usize;
    let mut packages = Vec::with_capacity(members.len());
    for member_id in member_ids {
        let package = members
            .remove(&member_id)
            .ok_or(CargoToolError::MetadataInvalid)?;
        validate_catalog_name(&package.name, limits.max_name_bytes)?;
        if !package_names.insert(package.name.clone()) {
            return Err(CargoToolError::MetadataInvalid);
        }
        if package.source.is_some() {
            return Err(CargoToolError::MetadataPathOutsideWorkspace);
        }
        validate_bound_file_path(
            bound_directory,
            &metadata.workspace_root,
            &package.manifest_path,
        )?;
        total_targets = total_targets
            .checked_add(package.targets.len())
            .ok_or(CargoToolError::CatalogTooLarge)?;
        if total_targets > limits.max_targets {
            return Err(CargoToolError::CatalogTooLarge);
        }

        let mut integration_tests = BTreeSet::new();
        for target in package.targets {
            validate_catalog_name(&target.name, limits.max_name_bytes)?;
            validate_bound_file_path(bound_directory, &metadata.workspace_root, &target.src_path)?;
            if target.kind.iter().any(|kind| kind == "test")
                && !integration_tests.insert(target.name)
            {
                return Err(CargoToolError::MetadataInvalid);
            }
        }
        packages.push(CargoPackage {
            name: package.name,
            integration_tests: integration_tests.into_iter().collect(),
        });
    }
    packages.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(CargoCatalog { packages })
}

/// Converts an absolute Cargo metadata path to a validated relative selector.
///
/// `strip_prefix` is deliberately only a parser: the returned selector is
/// subsequently resolved from the already-open execution-directory handle.
/// That handle-relative, no-follow traversal is the authority check.
fn metadata_relative_path(
    workspace_root: &Path,
    absolute_path: &Path,
) -> Result<RelativePath, CargoToolError> {
    if !workspace_root.is_absolute() || !absolute_path.is_absolute() {
        return Err(CargoToolError::MetadataPathOutsideWorkspace);
    }
    let relative = absolute_path
        .strip_prefix(workspace_root)
        .map_err(|_| CargoToolError::MetadataPathOutsideWorkspace)?;
    let mut slash_path = String::new();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            return Err(CargoToolError::MetadataPathOutsideWorkspace);
        };
        let component = component
            .to_str()
            .ok_or(CargoToolError::MetadataPathOutsideWorkspace)?;
        if !slash_path.is_empty() {
            slash_path.push('/');
        }
        slash_path.push_str(component);
    }
    RelativePath::parse(slash_path).map_err(|_| CargoToolError::MetadataPathOutsideWorkspace)
}

fn validate_bound_file_path(
    bound_directory: &ExecutionDirectory,
    workspace_root: &Path,
    absolute_path: &Path,
) -> Result<(), CargoToolError> {
    let relative = metadata_relative_path(workspace_root, absolute_path)?;
    let (parent, name) = open_bound_parent(bound_directory, &relative)?;
    let file = open_child_file(&parent, &name)
        .and_then(|file| {
            ensure_plain_file(&file)?;
            Ok(file)
        })
        .map_err(|_| CargoToolError::MetadataPathOutsideWorkspace)?;
    drop(file);
    Ok(())
}

fn validate_bound_directory_path(
    bound_directory: &ExecutionDirectory,
    workspace_root: &Path,
    absolute_path: &Path,
) -> Result<(), CargoToolError> {
    let relative = metadata_relative_path(workspace_root, absolute_path)?;
    let mut directory = bound_directory
        .cloned_directory()
        .map_err(|_| CargoToolError::MetadataPathOutsideWorkspace)?;
    ensure_plain_directory(&directory).map_err(|_| CargoToolError::MetadataPathOutsideWorkspace)?;
    for component in relative.components() {
        directory = open_child_directory(&directory, component.as_ref())
            .and_then(|directory| {
                ensure_plain_directory(&directory)?;
                Ok(directory)
            })
            .map_err(|_| CargoToolError::MetadataPathOutsideWorkspace)?;
    }
    Ok(())
}

fn open_bound_parent(
    bound_directory: &ExecutionDirectory,
    relative: &RelativePath,
) -> Result<(File, std::ffi::OsString), CargoToolError> {
    let mut components = relative.components().peekable();
    if components.peek().is_none() {
        return Err(CargoToolError::MetadataPathOutsideWorkspace);
    }
    let mut parent = bound_directory
        .cloned_directory()
        .map_err(|_| CargoToolError::MetadataPathOutsideWorkspace)?;
    ensure_plain_directory(&parent).map_err(|_| CargoToolError::MetadataPathOutsideWorkspace)?;
    while let Some(component) = components.next() {
        if components.peek().is_some() {
            parent = open_child_directory(&parent, component.as_ref())
                .and_then(|directory| {
                    ensure_plain_directory(&directory)?;
                    Ok(directory)
                })
                .map_err(|_| CargoToolError::MetadataPathOutsideWorkspace)?;
        } else {
            return Ok((parent, component.into()));
        }
    }
    unreachable!("the empty relative path returned before traversal")
}

fn validate_catalog_name(name: &str, max_name_bytes: usize) -> Result<(), CargoToolError> {
    if name.is_empty() || name.len() > max_name_bytes || !is_safe_cargo_selector(name) {
        return Err(CargoToolError::MetadataInvalid);
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct MetadataDocument {
    version: u32,
    packages: Vec<MetadataPackage>,
    workspace_members: Vec<String>,
    workspace_root: PathBuf,
    target_directory: PathBuf,
}

#[derive(Debug, Deserialize)]
struct MetadataPackage {
    id: String,
    name: String,
    manifest_path: PathBuf,
    source: Option<String>,
    targets: Vec<MetadataTarget>,
}

#[derive(Debug, Deserialize)]
struct MetadataTarget {
    name: String,
    kind: Vec<String>,
    src_path: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum CargoToolError {
    #[error("Cargo tool limits must all be non-zero")]
    InvalidLimits,
    #[error("the Cargo child-process environment is invalid")]
    InvalidEnvironment,
    #[error("the Cargo command was rejected by typed command policy")]
    CommandPolicy(#[source] CommandPolicyError),
    #[error("the selected package is not in the trusted Cargo catalog")]
    UnknownPackage,
    #[error("Cargo test requires an exact package selector")]
    PackageRequired,
    #[error("the selected integration test is not in the trusted Cargo catalog")]
    UnknownIntegrationTest,
    #[error("Cargo metadata was cancelled")]
    Cancelled,
    #[error("Cargo metadata timed out")]
    TimedOut,
    #[error("Cargo metadata exited unsuccessfully")]
    MetadataCommandFailed,
    #[error("Cargo metadata requires a dependency that is unavailable offline")]
    DependencyUnavailableOffline,
    #[error("Cargo metadata output was incomplete or exceeded its output limit")]
    MetadataOutputIncomplete,
    #[error("Cargo metadata output was not valid supported JSON")]
    MetadataInvalid,
    #[error("Cargo metadata did not describe the bound execution directory")]
    WorkspaceRootMismatch,
    #[error("Cargo metadata contains a workspace path that is not a bound plain worktree entry")]
    MetadataPathOutsideWorkspace,
    #[error("Cargo metadata exceeded its configured catalog limits")]
    CatalogTooLarge,
    #[error("the supervised Cargo process failed")]
    Process(#[source] ProcessError),
}

impl CargoToolError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidLimits
            | Self::InvalidEnvironment
            | Self::CommandPolicy(_)
            | Self::UnknownPackage
            | Self::PackageRequired
            | Self::UnknownIntegrationTest => "COMMAND_NOT_ALLOWED",
            Self::Cancelled => "COMMAND_CANCELLED",
            Self::TimedOut => "COMMAND_TIMED_OUT",
            Self::DependencyUnavailableOffline => "CARGO_DEPENDENCY_UNAVAILABLE_OFFLINE",
            Self::MetadataCommandFailed
            | Self::MetadataOutputIncomplete
            | Self::MetadataInvalid
            | Self::WorkspaceRootMismatch
            | Self::MetadataPathOutsideWorkspace
            | Self::CatalogTooLarge => "CARGO_METADATA_FAILED",
            Self::Process(error) => error.code(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    use crate::CapturedStream;

    fn tempdir() -> tempfile::TempDir {
        let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/cargo-tools-tests");
        std::fs::create_dir_all(&base).unwrap();
        let base = std::fs::canonicalize(base).unwrap();
        tempfile::Builder::new()
            .prefix("workspace-")
            .tempdir_in(base)
            .unwrap()
    }

    #[cfg(all(windows, target_env = "msvc"))]
    fn msvc_fixture() -> (tempfile::TempDir, PathBuf, Vec<(OsString, OsString)>) {
        let root = tempdir();
        let binary_directory = root.path().join("bin");
        let library_directory = root.path().join("lib");
        let include_directory = root.path().join("include");
        for directory in [&binary_directory, &library_directory, &include_directory] {
            std::fs::create_dir(directory).unwrap();
        }
        let linker = binary_directory.join("link.exe");
        std::fs::copy(std::env::current_exe().unwrap(), &linker).unwrap();
        let mut search_path = std::env::join_paths([&binary_directory, &binary_directory]).unwrap();
        search_path.push(";");
        let environment = vec![
            (OsString::from("PATH"), search_path),
            (
                OsString::from("LIB"),
                std::env::join_paths([&library_directory]).unwrap(),
            ),
            (
                OsString::from("INCLUDE"),
                std::env::join_paths([&include_directory]).unwrap(),
            ),
        ];
        (root, linker, environment)
    }

    fn execution_directory(path: &Path) -> ExecutionDirectory {
        ExecutionDirectory::open(path).unwrap()
    }

    fn limits() -> CargoToolLimits {
        CargoToolLimits::try_new(Duration::from_secs(5), 4, 8, 64).unwrap()
    }

    fn metadata(root: &Path) -> MetadataDocument {
        for directory in [
            root.join("alpha/src"),
            root.join("alpha/tests"),
            root.join("beta/src"),
            root.join("beta/tests"),
            root.join("target"),
        ] {
            std::fs::create_dir_all(directory).unwrap();
        }
        for file in [
            root.join("alpha/Cargo.toml"),
            root.join("alpha/src/lib.rs"),
            root.join("alpha/tests/integration_a.rs"),
            root.join("alpha/tests/integration_b.rs"),
            root.join("beta/Cargo.toml"),
            root.join("beta/src/lib.rs"),
            root.join("beta/tests/zeta_test.rs"),
        ] {
            std::fs::write(file, b"fixture").unwrap();
        }
        MetadataDocument {
            version: 1,
            workspace_root: root.to_path_buf(),
            target_directory: root.join("target"),
            workspace_members: vec!["member-b".to_owned(), "member-a".to_owned()],
            packages: vec![
                MetadataPackage {
                    id: "dependency".to_owned(),
                    name: "ignored-dependency".to_owned(),
                    manifest_path: root.join("ignored/Cargo.toml"),
                    source: Some("registry+https://example.invalid/index".to_owned()),
                    targets: vec![],
                },
                MetadataPackage {
                    id: "member-b".to_owned(),
                    name: "beta".to_owned(),
                    manifest_path: root.join("beta/Cargo.toml"),
                    source: None,
                    targets: vec![MetadataTarget {
                        name: "zeta_test".to_owned(),
                        kind: vec!["test".to_owned()],
                        src_path: root.join("beta/tests/zeta_test.rs"),
                    }],
                },
                MetadataPackage {
                    id: "member-a".to_owned(),
                    name: "alpha".to_owned(),
                    manifest_path: root.join("alpha/Cargo.toml"),
                    source: None,
                    targets: vec![
                        MetadataTarget {
                            name: "library".to_owned(),
                            kind: vec!["lib".to_owned()],
                            src_path: root.join("alpha/src/lib.rs"),
                        },
                        MetadataTarget {
                            name: "integration_b".to_owned(),
                            kind: vec!["test".to_owned()],
                            src_path: root.join("alpha/tests/integration_b.rs"),
                        },
                        MetadataTarget {
                            name: "integration_a".to_owned(),
                            kind: vec!["test".to_owned()],
                            src_path: root.join("alpha/tests/integration_a.rs"),
                        },
                    ],
                },
            ],
        }
    }

    fn captured(bytes: &[u8]) -> crate::CapturedStream {
        crate::CapturedStream {
            head: bytes.to_vec(),
            tail: Vec::new(),
            observed_bytes: bytes.len() as u64,
            omitted_observed_bytes: 0,
            truncated: false,
            complete: true,
        }
    }

    fn command_result(exit_code: Option<i32>) -> CommandResult {
        CommandResult {
            exit_code,
            signal: None,
            timed_out: false,
            cancelled: false,
            stdout: captured(b""),
            stderr: captured(b""),
            truncated: false,
            duration_ms: 1,
        }
    }

    #[test]
    fn limits_reject_every_zero_dimension() {
        assert!(matches!(
            CargoToolLimits::try_new(Duration::ZERO, 1, 1, 1),
            Err(CargoToolError::InvalidLimits)
        ));
        assert!(matches!(
            CargoToolLimits::try_new(Duration::from_secs(1), 0, 1, 1),
            Err(CargoToolError::InvalidLimits)
        ));
        assert!(matches!(
            CargoToolLimits::try_new(Duration::from_secs(1), 1, 0, 1),
            Err(CargoToolError::InvalidLimits)
        ));
        assert!(matches!(
            CargoToolLimits::try_new(Duration::from_secs(1), 1, 1, 0),
            Err(CargoToolError::InvalidLimits)
        ));
    }

    #[cfg(all(windows, target_env = "msvc"))]
    #[test]
    fn msvc_discovery_never_reads_host_environment() {
        for key in [
            "PATH",
            "LIB",
            "INCLUDE",
            "VCINSTALLDIR",
            "VSINSTALLDIR",
            "VCToolsInstallDir",
            "VCToolsVersion",
            "WindowsSdkDir",
        ] {
            assert!(NoHostMsvcEnvironment.get_env(key).is_none());
        }
    }

    #[cfg(all(windows, target_env = "msvc"))]
    #[test]
    fn msvc_environment_accepts_only_canonical_fixed_keys() {
        let (_root, linker, environment) = msvc_fixture();
        let discovered = validate_windows_msvc(&linker, &environment).unwrap();
        assert_eq!(
            discovered.linker.path(),
            std::fs::canonicalize(&linker).unwrap()
        );
        assert_eq!(discovered.search_directories.len(), 1);
        assert!(
            discovered
                .search_directories
                .iter()
                .chain(&discovered.library_directories)
                .chain(&discovered.include_directories)
                .all(|directory| directory.is_absolute() && directory.is_dir())
        );

        let mut unexpected = environment.clone();
        unexpected.push((OsString::from("CL"), OsString::from("/DUNTRUSTED")));
        assert!(matches!(
            validate_windows_msvc(&linker, &unexpected),
            Err(CargoToolError::InvalidEnvironment)
        ));

        let mut duplicate = environment.clone();
        duplicate.push(environment[0].clone());
        assert!(matches!(
            validate_windows_msvc(&linker, &duplicate),
            Err(CargoToolError::InvalidEnvironment)
        ));

        let missing_key = environment
            .iter()
            .filter(|(key, _)| key != "INCLUDE")
            .cloned()
            .collect::<Vec<_>>();
        assert!(matches!(
            validate_windows_msvc(&linker, &missing_key),
            Err(CargoToolError::InvalidEnvironment)
        ));
    }

    #[cfg(all(windows, target_env = "msvc"))]
    #[test]
    fn msvc_environment_fails_closed_for_relative_missing_or_mismatched_paths() {
        let (root, linker, environment) = msvc_fixture();

        let mut relative = environment.clone();
        relative[1].1 = OsString::from("relative-lib");
        assert!(matches!(
            validate_windows_msvc(&linker, &relative),
            Err(CargoToolError::InvalidEnvironment)
        ));

        let mut missing = environment.clone();
        missing[2].1 = root.path().join("missing-include").into_os_string();
        assert!(matches!(
            validate_windows_msvc(&linker, &missing),
            Err(CargoToolError::InvalidEnvironment)
        ));

        let other_binary_directory = root.path().join("other-bin");
        std::fs::create_dir(&other_binary_directory).unwrap();
        let mut mismatched = environment.clone();
        mismatched[0].1 = other_binary_directory.into_os_string();
        assert!(matches!(
            validate_windows_msvc(&linker, &mismatched),
            Err(CargoToolError::InvalidEnvironment)
        ));

        assert!(matches!(
            validate_windows_msvc(&root.path().join("missing-link.exe"), &environment),
            Err(CargoToolError::InvalidEnvironment)
        ));
    }

    #[cfg(all(windows, target_env = "msvc"))]
    #[test]
    fn msvc_linker_is_discovered_from_machine_configuration() {
        let tool = find_tool_with_env(std::env::consts::ARCH, "link.exe", &NoHostMsvcEnvironment)
            .expect("MSVC linker should be discoverable without host environment variables");
        let environment = tool
            .env()
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect::<Vec<_>>();
        let discovered = validate_windows_msvc(tool.path(), &environment).unwrap_or_else(|error| {
            panic!(
                "discovered MSVC linker environment should validate: path={:?}, environment={environment:?}, error={error}",
                tool.path()
            )
        });
        assert!(discovered.linker.path().is_absolute());
        assert_eq!(
            discovered.linker.path().file_name().and_then(OsStr::to_str),
            Some("link.exe")
        );
        assert!(
            discovered.search_directories.iter().any(|directory| {
                discovered.linker.path().parent() == Some(directory.as_path())
            })
        );
        let redaction_paths = discovered.redaction_paths();
        assert!(redaction_paths.contains(&discovered.linker.path().to_owned()));
        assert!(
            discovered
                .search_directories
                .iter()
                .chain(&discovered.library_directories)
                .chain(&discovered.include_directories)
                .all(|path| redaction_paths.contains(path))
        );
    }

    #[test]
    fn catalog_contains_only_workspace_members_in_deterministic_order() {
        let root = tempdir();
        let catalog = catalog_from_metadata(
            metadata(root.path()),
            &execution_directory(root.path()),
            limits(),
        )
        .unwrap();

        assert_eq!(
            catalog
                .packages()
                .iter()
                .map(CargoPackage::name)
                .collect::<Vec<_>>(),
            vec!["alpha", "beta"]
        );
        assert_eq!(
            catalog.packages()[0].integration_tests(),
            &["integration_a".to_owned(), "integration_b".to_owned()]
        );
    }

    #[test]
    fn package_and_test_selectors_must_match_the_refreshed_catalog_exactly() {
        let root = tempdir();
        let catalog = catalog_from_metadata(
            metadata(root.path()),
            &execution_directory(root.path()),
            limits(),
        )
        .unwrap();

        assert!(validate_package(&catalog, "alpha").is_ok());
        assert!(matches!(
            validate_package(&catalog, "Alpha"),
            Err(CargoToolError::UnknownPackage)
        ));
        assert!(validate_test_selection(&catalog, "alpha", Some("integration_a")).is_ok());
        assert!(matches!(
            validate_test_selection(&catalog, "alpha", Some("Integration_A")),
            Err(CargoToolError::UnknownIntegrationTest)
        ));
        assert!(matches!(
            validate_test_selection(&catalog, "missing", None),
            Err(CargoToolError::UnknownPackage)
        ));
        for unbound in [
            "integration-a",
            "integration_a ",
            "integration_a/../escape",
            "--test",
        ] {
            assert!(matches!(
                validate_test_selection(&catalog, "alpha", Some(unbound)),
                Err(CargoToolError::UnknownIntegrationTest)
            ));
        }
    }

    #[test]
    fn catalog_rejects_wrong_root_missing_member_and_duplicate_names() {
        let root = tempdir();
        let other = tempdir();
        assert!(matches!(
            catalog_from_metadata(
                metadata(other.path()),
                &execution_directory(root.path()),
                limits()
            ),
            Err(CargoToolError::WorkspaceRootMismatch)
        ));

        let mut missing = metadata(root.path());
        missing.packages.retain(|package| package.id != "member-a");
        assert!(matches!(
            catalog_from_metadata(missing, &execution_directory(root.path()), limits()),
            Err(CargoToolError::MetadataInvalid)
        ));

        let mut duplicate = metadata(root.path());
        duplicate.packages[1].name = "alpha".to_owned();
        assert!(matches!(
            catalog_from_metadata(duplicate, &execution_directory(root.path()), limits()),
            Err(CargoToolError::MetadataInvalid)
        ));
    }

    #[test]
    fn catalog_enforces_package_target_and_name_caps() {
        let root = tempdir();
        let package_limited_metadata = metadata(root.path());
        assert!(matches!(
            catalog_from_metadata(
                package_limited_metadata,
                &execution_directory(root.path()),
                CargoToolLimits::try_new(Duration::from_secs(1), 1, 8, 64).unwrap()
            ),
            Err(CargoToolError::CatalogTooLarge)
        ));

        let target_limited_metadata = metadata(root.path());
        assert!(matches!(
            catalog_from_metadata(
                target_limited_metadata,
                &execution_directory(root.path()),
                CargoToolLimits::try_new(Duration::from_secs(1), 4, 2, 64).unwrap()
            ),
            Err(CargoToolError::CatalogTooLarge)
        ));

        let name_limited_metadata = metadata(root.path());
        assert!(matches!(
            catalog_from_metadata(
                name_limited_metadata,
                &execution_directory(root.path()),
                CargoToolLimits::try_new(Duration::from_secs(1), 4, 8, 3).unwrap()
            ),
            Err(CargoToolError::MetadataInvalid)
        ));
    }

    #[test]
    fn metadata_paths_are_absolute_plain_entries_opened_from_the_bound_root() {
        let root = tempdir();
        let outside = tempdir();
        std::fs::write(outside.path().join("outside.rs"), b"outside").unwrap();

        let mut external_manifest = metadata(root.path());
        external_manifest.packages[2].manifest_path = outside.path().join("Cargo.toml");
        assert!(matches!(
            catalog_from_metadata(
                external_manifest,
                &execution_directory(root.path()),
                limits()
            ),
            Err(CargoToolError::MetadataPathOutsideWorkspace)
        ));

        let mut external_source = metadata(root.path());
        external_source.packages[2].targets[0].src_path = outside.path().join("outside.rs");
        assert!(matches!(
            catalog_from_metadata(external_source, &execution_directory(root.path()), limits()),
            Err(CargoToolError::MetadataPathOutsideWorkspace)
        ));

        let mut external_target = metadata(root.path());
        external_target.target_directory = outside.path().to_path_buf();
        assert!(matches!(
            catalog_from_metadata(external_target, &execution_directory(root.path()), limits()),
            Err(CargoToolError::MetadataPathOutsideWorkspace)
        ));

        let mut relative_manifest = metadata(root.path());
        relative_manifest.packages[2].manifest_path = PathBuf::from("alpha/Cargo.toml");
        assert!(matches!(
            catalog_from_metadata(
                relative_manifest,
                &execution_directory(root.path()),
                limits()
            ),
            Err(CargoToolError::MetadataPathOutsideWorkspace)
        ));

        let mut sourced_member = metadata(root.path());
        sourced_member.packages[2].source = Some("registry+https://example.invalid".to_owned());
        assert!(matches!(
            catalog_from_metadata(sourced_member, &execution_directory(root.path()), limits()),
            Err(CargoToolError::MetadataPathOutsideWorkspace)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn metadata_source_symlink_cannot_escape_the_bound_root() {
        let root = tempdir();
        let outside = tempdir();
        let outside_source = outside.path().join("outside.rs");
        std::fs::write(&outside_source, b"outside").unwrap();
        let mut document = metadata(root.path());
        let link = root.path().join("alpha/src/link.rs");
        std::os::unix::fs::symlink(&outside_source, &link).unwrap();
        document.packages[2].targets[0].src_path = link;

        assert!(matches!(
            catalog_from_metadata(document, &execution_directory(root.path()), limits()),
            Err(CargoToolError::MetadataPathOutsideWorkspace)
        ));

        let mut document = metadata(root.path());
        let target_link = root.path().join("target-link");
        std::os::unix::fs::symlink(outside.path(), &target_link).unwrap();
        document.target_directory = target_link;
        assert!(matches!(
            catalog_from_metadata(document, &execution_directory(root.path()), limits()),
            Err(CargoToolError::MetadataPathOutsideWorkspace)
        ));
    }

    #[cfg(windows)]
    #[test]
    fn metadata_source_symlink_cannot_escape_the_bound_root_when_links_are_available() {
        let root = tempdir();
        let outside = tempdir();
        let outside_source = outside.path().join("outside.rs");
        std::fs::write(&outside_source, b"outside").unwrap();
        let mut document = metadata(root.path());
        let link = root.path().join("alpha/src/link.rs");
        if std::os::windows::fs::symlink_file(&outside_source, &link).is_ok() {
            document.packages[2].targets[0].src_path = link;
            assert!(matches!(
                catalog_from_metadata(document, &execution_directory(root.path()), limits()),
                Err(CargoToolError::MetadataPathOutsideWorkspace)
            ));
        }

        let mut document = metadata(root.path());
        let target_link = root.path().join("target-link");
        if std::os::windows::fs::symlink_dir(outside.path(), &target_link).is_ok() {
            document.target_directory = target_link;
            assert!(matches!(
                catalog_from_metadata(document, &execution_directory(root.path()), limits()),
                Err(CargoToolError::MetadataPathOutsideWorkspace)
            ));
        }
    }

    #[test]
    fn status_precedence_is_cancelled_then_timed_out_then_exit() {
        let mut result = command_result(Some(0));
        assert_eq!(classify_run(&result), CargoRunStatus::Passed);
        result.exit_code = Some(7);
        assert_eq!(classify_run(&result), CargoRunStatus::Failed);
        result.timed_out = true;
        assert_eq!(classify_run(&result), CargoRunStatus::TimedOut);
        result.cancelled = true;
        assert_eq!(classify_run(&result), CargoRunStatus::Cancelled);
    }

    #[test]
    fn cargo_check_and_test_results_report_offline_dependencies_after_terminal_precedence() {
        let offline = b"error: no matching package named `missing` found while using offline mode";

        let mut cancelled = command_result(Some(1));
        cancelled.cancelled = true;
        cancelled.timed_out = true;
        cancelled.stderr = captured(offline);
        assert_eq!(
            cargo_run_result(cancelled).unwrap().status,
            CargoRunStatus::Cancelled
        );

        let mut timed_out = command_result(Some(1));
        timed_out.timed_out = true;
        timed_out.stderr = captured(offline);
        assert_eq!(
            cargo_run_result(timed_out).unwrap().status,
            CargoRunStatus::TimedOut
        );

        let mut unavailable = command_result(Some(1));
        unavailable.stderr = captured(offline);
        assert!(matches!(
            cargo_run_result(unavailable),
            Err(CargoToolError::DependencyUnavailableOffline)
        ));
    }

    #[test]
    fn metadata_requires_complete_untruncated_stdout() {
        let mut result = command_result(Some(0));
        result.stdout = CapturedStream {
            head: b"{}".to_vec(),
            tail: b"tail".to_vec(),
            observed_bytes: 12,
            omitted_observed_bytes: 6,
            truncated: true,
            complete: true,
        };
        result.truncated = true;
        assert!(matches!(
            complete_output(&result),
            Err(CargoToolError::MetadataOutputIncomplete)
        ));
    }

    #[test]
    fn metadata_json_is_parsed_only_after_a_successful_complete_command() {
        let root = tempdir();
        for directory in [
            root.path().join("demo/src"),
            root.path().join("demo/tests"),
            root.path().join("target"),
        ] {
            std::fs::create_dir_all(directory).unwrap();
        }
        for file in [
            root.path().join("demo/Cargo.toml"),
            root.path().join("demo/src/lib.rs"),
            root.path().join("demo/tests/api_test.rs"),
        ] {
            std::fs::write(file, b"fixture").unwrap();
        }
        let encoded = serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "workspace_root": root.path(),
            "target_directory": root.path().join("target"),
            "workspace_members": ["member"],
            "packages": [{
                "id": "member",
                "name": "demo_package",
                "manifest_path": root.path().join("demo/Cargo.toml"),
                "source": null,
                "targets": [
                    {
                        "name": "demo_package",
                        "kind": ["lib"],
                        "src_path": root.path().join("demo/src/lib.rs")
                    },
                    {
                        "name": "api_test",
                        "kind": ["test"],
                        "src_path": root.path().join("demo/tests/api_test.rs")
                    }
                ]
            }]
        }))
        .unwrap();
        let mut result = command_result(Some(0));
        result.stdout = captured(&encoded);

        let catalog =
            parse_metadata_result(result, &execution_directory(root.path()), None, limits())
                .unwrap();

        assert_eq!(catalog.packages()[0].name(), "demo_package");
        assert_eq!(
            catalog.packages()[0].integration_tests(),
            &["api_test".to_owned()]
        );
    }

    #[test]
    fn parses_real_offline_metadata_for_a_temporary_dependency_free_workspace() {
        let root = tempdir();
        std::fs::create_dir_all(root.path().join("demo/src")).unwrap();
        std::fs::create_dir_all(root.path().join("demo/tests")).unwrap();
        // The production worktree provisioner must likewise create the fixed
        // in-worktree target directory before CargoTools is constructed. Cargo
        // metadata reports the path but does not create it, and accepting an
        // unbound missing name would permit a symlink substitution race before
        // check/test.
        std::fs::create_dir_all(root.path().join("target")).unwrap();
        std::fs::write(
            root.path().join("Cargo.toml"),
            b"[workspace]\nmembers = [\"demo\"]\nresolver = \"2\"\n",
        )
        .unwrap();
        std::fs::write(
            root.path().join("demo/Cargo.toml"),
            b"[package]\nname = \"demo_package\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
        )
        .unwrap();
        std::fs::write(root.path().join("demo/src/lib.rs"), b"pub fn value() {}\n").unwrap();
        std::fs::write(
            root.path().join("demo/tests/api_test.rs"),
            b"#[test]\nfn api_test() {}\n",
        )
        .unwrap();

        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let mut command = std::process::Command::new(cargo);
        command
            .current_dir(root.path())
            .args(["metadata", "--format-version=1", "--no-deps", "--offline"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let child = {
            let _spawn_guard = crate::acquire_process_spawn_lock();
            command.spawn().unwrap()
        };
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mut result = command_result(Some(0));
        result.stdout = captured(&output.stdout);
        result.stderr = captured(&output.stderr);

        let catalog =
            parse_metadata_result(result, &execution_directory(root.path()), None, limits())
                .unwrap();
        assert_eq!(catalog.packages().len(), 1);
        assert_eq!(catalog.packages()[0].name(), "demo_package");
        assert_eq!(
            catalog.packages()[0].integration_tests(),
            &["api_test".to_owned()]
        );
    }

    #[test]
    fn metadata_cancellation_precedes_timeout_and_offline_failure_is_specific() {
        let root = tempdir();
        let mut result = command_result(Some(1));
        result.cancelled = true;
        result.timed_out = true;
        result.stderr = captured(
            b"error: no matching package named `missing` found; offline mode was requested",
        );
        assert!(matches!(
            parse_metadata_result(result, &execution_directory(root.path()), None, limits()),
            Err(CargoToolError::Cancelled)
        ));

        let mut result = command_result(Some(1));
        result.stderr = captured(
            b"error: no matching package named `missing` found; offline mode was requested",
        );
        assert!(matches!(
            parse_metadata_result(result, &execution_directory(root.path()), None, limits()),
            Err(CargoToolError::DependencyUnavailableOffline)
        ));
    }

    #[test]
    fn stable_error_codes_do_not_include_cargo_output() {
        assert_eq!(CargoToolError::UnknownPackage.code(), "COMMAND_NOT_ALLOWED");
        assert_eq!(CargoToolError::Cancelled.code(), "COMMAND_CANCELLED");
        assert_eq!(CargoToolError::TimedOut.code(), "COMMAND_TIMED_OUT");
        assert_eq!(
            CargoToolError::MetadataInvalid.code(),
            "CARGO_METADATA_FAILED"
        );
        assert_eq!(
            CargoToolError::DependencyUnavailableOffline.code(),
            "CARGO_DEPENDENCY_UNAVAILABLE_OFFLINE"
        );

        let policy = CargoToolError::CommandPolicy(CommandPolicyError::InvalidCargoSelection);
        assert!(std::error::Error::source(&policy).is_some());
    }
}
