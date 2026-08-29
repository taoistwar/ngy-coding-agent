use std::any::Any;
use std::collections::{HashMap, hash_map::Entry};
use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coding_agent_domain::{CanonicalPath, Repository, RepositoryId};
use coding_agent_provider::{ChatCompletionsClient, ClientLimits};
use coding_agent_runtime::{
    DirectoryIdentityMarker, ExecutionDirectory, NativeVolumeSampler, ProbedDeliveryGit,
    ProcessLimits, ProcessLivenessScope, RepositoryDiscoveryCommands, RootCapability,
    ToolchainPaths, VolumeSampler, WorktreeLimits, WorktreeProvisioner, discover_toolchain,
    probe_delivery_git as probe_delivery_git_capabilities,
};
#[cfg(feature = "test-support")]
use coding_agent_runtime::{ProcessFault, ProcessFaultController};
use tokio::sync::Semaphore;
use tokio::time::{Instant, timeout_at};
use tokio_util::sync::CancellationToken;

use crate::repository_service::{
    AuthenticatedRepositoryRuntime, DEFAULT_APPLICATION_WRITE_BUDGET_MILLIS,
    RepositoryRuntimeAttachmentError, RepositoryRuntimeAttachmentRegistry,
    RepositoryRuntimeRegistrar, RepositoryRuntimeRegistrationError,
};
use crate::task_manager::TaskManagerStorageSignals;
use crate::{
    CodingAgentPreparationControl, CodingAgentRunner, CodingAgentRunnerConfig, CodingAttemptError,
    FilesystemRepositoryIdentityResolver, MonitoredStorageScopeBinding, PlatformPaths,
    Project2RuntimeSessionFactory, RepositoryControlCoordinator, RepositoryDiscovery,
    RepositoryWorktreeProvisionerFactory, SchedulerConcurrencyLimits,
    StartupDirectStoreArtifactAdapter, StorageMonitorConfig, StorageMonitorHandle, StoragePolicy,
    StorageProbeTarget, TaskManagerLaunchResources, TaskRunner, TokioStorageMonitorClock,
    WorktreeArtifactObserver, WorktreeCodingAgentAttemptFactory, load_provider_config,
};

mod context;
mod delivery;

#[cfg(feature = "test-support")]
type TestDeliveryTargetBoundaryHook = Arc<dyn Fn(&'static str) + Send + Sync + 'static>;

#[cfg(feature = "test-support")]
#[derive(Clone)]
pub(crate) struct TestDeliveryTargetBoundary {
    repository_path: std::path::PathBuf,
    hook: TestDeliveryTargetBoundaryHook,
}

#[cfg(feature = "test-support")]
impl TestDeliveryTargetBoundary {
    pub(crate) fn new(
        repository_path: std::path::PathBuf,
        hook: TestDeliveryTargetBoundaryHook,
    ) -> Self {
        Self {
            repository_path,
            hook,
        }
    }

    pub(crate) fn matches(&self, repository: &Repository) -> bool {
        repository.git_root.as_path() == self.repository_path
    }

    pub(crate) fn hook(&self) -> TestDeliveryTargetBoundaryHook {
        Arc::clone(&self.hook)
    }
}

#[cfg(feature = "test-support")]
#[derive(Clone)]
pub(crate) struct TestDeliveryProcessFaultBoundary {
    repository_path: std::path::PathBuf,
    controller: Arc<Mutex<Option<ProcessFaultController>>>,
}

#[cfg(feature = "test-support")]
impl TestDeliveryProcessFaultBoundary {
    pub(crate) fn authenticate_preflight_first_child_cleanup_failure(
        repository_path: std::path::PathBuf,
    ) -> Self {
        Self {
            repository_path,
            controller: Arc::new(Mutex::new(Some(
                ProcessFaultController::for_child(1, ProcessFault::CleanupFailure)
                    .expect("the fixed delivery process-fault schedule is valid"),
            ))),
        }
    }

    pub(crate) fn matches(&self, repository: &Repository) -> bool {
        repository.git_root.as_path() == self.repository_path
    }

    pub(crate) fn take_controller(&self) -> Option<ProcessFaultController> {
        self.controller
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use delivery::production_delivery_registries_for_test;

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn production_delivery_dynamic_registries_for_test(
    store: coding_agent_store::Store,
    probe: Arc<ProbedDeliveryGit>,
    toolchain: ToolchainPaths,
    artifact_root: std::path::PathBuf,
    temporary_directory: std::path::PathBuf,
    repository_control: Arc<RepositoryControlCoordinator>,
    instance_process_scope: ProcessLivenessScope,
) -> (
    Arc<dyn crate::DeliveryRuntimeRegistry>,
    Arc<dyn crate::DeliveryLiveRuntimeRegistry>,
    Arc<dyn crate::DeliveryCleanupRuntimeRegistry>,
) {
    let provisioners = Arc::new(ProductionWorktreeProvisioners {
        toolchain: Arc::new(toolchain),
        artifact_root,
        temporary_directory,
        process_limits: production_process_limits(),
        worktree_limits: WorktreeLimits::try_new(Duration::from_secs(60))
            .expect("constant production worktree limits are valid"),
        instance_process_scope,
        sampler: Arc::new(NativeVolumeSampler::new()),
        prepare_slots: Arc::new(Semaphore::new(RUNTIME_ATTACHMENT_PREPARE_CONCURRENCY)),
        bound: Mutex::new(HashMap::new()),
    });
    let prepared =
        delivery::production_delivery_runtime(store, probe, provisioners, repository_control);
    (
        prepared.runtime(),
        prepared.live_runtime(),
        prepared.cleanup_runtime(),
    )
}

pub(crate) use context::ValidatedStartupInputs;
pub use context::{PreActorStartupRunnerContext, StartupRunnerContext};
use delivery::PreparedDeliveryStartup;

const ARTIFACT_RECONCILIATION_TIMEOUT: Duration = Duration::from_secs(5);
const DELIVERY_GIT_PROBE_TIMEOUT: Duration = Duration::from_secs(30);
const RUNTIME_ATTACHMENT_PREPARE_CONCURRENCY: usize = 4;
const REPOSITORY_DISCOVERY_COMMAND_TIMEOUT_MILLIS: u64 = 1_000;
const REPOSITORY_DISCOVERY_CLEANUP_TIMEOUT_MILLIS: u64 = 500;
#[cfg(any(test, feature = "test-support"))]
const PROCESS_TEST_REPOSITORY_DISCOVERY_COMMAND_TIMEOUT_MILLIS: u64 = 4_000;
const REPOSITORY_POST_DISCOVERY_RESERVE_MILLIS: u64 = 2_000;
// Two fixed discovery commands and one worst-case tree cleanup must leave a
// positive, explicit budget for the durable write and runtime attachments.
const _: () = assert!(
    DEFAULT_APPLICATION_WRITE_BUDGET_MILLIS
        > (2 * REPOSITORY_DISCOVERY_COMMAND_TIMEOUT_MILLIS)
            + REPOSITORY_DISCOVERY_CLEANUP_TIMEOUT_MILLIS
            + REPOSITORY_POST_DISCOVERY_RESERVE_MILLIS
);
#[cfg(any(test, feature = "test-support"))]
const _: () = assert!(
    DEFAULT_APPLICATION_WRITE_BUDGET_MILLIS
        > PROCESS_TEST_REPOSITORY_DISCOVERY_COMMAND_TIMEOUT_MILLIS
            + REPOSITORY_DISCOVERY_CLEANUP_TIMEOUT_MILLIS
);

fn build_storage_monitor(
    context: &StartupRunnerContext,
    sampler: Arc<dyn VolumeSampler>,
    storage_signals: TaskManagerStorageSignals,
) -> Result<StorageMonitorHandle, StartupRunnerFactoryError> {
    let path_target = |path: &std::path::Path| {
        let root = Arc::new(
            RootCapability::open(path)
                .map_err(|_| StartupRunnerFactoryError::new("STORAGE_MONITOR_UNAVAILABLE"))?,
        );
        let sample = sampler
            .sample(&root)
            .map_err(|_| StartupRunnerFactoryError::new("STORAGE_MONITOR_UNAVAILABLE"))?;
        Ok(StorageProbeTarget::new(sample.identity(), root))
    };
    let bindings = vec![
        MonitoredStorageScopeBinding::data(path_target(context.paths().data_dir.as_path())?),
        MonitoredStorageScopeBinding::runtime(path_target(context.paths().runtime_dir.as_path())?),
    ];
    let policy = StoragePolicy::try_from_runtime_config(context.runtime_config())
        .map_err(|_| StartupRunnerFactoryError::new("STORAGE_POLICY_INVALID"))?;
    StorageMonitorHandle::spawn(
        StorageMonitorConfig::new(
            policy,
            sampler,
            Arc::new(TokioStorageMonitorClock::new()),
            Arc::new(storage_signals.clone()),
            Arc::new(storage_signals),
        ),
        bindings,
    )
    .map_err(|_| StartupRunnerFactoryError::new("STORAGE_MONITOR_UNAVAILABLE"))
}

/// One inseparable runner/concurrency selection returned by startup.
#[derive(Clone)]
pub struct StartupRunnerSelection {
    runner: Arc<dyn TaskRunner>,
    launch_resources: TaskManagerLaunchResources,
    repository_registrar: Option<RepositoryRuntimeRegistrar>,
    repository_discovery: Option<RepositoryDiscovery>,
    delivery_startup: Option<PreparedDeliveryStartup>,
}

impl StartupRunnerSelection {
    #[cfg(any(test, feature = "test-support"))]
    pub fn new(runner: Arc<dyn TaskRunner>, launch_resources: TaskManagerLaunchResources) -> Self {
        Self {
            runner,
            launch_resources,
            repository_registrar: None,
            repository_discovery: None,
            delivery_startup: None,
        }
    }

    fn with_repository_runtime(
        runner: Arc<dyn TaskRunner>,
        launch_resources: TaskManagerLaunchResources,
        repository_registrar: RepositoryRuntimeRegistrar,
        repository_discovery: RepositoryDiscovery,
        delivery_startup: PreparedDeliveryStartup,
    ) -> Self {
        Self {
            runner,
            launch_resources,
            repository_registrar: Some(repository_registrar),
            repository_discovery: Some(repository_discovery),
            delivery_startup: Some(delivery_startup),
        }
    }

    pub fn runner(&self) -> Arc<dyn TaskRunner> {
        Arc::clone(&self.runner)
    }

    pub const fn concurrency(&self) -> NonZeroU32 {
        self.launch_resources.limits().global()
    }

    pub fn launch_resources(&self) -> TaskManagerLaunchResources {
        self.launch_resources.clone()
    }

    pub(crate) fn repository_registrar(&self) -> Option<RepositoryRuntimeRegistrar> {
        self.repository_registrar.clone()
    }

    pub(crate) fn repository_discovery(&self) -> Option<RepositoryDiscovery> {
        self.repository_discovery.clone()
    }

    pub(crate) fn delivery_startup(&self) -> Option<PreparedDeliveryStartup> {
        self.delivery_startup.clone()
    }
}

#[async_trait::async_trait]
pub trait StartupRunnerFactory: Send + Sync + 'static {
    async fn validate_pre_database(
        &self,
        _paths: &PlatformPaths,
    ) -> Result<Arc<dyn Any + Send + Sync>, StartupRunnerFactoryError> {
        Ok(Arc::new(()))
    }

    /// Proves the delivery Git capabilities after prior process ownership is
    /// exclusive and before SQLite is opened or migrated.
    async fn probe_delivery_git_pre_database(
        &self,
        _paths: &PlatformPaths,
        _process_liveness_scope: ProcessLivenessScope,
    ) -> Result<Arc<dyn Any + Send + Sync>, StartupRunnerFactoryError> {
        Ok(Arc::new(()))
    }

    /// Performs all startup work which must finish before cold recovery and
    /// before any dispatcher, writer, task-manager, or listener exists.
    async fn prepare_before_actors(
        &self,
        _context: &PreActorStartupRunnerContext,
    ) -> Result<Arc<dyn Any + Send + Sync>, StartupRunnerFactoryError> {
        Ok(Arc::new(()))
    }

    /// Constructs only live runner capabilities from the immutable pre-actor
    /// result. Production implementations must not reload configuration or
    /// repeat startup direct-Store reconciliation here.
    async fn create(
        &self,
        context: StartupRunnerContext,
    ) -> Result<StartupRunnerSelection, StartupRunnerFactoryError>;
}

/// Secret-safe startup failure. Detailed source errors remain behind the
/// composition boundary and must never include provider credentials or paths.
#[derive(Clone, PartialEq, Eq)]
pub struct StartupRunnerFactoryError {
    code: String,
    message: &'static str,
}

impl StartupRunnerFactoryError {
    pub fn new(code: impl Into<String>) -> Self {
        let code = code.into();
        let code = if valid_code(&code) {
            code
        } else {
            "RUNNER_STARTUP_FAILED".to_owned()
        };
        Self {
            code,
            message: "the coding task runner could not be started",
        }
    }

    pub fn code(&self) -> &str {
        &self.code
    }
}

impl fmt::Debug for StartupRunnerFactoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StartupRunnerFactoryError")
            .field("code", &self.code)
            .field("message", &self.message)
            .finish()
    }
}

impl fmt::Display for StartupRunnerFactoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for StartupRunnerFactoryError {}

fn valid_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 96
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

/// Explicit fixed selection for tests; production never constructs this type.
#[cfg(any(test, feature = "test-support"))]
pub struct FixedStartupRunnerFactory {
    runner: Arc<dyn TaskRunner>,
    scheduler_limits: FixedSchedulerLimitsMode,
    sampler: Arc<dyn VolumeSampler>,
    repository_discovery: FixedRepositoryDiscoveryMode,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FixedSchedulerLimitsMode {
    Fixed(NonZeroU32),
    RuntimeConfig,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FixedRepositoryDiscoveryMode {
    Disabled,
    Supervised(ProcessLimits),
}

/// Production composition root. It is intentionally inert until the locked
/// primary invokes [`StartupRunnerFactory::create`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ProductionStartupRunnerFactory;

struct PreparedProductionRunner {
    toolchain: Arc<ToolchainPaths>,
    delivery_git: Arc<ProbedDeliveryGit>,
    repository_discovery: RepositoryDiscovery,
    provisioners: Arc<ProductionWorktreeProvisioners>,
    repositories: Vec<Repository>,
    repository_attachments: HashMap<RepositoryId, AuthenticatedRepositoryRuntime>,
    repository_control: Arc<RepositoryControlCoordinator>,
    repository_identity_resolver: Arc<FilesystemRepositoryIdentityResolver>,
    scheduler_limits: SchedulerConcurrencyLimits,
    sampler: Arc<dyn VolumeSampler>,
    delivery_startup: PreparedDeliveryStartup,
}

struct ProbedProductionRuntime {
    toolchain: Arc<ToolchainPaths>,
    delivery_git: Arc<ProbedDeliveryGit>,
}

#[cfg(any(test, feature = "test-support"))]
struct PreparedFixedRunner {
    repository_discovery: RepositoryDiscovery,
    attachments: Arc<FixedRepositoryRuntimeAttachments>,
    repositories: Vec<Repository>,
    repository_attachments: HashMap<RepositoryId, AuthenticatedRepositoryRuntime>,
    repository_control: Arc<RepositoryControlCoordinator>,
    sampler: Arc<dyn VolumeSampler>,
    scheduler_limits: SchedulerConcurrencyLimits,
    delivery_startup: PreparedDeliveryStartup,
}

async fn prepare_repository_runtime<R>(
    context: &PreActorStartupRunnerContext,
    repositories: &[Repository],
    repository_control: &RepositoryControlCoordinator,
    attachments: &R,
) -> Result<HashMap<RepositoryId, AuthenticatedRepositoryRuntime>, StartupRunnerFactoryError>
where
    R: RepositoryRuntimeAttachmentRegistry + ?Sized,
{
    let mut prepared = HashMap::with_capacity(repositories.len());
    let deadline = Instant::now() + ARTIFACT_RECONCILIATION_TIMEOUT;
    for repository in repositories {
        let lookup = context
            .store()
            .repository_identity_lookup(repository.id)
            .await
            .map_err(|_| StartupRunnerFactoryError::new("REPOSITORY_IDENTITY_UNAVAILABLE"))?
            .ok_or_else(|| StartupRunnerFactoryError::new("REPOSITORY_IDENTITY_UNAVAILABLE"))?;
        let attachment = attachments
            .attach(repository, deadline)
            .await
            .map_err(|_| StartupRunnerFactoryError::new("REPOSITORY_IDENTITY_UNAVAILABLE"))?;
        let coordination_key = repository_control
            .register_authenticated_alias(lookup, attachment.common_git_identity())
            .map_err(|_| StartupRunnerFactoryError::new("REPOSITORY_IDENTITY_UNAVAILABLE"))?;
        if coordination_key
            != crate::RepositoryCoordinationKey::from_authenticated_marker(
                attachment.common_git_identity(),
            )
        {
            return Err(StartupRunnerFactoryError::new(
                "REPOSITORY_IDENTITY_UNAVAILABLE",
            ));
        }
        prepared.insert(repository.id, attachment);
    }
    Ok(prepared)
}

async fn register_prepared_storage_scopes(
    repositories: &[Repository],
    repository_attachments: &HashMap<RepositoryId, AuthenticatedRepositoryRuntime>,
    repository_control: &RepositoryControlCoordinator,
    storage_monitor: &StorageMonitorHandle,
) -> Result<(), StartupRunnerFactoryError> {
    let deadline = Instant::now() + ARTIFACT_RECONCILIATION_TIMEOUT;
    for repository in repositories {
        let attachment = repository_attachments
            .get(&repository.id)
            .ok_or_else(|| StartupRunnerFactoryError::new("REPOSITORY_IDENTITY_UNAVAILABLE"))?;
        let coordination_key = repository_control
            .coordination_key(repository.id)
            .map_err(|_| StartupRunnerFactoryError::new("REPOSITORY_IDENTITY_UNAVAILABLE"))?;
        timeout_at(
            deadline,
            storage_monitor.register_repository_scope(
                repository.id,
                coordination_key,
                attachment.storage_target(),
            ),
        )
        .await
        .map_err(|_| StartupRunnerFactoryError::new("STORAGE_MONITOR_UNAVAILABLE"))?
        .map_err(|_| StartupRunnerFactoryError::new("STORAGE_MONITOR_UNAVAILABLE"))?;
    }
    Ok(())
}

struct ProductionWorktreeProvisioners {
    toolchain: Arc<ToolchainPaths>,
    artifact_root: std::path::PathBuf,
    temporary_directory: std::path::PathBuf,
    process_limits: ProcessLimits,
    worktree_limits: WorktreeLimits,
    instance_process_scope: ProcessLivenessScope,
    sampler: Arc<dyn VolumeSampler>,
    prepare_slots: Arc<Semaphore>,
    bound: Mutex<HashMap<RepositoryId, BoundProductionProvisioner>>,
}

struct BoundProductionProvisioner {
    git_root: CanonicalPath,
    cargo_workspace_root: CanonicalPath,
    common_git_identity: DirectoryIdentityMarker,
    attachment: AuthenticatedRepositoryRuntime,
    startup_provisioner: Arc<WorktreeProvisioner>,
}

impl ProductionWorktreeProvisioners {
    fn build_provisioner(
        &self,
        repository: &Repository,
        process_liveness_scope: ProcessLivenessScope,
    ) -> Result<Arc<WorktreeProvisioner>, CodingAttemptError> {
        WorktreeProvisioner::from_trusted_paths(
            &self.toolchain,
            repository.id.to_string(),
            repository.git_root.as_path(),
            repository.cargo_workspace_root.as_path(),
            &self.artifact_root,
            &self.temporary_directory,
            process_liveness_scope,
            self.process_limits,
            self.worktree_limits,
        )
        .map(Arc::new)
        .map_err(|error| CodingAttemptError::new(error.code(), false))
    }

    fn startup_provisioner(
        &self,
        repository_id: RepositoryId,
    ) -> Result<Arc<WorktreeProvisioner>, RepositoryRuntimeRegistrationError> {
        self.bound
            .lock()
            .map_err(|_| RepositoryRuntimeRegistrationError::AttachmentUnavailable)?
            .get(&repository_id)
            .map(|bound| Arc::clone(&bound.startup_provisioner))
            .ok_or(RepositoryRuntimeRegistrationError::AttachmentUnavailable)
    }
}

#[async_trait::async_trait]
impl RepositoryRuntimeAttachmentRegistry for ProductionWorktreeProvisioners {
    async fn attach(
        &self,
        repository: &Repository,
        deadline: Instant,
    ) -> Result<AuthenticatedRepositoryRuntime, RepositoryRuntimeAttachmentError> {
        let permit = timeout_at(deadline, Arc::clone(&self.prepare_slots).acquire_owned())
            .await
            .map_err(|_| RepositoryRuntimeAttachmentError::DeadlineExceeded)?
            .map_err(|_| RepositoryRuntimeAttachmentError::Unavailable)?;
        let toolchain = Arc::clone(&self.toolchain);
        let artifact_root = self.artifact_root.clone();
        let temporary_directory = self.temporary_directory.clone();
        let process_limits = self.process_limits;
        let worktree_limits = self.worktree_limits;
        let process_liveness_scope = self.instance_process_scope.clone();
        let sampler = Arc::clone(&self.sampler);
        let repository_id = repository.id;
        let repository = repository.clone();
        let candidate = timeout_at(
            deadline,
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let provisioner = WorktreeProvisioner::from_trusted_paths(
                    &toolchain,
                    repository.id.to_string(),
                    repository.git_root.as_path(),
                    repository.cargo_workspace_root.as_path(),
                    &artifact_root,
                    &temporary_directory,
                    process_liveness_scope,
                    process_limits,
                    worktree_limits,
                )
                .map(Arc::new)
                .map_err(|error| {
                    if matches!(
                        error.code(),
                        "REPOSITORY_IDENTITY_UNAVAILABLE" | "REPOSITORY_INVALID"
                    ) {
                        RepositoryRuntimeAttachmentError::IdentityUnavailable
                    } else {
                        RepositoryRuntimeAttachmentError::Unavailable
                    }
                })?;
                let common_git_identity = provisioner.common_git_identity_marker();
                let common_git = Arc::new(
                    provisioner
                        .clone_common_git_capability_for_volume_sampling()
                        .map_err(|_| RepositoryRuntimeAttachmentError::IdentityUnavailable)?,
                );
                let volume = sampler
                    .sample(&common_git)
                    .map_err(|_| RepositoryRuntimeAttachmentError::StorageUnavailable)?
                    .identity();
                let attachment = AuthenticatedRepositoryRuntime::new(
                    common_git_identity,
                    StorageProbeTarget::new(volume, common_git),
                );
                Ok::<_, RepositoryRuntimeAttachmentError>(BoundProductionProvisioner {
                    git_root: repository.git_root.clone(),
                    cargo_workspace_root: repository.cargo_workspace_root.clone(),
                    common_git_identity,
                    attachment,
                    startup_provisioner: provisioner,
                })
            }),
        )
        .await
        .map_err(|_| RepositoryRuntimeAttachmentError::DeadlineExceeded)?
        .map_err(|_| RepositoryRuntimeAttachmentError::Unavailable)??;
        if Instant::now() >= deadline {
            return Err(RepositoryRuntimeAttachmentError::DeadlineExceeded);
        }

        let mut bound = self
            .bound
            .try_lock()
            .map_err(|_| RepositoryRuntimeAttachmentError::Unavailable)?;
        match bound.entry(repository_id) {
            Entry::Vacant(entry) => {
                let attachment = candidate.attachment.clone();
                entry.insert(candidate);
                Ok(attachment)
            }
            Entry::Occupied(entry)
                if entry.get().git_root == candidate.git_root
                    && entry.get().cargo_workspace_root == candidate.cargo_workspace_root
                    && entry.get().common_git_identity == candidate.common_git_identity =>
            {
                Ok(entry.get().attachment.clone())
            }
            Entry::Occupied(entry) => Err(RepositoryRuntimeAttachmentError::IdentityConflict {
                expected: entry.get().common_git_identity,
                observed: candidate.common_git_identity,
            }),
        }
    }
}

impl RepositoryWorktreeProvisionerFactory for ProductionWorktreeProvisioners {
    fn create(
        &self,
        repository: &Repository,
        process_liveness_scope: ProcessLivenessScope,
    ) -> Result<Arc<WorktreeProvisioner>, CodingAttemptError> {
        let (git_root, cargo_workspace_root, common_git_identity) = self
            .bound
            .lock()
            .map_err(|_| CodingAttemptError::new("REPOSITORY_INVALID", false))?
            .get(&repository.id)
            .map(|bound| {
                (
                    bound.git_root.clone(),
                    bound.cargo_workspace_root.clone(),
                    bound.common_git_identity,
                )
            })
            .ok_or_else(|| CodingAttemptError::new("REPOSITORY_INVALID", false))?;
        if git_root != repository.git_root
            || cargo_workspace_root != repository.cargo_workspace_root
        {
            return Err(CodingAttemptError::new(
                "REPOSITORY_IDENTITY_MISMATCH",
                false,
            ));
        }
        let provisioner = self.build_provisioner(repository, process_liveness_scope)?;
        if provisioner.common_git_identity_marker() != common_git_identity {
            return Err(CodingAttemptError::new(
                "REPOSITORY_IDENTITY_MISMATCH",
                false,
            ));
        }
        Ok(provisioner)
    }
}

#[cfg(any(test, feature = "test-support"))]
struct FixedRepositoryRuntimeAttachments {
    sampler: Arc<dyn VolumeSampler>,
    prepare_slots: Arc<Semaphore>,
    bound: Mutex<HashMap<RepositoryId, BoundFixedRepositoryRuntime>>,
}

#[cfg(any(test, feature = "test-support"))]
struct BoundFixedRepositoryRuntime {
    git_root: CanonicalPath,
    cargo_workspace_root: CanonicalPath,
    common_git_identity: DirectoryIdentityMarker,
    attachment: AuthenticatedRepositoryRuntime,
}

#[cfg(any(test, feature = "test-support"))]
#[async_trait::async_trait]
impl RepositoryRuntimeAttachmentRegistry for FixedRepositoryRuntimeAttachments {
    async fn attach(
        &self,
        repository: &Repository,
        deadline: Instant,
    ) -> Result<AuthenticatedRepositoryRuntime, RepositoryRuntimeAttachmentError> {
        let permit = timeout_at(deadline, Arc::clone(&self.prepare_slots).acquire_owned())
            .await
            .map_err(|_| RepositoryRuntimeAttachmentError::DeadlineExceeded)?
            .map_err(|_| RepositoryRuntimeAttachmentError::Unavailable)?;
        let sampler = Arc::clone(&self.sampler);
        let repository_id = repository.id;
        let repository = repository.clone();
        let candidate = timeout_at(
            deadline,
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let common_git = Arc::new(
                    RootCapability::open(repository.git_root.as_path().join(".git"))
                        .map_err(|_| RepositoryRuntimeAttachmentError::IdentityUnavailable)?,
                );
                let common_git_identity = common_git
                    .identity_marker()
                    .map_err(|_| RepositoryRuntimeAttachmentError::IdentityUnavailable)?;
                let volume = sampler
                    .sample(&common_git)
                    .map_err(|_| RepositoryRuntimeAttachmentError::StorageUnavailable)?
                    .identity();
                let attachment = AuthenticatedRepositoryRuntime::new(
                    common_git_identity,
                    StorageProbeTarget::new(volume, common_git),
                );
                Ok::<_, RepositoryRuntimeAttachmentError>(BoundFixedRepositoryRuntime {
                    git_root: repository.git_root.clone(),
                    cargo_workspace_root: repository.cargo_workspace_root.clone(),
                    common_git_identity,
                    attachment,
                })
            }),
        )
        .await
        .map_err(|_| RepositoryRuntimeAttachmentError::DeadlineExceeded)?
        .map_err(|_| RepositoryRuntimeAttachmentError::Unavailable)??;
        if Instant::now() >= deadline {
            return Err(RepositoryRuntimeAttachmentError::DeadlineExceeded);
        }
        let mut bound = self
            .bound
            .try_lock()
            .map_err(|_| RepositoryRuntimeAttachmentError::Unavailable)?;
        match bound.entry(repository_id) {
            Entry::Vacant(entry) => {
                let attachment = candidate.attachment.clone();
                entry.insert(candidate);
                Ok(attachment)
            }
            Entry::Occupied(entry)
                if entry.get().git_root == candidate.git_root
                    && entry.get().cargo_workspace_root == candidate.cargo_workspace_root
                    && entry.get().common_git_identity == candidate.common_git_identity =>
            {
                Ok(entry.get().attachment.clone())
            }
            Entry::Occupied(entry) => Err(RepositoryRuntimeAttachmentError::IdentityConflict {
                expected: entry.get().common_git_identity,
                observed: candidate.common_git_identity,
            }),
        }
    }
}

#[async_trait::async_trait]
impl StartupRunnerFactory for ProductionStartupRunnerFactory {
    async fn validate_pre_database(
        &self,
        paths: &PlatformPaths,
    ) -> Result<Arc<dyn Any + Send + Sync>, StartupRunnerFactoryError> {
        let provider_config = load_provider_config(paths)
            .map_err(|error| StartupRunnerFactoryError::new(error.code()))?;
        let provider = ChatCompletionsClient::new(provider_config, ClientLimits::default())
            .map_err(|error| StartupRunnerFactoryError::new(error.code))?;
        Ok(Arc::new(provider))
    }

    async fn probe_delivery_git_pre_database(
        &self,
        paths: &PlatformPaths,
        process_liveness_scope: ProcessLivenessScope,
    ) -> Result<Arc<dyn Any + Send + Sync>, StartupRunnerFactoryError> {
        let toolchain = Arc::new(
            discover_toolchain(
                paths.runtime_dir.as_path(),
                process_liveness_scope.clone(),
                None,
                None,
            )
            .await
            .map_err(|error| StartupRunnerFactoryError::new(error.code()))?,
        );
        let retained_runtime = paths
            .retain_private_runtime_directory()
            .map_err(|_| StartupRunnerFactoryError::new("DELIVERY_GIT_PROBE_FAILED"))?;
        let private_runtime = Arc::new(
            ExecutionDirectory::from_retained_directory(
                paths.runtime_dir.as_path(),
                retained_runtime,
            )
            .map_err(|_| StartupRunnerFactoryError::new("DELIVERY_GIT_PROBE_FAILED"))?,
        );
        let delivery_git = probe_delivery_git_capabilities(
            toolchain.git(),
            private_runtime,
            process_liveness_scope,
            delivery_probe_process_limits(),
            DELIVERY_GIT_PROBE_TIMEOUT,
            CancellationToken::new(),
        )
        .await
        .map_err(|error| StartupRunnerFactoryError::new(error.code()))?;

        Ok(Arc::new(ProbedProductionRuntime {
            toolchain,
            delivery_git: Arc::new(delivery_git),
        }))
    }

    async fn prepare_before_actors(
        &self,
        context: &PreActorStartupRunnerContext,
    ) -> Result<Arc<dyn Any + Send + Sync>, StartupRunnerFactoryError> {
        let probed = context.probed::<ProbedProductionRuntime>()?;
        let toolchain = Arc::clone(&probed.toolchain);
        let repository_discovery = supervised_repository_discovery(&toolchain, context)?;
        let worktree_limits = WorktreeLimits::try_new(Duration::from_secs(60))
            .map_err(|error| StartupRunnerFactoryError::new(error.code()))?;
        let sampler: Arc<dyn VolumeSampler> = Arc::new(NativeVolumeSampler::new());
        let provisioners = Arc::new(ProductionWorktreeProvisioners {
            toolchain: Arc::clone(&toolchain),
            artifact_root: context.paths().data_dir.clone(),
            temporary_directory: context.paths().runtime_dir.clone(),
            process_limits: production_process_limits(),
            worktree_limits,
            instance_process_scope: context.process_liveness_scope().clone(),
            sampler: Arc::clone(&sampler),
            prepare_slots: Arc::new(Semaphore::new(RUNTIME_ATTACHMENT_PREPARE_CONCURRENCY)),
            bound: Mutex::new(HashMap::new()),
        });
        let repositories = context
            .store()
            .list_repositories()
            .await
            .map_err(|_| StartupRunnerFactoryError::new("ARTIFACT_RECONCILIATION_FAILED"))?;
        let repository_control = Arc::new(RepositoryControlCoordinator::new());
        let repository_identity_resolver = Arc::new(FilesystemRepositoryIdentityResolver);
        let repository_attachments = prepare_repository_runtime(
            context,
            &repositories,
            repository_control.as_ref(),
            provisioners.as_ref(),
        )
        .await?;
        let observed_provisioners = repositories
            .iter()
            .map(|repository| {
                provisioners
                    .startup_provisioner(repository.id)
                    .map(|provisioner| (repository.id, provisioner))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| StartupRunnerFactoryError::new("REPOSITORY_IDENTITY_UNAVAILABLE"))?;
        let observer = WorktreeArtifactObserver::new(observed_provisioners);
        let adapter = StartupDirectStoreArtifactAdapter::new(context.store().clone());
        let delivery_startup = PreparedDeliveryStartup::load(context.store())
            .await
            .map_err(|_| StartupRunnerFactoryError::new("DELIVERY_OWNERSHIP_INCONSISTENT"))?;
        crate::artifact_reconciliation::reconcile_startup_artifacts_grouped_with_ownership(
            &adapter,
            repository_control.as_ref(),
            repository_identity_resolver.as_ref(),
            &observer,
            NonZeroUsize::new(RUNTIME_ATTACHMENT_PREPARE_CONCURRENCY)
                .expect("startup reconciliation concurrency is nonzero"),
            delivery_startup.ownership_router(),
        )
        .await
        .map_err(|_| StartupRunnerFactoryError::new("ARTIFACT_RECONCILIATION_FAILED"))?;
        let scheduler_limits = SchedulerConcurrencyLimits::try_new(
            context.runtime_config().max_concurrent_tasks().get(),
            context
                .runtime_config()
                .max_concurrent_tasks_per_repository()
                .get(),
        )
        .map_err(|_| StartupRunnerFactoryError::new("SCHEDULER_LIMITS_INVALID"))?;
        Ok(Arc::new(PreparedProductionRunner {
            toolchain,
            delivery_git: Arc::clone(&probed.delivery_git),
            repository_discovery,
            provisioners,
            repositories,
            repository_attachments,
            repository_control,
            repository_identity_resolver,
            scheduler_limits,
            sampler,
            delivery_startup,
        }))
    }

    async fn create(
        &self,
        context: StartupRunnerContext,
    ) -> Result<StartupRunnerSelection, StartupRunnerFactoryError> {
        let provider = context.production_provider()?;
        let prepared = context.prepared::<PreparedProductionRunner>()?;
        let storage_signals = TaskManagerStorageSignals::new();
        let storage_monitor = build_storage_monitor(
            &context,
            Arc::clone(&prepared.sampler),
            storage_signals.clone(),
        )?;
        register_prepared_storage_scopes(
            &prepared.repositories,
            &prepared.repository_attachments,
            prepared.repository_control.as_ref(),
            &storage_monitor,
        )
        .await?;
        let repository_registrar = RepositoryRuntimeRegistrar::new(
            context.store().clone(),
            Arc::clone(&prepared.repository_control),
            prepared.provisioners.clone(),
            storage_monitor.clone(),
        );
        let launch_resources = TaskManagerLaunchResources::new_with_storage_signals(
            prepared.scheduler_limits,
            Arc::clone(&prepared.repository_control),
            context.instance_id(),
            context.process_liveness_scope().clone(),
            storage_monitor,
            storage_signals,
        )
        .with_scheduler_projection_limits(
            context.runtime_config().max_queued_tasks(),
            context.runtime_config().cargo_jobs_per_task(),
        );

        let runtimes = Arc::new(Project2RuntimeSessionFactory::project_2_defaults(
            (*prepared.toolchain).clone(),
            context.paths().runtime_dir.clone(),
            context.runtime_config().cargo_jobs_per_task(),
        ));
        let attempts = Arc::new(WorktreeCodingAgentAttemptFactory::new(
            prepared.provisioners.clone(),
            runtimes,
        ));
        let runner = Arc::new(CodingAgentRunner::new(
            CodingAgentPreparationControl::new(
                context.store().clone(),
                context.writer().clone(),
                Arc::clone(&prepared.repository_control),
                prepared.repository_identity_resolver.clone(),
            ),
            provider,
            attempts,
            context.wall_clock(),
            CodingAgentRunnerConfig::default(),
        ));
        #[cfg(feature = "test-support")]
        let delivery_runtime = {
            let target_boundary = context.test_delivery_target_boundary();
            let process_fault = context.test_delivery_process_fault();
            if target_boundary.is_some() || process_fault.is_some() {
                delivery::production_delivery_runtime_with_test_support(
                    context.store().clone(),
                    Arc::clone(&prepared.delivery_git),
                    Arc::clone(&prepared.provisioners),
                    Arc::clone(&prepared.repository_control),
                    target_boundary,
                    process_fault,
                )
            } else {
                delivery::production_delivery_runtime(
                    context.store().clone(),
                    Arc::clone(&prepared.delivery_git),
                    Arc::clone(&prepared.provisioners),
                    Arc::clone(&prepared.repository_control),
                )
            }
        };
        #[cfg(not(feature = "test-support"))]
        let delivery_runtime = delivery::production_delivery_runtime(
            context.store().clone(),
            Arc::clone(&prepared.delivery_git),
            Arc::clone(&prepared.provisioners),
            Arc::clone(&prepared.repository_control),
        );
        let delivery_startup = prepared
            .delivery_startup
            .clone()
            .with_runtime(delivery_runtime);
        Ok(StartupRunnerSelection::with_repository_runtime(
            runner,
            launch_resources,
            repository_registrar,
            prepared.repository_discovery.clone(),
            delivery_startup,
        ))
    }
}

fn production_process_limits() -> ProcessLimits {
    ProcessLimits::try_new(
        512 * 1024,
        256 * 1024,
        Duration::from_secs(10 * 60),
        Duration::from_secs(5),
    )
    .expect("constant production process limits are valid")
}

fn delivery_probe_process_limits() -> ProcessLimits {
    ProcessLimits::try_new(
        64 * 1024,
        64 * 1024,
        DELIVERY_GIT_PROBE_TIMEOUT,
        Duration::from_secs(5),
    )
    .expect("constant delivery Git probe process limits are valid")
}

fn repository_discovery_process_limits() -> ProcessLimits {
    ProcessLimits::try_new(
        16 * 1024,
        16 * 1024,
        Duration::from_millis(REPOSITORY_DISCOVERY_COMMAND_TIMEOUT_MILLIS),
        Duration::from_millis(REPOSITORY_DISCOVERY_CLEANUP_TIMEOUT_MILLIS),
    )
    .expect("constant repository discovery process limits are valid")
}

#[cfg(any(test, feature = "test-support"))]
fn process_test_repository_discovery_process_limits() -> ProcessLimits {
    ProcessLimits::try_new(
        16 * 1024,
        16 * 1024,
        Duration::from_millis(PROCESS_TEST_REPOSITORY_DISCOVERY_COMMAND_TIMEOUT_MILLIS),
        Duration::from_millis(REPOSITORY_DISCOVERY_CLEANUP_TIMEOUT_MILLIS),
    )
    .expect("constant process-test repository discovery limits are valid")
}

fn supervised_repository_discovery(
    toolchain: &ToolchainPaths,
    context: &PreActorStartupRunnerContext,
) -> Result<RepositoryDiscovery, StartupRunnerFactoryError> {
    supervised_repository_discovery_with_limits(
        toolchain,
        context,
        repository_discovery_process_limits(),
    )
}

fn supervised_repository_discovery_with_limits(
    toolchain: &ToolchainPaths,
    context: &PreActorStartupRunnerContext,
    process_limits: ProcessLimits,
) -> Result<RepositoryDiscovery, StartupRunnerFactoryError> {
    RepositoryDiscoveryCommands::from_trusted_toolchain(
        toolchain,
        context.paths().runtime_dir.as_path(),
        context.process_liveness_scope().clone(),
        process_limits,
    )
    .map(RepositoryDiscovery::from_supervised_commands)
    .map_err(|_| StartupRunnerFactoryError::new("REPOSITORY_DISCOVERY_UNAVAILABLE"))
}

#[cfg(any(test, feature = "test-support"))]
impl FixedStartupRunnerFactory {
    pub fn new(runner: Arc<dyn TaskRunner>, concurrency: NonZeroU32) -> Self {
        Self::new_with_volume_sampler(runner, concurrency, Arc::new(NativeVolumeSampler::new()))
    }

    pub fn new_with_volume_sampler(
        runner: Arc<dyn TaskRunner>,
        concurrency: NonZeroU32,
        sampler: Arc<dyn VolumeSampler>,
    ) -> Self {
        Self {
            runner,
            scheduler_limits: FixedSchedulerLimitsMode::Fixed(concurrency),
            sampler,
            repository_discovery: FixedRepositoryDiscoveryMode::Disabled,
        }
    }

    pub(crate) fn new_for_process_test(
        runner: Arc<dyn TaskRunner>,
        sampler: Arc<dyn VolumeSampler>,
    ) -> Self {
        Self {
            runner,
            scheduler_limits: FixedSchedulerLimitsMode::RuntimeConfig,
            sampler,
            // Process-backed fixtures run beside other spawn-heavy tests. Give
            // them test-only command latency allowance while retaining the
            // caller's five-second end-to-end deadline and cleanup reserve.
            // Unlike production, these fixtures do not promise the separate
            // two-second post-discovery reserve.
            repository_discovery: FixedRepositoryDiscoveryMode::Supervised(
                process_test_repository_discovery_process_limits(),
            ),
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
#[async_trait::async_trait]
impl StartupRunnerFactory for FixedStartupRunnerFactory {
    async fn prepare_before_actors(
        &self,
        context: &PreActorStartupRunnerContext,
    ) -> Result<Arc<dyn Any + Send + Sync>, StartupRunnerFactoryError> {
        let repositories = context
            .store()
            .list_repositories()
            .await
            .map_err(|_| StartupRunnerFactoryError::new("REPOSITORY_IDENTITY_UNAVAILABLE"))?;
        let delivery_startup = PreparedDeliveryStartup::load(context.store())
            .await
            .map_err(|_| StartupRunnerFactoryError::new("DELIVERY_OWNERSHIP_INCONSISTENT"))?;
        let repository_control = Arc::new(RepositoryControlCoordinator::new());
        let sampler = Arc::clone(&self.sampler);
        let attachments = Arc::new(FixedRepositoryRuntimeAttachments {
            sampler: Arc::clone(&sampler),
            prepare_slots: Arc::new(Semaphore::new(RUNTIME_ATTACHMENT_PREPARE_CONCURRENCY)),
            bound: Mutex::new(HashMap::new()),
        });
        let repository_attachments = prepare_repository_runtime(
            context,
            &repositories,
            repository_control.as_ref(),
            attachments.as_ref(),
        )
        .await?;
        let scheduler_limits = match self.scheduler_limits {
            FixedSchedulerLimitsMode::Fixed(concurrency) => {
                SchedulerConcurrencyLimits::try_new(concurrency.get(), concurrency.get())
            }
            FixedSchedulerLimitsMode::RuntimeConfig => SchedulerConcurrencyLimits::try_new(
                context.runtime_config().max_concurrent_tasks().get(),
                context
                    .runtime_config()
                    .max_concurrent_tasks_per_repository()
                    .get(),
            ),
        }
        .map_err(|_| StartupRunnerFactoryError::new("SCHEDULER_LIMITS_INVALID"))?;
        let repository_discovery = match self.repository_discovery {
            FixedRepositoryDiscoveryMode::Disabled => {
                RepositoryDiscovery::new_without_commands_for_test(
                    context.paths().runtime_dir.clone(),
                )
            }
            FixedRepositoryDiscoveryMode::Supervised(process_limits) => {
                let toolchain = discover_toolchain(
                    context.paths().runtime_dir.as_path(),
                    context.process_liveness_scope().clone(),
                    None,
                    None,
                )
                .await
                .map_err(|error| StartupRunnerFactoryError::new(error.code()))?;
                supervised_repository_discovery_with_limits(&toolchain, context, process_limits)?
            }
        };
        Ok(Arc::new(PreparedFixedRunner {
            repository_discovery,
            attachments,
            repositories,
            repository_attachments,
            repository_control,
            sampler,
            scheduler_limits,
            delivery_startup,
        }))
    }

    async fn create(
        &self,
        context: StartupRunnerContext,
    ) -> Result<StartupRunnerSelection, StartupRunnerFactoryError> {
        let prepared = context.prepared::<PreparedFixedRunner>()?;
        let storage_signals = TaskManagerStorageSignals::new();
        let storage_monitor = build_storage_monitor(
            &context,
            Arc::clone(&prepared.sampler),
            storage_signals.clone(),
        )?;
        register_prepared_storage_scopes(
            &prepared.repositories,
            &prepared.repository_attachments,
            prepared.repository_control.as_ref(),
            &storage_monitor,
        )
        .await?;
        let repository_registrar = RepositoryRuntimeRegistrar::new(
            context.store().clone(),
            Arc::clone(&prepared.repository_control),
            prepared.attachments.clone(),
            storage_monitor.clone(),
        );
        Ok(StartupRunnerSelection::with_repository_runtime(
            Arc::clone(&self.runner),
            TaskManagerLaunchResources::new_with_storage_signals(
                prepared.scheduler_limits,
                Arc::clone(&prepared.repository_control),
                context.instance_id(),
                context.process_liveness_scope().clone(),
                storage_monitor,
                storage_signals,
            )
            .with_scheduler_projection_limits(
                context.runtime_config().max_queued_tasks(),
                context.runtime_config().cargo_jobs_per_task(),
            ),
            repository_registrar,
            prepared.repository_discovery.clone(),
            prepared.delivery_startup.clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Write as _;
    use std::num::NonZeroU32;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    use coding_agent_domain::{CanonicalPath, NewRepository};
    use coding_agent_runtime::{
        NativeVolumeSampler, ProcessLivenessDirectory, ProcessLivenessScope, RootCapability,
        VolumeSample, VolumeSampleError, VolumeSampler,
    };
    use coding_agent_store::{RegisterRepositoryOutcome, Store};
    use tokio::sync::{Notify, Semaphore};
    use tokio::time::{Instant, timeout};
    use uuid::Uuid;

    use super::{
        FixedRepositoryRuntimeAttachments, FixedStartupRunnerFactory, PreActorStartupRunnerContext,
        PreparedFixedRunner, ProductionStartupRunnerFactory,
        RUNTIME_ATTACHMENT_PREPARE_CONCURRENCY, RepositoryRuntimeAttachmentError,
        RepositoryRuntimeAttachmentRegistry, StartupRunnerFactory, StartupRunnerFactoryError,
        StartupRunnerSelection, ValidatedStartupInputs,
    };
    use crate::{
        EventDispatcherHandle, FakeTaskRunner, PlatformPaths, PrivateFile,
        RepositoryControlPoisonReason, RepositoryControlState, RepositoryDiscoveryError,
        StoreWriterHandle, SystemWallClock, load_runtime_config,
    };

    async fn prepare_fixed_factory(
        factory: &FixedStartupRunnerFactory,
    ) -> (
        tempfile::TempDir,
        Arc<PreparedFixedRunner>,
        ProcessLivenessScope,
    ) {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let paths = PlatformPaths::new(root.join("data"), root.join("runtime"));
        paths.prepare().unwrap();
        let store = Store::open(&paths.database_path).await.unwrap();
        store.migrate().await.unwrap();
        let runtime_config = load_runtime_config(&paths).unwrap();
        let instance_id = Uuid::new_v4();
        let process_liveness_scope = ProcessLivenessDirectory::open(&paths.runtime_dir)
            .unwrap()
            .instance_scope(*instance_id.as_bytes())
            .unwrap();
        let context = PreActorStartupRunnerContext::new(
            paths,
            store,
            Arc::new(SystemWallClock),
            ValidatedStartupInputs::new(runtime_config, Arc::new(())),
            instance_id,
            process_liveness_scope.clone(),
        );
        let prepared = factory
            .prepare_before_actors(&context)
            .await
            .unwrap()
            .downcast::<PreparedFixedRunner>()
            .unwrap();
        (temporary, prepared, process_liveness_scope)
    }

    fn create_discovery_workspace(root: &Path) -> PathBuf {
        let workspace = root.join("discovery-workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(workspace.join("Cargo.toml"), b"[workspace]\nmembers = []\n").unwrap();
        workspace.canonicalize().unwrap()
    }

    fn create_discovery_repository(root: &Path) -> PathBuf {
        let repository = create_discovery_workspace(root);
        assert!(
            Command::new("git")
                .args(["init", "--quiet"])
                .arg(&repository)
                .status()
                .unwrap()
                .success()
        );
        repository
    }

    #[tokio::test]
    async fn fixed_factory_default_repository_discovery_remains_fail_closed() {
        let factory = FixedStartupRunnerFactory::new(
            Arc::new(FakeTaskRunner::default()),
            NonZeroU32::new(2).unwrap(),
        );
        let (temporary, prepared, process_liveness_scope) = prepare_fixed_factory(&factory).await;
        let repository = create_discovery_workspace(temporary.path());

        let error = prepared
            .repository_discovery
            .discover(
                &repository,
                Instant::now() + crate::repository_service::DEFAULT_APPLICATION_WRITE_BUDGET,
            )
            .await
            .expect_err("ordinary fixed tests never run repository commands");

        assert_eq!(error, RepositoryDiscoveryError::CommandFailed);
        assert_eq!(process_liveness_scope.active_tree_count(), 0);
    }

    #[tokio::test]
    async fn process_test_fixed_factory_uses_supervised_repository_discovery() {
        let factory = FixedStartupRunnerFactory::new_for_process_test(
            Arc::new(FakeTaskRunner::default()),
            Arc::new(NativeVolumeSampler::new()),
        );
        let (temporary, prepared, process_liveness_scope) = prepare_fixed_factory(&factory).await;
        let repository = create_discovery_repository(temporary.path());

        let discovered = prepared
            .repository_discovery
            .discover(
                &repository,
                Instant::now() + crate::repository_service::DEFAULT_APPLICATION_WRITE_BUDGET,
            )
            .await
            .expect("process tests use pinned and supervised Git/Cargo discovery");

        assert_eq!(discovered.git_root, repository);
        assert_eq!(discovered.cargo_workspace_root, repository);
        assert_eq!(prepared.scheduler_limits.global().get(), 2);
        assert_eq!(prepared.scheduler_limits.per_repository().get(), 2);
        assert_eq!(process_liveness_scope.active_tree_count(), 0);
    }

    struct BlockingVolumeSampler {
        entered: Arc<Notify>,
        release: Arc<(Mutex<bool>, Condvar)>,
    }

    impl BlockingVolumeSampler {
        fn new() -> Self {
            Self {
                entered: Arc::new(Notify::new()),
                release: Arc::new((Mutex::new(false), Condvar::new())),
            }
        }

        fn release(&self) {
            let (released, wake) = &*self.release;
            *released.lock().unwrap() = true;
            wake.notify_all();
        }
    }

    impl VolumeSampler for BlockingVolumeSampler {
        fn sample(&self, root: &RootCapability) -> Result<VolumeSample, VolumeSampleError> {
            self.entered.notify_one();
            let (released, wake) = &*self.release;
            let released = released.lock().unwrap();
            let _released = wake.wait_while(released, |released| !*released).unwrap();
            NativeVolumeSampler::new().sample(root)
        }
    }

    #[test]
    fn startup_errors_reject_untrusted_codes_without_echoing_them() {
        let error = StartupRunnerFactoryError::new("bad code with provider-secret");

        assert_eq!(error.code(), "RUNNER_STARTUP_FAILED");
        assert!(!format!("{error:?}").contains("provider-secret"));
        assert!(!format!("{error}").contains("provider-secret"));
    }

    #[test]
    fn runner_and_configured_concurrency_are_one_selection() {
        let concurrency = NonZeroU32::new(3).unwrap();
        let selection = StartupRunnerSelection::new(
            Arc::new(FakeTaskRunner::default()),
            crate::task_manager::test_task_manager_launch_resources(3, 2),
        );

        assert_eq!(selection.concurrency(), concurrency);
        let _ = selection.runner();
    }

    #[tokio::test]
    async fn timed_out_attachment_prepare_cannot_commit_late_and_retry_converges() {
        let temporary = tempfile::tempdir().unwrap();
        let repository_root = temporary.path().join("repository");
        std::fs::create_dir_all(repository_root.join(".git")).unwrap();
        let repository_root = repository_root.canonicalize().unwrap();
        let store = Store::open(temporary.path().join("state.sqlite"))
            .await
            .unwrap();
        store.migrate().await.unwrap();
        let repository = match store
            .register_repository(NewRepository {
                selected_path: CanonicalPath::try_from_canonical(repository_root.clone()).unwrap(),
                display_name: "repository".to_owned(),
                git_root: CanonicalPath::try_from_canonical(repository_root.clone()).unwrap(),
                cargo_workspace_root: CanonicalPath::try_from_canonical(repository_root).unwrap(),
            })
            .await
            .unwrap()
        {
            RegisterRepositoryOutcome::Created(repository) => repository,
            RegisterRepositoryOutcome::Existing(_) => panic!("fixture repository must be new"),
        };
        let sampler = Arc::new(BlockingVolumeSampler::new());
        let attachments = Arc::new(FixedRepositoryRuntimeAttachments {
            sampler: sampler.clone(),
            prepare_slots: Arc::new(Semaphore::new(RUNTIME_ATTACHMENT_PREPARE_CONCURRENCY)),
            bound: Mutex::new(HashMap::new()),
        });
        let attempt = tokio::spawn({
            let attachments = Arc::clone(&attachments);
            let repository = repository.clone();
            async move {
                attachments
                    .attach(&repository, Instant::now() + Duration::from_millis(500))
                    .await
            }
        });
        sampler.entered.notified().await;

        assert!(
            matches!(
                timeout(Duration::from_secs(2), attempt)
                    .await
                    .expect("attachment returns at its deadline")
                    .unwrap(),
                Err(RepositoryRuntimeAttachmentError::DeadlineExceeded)
            ),
            "the blocked prepare returns only the path-free deadline error"
        );
        sampler.release();
        let all_prepare_slots = timeout(
            Duration::from_secs(2),
            Arc::clone(&attachments.prepare_slots)
                .acquire_many_owned(RUNTIME_ATTACHMENT_PREPARE_CONCURRENCY as u32),
        )
        .await
        .expect("late prepare operation exits")
        .unwrap();
        assert!(
            attachments.bound.lock().unwrap().is_empty(),
            "a candidate returned after timeout is never committed"
        );
        drop(all_prepare_slots);

        attachments
            .attach(&repository, Instant::now() + Duration::from_secs(2))
            .await
            .expect("an explicit Existing retry prepares and commits once");
        assert_eq!(attachments.bound.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn fixed_factory_dynamic_registrar_converges_existing_rows_without_rebinding() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let paths = PlatformPaths::new(root.join("data"), root.join("runtime"));
        paths.prepare().unwrap();
        let store = Store::open(&paths.database_path).await.unwrap();
        store.migrate().await.unwrap();
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
            .await
            .unwrap();
        let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher), 16);
        let runtime_config = load_runtime_config(&paths).unwrap();
        let instance_id = Uuid::new_v4();
        let process_liveness_scope = ProcessLivenessDirectory::open(&paths.runtime_dir)
            .unwrap()
            .instance_scope(*instance_id.as_bytes())
            .unwrap();
        let factory = FixedStartupRunnerFactory::new(
            Arc::new(FakeTaskRunner::default()),
            NonZeroU32::new(2).unwrap(),
        );
        let context = PreActorStartupRunnerContext::new(
            paths,
            store.clone(),
            Arc::new(SystemWallClock),
            ValidatedStartupInputs::new(runtime_config, Arc::new(())),
            instance_id,
            process_liveness_scope,
        );
        let prepared = factory.prepare_before_actors(&context).await.unwrap();
        let selection = factory
            .create(context.into_live(writer, prepared))
            .await
            .unwrap();
        let registrar = selection
            .repository_registrar()
            .expect("fixed startup installs a dynamic registrar");

        let first_root = temporary.path().join("dynamic-first");
        std::fs::create_dir_all(first_root.join(".git")).unwrap();
        let first_root = first_root.canonicalize().unwrap();
        let first_input = NewRepository {
            selected_path: CanonicalPath::try_from_canonical(first_root.clone()).unwrap(),
            display_name: "dynamic-first".to_owned(),
            git_root: CanonicalPath::try_from_canonical(first_root.clone()).unwrap(),
            cargo_workspace_root: CanonicalPath::try_from_canonical(first_root.clone()).unwrap(),
        };
        let first = match store
            .register_repository(first_input)
            .await
            .expect("durably register first repository")
        {
            RegisterRepositoryOutcome::Created(repository) => repository,
            RegisterRepositoryOutcome::Existing(_) => panic!("first row must be new"),
        };
        registrar
            .attach(&first, Instant::now() + Duration::from_secs(2))
            .await
            .expect("attach first repository runtime");
        let original_key = registrar
            .repository_control()
            .coordination_key(first.id)
            .unwrap();
        registrar
            .attach(&first, Instant::now() + Duration::from_secs(2))
            .await
            .expect("an exact live retry is idempotent");

        std::fs::rename(first_root.join(".git"), first_root.join(".git-retained")).unwrap();
        std::fs::create_dir(first_root.join(".git")).unwrap();
        assert!(
            registrar
                .attach(&first, Instant::now() + Duration::from_secs(2))
                .await
                .is_err(),
            "an Existing retry revalidates and rejects a replacement object"
        );
        assert_eq!(
            registrar.repository_control().coordination_key(first.id),
            Ok(original_key),
            "a path replacement never overwrites the installed identity"
        );
        assert_eq!(
            registrar.repository_control().poison_reason(first.id),
            Ok(Some(RepositoryControlPoisonReason::IdentityDrift))
        );
        assert_eq!(
            registrar.repository_control().control_state(first.id),
            Ok(RepositoryControlState::Poisoned),
            "identity drift makes the original coordination group inadmissible"
        );

        let retry_root = temporary.path().join("dynamic-retry");
        std::fs::create_dir_all(&retry_root).unwrap();
        let retry_root = retry_root.canonicalize().unwrap();
        let retry_input = NewRepository {
            selected_path: CanonicalPath::try_from_canonical(retry_root.clone()).unwrap(),
            display_name: "dynamic-retry".to_owned(),
            git_root: CanonicalPath::try_from_canonical(retry_root.clone()).unwrap(),
            cargo_workspace_root: CanonicalPath::try_from_canonical(retry_root.clone()).unwrap(),
        };
        let durable = match store
            .register_repository(retry_input.clone())
            .await
            .expect("commit retry fixture before runtime attachment")
        {
            RegisterRepositoryOutcome::Created(repository) => repository,
            RegisterRepositoryOutcome::Existing(_) => panic!("retry fixture row must be new"),
        };
        assert!(
            registrar
                .attach(&durable, Instant::now() + Duration::from_secs(2))
                .await
                .is_err(),
            "missing common Git capability fails after the durable row exists"
        );
        assert!(
            store
                .list_repositories()
                .await
                .unwrap()
                .iter()
                .any(|repository| repository.id == durable.id),
            "runtime attach failure never rolls back the durable row"
        );

        std::fs::create_dir(retry_root.join(".git")).unwrap();
        let existing = match store
            .register_repository(retry_input)
            .await
            .expect("explicit retry resolves the durable row")
        {
            RegisterRepositoryOutcome::Existing(repository) => repository,
            RegisterRepositoryOutcome::Created(_) => panic!("retry must reuse the durable row"),
        };
        registrar
            .attach(&existing, Instant::now() + Duration::from_secs(2))
            .await
            .expect("Existing retry converges all runtime registries");
        assert!(
            registrar
                .repository_control()
                .coordination_key(existing.id)
                .is_ok()
        );

        let shared_root = temporary.path().join("shared-seed");
        let shared_workspace_a = shared_root.join("workspace-a");
        let shared_workspace_b = shared_root.join("workspace-b");
        std::fs::create_dir_all(shared_root.join(".git")).unwrap();
        std::fs::create_dir_all(&shared_workspace_a).unwrap();
        std::fs::create_dir_all(&shared_workspace_b).unwrap();
        let shared_root = shared_root.canonicalize().unwrap();
        let shared_workspace_a = shared_workspace_a.canonicalize().unwrap();
        let shared_workspace_b = shared_workspace_b.canonicalize().unwrap();
        let register_shared = |display_name: &str, workspace: std::path::PathBuf| NewRepository {
            selected_path: CanonicalPath::try_from_canonical(workspace.clone()).unwrap(),
            display_name: display_name.to_owned(),
            git_root: CanonicalPath::try_from_canonical(shared_root.clone()).unwrap(),
            cargo_workspace_root: CanonicalPath::try_from_canonical(workspace).unwrap(),
        };
        let established = match store
            .register_repository(register_shared("shared-a", shared_workspace_a))
            .await
            .unwrap()
        {
            RegisterRepositoryOutcome::Created(repository) => repository,
            RegisterRepositoryOutcome::Existing(_) => panic!("shared A fixture must be new"),
        };
        let unavailable_alias = match store
            .register_repository(register_shared("shared-b", shared_workspace_b))
            .await
            .unwrap()
        {
            RegisterRepositoryOutcome::Created(repository) => repository,
            RegisterRepositoryOutcome::Existing(_) => panic!("shared B fixture must be new"),
        };
        registrar
            .attach(&established, Instant::now() + Duration::from_secs(2))
            .await
            .expect("establish shared durable seed");
        std::fs::rename(shared_root.join(".git"), shared_root.join(".git-retained")).unwrap();

        assert!(
            registrar
                .attach(&unavailable_alias, Instant::now() + Duration::from_secs(2))
                .await
                .is_err(),
            "a newly durable alias observes the unavailable shared seed"
        );
        assert_eq!(
            registrar
                .repository_control()
                .coordination_key(unavailable_alias.id),
            Err(crate::RepositoryControlError::UnknownRepository),
            "identity-unavailable evidence never creates the new alias"
        );
        assert_eq!(
            registrar.repository_control().poison_reason(established.id),
            Ok(Some(RepositoryControlPoisonReason::IdentityUnavailable)),
            "the already registered shared-seed group is poisoned"
        );
    }

    #[tokio::test]
    async fn production_factory_builds_a_noncontacted_https_runner_with_configured_concurrency() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let paths = PlatformPaths::new(root.join("data"), root.join("runtime"));
        paths.prepare().unwrap();
        let mut provider = PrivateFile::create_new(paths.data_dir.join("provider.json")).unwrap();
        provider
            .write_all(
                br#"{"base_url":"https://127.0.0.1:9/","model":"offline-unit","api_key":"offline-unit-secret","tool_choice_compatibility":"required_as_required"}"#,
            )
            .unwrap();
        provider.as_file().sync_all().unwrap();

        let store = Store::open(&paths.database_path).await.unwrap();
        store.migrate().await.unwrap();
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
            .await
            .unwrap();
        let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher), 16);
        let runtime_config = load_runtime_config(&paths).unwrap();
        let instance_id = Uuid::new_v4();
        let process_liveness_scope = ProcessLivenessDirectory::open(&paths.runtime_dir)
            .unwrap()
            .instance_scope(*instance_id.as_bytes())
            .unwrap();
        let factory = ProductionStartupRunnerFactory;
        let runner_inputs = factory.validate_pre_database(&paths).await.unwrap();
        let probed_runner_inputs = factory
            .probe_delivery_git_pre_database(&paths, process_liveness_scope.clone())
            .await
            .unwrap();
        let validated_inputs = ValidatedStartupInputs::new(runtime_config, runner_inputs)
            .with_probed_runner_inputs(probed_runner_inputs);
        let context = PreActorStartupRunnerContext::new(
            paths,
            store.clone(),
            Arc::new(SystemWallClock),
            validated_inputs,
            instance_id,
            process_liveness_scope,
        );
        let prepared = factory.prepare_before_actors(&context).await.unwrap();
        let selection = factory
            .create(context.into_live(writer, prepared))
            .await
            .unwrap();

        assert_eq!(selection.concurrency().get(), 2);
        assert_eq!(
            selection.launch_resources().limits().per_repository().get(),
            2
        );
        let supervised_root = temporary.path().join("supervised-discovery");
        std::fs::create_dir_all(&supervised_root).unwrap();
        std::fs::write(
            supervised_root.join("Cargo.toml"),
            b"[workspace]\nmembers = []\n",
        )
        .unwrap();
        assert!(
            Command::new("git")
                .args(["init", "--quiet"])
                .arg(&supervised_root)
                .status()
                .unwrap()
                .success(),
            "initialize the offline discovery fixture"
        );
        let supervised_root = supervised_root.canonicalize().unwrap();
        let discovered = selection
            .repository_discovery()
            .expect("production startup installs supervised discovery")
            .discover(
                &supervised_root,
                Instant::now() + crate::repository_service::DEFAULT_APPLICATION_WRITE_BUDGET,
            )
            .await
            .expect("pinned supervised Git/Cargo discovery succeeds");
        assert_eq!(discovered.git_root, supervised_root);
        assert_eq!(discovered.cargo_workspace_root, supervised_root);

        let shared_git_root = temporary.path().join("production-shared-git");
        let workspace_a = shared_git_root.join("workspace-a");
        let workspace_b = shared_git_root.join("workspace-b");
        std::fs::create_dir_all(shared_git_root.join(".git")).unwrap();
        std::fs::create_dir_all(&workspace_a).unwrap();
        std::fs::create_dir_all(&workspace_b).unwrap();
        let shared_git_root = shared_git_root.canonicalize().unwrap();
        let workspace_a = workspace_a.canonicalize().unwrap();
        let workspace_b = workspace_b.canonicalize().unwrap();
        let register_alias = |display_name: &str, workspace: std::path::PathBuf| NewRepository {
            selected_path: CanonicalPath::try_from_canonical(workspace.clone()).unwrap(),
            display_name: display_name.to_owned(),
            git_root: CanonicalPath::try_from_canonical(shared_git_root.clone()).unwrap(),
            cargo_workspace_root: CanonicalPath::try_from_canonical(workspace).unwrap(),
        };
        let alias_a = match store
            .register_repository(register_alias("alias-a", workspace_a))
            .await
            .unwrap()
        {
            RegisterRepositoryOutcome::Created(repository) => repository,
            RegisterRepositoryOutcome::Existing(_) => panic!("alias A fixture must be new"),
        };
        let alias_b = match store
            .register_repository(register_alias("alias-b", workspace_b))
            .await
            .unwrap()
        {
            RegisterRepositoryOutcome::Created(repository) => repository,
            RegisterRepositoryOutcome::Existing(_) => panic!("alias B fixture must be new"),
        };
        let registrar = selection
            .repository_registrar()
            .expect("production startup installs the dynamic registrar");
        for repository in [&alias_a, &alias_b] {
            registrar
                .attach(repository, Instant::now() + Duration::from_secs(2))
                .await
                .expect("attach production alias");
        }
        assert_eq!(
            registrar
                .repository_control()
                .coordination_key(alias_a.id)
                .unwrap(),
            registrar
                .repository_control()
                .coordination_key(alias_b.id)
                .unwrap(),
            "common-Git aliases share one real coordination group"
        );
        let launch_monitor = selection
            .launch_resources()
            .storage_monitor_for_test()
            .expect("production admission is monitor-backed");
        let alias_a_admission = launch_monitor
            .refresh_for_repository_admission(0, alias_a.id)
            .await
            .expect("run repository A's real admission refresh");
        assert!(
            alias_a_admission.repository_state(alias_a.id).is_some(),
            "the launch monitor evaluates the dynamically attached first alias"
        );
        let alias_b_admission = launch_monitor
            .refresh_for_repository_admission(0, alias_b.id)
            .await
            .expect("run repository B's real admission refresh");
        assert!(
            alias_b_admission.repository_state(alias_a.id).is_some()
                && alias_b_admission.repository_state(alias_b.id).is_some(),
            "the registrar and candidate admission share the same dynamic monitor"
        );
        let workspace_c = shared_git_root.join("workspace-c");
        std::fs::create_dir_all(&workspace_c).unwrap();
        let workspace_c = workspace_c.canonicalize().unwrap();
        let unavailable_alias = match store
            .register_repository(register_alias("alias-c", workspace_c))
            .await
            .unwrap()
        {
            RegisterRepositoryOutcome::Created(repository) => repository,
            RegisterRepositoryOutcome::Existing(_) => panic!("alias C fixture must be new"),
        };
        std::fs::rename(
            shared_git_root.join(".git"),
            shared_git_root.join(".git-retained"),
        )
        .unwrap();
        assert!(
            registrar
                .attach(&unavailable_alias, Instant::now() + Duration::from_secs(2))
                .await
                .is_err(),
            "production attachment observes the unavailable shared seed"
        );
        assert_eq!(
            registrar
                .repository_control()
                .coordination_key(unavailable_alias.id),
            Err(crate::RepositoryControlError::UnknownRepository),
            "production never creates an alias from unavailable identity evidence"
        );
        assert_eq!(
            registrar.repository_control().poison_reason(alias_a.id),
            Ok(Some(RepositoryControlPoisonReason::IdentityUnavailable)),
            "the production shared-seed group fails closed"
        );
        let _ = selection.runner();
    }
}
