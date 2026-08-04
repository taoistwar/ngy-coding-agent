use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use coding_agent_core::{
    AgentRuntime, ContextRedactor, DiffEvent, DiffFileStatus as CoreDiffFileStatus,
    DurableCheckpointAck, DurableEventAck, DurableRoleEvent, FinalizationGuard,
    FinalizationGuardError, MultiRoleInput, MultiRoleOrchestrator, MultiRoleOutcome,
    PreparedModelProvider, RepositoryCheckCatalog, RequiredCheckLedger, Role, RoleEngineFactory,
    RoleEvent, RoleEventSink, RuntimeError, TerminalSnapshot, WorkspaceCheckpoint,
    WorkspaceFingerprint, project_test_snapshot, project_unverified_test_snapshot,
};
use coding_agent_domain::{
    ActivityActor, ActivityEntry, ActivityLevel, CanonicalPath, DiffFile, DiffFileStatus,
    DiffSnapshot, EventId, Repository, RequiredCheckSelector, TaskFailure, TestSnapshot,
    UtcTimestamp,
};
use coding_agent_provider::ChatCompletionsClient;
use coding_agent_runtime::{
    ATTEMPT_IDENTITY_MISMATCH, ProcessLivenessScope, ProvisionedWorktree, RoleScopedEngineFactory,
    RuntimeSession, RuntimeSessionLimits, ToolchainPaths, WorktreeArtifactState, WorktreeError,
    WorktreeIdentity, WorktreeProvisioner, WorktreeReservation,
};
use coding_agent_store::{
    AttemptArtifactIdentity, AttemptArtifactState, ReserveAttemptArtifact, Store,
};
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;

use crate::{
    ArtifactMutationDisposition, LiveStoreWriterArtifactAdapter, RepositoryControlCoordinator,
    RepositoryControlError, RepositoryControlLease, RepositoryControlPoisonReason,
    RepositoryIdentityResolver, RunContext, RunnerEvent, RunnerEventError, RunnerEventSink,
    RunnerOutcome, StoreWriterHandle, TaskRunner, WallClock,
};

const DEFAULT_ARTIFACT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_DIFF_DEBOUNCE: Duration = Duration::from_millis(100);
const MAX_ARTIFACT_WRITE_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_DIFF_DEBOUNCE: Duration = Duration::from_secs(10);
const ARTIFACT_STORE_FAILED: &str = "ARTIFACT_STORE_FAILED";
const REPOSITORY_CONTROL_BUSY: &str = "REPOSITORY_CONTROL_BUSY";
const EVENT_SINK_REJECTED: &str = "CODING_RUNNER_EVENT_REJECTED";
const WORKTREE_STATE_INCONSISTENT: &str = "WORKTREE_STATE_INCONSISTENT";
const WORKTREE_RESERVATION_ABANDONED: &str = "WORKTREE_RESERVATION_ABANDONED";
const COMMAND_CANCELLED: &str = "COMMAND_CANCELLED";
const PROCESS_TREE_CLEANUP_FAILED: &str = "PROCESS_TREE_CLEANUP_FAILED";
const CODING_AGENT_FAILED: &str = "CODING_AGENT_FAILED";
const EVENT_SINK_MESSAGE: &str = "task progress could not be persisted";
const ISOLATION_CONTEXT: &str =
    "Work only inside the isolated Rust repository worktree supplied by the runtime.";
const MAX_REPOSITORY_CONTEXT_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodingAgentRunnerConfig {
    artifact_write_timeout: Duration,
    diff_debounce: Duration,
}

impl CodingAgentRunnerConfig {
    pub fn try_new(
        artifact_write_timeout: Duration,
        diff_debounce: Duration,
    ) -> Result<Self, CodingAgentRunnerConfigError> {
        if artifact_write_timeout.is_zero()
            || artifact_write_timeout > MAX_ARTIFACT_WRITE_TIMEOUT
            || diff_debounce.is_zero()
            || diff_debounce > MAX_DIFF_DEBOUNCE
        {
            return Err(CodingAgentRunnerConfigError::InvalidDuration);
        }
        Ok(Self {
            artifact_write_timeout,
            diff_debounce,
        })
    }

    pub const fn artifact_write_timeout(self) -> Duration {
        self.artifact_write_timeout
    }

    pub const fn diff_debounce(self) -> Duration {
        self.diff_debounce
    }
}

impl Default for CodingAgentRunnerConfig {
    fn default() -> Self {
        Self {
            artifact_write_timeout: DEFAULT_ARTIFACT_WRITE_TIMEOUT,
            diff_debounce: DEFAULT_DIFF_DEBOUNCE,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CodingAgentRunnerConfigError {
    #[error("coding runner durations are outside their safe bounds")]
    InvalidDuration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptReservation {
    base_commit: String,
    branch_name: String,
    worktree_path: CanonicalPath,
}

impl AttemptReservation {
    pub fn new(
        base_commit: impl Into<String>,
        branch_name: impl Into<String>,
        worktree_path: CanonicalPath,
    ) -> Self {
        Self {
            base_commit: base_commit.into(),
            branch_name: branch_name.into(),
            worktree_path,
        }
    }

    pub fn base_commit(&self) -> &str {
        &self.base_commit
    }

    pub fn branch_name(&self) -> &str {
        &self.branch_name
    }

    pub const fn worktree_path(&self) -> &CanonicalPath {
        &self.worktree_path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodingAttemptError {
    code: String,
    retryable: bool,
    repository_poison_required: bool,
}

impl CodingAttemptError {
    pub fn new(code: impl Into<String>, retryable: bool) -> Self {
        Self {
            code: code.into(),
            retryable,
            repository_poison_required: false,
        }
    }

    pub fn with_repository_poison_required(mut self) -> Self {
        self.repository_poison_required = true;
        self
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub const fn retryable(&self) -> bool {
        self.retryable
    }

    pub const fn repository_poison_required(&self) -> bool {
        self.repository_poison_required
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptArtifactObservation {
    Absent,
    Partial,
    Ready,
    Inconsistent,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodingAttemptProvisionError {
    cause: CodingAttemptError,
    observation: AttemptArtifactObservation,
    process_cleanup_unproven: bool,
    repository_poison_required: bool,
}

impl CodingAttemptProvisionError {
    pub fn new(cause: CodingAttemptError, observation: AttemptArtifactObservation) -> Self {
        let repository_poison_required = cause.repository_poison_required();
        Self {
            cause,
            observation,
            process_cleanup_unproven: false,
            repository_poison_required,
        }
    }

    pub fn with_unproven_process_cleanup(mut self) -> Self {
        self.process_cleanup_unproven = true;
        self
    }

    pub fn with_repository_poison_required(mut self) -> Self {
        self.repository_poison_required = true;
        self
    }

    pub const fn cause(&self) -> &CodingAttemptError {
        &self.cause
    }

    pub const fn observation(&self) -> AttemptArtifactObservation {
        self.observation
    }

    pub const fn process_cleanup_is_unproven(&self) -> bool {
        self.process_cleanup_unproven
    }

    pub const fn repository_poison_required(&self) -> bool {
        self.repository_poison_required
    }
}

#[cfg(feature = "test-support")]
#[async_trait::async_trait]
pub trait TestTaskRuntimeSession: Send + Sync + 'static {
    fn create_role_engine_factory(
        &self,
        provider: Arc<dyn PreparedModelProvider>,
        events: Arc<dyn RoleEventSink>,
        redactor: Arc<dyn ContextRedactor>,
    ) -> Arc<dyn RoleEngineFactory>;

    fn finalization_guard(&self) -> Arc<dyn FinalizationGuard>;

    async fn workspace_fingerprint(
        &self,
        cancellation: CancellationToken,
    ) -> Result<WorkspaceFingerprint, RuntimeError>;

    async fn required_check_selectors(
        &self,
        cancellation: CancellationToken,
    ) -> Result<Vec<RequiredCheckSelector>, RuntimeError>;

    async fn terminal_snapshot(
        &self,
        revision: u64,
        cancellation: CancellationToken,
    ) -> Result<TerminalSnapshot, RuntimeError>;
}

#[derive(Clone)]
enum TaskAgentRuntimeBackend {
    Production(Arc<RuntimeSession>),
    #[cfg(feature = "test-support")]
    Test(Arc<dyn TestTaskRuntimeSession>),
}

#[derive(Clone)]
pub struct TaskAgentRuntime {
    backend: TaskAgentRuntimeBackend,
    repository_context: String,
}

impl TaskAgentRuntime {
    pub fn try_new(
        runtime: Arc<RuntimeSession>,
        repository_context: impl Into<String>,
    ) -> Result<Self, CodingAttemptError> {
        Self::try_from_backend(
            TaskAgentRuntimeBackend::Production(runtime),
            repository_context,
        )
    }

    #[cfg(feature = "test-support")]
    pub fn try_for_test(
        runtime: Arc<dyn TestTaskRuntimeSession>,
        repository_context: impl Into<String>,
    ) -> Result<Self, CodingAttemptError> {
        Self::try_from_backend(TaskAgentRuntimeBackend::Test(runtime), repository_context)
    }

    fn try_from_backend(
        backend: TaskAgentRuntimeBackend,
        repository_context: impl Into<String>,
    ) -> Result<Self, CodingAttemptError> {
        let repository_context = repository_context.into();
        if repository_context.is_empty()
            || repository_context.len() > MAX_REPOSITORY_CONTEXT_BYTES
            || repository_context
                .chars()
                .any(|character| character.is_control() && !matches!(character, '\n' | '\t'))
        {
            return Err(CodingAttemptError::new("REPOSITORY_CONTEXT_INVALID", false));
        }
        Ok(Self {
            backend,
            repository_context,
        })
    }

    pub fn repository_context(&self) -> &str {
        &self.repository_context
    }

    fn create_role_engine_factory(
        &self,
        provider: Arc<dyn PreparedModelProvider>,
        events: Arc<dyn RoleEventSink>,
        redactor: Arc<dyn ContextRedactor>,
    ) -> Arc<dyn RoleEngineFactory> {
        match &self.backend {
            TaskAgentRuntimeBackend::Production(runtime) => Arc::new(RoleScopedEngineFactory::new(
                provider,
                Arc::clone(runtime),
                events,
                redactor,
            )),
            #[cfg(feature = "test-support")]
            TaskAgentRuntimeBackend::Test(runtime) => {
                runtime.create_role_engine_factory(provider, events, redactor)
            }
        }
    }

    fn finalization_guard(&self) -> Arc<dyn FinalizationGuard> {
        match &self.backend {
            TaskAgentRuntimeBackend::Production(runtime) => Arc::new(RuntimeFinalizationGuard {
                runtime: Arc::clone(runtime),
            }),
            #[cfg(feature = "test-support")]
            TaskAgentRuntimeBackend::Test(runtime) => runtime.finalization_guard(),
        }
    }

    async fn workspace_fingerprint(
        &self,
        cancellation: CancellationToken,
    ) -> Result<WorkspaceFingerprint, RuntimeError> {
        match &self.backend {
            TaskAgentRuntimeBackend::Production(runtime) => {
                runtime.workspace_fingerprint(cancellation).await
            }
            #[cfg(feature = "test-support")]
            TaskAgentRuntimeBackend::Test(runtime) => {
                runtime.workspace_fingerprint(cancellation).await
            }
        }
    }

    async fn required_check_selectors(
        &self,
        cancellation: CancellationToken,
    ) -> Result<Vec<RequiredCheckSelector>, RuntimeError> {
        match &self.backend {
            TaskAgentRuntimeBackend::Production(runtime) => {
                runtime.required_check_selectors(cancellation).await
            }
            #[cfg(feature = "test-support")]
            TaskAgentRuntimeBackend::Test(runtime) => {
                runtime.required_check_selectors(cancellation).await
            }
        }
    }

    async fn terminal_snapshot(
        &self,
        revision: u64,
        cancellation: CancellationToken,
    ) -> Result<TerminalSnapshot, RuntimeError> {
        match &self.backend {
            TaskAgentRuntimeBackend::Production(runtime) => {
                runtime.terminal_snapshot(revision, cancellation).await
            }
            #[cfg(feature = "test-support")]
            TaskAgentRuntimeBackend::Test(runtime) => {
                runtime.terminal_snapshot(revision, cancellation).await
            }
        }
    }
}

struct RuntimeFinalizationGuard {
    runtime: Arc<RuntimeSession>,
}

#[async_trait::async_trait]
impl FinalizationGuard for RuntimeFinalizationGuard {
    async fn verify_finalization(
        &self,
        expected_fingerprint: WorkspaceFingerprint,
        cancellation: CancellationToken,
    ) -> Result<(), FinalizationGuardError> {
        if cancellation.is_cancelled() {
            return Err(FinalizationGuardError::Runtime(cancelled_runtime_error()));
        }
        let identity_before = self.runtime.verify_attempt_identity();
        if cancellation.is_cancelled() {
            return Err(FinalizationGuardError::Runtime(cancelled_runtime_error()));
        }
        map_finalization_identity(identity_before)?;

        let fingerprint_result = self
            .runtime
            .workspace_fingerprint(cancellation.clone())
            .await;
        if cancellation.is_cancelled() {
            return Err(FinalizationGuardError::Runtime(cancelled_runtime_error()));
        }
        let actual_fingerprint = fingerprint_result.map_err(FinalizationGuardError::Runtime)?;

        let identity_after = self.runtime.verify_attempt_identity();
        if cancellation.is_cancelled() {
            return Err(FinalizationGuardError::Runtime(cancelled_runtime_error()));
        }
        map_finalization_identity(identity_after)?;

        if actual_fingerprint != expected_fingerprint {
            return Err(FinalizationGuardError::WorkspaceMismatch);
        }
        Ok(())
    }
}

fn map_finalization_identity(
    result: Result<(), RuntimeError>,
) -> Result<(), FinalizationGuardError> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if error.code == ATTEMPT_IDENTITY_MISMATCH => {
            Err(FinalizationGuardError::IdentityMismatch)
        }
        Err(error) => Err(FinalizationGuardError::Runtime(error)),
    }
}

/// One prepared task attempt. `prepare` must not perform Git or filesystem side
/// effects; the runner persists `reservation()` before calling `provision`.
#[async_trait::async_trait]
pub trait CodingAgentAttempt: Send + 'static {
    fn reservation(&self) -> &AttemptReservation;

    /// Re-opens an already durable ready artifact without replaying the Git
    /// side effect. Production worktree attempts override this method with
    /// capability-bound live validation.
    async fn open_existing_ready(
        &mut self,
        _cancellation: CancellationToken,
    ) -> Result<(), CodingAttemptError> {
        Err(CodingAttemptError::new(WORKTREE_STATE_INCONSISTENT, false))
    }

    async fn provision(
        &mut self,
        cancellation: CancellationToken,
    ) -> Result<(), CodingAttemptProvisionError>;

    /// Returns a fresh runtime plus bounded context derived from its validated
    /// Cargo catalog. The context must contain no absolute host paths.
    async fn runtime(
        &self,
        cancellation: CancellationToken,
    ) -> Result<TaskAgentRuntime, CodingAttemptError>;
}

#[async_trait::async_trait]
pub trait CodingAgentAttemptFactory: Send + Sync + 'static {
    async fn prepare(
        &self,
        identity: WorktreeIdentity,
        repository: Repository,
        process_liveness_scope: ProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<Box<dyn CodingAgentAttempt>, CodingAttemptError>;
}

pub trait RepositoryWorktreeProvisionerFactory: Send + Sync + 'static {
    fn create(
        &self,
        repository: &Repository,
        process_liveness_scope: ProcessLivenessScope,
    ) -> Result<Arc<WorktreeProvisioner>, CodingAttemptError>;
}

impl<F> RepositoryWorktreeProvisionerFactory for F
where
    F: Fn(
            &Repository,
            ProcessLivenessScope,
        ) -> Result<Arc<WorktreeProvisioner>, CodingAttemptError>
        + Send
        + Sync
        + 'static,
{
    fn create(
        &self,
        repository: &Repository,
        process_liveness_scope: ProcessLivenessScope,
    ) -> Result<Arc<WorktreeProvisioner>, CodingAttemptError> {
        self(repository, process_liveness_scope)
    }
}

#[async_trait::async_trait]
pub trait ProvisionedAgentRuntimeFactory: Send + Sync + 'static {
    async fn create(
        &self,
        worktree: &ProvisionedWorktree,
        process_liveness_scope: ProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<TaskAgentRuntime, CodingAttemptError>;
}

pub struct Project2RuntimeSessionFactory {
    toolchain: ToolchainPaths,
    temporary_directory: PathBuf,
    cargo_jobs_per_task: NonZeroU32,
    limits: RuntimeSessionLimits,
}

impl Project2RuntimeSessionFactory {
    pub fn new(
        toolchain: ToolchainPaths,
        temporary_directory: impl Into<PathBuf>,
        cargo_jobs_per_task: NonZeroU32,
        limits: RuntimeSessionLimits,
    ) -> Self {
        Self {
            toolchain,
            temporary_directory: temporary_directory.into(),
            cargo_jobs_per_task,
            limits,
        }
    }

    pub fn project_2_defaults(
        toolchain: ToolchainPaths,
        temporary_directory: impl Into<PathBuf>,
        cargo_jobs_per_task: NonZeroU32,
    ) -> Self {
        Self::new(
            toolchain,
            temporary_directory,
            cargo_jobs_per_task,
            RuntimeSessionLimits::project_2_defaults(),
        )
    }
}

#[async_trait::async_trait]
impl ProvisionedAgentRuntimeFactory for Project2RuntimeSessionFactory {
    async fn create(
        &self,
        worktree: &ProvisionedWorktree,
        process_liveness_scope: ProcessLivenessScope,
        _cancellation: CancellationToken,
    ) -> Result<TaskAgentRuntime, CodingAttemptError> {
        let runtime = RuntimeSession::from_provisioned_worktree(
            worktree,
            &self.toolchain,
            &self.temporary_directory,
            process_liveness_scope,
            self.cargo_jobs_per_task,
            self.limits,
        )
        .map_err(|error| CodingAttemptError::new(error.code(), false))?;
        let repository_context = runtime.repository_context();
        TaskAgentRuntime::try_new(Arc::new(runtime), repository_context)
    }
}

/// Production bridge from repository-bound worktree capabilities to one
/// task-scoped core runtime.
pub struct WorktreeCodingAgentAttemptFactory {
    provisioners: Arc<dyn RepositoryWorktreeProvisionerFactory>,
    runtimes: Arc<dyn ProvisionedAgentRuntimeFactory>,
}

impl WorktreeCodingAgentAttemptFactory {
    pub fn new(
        provisioners: Arc<dyn RepositoryWorktreeProvisionerFactory>,
        runtimes: Arc<dyn ProvisionedAgentRuntimeFactory>,
    ) -> Self {
        Self {
            provisioners,
            runtimes,
        }
    }
}

struct WorktreeCodingAgentAttempt {
    provisioner: Arc<WorktreeProvisioner>,
    runtime_factory: Arc<dyn ProvisionedAgentRuntimeFactory>,
    process_liveness_scope: ProcessLivenessScope,
    reservation: AttemptReservation,
    runtime_reservation: Option<WorktreeReservation>,
    worktree: Option<ProvisionedWorktree>,
}

#[async_trait::async_trait]
impl CodingAgentAttemptFactory for WorktreeCodingAgentAttemptFactory {
    async fn prepare(
        &self,
        identity: WorktreeIdentity,
        repository: Repository,
        process_liveness_scope: ProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<Box<dyn CodingAgentAttempt>, CodingAttemptError> {
        let provisioner = self
            .provisioners
            .create(&repository, process_liveness_scope.clone())?;
        let runtime_reservation = provisioner
            .prepare(identity, cancellation)
            .await
            .map_err(|error| worktree_attempt_error(&error))?;
        let reservation = AttemptReservation::new(
            runtime_reservation.base_commit(),
            runtime_reservation.branch_name(),
            CanonicalPath::try_from_canonical(runtime_reservation.worktree_path().to_owned())
                .map_err(|_| CodingAttemptError::new("WORKTREE_PATH_ESCAPE", false))?,
        );
        Ok(Box::new(WorktreeCodingAgentAttempt {
            provisioner,
            runtime_factory: Arc::clone(&self.runtimes),
            process_liveness_scope,
            reservation,
            runtime_reservation: Some(runtime_reservation),
            worktree: None,
        }))
    }
}

#[async_trait::async_trait]
impl CodingAgentAttempt for WorktreeCodingAgentAttempt {
    fn reservation(&self) -> &AttemptReservation {
        &self.reservation
    }

    async fn open_existing_ready(
        &mut self,
        cancellation: CancellationToken,
    ) -> Result<(), CodingAttemptError> {
        let reservation = self
            .runtime_reservation
            .as_ref()
            .cloned()
            .ok_or_else(|| CodingAttemptError::new(WORKTREE_STATE_INCONSISTENT, false))?;
        let worktree = self
            .provisioner
            .open_ready(&reservation, cancellation)
            .await
            .map_err(|error| worktree_attempt_error(&error))?;
        self.runtime_reservation = None;
        self.worktree = Some(worktree);
        Ok(())
    }

    async fn provision(
        &mut self,
        cancellation: CancellationToken,
    ) -> Result<(), CodingAttemptProvisionError> {
        let Some(reservation) = self.runtime_reservation.take() else {
            return Err(CodingAttemptProvisionError::new(
                CodingAttemptError::new(WORKTREE_STATE_INCONSISTENT, false),
                AttemptArtifactObservation::Inconsistent,
            ));
        };
        let ready_recovery = reservation.clone();
        match self
            .provisioner
            .provision_reserved(reservation, cancellation)
            .await
        {
            Ok(worktree) => {
                self.worktree = Some(worktree);
                Ok(())
            }
            Err(error) => {
                let process_cleanup_unproven = error.process_cleanup_is_unproven();
                let mut repository_poison_required = error.repository_poison_required();
                let cause = worktree_attempt_error(error.cause());
                if process_cleanup_unproven {
                    return Err(CodingAttemptProvisionError::new(
                        cause,
                        AttemptArtifactObservation::Unavailable,
                    )
                    .with_unproven_process_cleanup());
                }
                let (observation, cause) = match error.artifact_state() {
                    WorktreeArtifactState::Absent => (AttemptArtifactObservation::Absent, cause),
                    WorktreeArtifactState::Partial => (AttemptArtifactObservation::Partial, cause),
                    WorktreeArtifactState::Ready => {
                        match self
                            .provisioner
                            .open_ready(&ready_recovery, CancellationToken::new())
                            .await
                        {
                            Ok(worktree) => {
                                self.worktree = Some(worktree);
                                (AttemptArtifactObservation::Ready, cause)
                            }
                            Err(open_error) => {
                                repository_poison_required |=
                                    open_error.requires_repository_poison();
                                let state = open_error.artifact_state_after_failed_observation();
                                (
                                    attempt_artifact_observation(state),
                                    prefer_unproven_cleanup_cause(&cause, &open_error),
                                )
                            }
                        }
                    }
                    WorktreeArtifactState::Inconsistent => {
                        (AttemptArtifactObservation::Inconsistent, cause)
                    }
                    WorktreeArtifactState::Unavailable => {
                        (AttemptArtifactObservation::Unavailable, cause)
                    }
                };
                let failure = CodingAttemptProvisionError::new(cause, observation);
                Err(if repository_poison_required {
                    failure.with_repository_poison_required()
                } else {
                    failure
                })
            }
        }
    }

    async fn runtime(
        &self,
        cancellation: CancellationToken,
    ) -> Result<TaskAgentRuntime, CodingAttemptError> {
        let worktree = self
            .worktree
            .as_ref()
            .ok_or_else(|| CodingAttemptError::new(WORKTREE_STATE_INCONSISTENT, false))?;
        self.runtime_factory
            .create(worktree, self.process_liveness_scope.clone(), cancellation)
            .await
    }
}

fn attempt_artifact_observation(state: WorktreeArtifactState) -> AttemptArtifactObservation {
    match state {
        WorktreeArtifactState::Absent => AttemptArtifactObservation::Absent,
        WorktreeArtifactState::Partial => AttemptArtifactObservation::Partial,
        WorktreeArtifactState::Ready => AttemptArtifactObservation::Ready,
        WorktreeArtifactState::Inconsistent => AttemptArtifactObservation::Inconsistent,
        WorktreeArtifactState::Unavailable => AttemptArtifactObservation::Unavailable,
    }
}

fn prefer_unproven_cleanup_cause(
    original: &CodingAttemptError,
    observed: &WorktreeError,
) -> CodingAttemptError {
    if original.code() == PROCESS_TREE_CLEANUP_FAILED {
        original.clone()
    } else {
        worktree_attempt_error(observed)
    }
}

fn worktree_attempt_error(error: &WorktreeError) -> CodingAttemptError {
    let code = error.code();
    let attempt_error = CodingAttemptError::new(
        code,
        matches!(
            code,
            "COMMAND_TIMED_OUT"
                | "WORKTREE_CREATE_FAILED"
                | PROCESS_TREE_CLEANUP_FAILED
                | "COMMAND_OUTPUT_LIMIT"
        ),
    );
    if error.requires_repository_poison() {
        attempt_error.with_repository_poison_required()
    } else {
        attempt_error
    }
}

pub struct TaskModelSession {
    provider: Arc<dyn PreparedModelProvider>,
    redactor: Arc<dyn ContextRedactor>,
}

impl TaskModelSession {
    pub fn new(
        provider: Arc<dyn PreparedModelProvider>,
        redactor: Arc<dyn ContextRedactor>,
    ) -> Self {
        Self { provider, redactor }
    }

    pub fn provider(&self) -> Arc<dyn PreparedModelProvider> {
        Arc::clone(&self.provider)
    }

    pub fn redactor(&self) -> Arc<dyn ContextRedactor> {
        Arc::clone(&self.redactor)
    }
}

/// Produces a provider and matching API-key redactor with a fresh task-scoped
/// cumulative byte budget.
pub trait TaskModelProviderFactory: Send + Sync + 'static {
    fn start_task(&self) -> TaskModelSession;
}

impl TaskModelProviderFactory for ChatCompletionsClient {
    fn start_task(&self) -> TaskModelSession {
        TaskModelSession::new(
            Arc::new(ChatCompletionsClient::start_task(self)),
            self.context_redactor(),
        )
    }
}

pub struct CodingAgentPreparationControl {
    artifacts: LiveStoreWriterArtifactAdapter,
    repository_control: Arc<RepositoryControlCoordinator>,
    repository_identity_resolver: Arc<dyn RepositoryIdentityResolver>,
}

impl CodingAgentPreparationControl {
    pub fn new(
        store: Store,
        writer: StoreWriterHandle,
        repository_control: Arc<RepositoryControlCoordinator>,
        repository_identity_resolver: Arc<dyn RepositoryIdentityResolver>,
    ) -> Self {
        Self {
            artifacts: LiveStoreWriterArtifactAdapter::new(store, writer),
            repository_control,
            repository_identity_resolver,
        }
    }
}

pub struct CodingAgentRunner {
    artifacts: LiveStoreWriterArtifactAdapter,
    repository_control: Arc<RepositoryControlCoordinator>,
    repository_identity_resolver: Arc<dyn RepositoryIdentityResolver>,
    providers: Arc<dyn TaskModelProviderFactory>,
    attempts: Arc<dyn CodingAgentAttemptFactory>,
    clock: Arc<dyn WallClock>,
    config: CodingAgentRunnerConfig,
}

struct PreparedCodingAttempt {
    context: RunContext,
    attempt: Box<dyn CodingAgentAttempt>,
}

struct PreparationFailure(Box<RunnerOutcome>);

impl From<RunnerOutcome> for PreparationFailure {
    fn from(outcome: RunnerOutcome) -> Self {
        Self(Box::new(outcome))
    }
}

impl PreparationFailure {
    fn into_outcome(self) -> RunnerOutcome {
        *self.0
    }
}

impl CodingAgentRunner {
    pub fn new(
        preparation: CodingAgentPreparationControl,
        providers: Arc<dyn TaskModelProviderFactory>,
        attempts: Arc<dyn CodingAgentAttemptFactory>,
        clock: Arc<dyn WallClock>,
        config: CodingAgentRunnerConfig,
    ) -> Self {
        let CodingAgentPreparationControl {
            artifacts,
            repository_control,
            repository_identity_resolver,
        } = preparation;
        Self {
            artifacts,
            repository_control,
            repository_identity_resolver,
            providers,
            attempts,
            clock,
            config,
        }
    }

    fn write_deadline(&self) -> Instant {
        Instant::now() + self.config.artifact_write_timeout()
    }

    async fn reserve_artifact(
        &self,
        identity: AttemptArtifactIdentity,
        reservation: &AttemptReservation,
    ) -> ArtifactMutationDisposition {
        self.artifacts
            .reserve_attempt_artifact(
                ReserveAttemptArtifact {
                    identity,
                    base_commit: reservation.base_commit().to_owned(),
                    branch_name: reservation.branch_name().to_owned(),
                    worktree_path: reservation.worktree_path().clone(),
                },
                self.write_deadline(),
            )
            .await
    }

    async fn mark_ready(&self, identity: AttemptArtifactIdentity) -> ArtifactMutationDisposition {
        self.artifacts
            .mark_attempt_artifact_ready(identity, self.write_deadline())
            .await
    }

    async fn mark_inconsistent(
        &self,
        identity: AttemptArtifactIdentity,
        code: &str,
    ) -> ArtifactMutationDisposition {
        self.artifacts
            .mark_attempt_artifact_inconsistent(identity, safe_code(code), self.write_deadline())
            .await
    }

    async fn prepare_attempt(
        &self,
        mut context: RunContext,
    ) -> Result<PreparedCodingAttempt, PreparationFailure> {
        if context.cancellation.is_cancelled() {
            return Err(RunnerOutcome::Cancelled.into());
        }

        let mut lease = context
            .take_control_lease()
            .map_err(|_| invariant_preparation_outcome())?;

        if let Err(error) = self.repository_control.revalidate_repository(
            context.repository.id,
            self.repository_identity_resolver.as_ref(),
        ) {
            let _ = lease.poison(repository_poison_reason(error));
            return Err(repository_control_outcome(error));
        }

        let identity = match WorktreeIdentity::try_new(
            context.repository.id.to_string(),
            context.task.id.to_string(),
            context.task.attempt,
        ) {
            Ok(identity) => identity,
            Err(error) => {
                return Err(release_before_reservation(
                    lease,
                    RunnerOutcome::Failed(stable_failure(error.code(), false)),
                ));
            }
        };
        let mut attempt = match self
            .attempts
            .prepare(
                identity,
                context.repository.clone(),
                context.process_liveness_scope().clone(),
                context.cancellation.clone(),
            )
            .await
        {
            Ok(attempt) => attempt,
            Err(error) => {
                let outcome = outcome_for_attempt_error(error.clone(), &context.cancellation);
                if error.code() == PROCESS_TREE_CLEANUP_FAILED {
                    let _ = lease
                        .retain_fail_closed(RepositoryControlPoisonReason::GitChildOutcomeUnknown);
                    return Err(outcome.into());
                }
                if preparation_error_requires_poison(&error) {
                    let _ = lease.poison(preparation_error_poison_reason(&error));
                    return Err(outcome.into());
                }
                return Err(release_before_reservation(lease, outcome));
            }
        };
        if context.cancellation.is_cancelled() {
            return Err(release_before_reservation(lease, RunnerOutcome::Cancelled));
        }

        let artifact_identity = AttemptArtifactIdentity {
            task_id: context.task.id,
            repository_id: context.repository.id,
            attempt: context.task.attempt,
        };
        context
            .record_artifact_identity(artifact_identity)
            .map_err(|_| invariant_preparation_outcome())?;

        let reservation = self
            .reserve_artifact(artifact_identity, attempt.reservation())
            .await;
        match reservation {
            ArtifactMutationDisposition::Confirmed(artifact) => match artifact.state {
                AttemptArtifactState::Reserved => {}
                AttemptArtifactState::Ready => {
                    if let Err(error) = attempt.open_existing_ready(CancellationToken::new()).await
                    {
                        let _ = close_lease_after_validation_error(lease, error.code());
                        return Err(outcome_for_attempt_error(error, &context.cancellation).into());
                    }
                    return self.finish_prepared(context, attempt, lease).await;
                }
                AttemptArtifactState::Inconsistent => {
                    return Err(release_before_reservation(
                        lease,
                        RunnerOutcome::Failed(stable_failure(WORKTREE_STATE_INCONSISTENT, false)),
                    ));
                }
            },
            ArtifactMutationDisposition::Reconciled(evidence) => {
                let state = evidence.state();
                if lease
                    .mark_poisoned(RepositoryControlPoisonReason::ReservationWriteFailed)
                    .and_then(|()| lease.promote_to_reconciliation())
                    .is_err()
                {
                    let _ = lease
                        .retain_fail_closed(RepositoryControlPoisonReason::ReservationWriteFailed);
                    return Err(invariant_preparation_outcome());
                }
                let proof =
                    match lease.verify_artifact_reconciliation(artifact_identity, state, &evidence)
                    {
                        Ok(proof) => proof,
                        Err(_) => {
                            let _ = lease.retain_fail_closed(
                                RepositoryControlPoisonReason::ReservationWriteFailed,
                            );
                            return Err(invariant_preparation_outcome());
                        }
                    };
                if let Err(error) = self.repository_control.revalidate_repository(
                    context.repository.id,
                    self.repository_identity_resolver.as_ref(),
                ) {
                    return Err(self
                        .finish_repository_revalidation_failure(lease, artifact_identity, error)
                        .await);
                }
                match state {
                    AttemptArtifactState::Reserved => {
                        if lease.resume_operation_after_reconciliation(proof).is_err() {
                            let _ = lease.retain_fail_closed(
                                RepositoryControlPoisonReason::ReservationWriteFailed,
                            );
                            return Err(invariant_preparation_outcome());
                        }
                    }
                    AttemptArtifactState::Ready => {
                        if let Err(error) =
                            attempt.open_existing_ready(CancellationToken::new()).await
                        {
                            let _ = close_lease_after_validation_error(lease, error.code());
                            return Err(
                                outcome_for_attempt_error(error, &context.cancellation).into()
                            );
                        }
                        if let Err(error) = self.repository_control.revalidate_repository(
                            context.repository.id,
                            self.repository_identity_resolver.as_ref(),
                        ) {
                            return Err(self
                                .finish_repository_revalidation_failure(
                                    lease,
                                    artifact_identity,
                                    error,
                                )
                                .await);
                        }
                        if lease.clean_release_after_reconciliation(proof).is_err() {
                            return Err(invariant_preparation_outcome());
                        }
                        return mark_context_prepared(context, attempt).await;
                    }
                    AttemptArtifactState::Inconsistent => {
                        if lease.clean_release_after_reconciliation(proof).is_err() {
                            return Err(invariant_preparation_outcome());
                        }
                        return Err(RunnerOutcome::Failed(stable_failure(
                            WORKTREE_STATE_INCONSISTENT,
                            false,
                        ))
                        .into());
                    }
                }
            }
            ArtifactMutationDisposition::Unresolved => {
                let _ =
                    lease.retain_fail_closed(RepositoryControlPoisonReason::ReservationWriteFailed);
                return Err(
                    RunnerOutcome::Failed(stable_failure(ARTIFACT_STORE_FAILED, true)).into(),
                );
            }
            ArtifactMutationDisposition::Conflict => {
                let _ =
                    lease.retain_fail_closed(RepositoryControlPoisonReason::ReservationWriteFailed);
                return Err(RunnerOutcome::Failed(stable_failure(
                    WORKTREE_STATE_INCONSISTENT,
                    false,
                ))
                .into());
            }
        }

        let provision = attempt.provision(context.cancellation.clone()).await;
        let ready = match provision {
            Ok(()) => true,
            Err(error)
                if error.process_cleanup_is_unproven()
                    || error.cause().code() == PROCESS_TREE_CLEANUP_FAILED =>
            {
                let _ =
                    lease.retain_fail_closed(RepositoryControlPoisonReason::GitChildOutcomeUnknown);
                return Err(RunnerOutcome::ProcessCleanupUnproven.into());
            }
            Err(error) => {
                let cause = error.cause().clone();
                if cause.code() == "REPOSITORY_IDENTITY_MISMATCH"
                    || error.repository_poison_required()
                {
                    let disposition = self
                        .mark_inconsistent(artifact_identity, WORKTREE_STATE_INCONSISTENT)
                        .await;
                    if let Err(failure) = finish_identity_mismatch_mutation(lease, disposition) {
                        return Err(RunnerOutcome::Failed(failure).into());
                    }
                    return Err(outcome_for_attempt_error(cause, &context.cancellation).into());
                }
                match error.observation() {
                    AttemptArtifactObservation::Ready => true,
                    AttemptArtifactObservation::Absent => {
                        let disposition = self
                            .mark_inconsistent(artifact_identity, WORKTREE_RESERVATION_ABANDONED)
                            .await;
                        if let Err(failure) = finish_terminal_artifact_mutation(
                            lease,
                            disposition,
                            RepositoryControlPoisonReason::InconsistentWriteFailed,
                        ) {
                            return Err(RunnerOutcome::Failed(failure).into());
                        }
                        return Err(outcome_for_attempt_error(cause, &context.cancellation).into());
                    }
                    AttemptArtifactObservation::Partial => {
                        let disposition = self
                            .mark_inconsistent(artifact_identity, WORKTREE_STATE_INCONSISTENT)
                            .await;
                        if let Err(failure) = finish_terminal_artifact_mutation(
                            lease,
                            disposition,
                            RepositoryControlPoisonReason::InconsistentWriteFailed,
                        ) {
                            return Err(RunnerOutcome::Failed(failure).into());
                        }
                        return Err(outcome_for_attempt_error(cause, &context.cancellation).into());
                    }
                    AttemptArtifactObservation::Inconsistent => {
                        let disposition = self
                            .mark_inconsistent(artifact_identity, WORKTREE_STATE_INCONSISTENT)
                            .await;
                        if let Err(failure) = finish_terminal_artifact_mutation(
                            lease,
                            disposition,
                            RepositoryControlPoisonReason::InconsistentWriteFailed,
                        ) {
                            return Err(RunnerOutcome::Failed(failure).into());
                        }
                        return Err(outcome_for_attempt_error(cause, &context.cancellation).into());
                    }
                    AttemptArtifactObservation::Unavailable => {
                        let _ = lease.poison(RepositoryControlPoisonReason::IdentityUnavailable);
                        return Err(outcome_for_attempt_error(cause, &context.cancellation).into());
                    }
                }
            }
        };
        debug_assert!(ready);

        if let Err(error) = self.repository_control.revalidate_repository(
            context.repository.id,
            self.repository_identity_resolver.as_ref(),
        ) {
            return Err(self
                .finish_repository_revalidation_failure(lease, artifact_identity, error)
                .await);
        }

        let disposition = self.mark_ready(artifact_identity).await;
        if let Err(failure) = finish_terminal_artifact_mutation(
            lease,
            disposition,
            RepositoryControlPoisonReason::ReadyWriteFailed,
        ) {
            return Err(RunnerOutcome::Failed(failure).into());
        }
        mark_context_prepared(context, attempt).await
    }

    async fn finish_prepared(
        &self,
        context: RunContext,
        attempt: Box<dyn CodingAgentAttempt>,
        lease: RepositoryControlLease,
    ) -> Result<PreparedCodingAttempt, PreparationFailure> {
        if let Err(error) = self.repository_control.revalidate_repository(
            context.repository.id,
            self.repository_identity_resolver.as_ref(),
        ) {
            let artifact_identity = AttemptArtifactIdentity {
                task_id: context.task.id,
                repository_id: context.repository.id,
                attempt: context.task.attempt,
            };
            return Err(self
                .finish_repository_revalidation_failure(lease, artifact_identity, error)
                .await);
        }
        if lease.clean_release().is_err() {
            return Err(invariant_preparation_outcome());
        }
        mark_context_prepared(context, attempt).await
    }

    async fn finish_repository_revalidation_failure(
        &self,
        lease: RepositoryControlLease,
        artifact_identity: AttemptArtifactIdentity,
        error: RepositoryControlError,
    ) -> PreparationFailure {
        if matches!(
            error,
            RepositoryControlError::IdentityDrift | RepositoryControlError::AliasConflict
        ) {
            let disposition = self
                .mark_inconsistent(artifact_identity, WORKTREE_STATE_INCONSISTENT)
                .await;
            return match finish_identity_mismatch_mutation(lease, disposition) {
                Ok(()) => repository_control_outcome(error),
                Err(failure) => RunnerOutcome::Failed(failure).into(),
            };
        }
        let reason = repository_poison_reason(error);
        let completion = if error == RepositoryControlError::IdentityUnavailable {
            lease.poison(reason)
        } else {
            lease.retain_fail_closed(reason)
        };
        if completion.is_err() {
            invariant_preparation_outcome()
        } else {
            repository_control_outcome(error)
        }
    }

    async fn cleanup_terminal_events(
        &self,
        runtime: &TaskAgentRuntime,
        events: &Arc<AppRoleEventSink>,
        checkpoint: &mut WorkspaceCheckpoint,
        required_checks: Option<&RequiredCheckLedger>,
    ) -> Result<(), RuntimeError> {
        let capture_cancellation = CancellationToken::new();
        let terminal_snapshot = match timeout(
            self.config.artifact_write_timeout(),
            runtime.terminal_snapshot(checkpoint.generation(), capture_cancellation.clone()),
        )
        .await
        {
            Ok(Ok(snapshot)) => Some(snapshot),
            Ok(Err(error)) if error.code == PROCESS_TREE_CLEANUP_FAILED => return Err(error),
            Ok(Err(_)) => None,
            Err(_) => {
                capture_cancellation.cancel();
                None
            }
        };
        let projection_cancellation = CancellationToken::new();

        let terminal_verified = if let Some(mut snapshot) = terminal_snapshot {
            if checkpoint.observe_stable(snapshot.fingerprint).is_ok() {
                snapshot.diff.revision = checkpoint.generation();
                self.emit_cleanup_event(
                    events,
                    RoleEvent::Diff(snapshot.diff),
                    projection_cancellation.clone(),
                )
                .await?;
                true
            } else {
                false
            }
        } else {
            false
        };

        if let Some(required_checks) = required_checks {
            let tests = if terminal_verified {
                project_test_snapshot(required_checks, checkpoint)
            } else {
                project_unverified_test_snapshot(required_checks, checkpoint.generation())
            };
            self.emit_cleanup_event(events, RoleEvent::Tests(tests), projection_cancellation)
                .await?;
        }
        Ok(())
    }

    async fn emit_cleanup_event(
        &self,
        events: &Arc<AppRoleEventSink>,
        event: RoleEvent,
        cancellation: CancellationToken,
    ) -> Result<(), RuntimeError> {
        match timeout(
            self.config.artifact_write_timeout(),
            events.emit(event, cancellation.clone()),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                cancellation.cancel();
                Err(RuntimeError::new(
                    EVENT_SINK_REJECTED,
                    EVENT_SINK_MESSAGE,
                    true,
                ))
            }
        }
    }
}

async fn mark_context_prepared(
    mut context: RunContext,
    attempt: Box<dyn CodingAgentAttempt>,
) -> Result<PreparedCodingAttempt, PreparationFailure> {
    if context.artifact_identity().is_none() {
        return Err(invariant_preparation_outcome());
    }
    context
        .mark_preparation_complete()
        .await
        .map_err(|_| invariant_preparation_outcome())?;
    Ok(PreparedCodingAttempt { context, attempt })
}

fn release_before_reservation(
    lease: RepositoryControlLease,
    outcome: RunnerOutcome,
) -> PreparationFailure {
    if lease.clean_release().is_ok() {
        outcome.into()
    } else {
        invariant_preparation_outcome()
    }
}

fn finish_terminal_artifact_mutation(
    mut lease: RepositoryControlLease,
    disposition: ArtifactMutationDisposition,
    poison_reason: RepositoryControlPoisonReason,
) -> Result<(), TaskFailure> {
    match disposition {
        ArtifactMutationDisposition::Confirmed(artifact) => {
            if !matches!(
                artifact.state,
                AttemptArtifactState::Ready | AttemptArtifactState::Inconsistent
            ) {
                let _ = lease.retain_fail_closed(poison_reason);
                return Err(stable_failure(WORKTREE_STATE_INCONSISTENT, false));
            }
            lease
                .clean_release()
                .map_err(|_| stable_failure(REPOSITORY_CONTROL_BUSY, true))
        }
        ArtifactMutationDisposition::Reconciled(evidence) => {
            let identity = evidence.identity();
            let state = evidence.state();
            if !matches!(
                state,
                AttemptArtifactState::Ready | AttemptArtifactState::Inconsistent
            ) {
                let _ = lease.retain_fail_closed(poison_reason);
                return Err(stable_failure(WORKTREE_STATE_INCONSISTENT, false));
            }
            if lease
                .mark_poisoned(poison_reason)
                .and_then(|()| lease.promote_to_reconciliation())
                .is_err()
            {
                let _ = lease.retain_fail_closed(poison_reason);
                return Err(stable_failure(REPOSITORY_CONTROL_BUSY, true));
            }
            let proof = match lease.verify_artifact_reconciliation(identity, state, &evidence) {
                Ok(proof) => proof,
                Err(_) => {
                    let _ = lease.retain_fail_closed(poison_reason);
                    return Err(stable_failure(REPOSITORY_CONTROL_BUSY, true));
                }
            };
            lease
                .clean_release_after_reconciliation(proof)
                .map_err(|_| stable_failure(REPOSITORY_CONTROL_BUSY, true))
        }
        ArtifactMutationDisposition::Unresolved => {
            let _ = lease.retain_fail_closed(poison_reason);
            Err(stable_failure(ARTIFACT_STORE_FAILED, true))
        }
        ArtifactMutationDisposition::Conflict => {
            let _ = lease.retain_fail_closed(poison_reason);
            Err(stable_failure(WORKTREE_STATE_INCONSISTENT, false))
        }
    }
}

fn finish_identity_mismatch_mutation(
    lease: RepositoryControlLease,
    disposition: ArtifactMutationDisposition,
) -> Result<(), TaskFailure> {
    match disposition {
        ArtifactMutationDisposition::Confirmed(artifact)
            if artifact.state == AttemptArtifactState::Inconsistent =>
        {
            lease
                .poison(RepositoryControlPoisonReason::SideEffectIdentityMismatch)
                .map_err(|_| stable_failure(REPOSITORY_CONTROL_BUSY, true))
        }
        ArtifactMutationDisposition::Reconciled(evidence)
            if evidence.state() == AttemptArtifactState::Inconsistent =>
        {
            lease
                .poison(RepositoryControlPoisonReason::SideEffectIdentityMismatch)
                .map_err(|_| stable_failure(REPOSITORY_CONTROL_BUSY, true))
        }
        ArtifactMutationDisposition::Confirmed(_)
        | ArtifactMutationDisposition::Reconciled(_)
        | ArtifactMutationDisposition::Conflict => {
            let _ =
                lease.retain_fail_closed(RepositoryControlPoisonReason::SideEffectIdentityMismatch);
            Err(stable_failure(WORKTREE_STATE_INCONSISTENT, false))
        }
        ArtifactMutationDisposition::Unresolved => {
            let _ =
                lease.retain_fail_closed(RepositoryControlPoisonReason::InconsistentWriteFailed);
            Err(stable_failure(ARTIFACT_STORE_FAILED, true))
        }
    }
}

fn preparation_error_requires_poison(error: &CodingAttemptError) -> bool {
    error.repository_poison_required()
        || matches!(
            error.code(),
            "REPOSITORY_IDENTITY_MISMATCH" | "REPOSITORY_IDENTITY_UNAVAILABLE"
        )
}

fn repository_poison_reason_for_code(code: &str) -> RepositoryControlPoisonReason {
    match code {
        PROCESS_TREE_CLEANUP_FAILED => RepositoryControlPoisonReason::GitChildOutcomeUnknown,
        "REPOSITORY_IDENTITY_MISMATCH" => RepositoryControlPoisonReason::SideEffectIdentityMismatch,
        _ => RepositoryControlPoisonReason::IdentityUnavailable,
    }
}

fn preparation_error_poison_reason(error: &CodingAttemptError) -> RepositoryControlPoisonReason {
    if error.repository_poison_required() {
        RepositoryControlPoisonReason::SideEffectIdentityMismatch
    } else {
        repository_poison_reason_for_code(error.code())
    }
}

fn close_lease_after_validation_error(
    lease: RepositoryControlLease,
    code: &str,
) -> Result<(), RepositoryControlError> {
    let reason = repository_poison_reason_for_code(code);
    if reason == RepositoryControlPoisonReason::GitChildOutcomeUnknown {
        lease.retain_fail_closed(reason)
    } else {
        lease.poison(reason)
    }
}

fn repository_poison_reason(error: RepositoryControlError) -> RepositoryControlPoisonReason {
    match error {
        RepositoryControlError::IdentityDrift => RepositoryControlPoisonReason::IdentityDrift,
        RepositoryControlError::AliasConflict => RepositoryControlPoisonReason::AliasConflict,
        RepositoryControlError::IdentityUnavailable => {
            RepositoryControlPoisonReason::IdentityUnavailable
        }
        RepositoryControlError::Busy
        | RepositoryControlError::Poisoned
        | RepositoryControlError::UnknownRepository
        | RepositoryControlError::UnknownCoordinationKey
        | RepositoryControlError::NotPoisoned
        | RepositoryControlError::StaleLease
        | RepositoryControlError::LeaseSpaceExhausted
        | RepositoryControlError::InvalidReconciliationProof => {
            RepositoryControlPoisonReason::AbnormalLeaseDrop
        }
    }
}

fn repository_control_outcome(error: RepositoryControlError) -> PreparationFailure {
    let retryable = matches!(
        error,
        RepositoryControlError::Busy
            | RepositoryControlError::Poisoned
            | RepositoryControlError::IdentityUnavailable
    );
    RunnerOutcome::Failed(stable_failure(REPOSITORY_CONTROL_BUSY, retryable)).into()
}

fn invariant_preparation_outcome() -> PreparationFailure {
    RunnerOutcome::Failed(stable_failure(WORKTREE_STATE_INCONSISTENT, false)).into()
}

#[async_trait::async_trait]
impl TaskRunner for CodingAgentRunner {
    async fn run(&self, context: RunContext, sink: RunnerEventSink) -> RunnerOutcome {
        let PreparedCodingAttempt { context, attempt } = match self.prepare_attempt(context).await {
            Ok(prepared) => prepared,
            Err(failure) => return failure.into_outcome(),
        };

        let loop_cancellation = context.cancellation.child_token();
        let events = AppRoleEventSink::new(
            sink,
            Arc::clone(&self.clock),
            context.task.started_at.unwrap_or(context.task.created_at),
            self.config.diff_debounce(),
            context.task.id.to_string(),
            loop_cancellation.clone(),
        );
        if events
            .emit_system_activity("Prepared isolated worktree")
            .await
            .is_err()
        {
            return event_failure_outcome(&context.cancellation);
        }

        let task_runtime = match attempt.runtime(loop_cancellation.clone()).await {
            Ok(runtime) => runtime,
            Err(error) => return outcome_for_attempt_error(error, &context.cancellation),
        };
        let initial_fingerprint = match task_runtime
            .workspace_fingerprint(loop_cancellation.clone())
            .await
        {
            Ok(fingerprint) => fingerprint,
            Err(error) => {
                let _ = events.finish().await;
                return outcome_for_runtime_error(error, &context.cancellation);
            }
        };
        let repository_check_catalog = match task_runtime
            .required_check_selectors(loop_cancellation.clone())
            .await
        {
            Ok(catalog) => catalog,
            Err(error) => {
                let _ = events.finish().await;
                return outcome_for_runtime_error(error, &context.cancellation);
            }
        };
        let repository_context = format!(
            "{ISOLATION_CONTEXT}\n\n{}",
            task_runtime.repository_context()
        );
        let checkpoint = WorkspaceCheckpoint::new(initial_fingerprint);
        let model_session = self.providers.start_task();
        let role_events: Arc<dyn RoleEventSink> = events.clone();
        let engine_factory = task_runtime.create_role_engine_factory(
            model_session.provider(),
            role_events,
            model_session.redactor(),
        );
        let orchestrator =
            MultiRoleOrchestrator::new(engine_factory, task_runtime.finalization_guard());
        let report = orchestrator
            .run(
                MultiRoleInput {
                    task_prompt: &context.task.prompt,
                    repository_context: &repository_context,
                    checkpoint,
                    repository_check_catalog: &repository_check_catalog,
                },
                loop_cancellation,
            )
            .await;
        let (outcome, mut final_checkpoint, required_checks) = report.into_parts();
        if matches!(
            outcome,
            MultiRoleOutcome::Failed(_) | MultiRoleOutcome::Cancelled
        ) && let Err(error) = self
            .cleanup_terminal_events(
                &task_runtime,
                &events,
                &mut final_checkpoint,
                required_checks.as_ref(),
            )
            .await
        {
            let _ = events.finish().await;
            return outcome_for_runtime_error(error, &context.cancellation);
        }
        if events.finish().await.is_err() {
            return event_failure_outcome(&context.cancellation);
        }

        match outcome {
            MultiRoleOutcome::Approved(decision) => {
                RunnerOutcome::Approved(decision.evidence().clone())
            }
            MultiRoleOutcome::Rejected { decision, .. } => {
                RunnerOutcome::Rejected(decision.evidence().clone())
            }
            MultiRoleOutcome::Cancelled => RunnerOutcome::Cancelled,
            MultiRoleOutcome::Failed(failure) => {
                outcome_for_task_failure(failure.failure().clone())
            }
        }
    }
}

fn outcome_for_attempt_error(
    error: CodingAttemptError,
    cancellation: &CancellationToken,
) -> RunnerOutcome {
    if error.code() == PROCESS_TREE_CLEANUP_FAILED {
        RunnerOutcome::ProcessCleanupUnproven
    } else if cancellation.is_cancelled() || error.code() == COMMAND_CANCELLED {
        RunnerOutcome::Cancelled
    } else {
        RunnerOutcome::Failed(stable_failure(error.code(), error.retryable()))
    }
}

fn outcome_for_task_failure(failure: TaskFailure) -> RunnerOutcome {
    if failure.code == PROCESS_TREE_CLEANUP_FAILED {
        RunnerOutcome::ProcessCleanupUnproven
    } else {
        RunnerOutcome::Failed(failure)
    }
}

fn event_failure_outcome(cancellation: &CancellationToken) -> RunnerOutcome {
    if cancellation.is_cancelled() {
        RunnerOutcome::Cancelled
    } else {
        RunnerOutcome::Failed(stable_failure(EVENT_SINK_REJECTED, true))
    }
}

fn safe_code(code: &str) -> &str {
    if !code.is_empty()
        && code.len() <= 96
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        code
    } else {
        CODING_AGENT_FAILED
    }
}

fn stable_failure(code: &str, retryable: bool) -> TaskFailure {
    let code = safe_code(code);
    TaskFailure {
        code: code.to_owned(),
        message: match code {
            ARTIFACT_STORE_FAILED => "attempt artifact state could not be persisted",
            EVENT_SINK_REJECTED => EVENT_SINK_MESSAGE,
            WORKTREE_STATE_INCONSISTENT => "the isolated worktree state is inconsistent",
            _ => "the coding agent could not complete the task",
        }
        .to_owned(),
        retryable,
    }
}

struct EventProjection {
    clock: Arc<dyn WallClock>,
    fallback_time: UtcTimestamp,
    activity_id_scope: String,
    next_activity_id: AtomicU64,
}

impl EventProjection {
    fn new(
        clock: Arc<dyn WallClock>,
        fallback_time: UtcTimestamp,
        activity_id_scope: String,
    ) -> Self {
        Self {
            clock,
            fallback_time,
            activity_id_scope,
            next_activity_id: AtomicU64::new(1),
        }
    }

    fn system_activity(&self, message: impl Into<String>) -> Result<RunnerEvent, RuntimeError> {
        self.activity(ActivityActor::System, None, message)
    }

    fn role_activity(
        &self,
        activity: coding_agent_core::RoleActivityEvent,
    ) -> Result<RunnerEvent, RuntimeError> {
        let actor = match activity.role() {
            Role::Planner => ActivityActor::Planner,
            Role::Executor => ActivityActor::Executor,
            Role::Reviewer => ActivityActor::Reviewer,
        };
        self.activity(actor, Some(activity.role_run()), activity.message())
    }

    fn activity(
        &self,
        actor: ActivityActor,
        role_run: Option<u32>,
        message: impl Into<String>,
    ) -> Result<RunnerEvent, RuntimeError> {
        let ordinal = self.next_activity_id.fetch_add(1, Ordering::Relaxed);
        let created_at = UtcTimestamp::new(self.clock.now_utc()).unwrap_or(self.fallback_time);
        let entry = ActivityEntry::try_new(
            format!("coding-agent-{}-{ordinal}", self.activity_id_scope),
            ActivityLevel::Info,
            actor,
            role_run,
            message,
            created_at,
        )
        .map_err(|_| {
            RuntimeError::new(
                "ACTIVITY_EVENT_INVALID",
                "the role activity could not be projected",
                false,
            )
        })?;
        Ok(RunnerEvent::ActivityAppended(entry))
    }
}

fn map_diff(diff: DiffEvent) -> DiffSnapshot {
    DiffSnapshot {
        revision: diff.revision,
        files: diff
            .files
            .into_iter()
            .map(|file| DiffFile {
                path: file.path,
                status: match file.status {
                    CoreDiffFileStatus::Added => DiffFileStatus::Added,
                    CoreDiffFileStatus::Modified => DiffFileStatus::Modified,
                    CoreDiffFileStatus::Deleted => DiffFileStatus::Deleted,
                },
                patch: file.patch,
                additions: file.additions,
                deletions: file.deletions,
                truncated: file.truncated,
            })
            .collect(),
    }
}

#[derive(Debug, Clone)]
struct PersistedDiff {
    event: DiffEvent,
    sequence: u64,
}

#[derive(Debug, Clone)]
struct PersistedTests {
    snapshot: TestSnapshot,
    sequence: u64,
}

#[derive(Default)]
struct EventState {
    pending_diff: Option<DiffEvent>,
    last_diff: Option<PersistedDiff>,
    last_tests: Option<PersistedTests>,
    last_checkpoint: Option<DurableCheckpointAck>,
    debounce_epoch: u64,
    timer_armed: bool,
    closed: bool,
    failure: Option<RunnerEventError>,
}

struct AppRoleEventSink {
    sink: Arc<RunnerEventSink>,
    projection: EventProjection,
    debounce: Duration,
    loop_cancellation: CancellationToken,
    state: Mutex<EventState>,
    delivery: Mutex<()>,
    self_weak: Weak<Self>,
}

impl AppRoleEventSink {
    fn new(
        sink: RunnerEventSink,
        clock: Arc<dyn WallClock>,
        fallback_time: UtcTimestamp,
        debounce: Duration,
        activity_id_scope: String,
        loop_cancellation: CancellationToken,
    ) -> Arc<Self> {
        Arc::new_cyclic(|self_weak| Self {
            sink: Arc::new(sink),
            projection: EventProjection::new(clock, fallback_time, activity_id_scope),
            debounce,
            loop_cancellation,
            state: Mutex::new(EventState::default()),
            delivery: Mutex::new(()),
            self_weak: self_weak.clone(),
        })
    }

    async fn emit_system_activity(&self, message: &str) -> Result<(), RuntimeError> {
        let _delivery = self.delivery.lock().await;
        self.ensure_open().await?;
        let event = self.projection.system_activity(message)?;
        self.deliver_runner_event(event, &self.loop_cancellation)
            .await?;
        ensure_not_cancelled(&self.loop_cancellation)
    }

    async fn queue_diff(
        &self,
        diff: DiffEvent,
        cancellation: &CancellationToken,
    ) -> Result<(), RuntimeError> {
        ensure_not_cancelled(cancellation)?;
        let debounce_epoch = {
            let mut state = self.state.lock().await;
            if let Some(error) = state.failure {
                return Err(runtime_sink_error(error));
            }
            if state.closed {
                return Err(runtime_sink_error(RunnerEventError::TaskNotRunning));
            }
            state.pending_diff = Some(diff);
            state.last_checkpoint = None;
            if state.timer_armed {
                return Ok(());
            }
            state.timer_armed = true;
            state.debounce_epoch = state.debounce_epoch.wrapping_add(1);
            state.debounce_epoch
        };
        let delay = self.debounce;
        let weak = self.self_weak.clone();
        tokio::spawn(async move {
            sleep(delay).await;
            if let Some(events) = weak.upgrade() {
                events.flush_debounce_epoch(debounce_epoch).await;
            }
        });
        Ok(())
    }

    async fn flush_debounce_epoch(&self, debounce_epoch: u64) {
        let _delivery = self.delivery.lock().await;
        let diff = {
            let mut state = self.state.lock().await;
            if state.closed
                || state.failure.is_some()
                || !state.timer_armed
                || state.debounce_epoch != debounce_epoch
            {
                return;
            }
            state.timer_armed = false;
            state.pending_diff.take()
        };
        if let Some(diff) = diff {
            let cancellation = self.loop_cancellation.clone();
            if let Err(error) = self.deliver_diff(diff, &cancellation).await
                && error.code != COMMAND_CANCELLED
            {
                self.loop_cancellation.cancel();
            }
        }
    }

    async fn ensure_open(&self) -> Result<(), RuntimeError> {
        let state = self.state.lock().await;
        if let Some(error) = state.failure {
            return Err(runtime_sink_error(error));
        }
        if state.closed {
            return Err(runtime_sink_error(RunnerEventError::TaskNotRunning));
        }
        Ok(())
    }

    async fn deliver_runner_event(
        &self,
        event: RunnerEvent,
        cancellation: &CancellationToken,
    ) -> Result<u64, RuntimeError> {
        ensure_not_cancelled(cancellation)?;
        let event_id = match self.sink.append(event).await {
            Ok(event_id) => event_id,
            Err(error) => {
                self.record_failure(error).await;
                return Err(runtime_sink_error(error));
            }
        };
        let sequence = event_sequence(event_id)?;
        ensure_not_cancelled(cancellation)?;
        Ok(sequence)
    }

    async fn deliver_diff(
        &self,
        diff: DiffEvent,
        cancellation: &CancellationToken,
    ) -> Result<u64, RuntimeError> {
        if let Some(persisted) = self.state.lock().await.last_diff.as_ref()
            && persisted.event == diff
        {
            return Ok(persisted.sequence);
        }
        let sequence = self
            .deliver_runner_event(
                RunnerEvent::DiffUpdated(map_diff(diff.clone())),
                cancellation,
            )
            .await?;
        self.state.lock().await.last_diff = Some(PersistedDiff {
            event: diff,
            sequence,
        });
        Ok(sequence)
    }

    async fn flush_pending_diff(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<Option<u64>, RuntimeError> {
        let diff = {
            let mut state = self.state.lock().await;
            state.timer_armed = false;
            state.debounce_epoch = state.debounce_epoch.wrapping_add(1);
            state.pending_diff.take()
        };
        match diff {
            Some(diff) => self.deliver_diff(diff, cancellation).await.map(Some),
            None => Ok(None),
        }
    }

    async fn deliver_tests(
        &self,
        snapshot: TestSnapshot,
        cancellation: &CancellationToken,
    ) -> Result<u64, RuntimeError> {
        self.flush_pending_diff(cancellation).await?;
        if let Some(persisted) = self.state.lock().await.last_tests.as_ref()
            && persisted.snapshot == snapshot
        {
            return Ok(persisted.sequence);
        }
        let sequence = self
            .deliver_runner_event(RunnerEvent::TestUpdated(snapshot.clone()), cancellation)
            .await?;
        let mut state = self.state.lock().await;
        if state
            .last_checkpoint
            .is_some_and(|ack| ack.generation() != snapshot.revision)
        {
            state.last_checkpoint = None;
        }
        state.last_tests = Some(PersistedTests { snapshot, sequence });
        Ok(sequence)
    }

    async fn record_failure(&self, error: RunnerEventError) {
        let mut state = self.state.lock().await;
        state.failure.get_or_insert(error);
        self.loop_cancellation.cancel();
    }

    async fn finish(&self) -> Result<(), RuntimeError> {
        let _delivery = self.delivery.lock().await;
        let diff = {
            let mut state = self.state.lock().await;
            state.closed = true;
            state.timer_armed = false;
            state.debounce_epoch = state.debounce_epoch.wrapping_add(1);
            if let Some(error) = state.failure {
                return Err(runtime_sink_error(error));
            }
            state.pending_diff.take()
        };
        if let Some(diff) = diff {
            self.deliver_diff(diff, &CancellationToken::new()).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl RoleEventSink for AppRoleEventSink {
    async fn emit(
        &self,
        event: RoleEvent,
        cancellation: CancellationToken,
    ) -> Result<(), RuntimeError> {
        match event {
            RoleEvent::Diff(diff) => self.queue_diff(diff, &cancellation).await,
            RoleEvent::Activity(activity) => {
                let _delivery = self.delivery.lock().await;
                self.ensure_open().await?;
                let event = self.projection.role_activity(activity)?;
                self.deliver_runner_event(event, &cancellation).await?;
                Ok(())
            }
            RoleEvent::Tests(tests) => {
                let _delivery = self.delivery.lock().await;
                self.ensure_open().await?;
                self.deliver_tests(tests, &cancellation).await?;
                Ok(())
            }
        }
    }

    async fn emit_durable(
        &self,
        event: DurableRoleEvent,
        cancellation: CancellationToken,
    ) -> Result<DurableEventAck, RuntimeError> {
        let _delivery = self.delivery.lock().await;
        self.ensure_open().await?;
        ensure_not_cancelled(&cancellation)?;
        let sequence = match event {
            DurableRoleEvent::StructuredPlan(plan) | DurableRoleEvent::PlanUpdated(plan) => {
                self.deliver_runner_event(RunnerEvent::PlanUpdated(plan), &cancellation)
                    .await?
            }
            DurableRoleEvent::IntermediateReview {
                evidence,
                after_checkpoint_sequence,
            } => {
                let generation = evidence.workspace_generation();
                let barrier_matches = self.state.lock().await.last_checkpoint.is_some_and(|ack| {
                    ack.sequence() == after_checkpoint_sequence && ack.generation() == generation
                });
                if !barrier_matches {
                    return Err(RuntimeError::new(
                        "DURABLE_CHECKPOINT_MISMATCH",
                        "review evidence did not follow its durable checkpoint barrier",
                        false,
                    ));
                }
                let event_id = match self.sink.record_review(evidence).await {
                    Ok(event_id) => event_id,
                    Err(error) => {
                        self.record_failure(error).await;
                        return Err(runtime_sink_error(error));
                    }
                };
                let sequence = event_sequence(event_id)?;
                if sequence <= after_checkpoint_sequence {
                    return Err(RuntimeError::new(
                        "DURABLE_EVENT_ORDER_INVALID",
                        "review evidence did not advance the durable event sequence",
                        false,
                    ));
                }
                ensure_not_cancelled(&cancellation)?;
                sequence
            }
        };
        DurableEventAck::try_new(sequence)
    }

    async fn flush_checkpoint(
        &self,
        generation: u64,
        cancellation: CancellationToken,
    ) -> Result<DurableCheckpointAck, RuntimeError> {
        let _delivery = self.delivery.lock().await;
        self.ensure_open().await?;
        ensure_not_cancelled(&cancellation)?;
        self.flush_pending_diff(&cancellation).await?;
        let (diff_sequence, test_sequence) = {
            let state = self.state.lock().await;
            let diff_sequence = state
                .last_diff
                .as_ref()
                .filter(|persisted| persisted.event.revision == generation)
                .map(|persisted| persisted.sequence);
            let test_sequence = state
                .last_tests
                .as_ref()
                .filter(|persisted| persisted.snapshot.revision == generation)
                .map(|persisted| persisted.sequence);
            (diff_sequence, test_sequence)
        };
        let (Some(diff_sequence), Some(test_sequence)) = (diff_sequence, test_sequence) else {
            return Err(RuntimeError::new(
                "DURABLE_CHECKPOINT_INCOMPLETE",
                "the current diff and test projections were not both durable",
                false,
            ));
        };
        let ack = DurableCheckpointAck::try_new(diff_sequence.max(test_sequence), generation)?;
        self.state.lock().await.last_checkpoint = Some(ack);
        ensure_not_cancelled(&cancellation)?;
        Ok(ack)
    }
}

fn event_sequence(event_id: EventId) -> Result<u64, RuntimeError> {
    u64::try_from(event_id.get()).map_err(|_| {
        RuntimeError::new(
            "DURABLE_EVENT_ORDER_INVALID",
            "the durable event sequence was outside its valid range",
            false,
        )
    })
}

fn ensure_not_cancelled(cancellation: &CancellationToken) -> Result<(), RuntimeError> {
    if cancellation.is_cancelled() {
        Err(cancelled_runtime_error())
    } else {
        Ok(())
    }
}

fn cancelled_runtime_error() -> RuntimeError {
    RuntimeError::new(COMMAND_CANCELLED, "the coding task was cancelled", false)
}

fn outcome_for_runtime_error(
    error: RuntimeError,
    cancellation: &CancellationToken,
) -> RunnerOutcome {
    if error.code == PROCESS_TREE_CLEANUP_FAILED {
        RunnerOutcome::ProcessCleanupUnproven
    } else if cancellation.is_cancelled() || error.code == COMMAND_CANCELLED {
        RunnerOutcome::Cancelled
    } else {
        RunnerOutcome::Failed(stable_failure(&error.code, error.retryable))
    }
}

fn runtime_sink_error(error: RunnerEventError) -> RuntimeError {
    RuntimeError::new(
        EVENT_SINK_REJECTED,
        EVENT_SINK_MESSAGE,
        !matches!(error, RunnerEventError::TaskNotRunning),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use coding_agent_core::{Role, RoleActivityEvent};
    use coding_agent_domain::{ActivityActor, UtcTimestamp};

    use coding_agent_runtime::{WorktreeArtifactState, WorktreeError};

    use super::{
        AttemptArtifactObservation, CodingAgentRunnerConfig, CodingAttemptError, EventProjection,
        attempt_artifact_observation, prefer_unproven_cleanup_cause, worktree_attempt_error,
    };
    use crate::{RunnerEvent, WallClock};

    struct FixedClock;

    impl WallClock for FixedClock {
        fn now_utc(&self) -> time::OffsetDateTime {
            time::OffsetDateTime::UNIX_EPOCH
        }
    }

    fn timestamp() -> UtcTimestamp {
        UtcTimestamp::parse_rfc3339("2026-07-17T00:00:00Z").unwrap()
    }

    #[test]
    fn config_rejects_zero_and_unbounded_durations() {
        assert!(
            CodingAgentRunnerConfig::try_new(Duration::ZERO, Duration::from_millis(1)).is_err()
        );
        assert!(
            CodingAgentRunnerConfig::try_new(Duration::from_secs(1), Duration::from_secs(11))
                .is_err()
        );
    }

    #[test]
    fn production_worktree_bridge_preserves_exact_identity_and_unavailable_evidence() {
        let mismatch = WorktreeError::CommonGitIdentityMismatch;
        assert_eq!(
            mismatch.artifact_state_after_failed_observation(),
            WorktreeArtifactState::Inconsistent
        );
        assert_eq!(
            attempt_artifact_observation(mismatch.artifact_state_after_failed_observation()),
            AttemptArtifactObservation::Inconsistent
        );
        assert_eq!(
            worktree_attempt_error(&mismatch).code(),
            "REPOSITORY_IDENTITY_MISMATCH"
        );

        let unavailable = WorktreeError::CommonGitIdentityUnavailable;
        assert_eq!(
            unavailable.artifact_state_after_failed_observation(),
            WorktreeArtifactState::Unavailable
        );
        assert_eq!(
            attempt_artifact_observation(unavailable.artifact_state_after_failed_observation()),
            AttemptArtifactObservation::Unavailable
        );
        assert_eq!(
            worktree_attempt_error(&unavailable).code(),
            "REPOSITORY_IDENTITY_UNAVAILABLE"
        );

        let cleanup = CodingAttemptError::new("PROCESS_TREE_CLEANUP_FAILED", true);
        assert_eq!(
            prefer_unproven_cleanup_cause(&cleanup, &mismatch).code(),
            "PROCESS_TREE_CLEANUP_FAILED",
            "secondary validation evidence cannot erase an unknown process tree"
        );

        assert!(
            worktree_attempt_error(&WorktreeError::BranchConflict).repository_poison_required(),
            "pre-reservation branch collision preserves typed external-control evidence"
        );
        assert!(
            !worktree_attempt_error(&WorktreeError::WorktreeContentChanged)
                .repository_poison_required(),
            "ordinary content inconsistency does not invent repository identity drift"
        );
    }

    #[test]
    fn role_activity_projection_preserves_actor_run_and_global_id_scope() {
        let projection =
            EventProjection::new(Arc::new(FixedClock), timestamp(), "task-7".to_owned());
        let RunnerEvent::ActivityAppended(first) = projection
            .role_activity(RoleActivityEvent::try_new(Role::Planner, 1, "planned").unwrap())
            .unwrap()
        else {
            panic!("project role activity")
        };
        let RunnerEvent::ActivityAppended(second) = projection
            .role_activity(RoleActivityEvent::try_new(Role::Reviewer, 2, "reviewed").unwrap())
            .unwrap()
        else {
            panic!("project role activity")
        };
        assert_eq!(first.actor(), ActivityActor::Planner);
        assert_eq!(first.role_run(), Some(1));
        assert_eq!(first.id(), "coding-agent-task-7-1");
        assert_eq!(second.actor(), ActivityActor::Reviewer);
        assert_eq!(second.role_run(), Some(2));
        assert_eq!(second.id(), "coding-agent-task-7-2");
    }
}
