use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use coding_agent_core::{
    ActivityEvent, ActivityLevel as CoreActivityLevel, AgentEvent, AgentEventSink, AgentInput,
    AgentLimits, AgentLoop, AgentOutcome, AgentRuntime, ContextRedactor, DiffEvent,
    DiffFileStatus as CoreDiffFileStatus, ModelProvider, PlanItemStatus as CorePlanItemStatus,
    RuntimeError, TestStatus as CoreTestStatus,
};
use coding_agent_domain::{
    ActivityEntry, ActivityLevel, CanonicalPath, DiffFile, DiffFileStatus, DiffSnapshot, PlanItem,
    PlanItemStatus, PlanSnapshot, Repository, TaskFailure, TestCase, TestSnapshot, TestStatus,
    UtcTimestamp,
};
use coding_agent_provider::ChatCompletionsClient;
use coding_agent_runtime::{
    ProvisionedWorktree, RuntimeSession, RuntimeSessionLimits, ToolchainPaths,
    WorktreeArtifactState, WorktreeError, WorktreeIdentity, WorktreeProvisioner,
    WorktreeReservation,
};
use coding_agent_store::{
    AttemptArtifactIdentity, AttemptArtifactState, ReserveAttemptArtifact, StoreError,
};
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;

use crate::{
    RunContext, RunnerEvent, RunnerEventError, RunnerEventSink, RunnerOutcome, StoreWriterError,
    StoreWriterHandle, TaskRunner, WallClock,
};

const DEFAULT_ARTIFACT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_DIFF_DEBOUNCE: Duration = Duration::from_millis(100);
const MAX_ARTIFACT_WRITE_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_DIFF_DEBOUNCE: Duration = Duration::from_secs(10);
const ARTIFACT_STORE_FAILED: &str = "ARTIFACT_STORE_FAILED";
const EVENT_SINK_REJECTED: &str = "CODING_RUNNER_EVENT_REJECTED";
const WORKTREE_STATE_INCONSISTENT: &str = "WORKTREE_STATE_INCONSISTENT";
const COMMAND_CANCELLED: &str = "COMMAND_CANCELLED";
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
}

impl CodingAttemptError {
    pub fn new(code: impl Into<String>, retryable: bool) -> Self {
        Self {
            code: code.into(),
            retryable,
        }
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub const fn retryable(&self) -> bool {
        self.retryable
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptArtifactObservation {
    Absent,
    Partial,
    Ready,
    Inconsistent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodingAttemptProvisionError {
    cause: CodingAttemptError,
    observation: AttemptArtifactObservation,
}

impl CodingAttemptProvisionError {
    pub fn new(cause: CodingAttemptError, observation: AttemptArtifactObservation) -> Self {
        Self { cause, observation }
    }

    pub const fn cause(&self) -> &CodingAttemptError {
        &self.cause
    }

    pub const fn observation(&self) -> AttemptArtifactObservation {
        self.observation
    }
}

#[derive(Clone)]
pub struct TaskAgentRuntime {
    runtime: Arc<dyn AgentRuntime>,
    repository_context: String,
}

impl TaskAgentRuntime {
    pub fn try_new(
        runtime: Arc<dyn AgentRuntime>,
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
            runtime,
            repository_context,
        })
    }

    pub fn runtime(&self) -> Arc<dyn AgentRuntime> {
        Arc::clone(&self.runtime)
    }

    pub fn repository_context(&self) -> &str {
        &self.repository_context
    }
}

/// One prepared task attempt. `prepare` must not perform Git or filesystem side
/// effects; the runner persists `reservation()` before calling `provision`.
#[async_trait::async_trait]
pub trait CodingAgentAttempt: Send + 'static {
    fn reservation(&self) -> &AttemptReservation;

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
        cancellation: CancellationToken,
    ) -> Result<Box<dyn CodingAgentAttempt>, CodingAttemptError>;
}

pub trait RepositoryWorktreeProvisionerFactory: Send + Sync + 'static {
    fn create(
        &self,
        repository: &Repository,
    ) -> Result<Arc<WorktreeProvisioner>, CodingAttemptError>;
}

impl<F> RepositoryWorktreeProvisionerFactory for F
where
    F: Fn(&Repository) -> Result<Arc<WorktreeProvisioner>, CodingAttemptError>
        + Send
        + Sync
        + 'static,
{
    fn create(
        &self,
        repository: &Repository,
    ) -> Result<Arc<WorktreeProvisioner>, CodingAttemptError> {
        self(repository)
    }
}

#[async_trait::async_trait]
pub trait ProvisionedAgentRuntimeFactory: Send + Sync + 'static {
    async fn create(
        &self,
        worktree: &ProvisionedWorktree,
        cancellation: CancellationToken,
    ) -> Result<TaskAgentRuntime, CodingAttemptError>;
}

pub struct Project2RuntimeSessionFactory {
    toolchain: ToolchainPaths,
    temporary_directory: PathBuf,
    limits: RuntimeSessionLimits,
}

impl Project2RuntimeSessionFactory {
    pub fn new(
        toolchain: ToolchainPaths,
        temporary_directory: impl Into<PathBuf>,
        limits: RuntimeSessionLimits,
    ) -> Self {
        Self {
            toolchain,
            temporary_directory: temporary_directory.into(),
            limits,
        }
    }

    pub fn project_2_defaults(
        toolchain: ToolchainPaths,
        temporary_directory: impl Into<PathBuf>,
    ) -> Self {
        Self::new(
            toolchain,
            temporary_directory,
            RuntimeSessionLimits::project_2_defaults(),
        )
    }
}

#[async_trait::async_trait]
impl ProvisionedAgentRuntimeFactory for Project2RuntimeSessionFactory {
    async fn create(
        &self,
        worktree: &ProvisionedWorktree,
        _cancellation: CancellationToken,
    ) -> Result<TaskAgentRuntime, CodingAttemptError> {
        let runtime = RuntimeSession::from_provisioned_worktree(
            worktree,
            &self.toolchain,
            &self.temporary_directory,
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
        cancellation: CancellationToken,
    ) -> Result<Box<dyn CodingAgentAttempt>, CodingAttemptError> {
        let provisioner = self.provisioners.create(&repository)?;
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
                let observation = match error.artifact_state() {
                    WorktreeArtifactState::Absent => AttemptArtifactObservation::Absent,
                    WorktreeArtifactState::Partial => AttemptArtifactObservation::Partial,
                    WorktreeArtifactState::Ready => {
                        match self
                            .provisioner
                            .open_ready(&ready_recovery, CancellationToken::new())
                            .await
                        {
                            Ok(worktree) => {
                                self.worktree = Some(worktree);
                                AttemptArtifactObservation::Ready
                            }
                            Err(_) => AttemptArtifactObservation::Inconsistent,
                        }
                    }
                    WorktreeArtifactState::Inconsistent => AttemptArtifactObservation::Inconsistent,
                };
                let cause = if observation == AttemptArtifactObservation::Inconsistent {
                    CodingAttemptError::new(WORKTREE_STATE_INCONSISTENT, false)
                } else {
                    worktree_attempt_error(error.cause())
                };
                Err(CodingAttemptProvisionError::new(cause, observation))
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
        self.runtime_factory.create(worktree, cancellation).await
    }
}

fn worktree_attempt_error(error: &WorktreeError) -> CodingAttemptError {
    let code = error.code();
    CodingAttemptError::new(
        code,
        matches!(
            code,
            "COMMAND_TIMED_OUT"
                | "WORKTREE_CREATE_FAILED"
                | "PROCESS_TREE_CLEANUP_FAILED"
                | "COMMAND_OUTPUT_LIMIT"
        ),
    )
}

pub struct TaskModelSession {
    provider: Arc<dyn ModelProvider>,
    redactor: Arc<dyn ContextRedactor>,
}

impl TaskModelSession {
    pub fn new(provider: Arc<dyn ModelProvider>, redactor: Arc<dyn ContextRedactor>) -> Self {
        Self { provider, redactor }
    }

    pub fn provider(&self) -> Arc<dyn ModelProvider> {
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

pub struct CodingAgentRunner {
    writer: StoreWriterHandle,
    providers: Arc<dyn TaskModelProviderFactory>,
    attempts: Arc<dyn CodingAgentAttemptFactory>,
    clock: Arc<dyn WallClock>,
    limits: AgentLimits,
    config: CodingAgentRunnerConfig,
}

impl CodingAgentRunner {
    pub fn new(
        writer: StoreWriterHandle,
        providers: Arc<dyn TaskModelProviderFactory>,
        attempts: Arc<dyn CodingAgentAttemptFactory>,
        clock: Arc<dyn WallClock>,
        limits: AgentLimits,
        config: CodingAgentRunnerConfig,
    ) -> Self {
        Self {
            writer,
            providers,
            attempts,
            clock,
            limits,
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
    ) -> Result<AttemptArtifactState, TaskFailure> {
        let receipt = match self
            .writer
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
        {
            Ok(receipt) => receipt,
            Err(error) => {
                if matches!(
                    &error,
                    StoreWriterError::Store(StoreError::ArtifactIdentityConflict)
                ) {
                    let _ = self
                        .mark_inconsistent(identity, WORKTREE_STATE_INCONSISTENT)
                        .await;
                }
                return Err(artifact_write_failure(&error));
            }
        };
        Ok(receipt.value.artifact().state)
    }

    async fn mark_ready(&self, identity: AttemptArtifactIdentity) -> Result<(), TaskFailure> {
        self.writer
            .mark_attempt_artifact_ready(identity, self.write_deadline())
            .await
            .map(|_| ())
            .map_err(|error| artifact_write_failure(&error))
    }

    async fn mark_inconsistent(
        &self,
        identity: AttemptArtifactIdentity,
        code: &str,
    ) -> Result<(), TaskFailure> {
        self.writer
            .mark_attempt_artifact_inconsistent(identity, safe_code(code), self.write_deadline())
            .await
            .map(|_| ())
            .map_err(|error| artifact_write_failure(&error))
    }

    async fn finish_ready_provision_error(
        &self,
        context: &RunContext,
        sink: RunnerEventSink,
        task_runtime: Option<TaskAgentRuntime>,
        cause: CodingAttemptError,
    ) -> RunnerOutcome {
        let Some(task_runtime) = task_runtime else {
            return outcome_for_attempt_error(cause, &context.cancellation);
        };
        let events = AppAgentEventSink::new(
            sink,
            Arc::clone(&self.clock),
            context.task.started_at.unwrap_or(context.task.created_at),
            self.config.diff_debounce(),
            context.cancellation.child_token(),
        );
        let terminal_diff = task_runtime
            .runtime()
            .terminal_snapshot(0, CancellationToken::new())
            .await
            .ok()
            .map(|snapshot| snapshot.diff);
        if events.finish(terminal_diff).await.is_err() {
            return event_failure_outcome(&context.cancellation);
        }
        outcome_for_attempt_error(cause, &context.cancellation)
    }
}

#[async_trait::async_trait]
impl TaskRunner for CodingAgentRunner {
    async fn run(&self, context: RunContext, sink: RunnerEventSink) -> RunnerOutcome {
        if context.cancellation.is_cancelled() {
            return RunnerOutcome::Cancelled;
        }
        let identity = match WorktreeIdentity::try_new(
            context.repository.id.to_string(),
            context.task.id.to_string(),
            context.task.attempt,
        ) {
            Ok(identity) => identity,
            Err(error) => return RunnerOutcome::Failed(stable_failure(error.code(), false)),
        };
        let mut attempt = match self
            .attempts
            .prepare(
                identity,
                context.repository.clone(),
                context.cancellation.clone(),
            )
            .await
        {
            Ok(attempt) => attempt,
            Err(error) => return outcome_for_attempt_error(error, &context.cancellation),
        };
        if context.cancellation.is_cancelled() {
            return RunnerOutcome::Cancelled;
        }

        let artifact_identity = AttemptArtifactIdentity {
            task_id: context.task.id,
            repository_id: context.repository.id,
            attempt: context.task.attempt,
        };
        let state = match self
            .reserve_artifact(artifact_identity, attempt.reservation())
            .await
        {
            Ok(state) => state,
            Err(failure) => return RunnerOutcome::Failed(failure),
        };
        if state != AttemptArtifactState::Reserved {
            return RunnerOutcome::Failed(stable_failure(WORKTREE_STATE_INCONSISTENT, false));
        }
        if context.cancellation.is_cancelled() {
            let _ = self
                .mark_inconsistent(artifact_identity, COMMAND_CANCELLED)
                .await;
            return RunnerOutcome::Cancelled;
        }

        if let Err(error) = attempt.provision(context.cancellation.clone()).await {
            let cause = error.cause().clone();
            let ready = error.observation() == AttemptArtifactObservation::Ready;
            let artifact_update = match error.observation() {
                AttemptArtifactObservation::Ready => self.mark_ready(artifact_identity).await,
                AttemptArtifactObservation::Absent => {
                    self.mark_inconsistent(artifact_identity, error.cause().code())
                        .await
                }
                AttemptArtifactObservation::Partial | AttemptArtifactObservation::Inconsistent => {
                    self.mark_inconsistent(artifact_identity, WORKTREE_STATE_INCONSISTENT)
                        .await
                }
            };
            if let Err(failure) = artifact_update {
                if context.cancellation.is_cancelled() {
                    return RunnerOutcome::Cancelled;
                }
                return RunnerOutcome::Failed(failure);
            }
            if ready {
                let task_runtime = attempt.runtime(CancellationToken::new()).await.ok();
                return self
                    .finish_ready_provision_error(&context, sink, task_runtime, cause)
                    .await;
            }
            return outcome_for_attempt_error(cause, &context.cancellation);
        }
        if let Err(failure) = self.mark_ready(artifact_identity).await {
            return RunnerOutcome::Failed(failure);
        }

        let loop_cancellation = context.cancellation.child_token();
        let events = AppAgentEventSink::new(
            sink,
            Arc::clone(&self.clock),
            context.task.started_at.unwrap_or(context.task.created_at),
            self.config.diff_debounce(),
            loop_cancellation.clone(),
        );
        if events
            .emit(AgentEvent::Activity(ActivityEvent {
                level: CoreActivityLevel::Info,
                message: "Prepared isolated worktree".to_owned(),
            }))
            .await
            .is_err()
        {
            return event_failure_outcome(&context.cancellation);
        }

        let task_runtime = match attempt.runtime(CancellationToken::new()).await {
            Ok(runtime) => runtime,
            Err(error) => return outcome_for_attempt_error(error, &context.cancellation),
        };
        let model_session = self.providers.start_task();
        let repository_context = format!(
            "{ISOLATION_CONTEXT}\n\n{}",
            task_runtime.repository_context()
        );
        let agent = AgentLoop::new(
            model_session.provider(),
            task_runtime.runtime(),
            events.clone(),
            model_session.redactor(),
            self.limits,
        );
        let outcome = agent
            .run(
                AgentInput::new(context.task.prompt, repository_context),
                loop_cancellation,
            )
            .await;
        let terminal_diff = match &outcome {
            AgentOutcome::Completed(completion) => Some(completion.terminal_snapshot.diff.clone()),
            AgentOutcome::Failed(failure) => failure
                .terminal_snapshot
                .as_ref()
                .map(|snapshot| snapshot.diff.clone()),
            AgentOutcome::Cancelled(cancellation) => cancellation
                .terminal_snapshot
                .as_ref()
                .map(|snapshot| snapshot.diff.clone()),
        };
        if events.finish(terminal_diff).await.is_err() {
            return event_failure_outcome(&context.cancellation);
        }
        if context.cancellation.is_cancelled() {
            return RunnerOutcome::Cancelled;
        }

        match outcome {
            AgentOutcome::Completed(_) => RunnerOutcome::Succeeded,
            AgentOutcome::Cancelled(_) => RunnerOutcome::Cancelled,
            AgentOutcome::Failed(failure) => {
                RunnerOutcome::Failed(stable_failure(&failure.code, failure.retryable))
            }
        }
    }
}

fn outcome_for_attempt_error(
    error: CodingAttemptError,
    cancellation: &CancellationToken,
) -> RunnerOutcome {
    if cancellation.is_cancelled() || error.code() == COMMAND_CANCELLED {
        RunnerOutcome::Cancelled
    } else {
        RunnerOutcome::Failed(stable_failure(error.code(), error.retryable()))
    }
}

fn event_failure_outcome(cancellation: &CancellationToken) -> RunnerOutcome {
    if cancellation.is_cancelled() {
        RunnerOutcome::Cancelled
    } else {
        RunnerOutcome::Failed(stable_failure(EVENT_SINK_REJECTED, true))
    }
}

fn artifact_write_failure(error: &StoreWriterError) -> TaskFailure {
    match error {
        StoreWriterError::Store(
            StoreError::ArtifactIdentityConflict | StoreError::ArtifactStateConflict,
        ) => stable_failure(WORKTREE_STATE_INCONSISTENT, false),
        StoreWriterError::Busy | StoreWriterError::Closed | StoreWriterError::Store(_) => {
            stable_failure(ARTIFACT_STORE_FAILED, true)
        }
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
    next_activity_id: AtomicU64,
}

impl EventProjection {
    fn new(clock: Arc<dyn WallClock>, fallback_time: UtcTimestamp) -> Self {
        Self {
            clock,
            fallback_time,
            next_activity_id: AtomicU64::new(1),
        }
    }

    fn map(&self, event: AgentEvent) -> RunnerEvent {
        match event {
            AgentEvent::Plan(plan) => RunnerEvent::PlanUpdated(PlanSnapshot {
                revision: plan.revision,
                items: plan
                    .items
                    .into_iter()
                    .map(|item| PlanItem {
                        id: item.id,
                        title: item.title,
                        status: match item.status {
                            CorePlanItemStatus::Pending => PlanItemStatus::Pending,
                            CorePlanItemStatus::Running => PlanItemStatus::Running,
                            CorePlanItemStatus::Completed => PlanItemStatus::Completed,
                        },
                    })
                    .collect(),
            }),
            AgentEvent::Activity(activity) => {
                let ordinal = self.next_activity_id.fetch_add(1, Ordering::Relaxed);
                let created_at =
                    UtcTimestamp::new(self.clock.now_utc()).unwrap_or(self.fallback_time);
                RunnerEvent::ActivityAppended(ActivityEntry {
                    id: format!("coding-agent-{ordinal}"),
                    level: match activity.level {
                        CoreActivityLevel::Info => ActivityLevel::Info,
                        CoreActivityLevel::Warning => ActivityLevel::Warning,
                        CoreActivityLevel::Error => ActivityLevel::Error,
                    },
                    message: activity.message,
                    created_at,
                })
            }
            AgentEvent::Diff(diff) => RunnerEvent::DiffUpdated(map_diff(diff)),
            AgentEvent::Tests(tests) => RunnerEvent::TestUpdated(TestSnapshot {
                revision: tests.revision,
                status: map_test_status(tests.status),
                cases: tests
                    .cases
                    .into_iter()
                    .map(|case| TestCase {
                        id: case.id,
                        name: case.name,
                        status: map_test_status(case.status),
                        duration_ms: case.duration_ms.unwrap_or(0),
                        summary: case.summary,
                    })
                    .collect(),
            }),
        }
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

const fn map_test_status(status: CoreTestStatus) -> TestStatus {
    match status {
        CoreTestStatus::Queued => TestStatus::Queued,
        CoreTestStatus::Running => TestStatus::Running,
        CoreTestStatus::Passed => TestStatus::Passed,
        CoreTestStatus::Failed => TestStatus::Failed,
        CoreTestStatus::Cancelled => TestStatus::Cancelled,
    }
}

#[derive(Default)]
struct EventState {
    pending_diff: Option<DiffEvent>,
    last_diff: Option<DiffEvent>,
    generation: u64,
    timer_armed: bool,
    closed: bool,
    failure: Option<RunnerEventError>,
}

struct AppAgentEventSink {
    sink: Arc<RunnerEventSink>,
    projection: EventProjection,
    debounce: Duration,
    loop_cancellation: CancellationToken,
    state: Mutex<EventState>,
    delivery: Mutex<()>,
    self_weak: Weak<Self>,
}

impl AppAgentEventSink {
    fn new(
        sink: RunnerEventSink,
        clock: Arc<dyn WallClock>,
        fallback_time: UtcTimestamp,
        debounce: Duration,
        loop_cancellation: CancellationToken,
    ) -> Arc<Self> {
        Arc::new_cyclic(|self_weak| Self {
            sink: Arc::new(sink),
            projection: EventProjection::new(clock, fallback_time),
            debounce,
            loop_cancellation,
            state: Mutex::new(EventState::default()),
            delivery: Mutex::new(()),
            self_weak: self_weak.clone(),
        })
    }

    async fn queue_diff(&self, diff: DiffEvent) -> Result<(), RuntimeError> {
        let generation = {
            let mut state = self.state.lock().await;
            if let Some(error) = state.failure {
                return Err(runtime_sink_error(error));
            }
            if state.closed {
                return Err(runtime_sink_error(RunnerEventError::TaskNotRunning));
            }
            state.pending_diff = Some(diff);
            if state.timer_armed {
                return Ok(());
            }
            state.timer_armed = true;
            state.generation = state.generation.wrapping_add(1);
            state.generation
        };
        let delay = self.debounce;
        let weak = self.self_weak.clone();
        tokio::spawn(async move {
            sleep(delay).await;
            if let Some(events) = weak.upgrade() {
                events.flush_generation(generation).await;
            }
        });
        Ok(())
    }

    async fn flush_generation(&self, generation: u64) {
        let _delivery = self.delivery.lock().await;
        let diff = {
            let mut state = self.state.lock().await;
            if state.closed
                || state.failure.is_some()
                || !state.timer_armed
                || state.generation != generation
            {
                return;
            }
            state.timer_armed = false;
            state.pending_diff.take()
        };
        if let Some(diff) = diff {
            self.deliver_diff(diff).await;
        }
    }

    async fn deliver_diff(&self, diff: DiffEvent) {
        if let Err(error) = self
            .sink
            .append(RunnerEvent::DiffUpdated(map_diff(diff.clone())))
            .await
        {
            self.record_failure(error).await;
            return;
        }
        self.state.lock().await.last_diff = Some(diff);
    }

    async fn record_failure(&self, error: RunnerEventError) {
        let mut state = self.state.lock().await;
        state.failure.get_or_insert(error);
        self.loop_cancellation.cancel();
    }

    async fn deliver(&self, event: AgentEvent) -> Result<(), RuntimeError> {
        let _delivery = self.delivery.lock().await;
        {
            let state = self.state.lock().await;
            if let Some(error) = state.failure {
                return Err(runtime_sink_error(error));
            }
            if state.closed {
                return Err(runtime_sink_error(RunnerEventError::TaskNotRunning));
            }
        }
        if let Err(error) = self.sink.append(self.projection.map(event)).await {
            self.record_failure(error).await;
            return Err(runtime_sink_error(error));
        }
        Ok(())
    }

    async fn finish(&self, terminal_diff: Option<DiffEvent>) -> Result<(), RuntimeError> {
        let _delivery = self.delivery.lock().await;
        let diff = {
            let mut state = self.state.lock().await;
            state.closed = true;
            state.timer_armed = false;
            state.generation = state.generation.wrapping_add(1);
            if let Some(error) = state.failure {
                return Err(runtime_sink_error(error));
            }
            terminal_diff.or_else(|| state.pending_diff.take())
        };
        if let Some(diff) = diff {
            let duplicate = self.state.lock().await.last_diff.as_ref() == Some(&diff);
            if !duplicate {
                if let Err(error) = self
                    .sink
                    .append(RunnerEvent::DiffUpdated(map_diff(diff.clone())))
                    .await
                {
                    self.record_failure(error).await;
                    return Err(runtime_sink_error(error));
                }
                self.state.lock().await.last_diff = Some(diff);
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl AgentEventSink for AppAgentEventSink {
    async fn emit(&self, event: AgentEvent) -> Result<(), RuntimeError> {
        match event {
            AgentEvent::Diff(diff) => self.queue_diff(diff).await,
            other => self.deliver(other).await,
        }
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

    use coding_agent_core::{
        AgentEvent, DiffEvent, DiffFile as CoreDiffFile, DiffFileStatus as CoreDiffFileStatus,
        PlanEvent, PlanItem as CorePlanItem, PlanItemStatus as CorePlanItemStatus,
        TestCase as CoreTestCase, TestEvent, TestStatus as CoreTestStatus,
    };
    use coding_agent_domain::{DiffFileStatus, PlanItemStatus, TestStatus, UtcTimestamp};

    use super::{CodingAgentRunnerConfig, EventProjection};
    use crate::WallClock;

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
    fn neutral_events_map_without_losing_diff_or_test_metadata() {
        let projection = EventProjection::new(Arc::new(FixedClock), timestamp());
        let plan = projection.map(AgentEvent::Plan(PlanEvent {
            revision: 4,
            items: vec![CorePlanItem {
                id: "inspect".to_owned(),
                title: "Inspect".to_owned(),
                status: CorePlanItemStatus::Completed,
            }],
        }));
        let crate::RunnerEvent::PlanUpdated(plan) = plan else {
            panic!("map plan event")
        };
        assert_eq!(plan.items[0].status, PlanItemStatus::Completed);

        let diff = projection.map(AgentEvent::Diff(DiffEvent {
            revision: 5,
            files: vec![CoreDiffFile {
                path: "src/lib.rs".to_owned(),
                status: CoreDiffFileStatus::Modified,
                patch: "+change".to_owned(),
                additions: 1,
                deletions: 0,
                truncated: true,
            }],
        }));
        let crate::RunnerEvent::DiffUpdated(diff) = diff else {
            panic!("map diff event")
        };
        assert_eq!(diff.files[0].status, DiffFileStatus::Modified);
        assert!(diff.files[0].truncated);
        assert_eq!(diff.files[0].patch, "+change");

        let tests = projection.map(AgentEvent::Tests(TestEvent {
            revision: 6,
            status: CoreTestStatus::Running,
            cases: vec![CoreTestCase {
                id: "test".to_owned(),
                name: "test".to_owned(),
                status: CoreTestStatus::Running,
                duration_ms: None,
                summary: "running".to_owned(),
            }],
        }));
        let crate::RunnerEvent::TestUpdated(tests) = tests else {
            panic!("map tests event")
        };
        assert_eq!(tests.status, TestStatus::Running);
        assert_eq!(tests.cases[0].duration_ms, 0);
    }
}
