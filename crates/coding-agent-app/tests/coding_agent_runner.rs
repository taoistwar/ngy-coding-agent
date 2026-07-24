mod support;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coding_agent_app::{
    AttemptArtifactObservation, AttemptReservation, CodingAgentAttempt, CodingAgentAttemptFactory,
    CodingAgentRunner, CodingAgentRunnerConfig, CodingAttemptError, CodingAttemptProvisionError,
    EventDispatcherHandle, QuiesceResult, ServiceState, ServiceStateController, StoreWriterHandle,
    SystemWallClock, TaskAgentRuntime, TaskManagerHandle, TaskModelProviderFactory,
    TaskModelSession, TestTaskRuntimeSession,
};
use coding_agent_core::{
    ActionRequest, ContextRedactor, DiffEvent, FinalizationGuard, FinalizationGuardError,
    ModelMessage, ModelRequest, ModelResponse, PreparedModelProvider, PreparedProviderRequest,
    ProviderError, RawProviderResponse, ReviewDiffBundle, ReviewDiffCheckpoint, ReviewDiffManifest,
    Role, RoleActionRuntime, RoleEngine, RoleEngineFactory, RoleEventSink, RoleRun,
    RoleRuntimeResult, RuntimeActionRequest, RuntimeError, TerminalSnapshot, ToolCall,
    ToolCallBatch, ToolRequest, ToolResult, ValidationObservation, WorkspaceCheckpoint,
    WorkspaceFingerprint,
};
use coding_agent_domain::{
    ActivityActor, CanonicalPath, CheckEvidenceStatus, DeliveryReadiness, EventCursor,
    RequiredCheckSelector, ReviewVerdict, Task, TaskEventKind, TaskEventPayload, TaskId,
    TaskStatus, TestStatus,
};
use coding_agent_runtime::WorktreeIdentity;
use coding_agent_store::{AttemptArtifactState, Store};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const SECRET: &str = "provider-key-123456";

#[derive(Clone, Copy)]
enum ProviderMode {
    SuccessfulChange,
    TerminalCaptureFailure,
    TerminalCaptureTimeout,
    Blocking,
    UnsafeFailure,
}

struct ScriptedProviderFactory {
    mode: ProviderMode,
    starts: AtomicUsize,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    entered: Arc<Notify>,
}

impl ScriptedProviderFactory {
    fn new(mode: ProviderMode) -> Self {
        Self {
            mode,
            starts: AtomicUsize::new(0),
            requests: Arc::new(Mutex::new(Vec::new())),
            entered: Arc::new(Notify::new()),
        }
    }
}

impl TaskModelProviderFactory for ScriptedProviderFactory {
    fn start_task(&self) -> TaskModelSession {
        self.starts.fetch_add(1, Ordering::SeqCst);
        let responses = match self.mode {
            ProviderMode::SuccessfulChange
            | ProviderMode::TerminalCaptureFailure
            | ProviderMode::TerminalCaptureTimeout => scenario_responses()
                .into_iter()
                .map(Ok)
                .collect::<VecDeque<_>>(),
            ProviderMode::Blocking | ProviderMode::UnsafeFailure => VecDeque::new(),
        };
        TaskModelSession::new(
            Arc::new(ScriptedProvider {
                mode: self.mode,
                responses: Mutex::new(responses),
                requests: Arc::clone(&self.requests),
                entered: Arc::clone(&self.entered),
            }),
            Arc::new(TestRedactor),
        )
    }
}

struct ScriptedProvider {
    mode: ProviderMode,
    responses: Mutex<VecDeque<Result<ModelResponse, ProviderError>>>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    entered: Arc<Notify>,
}

struct ScriptedPreparedRequest {
    response: Option<ModelResponse>,
    blocks: bool,
}

#[async_trait::async_trait]
impl PreparedProviderRequest for ScriptedPreparedRequest {
    fn encoded_len(&self) -> usize {
        512
    }

    fn maximum_response_bytes(&self) -> usize {
        64 * 1024
    }

    async fn send(
        self: Box<Self>,
        cancellation: CancellationToken,
    ) -> Result<Box<dyn RawProviderResponse>, ProviderError> {
        if self.blocks {
            cancellation.cancelled().await;
            return Err(ProviderError::new(
                "PROVIDER_CANCELLED",
                "provider request cancelled",
                false,
            ));
        }
        Ok(Box::new(ScriptedRawResponse {
            response: self.response.expect("scripted response"),
        }))
    }
}

struct ScriptedRawResponse {
    response: ModelResponse,
}

impl RawProviderResponse for ScriptedRawResponse {
    fn encoded_len(&self) -> usize {
        256
    }

    fn decode(self: Box<Self>) -> Result<ModelResponse, ProviderError> {
        Ok(self.response)
    }
}

impl PreparedModelProvider for ScriptedProvider {
    fn prepare(
        &self,
        request: ModelRequest,
    ) -> Result<Box<dyn PreparedProviderRequest>, ProviderError> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request);
        self.entered.notify_one();
        match self.mode {
            ProviderMode::SuccessfulChange
            | ProviderMode::TerminalCaptureFailure
            | ProviderMode::TerminalCaptureTimeout => {
                let response = self
                    .responses
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .pop_front()
                    .expect("one scripted response per provider request")?;
                Ok(Box::new(ScriptedPreparedRequest {
                    response: Some(response),
                    blocks: false,
                }))
            }
            ProviderMode::Blocking => Ok(Box::new(ScriptedPreparedRequest {
                response: None,
                blocks: true,
            })),
            ProviderMode::UnsafeFailure => Err(ProviderError::new(
                "unsafe/code",
                format!("must not persist {SECRET}"),
                false,
            )),
        }
    }
}

struct TestRedactor;

impl ContextRedactor for TestRedactor {
    fn redact(&self, content: &str) -> String {
        content.replace(SECRET, "<redacted>")
    }
}

fn batch(calls: Vec<ToolCall>) -> ModelResponse {
    ModelResponse::ToolCalls(ToolCallBatch {
        assistant_content: None,
        reasoning_content: None,
        calls,
    })
}

fn plan_call() -> ToolCall {
    ToolCall {
        id: "plan".to_owned(),
        request: ActionRequest::decode(
            Role::Planner,
            "submit_plan",
            r#"{
                "summary":"Change and validate the demo",
                "steps":[{
                    "title":"Implement the change",
                    "description":"Update the demo and validate it.",
                    "acceptance_criteria":["The required test passes."]
                }],
                "initial_required_checks":[{
                    "kind":"cargo_test",
                    "package":"demo"
                }]
            }"#,
        )
        .unwrap(),
    }
}

fn executor_progress_call() -> ToolCall {
    ToolCall {
        id: "executor-progress".to_owned(),
        request: ActionRequest::decode(
            Role::Executor,
            "update_plan_progress",
            r#"{"updates":[{"step_id":"step-01","status":"completed"}]}"#,
        )
        .unwrap(),
    }
}

fn executor_validation_call() -> ToolCall {
    ToolCall {
        id: "executor-validation".to_owned(),
        request: ActionRequest::decode(Role::Executor, "cargo_test", r#"{"package":"demo"}"#)
            .unwrap(),
    }
}

fn executor_submission_call() -> ToolCall {
    ToolCall {
        id: "executor-submit".to_owned(),
        request: ActionRequest::decode(
            Role::Executor,
            "submit_execution",
            r#"{"summary":"the demo is ready"}"#,
        )
        .unwrap(),
    }
}

fn read_call(id: impl Into<String>) -> ToolCall {
    ToolCall::runtime(
        id,
        ToolRequest::ReadFile {
            path: "src/lib.rs".to_owned(),
            start_line: 1,
            end_line: 4,
        },
    )
}

fn reviewer_manifest_call(checkpoint: &WorkspaceCheckpoint) -> ToolCall {
    ToolCall {
        id: "reviewer-manifest".to_owned(),
        request: ActionRequest::Runtime(RuntimeActionRequest::ReviewDiffManifest {
            generation: checkpoint.generation(),
            workspace_digest: checkpoint.workspace_digest(),
        }),
    }
}

fn reviewer_approval_call() -> ToolCall {
    ToolCall {
        id: "reviewer-approved".to_owned(),
        request: ActionRequest::decode(
            Role::Reviewer,
            "submit_review",
            r#"{
                "verdict":"approved",
                "summary":"the validated demo is approved",
                "findings":[],
                "add_required_checks":[]
            }"#,
        )
        .unwrap(),
    }
}

fn reviewer_changes_call() -> ToolCall {
    ToolCall {
        id: "reviewer-changes".to_owned(),
        request: ActionRequest::decode(
            Role::Reviewer,
            "submit_review",
            r#"{
                "verdict":"changes_requested",
                "summary":"one correction is required",
                "findings":[{
                    "severity":"blocking",
                    "message":"make one bounded correction",
                    "path":"src/lib.rs",
                    "line":1
                }],
                "add_required_checks":[]
            }"#,
        )
        .unwrap(),
    }
}

fn scenario_responses() -> Vec<ModelResponse> {
    let checkpoint = WorkspaceCheckpoint::new(WorkspaceFingerprint::from_bytes([0; 32]));
    let mut responses = vec![
        batch(vec![plan_call()]),
        batch(vec![executor_progress_call()]),
        batch(vec![executor_validation_call()]),
        batch(vec![executor_submission_call()]),
        batch(vec![reviewer_changes_call()]),
        batch(vec![ToolCall {
            id: "executor-submit-round-2".to_owned(),
            request: ActionRequest::decode(
                Role::Executor,
                "submit_execution",
                r#"{"summary":"the requested correction is ready"}"#,
            )
            .unwrap(),
        }]),
    ];
    responses.extend((1..=3).map(|index| batch(vec![read_call(format!("reviewer-read-{index}"))])));
    responses.push(batch(
        (0..8)
            .map(|index| read_call(format!("reviewer-reserved-read-{index}")))
            .collect(),
    ));
    responses.push(batch(vec![reviewer_manifest_call(&checkpoint)]));
    responses.push(batch(vec![reviewer_approval_call()]));
    responses
}

#[derive(Clone, Copy)]
enum ProvisionMode {
    Ready,
    ReadyFailureCaptureTimeout,
    PartialFailure,
}

struct ScriptedAttemptFactory {
    mode: ProvisionMode,
    runtime: Arc<ScriptedRuntime>,
    prepared: AtomicUsize,
    provisioned: Arc<AtomicUsize>,
}

impl ScriptedAttemptFactory {
    fn new(mode: ProvisionMode, runtime: Arc<ScriptedRuntime>) -> Self {
        Self {
            mode,
            runtime,
            prepared: AtomicUsize::new(0),
            provisioned: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait::async_trait]
impl CodingAgentAttemptFactory for ScriptedAttemptFactory {
    async fn prepare(
        &self,
        identity: WorktreeIdentity,
        repository: coding_agent_domain::Repository,
        cancellation: CancellationToken,
    ) -> Result<Box<dyn CodingAgentAttempt>, CodingAttemptError> {
        if cancellation.is_cancelled() {
            return Err(CodingAttemptError::new("COMMAND_CANCELLED", false));
        }
        self.prepared.fetch_add(1, Ordering::SeqCst);
        let path = repository
            .git_root
            .as_path()
            .join("scripted-worktrees")
            .join(identity.task_id())
            .join(identity.attempt().to_string());
        Ok(Box::new(ScriptedAttempt {
            mode: self.mode,
            reservation: AttemptReservation::new(
                "0123456789abcdef0123456789abcdef01234567",
                identity.branch_name(),
                CanonicalPath::try_from_canonical(path).unwrap(),
            ),
            runtime: Arc::clone(&self.runtime),
            provisioned: Arc::clone(&self.provisioned),
            ready: AtomicBool::new(false),
        }))
    }
}

struct ScriptedAttempt {
    mode: ProvisionMode,
    reservation: AttemptReservation,
    runtime: Arc<ScriptedRuntime>,
    provisioned: Arc<AtomicUsize>,
    ready: AtomicBool,
}

#[async_trait::async_trait]
impl CodingAgentAttempt for ScriptedAttempt {
    fn reservation(&self) -> &AttemptReservation {
        &self.reservation
    }

    async fn provision(
        &mut self,
        _cancellation: CancellationToken,
    ) -> Result<(), CodingAttemptProvisionError> {
        self.provisioned.fetch_add(1, Ordering::SeqCst);
        match self.mode {
            ProvisionMode::Ready => {
                self.ready.store(true, Ordering::Release);
                Ok(())
            }
            ProvisionMode::ReadyFailureCaptureTimeout => {
                self.ready.store(true, Ordering::Release);
                Err(CodingAttemptProvisionError::new(
                    CodingAttemptError::new("WORKTREE_CREATE_FAILED", true),
                    AttemptArtifactObservation::Ready,
                ))
            }
            ProvisionMode::PartialFailure => Err(CodingAttemptProvisionError::new(
                CodingAttemptError::new("WORKTREE_CREATE_FAILED", true),
                AttemptArtifactObservation::Partial,
            )),
        }
    }

    async fn runtime(
        &self,
        _cancellation: CancellationToken,
    ) -> Result<TaskAgentRuntime, CodingAttemptError> {
        if !self.ready.load(Ordering::Acquire) {
            return Err(CodingAttemptError::new(
                "WORKTREE_STATE_INCONSISTENT",
                false,
            ));
        }
        TaskAgentRuntime::try_for_test(
            self.runtime.clone(),
            "Cargo package demo; targets: library; validation selector: package demo",
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EngineFactoryCall {
    role_run: RoleRun,
    review_checkpoint: Option<ReviewDiffCheckpoint>,
    scope_id: usize,
}

#[derive(Clone, Copy)]
enum TerminalCaptureMode {
    Success,
    AlwaysFailure,
    FailureThenTimeout,
    AlwaysTimeout,
}

struct ScriptedRuntimeState {
    workspace_version: AtomicU8,
    terminal_calls: AtomicUsize,
    terminal_cancellations: AtomicUsize,
    terminal_capture_mode: TerminalCaptureMode,
    engine_calls: Mutex<Vec<EngineFactoryCall>>,
    next_scope_id: AtomicUsize,
    finalization_calls: AtomicUsize,
    mutate_during_finalization: AtomicBool,
    finalization_expected_fingerprints: Mutex<Vec<WorkspaceFingerprint>>,
}

struct ScriptedRuntime {
    state: Arc<ScriptedRuntimeState>,
}

impl ScriptedRuntime {
    fn new(terminal_capture_mode: TerminalCaptureMode) -> Self {
        Self {
            state: Arc::new(ScriptedRuntimeState {
                workspace_version: AtomicU8::new(0),
                terminal_calls: AtomicUsize::new(0),
                terminal_cancellations: AtomicUsize::new(0),
                terminal_capture_mode,
                engine_calls: Mutex::new(Vec::new()),
                next_scope_id: AtomicUsize::new(1),
                finalization_calls: AtomicUsize::new(0),
                mutate_during_finalization: AtomicBool::new(false),
                finalization_expected_fingerprints: Mutex::new(Vec::new()),
            }),
        }
    }

    fn fingerprint(&self) -> WorkspaceFingerprint {
        WorkspaceFingerprint::from_bytes([self.state.workspace_version.load(Ordering::Acquire); 32])
    }

    fn terminal_calls(&self) -> usize {
        self.state.terminal_calls.load(Ordering::SeqCst)
    }

    fn terminal_cancellations(&self) -> usize {
        self.state.terminal_cancellations.load(Ordering::SeqCst)
    }

    fn engine_calls(&self) -> Vec<EngineFactoryCall> {
        self.state
            .engine_calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn mutate_workspace_during_finalization(&self) {
        self.state
            .mutate_during_finalization
            .store(true, Ordering::Release);
    }

    fn finalization_expected_fingerprints(&self) -> Vec<WorkspaceFingerprint> {
        self.state
            .finalization_expected_fingerprints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

struct ScriptedRoleRuntime {
    state: Arc<ScriptedRuntimeState>,
    role_run: RoleRun,
    review_checkpoint: Option<ReviewDiffCheckpoint>,
    scope_id: usize,
}

impl ScriptedRoleRuntime {
    fn fingerprint(&self) -> WorkspaceFingerprint {
        WorkspaceFingerprint::from_bytes([self.state.workspace_version.load(Ordering::Acquire); 32])
    }

    fn manifest(&self, checkpoint: &ReviewDiffCheckpoint) -> ReviewDiffManifest {
        ReviewDiffBundle::try_new(checkpoint, Vec::new(), &TestRedactor)
            .unwrap()
            .manifest()
            .clone()
    }

    async fn terminal_snapshot_result(
        &self,
        revision: u64,
        cancellation: CancellationToken,
    ) -> Result<TerminalSnapshot, RuntimeError> {
        let call = self.state.terminal_calls.fetch_add(1, Ordering::SeqCst);
        let should_fail = matches!(
            self.state.terminal_capture_mode,
            TerminalCaptureMode::AlwaysFailure
        ) || matches!(
            self.state.terminal_capture_mode,
            TerminalCaptureMode::FailureThenTimeout
        ) && call == 0;
        if should_fail {
            return Err(scripted_terminal_capture_failure());
        }
        let should_timeout = matches!(
            self.state.terminal_capture_mode,
            TerminalCaptureMode::AlwaysTimeout
        ) || matches!(
            self.state.terminal_capture_mode,
            TerminalCaptureMode::FailureThenTimeout
        ) && call > 0;
        if should_timeout {
            let state = Arc::clone(&self.state);
            let cancellation_observer = cancellation.clone();
            tokio::spawn(async move {
                cancellation_observer.cancelled().await;
                state.terminal_cancellations.fetch_add(1, Ordering::SeqCst);
            });
            return std::future::pending::<Result<TerminalSnapshot, RuntimeError>>().await;
        }
        Ok(TerminalSnapshot {
            fingerprint: self.fingerprint(),
            diff: DiffEvent {
                revision,
                files: Vec::new(),
            },
        })
    }
}

fn scripted_terminal_capture_failure() -> RuntimeError {
    RuntimeError::new(
        "SCRIPTED_TERMINAL_CAPTURE_FAILED",
        "the scripted terminal snapshot failed",
        true,
    )
}

#[async_trait::async_trait]
impl RoleActionRuntime for ScriptedRoleRuntime {
    async fn invoke(
        &self,
        request: RuntimeActionRequest,
        cancellation: CancellationToken,
    ) -> Result<RoleRuntimeResult, RuntimeError> {
        if cancellation.is_cancelled() {
            return Err(RuntimeError::new(
                "COMMAND_CANCELLED",
                "scripted runtime cancelled",
                false,
            ));
        }
        match request {
            RuntimeActionRequest::Validation { check } => Ok(RoleRuntimeResult::Validation(
                ValidationObservation::try_new(
                    ToolResult::text("passed"),
                    check,
                    CheckEvidenceStatus::Passed,
                    1,
                    false,
                )
                .unwrap(),
            )),
            RuntimeActionRequest::ReviewDiffManifest {
                generation,
                workspace_digest,
            } => {
                let checkpoint = self.review_checkpoint.as_ref().ok_or_else(|| {
                    RuntimeError::new(
                        "SCRIPTED_REVIEW_SCOPE_MISSING",
                        "review checkpoint authority is missing",
                        false,
                    )
                })?;
                if checkpoint.generation() != generation
                    || checkpoint.workspace_digest() != &workspace_digest
                {
                    return Err(RuntimeError::new(
                        "SCRIPTED_REVIEW_SCOPE_MISMATCH",
                        "review checkpoint authority did not match",
                        false,
                    ));
                }
                Ok(RoleRuntimeResult::ReviewDiffManifest(
                    self.manifest(checkpoint),
                ))
            }
            RuntimeActionRequest::Tool(_) => {
                Ok(RoleRuntimeResult::Tool(ToolResult::text(format!(
                    "ok {SECRET}; role={:?}; run={}; scope={}",
                    self.role_run.role(),
                    self.role_run.role_run(),
                    self.scope_id
                ))))
            }
            RuntimeActionRequest::ValidationSelector { .. } => Err(RuntimeError::new(
                "UNRESOLVED_VALIDATION_SELECTOR",
                "core must resolve validation selectors",
                false,
            )),
            RuntimeActionRequest::ReviewDiffChunks { .. } => Err(RuntimeError::new(
                "UNEXPECTED_REVIEW_CHUNKS",
                "the empty scripted review diff has no chunks",
                false,
            )),
        }
    }

    async fn workspace_fingerprint(
        &self,
        cancellation: CancellationToken,
    ) -> Result<WorkspaceFingerprint, RuntimeError> {
        if cancellation.is_cancelled() {
            return Err(RuntimeError::new(
                "COMMAND_CANCELLED",
                "scripted runtime cancelled",
                false,
            ));
        }
        Ok(self.fingerprint())
    }

    async fn terminal_snapshot(
        &self,
        revision: u64,
        cancellation: CancellationToken,
    ) -> Result<TerminalSnapshot, RuntimeError> {
        if cancellation.is_cancelled() {
            return Err(RuntimeError::new(
                "COMMAND_CANCELLED",
                "scripted runtime cancelled",
                false,
            ));
        }
        self.terminal_snapshot_result(revision, cancellation).await
    }

    async fn terminal_review_diff_manifest(
        &self,
        checkpoint: ReviewDiffCheckpoint,
        cancellation: CancellationToken,
    ) -> Result<ReviewDiffManifest, RuntimeError> {
        if cancellation.is_cancelled() {
            return Err(RuntimeError::new(
                "COMMAND_CANCELLED",
                "scripted runtime cancelled",
                false,
            ));
        }
        let scoped = self.review_checkpoint.as_ref().ok_or_else(|| {
            RuntimeError::new(
                "SCRIPTED_REVIEW_SCOPE_MISSING",
                "review checkpoint authority is missing",
                false,
            )
        })?;
        if scoped != &checkpoint {
            return Err(RuntimeError::new(
                "SCRIPTED_REVIEW_SCOPE_MISMATCH",
                "review checkpoint authority did not match",
                false,
            ));
        }
        Ok(self.manifest(&checkpoint))
    }
}

struct RecordingRoleEngineFactory {
    state: Arc<ScriptedRuntimeState>,
    provider: Arc<dyn PreparedModelProvider>,
    events: Arc<dyn RoleEventSink>,
    redactor: Arc<dyn ContextRedactor>,
}

impl RoleEngineFactory for RecordingRoleEngineFactory {
    fn create_engine(
        &self,
        role_run: RoleRun,
        review_checkpoint: Option<ReviewDiffCheckpoint>,
    ) -> Result<RoleEngine, RuntimeError> {
        let scope_is_exact = match role_run.role() {
            Role::Planner | Role::Executor => review_checkpoint.is_none(),
            Role::Reviewer => review_checkpoint.is_some(),
        };
        if !scope_is_exact {
            return Err(RuntimeError::new(
                "SCRIPTED_FACTORY_SCOPE_MISMATCH",
                "the role received the wrong checkpoint authority",
                false,
            ));
        }
        let scope_id = self.state.next_scope_id.fetch_add(1, Ordering::SeqCst);
        self.state
            .engine_calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(EngineFactoryCall {
                role_run,
                review_checkpoint: review_checkpoint.clone(),
                scope_id,
            });
        let runtime = Arc::new(ScriptedRoleRuntime {
            state: Arc::clone(&self.state),
            role_run,
            review_checkpoint,
            scope_id,
        });
        Ok(RoleEngine::new(
            Arc::clone(&self.provider),
            runtime,
            Arc::clone(&self.events),
            Arc::clone(&self.redactor),
        ))
    }
}

struct ScriptedFinalizationGuard {
    state: Arc<ScriptedRuntimeState>,
}

#[async_trait::async_trait]
impl FinalizationGuard for ScriptedFinalizationGuard {
    async fn verify_finalization(
        &self,
        expected_fingerprint: WorkspaceFingerprint,
        cancellation: CancellationToken,
    ) -> Result<(), FinalizationGuardError> {
        if cancellation.is_cancelled() {
            return Err(FinalizationGuardError::Runtime(RuntimeError::new(
                "COMMAND_CANCELLED",
                "scripted finalization cancelled",
                false,
            )));
        }
        self.state.finalization_calls.fetch_add(1, Ordering::SeqCst);
        self.state
            .finalization_expected_fingerprints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(expected_fingerprint);
        if self
            .state
            .mutate_during_finalization
            .swap(false, Ordering::AcqRel)
        {
            self.state.workspace_version.fetch_add(1, Ordering::AcqRel);
        }
        let actual_fingerprint = WorkspaceFingerprint::from_bytes(
            [self.state.workspace_version.load(Ordering::Acquire); 32],
        );
        if actual_fingerprint != expected_fingerprint {
            return Err(FinalizationGuardError::WorkspaceMismatch);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl TestTaskRuntimeSession for ScriptedRuntime {
    fn create_role_engine_factory(
        &self,
        provider: Arc<dyn PreparedModelProvider>,
        events: Arc<dyn RoleEventSink>,
        redactor: Arc<dyn ContextRedactor>,
    ) -> Arc<dyn RoleEngineFactory> {
        Arc::new(RecordingRoleEngineFactory {
            state: Arc::clone(&self.state),
            provider,
            events,
            redactor,
        })
    }

    fn finalization_guard(&self) -> Arc<dyn FinalizationGuard> {
        Arc::new(ScriptedFinalizationGuard {
            state: Arc::clone(&self.state),
        })
    }

    async fn workspace_fingerprint(
        &self,
        cancellation: CancellationToken,
    ) -> Result<WorkspaceFingerprint, RuntimeError> {
        if cancellation.is_cancelled() {
            return Err(RuntimeError::new(
                "COMMAND_CANCELLED",
                "scripted runtime cancelled",
                false,
            ));
        }
        Ok(self.fingerprint())
    }

    async fn required_check_selectors(
        &self,
        cancellation: CancellationToken,
    ) -> Result<Vec<RequiredCheckSelector>, RuntimeError> {
        if cancellation.is_cancelled() {
            return Err(RuntimeError::new(
                "COMMAND_CANCELLED",
                "scripted runtime cancelled",
                false,
            ));
        }
        Ok(vec![
            RequiredCheckSelector::try_cargo_test(Some("demo".to_owned()), None).unwrap(),
        ])
    }

    async fn terminal_snapshot(
        &self,
        revision: u64,
        cancellation: CancellationToken,
    ) -> Result<TerminalSnapshot, RuntimeError> {
        let scoped = ScriptedRoleRuntime {
            state: Arc::clone(&self.state),
            role_run: RoleRun::try_new(Role::Executor, 1).unwrap(),
            review_checkpoint: None,
            scope_id: 0,
        };
        scoped.terminal_snapshot(revision, cancellation).await
    }
}

struct Fixture {
    base: support::StoreFixture,
    writer: StoreWriterHandle,
    manager: TaskManagerHandle,
    providers: Arc<ScriptedProviderFactory>,
    attempts: Arc<ScriptedAttemptFactory>,
    runtime: Arc<ScriptedRuntime>,
}

impl Fixture {
    async fn new(provider_mode: ProviderMode, provision_mode: ProvisionMode) -> Self {
        let base = support::store_fixture().await;
        let dispatcher = EventDispatcherHandle::spawn(base.store.clone(), 1_024)
            .await
            .unwrap();
        let writer = StoreWriterHandle::spawn(base.store.clone(), Arc::new(dispatcher.clone()), 64);
        let providers = Arc::new(ScriptedProviderFactory::new(provider_mode));
        let terminal_capture_mode = match (provider_mode, provision_mode) {
            (_, ProvisionMode::ReadyFailureCaptureTimeout) => TerminalCaptureMode::AlwaysTimeout,
            (ProviderMode::TerminalCaptureFailure, _) => TerminalCaptureMode::AlwaysFailure,
            (ProviderMode::TerminalCaptureTimeout, _) => TerminalCaptureMode::FailureThenTimeout,
            _ => TerminalCaptureMode::Success,
        };
        let runtime = Arc::new(ScriptedRuntime::new(terminal_capture_mode));
        let attempts = Arc::new(ScriptedAttemptFactory::new(
            provision_mode,
            Arc::clone(&runtime),
        ));
        let artifact_timeout = if matches!(
            terminal_capture_mode,
            TerminalCaptureMode::FailureThenTimeout | TerminalCaptureMode::AlwaysTimeout
        ) {
            Duration::from_millis(100)
        } else {
            Duration::from_secs(5)
        };
        let runner = Arc::new(CodingAgentRunner::new(
            writer.clone(),
            providers.clone(),
            attempts.clone(),
            Arc::new(SystemWallClock),
            CodingAgentRunnerConfig::try_new(artifact_timeout, Duration::from_secs(10)).unwrap(),
        ));
        let manager = TaskManagerHandle::spawn(
            base.store.clone(),
            writer.clone(),
            dispatcher,
            ServiceStateController::new(ServiceState::Ready),
            runner,
            1,
            64,
        );
        Self {
            base,
            writer,
            manager,
            providers,
            attempts,
            runtime,
        }
    }

    async fn enqueue(&self) -> Task {
        let task = self
            .writer
            .create_task(
                support::new_task(self.base.repository.id, "change the demo"),
                support::deadline(),
            )
            .await
            .unwrap()
            .value
            .task()
            .clone();
        self.manager.notify_queued(task.id).await.unwrap();
        task
    }

    async fn wait_for(&self, task_id: TaskId, status: TaskStatus) -> Task {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let task = self.detail(task_id).await.task;
                if task.status == status {
                    break task;
                }
                if matches!(
                    task.status,
                    TaskStatus::Completed
                        | TaskStatus::Failed
                        | TaskStatus::Cancelled
                        | TaskStatus::Interrupted
                ) {
                    panic!(
                        "task reached unexpected terminal state {:?}: {:?}",
                        task.status, task.failure
                    );
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }

    async fn detail(&self, task_id: TaskId) -> coding_agent_store::TaskDetail {
        self.base.store.task_detail(task_id).await.unwrap().unwrap()
    }

    async fn event_kinds(&self, task_id: TaskId) -> Vec<TaskEventKind> {
        self.base
            .store
            .task_events_after(task_id, EventCursor::ZERO, usize::MAX)
            .await
            .unwrap()
            .events
            .into_iter()
            .map(|event| event.payload.kind())
            .collect()
    }

    async fn test_statuses(&self, task_id: TaskId) -> Vec<TestStatus> {
        self.base
            .store
            .task_events_after(task_id, EventCursor::ZERO, usize::MAX)
            .await
            .unwrap()
            .events
            .into_iter()
            .filter_map(|event| match event.payload {
                TaskEventPayload::TestUpdated { tests } => Some(tests.status),
                _ => None,
            })
            .collect()
    }

    fn store(&self) -> &Store {
        &self.base.store
    }
}

#[tokio::test]
async fn runner_persists_ready_artifact_and_maps_live_and_terminal_panels() {
    let fixture = Fixture::new(ProviderMode::SuccessfulChange, ProvisionMode::Ready).await;
    let task = fixture.enqueue().await;
    fixture.wait_for(task.id, TaskStatus::Completed).await;

    let detail = fixture.detail(task.id).await;
    assert!(detail.plan.unwrap().revision() >= 2);
    assert!(detail.activity.len() >= 4);
    let activity_ids = detail
        .activity
        .iter()
        .map(|entry| entry.id())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(activity_ids.len(), detail.activity.len());
    assert!(detail.activity.iter().all(|entry| {
        entry
            .id()
            .starts_with(&format!("coding-agent-{}-", task.id))
    }));
    for actor in [
        ActivityActor::Planner,
        ActivityActor::Executor,
        ActivityActor::Reviewer,
    ] {
        assert!(detail.activity.iter().any(|entry| {
            entry.actor() == actor && entry.role_run().is_some_and(|role_run| role_run > 0)
        }));
    }
    assert_eq!(detail.reviews.len(), 2);
    let diff = detail.diff.unwrap();
    assert_eq!(diff.revision, 0);
    assert!(diff.files.is_empty());
    assert_eq!(
        detail.tests.unwrap().status,
        coding_agent_domain::TestStatus::Passed
    );
    let artifact = fixture
        .store()
        .load_attempt_artifact(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(artifact.state, AttemptArtifactState::Ready);
    assert_eq!(fixture.providers.starts.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.attempts.prepared.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.attempts.provisioned.load(Ordering::SeqCst), 1);
    assert!(fixture.runtime.terminal_calls() >= 2);
    let engine_calls = fixture.runtime.engine_calls();
    assert_eq!(
        engine_calls
            .iter()
            .map(|call| call.role_run)
            .collect::<Vec<_>>(),
        [
            RoleRun::try_new(Role::Planner, 1).unwrap(),
            RoleRun::try_new(Role::Executor, 1).unwrap(),
            RoleRun::try_new(Role::Reviewer, 1).unwrap(),
            RoleRun::try_new(Role::Executor, 2).unwrap(),
            RoleRun::try_new(Role::Reviewer, 2).unwrap(),
        ]
    );
    assert_eq!(
        engine_calls
            .iter()
            .map(|call| call.scope_id)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        5,
        "every RoleRun receives a fresh exactly-scoped runtime/engine"
    );
    assert!(engine_calls[0].review_checkpoint.is_none());
    assert!(engine_calls[1].review_checkpoint.is_none());
    assert!(engine_calls[3].review_checkpoint.is_none());
    assert_eq!(
        engine_calls[2]
            .review_checkpoint
            .as_ref()
            .unwrap()
            .generation(),
        0
    );
    assert_eq!(
        engine_calls[4]
            .review_checkpoint
            .as_ref()
            .unwrap()
            .generation(),
        0
    );
    assert_eq!(
        fixture
            .runtime
            .state
            .finalization_calls
            .load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        fixture.runtime.finalization_expected_fingerprints(),
        [WorkspaceFingerprint::from_bytes([0; 32])]
    );

    let kinds = fixture.event_kinds(task.id).await;
    assert!(kinds.contains(&TaskEventKind::PlanUpdated));
    assert!(kinds.contains(&TaskEventKind::ActivityAppended));
    assert!(kinds.contains(&TaskEventKind::DiffUpdated));
    assert!(kinds.contains(&TaskEventKind::TestUpdated));
    assert!(kinds.contains(&TaskEventKind::ReviewUpdated));
    assert_eq!(
        kinds
            .iter()
            .filter(|kind| **kind == TaskEventKind::ReviewUpdated)
            .count(),
        2,
        "round one uses RoleEventSink::record_review and final approval is finalized atomically"
    );
    assert_eq!(kinds.last(), Some(&TaskEventKind::TaskCompleted));
    let last_panel = kinds
        .iter()
        .enumerate()
        .filter(|(_, kind)| {
            matches!(
                kind,
                TaskEventKind::DiffUpdated | TaskEventKind::TestUpdated
            )
        })
        .map(|(index, _)| index)
        .max()
        .unwrap();
    let first_review = kinds
        .iter()
        .position(|kind| *kind == TaskEventKind::ReviewUpdated)
        .unwrap();
    let review = kinds
        .iter()
        .rposition(|kind| *kind == TaskEventKind::ReviewUpdated)
        .unwrap();
    let terminal = kinds
        .iter()
        .position(|kind| *kind == TaskEventKind::TaskCompleted)
        .unwrap();
    assert!(
        kinds[..first_review].iter().any(|kind| {
            matches!(
                kind,
                TaskEventKind::DiffUpdated | TaskEventKind::TestUpdated
            )
        }),
        "intermediate review follows its durable diff/test checkpoint"
    );
    assert!(last_panel < review && review < terminal);
    assert_eq!(
        kinds
            .iter()
            .filter(|kind| **kind == TaskEventKind::DiffUpdated)
            .count(),
        1,
        "live and terminal copies of the same snapshot are coalesced"
    );

    let requests = fixture
        .providers
        .requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(requests.len(), scenario_responses().len());
    let first_user = requests[0]
        .messages
        .iter()
        .find_map(|message| match message {
            ModelMessage::User(content) => Some(content),
            _ => None,
        })
        .unwrap();
    assert!(first_user.contains("Cargo package demo"));
    assert!(!first_user.contains(&fixture.base.repository.git_root.to_string()));
    assert!(
        requests
            .iter()
            .flat_map(|request| &request.messages)
            .all(|message| match message {
                ModelMessage::ToolResult { content, .. } => !content.contains(SECRET),
                _ => true,
            })
    );
    assert!(
        requests
            .iter()
            .flat_map(|request| &request.messages)
            .any(|message| match message {
                ModelMessage::ToolResult { content, .. } => content.contains("<redacted>"),
                _ => false,
            })
    );
}

#[tokio::test]
async fn finalization_guard_workspace_change_fails_closed_and_never_persists_approval() {
    let fixture = Fixture::new(ProviderMode::SuccessfulChange, ProvisionMode::Ready).await;
    fixture.runtime.mutate_workspace_during_finalization();
    let task = fixture.enqueue().await;
    let failed = fixture.wait_for(task.id, TaskStatus::Failed).await;

    assert_eq!(
        failed.delivery_readiness,
        DeliveryReadiness::Unreviewed,
        "a post-review workspace change must never remain approved"
    );
    assert_eq!(
        failed.failure.as_ref().map(|failure| failure.code.as_str()),
        Some("QUALITY_EVIDENCE_MISMATCH")
    );
    assert_eq!(
        fixture.runtime.finalization_expected_fingerprints(),
        [WorkspaceFingerprint::from_bytes([0; 32])],
        "the finalization guard must receive the exact reviewed fingerprint"
    );
    assert_eq!(
        fixture
            .runtime
            .state
            .workspace_version
            .load(Ordering::Acquire),
        1,
        "the scripted guard changes the workspace before it returns"
    );

    let detail = fixture.detail(task.id).await;
    assert_eq!(
        detail.task.delivery_readiness,
        DeliveryReadiness::Unreviewed
    );
    assert_eq!(detail.reviews.len(), 1);
    assert_eq!(
        detail.reviews[0].verdict(),
        ReviewVerdict::ChangesRequested,
        "the final approval must not be persisted after the guard mismatch"
    );
    assert_eq!(
        fixture
            .event_kinds(task.id)
            .await
            .iter()
            .filter(|kind| **kind == TaskEventKind::ReviewUpdated)
            .count(),
        1
    );
}

#[tokio::test]
async fn normal_cancellation_forces_terminal_diff_and_retains_ready_artifact() {
    let fixture = Fixture::new(ProviderMode::Blocking, ProvisionMode::Ready).await;
    let task = fixture.enqueue().await;
    tokio::time::timeout(Duration::from_secs(5), fixture.providers.entered.notified())
        .await
        .unwrap();
    fixture.manager.cancel(task.id).await.unwrap();
    fixture.wait_for(task.id, TaskStatus::Cancelled).await;

    let detail = fixture.detail(task.id).await;
    assert!(detail.diff.is_some());
    assert!(fixture.runtime.terminal_calls() >= 1);
    assert_eq!(
        fixture
            .store()
            .load_attempt_artifact(task.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        AttemptArtifactState::Ready
    );
}

#[tokio::test]
async fn forced_quiesce_rejects_the_runners_late_terminal_diff() {
    let fixture = Fixture::new(ProviderMode::Blocking, ProvisionMode::Ready).await;
    let task = fixture.enqueue().await;
    tokio::time::timeout(Duration::from_secs(5), fixture.providers.entered.notified())
        .await
        .unwrap();

    let active = match fixture
        .manager
        .quiesce_and_interrupt(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap()
    {
        QuiesceResult::Durable { active, .. } => active,
        QuiesceResult::Frozen { .. } => panic!("quiesce should commit"),
    };
    assert_eq!(
        fixture.detail(task.id).await.task.status,
        TaskStatus::Interrupted
    );
    assert_eq!(active.len(), 1);
    active.first().unwrap().cancellation.cancel();
    active.into_iter().next().unwrap().done.await.unwrap();

    let detail = fixture.detail(task.id).await;
    assert_eq!(detail.task.status, TaskStatus::Interrupted);
    assert!(
        detail.diff.is_none(),
        "the post-barrier diff stays rejected"
    );
    assert!(fixture.runtime.terminal_calls() >= 1);
    assert_eq!(
        fixture.event_kinds(task.id).await.last(),
        Some(&TaskEventKind::TaskInterrupted)
    );
}

#[tokio::test]
async fn unsafe_provider_failure_is_safely_mapped_and_ready_artifact_is_retained() {
    let fixture = Fixture::new(ProviderMode::UnsafeFailure, ProvisionMode::Ready).await;
    let task = fixture.enqueue().await;
    let failed = fixture.wait_for(task.id, TaskStatus::Failed).await;

    let failure = failed.failure.unwrap();
    assert_eq!(failure.code, "PLANNER_PROVIDER_FAILED");
    assert_eq!(
        failure.message,
        "The multi-role coding agent could not complete the task"
    );
    assert!(!failure.message.contains(SECRET));
    assert_eq!(
        fixture
            .store()
            .load_attempt_artifact(task.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        AttemptArtifactState::Ready
    );
    assert!(fixture.detail(task.id).await.diff.is_some());
}

#[tokio::test]
async fn terminal_capture_failure_replaces_stale_passed_or_running_checks_with_queued() {
    let fixture = Fixture::new(ProviderMode::TerminalCaptureFailure, ProvisionMode::Ready).await;
    let task = fixture.enqueue().await;
    fixture.wait_for(task.id, TaskStatus::Failed).await;

    let statuses = fixture.test_statuses(task.id).await;
    assert!(
        statuses.contains(&TestStatus::Running) || statuses.contains(&TestStatus::Passed),
        "the regression must first observe an active or passed required check"
    );
    assert_eq!(statuses.last(), Some(&TestStatus::Queued));
    let tests = fixture.detail(task.id).await.tests.unwrap();
    assert_eq!(tests.status, TestStatus::Queued);
    assert!(
        tests
            .cases
            .iter()
            .all(|case| case.status == TestStatus::Queued)
    );
    assert!(fixture.runtime.terminal_calls() >= 2);
}

#[tokio::test]
async fn terminal_capture_timeout_uses_fresh_projection_token_and_clears_stale_checks() {
    let fixture = Fixture::new(ProviderMode::TerminalCaptureTimeout, ProvisionMode::Ready).await;
    let task = fixture.enqueue().await;
    fixture.wait_for(task.id, TaskStatus::Failed).await;

    let statuses = fixture.test_statuses(task.id).await;
    assert!(statuses.contains(&TestStatus::Running) || statuses.contains(&TestStatus::Passed));
    assert_eq!(statuses.last(), Some(&TestStatus::Queued));
    assert_eq!(
        fixture.detail(task.id).await.tests.unwrap().status,
        TestStatus::Queued
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        while fixture.runtime.terminal_cancellations() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ready_observation_capture_timeout_cancels_capture_without_starting_provider() {
    let fixture = Fixture::new(
        ProviderMode::SuccessfulChange,
        ProvisionMode::ReadyFailureCaptureTimeout,
    )
    .await;
    let task = fixture.enqueue().await;
    let failed = fixture.wait_for(task.id, TaskStatus::Failed).await;

    assert_eq!(failed.failure.unwrap().code, "WORKTREE_CREATE_FAILED");
    assert_eq!(fixture.providers.starts.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.runtime.terminal_calls(), 1);
    assert_eq!(
        fixture
            .store()
            .load_attempt_artifact(task.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        AttemptArtifactState::Ready
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        while fixture.runtime.terminal_cancellations() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn partial_provision_failure_is_marked_inconsistent_and_never_starts_provider() {
    let fixture = Fixture::new(
        ProviderMode::SuccessfulChange,
        ProvisionMode::PartialFailure,
    )
    .await;
    let task = fixture.enqueue().await;
    let failed = fixture.wait_for(task.id, TaskStatus::Failed).await;

    assert_eq!(failed.failure.unwrap().code, "WORKTREE_CREATE_FAILED");
    let artifact = fixture
        .store()
        .load_attempt_artifact(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(artifact.state, AttemptArtifactState::Inconsistent);
    assert_eq!(
        artifact.failure_code.as_deref(),
        Some("WORKTREE_STATE_INCONSISTENT")
    );
    assert_eq!(fixture.providers.starts.load(Ordering::SeqCst), 0);
}
