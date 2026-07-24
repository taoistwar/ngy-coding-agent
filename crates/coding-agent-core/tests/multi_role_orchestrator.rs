use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use coding_agent_core::{
    ActionRequest, ContextRedactor, DiffEvent, DurableCheckpointAck, DurableEventAck,
    DurableRoleEvent, FinalizationGuard, FinalizationGuardError, ModelMessage, ModelRequest,
    ModelResponse, MultiRoleInput, MultiRoleOrchestrator, MultiRoleOutcome, MultiRoleRunReport,
    PreparedModelProvider, PreparedProviderRequest, ProviderError, RawProviderResponse,
    ReviewDiffBundle, ReviewDiffCheckpoint, ReviewDiffManifest, Role, RoleActionRuntime,
    RoleEngine, RoleEngineFactory, RoleEvent, RoleEventSink, RoleRun, RoleRuntimeResult,
    RuntimeActionRequest, RuntimeError, TerminalSnapshot, ToolCall, ToolCallBatch, ToolRequest,
    ToolResult, ValidationObservation, WorkspaceCheckpoint, WorkspaceFingerprint,
};
use coding_agent_domain::{
    CheckEvidenceStatus, DeliveryReadiness, RequiredCheckSelector, ReviewVerdict, TaskStatus,
};
use tokio_util::sync::CancellationToken;

struct IdentityRedactor;

impl ContextRedactor for IdentityRedactor {
    fn redact(&self, content: &str) -> String {
        content.to_owned()
    }
}

struct ProviderScript {
    response: ModelResponse,
}

struct ScriptedPreparedRequest {
    response: ModelResponse,
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
        _cancellation: CancellationToken,
    ) -> Result<Box<dyn RawProviderResponse>, ProviderError> {
        Ok(Box::new(ScriptedRawResponse {
            response: self.response,
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

struct ScriptedProvider {
    scripts: Mutex<VecDeque<ProviderScript>>,
    requests: Mutex<Vec<ModelRequest>>,
}

impl ScriptedProvider {
    fn new(responses: Vec<ModelResponse>) -> Self {
        Self {
            scripts: Mutex::new(
                responses
                    .into_iter()
                    .map(|response| ProviderScript { response })
                    .collect(),
            ),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<ModelRequest> {
        self.requests.lock().unwrap().clone()
    }

    fn remaining(&self) -> usize {
        self.scripts.lock().unwrap().len()
    }
}

impl PreparedModelProvider for ScriptedProvider {
    fn prepare(
        &self,
        request: ModelRequest,
    ) -> Result<Box<dyn PreparedProviderRequest>, ProviderError> {
        self.requests.lock().unwrap().push(request);
        let script = self.scripts.lock().unwrap().pop_front().ok_or_else(|| {
            ProviderError::new(
                "SCRIPTED_PROVIDER_EXHAUSTED",
                "no scripted response remains",
                false,
            )
        })?;
        Ok(Box::new(ScriptedPreparedRequest {
            response: script.response,
        }))
    }
}

struct StableRuntime {
    fingerprint: WorkspaceFingerprint,
    manifest: ReviewDiffManifest,
    requests: Mutex<Vec<RuntimeActionRequest>>,
}

impl StableRuntime {
    fn new(checkpoint: &WorkspaceCheckpoint) -> Self {
        let manifest = ReviewDiffBundle::try_new(
            &ReviewDiffCheckpoint::from_workspace_checkpoint(checkpoint),
            Vec::new(),
            &IdentityRedactor,
        )
        .unwrap()
        .manifest()
        .clone();
        Self {
            fingerprint: checkpoint.fingerprint(),
            manifest,
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<RuntimeActionRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl RoleActionRuntime for StableRuntime {
    async fn invoke(
        &self,
        request: RuntimeActionRequest,
        _cancellation: CancellationToken,
    ) -> Result<RoleRuntimeResult, RuntimeError> {
        self.requests.lock().unwrap().push(request.clone());
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
            RuntimeActionRequest::ReviewDiffManifest { .. } => {
                Ok(RoleRuntimeResult::ReviewDiffManifest(self.manifest.clone()))
            }
            RuntimeActionRequest::Tool(_) => {
                Ok(RoleRuntimeResult::Tool(ToolResult::text("inspected")))
            }
            RuntimeActionRequest::ValidationSelector { .. } => Err(RuntimeError::new(
                "UNRESOLVED_VALIDATION_SELECTOR",
                "core must resolve selectors before runtime invocation",
                false,
            )),
            RuntimeActionRequest::ReviewDiffChunks { .. } => Err(RuntimeError::new(
                "UNEXPECTED_REVIEW_CHUNKS",
                "the empty review diff has no chunks",
                false,
            )),
        }
    }

    async fn workspace_fingerprint(
        &self,
        _cancellation: CancellationToken,
    ) -> Result<WorkspaceFingerprint, RuntimeError> {
        Ok(self.fingerprint)
    }

    async fn terminal_snapshot(
        &self,
        generation: u64,
        _cancellation: CancellationToken,
    ) -> Result<TerminalSnapshot, RuntimeError> {
        Ok(TerminalSnapshot {
            fingerprint: self.fingerprint,
            diff: DiffEvent {
                revision: generation,
                files: Vec::new(),
            },
        })
    }

    async fn terminal_review_diff_manifest(
        &self,
        _checkpoint: ReviewDiffCheckpoint,
        _cancellation: CancellationToken,
    ) -> Result<ReviewDiffManifest, RuntimeError> {
        Ok(self.manifest.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EngineFactoryCall {
    role_run: RoleRun,
    review_checkpoint: Option<ReviewDiffCheckpoint>,
}

struct RecordingEngineFactory {
    provider: Arc<ScriptedProvider>,
    runtime: Arc<StableRuntime>,
    events: Arc<RecordingEvents>,
    calls: Mutex<Vec<EngineFactoryCall>>,
    fail_on: Option<RoleRun>,
    cancel_on: Option<(RoleRun, CancellationToken)>,
}

impl RecordingEngineFactory {
    fn new(
        provider: Arc<ScriptedProvider>,
        runtime: Arc<StableRuntime>,
        events: Arc<RecordingEvents>,
    ) -> Self {
        Self {
            provider,
            runtime,
            events,
            calls: Mutex::new(Vec::new()),
            fail_on: None,
            cancel_on: None,
        }
    }

    fn failing(
        provider: Arc<ScriptedProvider>,
        runtime: Arc<StableRuntime>,
        events: Arc<RecordingEvents>,
        fail_on: RoleRun,
    ) -> Self {
        Self {
            provider,
            runtime,
            events,
            calls: Mutex::new(Vec::new()),
            fail_on: Some(fail_on),
            cancel_on: None,
        }
    }

    fn cancelling(
        provider: Arc<ScriptedProvider>,
        runtime: Arc<StableRuntime>,
        events: Arc<RecordingEvents>,
        cancel_on: RoleRun,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            provider,
            runtime,
            events,
            calls: Mutex::new(Vec::new()),
            fail_on: None,
            cancel_on: Some((cancel_on, cancellation)),
        }
    }

    fn calls(&self) -> Vec<EngineFactoryCall> {
        self.calls.lock().unwrap().clone()
    }
}

impl RoleEngineFactory for RecordingEngineFactory {
    fn create_engine(
        &self,
        role_run: RoleRun,
        review_checkpoint: Option<ReviewDiffCheckpoint>,
    ) -> Result<RoleEngine, RuntimeError> {
        let shape_is_valid = match role_run.role() {
            Role::Planner | Role::Executor => review_checkpoint.is_none(),
            Role::Reviewer => review_checkpoint.is_some(),
        };
        if !shape_is_valid {
            return Err(RuntimeError::new(
                "SCRIPTED_FACTORY_SCOPE_MISMATCH",
                "role engine factory received the wrong checkpoint authority",
                false,
            ));
        }
        self.calls.lock().unwrap().push(EngineFactoryCall {
            role_run,
            review_checkpoint,
        });
        if let Some((cancel_on, cancellation)) = &self.cancel_on
            && *cancel_on == role_run
        {
            cancellation.cancel();
        }
        if self.fail_on == Some(role_run) {
            return Err(RuntimeError::new(
                "SCRIPTED_ENGINE_FACTORY_FAILED",
                "scripted role engine construction failed",
                true,
            ));
        }
        Ok(RoleEngine::new(
            self.provider.clone(),
            self.runtime.clone(),
            self.events.clone(),
            Arc::new(IdentityRedactor),
        ))
    }
}

#[derive(Debug, Clone)]
struct DurableRecord {
    event: DurableRoleEvent,
    sequence: u64,
}

#[derive(Debug, Clone, Copy, Default)]
enum IntermediateMode {
    #[default]
    Normal,
    SameSequence,
    Error,
    Cancel,
}

#[derive(Default)]
struct RecordingEvents {
    ordinary: Mutex<Vec<RoleEvent>>,
    durable: Mutex<Vec<DurableRecord>>,
    flushes: Mutex<Vec<(u64, u64)>>,
    next_sequence: AtomicU64,
    intermediate_attempts: AtomicUsize,
    intermediate_mode: IntermediateMode,
}

impl RecordingEvents {
    fn with_intermediate_mode(intermediate_mode: IntermediateMode) -> Self {
        Self {
            intermediate_mode,
            ..Self::default()
        }
    }

    fn ordinary(&self) -> Vec<RoleEvent> {
        self.ordinary.lock().unwrap().clone()
    }

    fn durable(&self) -> Vec<DurableRecord> {
        self.durable.lock().unwrap().clone()
    }

    fn flushes(&self) -> Vec<(u64, u64)> {
        self.flushes.lock().unwrap().clone()
    }

    fn next_sequence(&self) -> u64 {
        self.next_sequence.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn intermediate_attempts(&self) -> usize {
        self.intermediate_attempts.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl RoleEventSink for RecordingEvents {
    async fn emit(
        &self,
        event: RoleEvent,
        _cancellation: CancellationToken,
    ) -> Result<(), RuntimeError> {
        self.ordinary.lock().unwrap().push(event);
        Ok(())
    }

    async fn emit_durable(
        &self,
        event: DurableRoleEvent,
        cancellation: CancellationToken,
    ) -> Result<DurableEventAck, RuntimeError> {
        if let DurableRoleEvent::IntermediateReview {
            after_checkpoint_sequence,
            ..
        } = &event
        {
            self.intermediate_attempts.fetch_add(1, Ordering::SeqCst);
            match self.intermediate_mode {
                IntermediateMode::Normal => {}
                IntermediateMode::SameSequence => {
                    let sequence = *after_checkpoint_sequence;
                    self.durable
                        .lock()
                        .unwrap()
                        .push(DurableRecord { event, sequence });
                    return DurableEventAck::try_new(sequence);
                }
                IntermediateMode::Error => {
                    return Err(RuntimeError::new(
                        "SCRIPTED_INTERMEDIATE_STORE_FAILED",
                        "intermediate review was not durably stored",
                        false,
                    ));
                }
                IntermediateMode::Cancel => {
                    cancellation.cancel();
                    return Err(RuntimeError::new(
                        "SCRIPTED_INTERMEDIATE_STORE_FAILED",
                        "cancellation raced with intermediate storage",
                        false,
                    ));
                }
            }
        }
        let sequence = self.next_sequence();
        self.durable
            .lock()
            .unwrap()
            .push(DurableRecord { event, sequence });
        DurableEventAck::try_new(sequence)
    }

    async fn flush_checkpoint(
        &self,
        generation: u64,
        _cancellation: CancellationToken,
    ) -> Result<DurableCheckpointAck, RuntimeError> {
        let sequence = self.next_sequence();
        self.flushes.lock().unwrap().push((generation, sequence));
        DurableCheckpointAck::try_new(sequence, generation)
    }
}

#[derive(Default)]
struct PassingFinalizationGuard {
    calls: AtomicUsize,
    expected_fingerprints: Mutex<Vec<WorkspaceFingerprint>>,
}

impl PassingFinalizationGuard {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn expected_fingerprints(&self) -> Vec<WorkspaceFingerprint> {
        self.expected_fingerprints.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl FinalizationGuard for PassingFinalizationGuard {
    async fn verify_finalization(
        &self,
        expected_fingerprint: WorkspaceFingerprint,
        _cancellation: CancellationToken,
    ) -> Result<(), FinalizationGuardError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.expected_fingerprints
            .lock()
            .unwrap()
            .push(expected_fingerprint);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum GuardMode {
    IdentityMismatch,
    WorkspaceMismatch,
    RuntimeError,
    Cancel,
}

struct ScriptedFinalizationGuard {
    mode: GuardMode,
    calls: AtomicUsize,
    expected_fingerprints: Mutex<Vec<WorkspaceFingerprint>>,
}

impl ScriptedFinalizationGuard {
    fn new(mode: GuardMode) -> Self {
        Self {
            mode,
            calls: AtomicUsize::new(0),
            expected_fingerprints: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn expected_fingerprints(&self) -> Vec<WorkspaceFingerprint> {
        self.expected_fingerprints.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl FinalizationGuard for ScriptedFinalizationGuard {
    async fn verify_finalization(
        &self,
        expected_fingerprint: WorkspaceFingerprint,
        cancellation: CancellationToken,
    ) -> Result<(), FinalizationGuardError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.expected_fingerprints
            .lock()
            .unwrap()
            .push(expected_fingerprint);
        match self.mode {
            GuardMode::IdentityMismatch => Err(FinalizationGuardError::IdentityMismatch),
            GuardMode::WorkspaceMismatch => Err(FinalizationGuardError::WorkspaceMismatch),
            GuardMode::RuntimeError => Err(FinalizationGuardError::Runtime(RuntimeError::new(
                "SCRIPTED_GUARD_FAILED",
                "trusted attempt identity lookup failed",
                true,
            ))),
            GuardMode::Cancel => {
                cancellation.cancel();
                Ok(())
            }
        }
    }
}

fn batch(calls: Vec<ToolCall>) -> ModelResponse {
    ModelResponse::ToolCalls(ToolCallBatch {
        assistant_content: None,
        reasoning_content: None,
        calls,
    })
}

fn plan_call(id: &str) -> ToolCall {
    ToolCall {
        id: id.to_owned(),
        request: ActionRequest::decode(
            Role::Planner,
            "submit_plan",
            &serde_json::json!({
                "summary": "Implement the multi-role orchestrator",
                "steps": [{
                    "title": "Implement orchestrator",
                    "description": "Run the bounded role sequence.",
                    "acceptance_criteria": ["The approved sequence is durable."]
                }],
                "initial_required_checks": [{
                    "kind": "cargo_test",
                    "package": "coding-agent-core",
                    "integration_test": "multi_role_orchestrator"
                }]
            })
            .to_string(),
        )
        .unwrap(),
    }
}

fn executor_progress_call(id: &str) -> ToolCall {
    ToolCall {
        id: id.to_owned(),
        request: ActionRequest::decode(
            Role::Executor,
            "update_plan_progress",
            r#"{"updates":[{"step_id":"step-01","status":"completed"}]}"#,
        )
        .unwrap(),
    }
}

fn executor_validation_call(id: &str) -> ToolCall {
    ToolCall {
        id: id.to_owned(),
        request: ActionRequest::decode(
            Role::Executor,
            "cargo_test",
            r#"{"package":"coding-agent-core","integration_test":"multi_role_orchestrator"}"#,
        )
        .unwrap(),
    }
}

fn executor_submission_call(id: &str, round: u32) -> ToolCall {
    ToolCall {
        id: id.to_owned(),
        request: ActionRequest::decode(
            Role::Executor,
            "submit_execution",
            &serde_json::json!({"summary": format!("round {round} ready")}).to_string(),
        )
        .unwrap(),
    }
}

fn read_call(id: &str) -> ToolCall {
    ToolCall::runtime(
        id,
        ToolRequest::ReadFile {
            path: "src/lib.rs".to_owned(),
            start_line: 1,
            end_line: 4,
        },
    )
}

fn reviewer_manifest_call(id: &str, checkpoint: &WorkspaceCheckpoint) -> ToolCall {
    ToolCall {
        id: id.to_owned(),
        request: ActionRequest::Runtime(RuntimeActionRequest::ReviewDiffManifest {
            generation: checkpoint.generation(),
            workspace_digest: checkpoint.workspace_digest(),
        }),
    }
}

fn reviewer_submission_call(id: &str, verdict: &str, add_check: bool) -> ToolCall {
    let findings = if verdict == "changes_requested" {
        serde_json::json!([{
            "severity": "blocking",
            "message": "A blocking issue remains",
            "path": "src/lib.rs",
            "line": 1
        }])
    } else {
        serde_json::json!([])
    };
    let add_required_checks = if add_check {
        serde_json::json!([{
            "kind": "cargo_check",
            "package": "coding-agent-domain"
        }])
    } else {
        serde_json::json!([])
    };
    ToolCall {
        id: id.to_owned(),
        request: ActionRequest::decode(
            Role::Reviewer,
            "submit_review",
            &serde_json::json!({
                "verdict": verdict,
                "summary": format!("{verdict} review"),
                "findings": findings,
                "add_required_checks": add_required_checks
            })
            .to_string(),
        )
        .unwrap(),
    }
}

fn reviewer_changes_with_added_checks(id: &str, packages: &[&str]) -> ToolCall {
    let additions = packages
        .iter()
        .map(|package| {
            serde_json::json!({
                "kind": "cargo_check",
                "package": package
            })
        })
        .collect::<Vec<_>>();
    ToolCall {
        id: id.to_owned(),
        request: ActionRequest::decode(
            Role::Reviewer,
            "submit_review",
            &serde_json::json!({
                "verdict": "changes_requested",
                "summary": "changes_requested review",
                "findings": [{
                    "severity": "blocking",
                    "message": "A blocking issue remains",
                    "path": "src/lib.rs",
                    "line": 1
                }],
                "add_required_checks": additions
            })
            .to_string(),
        )
        .unwrap(),
    }
}

fn approval_responses(round: u32, checkpoint: &WorkspaceCheckpoint) -> Vec<ModelResponse> {
    let mut responses = (1..=3)
        .map(|index| batch(vec![read_call(&format!("r{round}-read-{index}"))]))
        .collect::<Vec<_>>();
    responses.push(batch(
        (0..8)
            .map(|index| read_call(&format!("r{round}-reserved-read-{index}")))
            .collect(),
    ));
    responses.push(batch(vec![reviewer_manifest_call(
        &format!("r{round}-manifest"),
        checkpoint,
    )]));
    responses.push(batch(vec![reviewer_submission_call(
        &format!("r{round}-approved"),
        "approved",
        false,
    )]));
    responses
}

fn scenario_responses(
    rework_rounds: u32,
    final_approved: bool,
    checkpoint: &WorkspaceCheckpoint,
) -> Vec<ModelResponse> {
    assert!(rework_rounds <= 2);
    let final_round = rework_rounds + 1;
    let mut responses = vec![
        batch(vec![plan_call("plan")]),
        batch(vec![executor_progress_call("e1-progress")]),
        batch(vec![executor_validation_call("e1-validation")]),
        batch(vec![executor_submission_call("reused-executor-submit", 1)]),
    ];
    for round in 1..=final_round {
        if round <= rework_rounds || !final_approved {
            responses.push(batch(vec![reviewer_submission_call(
                "reused-review-submit",
                "changes_requested",
                round == 1,
            )]));
        } else {
            responses.extend(approval_responses(round, checkpoint));
        }
        if round < final_round {
            if round == 1 {
                responses.push(batch(vec![ToolCall {
                    id: "added-check-validation".to_owned(),
                    request: ActionRequest::decode(
                        Role::Executor,
                        "cargo_check",
                        r#"{"package":"coding-agent-domain"}"#,
                    )
                    .unwrap(),
                }]));
            }
            responses.push(batch(vec![executor_submission_call(
                "reused-executor-submit",
                round + 1,
            )]));
        }
    }
    responses
}

fn rework_budget_exhaustion_responses() -> Vec<ModelResponse> {
    let mut responses = (1..=6)
        .map(|index| batch(vec![read_call(&format!("p-read-{index}"))]))
        .collect::<Vec<_>>();
    responses.push(batch(vec![plan_call("plan")]));

    responses.push(batch(vec![executor_progress_call("e1-progress")]));
    responses.extend((1..=15).map(|index| batch(vec![read_call(&format!("e1-read-{index}"))])));
    responses.push(batch(vec![executor_validation_call("e1-validation")]));
    responses.push(batch(vec![executor_submission_call("e1-submit", 1)]));

    responses.extend((1..=3).map(|index| batch(vec![read_call(&format!("r1-read-{index}"))])));
    responses.push(batch(vec![reviewer_submission_call(
        "r1-changes",
        "changes_requested",
        true,
    )]));

    responses.extend((1..=16).map(|index| batch(vec![read_call(&format!("e2-read-{index}"))])));
    responses.push(batch(vec![ToolCall {
        id: "e2-added-check".to_owned(),
        request: ActionRequest::decode(
            Role::Executor,
            "cargo_check",
            r#"{"package":"coding-agent-domain"}"#,
        )
        .unwrap(),
    }]));
    responses.push(batch(vec![executor_submission_call("e2-submit", 2)]));

    responses.extend((1..=3).map(|index| batch(vec![read_call(&format!("r2-read-{index}"))])));
    responses.push(batch(vec![reviewer_changes_with_added_checks(
        "r2-changes",
        &[
            "coding-agent-provider",
            "coding-agent-runtime",
            "coding-agent-store",
            "coding-agent-app",
        ],
    )]));
    assert_eq!(responses.len(), 51);
    responses
}

fn catalog() -> Vec<RequiredCheckSelector> {
    vec![
        RequiredCheckSelector::try_cargo_test(
            Some("coding-agent-core".to_owned()),
            Some("multi_role_orchestrator".to_owned()),
        )
        .unwrap(),
    ]
}

async fn run_with_ports(
    responses: Vec<ModelResponse>,
    events: Arc<RecordingEvents>,
    guard: Arc<dyn FinalizationGuard>,
) -> (
    MultiRoleRunReport,
    Arc<ScriptedProvider>,
    Arc<StableRuntime>,
    Arc<RecordingEngineFactory>,
) {
    let checkpoint = WorkspaceCheckpoint::new(WorkspaceFingerprint::from_bytes([0x42; 32]));
    let provider = Arc::new(ScriptedProvider::new(responses));
    let runtime = Arc::new(StableRuntime::new(&checkpoint));
    let factory = Arc::new(RecordingEngineFactory::new(
        provider.clone(),
        runtime.clone(),
        events,
    ));
    let reviewer_catalog = catalog();
    let report = MultiRoleOrchestrator::new(factory.clone(), guard)
        .run(
            MultiRoleInput {
                task_prompt: "task",
                repository_context: "repository",
                checkpoint,
                repository_check_catalog: &reviewer_catalog,
            },
            CancellationToken::new(),
        )
        .await;
    (report, provider, runtime, factory)
}

async fn run_scenario(
    rework_rounds: u32,
    final_approved: bool,
) -> (
    MultiRoleRunReport,
    Arc<ScriptedProvider>,
    Arc<StableRuntime>,
    Arc<RecordingEvents>,
    Arc<PassingFinalizationGuard>,
    Arc<RecordingEngineFactory>,
) {
    let checkpoint = WorkspaceCheckpoint::new(WorkspaceFingerprint::from_bytes([0x42; 32]));
    let provider = Arc::new(ScriptedProvider::new(scenario_responses(
        rework_rounds,
        final_approved,
        &checkpoint,
    )));
    let runtime = Arc::new(StableRuntime::new(&checkpoint));
    let events = Arc::new(RecordingEvents::default());
    let guard = Arc::new(PassingFinalizationGuard::default());
    let factory = Arc::new(RecordingEngineFactory::new(
        provider.clone(),
        runtime.clone(),
        events.clone(),
    ));
    let planner_catalog = catalog();
    let report = MultiRoleOrchestrator::new(factory.clone(), guard.clone())
        .run(
            MultiRoleInput {
                task_prompt: "task",
                repository_context: "repository",
                checkpoint,
                repository_check_catalog: &planner_catalog,
            },
            CancellationToken::new(),
        )
        .await;
    (report, provider, runtime, events, guard, factory)
}

#[tokio::test]
async fn first_review_approval_runs_fresh_planner_executor_reviewer_handoffs() {
    let checkpoint = WorkspaceCheckpoint::new(WorkspaceFingerprint::from_bytes([0x42; 32]));
    let mut responses = vec![
        batch(vec![plan_call("plan")]),
        batch(vec![executor_progress_call("e1-progress")]),
        batch(vec![executor_validation_call("e1-validation")]),
        batch(vec![executor_submission_call("e1-submit", 1)]),
    ];
    responses.extend(approval_responses(1, &checkpoint));

    let provider = Arc::new(ScriptedProvider::new(responses));
    let runtime = Arc::new(StableRuntime::new(&checkpoint));
    let events = Arc::new(RecordingEvents::default());
    let guard = Arc::new(PassingFinalizationGuard::default());
    let expected_review_checkpoint = ReviewDiffCheckpoint::from_workspace_checkpoint(&checkpoint);
    let factory = Arc::new(RecordingEngineFactory::new(
        provider.clone(),
        runtime,
        events.clone(),
    ));
    let orchestrator = MultiRoleOrchestrator::new(factory.clone(), guard.clone());
    let catalog = catalog();

    let report = orchestrator
        .run(
            MultiRoleInput {
                task_prompt: "task",
                repository_context: "repository",
                checkpoint,
                repository_check_catalog: &catalog,
            },
            CancellationToken::new(),
        )
        .await;

    assert_eq!(report.status(), TaskStatus::Completed);
    assert_eq!(
        report.delivery_readiness(),
        DeliveryReadiness::ReviewApproved
    );
    assert_eq!(report.failure(), None);
    assert_eq!(report.final_workspace_generation(), 0);
    assert_eq!(
        report.final_workspace_digest(),
        *expected_review_checkpoint.workspace_digest()
    );
    assert_eq!(report.required_checks().unwrap().checks().len(), 1);
    assert_eq!(
        factory.calls(),
        [
            EngineFactoryCall {
                role_run: RoleRun::try_new(Role::Planner, 1).unwrap(),
                review_checkpoint: None,
            },
            EngineFactoryCall {
                role_run: RoleRun::try_new(Role::Executor, 1).unwrap(),
                review_checkpoint: None,
            },
            EngineFactoryCall {
                role_run: RoleRun::try_new(Role::Reviewer, 1).unwrap(),
                review_checkpoint: Some(expected_review_checkpoint),
            },
        ]
    );
    let (outcome, final_checkpoint, required_checks) = report.into_parts();
    assert_eq!(final_checkpoint.generation(), 0);
    assert!(required_checks.is_some());
    let MultiRoleOutcome::Approved(decision) = outcome else {
        panic!("first review must approve");
    };
    assert_eq!(decision.evidence().verdict(), ReviewVerdict::Approved);
    assert_eq!(guard.calls(), 1);
    assert_eq!(
        guard.expected_fingerprints(),
        [WorkspaceFingerprint::from_bytes([0x42; 32])]
    );
    assert_eq!(provider.remaining(), 0);
    assert_eq!(provider.requests().len(), 10);

    let starts = events
        .ordinary()
        .into_iter()
        .filter_map(|event| match event {
            RoleEvent::Activity(activity) if activity.message().ends_with("started") => {
                Some((activity.role(), activity.role_run()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        starts,
        [(Role::Planner, 1), (Role::Executor, 1), (Role::Reviewer, 1)]
    );

    let requests = provider.requests();
    for index in [0, 1, 4] {
        assert!(matches!(
            requests[index].messages.as_slice(),
            [ModelMessage::System(_), ModelMessage::User(_)]
        ));
    }
    assert!(
        events
            .durable()
            .iter()
            .all(|record| !matches!(record.event, DurableRoleEvent::IntermediateReview { .. }))
    );
    assert_eq!(
        decision.durable_sequence(),
        events.flushes().last().unwrap().1
    );
}

#[tokio::test]
async fn one_and_two_reworks_approve_while_third_changes_rejects_at_seven_role_runs() {
    for (rework_rounds, final_approved, expected_requests) in
        [(1, true, 13), (2, true, 15), (2, false, 10)]
    {
        let (report, provider, runtime, events, guard, factory) =
            run_scenario(rework_rounds, final_approved).await;
        let expected_final_digest = report.final_workspace_digest();
        assert_eq!(report.final_workspace_generation(), 0);
        assert!(report.required_checks().is_some());
        let (outcome, final_checkpoint, required_checks) = report.into_parts();
        assert_eq!(final_checkpoint.workspace_digest(), expected_final_digest);
        assert_eq!(required_checks.unwrap().checks().len(), 2);
        assert_eq!(provider.remaining(), 0);
        assert_eq!(provider.requests().len(), expected_requests);
        assert_eq!(guard.calls(), 1);
        assert_eq!(
            guard.expected_fingerprints(),
            [WorkspaceFingerprint::from_bytes([0x42; 32])]
        );

        let final_round = rework_rounds + 1;
        let final_sequence = match &outcome {
            MultiRoleOutcome::Approved(decision) if final_approved => {
                assert_eq!(outcome.status(), TaskStatus::Completed);
                assert_eq!(
                    outcome.delivery_readiness(),
                    DeliveryReadiness::ReviewApproved
                );
                assert_eq!(outcome.failure(), None);
                decision.durable_sequence()
            }
            MultiRoleOutcome::Rejected { decision, failure } if !final_approved => {
                assert_eq!(outcome.status(), TaskStatus::Failed);
                assert_eq!(
                    outcome.delivery_readiness(),
                    DeliveryReadiness::ReviewRejected
                );
                assert_eq!(failure.code, "REVIEW_REJECTED");
                assert!(failure.retryable);
                decision.durable_sequence()
            }
            _ => panic!("unexpected final multi-role outcome: {outcome:?}"),
        };
        assert_eq!(final_sequence, events.flushes().last().unwrap().1);

        let starts = events
            .ordinary()
            .into_iter()
            .filter_map(|event| match event {
                RoleEvent::Activity(activity) if activity.message().ends_with("started") => {
                    Some((activity.role(), activity.role_run()))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let mut expected_starts = vec![(Role::Planner, 1)];
        for round in 1..=final_round {
            expected_starts.push((Role::Executor, round));
            expected_starts.push((Role::Reviewer, round));
        }
        assert_eq!(starts, expected_starts);
        assert_eq!(starts.len(), 3 + 2 * rework_rounds as usize);
        assert_eq!(
            starts
                .iter()
                .filter(|(role, _)| *role == Role::Planner)
                .count(),
            1
        );
        let factory_calls = factory.calls();
        assert_eq!(factory_calls.len(), starts.len());
        let expected_review_checkpoint =
            ReviewDiffCheckpoint::from_workspace_checkpoint(&final_checkpoint);
        for (call, (role, role_run)) in factory_calls.iter().zip(&starts) {
            assert_eq!(call.role_run, RoleRun::try_new(*role, *role_run).unwrap());
            match role {
                Role::Reviewer => {
                    assert_eq!(
                        call.review_checkpoint.as_ref(),
                        Some(&expected_review_checkpoint)
                    );
                }
                Role::Planner | Role::Executor => {
                    assert_eq!(call.review_checkpoint, None);
                }
            }
        }

        let intermediate = events
            .durable()
            .into_iter()
            .filter_map(|record| match record.event {
                DurableRoleEvent::IntermediateReview {
                    evidence,
                    after_checkpoint_sequence,
                } => Some((evidence, after_checkpoint_sequence, record.sequence)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(intermediate.len(), rework_rounds as usize);
        for (index, (evidence, panel_sequence, ack_sequence)) in intermediate.iter().enumerate() {
            assert_eq!(usize::from(evidence.round()), index + 1);
            assert_eq!(evidence.verdict(), ReviewVerdict::ChangesRequested);
            assert!(
                events
                    .flushes()
                    .iter()
                    .any(|(_, sequence)| sequence == panel_sequence)
            );
            assert!(ack_sequence > panel_sequence);
        }

        let requests = provider.requests();
        let mut first_request_indexes = vec![0, 1, 4];
        if final_round >= 2 {
            first_request_indexes.extend([5, 7]);
        }
        if final_round >= 3 {
            first_request_indexes.extend([8, 9]);
        }
        for index in first_request_indexes {
            assert!(matches!(
                requests[index].messages.as_slice(),
                [ModelMessage::System(_), ModelMessage::User(_)]
            ));
        }
        if rework_rounds > 0 {
            let ModelMessage::User(executor_two_handoff) = &requests[5].messages[1] else {
                panic!("Executor two must start from a user handoff");
            };
            assert!(executor_two_handoff.contains("\"source_review_round\":1"));
            assert!(executor_two_handoff.contains("\"package\":\"coding-agent-domain\""));
            assert!(!executor_two_handoff.contains("reused-review-submit"));
            assert!(runtime.requests().iter().any(|request| {
                matches!(
                    request,
                    RuntimeActionRequest::Validation { check }
                        if !check.is_cargo_test()
                            && check.package() == Some("coding-agent-domain")
                )
            }));
        }
    }
}

#[tokio::test]
async fn intermediate_barrier_regression_store_error_and_cancellation_never_start_executor_two() {
    for (mode, expected_code, cancelled) in [
        (
            IntermediateMode::SameSequence,
            Some("QUALITY_EVIDENCE_MISMATCH"),
            false,
        ),
        (
            IntermediateMode::Error,
            Some("QUALITY_EVIDENCE_STORE_FAILED"),
            false,
        ),
        (IntermediateMode::Cancel, None, true),
    ] {
        let checkpoint = WorkspaceCheckpoint::new(WorkspaceFingerprint::from_bytes([0x42; 32]));
        let responses = scenario_responses(1, true, &checkpoint);
        let events = Arc::new(RecordingEvents::with_intermediate_mode(mode));
        let guard = Arc::new(PassingFinalizationGuard::default());
        let guard_port: Arc<dyn FinalizationGuard> = guard.clone();
        let (report, provider, _runtime, factory) =
            run_with_ports(responses, events.clone(), guard_port).await;

        assert_eq!(report.delivery_readiness(), DeliveryReadiness::Unreviewed);
        assert_eq!(report.required_checks().unwrap().checks().len(), 2);
        assert_eq!(report.final_workspace_generation(), 0);
        assert_eq!(events.intermediate_attempts(), 1);
        assert_eq!(guard.calls(), 0);
        assert_eq!(provider.remaining(), 8);
        assert_eq!(factory.calls().len(), 3);
        assert!(!events.ordinary().iter().any(|event| {
            matches!(
                event,
                RoleEvent::Activity(activity)
                    if activity.role() == Role::Executor && activity.role_run() == 2
            )
        }));

        if cancelled {
            assert_eq!(report.status(), TaskStatus::Cancelled);
            assert_eq!(report.failure(), None);
            assert!(matches!(report.outcome(), MultiRoleOutcome::Cancelled));
        } else {
            assert_eq!(report.status(), TaskStatus::Failed);
            assert_eq!(report.failure().unwrap().code, expected_code.unwrap());
            assert!(matches!(report.outcome(), MultiRoleOutcome::Failed(_)));
        }
        if matches!(mode, IntermediateMode::SameSequence) {
            let record = events
                .durable()
                .into_iter()
                .find(|record| matches!(record.event, DurableRoleEvent::IntermediateReview { .. }))
                .unwrap();
            let DurableRoleEvent::IntermediateReview {
                after_checkpoint_sequence,
                ..
            } = record.event
            else {
                unreachable!();
            };
            assert_eq!(record.sequence, after_checkpoint_sequence);
        }
    }
}

#[tokio::test]
async fn final_guard_identity_workspace_runtime_and_cancellation_fail_closed_after_validated_panel()
{
    for (mode, expected_code, cancelled) in [
        (
            GuardMode::IdentityMismatch,
            Some("QUALITY_EVIDENCE_MISMATCH"),
            false,
        ),
        (
            GuardMode::WorkspaceMismatch,
            Some("QUALITY_EVIDENCE_MISMATCH"),
            false,
        ),
        (
            GuardMode::RuntimeError,
            Some("REVIEWER_RUNTIME_FAILED"),
            false,
        ),
        (GuardMode::Cancel, None, true),
    ] {
        let checkpoint = WorkspaceCheckpoint::new(WorkspaceFingerprint::from_bytes([0x42; 32]));
        let responses = scenario_responses(0, true, &checkpoint);
        let events = Arc::new(RecordingEvents::default());
        let guard = Arc::new(ScriptedFinalizationGuard::new(mode));
        let guard_port: Arc<dyn FinalizationGuard> = guard.clone();
        let (report, provider, _runtime, factory) =
            run_with_ports(responses, events.clone(), guard_port).await;

        assert_eq!(guard.calls(), 1);
        assert_eq!(
            guard.expected_fingerprints(),
            [WorkspaceFingerprint::from_bytes([0x42; 32])],
            "the guard must receive the exact fingerprint behind reviewer evidence"
        );
        assert_eq!(provider.remaining(), 0);
        assert_eq!(factory.calls().len(), 3);
        assert_eq!(events.flushes().len(), 2);
        assert_eq!(report.required_checks().unwrap().checks().len(), 1);
        assert_eq!(report.delivery_readiness(), DeliveryReadiness::Unreviewed);
        if cancelled {
            assert_eq!(report.status(), TaskStatus::Cancelled);
            assert_eq!(report.failure(), None);
            assert!(matches!(report.outcome(), MultiRoleOutcome::Cancelled));
        } else {
            assert_eq!(report.status(), TaskStatus::Failed);
            assert_eq!(report.failure().unwrap().code, expected_code.unwrap());
            assert!(matches!(report.outcome(), MultiRoleOutcome::Failed(_)));
        }
    }
}

#[tokio::test]
async fn planner_and_reviewer_factory_failures_preserve_report_state_without_fallback_engine() {
    let checkpoint = WorkspaceCheckpoint::new(WorkspaceFingerprint::from_bytes([0x42; 32]));
    let responses = scenario_responses(0, true, &checkpoint);
    let provider = Arc::new(ScriptedProvider::new(responses));
    let runtime = Arc::new(StableRuntime::new(&checkpoint));
    let events = Arc::new(RecordingEvents::default());
    let guard = Arc::new(PassingFinalizationGuard::default());
    let reviewer_run = RoleRun::try_new(Role::Reviewer, 1).unwrap();
    let factory = Arc::new(RecordingEngineFactory::failing(
        provider.clone(),
        runtime,
        events.clone(),
        reviewer_run,
    ));
    let reviewer_catalog = catalog();
    let report = MultiRoleOrchestrator::new(factory.clone(), guard.clone())
        .run(
            MultiRoleInput {
                task_prompt: "task",
                repository_context: "repository",
                checkpoint,
                repository_check_catalog: &reviewer_catalog,
            },
            CancellationToken::new(),
        )
        .await;

    assert_eq!(report.status(), TaskStatus::Failed);
    assert_eq!(report.delivery_readiness(), DeliveryReadiness::Unreviewed);
    assert_eq!(report.failure().unwrap().code, "REVIEWER_RUNTIME_FAILED");
    assert!(report.failure().unwrap().retryable);
    assert_eq!(report.final_workspace_generation(), 0);
    assert_eq!(report.required_checks().unwrap().checks().len(), 1);
    assert_eq!(factory.calls().last().unwrap().role_run, reviewer_run);
    assert_eq!(provider.remaining(), 6);
    assert_eq!(guard.calls(), 0);
    assert!(!events.ordinary().iter().any(|event| {
        matches!(
            event,
            RoleEvent::Activity(activity) if activity.role() == Role::Reviewer
        )
    }));
    assert!(matches!(report.outcome(), MultiRoleOutcome::Failed(_)));

    let checkpoint = WorkspaceCheckpoint::new(WorkspaceFingerprint::from_bytes([0x42; 32]));
    let responses = scenario_responses(0, true, &checkpoint);
    let provider = Arc::new(ScriptedProvider::new(responses));
    let runtime = Arc::new(StableRuntime::new(&checkpoint));
    let events = Arc::new(RecordingEvents::default());
    let guard = Arc::new(PassingFinalizationGuard::default());
    let planner_run = RoleRun::try_new(Role::Planner, 1).unwrap();
    let factory = Arc::new(RecordingEngineFactory::failing(
        provider.clone(),
        runtime,
        events.clone(),
        planner_run,
    ));
    let planner_catalog = catalog();
    let report = MultiRoleOrchestrator::new(factory.clone(), guard.clone())
        .run(
            MultiRoleInput {
                task_prompt: "task",
                repository_context: "repository",
                checkpoint,
                repository_check_catalog: &planner_catalog,
            },
            CancellationToken::new(),
        )
        .await;

    assert_eq!(report.status(), TaskStatus::Failed);
    assert_eq!(report.failure().unwrap().code, "PLANNER_RUNTIME_FAILED");
    assert!(report.failure().unwrap().retryable);
    assert_eq!(report.final_workspace_generation(), 0);
    assert_eq!(report.required_checks(), None);
    assert_eq!(provider.remaining(), 10);
    assert_eq!(factory.calls().len(), 1);
    assert_eq!(factory.calls()[0].role_run, planner_run);
    assert!(events.ordinary().is_empty());
    assert_eq!(guard.calls(), 0);
}

#[tokio::test]
async fn third_reviewer_invalid_response_is_failed_and_never_review_rejected() {
    let checkpoint = WorkspaceCheckpoint::new(WorkspaceFingerprint::from_bytes([0x42; 32]));
    let mut responses = scenario_responses(2, false, &checkpoint);
    *responses.last_mut().unwrap() = ModelResponse::Final {
        content: "invalid normal assistant final".to_owned(),
    };
    let events = Arc::new(RecordingEvents::default());
    let guard = Arc::new(PassingFinalizationGuard::default());
    let guard_port: Arc<dyn FinalizationGuard> = guard.clone();
    let (report, provider, _runtime, factory) = run_with_ports(responses, events, guard_port).await;

    assert_eq!(report.status(), TaskStatus::Failed);
    assert_eq!(report.delivery_readiness(), DeliveryReadiness::Unreviewed);
    assert_eq!(report.failure().unwrap().code, "REVIEWER_INVALID_OUTPUT");
    assert_eq!(report.required_checks().unwrap().checks().len(), 2);
    assert_eq!(provider.remaining(), 0);
    assert_eq!(factory.calls().len(), 7);
    assert_eq!(guard.calls(), 0);
    assert!(matches!(report.outcome(), MultiRoleOutcome::Failed(_)));
}

#[tokio::test]
async fn shared_task_budget_exhaustion_at_rework_executor_is_failed_not_rejected() {
    let responses = rework_budget_exhaustion_responses();
    let events = Arc::new(RecordingEvents::default());
    let guard = Arc::new(PassingFinalizationGuard::default());
    let guard_port: Arc<dyn FinalizationGuard> = guard.clone();
    let (report, provider, _runtime, factory) =
        run_with_ports(responses, events.clone(), guard_port).await;

    assert_eq!(report.status(), TaskStatus::Failed);
    assert_eq!(report.delivery_readiness(), DeliveryReadiness::Unreviewed);
    assert_eq!(
        report.failure().unwrap().code,
        "EXECUTOR_TASK_BUDGET_EXHAUSTED"
    );
    assert_eq!(report.required_checks().unwrap().checks().len(), 6);
    assert_eq!(provider.remaining(), 0);
    assert_eq!(guard.calls(), 0);
    let executor_three = RoleRun::try_new(Role::Executor, 3).unwrap();
    assert_eq!(factory.calls().last().unwrap().role_run, executor_three);
    let MultiRoleOutcome::Failed(failure) = report.outcome() else {
        panic!("shared task budget exhaustion must be a Failed outcome");
    };
    assert_eq!(failure.role_run(), executor_three);
    assert!(!events.ordinary().iter().any(|event| {
        matches!(
            event,
            RoleEvent::Activity(activity)
                if activity.role() == Role::Executor && activity.role_run() == 3
        )
    }));
}

#[tokio::test]
async fn late_cancellation_during_reviewer_factory_never_starts_reviewer() {
    let checkpoint = WorkspaceCheckpoint::new(WorkspaceFingerprint::from_bytes([0x42; 32]));
    let responses = scenario_responses(0, true, &checkpoint);
    let provider = Arc::new(ScriptedProvider::new(responses));
    let runtime = Arc::new(StableRuntime::new(&checkpoint));
    let events = Arc::new(RecordingEvents::default());
    let guard = Arc::new(PassingFinalizationGuard::default());
    let cancellation = CancellationToken::new();
    let reviewer_run = RoleRun::try_new(Role::Reviewer, 1).unwrap();
    let factory = Arc::new(RecordingEngineFactory::cancelling(
        provider.clone(),
        runtime,
        events.clone(),
        reviewer_run,
        cancellation.clone(),
    ));
    let catalog = catalog();
    let report = MultiRoleOrchestrator::new(factory.clone(), guard.clone())
        .run(
            MultiRoleInput {
                task_prompt: "task",
                repository_context: "repository",
                checkpoint,
                repository_check_catalog: &catalog,
            },
            cancellation,
        )
        .await;

    assert_eq!(report.status(), TaskStatus::Cancelled);
    assert_eq!(report.delivery_readiness(), DeliveryReadiness::Unreviewed);
    assert_eq!(report.failure(), None);
    assert_eq!(report.required_checks().unwrap().checks().len(), 1);
    assert_eq!(factory.calls().last().unwrap().role_run, reviewer_run);
    assert_eq!(provider.remaining(), 6);
    assert_eq!(guard.calls(), 0);
    assert!(!events.ordinary().iter().any(|event| {
        matches!(
            event,
            RoleEvent::Activity(activity) if activity.role() == Role::Reviewer
        )
    }));
    assert!(matches!(report.outcome(), MultiRoleOutcome::Cancelled));
}
