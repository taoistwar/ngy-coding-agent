use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use coding_agent_core::{
    ActionRequest, BudgetError, BudgetResource, ContextRedactor, ContinuationHandoff, DiffEvent,
    DiffFile, DiffFileStatus, DurableCheckpointAck, DurableEventAck, DurableRoleEvent,
    ExecutorRoleInput, ExecutorRoleLoop, ExecutorRoleOutcome, ModelMessage, ModelRequest,
    ModelResponse, ModelToolChoice, PlannerHandoff, PlannerRoleInput, PlannerRoleLoop,
    PlannerRoleOutcome, PreparedModelProvider, PreparedProviderRequest, ProviderError,
    RawProviderResponse, RequiredAction, RequiredBudgetAction, RequiredCheckLedger,
    RetainedToolResult, ReviewDiffBundle, ReviewDiffCheckpoint, ReviewDiffChunkRequest,
    ReviewDiffInputFile, ReviewerRoleInput, ReviewerRoleLoop, ReviewerRoleOutcome, Role,
    RoleActionRuntime, RoleActivityEvent, RoleEngine, RoleEvent, RoleEventSink, RoleHandoff,
    RoleLoopError, RoleRun, RoleRuntimeResult, RoleTranscript, RuntimeActionRequest, RuntimeError,
    TaskBudgetLedger, TerminalSnapshot, ToolCall, ToolCallBatch, ToolRequest, ToolResult,
    ToolStatus, ValidatedExecution, ValidationObservation, WorkspaceCheckpoint,
    WorkspaceFingerprint, project_test_snapshot,
};
use coding_agent_domain::{
    CheckEvidenceStatus, FindingSeverity, NewReviewEvidence, PlanItem, PlanItemStatus,
    PlanSnapshot, RequiredCheck, RequiredCheckSelector, ReviewDecisionSource, ReviewFinding,
    ReviewVerdict, TestStatus,
};
use tokio_util::sync::CancellationToken;

struct IdentityRedactor;

impl ContextRedactor for IdentityRedactor {
    fn redact(&self, content: &str) -> String {
        content.to_owned()
    }
}

struct WrapperBoundaryRedactor;

impl ContextRedactor for WrapperBoundaryRedactor {
    fn redact(&self, content: &str) -> String {
        if content.contains("\"role\":\"tool\"") {
            content.replace("\"role\":\"tool\"", "\"role\":\"<redacted>\"")
        } else {
            content.to_owned()
        }
    }
}

struct SecretRedactor;

impl ContextRedactor for SecretRedactor {
    fn redact(&self, content: &str) -> String {
        content.replace("TOP_SECRET", "[REDACTED]")
    }
}

enum SendScript {
    Raw {
        encoded_len: usize,
        decoded: Result<ModelResponse, ProviderError>,
        cancel_before_return: bool,
    },
    Transport {
        error: ProviderError,
        cancel_before_return: bool,
    },
}

struct ProviderScript {
    request_len: usize,
    maximum_response_bytes: usize,
    send: SendScript,
}

impl ProviderScript {
    fn response(request_len: usize, encoded_len: usize, response: ModelResponse) -> Self {
        Self {
            request_len,
            maximum_response_bytes: 64 * 1024,
            send: SendScript::Raw {
                encoded_len,
                decoded: Ok(response),
                cancel_before_return: false,
            },
        }
    }

    fn response_then_cancel(
        request_len: usize,
        encoded_len: usize,
        response: ModelResponse,
    ) -> Self {
        Self {
            request_len,
            maximum_response_bytes: 64 * 1024,
            send: SendScript::Raw {
                encoded_len,
                decoded: Ok(response),
                cancel_before_return: true,
            },
        }
    }

    fn decode_error(request_len: usize, encoded_len: usize) -> Self {
        Self {
            request_len,
            maximum_response_bytes: 64 * 1024,
            send: SendScript::Raw {
                encoded_len,
                decoded: Err(ProviderError::new(
                    "SCRIPTED_DECODE_FAILED",
                    "scripted malformed response",
                    false,
                )),
                cancel_before_return: false,
            },
        }
    }

    fn transport_error(request_len: usize) -> Self {
        Self {
            request_len,
            maximum_response_bytes: 64 * 1024,
            send: SendScript::Transport {
                error: ProviderError::new(
                    "SCRIPTED_TRANSPORT_FAILED",
                    "scripted no-response transport failure",
                    false,
                ),
                cancel_before_return: false,
            },
        }
    }

    fn transport_error_then_cancel(request_len: usize) -> Self {
        Self {
            request_len,
            maximum_response_bytes: 64 * 1024,
            send: SendScript::Transport {
                error: ProviderError::new(
                    "SCRIPTED_TRANSPORT_FAILED",
                    "scripted no-response transport failure",
                    false,
                ),
                cancel_before_return: true,
            },
        }
    }
}

struct ScriptedPreparedRequest {
    script: ProviderScript,
    sends: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl PreparedProviderRequest for ScriptedPreparedRequest {
    fn encoded_len(&self) -> usize {
        self.script.request_len
    }

    fn maximum_response_bytes(&self) -> usize {
        self.script.maximum_response_bytes
    }

    async fn send(
        self: Box<Self>,
        cancellation: CancellationToken,
    ) -> Result<Box<dyn RawProviderResponse>, ProviderError> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        match self.script.send {
            SendScript::Raw {
                encoded_len,
                decoded,
                cancel_before_return,
            } => {
                if cancel_before_return {
                    cancellation.cancel();
                }
                Ok(Box::new(ScriptedRawResponse {
                    encoded_len,
                    decoded,
                }))
            }
            SendScript::Transport {
                error,
                cancel_before_return,
            } => {
                if cancel_before_return {
                    cancellation.cancel();
                }
                Err(error)
            }
        }
    }
}

struct ScriptedRawResponse {
    encoded_len: usize,
    decoded: Result<ModelResponse, ProviderError>,
}

impl RawProviderResponse for ScriptedRawResponse {
    fn encoded_len(&self) -> usize {
        self.encoded_len
    }

    fn decode(self: Box<Self>) -> Result<ModelResponse, ProviderError> {
        self.decoded
    }
}

struct ScriptedProvider {
    scripts: Mutex<VecDeque<ProviderScript>>,
    requests: Mutex<Vec<ModelRequest>>,
    sends: Arc<AtomicU64>,
}

impl ScriptedProvider {
    fn new(scripts: Vec<ProviderScript>) -> Self {
        Self {
            scripts: Mutex::new(scripts.into()),
            requests: Mutex::new(Vec::new()),
            sends: Arc::new(AtomicU64::new(0)),
        }
    }

    fn requests(&self) -> Vec<ModelRequest> {
        self.requests.lock().unwrap().clone()
    }

    fn sends(&self) -> u64 {
        self.sends.load(Ordering::SeqCst)
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
                "no scripted provider response remains",
                false,
            )
        })?;
        Ok(Box::new(ScriptedPreparedRequest {
            script,
            sends: Arc::clone(&self.sends),
        }))
    }
}

struct ScriptedRuntime {
    scripts: Mutex<VecDeque<Result<RoleRuntimeResult, RuntimeError>>>,
    requests: Mutex<Vec<RuntimeActionRequest>>,
    fingerprints: Mutex<VecDeque<Result<WorkspaceFingerprint, RuntimeError>>>,
    last_fingerprint: Mutex<WorkspaceFingerprint>,
    terminal_snapshots: Mutex<VecDeque<Result<TerminalSnapshot, RuntimeError>>>,
    terminal_manifests:
        Mutex<VecDeque<Result<coding_agent_core::ReviewDiffManifest, RuntimeError>>>,
    cancel_before_return: bool,
}

impl ScriptedRuntime {
    fn tool_results(contents: &[&str]) -> Self {
        Self {
            scripts: Mutex::new(
                contents
                    .iter()
                    .map(|content| Ok(RoleRuntimeResult::Tool(ToolResult::text(*content))))
                    .collect(),
            ),
            requests: Mutex::new(Vec::new()),
            fingerprints: Mutex::new(VecDeque::new()),
            last_fingerprint: Mutex::new(WorkspaceFingerprint::from_bytes([0x42; 32])),
            terminal_snapshots: Mutex::new(VecDeque::new()),
            terminal_manifests: Mutex::new(VecDeque::new()),
            cancel_before_return: false,
        }
    }

    fn with_result(result: Result<RoleRuntimeResult, RuntimeError>) -> Self {
        Self {
            scripts: Mutex::new(VecDeque::from([result])),
            requests: Mutex::new(Vec::new()),
            fingerprints: Mutex::new(VecDeque::new()),
            last_fingerprint: Mutex::new(WorkspaceFingerprint::from_bytes([0x42; 32])),
            terminal_snapshots: Mutex::new(VecDeque::new()),
            terminal_manifests: Mutex::new(VecDeque::new()),
            cancel_before_return: false,
        }
    }

    fn with_result_then_cancel(result: RoleRuntimeResult) -> Self {
        Self {
            scripts: Mutex::new(VecDeque::from([Ok(result)])),
            requests: Mutex::new(Vec::new()),
            fingerprints: Mutex::new(VecDeque::new()),
            last_fingerprint: Mutex::new(WorkspaceFingerprint::from_bytes([0x42; 32])),
            terminal_snapshots: Mutex::new(VecDeque::new()),
            terminal_manifests: Mutex::new(VecDeque::new()),
            cancel_before_return: true,
        }
    }

    fn with_scripts_and_fingerprints(
        scripts: Vec<Result<RoleRuntimeResult, RuntimeError>>,
        fingerprints: Vec<WorkspaceFingerprint>,
    ) -> Self {
        Self::with_scripts_and_fingerprint_results(
            scripts,
            fingerprints.into_iter().map(Ok).collect(),
        )
    }

    fn with_scripts_and_fingerprint_results(
        scripts: Vec<Result<RoleRuntimeResult, RuntimeError>>,
        fingerprints: Vec<Result<WorkspaceFingerprint, RuntimeError>>,
    ) -> Self {
        Self {
            scripts: Mutex::new(scripts.into()),
            requests: Mutex::new(Vec::new()),
            fingerprints: Mutex::new(fingerprints.into()),
            last_fingerprint: Mutex::new(WorkspaceFingerprint::from_bytes([0x42; 32])),
            terminal_snapshots: Mutex::new(VecDeque::new()),
            terminal_manifests: Mutex::new(VecDeque::new()),
            cancel_before_return: false,
        }
    }

    fn reviewer(
        scripts: Vec<Result<RoleRuntimeResult, RuntimeError>>,
        fingerprints: Vec<WorkspaceFingerprint>,
        terminal_snapshots: Vec<Result<TerminalSnapshot, RuntimeError>>,
        terminal_manifests: Vec<Result<coding_agent_core::ReviewDiffManifest, RuntimeError>>,
    ) -> Self {
        Self {
            scripts: Mutex::new(scripts.into()),
            requests: Mutex::new(Vec::new()),
            fingerprints: Mutex::new(fingerprints.into_iter().map(Ok).collect()),
            last_fingerprint: Mutex::new(WorkspaceFingerprint::from_bytes([0x42; 32])),
            terminal_snapshots: Mutex::new(terminal_snapshots.into()),
            terminal_manifests: Mutex::new(terminal_manifests.into()),
            cancel_before_return: false,
        }
    }

    fn requests(&self) -> Vec<RuntimeActionRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl RoleActionRuntime for ScriptedRuntime {
    async fn invoke(
        &self,
        request: RuntimeActionRequest,
        cancellation: CancellationToken,
    ) -> Result<RoleRuntimeResult, RuntimeError> {
        self.requests.lock().unwrap().push(request);
        let result = self.scripts.lock().unwrap().pop_front().unwrap_or_else(|| {
            Ok(RoleRuntimeResult::Tool(ToolResult::text(
                "default scripted result",
            )))
        });
        if self.cancel_before_return {
            cancellation.cancel();
        }
        result
    }

    async fn workspace_fingerprint(
        &self,
        _cancellation: CancellationToken,
    ) -> Result<WorkspaceFingerprint, RuntimeError> {
        if let Some(fingerprint) = self.fingerprints.lock().unwrap().pop_front() {
            let fingerprint = fingerprint?;
            *self.last_fingerprint.lock().unwrap() = fingerprint;
            return Ok(fingerprint);
        }
        Ok(*self.last_fingerprint.lock().unwrap())
    }

    async fn terminal_snapshot(
        &self,
        generation: u64,
        _cancellation: CancellationToken,
    ) -> Result<TerminalSnapshot, RuntimeError> {
        if let Some(snapshot) = self.terminal_snapshots.lock().unwrap().pop_front() {
            return snapshot;
        }
        Ok(TerminalSnapshot {
            fingerprint: WorkspaceFingerprint::from_bytes([0x42; 32]),
            diff: coding_agent_core::DiffEvent {
                revision: generation,
                files: Vec::new(),
            },
        })
    }

    async fn terminal_review_diff_manifest(
        &self,
        checkpoint: coding_agent_core::ReviewDiffCheckpoint,
        _cancellation: CancellationToken,
    ) -> Result<coding_agent_core::ReviewDiffManifest, RuntimeError> {
        if let Some(manifest) = self.terminal_manifests.lock().unwrap().pop_front() {
            return manifest;
        }
        Ok(
            coding_agent_core::ReviewDiffBundle::try_new(
                &checkpoint,
                Vec::new(),
                &IdentityRedactor,
            )
            .unwrap()
            .manifest()
            .clone(),
        )
    }
}

#[derive(Default)]
struct RecordingEvents {
    ordinary: Mutex<Vec<RoleEvent>>,
    durable: Mutex<Vec<DurableRoleEvent>>,
    flush_generations: Mutex<Vec<u64>>,
    advance_on_flush: Mutex<Option<(Arc<ScriptedRuntime>, WorkspaceFingerprint)>>,
    fail_ordinary_at: AtomicU64,
    ordinary_emissions: AtomicU64,
    fail_durable: AtomicBool,
    fail_flush: AtomicBool,
    wrong_flush_generation: AtomicBool,
    cancel_ordinary: AtomicBool,
    cancel_durable: AtomicBool,
    cancel_flush: AtomicBool,
    next_sequence: AtomicU64,
}

impl RecordingEvents {
    fn failing_ordinary_at(emission: u64) -> Self {
        Self {
            fail_ordinary_at: AtomicU64::new(emission),
            ..Self::default()
        }
    }

    fn failing_durable() -> Self {
        Self {
            fail_durable: AtomicBool::new(true),
            ..Self::default()
        }
    }

    fn cancelling_ordinary() -> Self {
        Self {
            cancel_ordinary: AtomicBool::new(true),
            ..Self::default()
        }
    }

    fn cancelling_durable() -> Self {
        Self {
            cancel_durable: AtomicBool::new(true),
            ..Self::default()
        }
    }

    fn failing_flush() -> Self {
        Self {
            fail_flush: AtomicBool::new(true),
            ..Self::default()
        }
    }

    fn wrong_flush_generation() -> Self {
        Self {
            wrong_flush_generation: AtomicBool::new(true),
            ..Self::default()
        }
    }

    fn cancelling_flush() -> Self {
        Self {
            cancel_flush: AtomicBool::new(true),
            ..Self::default()
        }
    }

    fn advancing_flush(runtime: Arc<ScriptedRuntime>, fingerprint: WorkspaceFingerprint) -> Self {
        Self {
            advance_on_flush: Mutex::new(Some((runtime, fingerprint))),
            ..Self::default()
        }
    }

    fn ordinary(&self) -> Vec<RoleEvent> {
        self.ordinary.lock().unwrap().clone()
    }

    fn durable(&self) -> Vec<DurableRoleEvent> {
        self.durable.lock().unwrap().clone()
    }

    fn flush_generations(&self) -> Vec<u64> {
        self.flush_generations.lock().unwrap().clone()
    }

    fn next_sequence(&self) -> u64 {
        self.next_sequence.fetch_add(1, Ordering::SeqCst) + 1
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
        let emission = self.ordinary_emissions.fetch_add(1, Ordering::SeqCst) + 1;
        if emission == self.fail_ordinary_at.load(Ordering::SeqCst) {
            return Err(RuntimeError::new(
                "SCRIPTED_ORDINARY_FAILURE",
                "the ordinary event was not accepted",
                false,
            ));
        }
        if self.cancel_ordinary.load(Ordering::SeqCst) {
            return Err(RuntimeError::new(
                "COMMAND_CANCELLED",
                "ordinary event emission was cancelled",
                false,
            ));
        }
        Ok(())
    }

    async fn emit_durable(
        &self,
        event: DurableRoleEvent,
        _cancellation: CancellationToken,
    ) -> Result<DurableEventAck, RuntimeError> {
        self.durable.lock().unwrap().push(event);
        if self.cancel_durable.load(Ordering::SeqCst) {
            return Err(RuntimeError::new(
                "COMMAND_CANCELLED",
                "durable event emission was cancelled",
                false,
            ));
        }
        if self.fail_durable.load(Ordering::SeqCst) {
            return Err(RuntimeError::new(
                "SCRIPTED_DURABLE_FAILURE",
                "the durable event was not acknowledged",
                false,
            ));
        }
        DurableEventAck::try_new(self.next_sequence())
    }

    async fn flush_checkpoint(
        &self,
        generation: u64,
        cancellation: CancellationToken,
    ) -> Result<DurableCheckpointAck, RuntimeError> {
        self.flush_generations.lock().unwrap().push(generation);
        if self.fail_flush.load(Ordering::SeqCst) {
            return Err(RuntimeError::new(
                "SCRIPTED_FLUSH_FAILURE",
                "the checkpoint barrier failed",
                false,
            ));
        }
        let generation = if self.wrong_flush_generation.load(Ordering::SeqCst) {
            generation.saturating_add(1)
        } else {
            generation
        };
        if self.cancel_flush.load(Ordering::SeqCst) {
            cancellation.cancel();
        }
        if let Some((runtime, fingerprint)) = self.advance_on_flush.lock().unwrap().take() {
            runtime
                .fingerprints
                .lock()
                .unwrap()
                .push_front(Ok(fingerprint));
        }
        DurableCheckpointAck::try_new(self.next_sequence(), generation)
    }
}

fn checkpoint() -> WorkspaceCheckpoint {
    WorkspaceCheckpoint::new(WorkspaceFingerprint::from_bytes([0x42; 32]))
}

fn catalog() -> Vec<RequiredCheckSelector> {
    vec![
        RequiredCheckSelector::try_cargo_test(
            Some("coding-agent-core".to_owned()),
            Some("role_loop".to_owned()),
        )
        .unwrap(),
        RequiredCheckSelector::try_cargo_check(Some("coding-agent-core".to_owned())).unwrap(),
    ]
}

fn structured_check() -> RequiredCheck {
    RequiredCheck::try_cargo_test(
        "check-01",
        Some("coding-agent-core".to_owned()),
        Some("role_loop".to_owned()),
    )
    .unwrap()
}

fn structured_plan() -> PlanSnapshot {
    PlanSnapshot::try_structured(
        1,
        "Implement the reusable role engine",
        vec![
            PlanItem::try_structured(
                "step-01",
                "Implement role engine",
                "Keep role transcripts isolated.",
                vec!["Each role starts from a fresh handoff.".to_owned()],
                PlanItemStatus::Pending,
            )
            .unwrap(),
        ],
        vec![structured_check()],
    )
    .unwrap()
}

fn passed_executor_checks(check_count: usize) -> (WorkspaceCheckpoint, RequiredCheckLedger) {
    assert!((1..=16).contains(&check_count));
    let mut checks = vec![structured_check()];
    checks.extend((2..=check_count).map(|index| {
        RequiredCheck::try_cargo_test(
            format!("check-{index:02}"),
            Some(format!("executor-package-{index:02}")),
            None,
        )
        .unwrap()
    }));
    let mut checkpoint = checkpoint();
    let mut ledger = RequiredCheckLedger::try_new(checks.clone()).unwrap();
    for check in checks {
        ledger.queue_check(&mut checkpoint, check.id()).unwrap();
        let token = ledger
            .mark_check_running(
                &checkpoint,
                check.id(),
                coding_agent_domain::CheckActor::Executor,
                1,
            )
            .unwrap();
        ledger
            .finish_check(
                &mut checkpoint,
                token,
                CheckEvidenceStatus::Passed,
                1,
                "passed",
                false,
            )
            .unwrap();
    }
    (checkpoint, ledger)
}

fn planner_transcript() -> RoleTranscript {
    let checkpoint = checkpoint();
    let catalog = catalog();
    let handoff = PlannerHandoff::try_new(
        "task",
        "repository",
        &checkpoint,
        &catalog,
        &IdentityRedactor,
    )
    .unwrap();
    RoleTranscript::try_for_planner(
        RoleRun::try_new(Role::Planner, 1).unwrap(),
        "planner policy",
        handoff,
        &IdentityRedactor,
    )
    .unwrap()
}

fn continuation_transcript(role: Role, role_run: u32) -> RoleTranscript {
    let handoff = ContinuationHandoff::try_new(
        role,
        "task",
        "repository",
        &structured_plan(),
        &checkpoint(),
        &IdentityRedactor,
    )
    .unwrap();
    RoleTranscript::try_fresh(
        RoleRun::try_new(role, role_run).unwrap(),
        format!("{role:?} policy"),
        RoleHandoff::Continuation(handoff),
        &IdentityRedactor,
    )
    .unwrap()
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

fn validation_call(id: &str, check: &RequiredCheck) -> ToolCall {
    ToolCall {
        id: id.to_owned(),
        request: ActionRequest::Runtime(RuntimeActionRequest::Validation {
            check: check.clone(),
        }),
    }
}

fn validation_result(check: &RequiredCheck) -> RoleRuntimeResult {
    RoleRuntimeResult::Validation(
        ValidationObservation::try_new(
            ToolResult::text("passed"),
            check.clone(),
            CheckEvidenceStatus::Passed,
            1,
            false,
        )
        .unwrap(),
    )
}

fn failed_validation_result(check: &RequiredCheck) -> RoleRuntimeResult {
    RoleRuntimeResult::Validation(
        ValidationObservation::try_new(
            ToolResult::failed_text("failed"),
            check.clone(),
            CheckEvidenceStatus::Failed,
            2,
            false,
        )
        .unwrap(),
    )
}

fn retained_result(id: &str, content: &str) -> RetainedToolResult {
    RetainedToolResult::try_from_parts(id, content, ToolStatus::Succeeded, false, &IdentityRedactor)
        .unwrap()
}

fn plan_call(id: &str) -> ToolCall {
    let arguments = serde_json::json!({
        "summary": "Implement the bounded Planner handoff",
        "steps": [{
            "title": "Implement role loop",
            "description": "Keep the task session and budget shared.",
            "acceptance_criteria": ["The Planner plan is durably acknowledged."]
        }],
        "initial_required_checks": [{
            "kind": "cargo_test",
            "package": "coding-agent-core",
            "integration_test": "role_loop"
        }]
    });
    ToolCall {
        id: id.to_owned(),
        request: ActionRequest::decode(Role::Planner, "submit_plan", &arguments.to_string())
            .unwrap(),
    }
}

fn blocked_call(id: &str) -> ToolCall {
    ToolCall {
        id: id.to_owned(),
        request: ActionRequest::decode(
            Role::Planner,
            "report_blocked",
            r#"{"reason":"missing_required_context","summary":"required context is unavailable"}"#,
        )
        .unwrap(),
    }
}

fn executor_progress_call(id: &str, step_id: &str, status: &str) -> ToolCall {
    ToolCall {
        id: id.to_owned(),
        request: ActionRequest::decode(
            Role::Executor,
            "update_plan_progress",
            &serde_json::json!({
                "updates": [{"step_id": step_id, "status": status}]
            })
            .to_string(),
        )
        .unwrap(),
    }
}

fn executor_validation_selector_call(id: &str, check: &RequiredCheck) -> ToolCall {
    let arguments = match check.selector().kind() {
        coding_agent_domain::RequiredCheckKind::CargoCheck => {
            serde_json::json!({"package": check.package()})
        }
        coding_agent_domain::RequiredCheckKind::CargoTest => serde_json::json!({
            "package": check.package(),
            "integration_test": check.integration_test()
        }),
    };
    ToolCall {
        id: id.to_owned(),
        request: ActionRequest::decode(
            Role::Executor,
            if check.is_cargo_test() {
                "cargo_test"
            } else {
                "cargo_check"
            },
            &arguments.to_string(),
        )
        .unwrap(),
    }
}

fn executor_submission_call(id: &str, summary: &str) -> ToolCall {
    ToolCall {
        id: id.to_owned(),
        request: ActionRequest::decode(
            Role::Executor,
            "submit_execution",
            &serde_json::json!({"summary": summary}).to_string(),
        )
        .unwrap(),
    }
}

fn executor_blocked_call(id: &str) -> ToolCall {
    ToolCall {
        id: id.to_owned(),
        request: ActionRequest::decode(
            Role::Executor,
            "report_blocked",
            r#"{"reason":"missing_required_context","summary":"context unavailable"}"#,
        )
        .unwrap(),
    }
}

fn replace_call(id: &str, content: &str) -> ToolCall {
    ToolCall {
        id: id.to_owned(),
        request: ActionRequest::runtime(ToolRequest::ReplaceFile {
            path: "src/lib.rs".to_owned(),
            expected_sha256: None,
            content: content.to_owned(),
        }),
    }
}

fn batch(calls: Vec<ToolCall>) -> ModelResponse {
    ModelResponse::ToolCalls(ToolCallBatch {
        assistant_content: None,
        reasoning_content: None,
        calls,
    })
}

fn make_loop(
    provider: Arc<ScriptedProvider>,
    runtime: Arc<ScriptedRuntime>,
    events: Arc<RecordingEvents>,
    redactor: Arc<dyn ContextRedactor>,
) -> PlannerRoleLoop {
    PlannerRoleLoop::new(provider, runtime, events, redactor)
}

fn make_executor_loop(
    provider: Arc<ScriptedProvider>,
    runtime: Arc<ScriptedRuntime>,
    events: Arc<RecordingEvents>,
) -> ExecutorRoleLoop {
    ExecutorRoleLoop::new(provider, runtime, events, Arc::new(IdentityRedactor))
}

fn make_reviewer_loop(
    provider: Arc<ScriptedProvider>,
    runtime: Arc<ScriptedRuntime>,
    events: Arc<RecordingEvents>,
) -> ReviewerRoleLoop {
    ReviewerRoleLoop::new(provider, runtime, events, Arc::new(IdentityRedactor))
}

async fn prepared_reviewer_state() -> (
    PlanSnapshot,
    WorkspaceCheckpoint,
    RequiredCheckLedger,
    ValidatedExecution,
    TaskBudgetLedger,
) {
    let provider = Arc::new(ScriptedProvider::new(vec![
        ProviderScript::response(
            256,
            128,
            batch(vec![executor_progress_call(
                "prepare-progress",
                "step-01",
                "completed",
            )]),
        ),
        ProviderScript::response(
            512,
            128,
            batch(vec![executor_submission_call(
                "prepare-submit",
                "reviewer handoff",
            )]),
        ),
    ]));
    let runtime = Arc::new(ScriptedRuntime::tool_results(&[]));
    let events = Arc::new(RecordingEvents::default());
    let executor = make_executor_loop(provider, runtime, events);
    let mut plan = structured_plan();
    let (mut checkpoint, mut checks) = passed_executor_checks(1);
    let mut ledger = TaskBudgetLedger::try_new().unwrap();
    let outcome = executor
        .run(
            ExecutorRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &mut plan,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                latest_reviewer_findings: None,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let ExecutorRoleOutcome::Submitted(execution) = outcome else {
        panic!("reviewer setup executor must submit");
    };
    (plan, checkpoint, checks, execution, ledger)
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

fn reviewer_chunk_call(
    id: &str,
    manifest: &coding_agent_core::ReviewDiffManifest,
    start_chunk: u8,
    count: u8,
) -> ToolCall {
    ToolCall {
        id: id.to_owned(),
        request: ActionRequest::Runtime(RuntimeActionRequest::ReviewDiffChunks {
            generation: manifest.generation(),
            workspace_digest: manifest.workspace_digest().clone(),
            manifest_sha256: manifest.manifest_sha256().to_owned(),
            start_chunk,
            count,
        }),
    }
}

fn reviewer_submission_call(
    id: &str,
    verdict: &str,
    add_required_checks: serde_json::Value,
) -> ToolCall {
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

fn reviewer_optional_check_call(id: &str, package: &str) -> ToolCall {
    ToolCall {
        id: id.to_owned(),
        request: ActionRequest::decode(
            Role::Reviewer,
            "cargo_test",
            &serde_json::json!({
                "package": package,
                "integration_test": null
            })
            .to_string(),
        )
        .unwrap(),
    }
}

fn review_fixture(
    checkpoint: &WorkspaceCheckpoint,
    patch: String,
) -> (ReviewDiffBundle, TerminalSnapshot) {
    let input =
        ReviewDiffInputFile::try_new("src/lib.rs", DiffFileStatus::Modified, 1, 0, patch.clone())
            .unwrap();
    let authority = ReviewDiffCheckpoint::from_workspace_checkpoint(checkpoint);
    let bundle = ReviewDiffBundle::try_new(&authority, vec![input], &IdentityRedactor).unwrap();
    let terminal = TerminalSnapshot {
        fingerprint: checkpoint.fingerprint(),
        diff: DiffEvent {
            revision: checkpoint.generation(),
            files: vec![DiffFile {
                path: "src/lib.rs".to_owned(),
                status: DiffFileStatus::Modified,
                patch,
                additions: 1,
                deletions: 0,
                truncated: false,
            }],
        },
    };
    (bundle, terminal)
}

fn four_reviewer_reads() -> Vec<ProviderScript> {
    let mut scripts = (1..=3)
        .map(|index| {
            ProviderScript::response(
                index * 256,
                128,
                batch(vec![read_call(&format!("review-read-{index}"))]),
            )
        })
        .collect::<Vec<_>>();
    scripts.push(ProviderScript::response(
        1_024,
        128,
        batch(
            (0..8)
                .map(|index| read_call(&format!("reserved-read-{index}")))
                .collect(),
        ),
    ));
    scripts
}

#[test]
fn fresh_role_transcripts_isolate_history_and_reset_opaque_id_namespaces() {
    let plan = structured_plan();
    let checkpoint = checkpoint();
    let mut executor = RoleTranscript::try_fresh(
        RoleRun::try_new(Role::Executor, 1).unwrap(),
        "executor policy",
        ContinuationHandoff::try_new(
            Role::Executor,
            "task",
            "repository",
            &plan,
            &checkpoint,
            &IdentityRedactor,
        )
        .unwrap()
        .into(),
        &IdentityRedactor,
    )
    .unwrap();
    let executor_batch = ToolCallBatch {
        assistant_content: Some("executor-assistant-provider-request-id-123".to_owned()),
        reasoning_content: Some("opaque-executor-reasoning-state".to_owned()),
        calls: vec![read_call("same-opaque-id")],
    };
    executor
        .append_runtime_batch(
            executor_batch.clone(),
            vec![retained_result("same-opaque-id", "executor-private-result")],
        )
        .unwrap();
    assert!(executor.preflight_runtime_batch(&executor_batch).is_err());

    let mut reviewer = RoleTranscript::try_fresh(
        RoleRun::try_new(Role::Reviewer, 1).unwrap(),
        "reviewer policy",
        ContinuationHandoff::try_new(
            Role::Reviewer,
            "task",
            "repository",
            &plan,
            &checkpoint,
            &IdentityRedactor,
        )
        .unwrap()
        .into(),
        &IdentityRedactor,
    )
    .unwrap();
    let fresh_request = reviewer.request(ModelToolChoice::Auto);
    assert_eq!(fresh_request.messages.len(), 2);
    assert!(matches!(fresh_request.messages[0], ModelMessage::System(_)));
    let ModelMessage::User(handoff) = &fresh_request.messages[1] else {
        panic!("fresh Reviewer request must contain only its typed handoff");
    };
    for prior_turn_data in [
        "executor-assistant-provider-request-id-123",
        "opaque-executor-reasoning-state",
        "executor-private-result",
        "same-opaque-id",
    ] {
        assert!(!handoff.contains(prior_turn_data));
    }
    assert_eq!(fresh_request.allowed_actions.role(), Some(Role::Reviewer));

    let reviewer_batch = ToolCallBatch {
        assistant_content: None,
        reasoning_content: Some("reviewer-owned-reasoning-state".to_owned()),
        calls: vec![read_call("same-opaque-id")],
    };
    reviewer.preflight_runtime_batch(&reviewer_batch).unwrap();
    reviewer
        .append_runtime_batch(
            reviewer_batch,
            vec![retained_result("same-opaque-id", "reviewer result")],
        )
        .unwrap();

    let legacy = PlanSnapshot::legacy(
        1,
        vec![PlanItem::legacy(
            "legacy-step",
            "legacy",
            PlanItemStatus::Pending,
        )],
    );
    assert!(
        ContinuationHandoff::try_new(
            Role::Executor,
            "task",
            "repository",
            &legacy,
            &checkpoint,
            &IdentityRedactor,
        )
        .is_err()
    );
}

#[tokio::test]
async fn role_engine_rejects_batch_substitution_before_runtime_and_closes_the_exchange() {
    let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
        100,
        50,
        batch(vec![read_call("permit-bound")]),
    )]));
    let runtime = Arc::new(ScriptedRuntime::tool_results(&[]));
    let engine = RoleEngine::new(
        provider.clone(),
        runtime.clone(),
        Arc::new(RecordingEvents::default()),
        Arc::new(IdentityRedactor),
    );
    let mut transcript = planner_transcript();
    let mut ledger = TaskBudgetLedger::new();
    let mut lease = ledger.start_planner().unwrap();
    let (mut receipt, response) = engine
        .exploratory_exchange(
            &transcript,
            &mut ledger,
            &mut lease,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let ModelResponse::ToolCalls(bound_batch) = response else {
        panic!("scripted provider must return a runtime batch");
    };
    let permit = ledger
        .preflight_exploratory_runtime_batch(&mut lease, &mut receipt, bound_batch.calls.clone())
        .unwrap();
    let substituted = ToolCallBatch {
        assistant_content: bound_batch.assistant_content,
        reasoning_content: bound_batch.reasoning_content,
        calls: vec![read_call("substituted-id")],
    };

    let error = engine
        .execute_preflighted_runtime_batch(
            &mut transcript,
            substituted,
            permit,
            &mut ledger,
            &mut lease,
            &receipt,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RoleLoopError::Contract(_)));
    assert!(runtime.requests().is_empty());
    assert_eq!(ledger.usage().model_visible_calls(), 1);
    assert_eq!(ledger.usage().retained_result_bytes(), 0);
    assert_eq!(transcript.message_count(), 2);
    ledger.abort_role_on_failure(lease).unwrap();
    assert_eq!(ledger.active_role(), None);
}

#[tokio::test]
async fn role_engine_rejects_cross_role_transcript_and_releases_the_bound_batch() {
    let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
        100,
        50,
        batch(vec![read_call("bound")]),
    )]));
    let runtime = Arc::new(ScriptedRuntime::tool_results(&[]));
    let engine = RoleEngine::new(
        provider.clone(),
        runtime.clone(),
        Arc::new(RecordingEvents::default()),
        Arc::new(IdentityRedactor),
    );
    let planner = planner_transcript();
    let mut wrong_transcript = continuation_transcript(Role::Executor, 1);
    let mut ledger = TaskBudgetLedger::new();
    let mut lease = ledger.start_planner().unwrap();
    let (mut receipt, response) = engine
        .exploratory_exchange(&planner, &mut ledger, &mut lease, CancellationToken::new())
        .await
        .unwrap();
    let ModelResponse::ToolCalls(bound_batch) = response else {
        panic!("scripted provider must return a runtime batch");
    };
    let permit = ledger
        .preflight_exploratory_runtime_batch(&mut lease, &mut receipt, bound_batch.calls.clone())
        .unwrap();

    let error = engine
        .execute_preflighted_runtime_batch(
            &mut wrong_transcript,
            bound_batch,
            permit,
            &mut ledger,
            &mut lease,
            &receipt,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RoleLoopError::Contract(_)));
    assert!(runtime.requests().is_empty());
    assert_eq!(wrong_transcript.message_count(), 2);
    ledger.abort_role_on_failure(lease).unwrap();
    assert_eq!(ledger.active_role(), None);
}

#[tokio::test]
async fn role_engine_rejects_the_wrong_required_slot_before_provider_prepare() {
    let provider = Arc::new(ScriptedProvider::new(Vec::new()));
    let runtime = Arc::new(ScriptedRuntime::tool_results(&[]));
    let engine = RoleEngine::new(
        provider.clone(),
        runtime.clone(),
        Arc::new(RecordingEvents::default()),
        Arc::new(IdentityRedactor),
    );
    let transcript = planner_transcript();
    let mut ledger = TaskBudgetLedger::new();
    let mut lease = ledger.start_planner().unwrap();
    let mut action = ledger.begin_required_action(&mut lease).unwrap();

    let error = engine
        .required_exchange(
            &transcript,
            RequiredAction::Validation(structured_check()),
            &mut ledger,
            &mut lease,
            &mut action,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RoleLoopError::Contract(_)));
    assert!(provider.requests().is_empty());
    assert_eq!(provider.sends(), 0);
    assert!(runtime.requests().is_empty());
    assert_eq!(ledger.usage().model_responses(), 0);
    ledger.abort_role_on_failure(lease).unwrap();
    assert_eq!(ledger.active_role(), None);
}

#[tokio::test]
async fn required_runtime_failures_release_pending_results_and_close_the_exchange() {
    #[derive(Clone, Copy)]
    enum FailureCase {
        Runtime,
        CancelAfterSuccess,
        TypedMismatch,
        RetainedEncoding,
    }

    for case in [
        FailureCase::Runtime,
        FailureCase::CancelAfterSuccess,
        FailureCase::TypedMismatch,
        FailureCase::RetainedEncoding,
    ] {
        let check = structured_check();
        let runtime = match case {
            FailureCase::Runtime => Arc::new(ScriptedRuntime::with_result(Err(RuntimeError::new(
                "SCRIPTED_RUNTIME_FAILURE",
                "required validation failed",
                false,
            )))),
            FailureCase::CancelAfterSuccess => Arc::new(ScriptedRuntime::with_result_then_cancel(
                validation_result(&check),
            )),
            FailureCase::TypedMismatch => Arc::new(ScriptedRuntime::with_result(Ok(
                RoleRuntimeResult::Tool(ToolResult::text("wrong typed result")),
            ))),
            FailureCase::RetainedEncoding => {
                Arc::new(ScriptedRuntime::with_result(Ok(validation_result(&check))))
            }
        };
        let redactor: Arc<dyn ContextRedactor> = match case {
            FailureCase::RetainedEncoding => Arc::new(WrapperBoundaryRedactor),
            _ => Arc::new(IdentityRedactor),
        };
        let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
            100,
            50,
            batch(vec![validation_call("required-check", &check)]),
        )]));
        let engine = RoleEngine::new(
            provider.clone(),
            runtime.clone(),
            Arc::new(RecordingEvents::default()),
            redactor,
        );
        let mut transcript = continuation_transcript(Role::Executor, 1);
        let checks = RequiredCheckLedger::try_new(vec![check.clone()]).unwrap();
        let checkpoint = checkpoint();
        let mut ledger = TaskBudgetLedger::new();
        let mut lease = ledger.start_executor(1, &checks, &checkpoint).unwrap();
        let mut action = ledger.begin_required_action(&mut lease).unwrap();
        let required = RequiredAction::Validation(check.clone());
        let (mut receipt, response) = engine
            .required_exchange(
                &transcript,
                required.clone(),
                &mut ledger,
                &mut lease,
                &mut action,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let ModelResponse::ToolCalls(required_batch) = response else {
            panic!("scripted provider must return the required validation");
        };

        let error = engine
            .execute_required_runtime_action(
                &mut transcript,
                required_batch,
                &required,
                action,
                &mut ledger,
                &mut lease,
                &mut receipt,
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        match case {
            FailureCase::Runtime => assert!(matches!(error, RoleLoopError::Runtime(_))),
            FailureCase::CancelAfterSuccess => {
                assert!(matches!(error, RoleLoopError::Cancelled))
            }
            FailureCase::TypedMismatch => {
                assert!(matches!(error, RoleLoopError::RuntimeResultMismatch))
            }
            FailureCase::RetainedEncoding => {
                assert!(matches!(error, RoleLoopError::Retained(_)))
            }
        }
        assert_eq!(runtime.requests().len(), 1);
        assert_eq!(transcript.message_count(), 2);
        assert_eq!(ledger.usage().model_responses(), 1);
        assert_eq!(ledger.usage().model_visible_calls(), 1);
        assert_eq!(ledger.usage().provider_bytes(), 150);
        assert_eq!(ledger.usage().retained_result_bytes(), 0);
        ledger.abort_role_on_failure(lease).unwrap();
        assert_eq!(ledger.active_role(), None);
    }
}

#[tokio::test]
async fn required_validation_success_retains_appends_and_advances_the_exact_slot() {
    let check = structured_check();
    let expected = validation_result(&check);
    let runtime = Arc::new(ScriptedRuntime::with_result(Ok(expected.clone())));
    let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
        100,
        50,
        batch(vec![validation_call("required-check", &check)]),
    )]));
    let engine = RoleEngine::new(
        provider.clone(),
        runtime.clone(),
        Arc::new(RecordingEvents::default()),
        Arc::new(IdentityRedactor),
    );
    let mut transcript = continuation_transcript(Role::Executor, 1);
    let checks = RequiredCheckLedger::try_new(vec![check.clone()]).unwrap();
    let checkpoint = checkpoint();
    let mut ledger = TaskBudgetLedger::new();
    let mut lease = ledger.start_executor(1, &checks, &checkpoint).unwrap();
    let mut action = ledger.begin_required_action(&mut lease).unwrap();
    let required = RequiredAction::Validation(check);
    let (mut receipt, response) = engine
        .required_exchange(
            &transcript,
            required.clone(),
            &mut ledger,
            &mut lease,
            &mut action,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let ModelResponse::ToolCalls(required_batch) = response else {
        panic!("scripted provider must return the required validation");
    };

    let observed = engine
        .execute_required_runtime_action(
            &mut transcript,
            required_batch,
            &required,
            action,
            &mut ledger,
            &mut lease,
            &mut receipt,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(observed, expected);
    assert_eq!(runtime.requests().len(), 1);
    assert_eq!(transcript.message_count(), 4);
    let request = transcript.request(ModelToolChoice::Auto);
    assert!(matches!(
        &request.messages[2],
        ModelMessage::AssistantToolCalls(ToolCallBatch { calls, .. })
            if matches!(calls.as_slice(), [call] if call.id == "required-check")
    ));
    assert!(matches!(
        &request.messages[3],
        ModelMessage::ToolResult {
            tool_call_id,
            content,
        } if tool_call_id == "required-check"
            && content.starts_with("[tool_status=succeeded")
    ));
    assert_eq!(ledger.usage().model_responses(), 1);
    assert_eq!(ledger.usage().model_visible_calls(), 1);
    assert!(ledger.usage().retained_result_bytes() > 0);
    assert_eq!(
        lease.next_required_action(),
        Some(&RequiredBudgetAction::ExecutorTerminal)
    );
    let terminal_action = ledger.begin_required_action(&mut lease).unwrap();
    assert_eq!(
        terminal_action.action(),
        &RequiredBudgetAction::ExecutorTerminal
    );
    ledger.abort_role_on_failure(lease).unwrap();
    assert_eq!(ledger.active_role(), None);
}

#[tokio::test]
async fn role_engine_cancellation_checks_bracket_provider_and_runtime_awaits() {
    {
        let provider = Arc::new(ScriptedProvider::new(Vec::new()));
        let runtime = Arc::new(ScriptedRuntime::tool_results(&[]));
        let engine = RoleEngine::new(
            provider.clone(),
            runtime,
            Arc::new(RecordingEvents::default()),
            Arc::new(IdentityRedactor),
        );
        let transcript = planner_transcript();
        let mut ledger = TaskBudgetLedger::new();
        let mut lease = ledger.start_planner().unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let error = engine
            .exploratory_exchange(&transcript, &mut ledger, &mut lease, cancellation)
            .await
            .unwrap_err();

        assert!(matches!(error, RoleLoopError::Cancelled));
        assert!(provider.requests().is_empty());
        assert_eq!(provider.sends(), 0);
        assert_eq!(ledger.usage().model_responses(), 0);
        ledger.abort_role_on_failure(lease).unwrap();
    }

    {
        let provider = Arc::new(ScriptedProvider::new(vec![
            ProviderScript::response_then_cancel(100, 50, batch(vec![plan_call("cancelled")])),
        ]));
        let runtime = Arc::new(ScriptedRuntime::tool_results(&[]));
        let engine = RoleEngine::new(
            provider.clone(),
            runtime,
            Arc::new(RecordingEvents::default()),
            Arc::new(IdentityRedactor),
        );
        let transcript = planner_transcript();
        let mut ledger = TaskBudgetLedger::new();
        let mut lease = ledger.start_planner().unwrap();

        let error = engine
            .exploratory_exchange(
                &transcript,
                &mut ledger,
                &mut lease,
                CancellationToken::new(),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, RoleLoopError::Cancelled));
        assert_eq!(provider.sends(), 1);
        assert_eq!(ledger.usage().model_responses(), 1);
        assert_eq!(ledger.usage().model_visible_calls(), 0);
        ledger.abort_role_on_failure(lease).unwrap();
    }

    for required in [false, true] {
        let provider = Arc::new(ScriptedProvider::new(vec![
            ProviderScript::transport_error_then_cancel(100),
        ]));
        let runtime = Arc::new(ScriptedRuntime::tool_results(&[]));
        let engine = RoleEngine::new(
            provider.clone(),
            runtime,
            Arc::new(RecordingEvents::default()),
            Arc::new(IdentityRedactor),
        );
        let transcript = planner_transcript();
        let mut ledger = TaskBudgetLedger::new();
        let mut lease = ledger.start_planner().unwrap();

        let error = if required {
            let mut action = ledger.begin_required_action(&mut lease).unwrap();
            engine
                .required_exchange(
                    &transcript,
                    RequiredAction::terminal_or_blocked(coding_agent_core::ControlKind::SubmitPlan)
                        .unwrap(),
                    &mut ledger,
                    &mut lease,
                    &mut action,
                    CancellationToken::new(),
                )
                .await
                .unwrap_err()
        } else {
            engine
                .exploratory_exchange(
                    &transcript,
                    &mut ledger,
                    &mut lease,
                    CancellationToken::new(),
                )
                .await
                .unwrap_err()
        };

        assert!(matches!(error, RoleLoopError::Cancelled));
        assert_eq!(provider.sends(), 1);
        assert_eq!(ledger.usage().model_responses(), 0);
        assert_eq!(ledger.usage().provider_bytes(), 100);
        ledger.abort_role_on_failure(lease).unwrap();
        assert_eq!(ledger.active_role(), None);
    }

    {
        let provider = Arc::new(ScriptedProvider::new(Vec::new()));
        let runtime = Arc::new(ScriptedRuntime::tool_results(&[]));
        let engine = RoleEngine::new(
            provider,
            runtime.clone(),
            Arc::new(RecordingEvents::default()),
            Arc::new(IdentityRedactor),
        );
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let error = engine
            .invoke_runtime(
                RuntimeActionRequest::Tool(ToolRequest::ReadFile {
                    path: "src/lib.rs".to_owned(),
                    start_line: 1,
                    end_line: 1,
                }),
                cancellation,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, RoleLoopError::Cancelled));
        assert!(runtime.requests().is_empty());
    }

    {
        let provider = Arc::new(ScriptedProvider::new(Vec::new()));
        let runtime = Arc::new(ScriptedRuntime::with_result_then_cancel(
            RoleRuntimeResult::Tool(ToolResult::text("discarded")),
        ));
        let engine = RoleEngine::new(
            provider,
            runtime.clone(),
            Arc::new(RecordingEvents::default()),
            Arc::new(IdentityRedactor),
        );

        let error = engine
            .invoke_runtime(
                RuntimeActionRequest::Tool(ToolRequest::ReadFile {
                    path: "src/lib.rs".to_owned(),
                    start_line: 1,
                    end_line: 1,
                }),
                CancellationToken::new(),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, RoleLoopError::Cancelled));
        assert_eq!(runtime.requests().len(), 1);
    }
}

#[tokio::test]
async fn planner_uses_a_fresh_transcript_shared_ledger_and_durable_plan_ack() {
    let provider = Arc::new(ScriptedProvider::new(vec![
        ProviderScript::response(101, 41, batch(vec![read_call("inspect")])),
        ProviderScript::response(103, 43, batch(vec![plan_call("submit")])),
    ]));
    let runtime = Arc::new(ScriptedRuntime::tool_results(&["bounded source"]));
    let events = Arc::new(RecordingEvents::default());
    let role_loop = make_loop(
        Arc::clone(&provider),
        Arc::clone(&runtime),
        Arc::clone(&events),
        Arc::new(IdentityRedactor),
    );
    let checkpoint = checkpoint();
    let catalog = catalog();
    let mut ledger = TaskBudgetLedger::new();

    let outcome = role_loop
        .run(
            PlannerRoleInput {
                task_prompt: "Implement Project 3",
                repository_context: "repository root contains Rust crates",
                checkpoint: &checkpoint,
                repository_check_catalog: &catalog,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap();

    let PlannerRoleOutcome::Submitted(submitted) = outcome else {
        panic!("Planner must submit the scripted plan");
    };
    assert_eq!(submitted.durable_sequence(), 1);
    assert_eq!(submitted.plan().format_version(), 1);
    assert_eq!(submitted.plan().items()[0].id(), "step-01");
    assert_eq!(submitted.required_checks()[0].id(), "check-01");
    assert_eq!(ledger.active_role(), None);
    assert_eq!(ledger.usage().model_responses(), 2);
    assert_eq!(ledger.usage().model_visible_calls(), 2);
    assert_eq!(ledger.usage().provider_bytes(), 101 + 41 + 103 + 43);
    assert_eq!(runtime.requests().len(), 1);

    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].messages.len(), 2);
    assert!(matches!(requests[0].messages[0], ModelMessage::System(_)));
    assert!(matches!(requests[0].messages[1], ModelMessage::User(_)));
    assert_eq!(requests[1].messages.len(), 4);
    assert!(matches!(
        requests[1].messages[2],
        ModelMessage::AssistantToolCalls(_)
    ));
    assert!(matches!(
        &requests[1].messages[3],
        ModelMessage::ToolResult { tool_call_id, .. } if tool_call_id == "inspect"
    ));
    assert_eq!(requests[0].allowed_actions.role(), Some(Role::Planner));
    assert_eq!(requests[1].tool_choice, ModelToolChoice::Auto);

    let ordinary = events.ordinary();
    assert!(matches!(
        &ordinary[0],
        RoleEvent::Activity(activity)
            if activity.role() == Role::Planner
                && activity.role_run() == 1
                && activity.message() == "Planner started"
    ));
    assert!(matches!(
        &ordinary[1],
        RoleEvent::Activity(activity)
            if activity.role() == Role::Planner && activity.role_run() == 1
    ));
    assert!(matches!(
        events.durable().as_slice(),
        [DurableRoleEvent::StructuredPlan(plan)] if plan.items()[0].id() == "step-01"
    ));
}

#[tokio::test]
async fn planner_same_run_tool_id_reuse_is_rejected_before_second_runtime_dispatch() {
    let provider = Arc::new(ScriptedProvider::new(vec![
        ProviderScript::response(100, 40, batch(vec![read_call("same")])),
        ProviderScript::response(100, 40, batch(vec![read_call("same")])),
    ]));
    let runtime = Arc::new(ScriptedRuntime::tool_results(&["first"]));
    let events = Arc::new(RecordingEvents::default());
    let role_loop = make_loop(
        Arc::clone(&provider),
        Arc::clone(&runtime),
        events,
        Arc::new(IdentityRedactor),
    );
    let checkpoint = checkpoint();
    let catalog = catalog();
    let mut ledger = TaskBudgetLedger::new();

    let error = role_loop
        .run(
            PlannerRoleInput {
                task_prompt: "task",
                repository_context: "repo",
                checkpoint: &checkpoint,
                repository_check_catalog: &catalog,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, RoleLoopError::Transcript(_)));
    assert_eq!(runtime.requests().len(), 1);
    assert_eq!(ledger.active_role(), None);
    assert_eq!(ledger.usage().model_responses(), 2);
    assert_eq!(ledger.usage().model_visible_calls(), 1);
}

#[tokio::test]
async fn planner_wrong_role_and_mixed_batches_have_zero_runtime_side_effects() {
    for response in [
        batch(vec![ToolCall::runtime(
            "write",
            ToolRequest::ReplaceFile {
                path: "src/lib.rs".to_owned(),
                expected_sha256: None,
                content: "mutated".to_owned(),
            },
        )]),
        batch(vec![read_call("read"), plan_call("terminal")]),
    ] {
        let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
            100, 40, response,
        )]));
        let runtime = Arc::new(ScriptedRuntime::tool_results(&[]));
        let role_loop = make_loop(
            provider,
            Arc::clone(&runtime),
            Arc::new(RecordingEvents::default()),
            Arc::new(IdentityRedactor),
        );
        let checkpoint = checkpoint();
        let catalog = catalog();
        let mut ledger = TaskBudgetLedger::new();

        let error = role_loop
            .run(
                PlannerRoleInput {
                    task_prompt: "task",
                    repository_context: "repo",
                    checkpoint: &checkpoint,
                    repository_check_catalog: &catalog,
                },
                &mut ledger,
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, RoleLoopError::PlannerActionNotAllowed));
        assert_eq!(
            error.planner_failure_code(),
            Some("PLANNER_ACTION_NOT_ALLOWED")
        );
        assert!(runtime.requests().is_empty());
        assert_eq!(ledger.active_role(), None);
        assert_eq!(ledger.usage().model_visible_calls(), 0);
    }
}

#[tokio::test]
async fn planner_reservation_rejects_the_whole_auto_batch_then_forces_one_required_terminal() {
    let oversized_batch = (0..12)
        .map(|index| read_call(&format!("read-{index}")))
        .collect();
    let provider = Arc::new(ScriptedProvider::new(vec![
        ProviderScript::response(100, 50, batch(oversized_batch)),
        ProviderScript::response(120, 60, batch(vec![plan_call("required-plan")])),
    ]));
    let runtime = Arc::new(ScriptedRuntime::tool_results(&[]));
    let role_loop = make_loop(
        Arc::clone(&provider),
        Arc::clone(&runtime),
        Arc::new(RecordingEvents::default()),
        Arc::new(IdentityRedactor),
    );
    let checkpoint = checkpoint();
    let catalog = catalog();
    let mut ledger = TaskBudgetLedger::new();

    let outcome = role_loop
        .run(
            PlannerRoleInput {
                task_prompt: "task",
                repository_context: "repo",
                checkpoint: &checkpoint,
                repository_check_catalog: &catalog,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap();

    assert!(matches!(outcome, PlannerRoleOutcome::Submitted(_)));
    assert!(runtime.requests().is_empty());
    assert_eq!(ledger.active_role(), None);
    assert_eq!(ledger.usage().model_responses(), 2);
    assert_eq!(ledger.usage().model_visible_calls(), 1);
    assert_eq!(provider.sends(), 2);
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].messages.len(), 2);
    assert!(matches!(
        requests[1].tool_choice,
        ModelToolChoice::Required(_)
    ));
}

#[tokio::test]
async fn planner_required_fallback_rejects_a_same_run_historical_id() {
    let reservation_blocking_batch = (0..11)
        .map(|index| read_call(&format!("blocked-{index}")))
        .collect();
    let provider = Arc::new(ScriptedProvider::new(vec![
        ProviderScript::response(100, 40, batch(vec![read_call("historical")])),
        ProviderScript::response(100, 40, batch(reservation_blocking_batch)),
        ProviderScript::response(100, 40, batch(vec![plan_call("historical")])),
    ]));
    let runtime = Arc::new(ScriptedRuntime::tool_results(&["first result"]));
    let role_loop = make_loop(
        provider,
        Arc::clone(&runtime),
        Arc::new(RecordingEvents::default()),
        Arc::new(IdentityRedactor),
    );
    let checkpoint = checkpoint();
    let catalog = catalog();
    let mut ledger = TaskBudgetLedger::new();

    let error = role_loop
        .run(
            PlannerRoleInput {
                task_prompt: "task",
                repository_context: "repo",
                checkpoint: &checkpoint,
                repository_check_catalog: &catalog,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, RoleLoopError::Transcript(_)));
    assert_eq!(runtime.requests().len(), 1);
    assert_eq!(ledger.active_role(), None);
    assert_eq!(ledger.usage().model_responses(), 3);
    assert_eq!(ledger.usage().model_visible_calls(), 1);
}

#[tokio::test]
async fn planner_required_fallback_accepts_typed_report_blocked_as_the_reserved_terminal() {
    let reservation_blocking_batch = (0..12)
        .map(|index| read_call(&format!("blocked-{index}")))
        .collect();
    let provider = Arc::new(ScriptedProvider::new(vec![
        ProviderScript::response(100, 40, batch(reservation_blocking_batch)),
        ProviderScript::response(100, 40, batch(vec![blocked_call("blocked")])),
    ]));
    let runtime = Arc::new(ScriptedRuntime::tool_results(&[]));
    let events = Arc::new(RecordingEvents::default());
    let role_loop = make_loop(
        provider,
        Arc::clone(&runtime),
        Arc::clone(&events),
        Arc::new(IdentityRedactor),
    );
    let checkpoint = checkpoint();
    let catalog = catalog();
    let mut ledger = TaskBudgetLedger::new();

    let outcome = role_loop
        .run(
            PlannerRoleInput {
                task_prompt: "task",
                repository_context: "repo",
                checkpoint: &checkpoint,
                repository_check_catalog: &catalog,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap();

    assert!(matches!(outcome, PlannerRoleOutcome::Blocked(_)));
    assert!(runtime.requests().is_empty());
    assert!(events.durable().is_empty());
    assert_eq!(ledger.active_role(), None);
    assert_eq!(ledger.usage().model_responses(), 2);
    assert_eq!(ledger.usage().model_visible_calls(), 1);
}

#[tokio::test]
async fn planner_required_fallback_honors_cancellation_after_receiving_the_response() {
    let reservation_blocking_batch = (0..12)
        .map(|index| read_call(&format!("blocked-{index}")))
        .collect();
    let provider = Arc::new(ScriptedProvider::new(vec![
        ProviderScript::response(100, 40, batch(reservation_blocking_batch)),
        ProviderScript::response_then_cancel(100, 40, batch(vec![plan_call("plan")])),
    ]));
    let events = Arc::new(RecordingEvents::default());
    let role_loop = make_loop(
        provider,
        Arc::new(ScriptedRuntime::tool_results(&[])),
        Arc::clone(&events),
        Arc::new(IdentityRedactor),
    );
    let checkpoint = checkpoint();
    let catalog = catalog();
    let mut ledger = TaskBudgetLedger::new();

    let error = role_loop
        .run(
            PlannerRoleInput {
                task_prompt: "task",
                repository_context: "repo",
                checkpoint: &checkpoint,
                repository_check_catalog: &catalog,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, RoleLoopError::Cancelled));
    assert!(events.durable().is_empty());
    assert_eq!(ledger.active_role(), None);
    assert_eq!(ledger.usage().model_responses(), 2);
    assert_eq!(ledger.usage().model_visible_calls(), 0);
}

#[tokio::test]
async fn planner_invalid_catalog_plan_preserves_original_error_and_releases_lease() {
    let mut untrusted = plan_call("plan");
    untrusted.request = ActionRequest::decode(
        Role::Planner,
        "submit_plan",
        &serde_json::json!({
            "summary": "untrusted selector",
            "steps": [{
                "title": "step",
                "description": "description",
                "acceptance_criteria": ["criterion"]
            }],
            "initial_required_checks": [{
                "kind": "cargo_test",
                "package": "not-in-catalog",
                "integration_test": null
            }]
        })
        .to_string(),
    )
    .unwrap();
    let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
        100,
        50,
        batch(vec![untrusted]),
    )]));
    let role_loop = make_loop(
        provider,
        Arc::new(ScriptedRuntime::tool_results(&[])),
        Arc::new(RecordingEvents::default()),
        Arc::new(IdentityRedactor),
    );
    let checkpoint = checkpoint();
    let catalog = catalog();
    let mut ledger = TaskBudgetLedger::new();

    let error = role_loop
        .run(
            PlannerRoleInput {
                task_prompt: "task",
                repository_context: "repo",
                checkpoint: &checkpoint,
                repository_check_catalog: &catalog,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, RoleLoopError::InvalidPlannerPlan));
    assert_eq!(error.planner_failure_code(), Some("PLANNER_INVALID_OUTPUT"));
    assert_eq!(ledger.active_role(), None);
    assert_eq!(ledger.usage().model_responses(), 1);
    assert_eq!(ledger.usage().model_visible_calls(), 0);
}

#[tokio::test]
async fn planner_normal_final_uses_invalid_output_code_without_runtime_side_effects() {
    let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
        100,
        50,
        ModelResponse::Final {
            content: "ordinary final".to_owned(),
        },
    )]));
    let runtime = Arc::new(ScriptedRuntime::tool_results(&[]));
    let role_loop = make_loop(
        provider,
        runtime.clone(),
        Arc::new(RecordingEvents::default()),
        Arc::new(IdentityRedactor),
    );
    let checkpoint = checkpoint();
    let catalog = catalog();
    let mut ledger = TaskBudgetLedger::new();

    let error = role_loop
        .run(
            PlannerRoleInput {
                task_prompt: "task",
                repository_context: "repo",
                checkpoint: &checkpoint,
                repository_check_catalog: &catalog,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, RoleLoopError::InvalidPlannerPlan));
    assert_eq!(error.planner_failure_code(), Some("PLANNER_INVALID_OUTPUT"));
    assert!(runtime.requests().is_empty());
    assert_eq!(ledger.active_role(), None);
}

#[tokio::test]
async fn planner_structured_secret_is_detected_before_runtime_or_durable_events() {
    let secret_call = ToolCall::runtime(
        "secret-read",
        ToolRequest::ReadFile {
            path: "src/TOP_SECRET.rs".to_owned(),
            start_line: 1,
            end_line: 1,
        },
    );
    let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
        100,
        50,
        batch(vec![secret_call]),
    )]));
    let runtime = Arc::new(ScriptedRuntime::tool_results(&[]));
    let events = Arc::new(RecordingEvents::default());
    let role_loop = make_loop(
        provider,
        runtime.clone(),
        events.clone(),
        Arc::new(SecretRedactor),
    );
    let checkpoint = checkpoint();
    let catalog = catalog();
    let mut ledger = TaskBudgetLedger::new();

    let error = role_loop
        .run(
            PlannerRoleInput {
                task_prompt: "task",
                repository_context: "repo",
                checkpoint: &checkpoint,
                repository_check_catalog: &catalog,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RoleLoopError::Contract(coding_agent_core::RoleContractError::RedactionMutation)
    ));
    assert_eq!(
        error.planner_failure_code(),
        Some("PROVIDER_SECRET_DETECTED")
    );
    assert!(runtime.requests().is_empty());
    assert!(events.durable().is_empty());
    assert_eq!(ledger.active_role(), None);
}

#[tokio::test]
async fn planner_retained_wrapper_composition_failure_releases_batch_and_role() {
    let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
        100,
        50,
        batch(vec![read_call("read")]),
    )]));
    let runtime = Arc::new(ScriptedRuntime::tool_results(&["safe result"]));
    let role_loop = make_loop(
        provider,
        Arc::clone(&runtime),
        Arc::new(RecordingEvents::default()),
        Arc::new(WrapperBoundaryRedactor),
    );
    let checkpoint = checkpoint();
    let catalog = catalog();
    let mut ledger = TaskBudgetLedger::new();

    let error = role_loop
        .run(
            PlannerRoleInput {
                task_prompt: "task",
                repository_context: "repo",
                checkpoint: &checkpoint,
                repository_check_catalog: &catalog,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, RoleLoopError::Retained(_)));
    assert_eq!(runtime.requests().len(), 1);
    assert_eq!(ledger.active_role(), None);
    assert_eq!(ledger.usage().retained_result_bytes(), 0);
    assert_eq!(ledger.usage().model_visible_calls(), 1);
}

#[tokio::test]
async fn planner_runtime_mismatch_and_cancellation_release_batch_and_role() {
    let check =
        RequiredCheck::try_cargo_check("check-01", Some("coding-agent-core".to_owned())).unwrap();
    let observation = ValidationObservation::try_new(
        ToolResult::text("passed"),
        check,
        CheckEvidenceStatus::Passed,
        1,
        false,
    )
    .unwrap();
    let cases = [
        (Ok(RoleRuntimeResult::Validation(observation)), false),
        (
            Err(RuntimeError::new("COMMAND_CANCELLED", "cancelled", false)),
            true,
        ),
    ];

    for (script, cancelled) in cases {
        let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
            100,
            50,
            batch(vec![read_call("read")]),
        )]));
        let runtime = Arc::new(ScriptedRuntime::with_result(script));
        let role_loop = make_loop(
            provider,
            runtime,
            Arc::new(RecordingEvents::default()),
            Arc::new(IdentityRedactor),
        );
        let checkpoint = checkpoint();
        let catalog = catalog();
        let mut ledger = TaskBudgetLedger::new();

        let error = role_loop
            .run(
                PlannerRoleInput {
                    task_prompt: "task",
                    repository_context: "repo",
                    checkpoint: &checkpoint,
                    repository_check_catalog: &catalog,
                },
                &mut ledger,
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert_eq!(matches!(error, RoleLoopError::Cancelled), cancelled);
        assert_eq!(ledger.active_role(), None);
        assert_eq!(ledger.usage().retained_result_bytes(), 0);
    }
}

#[tokio::test]
async fn planner_durable_event_failure_never_returns_a_submitted_handoff() {
    let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
        100,
        50,
        batch(vec![plan_call("plan")]),
    )]));
    let events = Arc::new(RecordingEvents::failing_durable());
    let role_loop = make_loop(
        provider,
        Arc::new(ScriptedRuntime::tool_results(&[])),
        Arc::clone(&events),
        Arc::new(IdentityRedactor),
    );
    let checkpoint = checkpoint();
    let catalog = catalog();
    let mut ledger = TaskBudgetLedger::new();

    let error = role_loop
        .run(
            PlannerRoleInput {
                task_prompt: "task",
                repository_context: "repo",
                checkpoint: &checkpoint,
                repository_check_catalog: &catalog,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, RoleLoopError::Runtime(_)));
    assert_eq!(events.durable().len(), 1);
    assert_eq!(ledger.active_role(), None);
    assert_eq!(ledger.usage().model_visible_calls(), 1);
}

#[tokio::test]
async fn planner_event_sink_cancellation_is_classified_as_cancelled() {
    let checkpoint = checkpoint();
    let catalog = catalog();

    let provider = Arc::new(ScriptedProvider::new(Vec::new()));
    let role_loop = make_loop(
        Arc::clone(&provider),
        Arc::new(ScriptedRuntime::tool_results(&[])),
        Arc::new(RecordingEvents::cancelling_ordinary()),
        Arc::new(IdentityRedactor),
    );
    let mut ledger = TaskBudgetLedger::new();
    let error = role_loop
        .run(
            PlannerRoleInput {
                task_prompt: "task",
                repository_context: "repo",
                checkpoint: &checkpoint,
                repository_check_catalog: &catalog,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RoleLoopError::Cancelled));
    assert_eq!(provider.sends(), 0);
    assert_eq!(ledger.active_role(), None);

    let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
        100,
        40,
        batch(vec![plan_call("plan")]),
    )]));
    let role_loop = make_loop(
        provider,
        Arc::new(ScriptedRuntime::tool_results(&[])),
        Arc::new(RecordingEvents::cancelling_durable()),
        Arc::new(IdentityRedactor),
    );
    let mut ledger = TaskBudgetLedger::new();
    let error = role_loop
        .run(
            PlannerRoleInput {
                task_prompt: "task",
                repository_context: "repo",
                checkpoint: &checkpoint,
                repository_check_catalog: &catalog,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RoleLoopError::Cancelled));
    assert_eq!(ledger.active_role(), None);
}

#[tokio::test]
async fn planner_received_decode_failure_is_counted_before_rejection() {
    let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::decode_error(
        127, 333,
    )]));
    let role_loop = make_loop(
        provider,
        Arc::new(ScriptedRuntime::tool_results(&[])),
        Arc::new(RecordingEvents::default()),
        Arc::new(IdentityRedactor),
    );
    let checkpoint = checkpoint();
    let catalog = catalog();
    let mut ledger = TaskBudgetLedger::new();

    let error = role_loop
        .run(
            PlannerRoleInput {
                task_prompt: "task",
                repository_context: "repo",
                checkpoint: &checkpoint,
                repository_check_catalog: &catalog,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, RoleLoopError::Provider(_)));
    assert_eq!(ledger.usage().provider_bytes(), 127 + 333);
    assert_eq!(ledger.usage().model_responses(), 1);
    assert_eq!(ledger.active_role(), None);
}

#[tokio::test]
async fn planner_no_response_transport_counts_request_only_and_releases_role() {
    let provider = Arc::new(ScriptedProvider::new(vec![
        ProviderScript::transport_error(211),
    ]));
    let role_loop = make_loop(
        provider,
        Arc::new(ScriptedRuntime::tool_results(&[])),
        Arc::new(RecordingEvents::default()),
        Arc::new(IdentityRedactor),
    );
    let checkpoint = checkpoint();
    let catalog = catalog();
    let mut ledger = TaskBudgetLedger::new();

    let error = role_loop
        .run(
            PlannerRoleInput {
                task_prompt: "task",
                repository_context: "repo",
                checkpoint: &checkpoint,
                repository_check_catalog: &catalog,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, RoleLoopError::Provider(_)));
    assert_eq!(ledger.usage().provider_bytes(), 211);
    assert_eq!(ledger.usage().model_responses(), 0);
    assert_eq!(ledger.active_role(), None);
}

#[test]
fn planner_activity_constructor_keeps_actor_and_positive_role_run_typed() {
    let activity = RoleActivityEvent::try_new(Role::Planner, 1, "planning").unwrap();
    assert_eq!(activity.role(), Role::Planner);
    assert_eq!(activity.role_run(), 1);
    assert!(RoleActivityEvent::try_new(Role::Planner, 0, "planning").is_err());
}

#[test]
fn executor_handoffs_cover_rounds_one_to_three_with_correct_rework_banners() {
    let plan = structured_plan();
    let checkpoint = checkpoint();
    let checks = RequiredCheckLedger::try_new(vec![structured_check()]).unwrap();
    let review_1_finding = ReviewFinding::try_for_review(
        1,
        1,
        FindingSeverity::Blocking,
        "The implementation still needs a fix.",
        Some("src/lib.rs".to_owned()),
        Some(1),
    )
    .unwrap();
    let review_2_finding = ReviewFinding::try_for_review(
        2,
        1,
        FindingSeverity::Blocking,
        "The rework still needs a second fix.",
        Some("src/lib.rs".to_owned()),
        Some(2),
    )
    .unwrap();

    let first = ContinuationHandoff::try_for_executor(
        1,
        "task",
        "repository",
        &plan,
        &checkpoint,
        &checks,
        None,
        &IdentityRedactor,
    )
    .unwrap();
    assert!(!first.canonical_json().contains("Rework round"));

    let second = ContinuationHandoff::try_for_executor(
        2,
        "task",
        "repository",
        &plan,
        &checkpoint,
        &checks,
        Some(std::slice::from_ref(&review_1_finding)),
        &IdentityRedactor,
    )
    .unwrap();
    assert!(second.canonical_json().contains("Rework round 1"));
    assert!(second.canonical_json().contains("\"required_checks\""));
    assert!(second.canonical_json().contains("\"findings\""));

    assert!(
        ContinuationHandoff::try_for_executor(
            3,
            "task",
            "repository",
            &plan,
            &checkpoint,
            &checks,
            Some(std::slice::from_ref(&review_1_finding)),
            &IdentityRedactor,
        )
        .is_err()
    );
    let third = ContinuationHandoff::try_for_executor(
        3,
        "task",
        "repository",
        &plan,
        &checkpoint,
        &checks,
        Some(std::slice::from_ref(&review_2_finding)),
        &IdentityRedactor,
    )
    .unwrap();
    assert!(third.canonical_json().contains("Rework round 2"));
    assert!(third.canonical_json().contains("\"source_review_round\":2"));

    let wrong_check =
        RequiredCheck::try_cargo_test("check-99", Some("other-package".to_owned()), None).unwrap();
    let wrong_ledger = RequiredCheckLedger::try_new(vec![wrong_check]).unwrap();
    assert!(
        ContinuationHandoff::try_for_executor(
            1,
            "task",
            "repository",
            &plan,
            &checkpoint,
            &wrong_ledger,
            None,
            &IdentityRedactor,
        )
        .is_err()
    );
}

#[derive(Debug, Clone, Copy)]
enum ExecutorCheckRoute {
    Exploratory,
    Required,
}

#[derive(Debug, Clone, Copy)]
enum ExecutorCheckFailure {
    Runtime,
    Observer,
    QueuedEvent,
    RunningEvent,
    WorkspaceAdvanced,
    ResultMismatch,
    Cancelled,
}

fn scripted_executor_check_error(code: &str) -> RuntimeError {
    RuntimeError::new(code, "scripted Executor check failure", false)
}

fn assert_executor_check_failure(
    error: &RoleLoopError,
    failure: ExecutorCheckFailure,
    route: ExecutorCheckRoute,
) {
    match failure {
        ExecutorCheckFailure::Runtime => assert!(
            matches!(error, RoleLoopError::Runtime(runtime) if runtime.code == "SCRIPTED_VALIDATION_FAILURE"),
            "{route:?} returned {error:?}"
        ),
        ExecutorCheckFailure::Observer => assert!(
            matches!(error, RoleLoopError::Runtime(runtime) if runtime.code == "SCRIPTED_FINGERPRINT_FAILURE"),
            "{route:?} returned {error:?}"
        ),
        ExecutorCheckFailure::QueuedEvent | ExecutorCheckFailure::RunningEvent => assert!(
            matches!(error, RoleLoopError::Runtime(runtime) if runtime.code == "SCRIPTED_ORDINARY_FAILURE"),
            "{route:?} returned {error:?}"
        ),
        ExecutorCheckFailure::WorkspaceAdvanced => assert!(
            matches!(error, RoleLoopError::QualityEvidenceMismatch),
            "{route:?} returned {error:?}"
        ),
        ExecutorCheckFailure::ResultMismatch => assert!(
            matches!(error, RoleLoopError::RuntimeResultMismatch),
            "{route:?} returned {error:?}"
        ),
        ExecutorCheckFailure::Cancelled => assert!(
            matches!(error, RoleLoopError::Cancelled),
            "{route:?} returned {error:?}"
        ),
    }
}

async fn assert_executor_check_attempt_is_abandoned(
    route: ExecutorCheckRoute,
    failure: ExecutorCheckFailure,
) {
    let a = WorkspaceFingerprint::from_bytes([0x42; 32]);
    let b = WorkspaceFingerprint::from_bytes([0x43; 32]);
    let c = WorkspaceFingerprint::from_bytes([0x44; 32]);
    let check = structured_check();
    let provider_scripts = match route {
        ExecutorCheckRoute::Exploratory => vec![ProviderScript::response(
            256,
            128,
            batch(vec![executor_validation_selector_call(
                "exploratory-validation",
                &check,
            )]),
        )],
        ExecutorCheckRoute::Required => vec![
            ProviderScript::response(
                256,
                128,
                batch(vec![read_call("superseded-exploratory-read")]),
            ),
            ProviderScript::response(
                512,
                128,
                batch(vec![validation_call("required-validation", &check)]),
            ),
        ],
    };
    let provider = Arc::new(ScriptedProvider::new(provider_scripts));

    let runtime = Arc::new(match failure {
        ExecutorCheckFailure::Runtime => ScriptedRuntime::with_result(Err(
            scripted_executor_check_error("SCRIPTED_VALIDATION_FAILURE"),
        )),
        ExecutorCheckFailure::ResultMismatch => ScriptedRuntime::with_result(Ok(
            RoleRuntimeResult::Tool(ToolResult::text("wrong typed result")),
        )),
        ExecutorCheckFailure::Cancelled => {
            ScriptedRuntime::with_result_then_cancel(validation_result(&check))
        }
        _ => ScriptedRuntime::with_result(Ok(validation_result(&check))),
    });
    let mut fingerprint_results = match route {
        ExecutorCheckRoute::Exploratory => vec![Ok(a), Ok(a), Ok(a)],
        ExecutorCheckRoute::Required => vec![Ok(a), Ok(a), Ok(b), Ok(b)],
    };
    match failure {
        ExecutorCheckFailure::Observer => fingerprint_results.push(Err(
            scripted_executor_check_error("SCRIPTED_FINGERPRINT_FAILURE"),
        )),
        ExecutorCheckFailure::WorkspaceAdvanced => fingerprint_results.push(Ok(match route {
            ExecutorCheckRoute::Exploratory => b,
            ExecutorCheckRoute::Required => c,
        })),
        _ => {}
    }
    runtime
        .fingerprints
        .lock()
        .unwrap()
        .extend(fingerprint_results);

    let failed_emission = match (route, failure) {
        (ExecutorCheckRoute::Exploratory, ExecutorCheckFailure::QueuedEvent) => Some(2),
        (ExecutorCheckRoute::Exploratory, ExecutorCheckFailure::RunningEvent) => Some(3),
        (ExecutorCheckRoute::Required, ExecutorCheckFailure::QueuedEvent) => Some(3),
        (ExecutorCheckRoute::Required, ExecutorCheckFailure::RunningEvent) => Some(4),
        _ => None,
    };
    let events = Arc::new(match failed_emission {
        Some(emission) => RecordingEvents::failing_ordinary_at(emission),
        None => RecordingEvents::default(),
    });
    let executor = make_executor_loop(provider, Arc::clone(&runtime), Arc::clone(&events));
    let mut plan = structured_plan();
    let mut checkpoint = checkpoint();
    let mut checks = RequiredCheckLedger::try_new(vec![check.clone()]).unwrap();
    let mut ledger = TaskBudgetLedger::try_new().unwrap();

    let error = executor
        .run(
            ExecutorRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &mut plan,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                latest_reviewer_findings: None,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert_executor_check_failure(&error, failure, route);
    assert_eq!(
        ledger.active_role(),
        None,
        "{route:?}/{failure:?} left an active budget role"
    );
    let snapshot = project_test_snapshot(&checks, &checkpoint);
    assert_eq!(
        snapshot.status,
        TestStatus::Queued,
        "{route:?}/{failure:?} projected a non-queued aggregate"
    );
    assert!(
        snapshot
            .cases
            .iter()
            .all(|case| case.status != TestStatus::Running),
        "{route:?}/{failure:?} leaked a Running check"
    );

    // `queue_check` rejects both a current Queued and a current Running
    // attempt, so success directly proves that the active entry was removed.
    checks.queue_check(&mut checkpoint, check.id()).unwrap();
    checks
        .abandon_queued_check(&checkpoint, check.id())
        .unwrap();

    let emitted_tests = events
        .ordinary()
        .into_iter()
        .filter_map(|event| match event {
            RoleEvent::Tests(snapshot) => Some(snapshot),
            _ => None,
        })
        .collect::<Vec<_>>();
    if matches!(failure, ExecutorCheckFailure::Cancelled) {
        assert_eq!(
            emitted_tests.last().map(|snapshot| snapshot.status),
            Some(TestStatus::Running),
            "{route:?} cancellation must not emit a compensating event"
        );
    } else {
        assert_eq!(
            emitted_tests.last().map(|snapshot| snapshot.status),
            Some(TestStatus::Queued),
            "{route:?}/{failure:?} must best-effort project the abandoned attempt"
        );
    }
}

#[tokio::test]
async fn executor_exploratory_validation_abandons_every_unfinished_check_attempt() {
    for failure in [
        ExecutorCheckFailure::Runtime,
        ExecutorCheckFailure::Observer,
        ExecutorCheckFailure::QueuedEvent,
        ExecutorCheckFailure::RunningEvent,
        ExecutorCheckFailure::WorkspaceAdvanced,
        ExecutorCheckFailure::ResultMismatch,
        ExecutorCheckFailure::Cancelled,
    ] {
        assert_executor_check_attempt_is_abandoned(ExecutorCheckRoute::Exploratory, failure).await;
    }
}

#[tokio::test]
async fn executor_required_validation_abandons_every_unfinished_check_attempt() {
    for failure in [
        ExecutorCheckFailure::Runtime,
        ExecutorCheckFailure::Observer,
        ExecutorCheckFailure::QueuedEvent,
        ExecutorCheckFailure::RunningEvent,
        ExecutorCheckFailure::WorkspaceAdvanced,
        ExecutorCheckFailure::ResultMismatch,
        ExecutorCheckFailure::Cancelled,
    ] {
        assert_executor_check_attempt_is_abandoned(ExecutorCheckRoute::Required, failure).await;
    }
}

#[tokio::test]
async fn executor_plan_progress_validation_and_submit_preserve_typed_boundaries() {
    let check = structured_check();
    let provider = Arc::new(ScriptedProvider::new(vec![
        ProviderScript::response(
            256,
            128,
            batch(vec![executor_progress_call(
                "progress-1",
                "step-01",
                "completed",
            )]),
        ),
        ProviderScript::response(
            512,
            128,
            batch(vec![executor_validation_selector_call(
                "validation-1",
                &check,
            )]),
        ),
        ProviderScript::response(
            768,
            128,
            batch(vec![executor_submission_call(
                "submit-1",
                "ready for review",
            )]),
        ),
    ]));
    let runtime = Arc::new(ScriptedRuntime::with_result(Ok(validation_result(&check))));
    let events = Arc::new(RecordingEvents::default());
    let executor = make_executor_loop(
        Arc::clone(&provider),
        Arc::clone(&runtime),
        Arc::clone(&events),
    );
    let mut plan = structured_plan();
    let original_check = plan.initial_required_checks()[0].clone();
    let mut checkpoint = checkpoint();
    let mut checks = RequiredCheckLedger::try_new(vec![check.clone()]).unwrap();
    let mut ledger = TaskBudgetLedger::try_new().unwrap();

    let outcome = executor
        .run(
            ExecutorRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &mut plan,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                latest_reviewer_findings: None,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap();

    let ExecutorRoleOutcome::Submitted(execution) = outcome else {
        panic!("Executor must submit");
    };
    assert_eq!(execution.summary(), "ready for review");
    assert_eq!(execution.workspace_generation(), 0);
    assert_eq!(execution.tests().status, TestStatus::Passed);
    assert_eq!(plan.items()[0].status(), PlanItemStatus::Completed);
    assert_eq!(plan.initial_required_checks(), [original_check]);
    assert!(checks.all_current_checks_passed(&checkpoint));
    assert!(matches!(
        events.durable().as_slice(),
        [DurableRoleEvent::PlanUpdated(updated)] if updated.items()[0].status() == PlanItemStatus::Completed
    ));

    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    assert!(requests[1].messages.iter().any(|message| {
        matches!(
            message,
            ModelMessage::ToolResult { tool_call_id, content }
                if tool_call_id == "progress-1"
                    && content.contains("Plan progress durably updated")
        )
    }));
    assert!(matches!(
        runtime.requests().as_slice(),
        [RuntimeActionRequest::Validation { check: executed }] if executed == &check
    ));
    assert!(events.ordinary().iter().any(|event| {
        matches!(
            event,
            RoleEvent::Tests(snapshot) if snapshot.status == TestStatus::Running
        )
    }));
    assert!(events.ordinary().iter().any(|event| {
        matches!(
            event,
            RoleEvent::Tests(snapshot) if snapshot.status == TestStatus::Passed
        )
    }));
}

#[tokio::test]
async fn executor_plan_progress_waits_for_durable_ack_and_never_forges_success_on_failure() {
    let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
        256,
        128,
        batch(vec![executor_progress_call(
            "progress-fails",
            "step-01",
            "completed",
        )]),
    )]));
    let runtime = Arc::new(ScriptedRuntime::tool_results(&[]));
    let events = Arc::new(RecordingEvents::failing_durable());
    let executor = make_executor_loop(Arc::clone(&provider), runtime, Arc::clone(&events));
    let mut plan = structured_plan();
    let mut checkpoint = checkpoint();
    let mut checks = RequiredCheckLedger::try_new(vec![structured_check()]).unwrap();
    let mut ledger = TaskBudgetLedger::try_new().unwrap();

    let error = executor
        .run(
            ExecutorRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &mut plan,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                latest_reviewer_findings: None,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, RoleLoopError::Runtime(_)));
    assert_eq!(plan.revision(), 1);
    assert_eq!(plan.items()[0].status(), PlanItemStatus::Pending);
    assert_eq!(provider.requests().len(), 1);
    assert!(matches!(
        events.durable().as_slice(),
        [DurableRoleEvent::PlanUpdated(_)]
    ));
    assert_eq!(ledger.active_role(), None);
}

#[tokio::test]
async fn executor_observes_a_to_b_to_a_after_each_runtime_action() {
    let a = WorkspaceFingerprint::from_bytes([0x42; 32]);
    let b = WorkspaceFingerprint::from_bytes([0x43; 32]);
    let provider = Arc::new(ScriptedProvider::new(vec![
        ProviderScript::response(
            256,
            128,
            batch(vec![
                replace_call("replace-1", "first"),
                replace_call("replace-2", "second"),
            ]),
        ),
        ProviderScript::response(
            512,
            128,
            batch(vec![executor_blocked_call("blocked-after-write")]),
        ),
    ]));
    let runtime = Arc::new(ScriptedRuntime::with_scripts_and_fingerprints(
        vec![
            Ok(RoleRuntimeResult::Tool(ToolResult::text("first replaced"))),
            Ok(RoleRuntimeResult::Tool(ToolResult::text("second replaced"))),
        ],
        vec![a, a, a, b, b, a],
    ));
    let events = Arc::new(RecordingEvents::default());
    let executor = make_executor_loop(provider, runtime, Arc::clone(&events));
    let mut plan = structured_plan();
    let mut checkpoint = checkpoint();
    let mut checks = RequiredCheckLedger::try_new(vec![structured_check()]).unwrap();
    let mut ledger = TaskBudgetLedger::try_new().unwrap();

    let outcome = executor
        .run(
            ExecutorRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &mut plan,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                latest_reviewer_findings: None,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap();

    assert!(matches!(outcome, ExecutorRoleOutcome::Blocked(_)));
    assert_eq!(checkpoint.generation(), 2);
    assert_eq!(checkpoint.fingerprint(), a);
    let revisions = events
        .ordinary()
        .into_iter()
        .filter_map(|event| match event {
            RoleEvent::Tests(snapshot) => Some(snapshot.revision),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(revisions.contains(&1));
    assert!(revisions.contains(&2));
}

#[tokio::test]
async fn executor_read_only_batch_refreshes_external_change_before_budget_preflight() {
    let a = WorkspaceFingerprint::from_bytes([0x42; 32]);
    let b = WorkspaceFingerprint::from_bytes([0x43; 32]);
    let provider = Arc::new(ScriptedProvider::new(
        (1..=4)
            .map(|index| {
                ProviderScript::response(
                    index * 256,
                    128,
                    batch(vec![read_call(&format!("read-{index}"))]),
                )
            })
            .collect(),
    ));
    let runtime = Arc::new(ScriptedRuntime::with_scripts_and_fingerprints(
        vec![
            Ok(RoleRuntimeResult::Tool(ToolResult::text("first read"))),
            Ok(RoleRuntimeResult::Tool(ToolResult::text("second read"))),
            Ok(RoleRuntimeResult::Tool(ToolResult::text("third read"))),
        ],
        std::iter::repeat_n(a, 10)
            .chain(std::iter::once(b))
            .collect(),
    ));
    let events = Arc::new(RecordingEvents::default());
    let executor = make_executor_loop(provider, Arc::clone(&runtime), events);
    let mut plan = structured_plan();
    let (mut checkpoint, mut checks) = passed_executor_checks(16);
    let mut ledger = TaskBudgetLedger::try_new().unwrap();

    let error = executor
        .run(
            ExecutorRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &mut plan,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                latest_reviewer_findings: None,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert_eq!(
        error.executor_failure_code(),
        Some("EXECUTOR_STEP_LIMIT_REACHED")
    );
    assert_eq!(runtime.requests().len(), 3);
    assert_eq!(checkpoint.generation(), 1);
    assert_eq!(checkpoint.fingerprint(), b);
    assert_eq!(ledger.active_role(), None);
}

#[tokio::test]
async fn executor_workspace_change_after_permit_before_first_dispatch_recovers_to_required_check() {
    let a = WorkspaceFingerprint::from_bytes([0x42; 32]);
    let b = WorkspaceFingerprint::from_bytes([0x43; 32]);
    let check = structured_check();
    let provider = Arc::new(ScriptedProvider::new(vec![
        ProviderScript::response(256, 128, batch(vec![read_call("discarded-read")])),
        ProviderScript::response(
            512,
            128,
            batch(vec![validation_call("required-check", &check)]),
        ),
        ProviderScript::response(768, 128, batch(vec![executor_blocked_call("blocked")])),
    ]));
    let runtime = Arc::new(ScriptedRuntime::with_scripts_and_fingerprints(
        vec![Ok(validation_result(&check))],
        vec![a, a, b, b, b],
    ));
    let events = Arc::new(RecordingEvents::default());
    let executor = make_executor_loop(
        Arc::clone(&provider),
        Arc::clone(&runtime),
        Arc::clone(&events),
    );
    let mut plan = structured_plan();
    let (mut checkpoint, mut checks) = passed_executor_checks(1);
    let mut ledger = TaskBudgetLedger::try_new().unwrap();

    let outcome = executor
        .run(
            ExecutorRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &mut plan,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                latest_reviewer_findings: None,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap();

    assert!(matches!(outcome, ExecutorRoleOutcome::Blocked(_)));
    assert!(matches!(
        runtime.requests().as_slice(),
        [RuntimeActionRequest::Validation { check: executed }] if executed == &check
    ));
    assert_eq!(checkpoint.generation(), 1);
    assert!(checks.all_current_checks_passed(&checkpoint));
    assert_eq!(provider.requests().len(), 3);
    assert!(!provider.requests()[1].messages.iter().any(|message| {
        matches!(
            message,
            ModelMessage::AssistantToolCalls(batch)
                if batch.calls.iter().any(|call| call.id == "discarded-read")
        )
    }));
    assert!(events.ordinary().iter().any(|event| {
        matches!(
            event,
            RoleEvent::Tests(snapshot)
                if snapshot.revision == 1 && snapshot.status == TestStatus::Queued
        )
    }));
    assert_eq!(ledger.active_role(), None);
}

#[tokio::test]
async fn executor_workspace_change_after_partial_batch_fails_closed() {
    let a = WorkspaceFingerprint::from_bytes([0x42; 32]);
    let b = WorkspaceFingerprint::from_bytes([0x43; 32]);
    let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
        256,
        128,
        batch(vec![read_call("executed-read"), read_call("skipped-read")]),
    )]));
    let runtime = Arc::new(ScriptedRuntime::with_scripts_and_fingerprints(
        vec![Ok(RoleRuntimeResult::Tool(ToolResult::text("first read")))],
        vec![a, a, a, a, b],
    ));
    let events = Arc::new(RecordingEvents::default());
    let executor = make_executor_loop(provider, Arc::clone(&runtime), events);
    let mut plan = structured_plan();
    let (mut checkpoint, mut checks) = passed_executor_checks(1);
    let mut ledger = TaskBudgetLedger::try_new().unwrap();

    let error = executor
        .run(
            ExecutorRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &mut plan,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                latest_reviewer_findings: None,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, RoleLoopError::QualityEvidenceMismatch));
    assert_eq!(
        error.executor_failure_code(),
        Some("QUALITY_EVIDENCE_MISMATCH")
    );
    assert!(matches!(
        runtime.requests().as_slice(),
        [RuntimeActionRequest::Tool(ToolRequest::ReadFile { .. })]
    ));
    assert_eq!(checkpoint.generation(), 1);
    assert_eq!(checkpoint.fingerprint(), b);
    assert_eq!(ledger.usage().model_visible_calls(), 2);
    assert_eq!(ledger.usage().retained_result_bytes(), 0);
    assert_eq!(ledger.active_role(), None);
}

#[tokio::test]
async fn executor_rerun_replaces_latest_observation_and_projects_failed_snapshot() {
    let check = structured_check();
    let provider = Arc::new(ScriptedProvider::new(vec![
        ProviderScript::response(
            256,
            128,
            batch(vec![executor_progress_call(
                "progress",
                "step-01",
                "completed",
            )]),
        ),
        ProviderScript::response(
            512,
            128,
            batch(vec![executor_validation_selector_call("pass", &check)]),
        ),
        ProviderScript::response(
            768,
            128,
            batch(vec![executor_validation_selector_call("fail", &check)]),
        ),
        ProviderScript::response(1_024, 128, batch(vec![executor_blocked_call("blocked")])),
    ]));
    let runtime = Arc::new(ScriptedRuntime::with_scripts_and_fingerprints(
        vec![
            Ok(validation_result(&check)),
            Ok(failed_validation_result(&check)),
        ],
        Vec::new(),
    ));
    let events = Arc::new(RecordingEvents::default());
    let executor = make_executor_loop(provider, runtime, Arc::clone(&events));
    let mut plan = structured_plan();
    let mut checkpoint = checkpoint();
    let mut checks = RequiredCheckLedger::try_new(vec![check.clone()]).unwrap();
    let mut ledger = TaskBudgetLedger::try_new().unwrap();

    let outcome = executor
        .run(
            ExecutorRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &mut plan,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                latest_reviewer_findings: None,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap();

    let ExecutorRoleOutcome::Blocked(blocked) = outcome else {
        panic!("the scripted blocker must terminate the stage");
    };
    assert_eq!(
        blocked.stage_failure().status(),
        coding_agent_domain::TaskStatus::Failed
    );
    assert_eq!(
        blocked.stage_failure().delivery_readiness(),
        coding_agent_domain::DeliveryReadiness::Unreviewed
    );
    assert_eq!(
        blocked.stage_failure().failure().code,
        "EXECUTOR_BLOCKED_MISSING_CONTEXT"
    );
    let observation = checkpoint.current_observation(check.id()).unwrap();
    assert_eq!(observation.status(), CheckEvidenceStatus::Failed);
    assert!(events.ordinary().iter().any(|event| {
        matches!(
            event,
            RoleEvent::Tests(snapshot) if snapshot.status == TestStatus::Failed
        )
    }));
}

#[tokio::test]
async fn executor_submit_without_current_evidence_and_normal_final_use_invalid_output_code() {
    for scripts in [
        vec![ProviderScript::response(
            256,
            128,
            batch(vec![executor_submission_call("submit", "premature")]),
        )],
        vec![ProviderScript::response(
            256,
            128,
            ModelResponse::Final {
                content: "ordinary final".to_owned(),
            },
        )],
    ] {
        let provider = Arc::new(ScriptedProvider::new(scripts));
        let runtime = Arc::new(ScriptedRuntime::tool_results(&[]));
        let events = Arc::new(RecordingEvents::default());
        let executor = make_executor_loop(provider, runtime, events);
        let mut plan = structured_plan();
        let mut checkpoint = checkpoint();
        let mut checks = RequiredCheckLedger::try_new(vec![structured_check()]).unwrap();
        let mut ledger = TaskBudgetLedger::try_new().unwrap();

        let error = executor
            .run(
                ExecutorRoleInput {
                    review_round: 1,
                    task_prompt: "task",
                    repository_context: "repository",
                    plan: &mut plan,
                    checkpoint: &mut checkpoint,
                    required_checks: &mut checks,
                    latest_reviewer_findings: None,
                },
                &mut ledger,
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.executor_failure_code(),
            Some("EXECUTOR_INVALID_OUTPUT")
        );
    }
}

#[tokio::test]
async fn executor_unauthorized_action_uses_action_not_allowed_code() {
    let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
        256,
        128,
        batch(vec![plan_call("planner-only")]),
    )]));
    let runtime = Arc::new(ScriptedRuntime::tool_results(&[]));
    let events = Arc::new(RecordingEvents::default());
    let executor = make_executor_loop(provider, runtime, events);
    let mut plan = structured_plan();
    let mut checkpoint = checkpoint();
    let mut checks = RequiredCheckLedger::try_new(vec![structured_check()]).unwrap();
    let mut ledger = TaskBudgetLedger::try_new().unwrap();

    let error = executor
        .run(
            ExecutorRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &mut plan,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                latest_reviewer_findings: None,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert_eq!(
        error.executor_failure_code(),
        Some("EXECUTOR_ACTION_NOT_ALLOWED")
    );
    assert_eq!(ledger.active_role(), None);
}

#[tokio::test]
async fn executor_structured_secret_is_detected_before_runtime_or_panel_events() {
    let secret_call = ToolCall::runtime(
        "secret-read",
        ToolRequest::ReadFile {
            path: "src/TOP_SECRET.rs".to_owned(),
            start_line: 1,
            end_line: 1,
        },
    );
    let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
        256,
        128,
        batch(vec![secret_call]),
    )]));
    let runtime = Arc::new(ScriptedRuntime::tool_results(&[]));
    let events = Arc::new(RecordingEvents::default());
    let executor = ExecutorRoleLoop::new(
        provider,
        runtime.clone(),
        events.clone(),
        Arc::new(SecretRedactor),
    );
    let mut plan = structured_plan();
    let (mut checkpoint, mut checks) = passed_executor_checks(1);
    let mut ledger = TaskBudgetLedger::try_new().unwrap();

    let error = executor
        .run(
            ExecutorRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &mut plan,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                latest_reviewer_findings: None,
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RoleLoopError::Contract(coding_agent_core::RoleContractError::RedactionMutation)
    ));
    assert_eq!(
        error.executor_failure_code(),
        Some("PROVIDER_SECRET_DETECTED")
    );
    assert!(runtime.requests().is_empty());
    assert!(
        !events
            .ordinary()
            .iter()
            .any(|event| matches!(event, RoleEvent::Diff(_) | RoleEvent::Tests(_)))
    );
    assert!(events.durable().is_empty());
    assert_eq!(ledger.active_role(), None);
    assert_eq!(ledger.pending_reviewer_reservation(), None);
}

#[test]
fn executor_task_reservation_exhaustion_uses_task_budget_code() {
    let error = RoleLoopError::Budget(BudgetError::TaskLimitExceeded {
        resource: BudgetResource::ModelResponses,
    });
    assert_eq!(
        error.executor_failure_code(),
        Some("EXECUTOR_TASK_BUDGET_EXHAUSTED")
    );
}

#[test]
fn planner_and_executor_command_timeouts_use_stage_specific_codes() {
    let planner = RoleLoopError::Runtime(RuntimeError::new(
        "COMMAND_TIMED_OUT",
        "runtime timed out",
        false,
    ));
    let executor = RoleLoopError::Runtime(RuntimeError::new(
        "COMMAND_TIMED_OUT",
        "runtime timed out",
        false,
    ));

    assert_eq!(planner.planner_failure_code(), Some("PLANNER_TIMEOUT"));
    assert_eq!(executor.executor_failure_code(), Some("EXECUTOR_TIMEOUT"));
}

#[tokio::test]
async fn executor_flush_failure_or_wrong_generation_never_returns_submitted() {
    for (events, expected_code) in [
        (
            Arc::new(RecordingEvents::failing_flush()),
            "EXECUTOR_RUNTIME_FAILED",
        ),
        (
            Arc::new(RecordingEvents::wrong_flush_generation()),
            "QUALITY_EVIDENCE_MISMATCH",
        ),
    ] {
        let check = structured_check();
        let provider = Arc::new(ScriptedProvider::new(vec![
            ProviderScript::response(
                256,
                128,
                batch(vec![executor_progress_call(
                    "progress",
                    "step-01",
                    "completed",
                )]),
            ),
            ProviderScript::response(
                512,
                128,
                batch(vec![executor_validation_selector_call(
                    "validation",
                    &check,
                )]),
            ),
            ProviderScript::response(
                768,
                128,
                batch(vec![executor_submission_call("submit", "ready")]),
            ),
        ]));
        let runtime = Arc::new(ScriptedRuntime::with_result(Ok(validation_result(&check))));
        let executor = make_executor_loop(provider, runtime, events);
        let mut plan = structured_plan();
        let mut checkpoint = checkpoint();
        let mut checks = RequiredCheckLedger::try_new(vec![check]).unwrap();
        let mut ledger = TaskBudgetLedger::try_new().unwrap();

        let error = executor
            .run(
                ExecutorRoleInput {
                    review_round: 1,
                    task_prompt: "task",
                    repository_context: "repository",
                    plan: &mut plan,
                    checkpoint: &mut checkpoint,
                    required_checks: &mut checks,
                    latest_reviewer_findings: None,
                },
                &mut ledger,
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.executor_failure_code(), Some(expected_code));
        assert_eq!(ledger.pending_reviewer_reservation(), None);
    }
}

#[tokio::test]
async fn reviewer_full_coverage_becomes_visible_on_terminal_request_and_approves() {
    let (plan, mut checkpoint, mut checks, execution, mut ledger) = prepared_reviewer_state().await;
    let (bundle, terminal) = review_fixture(&checkpoint, "+reviewed\n".to_owned());
    let manifest = bundle.manifest().clone();
    assert_eq!(manifest.chunk_count(), 1);
    let chunk_request = ReviewDiffChunkRequest::for_manifest(&manifest, 0, 1).unwrap();
    let chunks = bundle.chunk_batch(&chunk_request).unwrap();

    let mut scripts = four_reviewer_reads();
    scripts.extend([
        ProviderScript::response(
            1_280,
            128,
            batch(vec![reviewer_manifest_call("review-manifest", &checkpoint)]),
        ),
        ProviderScript::response(
            1_536,
            128,
            batch(vec![reviewer_chunk_call("review-chunk", &manifest, 0, 1)]),
        ),
        ProviderScript::response(
            1_792,
            128,
            batch(vec![reviewer_submission_call(
                "review-approved",
                "approved",
                serde_json::json!([]),
            )]),
        ),
    ]);
    let provider = Arc::new(ScriptedProvider::new(scripts));
    let runtime = Arc::new(ScriptedRuntime::reviewer(
        vec![
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 1"))),
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 2"))),
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 3"))),
            Ok(RoleRuntimeResult::ReviewDiffManifest(manifest.clone())),
            Ok(RoleRuntimeResult::ReviewDiffChunks(chunks)),
        ],
        Vec::new(),
        vec![Ok(terminal)],
        vec![Ok(manifest)],
    ));
    let events = Arc::new(RecordingEvents::default());
    let reviewer = make_reviewer_loop(
        Arc::clone(&provider),
        Arc::clone(&runtime),
        Arc::clone(&events),
    );

    let outcome = reviewer
        .run(
            ReviewerRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &plan,
                execution: &execution,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                previous_reviews: &[],
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap();

    let ReviewerRoleOutcome::Decided(decision) = outcome else {
        panic!("full coverage must approve");
    };
    assert_eq!(decision.evidence().verdict(), ReviewVerdict::Approved);
    let coverage = decision.evidence().coverage().unwrap();
    assert!(coverage.is_complete());
    assert_eq!(coverage.covered_chunks(), [0]);
    assert_eq!(provider.requests().len(), 7);
    assert!(provider.requests()[6].messages.iter().any(|message| {
        matches!(
            message,
            ModelMessage::ToolResult { tool_call_id, .. } if tool_call_id == "review-chunk"
        )
    }));
    assert_eq!(ledger.active_role(), None);
    assert_eq!(ledger.pending_reviewer_reservation(), None);
    assert!(
        events
            .ordinary()
            .iter()
            .any(|event| matches!(event, RoleEvent::Diff(_)))
    );
}

#[tokio::test]
async fn reviewer_partial_coverage_changes_requested_stops_chunks_and_releases_reservation() {
    let (plan, mut checkpoint, mut checks, execution, mut ledger) = prepared_reviewer_state().await;
    let patch = format!("+{}\n", "x".repeat(40_000));
    let (bundle, terminal) = review_fixture(&checkpoint, patch);
    let manifest = bundle.manifest().clone();
    assert!(manifest.chunk_count() > 2);
    let chunk_request = ReviewDiffChunkRequest::for_manifest(&manifest, 0, 2).unwrap();
    let chunks = bundle.chunk_batch(&chunk_request).unwrap();

    let mut scripts = four_reviewer_reads();
    scripts.extend([
        ProviderScript::response(
            1_280,
            128,
            batch(vec![reviewer_manifest_call(
                "partial-manifest",
                &checkpoint,
            )]),
        ),
        ProviderScript::response(
            1_536,
            128,
            batch(vec![reviewer_chunk_call(
                "partial-first-batch",
                &manifest,
                0,
                2,
            )]),
        ),
        ProviderScript::response(
            1_792,
            128,
            batch(vec![reviewer_submission_call(
                "partial-changes",
                "changes_requested",
                serde_json::json!([]),
            )]),
        ),
    ]);
    let provider = Arc::new(ScriptedProvider::new(scripts));
    let runtime = Arc::new(ScriptedRuntime::reviewer(
        vec![
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 1"))),
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 2"))),
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 3"))),
            Ok(RoleRuntimeResult::ReviewDiffManifest(manifest.clone())),
            Ok(RoleRuntimeResult::ReviewDiffChunks(chunks)),
        ],
        Vec::new(),
        vec![Ok(terminal)],
        Vec::new(),
    ));
    let reviewer = make_reviewer_loop(
        Arc::clone(&provider),
        Arc::clone(&runtime),
        Arc::new(RecordingEvents::default()),
    );

    let outcome = reviewer
        .run(
            ReviewerRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &plan,
                execution: &execution,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                previous_reviews: &[],
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap();

    let ReviewerRoleOutcome::Decided(decision) = outcome else {
        panic!("partial coverage may terminate only with changes_requested");
    };
    assert_eq!(
        decision.evidence().verdict(),
        ReviewVerdict::ChangesRequested
    );
    let coverage = decision.evidence().coverage().unwrap();
    assert_eq!(coverage.covered_chunks(), [0, 1]);
    assert!(!coverage.is_complete());
    assert_eq!(provider.requests().len(), 7);
    assert_eq!(runtime.requests().len(), 5);
    assert_eq!(ledger.active_role(), None);
    assert_eq!(ledger.pending_reviewer_reservation(), None);
}

#[tokio::test]
async fn reviewer_final_chunk_terminal_transport_failure_never_creates_evidence() {
    let (plan, mut checkpoint, mut checks, execution, mut ledger) = prepared_reviewer_state().await;
    let (bundle, _terminal) = review_fixture(&checkpoint, "+reviewed\n".to_owned());
    let manifest = bundle.manifest().clone();
    let request = ReviewDiffChunkRequest::for_manifest(&manifest, 0, 1).unwrap();
    let chunks = bundle.chunk_batch(&request).unwrap();
    let mut scripts = four_reviewer_reads();
    scripts.extend([
        ProviderScript::response(
            1_280,
            128,
            batch(vec![reviewer_manifest_call("failed-manifest", &checkpoint)]),
        ),
        ProviderScript::response(
            1_536,
            128,
            batch(vec![reviewer_chunk_call(
                "failed-final-chunk",
                &manifest,
                0,
                1,
            )]),
        ),
        ProviderScript::transport_error(1_792),
    ]);
    let provider = Arc::new(ScriptedProvider::new(scripts));
    let runtime = Arc::new(ScriptedRuntime::reviewer(
        vec![
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 1"))),
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 2"))),
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 3"))),
            Ok(RoleRuntimeResult::ReviewDiffManifest(manifest)),
            Ok(RoleRuntimeResult::ReviewDiffChunks(chunks)),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    ));
    let events = Arc::new(RecordingEvents::default());
    let reviewer = make_reviewer_loop(Arc::clone(&provider), runtime, Arc::clone(&events));

    let error = reviewer
        .run(
            ReviewerRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &plan,
                execution: &execution,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                previous_reviews: &[],
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

    assert_eq!(
        error.reviewer_failure_code(),
        Some("REVIEWER_PROVIDER_FAILED")
    );
    assert_eq!(provider.sends(), 7);
    assert!(
        !events
            .ordinary()
            .iter()
            .any(|event| matches!(event, RoleEvent::Diff(_)))
    );
    assert_eq!(ledger.active_role(), None);
    assert_eq!(ledger.pending_reviewer_reservation(), None);
}

#[tokio::test]
async fn reviewer_optional_check_is_appended_before_run_and_failed_check_cannot_approve() {
    let (plan, mut checkpoint, mut checks, execution, mut ledger) = prepared_reviewer_state().await;
    let optional =
        RequiredCheck::try_cargo_test("check-02", Some("optional-package".to_owned()), None)
            .unwrap();
    let provider = Arc::new(ScriptedProvider::new(vec![
        ProviderScript::response(
            256,
            128,
            batch(vec![reviewer_optional_check_call(
                "optional-check",
                "optional-package",
            )]),
        ),
        ProviderScript::response(
            512,
            128,
            batch(vec![reviewer_submission_call(
                "optional-changes",
                "changes_requested",
                serde_json::json!([]),
            )]),
        ),
    ]));
    let runtime = Arc::new(ScriptedRuntime::reviewer(
        vec![Ok(failed_validation_result(&optional))],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    ));
    let reviewer = make_reviewer_loop(
        Arc::clone(&provider),
        Arc::clone(&runtime),
        Arc::new(RecordingEvents::default()),
    );

    let outcome = reviewer
        .run(
            ReviewerRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &plan,
                execution: &execution,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                previous_reviews: &[],
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap();

    let ReviewerRoleOutcome::Decided(decision) = outcome else {
        panic!("failed optional check must produce changes_requested");
    };
    assert_eq!(
        decision.evidence().verdict(),
        ReviewVerdict::ChangesRequested
    );
    assert!(decision.evidence().coverage().is_none());
    assert_eq!(
        decision.evidence().added_required_checks(),
        std::slice::from_ref(&optional)
    );
    assert_eq!(
        checkpoint
            .current_observation(optional.id())
            .unwrap()
            .status(),
        CheckEvidenceStatus::Failed
    );
    assert!(matches!(
        runtime.requests().as_slice(),
        [RuntimeActionRequest::Validation { check }] if check == &optional
    ));
}

#[tokio::test]
async fn reviewer_submission_only_check_is_excluded_from_system_workspace_change() {
    let (plan, mut checkpoint, mut checks, execution, mut ledger) = prepared_reviewer_state().await;
    let a = WorkspaceFingerprint::from_bytes([0x42; 32]);
    let b = WorkspaceFingerprint::from_bytes([0x43; 32]);
    let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
        256,
        128,
        batch(vec![reviewer_submission_call(
            "mutating-submission",
            "changes_requested",
            serde_json::json!([{
                "kind": "cargo_test",
                "package": "submission-only",
                "integration_test": null
            }]),
        )]),
    )]));
    let runtime = Arc::new(ScriptedRuntime::reviewer(
        Vec::new(),
        vec![a, b],
        vec![Ok(TerminalSnapshot {
            fingerprint: b,
            diff: DiffEvent {
                revision: 1,
                files: Vec::new(),
            },
        })],
        Vec::new(),
    ));
    let reviewer = make_reviewer_loop(
        Arc::clone(&provider),
        runtime,
        Arc::new(RecordingEvents::default()),
    );

    let outcome = reviewer
        .run(
            ReviewerRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &plan,
                execution: &execution,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                previous_reviews: &[],
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap();

    let ReviewerRoleOutcome::Decided(decision) = outcome else {
        panic!("workspace mutation must be a system decision");
    };
    assert_eq!(
        decision.evidence().decision_source(),
        ReviewDecisionSource::System
    );
    assert_eq!(
        decision.evidence().verdict(),
        ReviewVerdict::ChangesRequested
    );
    assert_eq!(
        decision.evidence().findings(),
        [ReviewFinding::system_workspace_changed(1).unwrap()]
    );
    assert!(decision.evidence().added_required_checks().is_empty());
    assert_eq!(decision.evidence().required_checks().len(), 1);
    assert_eq!(checks.checks().len(), 1);
    assert_eq!(checkpoint.generation(), 1);
}

#[tokio::test]
async fn reviewer_runtime_workspace_changed_error_with_advanced_fingerprint_is_system_decision() {
    let (plan, mut checkpoint, mut checks, execution, mut ledger) = prepared_reviewer_state().await;
    let a = WorkspaceFingerprint::from_bytes([0x42; 32]);
    let b = WorkspaceFingerprint::from_bytes([0x43; 32]);
    let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
        256,
        128,
        batch(vec![reviewer_optional_check_call(
            "mutating-check",
            "mutating-package",
        )]),
    )]));
    let runtime = Arc::new(ScriptedRuntime::reviewer(
        vec![Err(RuntimeError::new(
            "WORKSPACE_CHANGED",
            "validation mutated the deliverable",
            false,
        ))],
        vec![a, a, a, b],
        vec![Ok(TerminalSnapshot {
            fingerprint: b,
            diff: DiffEvent {
                revision: 1,
                files: Vec::new(),
            },
        })],
        Vec::new(),
    ));
    let reviewer = make_reviewer_loop(
        Arc::clone(&provider),
        runtime,
        Arc::new(RecordingEvents::default()),
    );

    let outcome = reviewer
        .run(
            ReviewerRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &plan,
                execution: &execution,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                previous_reviews: &[],
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let ReviewerRoleOutcome::Decided(decision) = outcome else {
        panic!("confirmed runtime mutation must be a system decision");
    };
    assert_eq!(
        decision.evidence().decision_source(),
        ReviewDecisionSource::System
    );
    assert_eq!(
        decision.evidence().findings(),
        [ReviewFinding::system_workspace_changed(1).unwrap()]
    );
    assert!(decision.evidence().coverage().is_none());
    assert!(decision.evidence().check_evidence().is_empty());
    assert_eq!(decision.evidence().added_required_checks().len(), 1);
    assert_eq!(provider.sends(), 1);
    assert_eq!(ledger.usage().model_responses(), 3);
    assert_eq!(ledger.usage().model_visible_calls(), 3);
    assert_eq!(ledger.active_role(), None);
}

#[test]
fn reviewer_rounds_one_to_three_use_fresh_exact_handoffs_without_prior_transcripts() {
    let plan = structured_plan();
    let (checkpoint, mut checks) = passed_executor_checks(1);
    let round_1 = ContinuationHandoff::try_for_reviewer(
        1,
        "task",
        "repository",
        &plan,
        "executor-only-summary",
        &checkpoint,
        &checks,
        &[],
        &IdentityRedactor,
    )
    .unwrap();
    let transcript_1 = RoleTranscript::try_fresh(
        RoleRun::try_new(Role::Reviewer, 1).unwrap(),
        "reviewer policy",
        round_1.into(),
        &IdentityRedactor,
    )
    .unwrap();
    assert_eq!(
        transcript_1.request(ModelToolChoice::Auto).messages.len(),
        2
    );

    let check_2 = RequiredCheck::try_cargo_check("check-02", Some("round-two".to_owned())).unwrap();
    checks.append_checks(vec![check_2.clone()]).unwrap();
    let finding_1 = ReviewFinding::try_for_review(
        1,
        1,
        FindingSeverity::Blocking,
        "round one blocking finding",
        Some("src/lib.rs".to_owned()),
        Some(1),
    )
    .unwrap();
    let review_1 = NewReviewEvidence::try_new(
        1,
        ReviewDecisionSource::Reviewer,
        checkpoint.generation(),
        checkpoint.workspace_digest(),
        ReviewVerdict::ChangesRequested,
        "forbidden prior summary one",
        vec![finding_1],
        vec![check_2],
        checks.checks().to_vec(),
        checks.current_evidence(&checkpoint),
        None,
    )
    .unwrap();
    let round_2 = ContinuationHandoff::try_for_reviewer(
        2,
        "task",
        "repository",
        &plan,
        "second executor summary",
        &checkpoint,
        &checks,
        std::slice::from_ref(&review_1),
        &IdentityRedactor,
    )
    .unwrap();
    assert!(
        !round_2
            .canonical_json()
            .contains("forbidden prior summary one")
    );
    let transcript_2 = RoleTranscript::try_fresh(
        RoleRun::try_new(Role::Reviewer, 2).unwrap(),
        "reviewer policy",
        round_2.into(),
        &IdentityRedactor,
    )
    .unwrap();
    assert_eq!(
        transcript_2.request(ModelToolChoice::Auto).messages.len(),
        2
    );

    let check_3 =
        RequiredCheck::try_cargo_check("check-03", Some("round-three".to_owned())).unwrap();
    checks.append_checks(vec![check_3.clone()]).unwrap();
    let finding_2 = ReviewFinding::try_for_review(
        2,
        1,
        FindingSeverity::Blocking,
        "round two blocking finding",
        Some("src/lib.rs".to_owned()),
        Some(2),
    )
    .unwrap();
    let review_2 = NewReviewEvidence::try_new(
        2,
        ReviewDecisionSource::Reviewer,
        checkpoint.generation(),
        checkpoint.workspace_digest(),
        ReviewVerdict::ChangesRequested,
        "forbidden prior summary two",
        vec![finding_2],
        vec![check_3],
        checks.checks().to_vec(),
        checks.current_evidence(&checkpoint),
        None,
    )
    .unwrap();
    let round_3 = ContinuationHandoff::try_for_reviewer(
        3,
        "task",
        "repository",
        &plan,
        "third executor summary",
        &checkpoint,
        &checks,
        &[review_1.clone(), review_2],
        &IdentityRedactor,
    )
    .unwrap();
    for forbidden in [
        "forbidden prior summary one",
        "forbidden prior summary two",
        "provider request id",
        "opaque reasoning",
        "tool result",
    ] {
        assert!(!round_3.canonical_json().contains(forbidden));
    }
    let transcript_3 = RoleTranscript::try_fresh(
        RoleRun::try_new(Role::Reviewer, 3).unwrap(),
        "reviewer policy",
        round_3.into(),
        &IdentityRedactor,
    )
    .unwrap();
    assert_eq!(
        transcript_3.request(ModelToolChoice::Auto).messages.len(),
        2
    );

    let extra = RequiredCheck::try_cargo_check("check-02", Some("extra".to_owned())).unwrap();
    let extra_checks = RequiredCheckLedger::try_new(vec![structured_check(), extra]).unwrap();
    assert!(
        ContinuationHandoff::try_for_reviewer(
            1,
            "task",
            "repository",
            &plan,
            "executor",
            &checkpoint,
            &extra_checks,
            &[],
            &IdentityRedactor,
        )
        .is_err()
    );
    assert!(
        ContinuationHandoff::try_for_reviewer(
            3,
            "task",
            "repository",
            &plan,
            "executor",
            &checkpoint,
            &checks,
            std::slice::from_ref(&review_1),
            &IdentityRedactor,
        )
        .is_err()
    );
}

#[tokio::test]
async fn reviewer_required_manifest_and_chunk_workspace_errors_commit_system_decision() {
    for fail_on_chunk in [false, true] {
        let (plan, mut checkpoint, mut checks, execution, mut ledger) =
            prepared_reviewer_state().await;
        let a = WorkspaceFingerprint::from_bytes([0x42; 32]);
        let b = WorkspaceFingerprint::from_bytes([0x43; 32]);
        let (bundle, _) = review_fixture(&checkpoint, "+reviewed\n".to_owned());
        let manifest = bundle.manifest().clone();
        let chunk_request = ReviewDiffChunkRequest::for_manifest(&manifest, 0, 1).unwrap();

        let mut provider_scripts = four_reviewer_reads();
        provider_scripts.push(ProviderScript::response(
            1_280,
            128,
            batch(vec![reviewer_manifest_call(
                "changing-manifest",
                &checkpoint,
            )]),
        ));
        if fail_on_chunk {
            provider_scripts.push(ProviderScript::response(
                1_536,
                128,
                batch(vec![reviewer_chunk_call("changing-chunk", &manifest, 0, 1)]),
            ));
        }
        let provider = Arc::new(ScriptedProvider::new(provider_scripts));
        let mut runtime_scripts = vec![
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 1"))),
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 2"))),
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 3"))),
        ];
        if fail_on_chunk {
            runtime_scripts.push(Ok(RoleRuntimeResult::ReviewDiffManifest(manifest.clone())));
            runtime_scripts.push(Err(RuntimeError::new(
                "WORKTREE_CHANGED_DURING_DIFF",
                "chunk collection raced with a workspace change",
                false,
            )));
        } else {
            runtime_scripts.push(Err(RuntimeError::new(
                "WORKSPACE_CHANGED",
                "manifest collection raced with a workspace change",
                false,
            )));
        }
        let stable_observations = if fail_on_chunk { 19 } else { 16 };
        let fingerprints = std::iter::repeat_n(a, stable_observations)
            .chain(std::iter::once(b))
            .collect();
        let runtime = Arc::new(ScriptedRuntime::reviewer(
            runtime_scripts,
            fingerprints,
            vec![Ok(TerminalSnapshot {
                fingerprint: b,
                diff: DiffEvent {
                    revision: 1,
                    files: Vec::new(),
                },
            })],
            Vec::new(),
        ));
        let reviewer = make_reviewer_loop(
            Arc::clone(&provider),
            Arc::clone(&runtime),
            Arc::new(RecordingEvents::default()),
        );

        let outcome = reviewer
            .run(
                ReviewerRoleInput {
                    review_round: 1,
                    task_prompt: "task",
                    repository_context: "repository",
                    plan: &plan,
                    execution: &execution,
                    checkpoint: &mut checkpoint,
                    required_checks: &mut checks,
                    previous_reviews: &[],
                },
                &mut ledger,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let ReviewerRoleOutcome::Decided(decision) = outcome else {
            panic!("confirmed required coverage race must be a system decision");
        };
        assert_eq!(
            decision.evidence().decision_source(),
            ReviewDecisionSource::System
        );
        assert_eq!(checkpoint.generation(), 1);
        assert_eq!(ledger.active_role(), None);
        assert_eq!(ledger.pending_reviewer_reservation(), None);
        assert_eq!(runtime.requests().len(), if fail_on_chunk { 5 } else { 4 });
        if fail_on_chunk {
            assert_eq!(
                bundle.chunk_batch(&chunk_request).unwrap().chunks().len(),
                1
            );
        }
    }
}

#[tokio::test]
async fn reviewer_terminal_recapture_truncation_mismatch_and_flush_cancellation_fail_unreviewed() {
    #[derive(Clone, Copy)]
    enum Case {
        Truncated,
        PatchMismatch,
        ManifestMismatch,
        CancelDuringFlush,
    }

    for case in [
        Case::Truncated,
        Case::PatchMismatch,
        Case::ManifestMismatch,
        Case::CancelDuringFlush,
    ] {
        let (plan, mut checkpoint, mut checks, execution, mut ledger) =
            prepared_reviewer_state().await;
        let (bundle, mut terminal) = review_fixture(&checkpoint, "+reviewed\n".to_owned());
        let manifest = bundle.manifest().clone();
        let chunk_request = ReviewDiffChunkRequest::for_manifest(&manifest, 0, 1).unwrap();
        let chunks = bundle.chunk_batch(&chunk_request).unwrap();
        if matches!(case, Case::Truncated) {
            terminal.diff.files[0].truncated = true;
        }
        if matches!(case, Case::PatchMismatch) {
            terminal.diff.files[0].patch = "+different\n".to_owned();
        }
        let terminal_manifest = if matches!(case, Case::ManifestMismatch) {
            review_fixture(&checkpoint, "+different\n".to_owned())
                .0
                .manifest()
                .clone()
        } else {
            manifest.clone()
        };

        let mut scripts = four_reviewer_reads();
        scripts.extend([
            ProviderScript::response(
                1_280,
                128,
                batch(vec![reviewer_manifest_call(
                    "terminal-manifest",
                    &checkpoint,
                )]),
            ),
            ProviderScript::response(
                1_536,
                128,
                batch(vec![reviewer_chunk_call("terminal-chunk", &manifest, 0, 1)]),
            ),
            ProviderScript::response(
                1_792,
                128,
                batch(vec![reviewer_submission_call(
                    "terminal-approved",
                    "approved",
                    serde_json::json!([]),
                )]),
            ),
        ]);
        let provider = Arc::new(ScriptedProvider::new(scripts));
        let runtime = Arc::new(ScriptedRuntime::reviewer(
            vec![
                Ok(RoleRuntimeResult::Tool(ToolResult::text("read 1"))),
                Ok(RoleRuntimeResult::Tool(ToolResult::text("read 2"))),
                Ok(RoleRuntimeResult::Tool(ToolResult::text("read 3"))),
                Ok(RoleRuntimeResult::ReviewDiffManifest(manifest)),
                Ok(RoleRuntimeResult::ReviewDiffChunks(chunks)),
            ],
            Vec::new(),
            vec![Ok(terminal)],
            vec![Ok(terminal_manifest)],
        ));
        let events = if matches!(case, Case::CancelDuringFlush) {
            Arc::new(RecordingEvents::cancelling_flush())
        } else {
            Arc::new(RecordingEvents::default())
        };
        let reviewer = make_reviewer_loop(provider, runtime, events);
        let error = reviewer
            .run(
                ReviewerRoleInput {
                    review_round: 1,
                    task_prompt: "task",
                    repository_context: "repository",
                    plan: &plan,
                    execution: &execution,
                    checkpoint: &mut checkpoint,
                    required_checks: &mut checks,
                    previous_reviews: &[],
                },
                &mut ledger,
                CancellationToken::new(),
            )
            .await
            .unwrap_err();

        match case {
            Case::Truncated => {
                assert!(matches!(error, RoleLoopError::TerminalDiffTruncated));
                assert_eq!(
                    error.reviewer_failure_code(),
                    Some("TERMINAL_DIFF_TRUNCATED")
                );
            }
            Case::PatchMismatch | Case::ManifestMismatch => {
                assert!(matches!(error, RoleLoopError::QualityEvidenceMismatch));
                assert_eq!(
                    error.reviewer_failure_code(),
                    Some("QUALITY_EVIDENCE_MISMATCH")
                );
            }
            Case::CancelDuringFlush => {
                assert!(matches!(error, RoleLoopError::Cancelled));
                assert_eq!(error.reviewer_failure_code(), None);
            }
        }
        assert_eq!(ledger.active_role(), None);
        assert_eq!(ledger.pending_reviewer_reservation(), None);
    }
}

#[tokio::test]
async fn reviewer_zero_and_multi_chunk_approval_cover_every_chunk_without_auto_rerunning_checks() {
    for zero_chunk in [true, false] {
        let (plan, mut checkpoint, mut checks, execution, mut ledger) =
            prepared_reviewer_state().await;
        let (bundle, terminal) = if zero_chunk {
            let authority = ReviewDiffCheckpoint::from_workspace_checkpoint(&checkpoint);
            (
                ReviewDiffBundle::try_new(&authority, Vec::new(), &IdentityRedactor).unwrap(),
                TerminalSnapshot {
                    fingerprint: checkpoint.fingerprint(),
                    diff: DiffEvent {
                        revision: checkpoint.generation(),
                        files: Vec::new(),
                    },
                },
            )
        } else {
            review_fixture(&checkpoint, format!("+{}\n", "x".repeat(40_000)))
        };
        let manifest = bundle.manifest().clone();
        assert_eq!(manifest.chunk_count() == 0, zero_chunk);
        if !zero_chunk {
            assert!(manifest.chunk_count() > 2);
            assert!(manifest.required_batch_count() >= 2);
        }

        let mut scripts = four_reviewer_reads();
        scripts.push(ProviderScript::response(
            1_280,
            128,
            batch(vec![reviewer_manifest_call(
                "complete-manifest",
                &checkpoint,
            )]),
        ));
        let mut runtime_scripts = vec![
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 1"))),
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 2"))),
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 3"))),
            Ok(RoleRuntimeResult::ReviewDiffManifest(manifest.clone())),
        ];
        for batch_index in 0..manifest.required_batch_count() {
            let start = batch_index * 2;
            let count = (manifest.chunk_count() - start).min(2);
            let request = ReviewDiffChunkRequest::for_manifest(&manifest, start, count).unwrap();
            runtime_scripts.push(Ok(RoleRuntimeResult::ReviewDiffChunks(
                bundle.chunk_batch(&request).unwrap(),
            )));
            scripts.push(ProviderScript::response(
                1_536 + usize::from(batch_index) * 256,
                128,
                batch(vec![reviewer_chunk_call(
                    &format!("complete-chunk-{batch_index}"),
                    &manifest,
                    start,
                    count,
                )]),
            ));
        }
        scripts.push(ProviderScript::response(
            2_304,
            128,
            batch(vec![reviewer_submission_call(
                "complete-approved",
                "approved",
                serde_json::json!([]),
            )]),
        ));
        let provider = Arc::new(ScriptedProvider::new(scripts));
        let runtime = Arc::new(ScriptedRuntime::reviewer(
            runtime_scripts,
            Vec::new(),
            vec![Ok(terminal)],
            vec![Ok(manifest.clone())],
        ));
        let reviewer = make_reviewer_loop(
            Arc::clone(&provider),
            Arc::clone(&runtime),
            Arc::new(RecordingEvents::default()),
        );
        let outcome = reviewer
            .run(
                ReviewerRoleInput {
                    review_round: 1,
                    task_prompt: "task",
                    repository_context: "repository",
                    plan: &plan,
                    execution: &execution,
                    checkpoint: &mut checkpoint,
                    required_checks: &mut checks,
                    previous_reviews: &[],
                },
                &mut ledger,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let ReviewerRoleOutcome::Decided(decision) = outcome else {
            panic!("complete zero/multi chunk coverage must approve");
        };
        let coverage = decision.evidence().coverage().unwrap();
        assert!(coverage.is_complete());
        assert_eq!(coverage.total_chunks(), manifest.chunk_count());
        assert_eq!(
            coverage.covered_chunks(),
            (0..manifest.chunk_count()).collect::<Vec<_>>()
        );
        assert!(!runtime.requests().iter().any(|request| {
            matches!(
                request,
                RuntimeActionRequest::Validation { .. }
                    | RuntimeActionRequest::ValidationSelector { .. }
            )
        }));
        if !zero_chunk {
            let final_chunk_id = format!("complete-chunk-{}", manifest.required_batch_count() - 1);
            let requests = provider.requests();
            assert!(requests.last().unwrap().messages.iter().any(|message| {
                matches!(
                    message,
                    ModelMessage::ToolResult { tool_call_id, .. }
                        if tool_call_id == &final_chunk_id
                )
            }));
        }
    }
}

#[tokio::test]
async fn reviewer_full_coverage_still_rejects_approval_after_optional_check_failed() {
    let (plan, mut checkpoint, mut checks, execution, mut ledger) = prepared_reviewer_state().await;
    let optional =
        RequiredCheck::try_cargo_test("check-02", Some("failing-optional".to_owned()), None)
            .unwrap();
    let (bundle, terminal) = review_fixture(&checkpoint, "+reviewed\n".to_owned());
    let manifest = bundle.manifest().clone();
    let chunk_request = ReviewDiffChunkRequest::for_manifest(&manifest, 0, 1).unwrap();
    let chunks = bundle.chunk_batch(&chunk_request).unwrap();
    let mut scripts = vec![
        ProviderScript::response(
            256,
            128,
            batch(vec![reviewer_optional_check_call(
                "failing-optional",
                "failing-optional",
            )]),
        ),
        ProviderScript::response(512, 128, batch(vec![read_call("after-failure-1")])),
        ProviderScript::response(768, 128, batch(vec![read_call("after-failure-2")])),
        ProviderScript::response(
            1_024,
            128,
            batch(
                (0..8)
                    .map(|index| read_call(&format!("failure-reserved-{index}")))
                    .collect(),
            ),
        ),
    ];
    scripts.extend([
        ProviderScript::response(
            1_280,
            128,
            batch(vec![reviewer_manifest_call(
                "failure-manifest",
                &checkpoint,
            )]),
        ),
        ProviderScript::response(
            1_536,
            128,
            batch(vec![reviewer_chunk_call("failure-chunk", &manifest, 0, 1)]),
        ),
        ProviderScript::response(
            1_792,
            128,
            batch(vec![reviewer_submission_call(
                "failure-approved",
                "approved",
                serde_json::json!([]),
            )]),
        ),
    ]);
    let runtime = Arc::new(ScriptedRuntime::reviewer(
        vec![
            Ok(failed_validation_result(&optional)),
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 1"))),
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 2"))),
            Ok(RoleRuntimeResult::ReviewDiffManifest(manifest.clone())),
            Ok(RoleRuntimeResult::ReviewDiffChunks(chunks)),
        ],
        Vec::new(),
        vec![Ok(terminal)],
        vec![Ok(manifest)],
    ));
    let events = Arc::new(RecordingEvents::default());
    let reviewer = make_reviewer_loop(
        Arc::new(ScriptedProvider::new(scripts)),
        runtime,
        Arc::clone(&events),
    );

    let error = reviewer
        .run(
            ReviewerRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &plan,
                execution: &execution,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                previous_reviews: &[],
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RoleLoopError::InvalidReviewerOutput));
    assert_eq!(
        error.reviewer_failure_code(),
        Some("REVIEWER_INVALID_OUTPUT")
    );
    assert_eq!(
        checkpoint
            .current_observation(optional.id())
            .unwrap()
            .status(),
        CheckEvidenceStatus::Failed
    );
    assert!(
        !events
            .ordinary()
            .iter()
            .any(|event| matches!(event, RoleEvent::Diff(_)))
    );
    assert_eq!(ledger.active_role(), None);
}

#[tokio::test]
async fn reviewer_and_executor_terminal_diff_events_never_emit_raw_secrets() {
    {
        let provider = Arc::new(ScriptedProvider::new(vec![
            ProviderScript::response(
                256,
                128,
                batch(vec![executor_progress_call(
                    "secret-progress",
                    "step-01",
                    "completed",
                )]),
            ),
            ProviderScript::response(
                512,
                128,
                batch(vec![executor_submission_call(
                    "secret-submit",
                    "ready for review",
                )]),
            ),
        ]));
        let terminal = TerminalSnapshot {
            fingerprint: WorkspaceFingerprint::from_bytes([0x42; 32]),
            diff: DiffEvent {
                revision: 0,
                files: vec![DiffFile {
                    path: "src/lib.rs".to_owned(),
                    status: DiffFileStatus::Modified,
                    patch: "+token=TOP_SECRET\n".to_owned(),
                    additions: 1,
                    deletions: 0,
                    truncated: false,
                }],
            },
        };
        let runtime = Arc::new(ScriptedRuntime::reviewer(
            Vec::new(),
            Vec::new(),
            vec![Ok(terminal)],
            Vec::new(),
        ));
        let events = Arc::new(RecordingEvents::default());
        let executor =
            ExecutorRoleLoop::new(provider, runtime, events.clone(), Arc::new(SecretRedactor));
        let mut plan = structured_plan();
        let (mut checkpoint, mut checks) = passed_executor_checks(1);
        let mut ledger = TaskBudgetLedger::try_new().unwrap();
        let outcome = executor
            .run(
                ExecutorRoleInput {
                    review_round: 1,
                    task_prompt: "task",
                    repository_context: "repository",
                    plan: &mut plan,
                    checkpoint: &mut checkpoint,
                    required_checks: &mut checks,
                    latest_reviewer_findings: None,
                },
                &mut ledger,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, ExecutorRoleOutcome::Submitted(_)));
        assert!(events.ordinary().iter().any(|event| {
            matches!(
                event,
                RoleEvent::Diff(diff)
                    if diff.files[0].patch == "+token=[REDACTED]\n"
                        && !diff.files[0].patch.contains("TOP_SECRET")
            )
        }));
    }

    {
        let (plan, mut checkpoint, mut checks, execution, mut ledger) =
            prepared_reviewer_state().await;
        let raw_patch = "+token=TOP_SECRET\n".to_owned();
        let authority = ReviewDiffCheckpoint::from_workspace_checkpoint(&checkpoint);
        let bundle = ReviewDiffBundle::try_new(
            &authority,
            vec![
                ReviewDiffInputFile::try_new(
                    "src/lib.rs",
                    DiffFileStatus::Modified,
                    1,
                    0,
                    raw_patch.clone(),
                )
                .unwrap(),
            ],
            &SecretRedactor,
        )
        .unwrap();
        let manifest = bundle.manifest().clone();
        let chunk_request = ReviewDiffChunkRequest::for_manifest(&manifest, 0, 1).unwrap();
        let chunks = bundle.chunk_batch(&chunk_request).unwrap();
        let terminal = TerminalSnapshot {
            fingerprint: checkpoint.fingerprint(),
            diff: DiffEvent {
                revision: checkpoint.generation(),
                files: vec![DiffFile {
                    path: "src/lib.rs".to_owned(),
                    status: DiffFileStatus::Modified,
                    patch: raw_patch,
                    additions: 1,
                    deletions: 0,
                    truncated: false,
                }],
            },
        };
        let mut scripts = four_reviewer_reads();
        scripts.extend([
            ProviderScript::response(
                1_280,
                128,
                batch(vec![reviewer_manifest_call("secret-manifest", &checkpoint)]),
            ),
            ProviderScript::response(
                1_536,
                128,
                batch(vec![reviewer_chunk_call("secret-chunk", &manifest, 0, 1)]),
            ),
            ProviderScript::response(
                1_792,
                128,
                batch(vec![reviewer_submission_call(
                    "secret-approved",
                    "approved",
                    serde_json::json!([]),
                )]),
            ),
        ]);
        let runtime = Arc::new(ScriptedRuntime::reviewer(
            vec![
                Ok(RoleRuntimeResult::Tool(ToolResult::text("read 1"))),
                Ok(RoleRuntimeResult::Tool(ToolResult::text("read 2"))),
                Ok(RoleRuntimeResult::Tool(ToolResult::text("read 3"))),
                Ok(RoleRuntimeResult::ReviewDiffManifest(manifest.clone())),
                Ok(RoleRuntimeResult::ReviewDiffChunks(chunks)),
            ],
            Vec::new(),
            vec![Ok(terminal)],
            vec![Ok(manifest)],
        ));
        let events = Arc::new(RecordingEvents::default());
        let reviewer = ReviewerRoleLoop::new(
            Arc::new(ScriptedProvider::new(scripts)),
            runtime,
            events.clone(),
            Arc::new(SecretRedactor),
        );
        let outcome = reviewer
            .run(
                ReviewerRoleInput {
                    review_round: 1,
                    task_prompt: "task",
                    repository_context: "repository",
                    plan: &plan,
                    execution: &execution,
                    checkpoint: &mut checkpoint,
                    required_checks: &mut checks,
                    previous_reviews: &[],
                },
                &mut ledger,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, ReviewerRoleOutcome::Decided(_)));
        assert!(events.ordinary().iter().any(|event| {
            matches!(
                event,
                RoleEvent::Diff(diff)
                    if diff.files[0].patch == "+token=[REDACTED]\n"
                        && !diff.files[0].patch.contains("TOP_SECRET")
            )
        }));
    }
}

#[tokio::test]
async fn reviewer_barrier_workspace_race_rolls_back_submission_checks_and_converges_to_system() {
    let (plan, mut checkpoint, mut checks, execution, mut ledger) = prepared_reviewer_state().await;
    let a = WorkspaceFingerprint::from_bytes([0x42; 32]);
    let b = WorkspaceFingerprint::from_bytes([0x43; 32]);
    let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
        256,
        128,
        batch(vec![reviewer_submission_call(
            "barrier-changes",
            "changes_requested",
            serde_json::json!([{
                "kind": "cargo_check",
                "package": "submission-barrier"
            }]),
        )]),
    )]));
    let runtime = Arc::new(ScriptedRuntime::reviewer(
        Vec::new(),
        Vec::new(),
        vec![
            Ok(TerminalSnapshot {
                fingerprint: a,
                diff: DiffEvent {
                    revision: 0,
                    files: Vec::new(),
                },
            }),
            Ok(TerminalSnapshot {
                fingerprint: b,
                diff: DiffEvent {
                    revision: 1,
                    files: Vec::new(),
                },
            }),
        ],
        Vec::new(),
    ));
    let events = Arc::new(RecordingEvents::advancing_flush(Arc::clone(&runtime), b));
    let reviewer = make_reviewer_loop(provider, runtime, Arc::clone(&events));

    let outcome = reviewer
        .run(
            ReviewerRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &plan,
                execution: &execution,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                previous_reviews: &[],
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let ReviewerRoleOutcome::Decided(decision) = outcome else {
        panic!("barrier race must converge to a system decision");
    };
    assert_eq!(
        decision.evidence().decision_source(),
        ReviewDecisionSource::System
    );
    assert_eq!(decision.evidence().workspace_generation(), 1);
    assert!(decision.evidence().added_required_checks().is_empty());
    assert_eq!(checks.checks().len(), 1);
    assert_eq!(events.flush_generations(), [0, 1]);
    assert_eq!(decision.durable_sequence(), 2);
    let panels = events
        .ordinary()
        .into_iter()
        .filter_map(|event| match event {
            RoleEvent::Diff(diff) => Some(("diff", diff.revision)),
            RoleEvent::Tests(tests) => Some(("tests", tests.revision)),
            RoleEvent::Activity(_) => None,
        })
        .collect::<Vec<_>>();
    assert!(panels.ends_with(&[("diff", 0), ("tests", 0), ("diff", 1), ("tests", 1),]));
}

#[tokio::test]
async fn reviewer_workspace_error_without_fingerprint_advance_preserves_error_and_cleans_check() {
    let (plan, mut checkpoint, mut checks, execution, mut ledger) = prepared_reviewer_state().await;
    let optional =
        RequiredCheck::try_cargo_test("check-02", Some("unchanged-error".to_owned()), None)
            .unwrap();
    let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
        256,
        128,
        batch(vec![reviewer_optional_check_call(
            "unchanged-error",
            "unchanged-error",
        )]),
    )]));
    let runtime = Arc::new(ScriptedRuntime::reviewer(
        vec![Err(RuntimeError::new(
            "WORKSPACE_CHANGED",
            "runtime reported a race",
            false,
        ))],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    ));
    let events = Arc::new(RecordingEvents::default());
    let reviewer = make_reviewer_loop(Arc::clone(&provider), runtime, Arc::clone(&events));

    let error = reviewer
        .run(
            ReviewerRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &plan,
                execution: &execution,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                previous_reviews: &[],
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&error, RoleLoopError::Runtime(runtime) if runtime.code == "WORKSPACE_CHANGED")
    );
    assert_eq!(provider.sends(), 1);
    assert_eq!(checkpoint.generation(), 0);
    let statuses = events
        .ordinary()
        .into_iter()
        .filter_map(|event| match event {
            RoleEvent::Tests(snapshot) => Some(snapshot.status),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(statuses.contains(&TestStatus::Running));
    assert_eq!(statuses.last(), Some(&TestStatus::Queued));
    checks
        .queue_check(&mut checkpoint, optional.id())
        .expect("failed runtime attempt must not leave the optional check active");
    assert_eq!(ledger.active_role(), None);
    assert_eq!(ledger.pending_reviewer_reservation(), None);
}

#[tokio::test]
async fn reviewer_required_failure_token_is_aborted_when_system_flush_is_cancelled() {
    let (plan, mut checkpoint, mut checks, execution, mut ledger) = prepared_reviewer_state().await;
    let a = WorkspaceFingerprint::from_bytes([0x42; 32]);
    let b = WorkspaceFingerprint::from_bytes([0x43; 32]);
    let (bundle, _) = review_fixture(&checkpoint, "+reviewed\n".to_owned());
    let manifest = bundle.manifest().clone();
    let mut scripts = four_reviewer_reads();
    scripts.push(ProviderScript::response(
        1_280,
        128,
        batch(vec![reviewer_manifest_call(
            "cancelled-system-manifest",
            &checkpoint,
        )]),
    ));
    let runtime = Arc::new(ScriptedRuntime::reviewer(
        vec![
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 1"))),
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 2"))),
            Ok(RoleRuntimeResult::Tool(ToolResult::text("read 3"))),
            Err(RuntimeError::new(
                "WORKSPACE_CHANGED",
                "manifest raced",
                false,
            )),
        ],
        std::iter::repeat_n(a, 16)
            .chain(std::iter::once(b))
            .collect(),
        vec![Ok(TerminalSnapshot {
            fingerprint: b,
            diff: DiffEvent {
                revision: 1,
                files: Vec::new(),
            },
        })],
        Vec::new(),
    ));
    let events = Arc::new(RecordingEvents::cancelling_flush());
    let reviewer = make_reviewer_loop(Arc::new(ScriptedProvider::new(scripts)), runtime, events);

    let error = reviewer
        .run(
            ReviewerRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &plan,
                execution: &execution,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                previous_reviews: &[],
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RoleLoopError::Cancelled));
    assert_eq!(ledger.active_role(), None);
    assert_eq!(ledger.pending_reviewer_reservation(), None);
    assert_eq!(manifest.generation(), 0);
}

#[tokio::test]
async fn reviewer_provider_response_cancel_race_wins_before_report_blocked_is_interpreted() {
    let (plan, mut checkpoint, mut checks, execution, mut ledger) = prepared_reviewer_state().await;
    let blocked = ToolCall {
        id: "cancelled-blocked".to_owned(),
        request: ActionRequest::decode(
            Role::Reviewer,
            "report_blocked",
            r#"{"reason":"missing_required_context","summary":"context unavailable"}"#,
        )
        .unwrap(),
    };
    let provider = Arc::new(ScriptedProvider::new(vec![
        ProviderScript::response_then_cancel(256, 128, batch(vec![blocked])),
    ]));
    let runtime = Arc::new(ScriptedRuntime::tool_results(&[]));
    let reviewer = make_reviewer_loop(
        Arc::clone(&provider),
        Arc::clone(&runtime),
        Arc::new(RecordingEvents::default()),
    );

    let error = reviewer
        .run(
            ReviewerRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &plan,
                execution: &execution,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                previous_reviews: &[],
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RoleLoopError::Cancelled));
    assert_eq!(provider.sends(), 1);
    assert!(runtime.requests().is_empty());
    assert_eq!(ledger.active_role(), None);
    assert_eq!(ledger.pending_reviewer_reservation(), None);
}

#[tokio::test]
async fn reviewer_structured_secret_is_detected_before_runtime_or_panel_events() {
    let (plan, mut checkpoint, mut checks, execution, mut ledger) = prepared_reviewer_state().await;
    let secret_call = ToolCall::runtime(
        "secret-read",
        ToolRequest::ReadFile {
            path: "src/TOP_SECRET.rs".to_owned(),
            start_line: 1,
            end_line: 1,
        },
    );
    let provider = Arc::new(ScriptedProvider::new(vec![ProviderScript::response(
        256,
        128,
        batch(vec![secret_call]),
    )]));
    let runtime = Arc::new(ScriptedRuntime::tool_results(&[]));
    let events = Arc::new(RecordingEvents::default());
    let reviewer = ReviewerRoleLoop::new(
        provider,
        runtime.clone(),
        events.clone(),
        Arc::new(SecretRedactor),
    );

    let error = reviewer
        .run(
            ReviewerRoleInput {
                review_round: 1,
                task_prompt: "task",
                repository_context: "repository",
                plan: &plan,
                execution: &execution,
                checkpoint: &mut checkpoint,
                required_checks: &mut checks,
                previous_reviews: &[],
            },
            &mut ledger,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RoleLoopError::Contract(coding_agent_core::RoleContractError::RedactionMutation)
    ));
    assert_eq!(
        error.reviewer_failure_code(),
        Some("PROVIDER_SECRET_DETECTED")
    );
    assert!(runtime.requests().is_empty());
    assert!(
        !events
            .ordinary()
            .iter()
            .any(|event| { matches!(event, RoleEvent::Diff(_) | RoleEvent::Tests(_)) })
    );
    assert_eq!(ledger.active_role(), None);
}
