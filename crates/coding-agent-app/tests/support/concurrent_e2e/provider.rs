use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use coding_agent_app::{TaskModelProviderFactory, TaskModelSession};
use coding_agent_core::{
    ActionRequest, ContextRedactor, ControlKind, ModelMessage, ModelRequest, ModelResponse,
    ModelToolChoice, PreparedModelProvider, PreparedProviderRequest, ProviderError,
    RawProviderResponse, RequiredAction, Role, RuntimeActionRequest, ToolCall, ToolCallBatch,
    ToolRequest,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::{CHANGED_SOURCE, INTEGRATION_TEST, PACKAGE};

pub(super) struct ScriptedProviderFactory {
    role_barrier: Arc<RoleLoopBarrier>,
}

impl ScriptedProviderFactory {
    pub(super) fn new(role_barrier: Arc<RoleLoopBarrier>) -> Self {
        Self { role_barrier }
    }
}

impl TaskModelProviderFactory for ScriptedProviderFactory {
    fn start_task(&self) -> TaskModelSession {
        TaskModelSession::new(
            Arc::new(ScriptedProvider {
                first_request: AtomicBool::new(true),
                role_barrier: Arc::clone(&self.role_barrier),
            }),
            Arc::new(IdentityRedactor),
        )
    }
}

struct ScriptedProvider {
    first_request: AtomicBool,
    role_barrier: Arc<RoleLoopBarrier>,
}

impl PreparedModelProvider for ScriptedProvider {
    fn prepare(
        &self,
        request: ModelRequest,
    ) -> Result<Box<dyn PreparedProviderRequest>, ProviderError> {
        let response = response_for(&request)?;
        let block_on_role_barrier = self.first_request.swap(false, Ordering::AcqRel);
        Ok(Box::new(ScriptedPreparedRequest {
            response,
            role_barrier: Arc::clone(&self.role_barrier),
            block_on_role_barrier,
        }))
    }
}

struct ScriptedPreparedRequest {
    response: ModelResponse,
    role_barrier: Arc<RoleLoopBarrier>,
    block_on_role_barrier: bool,
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
        if self.block_on_role_barrier {
            self.role_barrier.enter_and_wait().await;
        }
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

fn response_for(request: &ModelRequest) -> Result<ModelResponse, ProviderError> {
    let role = request
        .allowed_actions
        .role()
        .ok_or_else(|| scripted_provider_error("SCRIPTED_ROLE_MISSING"))?;
    if let Some(required) = required_action(request) {
        return Ok(batch(vec![required_call(role, required)?]));
    }
    let call = match role {
        Role::Planner => submit_plan_call(),
        Role::Executor => executor_call(request)?,
        Role::Reviewer => return reviewer_response(request),
    };
    Ok(batch(vec![call]))
}

fn required_action(request: &ModelRequest) -> Option<&RequiredAction> {
    match &request.tool_choice {
        ModelToolChoice::Required(required) => Some(required),
        _ => None,
    }
}

fn required_call(role: Role, required: &RequiredAction) -> Result<ToolCall, ProviderError> {
    let request = match required {
        RequiredAction::LegacyCargoTest => cargo_test_selector()?,
        RequiredAction::Validation(check) => {
            ActionRequest::Runtime(RuntimeActionRequest::Validation {
                check: check.clone(),
            })
        }
        RequiredAction::ReviewDiffManifest {
            generation,
            workspace_digest,
        }
        | RequiredAction::ReviewDiffManifestOrTerminal {
            generation,
            workspace_digest,
        } => ActionRequest::Runtime(RuntimeActionRequest::ReviewDiffManifest {
            generation: *generation,
            workspace_digest: workspace_digest.clone(),
        }),
        RequiredAction::ReviewDiffChunks {
            generation,
            workspace_digest,
            manifest_sha256,
            start_chunk,
            count,
        }
        | RequiredAction::ReviewDiffChunksOrTerminal {
            generation,
            workspace_digest,
            manifest_sha256,
            start_chunk,
            count,
        } => ActionRequest::Runtime(RuntimeActionRequest::ReviewDiffChunks {
            generation: *generation,
            workspace_digest: workspace_digest.clone(),
            manifest_sha256: manifest_sha256.clone(),
            start_chunk: *start_chunk,
            count: *count,
        }),
        RequiredAction::Terminal(kind) | RequiredAction::TerminalOrBlocked(kind) => {
            terminal_action(role, *kind)?
        }
    };
    Ok(ToolCall {
        id: format!("required-{}", request.name()),
        request,
    })
}

fn terminal_action(role: Role, kind: ControlKind) -> Result<ActionRequest, ProviderError> {
    match kind {
        ControlKind::SubmitPlan => Ok(submit_plan_call().request),
        ControlKind::SubmitExecution => Ok(submit_execution_call().request),
        ControlKind::SubmitReview => Ok(submit_review_call().request),
        ControlKind::UpdatePlanProgress => Ok(update_progress_call().request),
        ControlKind::ReportBlocked => ActionRequest::decode(
            role,
            "report_blocked",
            r#"{"reason":"missing_required_context","summary":"scripted provider was forced to block"}"#,
        )
        .map_err(|_| scripted_provider_error("SCRIPTED_CONTROL_INVALID")),
    }
}

fn executor_call(request: &ModelRequest) -> Result<ToolCall, ProviderError> {
    let history = action_history(request);
    if !history.contains(&"read_file") {
        return Ok(ToolCall::runtime(
            "executor-read",
            ToolRequest::ReadFile {
                path: "src/lib.rs".to_owned(),
                start_line: 1,
                end_line: 12,
            },
        ));
    }
    if !history.contains(&"replace_file") {
        let expected_sha256 = latest_success_payload(request)
            .and_then(|payload| {
                payload
                    .get("sha256")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .ok_or_else(|| scripted_provider_error("SCRIPTED_READ_DIGEST_MISSING"))?;
        return Ok(ToolCall::runtime(
            "executor-replace",
            ToolRequest::ReplaceFile {
                path: "src/lib.rs".to_owned(),
                expected_sha256: Some(expected_sha256),
                content: CHANGED_SOURCE.to_owned(),
            },
        ));
    }
    if !history.contains(&"cargo_test") {
        return Ok(ToolCall {
            id: "executor-test".to_owned(),
            request: cargo_test_selector()?,
        });
    }
    if !history.contains(&"update_plan_progress") {
        return Ok(update_progress_call());
    }
    Ok(submit_execution_call())
}

fn reviewer_response(request: &ModelRequest) -> Result<ModelResponse, ProviderError> {
    let history = action_history(request);
    if history.contains(&"review_diff_manifest") && !history.contains(&"review_diff_chunks") {
        return Err(scripted_provider_error(
            "SCRIPTED_REVIEW_CHUNK_REQUIREMENT_MISSING",
        ));
    }
    if history.contains(&"review_diff_chunks") {
        return Ok(batch(vec![submit_review_call()]));
    }
    let read_count = history.iter().filter(|name| **name == "read_file").count();
    if read_count < 3 {
        return Ok(batch(vec![ToolCall::runtime(
            format!("reviewer-read-{read_count}"),
            ToolRequest::ReadFile {
                path: "src/lib.rs".to_owned(),
                start_line: 1,
                end_line: 12,
            },
        )]));
    }
    if read_count == 3 {
        return Ok(batch(
            (0..8)
                .map(|index| {
                    ToolCall::runtime(
                        format!("reviewer-reserved-read-{index}"),
                        ToolRequest::ReadFile {
                            path: "src/lib.rs".to_owned(),
                            start_line: 1,
                            end_line: 12,
                        },
                    )
                })
                .collect(),
        ));
    }
    Err(scripted_provider_error(
        "SCRIPTED_REVIEW_REQUIREMENT_MISSING",
    ))
}

fn submit_plan_call() -> ToolCall {
    decoded_call(
        Role::Planner,
        "planner-submit",
        "submit_plan",
        r#"{
            "summary":"Change and validate one isolated worktree",
            "steps":[{
                "title":"Implement and validate",
                "description":"Update the answer implementation and run its focused test.",
                "acceptance_criteria":["The answer integration test passes."]
            }],
            "initial_required_checks":[{
                "kind":"cargo_test",
                "package":"offline_fixture",
                "integration_test":"answer"
            }]
        }"#,
    )
}

fn update_progress_call() -> ToolCall {
    decoded_call(
        Role::Executor,
        "executor-progress",
        "update_plan_progress",
        r#"{"updates":[{"step_id":"step-01","status":"completed"}]}"#,
    )
}

fn submit_execution_call() -> ToolCall {
    decoded_call(
        Role::Executor,
        "executor-submit",
        "submit_execution",
        r#"{"summary":"the isolated implementation and current test are complete"}"#,
    )
}

fn submit_review_call() -> ToolCall {
    decoded_call(
        Role::Reviewer,
        "reviewer-submit",
        "submit_review",
        r#"{
            "verdict":"approved",
            "summary":"the complete current diff and passed check are approved",
            "findings":[],
            "add_required_checks":[]
        }"#,
    )
}

fn cargo_test_selector() -> Result<ActionRequest, ProviderError> {
    ActionRequest::decode(
        Role::Executor,
        "cargo_test",
        &format!(r#"{{"package":"{PACKAGE}","integration_test":"{INTEGRATION_TEST}"}}"#),
    )
    .map_err(|_| scripted_provider_error("SCRIPTED_VALIDATION_INVALID"))
}

fn decoded_call(role: Role, id: &str, name: &str, arguments: &str) -> ToolCall {
    ToolCall {
        id: id.to_owned(),
        request: ActionRequest::decode(role, name, arguments)
            .unwrap_or_else(|_| panic!("scripted {name} action is valid")),
    }
}

fn action_history(request: &ModelRequest) -> Vec<&'static str> {
    request
        .messages
        .iter()
        .filter_map(|message| match message {
            ModelMessage::AssistantToolCalls(batch) => Some(batch),
            _ => None,
        })
        .flat_map(|batch| batch.calls.iter().map(|call| call.request.name()))
        .collect()
}

fn latest_success_payload(request: &ModelRequest) -> Option<serde_json::Value> {
    let content = request
        .messages
        .iter()
        .rev()
        .find_map(|message| match message {
            ModelMessage::ToolResult { content, .. } => Some(content.as_str()),
            _ => None,
        })?;
    let (_, payload) = content.split_once('\n')?;
    serde_json::from_str(payload).ok()
}

fn batch(calls: Vec<ToolCall>) -> ModelResponse {
    ModelResponse::ToolCalls(ToolCallBatch {
        assistant_content: None,
        reasoning_content: None,
        calls,
    })
}

fn scripted_provider_error(code: &str) -> ProviderError {
    ProviderError::new(
        code,
        "the offline scripted provider could not continue",
        false,
    )
}

struct IdentityRedactor;

impl ContextRedactor for IdentityRedactor {
    fn redact(&self, content: &str) -> String {
        content.to_owned()
    }
}

pub(super) struct RoleLoopBarrier {
    expected: usize,
    entered: AtomicUsize,
    active: AtomicUsize,
    maximum_active: AtomicUsize,
    entered_notify: Notify,
    release: CancellationToken,
}

impl RoleLoopBarrier {
    pub(super) fn new(expected: usize) -> Self {
        Self {
            expected,
            entered: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            maximum_active: AtomicUsize::new(0),
            entered_notify: Notify::new(),
            release: CancellationToken::new(),
        }
    }

    async fn enter_and_wait(&self) {
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.maximum_active.fetch_max(active, Ordering::AcqRel);
        self.entered.fetch_add(1, Ordering::AcqRel);
        self.entered_notify.notify_waiters();
        self.release.cancelled().await;
        self.active.fetch_sub(1, Ordering::AcqRel);
    }

    pub(super) async fn wait_for_entries(&self, expected: usize) {
        assert_eq!(
            expected, self.expected,
            "the test must wait for the configured role-loop barrier size"
        );
        tokio::time::timeout(Duration::from_secs(120), async {
            loop {
                let notified = self.entered_notify.notified();
                if self.entered.load(Ordering::Acquire) >= expected {
                    return;
                }
                notified.await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "only {} of {expected} role loops reached the barrier",
                self.entered.load(Ordering::Acquire)
            )
        });
    }

    pub(super) fn release(&self) {
        self.release.cancel();
    }

    pub(super) fn maximum_active(&self) -> usize {
        self.maximum_active.load(Ordering::Acquire)
    }
}
