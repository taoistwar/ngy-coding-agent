#![cfg(feature = "test-support")]

mod support;

use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::State;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{Request, Response, StatusCode};
use axum::routing::post;
use coding_agent_api::{ApiBackend, AuthContext, SseBackend, TaskEventDto};
use coding_agent_app::{
    ApplicationBackend, CodingAgentRunner, CodingAgentRunnerConfig, CodingAttemptError,
    EventDispatcherHandle, MutationGate, Project2RuntimeSessionFactory,
    ProvisionedAgentRuntimeFactory, RepositoryDiscovery, RepositoryWorktreeProvisionerFactory,
    ServiceState, ServiceStateController, StoreWriterFaultPoint, StoreWriterFaultSpec,
    StoreWriterHandle, StoreWriterOperationKind, StoreWriterTestController, SystemWallClock,
    TaskAgentRuntime, TaskManagerHandle, TaskModelProviderFactory, TaskModelSession, WallClock,
    WorktreeCodingAgentAttemptFactory,
};
use coding_agent_domain::{
    ActivityActor, CanonicalPath, CheckEvidenceStatus, DeliveryReadiness, EventCursor,
    NewRepository, Repository, ReviewVerdict, Task, TaskEventKind, TaskId, TaskStatus,
    UtcTimestamp,
};
use coding_agent_provider::{ChatCompletionsClient, ClientLimits, ProviderConfig};
use coding_agent_runtime::{
    ProcessLimits, ProvisionedWorktree, ToolchainPaths, WorktreeLimits, WorktreeProvisioner,
    discover_toolchain,
};
use coding_agent_store::{AttemptArtifactState, RegisterRepositoryOutcome, Store};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const API_KEY: &str = "offline-multi-role-key";
const PACKAGE: &str = "offline_fixture";
const INTEGRATION_TEST: &str = "answer";
const INITIAL_SOURCE: &str = "pub fn answer() -> u32 { 41 }\n";

static E2E_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Journey {
    Quality {
        rework_rounds: u8,
        final_approved: bool,
    },
    PlannerBlocked,
    ProviderDisconnect,
    Blocking,
    BudgetExhaustion,
    RuntimeFailure,
    ReviewerMutation,
    InvalidCoverage,
}

impl Journey {
    fn quality(rework_rounds: u8, final_approved: bool) -> Self {
        assert!(rework_rounds <= 2);
        Self::Quality {
            rework_rounds,
            final_approved,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WireRole {
    Planner,
    Executor,
    Reviewer,
}

#[derive(Debug, Clone)]
struct CapturedRequest {
    role: WireRole,
    role_run: u8,
    initial: bool,
    body: serde_json::Value,
}

#[derive(Debug, Clone)]
struct ObservedTool {
    role: WireRole,
    role_run: u8,
    name: String,
    succeeded: bool,
    payload: Option<serde_json::Value>,
}

struct ProviderState {
    journey: Journey,
    calls: AtomicUsize,
    requests: Mutex<Vec<CapturedRequest>>,
    observations: Mutex<Vec<ObservedTool>>,
}

impl ProviderState {
    fn new(journey: Journey) -> Self {
        Self {
            journey,
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            observations: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<CapturedRequest> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn observations(&self) -> Vec<ObservedTool> {
        self.observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

struct LoopbackProvider {
    address: std::net::SocketAddr,
    state: Arc<ProviderState>,
    task: JoinHandle<()>,
}

impl LoopbackProvider {
    async fn spawn(journey: Journey) -> Self {
        let state = Arc::new(ProviderState::new(journey));
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind loopback-only scripted provider");
        let address = listener.local_addr().expect("scripted provider address");
        assert!(address.ip().is_loopback());
        let router = Router::new()
            .route("/v1/chat/completions", post(provider_request))
            .with_state(Arc::clone(&state));
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        Self {
            address,
            state,
            task,
        }
    }

    fn base_url(&self) -> String {
        assert!(self.address.ip().is_loopback());
        format!("http://{}", self.address)
    }

    async fn wait_for_calls(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(30), async {
            while self.state.calls.load(Ordering::Acquire) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("provider did not receive {expected} requests"));
    }
}

impl Drop for LoopbackProvider {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct CountingProviderFactory {
    client: ChatCompletionsClient,
    starts: AtomicUsize,
}

impl CountingProviderFactory {
    fn new(client: ChatCompletionsClient) -> Self {
        Self {
            client,
            starts: AtomicUsize::new(0),
        }
    }
}

impl TaskModelProviderFactory for CountingProviderFactory {
    fn start_task(&self) -> TaskModelSession {
        self.starts.fetch_add(1, Ordering::SeqCst);
        <ChatCompletionsClient as TaskModelProviderFactory>::start_task(&self.client)
    }
}

struct FailingRuntimeFactory;

#[async_trait::async_trait]
impl ProvisionedAgentRuntimeFactory for FailingRuntimeFactory {
    async fn create(
        &self,
        _worktree: &ProvisionedWorktree,
        _cancellation: CancellationToken,
    ) -> Result<TaskAgentRuntime, CodingAttemptError> {
        Err(CodingAttemptError::new("RUNTIME_SESSION_FAILED", true))
    }
}

async fn provider_request(
    State(state): State<Arc<ProviderState>>,
    request: Request<Body>,
) -> Response<Body> {
    assert_eq!(
        request
            .headers()
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer offline-multi-role-key")
    );
    let encoded = to_bytes(request.into_body(), 512 * 1024)
        .await
        .expect("read bounded provider request");
    let body: serde_json::Value =
        serde_json::from_slice(&encoded).expect("provider request is JSON");
    assert_eq!(body["model"], "offline-multi-role-model");
    assert_eq!(body["parallel_tool_calls"], false);
    assert_eq!(body["stream"], false);

    let role = request_role(&body);
    let role_run = request_role_run(&body, role);
    let initial = body["messages"]
        .as_array()
        .is_some_and(|messages| messages.len() == 2);
    record_latest_tool_observation(&state, &body, role, role_run);
    state
        .requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(CapturedRequest {
            role,
            role_run,
            initial,
            body: body.clone(),
        });
    let global_call = state.calls.fetch_add(1, Ordering::AcqRel);

    match state.journey {
        Journey::ProviderDisconnect if global_call == 0 => disconnected_response(),
        Journey::Blocking if global_call == 0 => {
            tokio::time::sleep(Duration::from_secs(30)).await;
            tool_response(
                "late-blocked",
                "report_blocked",
                serde_json::json!({
                    "reason": "missing_required_context",
                    "summary": "late scripted blocker"
                }),
            )
        }
        Journey::PlannerBlocked if role == WireRole::Planner => tool_response(
            "planner-blocked",
            "report_blocked",
            serde_json::json!({
                "reason": "missing_required_context",
                "summary": "the scripted fixture intentionally omits required context"
            }),
        ),
        Journey::BudgetExhaustion if role == WireRole::Planner => tool_batch_response(
            (0..20)
                .map(|index| {
                    (
                        format!("planner-budget-read-{index}"),
                        "read_file".to_owned(),
                        serde_json::json!({
                            "path": "src/lib.rs",
                            "start_line": 1,
                            "end_line": 8
                        }),
                    )
                })
                .collect(),
        ),
        _ => match role {
            WireRole::Planner => planner_response(),
            WireRole::Executor => executor_response(&body, role_run),
            WireRole::Reviewer => reviewer_response(state.journey, &body, role_run),
        },
    }
}

fn planner_response() -> Response<Body> {
    tool_response(
        "planner-submit",
        "submit_plan",
        serde_json::json!({
            "summary": "Change the answer in one isolated worktree and validate it",
            "steps": [{
                "title": "Implement and validate",
                "description": "Update the answer implementation and run its focused test.",
                "acceptance_criteria": ["The answer integration test passes."]
            }],
            "initial_required_checks": [{
                "kind": "cargo_test",
                "package": PACKAGE,
                "integration_test": INTEGRATION_TEST
            }]
        }),
    )
}

fn executor_response(body: &serde_json::Value, role_run: u8) -> Response<Body> {
    let history = tool_history(body);
    if !history.iter().any(|(_, name)| name == "read_file") {
        return tool_response(
            &format!("executor-{role_run}-read"),
            "read_file",
            serde_json::json!({
                "path": "src/lib.rs",
                "start_line": 1,
                "end_line": 12
            }),
        );
    }
    if !history.iter().any(|(_, name)| name == "replace_file") {
        let payload = latest_success_payload(body);
        let expected_sha256 = payload["sha256"]
            .as_str()
            .expect("read_file observation has sha256");
        return tool_response(
            &format!("executor-{role_run}-replace"),
            "replace_file",
            serde_json::json!({
                "path": "src/lib.rs",
                "expected_sha256": expected_sha256,
                "content": format!(
                    "pub fn answer() -> u32 {{ 42 }}\n// validated executor round {role_run}\n"
                )
            }),
        );
    }
    if !history.iter().any(|(_, name)| name == "cargo_test") {
        return tool_response(
            &format!("executor-{role_run}-test"),
            "cargo_test",
            validation_arguments(body, "cargo_test"),
        );
    }
    if role_run == 1
        && !history
            .iter()
            .any(|(_, name)| name == "update_plan_progress")
    {
        return tool_response(
            "executor-1-progress",
            "update_plan_progress",
            serde_json::json!({
                "updates": [{"step_id": "step-01", "status": "completed"}]
            }),
        );
    }
    tool_response(
        &format!("executor-{role_run}-submit"),
        "submit_execution",
        serde_json::json!({
            "summary": format!("executor round {role_run} completed the change and current test")
        }),
    )
}

fn reviewer_response(journey: Journey, body: &serde_json::Value, role_run: u8) -> Response<Body> {
    if journey == Journey::ReviewerMutation {
        return tool_response(
            "reviewer-illegal-mutation",
            "replace_file",
            serde_json::json!({
                "path": "src/lib.rs",
                "expected_sha256": "0".repeat(64),
                "content": "reviewer must never write\n"
            }),
        );
    }

    let (rework_rounds, final_approved) = match journey {
        Journey::Quality {
            rework_rounds,
            final_approved,
        } => (rework_rounds, final_approved),
        Journey::InvalidCoverage => (0, true),
        _ => (0, true),
    };
    let final_round = rework_rounds + 1;
    if role_run <= rework_rounds || (!final_approved && role_run == final_round) {
        return changes_requested_response(role_run);
    }

    let history = tool_history(body);
    if !history
        .iter()
        .any(|(_, name)| name == "review_diff_manifest")
    {
        let manifest_properties = function_properties(body, "review_diff_manifest");
        if manifest_properties
            .get("generation")
            .and_then(|property| property.get("const"))
            .is_none()
        {
            let read_count = history
                .iter()
                .filter(|(_, name)| name == "read_file")
                .count();
            if read_count < 3 {
                return tool_response(
                    &format!("reviewer-{role_run}-read-{read_count}"),
                    "read_file",
                    serde_json::json!({
                        "path": "src/lib.rs",
                        "start_line": 1,
                        "end_line": 12
                    }),
                );
            }
            return tool_batch_response(
                (0..8)
                    .map(|index| {
                        (
                            format!("reviewer-{role_run}-reserved-read-{index}"),
                            "read_file".to_owned(),
                            serde_json::json!({
                                "path": "src/lib.rs",
                                "start_line": 1,
                                "end_line": 12
                            }),
                        )
                    })
                    .collect(),
            );
        }
        let checkpoint = request_handoff(body)["checkpoint"].clone();
        return tool_response(
            &format!("reviewer-{role_run}-manifest"),
            "review_diff_manifest",
            serde_json::json!({
                "generation": checkpoint["generation"],
                "workspace_digest": checkpoint["workspace_digest"]
            }),
        );
    }

    if let Some(arguments) = required_chunk_arguments(body) {
        let arguments = if journey == Journey::InvalidCoverage {
            let mut invalid = arguments;
            let start = invalid["start_chunk"]
                .as_u64()
                .expect("required chunk start");
            invalid["start_chunk"] = serde_json::json!(start + 1);
            invalid
        } else {
            arguments
        };
        return tool_response(
            &format!("reviewer-{role_run}-chunks-{}", arguments["start_chunk"]),
            "review_diff_chunks",
            arguments,
        );
    }

    tool_response(
        &format!("reviewer-{role_run}-approved"),
        "submit_review",
        serde_json::json!({
            "verdict": "approved",
            "summary": format!("reviewer round {role_run} covered the complete current diff"),
            "findings": [],
            "add_required_checks": []
        }),
    )
}

fn changes_requested_response(role_run: u8) -> Response<Body> {
    tool_response(
        &format!("reviewer-{role_run}-changes"),
        "submit_review",
        serde_json::json!({
            "verdict": "changes_requested",
            "summary": format!("reviewer round {role_run} requests one bounded correction"),
            "findings": [{
                "severity": "blocking",
                "message": format!("refresh the round {role_run} implementation marker"),
                "path": "src/lib.rs",
                "line": 2
            }],
            "add_required_checks": []
        }),
    )
}

fn request_role(body: &serde_json::Value) -> WireRole {
    let policy = body["messages"][0]["content"]
        .as_str()
        .expect("role system policy");
    if policy.contains("Planner #1") {
        WireRole::Planner
    } else if policy.contains("Executor") {
        WireRole::Executor
    } else if policy.contains("Reviewer") {
        WireRole::Reviewer
    } else {
        panic!("unknown role policy: {policy}");
    }
}

fn request_role_run(body: &serde_json::Value, role: WireRole) -> u8 {
    if role == WireRole::Planner {
        return 1;
    }
    request_handoff(body)["role_run"]
        .as_u64()
        .and_then(|value| u8::try_from(value).ok())
        .expect("bounded role_run in canonical handoff")
}

fn request_handoff(body: &serde_json::Value) -> serde_json::Value {
    let content = body["messages"][1]["content"]
        .as_str()
        .expect("canonical role handoff");
    serde_json::from_str(content).expect("role handoff is canonical JSON")
}

fn tool_history(body: &serde_json::Value) -> Vec<(String, String)> {
    body["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .filter(|message| message["role"] == "assistant")
        .flat_map(|message| {
            message["tool_calls"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|call| {
                    (
                        call["id"].as_str().expect("tool id").to_owned(),
                        call["function"]["name"]
                            .as_str()
                            .expect("tool name")
                            .to_owned(),
                    )
                })
        })
        .collect()
}

fn validation_arguments(body: &serde_json::Value, name: &str) -> serde_json::Value {
    let properties = function_properties(body, name);
    let mut arguments = serde_json::Map::new();
    if let Some(check_id) = properties
        .get("check_id")
        .and_then(|property| property.get("const"))
    {
        arguments.insert("check_id".to_owned(), check_id.clone());
    }
    arguments.insert(
        "package".to_owned(),
        properties
            .get("package")
            .and_then(|property| property.get("const"))
            .cloned()
            .unwrap_or_else(|| serde_json::json!(PACKAGE)),
    );
    arguments.insert(
        "integration_test".to_owned(),
        properties
            .get("integration_test")
            .and_then(|property| property.get("const"))
            .cloned()
            .unwrap_or_else(|| serde_json::json!(INTEGRATION_TEST)),
    );
    serde_json::Value::Object(arguments)
}

fn required_chunk_arguments(body: &serde_json::Value) -> Option<serde_json::Value> {
    let properties = function_properties_optional(body, "review_diff_chunks")?;
    let start_chunk = properties.get("start_chunk")?.get("const")?.clone();
    Some(serde_json::json!({
        "generation": properties.get("generation")?.get("const")?.clone(),
        "workspace_digest": {
            "algorithm": properties
                .get("workspace_digest")?
                .get("properties")?
                .get("algorithm")?
                .get("const")?
                .clone(),
            "value": properties
                .get("workspace_digest")?
                .get("properties")?
                .get("value")?
                .get("const")?
                .clone()
        },
        "manifest_sha256": properties.get("manifest_sha256")?.get("const")?.clone(),
        "start_chunk": start_chunk,
        "count": properties.get("count")?.get("const")?.clone()
    }))
}

fn function_properties<'a>(
    body: &'a serde_json::Value,
    name: &str,
) -> &'a serde_json::Map<String, serde_json::Value> {
    function_properties_optional(body, name)
        .unwrap_or_else(|| panic!("request does not expose {name}"))
}

fn function_properties_optional<'a>(
    body: &'a serde_json::Value,
    name: &str,
) -> Option<&'a serde_json::Map<String, serde_json::Value>> {
    body["tools"]
        .as_array()?
        .iter()
        .find(|tool| tool["function"]["name"] == name)?
        .get("function")?
        .get("parameters")?
        .get("properties")?
        .as_object()
}

fn record_latest_tool_observation(
    state: &ProviderState,
    body: &serde_json::Value,
    role: WireRole,
    role_run: u8,
) {
    let Some(tool_message) = body["messages"].as_array().and_then(|messages| {
        messages
            .iter()
            .rev()
            .find(|message| message["role"] == "tool")
    }) else {
        return;
    };
    let id = tool_message["tool_call_id"]
        .as_str()
        .expect("tool result call id");
    let name = tool_history(body)
        .into_iter()
        .find_map(|(candidate_id, name)| (candidate_id == id).then_some(name))
        .expect("tool result references a transcript call");
    let content = tool_message["content"]
        .as_str()
        .expect("tool result content");
    let succeeded = content.starts_with("[tool_status=succeeded;");
    let payload = content
        .split_once('\n')
        .and_then(|(_, payload)| serde_json::from_str(payload).ok());
    let mut observations = state
        .observations
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if observations.last().is_some_and(|previous| {
        previous.role == role
            && previous.role_run == role_run
            && previous.name == name
            && previous.payload == payload
    }) {
        return;
    }
    observations.push(ObservedTool {
        role,
        role_run,
        name,
        succeeded,
        payload,
    });
}

fn latest_success_payload(body: &serde_json::Value) -> serde_json::Value {
    let content = body["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .rev()
        .find(|message| message["role"] == "tool")
        .and_then(|message| message["content"].as_str())
        .expect("latest tool result");
    assert!(
        content.starts_with("[tool_status=succeeded;"),
        "expected successful typed tool result, got {content}"
    );
    let (_, payload) = content.split_once('\n').expect("tool result envelope");
    serde_json::from_str(payload).expect("tool result JSON")
}

fn tool_response(id: &str, name: &str, arguments: serde_json::Value) -> Response<Body> {
    json_response(serde_json::json!({
        "id": format!("chatcmpl-{id}"),
        "object": "chat.completion",
        "created": 1,
        "model": "offline-multi-role-model",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": id,
                    "type": "function",
                    "function": {
                        "name": name,
                        "arguments": serde_json::to_string(&arguments).unwrap()
                    }
                }]
            },
            "finish_reason": "tool_calls"
        }]
    }))
}

fn tool_batch_response(calls: Vec<(String, String, serde_json::Value)>) -> Response<Body> {
    let calls = calls
        .into_iter()
        .enumerate()
        .map(|(index, (id, name, arguments))| {
            serde_json::json!({
                "index": index,
                "id": id,
                "type": "function",
                "function": {
                    "name": name,
                    "arguments": serde_json::to_string(&arguments).unwrap()
                }
            })
        })
        .collect::<Vec<_>>();
    json_response(serde_json::json!({
        "id": "chatcmpl-reviewer-batch",
        "object": "chat.completion",
        "created": 1,
        "model": "offline-multi-role-model",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": calls
            },
            "finish_reason": "tool_calls"
        }]
    }))
}

fn json_response(value: serde_json::Value) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&value).unwrap()))
        .unwrap()
}

fn disconnected_response() -> Response<Body> {
    let stream = futures_util::stream::iter([
        Ok::<Bytes, io::Error>(Bytes::from_static(b"{\"choices\":[")),
        Err(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "offline injected disconnect",
        )),
    ]);
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from_stream(stream))
        .unwrap()
}

struct E2eFixture {
    root: PathBuf,
    repository_path: PathBuf,
    artifact_root: PathBuf,
    base_commit: String,
    store: Store,
    repository: Repository,
    writer: StoreWriterHandle,
    dispatcher: EventDispatcherHandle,
    manager: TaskManagerHandle,
    service_state: ServiceStateController,
    provider: LoopbackProvider,
    providers: Arc<CountingProviderFactory>,
    _temp: TempDir,
}

impl E2eFixture {
    async fn new(journey: Journey) -> Self {
        Self::new_with_writer_faults(journey, None).await
    }

    async fn new_with_writer_faults(
        journey: Journey,
        controller: Option<Arc<StoreWriterTestController>>,
    ) -> Self {
        let temporary = tempfile::Builder::new()
            .prefix("multi-role-offline-e2e-")
            .tempdir()
            .expect("create offline E2E fixture");
        let root = temporary.path().canonicalize().expect("canonical E2E root");
        let repository_path = root.join("repository");
        let runtime_directory = root.join("runtime");
        let artifact_root = root.join("artifacts");
        for directory in [
            repository_path.join("src"),
            repository_path.join("tests"),
            runtime_directory.clone(),
            artifact_root.clone(),
        ] {
            std::fs::create_dir_all(directory).expect("create E2E directory");
        }
        seed_repository(&repository_path);
        let base_commit = git_line(&repository_path, &["rev-parse", "HEAD"]);

        let rustc = concrete_rustc();
        let git = path_executable(if cfg!(windows) { "git.exe" } else { "git" });
        let toolchain = discover_toolchain(
            &runtime_directory,
            Some(rustc.as_path()),
            Some(git.as_path()),
        )
        .await
        .expect("discover E2E toolchain");

        let store = Store::open(root.join("store.sqlite3"))
            .await
            .expect("open E2E store");
        store.migrate().await.expect("migrate E2E store");
        let repository = match store
            .register_repository(NewRepository {
                selected_path: canonical(&repository_path),
                display_name: "multi-role-offline-e2e".to_owned(),
                git_root: canonical(&repository_path),
                cargo_workspace_root: canonical(&repository_path),
            })
            .await
            .expect("register E2E repository")
        {
            RegisterRepositoryOutcome::Created(repository)
            | RegisterRepositoryOutcome::Existing(repository) => repository,
        };

        let provider = LoopbackProvider::spawn(journey).await;
        let providers = Arc::new(CountingProviderFactory::new(provider_client(
            &provider, journey,
        )));
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 1_024)
            .await
            .expect("spawn E2E dispatcher");
        let writer = if let Some(controller) = controller {
            StoreWriterHandle::spawn_with_test_controller(
                store.clone(),
                Arc::new(dispatcher.clone()),
                128,
                controller,
            )
        } else {
            StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 128)
        };
        let provisioners = provisioner_factory(
            toolchain.clone(),
            artifact_root.clone(),
            runtime_directory.clone(),
        );
        let runtimes: Arc<dyn ProvisionedAgentRuntimeFactory> =
            if journey == Journey::RuntimeFailure {
                Arc::new(FailingRuntimeFactory)
            } else {
                Arc::new(Project2RuntimeSessionFactory::project_2_defaults(
                    toolchain,
                    runtime_directory,
                ))
            };
        let attempts = Arc::new(WorktreeCodingAgentAttemptFactory::new(
            provisioners,
            runtimes,
        ));
        let runner = Arc::new(CodingAgentRunner::new(
            writer.clone(),
            providers.clone(),
            attempts,
            Arc::new(SystemWallClock),
            CodingAgentRunnerConfig::try_new(Duration::from_secs(10), Duration::from_millis(10))
                .expect("valid E2E runner config"),
        ));
        let service_state = ServiceStateController::new(ServiceState::Ready);
        let manager = TaskManagerHandle::spawn(
            store.clone(),
            writer.clone(),
            dispatcher.clone(),
            service_state.clone(),
            runner,
            1,
            128,
        );

        Self {
            root,
            repository_path,
            artifact_root,
            base_commit,
            store,
            repository,
            writer,
            dispatcher,
            manager,
            service_state,
            provider,
            providers,
            _temp: temporary,
        }
    }

    async fn enqueue(&self, prompt: &str) -> Task {
        let task = self
            .writer
            .create_task(
                support::new_task(self.repository.id, prompt),
                support::deadline(),
            )
            .await
            .expect("persist E2E task")
            .value
            .task()
            .clone();
        self.manager
            .notify_queued(task.id)
            .await
            .expect("notify E2E task");
        task
    }

    async fn wait_for_terminal(&self, task_id: TaskId) -> Task {
        tokio::time::timeout(Duration::from_secs(300), async {
            loop {
                let task = self
                    .store
                    .task_detail(task_id)
                    .await
                    .expect("read E2E task")
                    .expect("E2E task exists")
                    .task;
                if matches!(
                    task.status,
                    TaskStatus::Completed
                        | TaskStatus::Failed
                        | TaskStatus::Cancelled
                        | TaskStatus::Interrupted
                ) {
                    return task;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("task {task_id} did not become terminal"))
    }

    fn backend(&self, security: &support::SecurityFixture) -> Arc<ApplicationBackend> {
        Arc::new(ApplicationBackend::new(
            self.store.clone(),
            self.writer.clone(),
            self.dispatcher.clone(),
            self.manager.clone(),
            RepositoryDiscovery::new(self.root.clone()),
            None,
            security.manager.clone(),
            self.service_state.clone(),
            MutationGate::new(self.service_state.clone()),
            support::timestamp(),
            4,
            Duration::from_secs(2),
            Arc::new(|| {}),
        ))
    }

    async fn assert_single_isolated_attempt(&self, task_id: TaskId, expected_source: &str) {
        self.assert_single_isolated_attempt_with_provider_starts(task_id, expected_source, 1)
            .await;
    }

    async fn assert_single_isolated_attempt_with_provider_starts(
        &self,
        task_id: TaskId,
        expected_source: &str,
        expected_provider_starts: usize,
    ) {
        assert_eq!(
            std::fs::read_to_string(self.repository_path.join("src/lib.rs")).unwrap(),
            INITIAL_SOURCE,
            "the original checkout must remain unchanged"
        );
        let artifact = self
            .store
            .load_attempt_artifact(task_id)
            .await
            .expect("load attempt artifact")
            .expect("attempt artifact exists");
        assert_eq!(artifact.state, AttemptArtifactState::Ready);
        assert_eq!(artifact.identity.attempt, 1);
        assert_eq!(artifact.identity.repository_id, self.repository.id);
        assert_eq!(artifact.base_commit, self.base_commit);
        assert!(
            artifact
                .worktree_path
                .as_path()
                .starts_with(&self.artifact_root)
        );
        assert_eq!(
            std::fs::read_to_string(artifact.worktree_path.as_path().join("src/lib.rs")).unwrap(),
            expected_source
        );
        assert_eq!(
            self.providers.starts.load(Ordering::SeqCst),
            expected_provider_starts
        );
    }
}

#[tokio::test]
async fn quality_journeys_use_fresh_roles_and_keep_sqlite_rest_and_sse_identical() {
    let _guard = E2E_LOCK.lock().await;
    for (rework_rounds, final_approved) in [(0, true), (1, true), (2, true), (2, false)] {
        let fixture = E2eFixture::new(Journey::quality(rework_rounds, final_approved)).await;
        let mut live = fixture.dispatcher.subscribe();
        let task = fixture
            .enqueue("make answer return forty two and independently review the complete diff")
            .await;
        let terminal = fixture.wait_for_terminal(task.id).await;
        let expected_rounds = usize::from(rework_rounds) + 1;
        if final_approved {
            assert_eq!(
                terminal.status,
                TaskStatus::Completed,
                "reworks={rework_rounds}: failure={:?}; requests={:?}",
                terminal.failure,
                fixture
                    .provider
                    .state
                    .requests()
                    .iter()
                    .map(|request| (request.role, request.role_run, tool_history(&request.body)))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                terminal.delivery_readiness,
                DeliveryReadiness::ReviewApproved
            );
            assert!(terminal.failure.is_none());
        } else {
            assert_eq!(terminal.status, TaskStatus::Failed);
            assert_eq!(
                terminal.delivery_readiness,
                DeliveryReadiness::ReviewRejected
            );
            assert_eq!(
                terminal
                    .failure
                    .as_ref()
                    .map(|failure| failure.code.as_str()),
                Some("REVIEW_REJECTED")
            );
        }

        let detail = fixture.store.task_detail(task.id).await.unwrap().unwrap();
        assert_eq!(detail.reviews.len(), expected_rounds);
        assert_eq!(
            detail
                .activity
                .iter()
                .filter(|entry| entry.actor() == ActivityActor::Planner)
                .filter(|entry| entry.message().ends_with("started"))
                .count(),
            1
        );
        for (index, review) in detail.reviews.iter().enumerate() {
            assert_eq!(usize::from(review.round()), index + 1);
            let expected_verdict = if final_approved && index + 1 == expected_rounds {
                ReviewVerdict::Approved
            } else {
                ReviewVerdict::ChangesRequested
            };
            assert_eq!(review.verdict(), expected_verdict);
        }
        let final_review = detail.reviews.last().expect("final review");
        assert_eq!(
            final_review.workspace_generation(),
            detail.diff.as_ref().expect("terminal diff").revision
        );
        assert!(final_review.check_evidence().iter().all(|evidence| {
            evidence.workspace_generation() == final_review.workspace_generation()
                && evidence.workspace_digest() == final_review.workspace_digest()
        }));
        if final_approved {
            let coverage = final_review.coverage().expect("approved coverage");
            assert!(coverage.is_complete());
            assert_eq!(coverage.generation(), final_review.workspace_generation());
            assert_eq!(coverage.workspace_digest(), final_review.workspace_digest());
        } else {
            assert!(final_review.coverage().is_none());
        }

        let expected_source = format!(
            "pub fn answer() -> u32 {{ 42 }}\n// validated executor round {}\n",
            expected_rounds
        );
        fixture
            .assert_single_isolated_attempt(task.id, &expected_source)
            .await;

        let initial_requests = fixture
            .provider
            .state
            .requests()
            .into_iter()
            .filter(|request| request.initial)
            .collect::<Vec<_>>();
        let mut expected_roles = vec![(WireRole::Planner, 1)];
        for role_run in 1..=u8::try_from(expected_rounds).unwrap() {
            expected_roles.push((WireRole::Executor, role_run));
            expected_roles.push((WireRole::Reviewer, role_run));
        }
        assert_eq!(
            initial_requests
                .iter()
                .map(|request| (request.role, request.role_run))
                .collect::<Vec<_>>(),
            expected_roles
        );
        assert!(initial_requests.iter().all(|request| {
            request.body["messages"]
                .as_array()
                .is_some_and(|messages| messages.len() == 2)
        }));

        let observations = fixture.provider.state.observations();
        for required in ["read_file", "replace_file"] {
            assert!(
                observations
                    .iter()
                    .any(|observation| observation.name == required
                        && observation.succeeded
                        && observation.payload.is_some()),
                "missing typed {required} observation: {observations:?}"
            );
        }
        assert!(
            observations
                .iter()
                .any(|observation| observation.name == "cargo_test" && observation.succeeded),
            "missing successful Cargo validation observation: {observations:?}"
        );
        assert!(
            final_review
                .check_evidence()
                .iter()
                .all(|evidence| evidence.status() == CheckEvidenceStatus::Passed),
            "review evidence must retain typed passed checks"
        );
        if final_approved {
            for required in ["review_diff_manifest", "review_diff_chunks"] {
                assert!(
                    observations
                        .iter()
                        .any(|observation| observation.name == required
                            && observation.succeeded
                            && observation.payload.is_some()),
                    "missing typed {required} observation"
                );
            }
        }

        let page = fixture
            .store
            .task_events_after(task.id, EventCursor::ZERO, usize::MAX)
            .await
            .unwrap();
        fixture
            .dispatcher
            .flush_to(page.high_watermark)
            .await
            .unwrap();
        let mut live_events = Vec::new();
        while let Ok(event) = live.try_recv() {
            if event.task_id == task.id {
                live_events.push(event);
            }
        }
        assert_eq!(live_events, page.events);

        let security = support::SecurityFixture::production();
        let backend = fixture.backend(&security);
        let rest = backend
            .task_detail(
                &AuthContext {
                    session_id: "offline-authorized".to_owned(),
                },
                task.id,
            )
            .await
            .unwrap();
        let through = backend.latest_event_id().await.unwrap();
        let sse = backend
            .events_between(0, through, usize::MAX)
            .await
            .unwrap()
            .into_iter()
            .filter(|event| serde_json::to_value(event).unwrap()["task_id"] == task.id.to_string())
            .collect::<Vec<_>>();
        let sqlite_dtos = page
            .events
            .iter()
            .cloned()
            .map(TaskEventDto::from)
            .collect::<Vec<_>>();
        assert_eq!(
            sse.iter()
                .map(|event| serde_json::to_value(event).unwrap())
                .collect::<Vec<_>>(),
            sqlite_dtos
                .iter()
                .map(|event| serde_json::to_value(event).unwrap())
                .collect::<Vec<_>>()
        );
        let rest_json = serde_json::to_value(rest).unwrap();
        let rest_reviews = rest_json["reviews"].as_array().unwrap();
        let sse_reviews = sse
            .iter()
            .filter_map(|event| {
                let value = serde_json::to_value(event).unwrap();
                (value["kind"] == "review.updated").then(|| value["payload"]["review"].clone())
            })
            .collect::<Vec<_>>();
        assert_eq!(rest_reviews, &sse_reviews);
        assert_eq!(
            page.events.last().map(|event| event.payload.kind()),
            Some(if final_approved {
                TaskEventKind::TaskCompleted
            } else {
                TaskEventKind::TaskFailed
            })
        );
    }
}

#[tokio::test]
async fn loopback_network_guard_rejects_every_non_loopback_provider_origin() {
    let _guard = E2E_LOCK.lock().await;
    for forbidden in [
        "https://api.openai.com",
        "http://example.com:4317",
        "http://192.168.1.8:4317",
        "http://localhost:4317",
    ] {
        assert!(
            offline_provider_config(forbidden).is_none(),
            "offline E2E network guard accepted {forbidden}"
        );
    }
    for allowed in ["http://127.0.0.1:4317", "http://[::1]:4317"] {
        offline_provider_config(allowed)
            .expect("IP-literal loopback is the only offline provider origin");
    }
}

#[tokio::test]
async fn runtime_factory_and_role_budget_failures_are_unreviewed_and_fail_closed() {
    let _guard = E2E_LOCK.lock().await;
    for (journey, expected_code, retryable, expected_provider_starts) in [
        (Journey::RuntimeFailure, "RUNTIME_SESSION_FAILED", true, 0),
        (
            Journey::BudgetExhaustion,
            "PLANNER_STEP_LIMIT_REACHED",
            false,
            1,
        ),
    ] {
        let fixture = E2eFixture::new(journey).await;
        let task = fixture
            .enqueue("exercise a bounded runtime or task budget failure")
            .await;
        let terminal = fixture.wait_for_terminal(task.id).await;
        assert_eq!(terminal.status, TaskStatus::Failed, "journey={journey:?}");
        assert_eq!(
            terminal.delivery_readiness,
            DeliveryReadiness::Unreviewed,
            "journey={journey:?}"
        );
        let failure = terminal.failure.as_ref().expect("stable failure");
        assert_eq!(failure.code, expected_code, "journey={journey:?}");
        assert_eq!(failure.retryable, retryable, "journey={journey:?}");
        let detail = fixture.store.task_detail(task.id).await.unwrap().unwrap();
        assert!(detail.reviews.is_empty(), "journey={journey:?}");
        assert!(
            !detail
                .activity
                .iter()
                .any(|entry| entry.actor() == ActivityActor::Reviewer),
            "failure before review cannot manufacture Reviewer activity"
        );
        fixture
            .assert_single_isolated_attempt_with_provider_starts(
                task.id,
                INITIAL_SOURCE,
                expected_provider_starts,
            )
            .await;
    }
}

#[tokio::test]
async fn blocked_provider_mutation_and_coverage_fail_closed_without_approval() {
    let _guard = E2E_LOCK.lock().await;
    for (journey, expected_code, retryable) in [
        (
            Journey::PlannerBlocked,
            "PLANNER_BLOCKED_MISSING_CONTEXT",
            true,
        ),
        (Journey::ProviderDisconnect, "PLANNER_PROVIDER_FAILED", true),
        (
            Journey::ReviewerMutation,
            "REVIEWER_ACTION_NOT_ALLOWED",
            false,
        ),
        (
            Journey::InvalidCoverage,
            "REVIEWER_ACTION_NOT_ALLOWED",
            false,
        ),
    ] {
        let fixture = E2eFixture::new(journey).await;
        let task = fixture
            .enqueue("exercise a fail-closed role boundary")
            .await;
        let terminal = fixture.wait_for_terminal(task.id).await;
        assert_eq!(terminal.status, TaskStatus::Failed, "journey={journey:?}");
        assert_eq!(
            terminal.delivery_readiness,
            DeliveryReadiness::Unreviewed,
            "journey={journey:?}"
        );
        let failure = terminal.failure.expect("stable failure");
        assert_eq!(failure.code, expected_code, "journey={journey:?}");
        assert_eq!(failure.retryable, retryable, "journey={journey:?}");
        assert!(!failure.message.contains(API_KEY));
        let detail = fixture.store.task_detail(task.id).await.unwrap().unwrap();
        assert!(
            !detail
                .reviews
                .iter()
                .any(|review| review.verdict() == ReviewVerdict::Approved),
            "journey={journey:?}"
        );
        fixture
            .assert_single_isolated_attempt(
                task.id,
                if matches!(
                    journey,
                    Journey::ReviewerMutation | Journey::InvalidCoverage
                ) {
                    "pub fn answer() -> u32 { 42 }\n// validated executor round 1\n"
                } else {
                    INITIAL_SOURCE
                },
            )
            .await;
    }
}

#[tokio::test]
async fn intermediate_review_store_failure_recovers_as_interrupted_without_false_approval() {
    let _guard = E2E_LOCK.lock().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailBeforeExecute,
            operation: Some(StoreWriterOperationKind::RecordReview),
            count: 1,
        }])
        .unwrap(),
    );
    let fixture = E2eFixture::new_with_writer_faults(
        Journey::quality(1, true),
        Some(Arc::clone(&controller)),
    )
    .await;
    let mut recoveries = fixture.manager.subscribe_degraded_recovery();
    let task = fixture
        .enqueue("persist the first review through the degraded recovery path")
        .await;

    let recovery = tokio::time::timeout(Duration::from_secs(120), recoveries.recv())
        .await
        .expect("degraded recovery completes")
        .expect("degraded recovery channel remains open");
    assert_eq!(recovery.replayed_pending_count, 1);
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::FailBeforeExecute,
            StoreWriterOperationKind::RecordReview,
        ),
        1
    );
    let interrupted = fixture.wait_for_terminal(task.id).await;
    assert_eq!(interrupted.status, TaskStatus::Interrupted);
    assert_eq!(
        interrupted.delivery_readiness,
        DeliveryReadiness::Unreviewed
    );
    assert_eq!(
        interrupted
            .failure
            .as_ref()
            .map(|failure| failure.code.as_str()),
        Some("STORE_WRITE_FAILED")
    );
    let detail = fixture.store.task_detail(task.id).await.unwrap().unwrap();
    assert_eq!(detail.reviews.len(), 1);
    assert_eq!(detail.reviews[0].verdict(), ReviewVerdict::ChangesRequested);
    assert!(
        !detail
            .reviews
            .iter()
            .any(|review| review.verdict() == ReviewVerdict::Approved)
    );
    fixture
        .assert_single_isolated_attempt(
            task.id,
            "pub fn answer() -> u32 { 42 }\n// validated executor round 1\n",
        )
        .await;
}

#[tokio::test]
async fn cancellation_and_reopened_sqlite_recovery_are_durable_during_provider_wait() {
    let _guard = E2E_LOCK.lock().await;

    let cancelled_fixture = E2eFixture::new(Journey::Blocking).await;
    let cancelled = cancelled_fixture
        .enqueue("cancel the blocked planner")
        .await;
    cancelled_fixture.provider.wait_for_calls(1).await;
    cancelled_fixture
        .manager
        .cancel(cancelled.id)
        .await
        .unwrap();
    let cancelled_terminal = cancelled_fixture.wait_for_terminal(cancelled.id).await;
    assert_eq!(cancelled_terminal.status, TaskStatus::Cancelled);
    assert_eq!(
        cancelled_terminal.delivery_readiness,
        DeliveryReadiness::Unreviewed
    );
    cancelled_fixture
        .assert_single_isolated_attempt(cancelled.id, INITIAL_SOURCE)
        .await;

    let restart_fixture = E2eFixture::new(Journey::Blocking).await;
    let restart = restart_fixture
        .enqueue("recover the blocked planner after restart")
        .await;
    restart_fixture.provider.wait_for_calls(1).await;
    let reopened = Store::open(restart_fixture.root.join("store.sqlite3"))
        .await
        .expect("reopen SQLite");
    reopened.migrate().await.expect("migrate reopened SQLite");
    let recovery = reopened
        .recover_incomplete(
            UtcTimestamp::new(SystemWallClock.now_utc()).unwrap(),
            coding_agent_domain::TaskFailure {
                code: "APP_RESTARTED".to_owned(),
                message: "task was interrupted because the application restarted".to_owned(),
                retryable: true,
            },
        )
        .await
        .expect("recover abandoned task");
    assert_eq!(recovery.interrupted_count, 1);

    let retired = restart_fixture
        .manager
        .quiesce_and_interrupt(Instant::now() + Duration::from_secs(10))
        .await
        .expect("retire old manager");
    let handles = match retired {
        coding_agent_app::QuiesceResult::Durable { active, .. } => active,
        coding_agent_app::QuiesceResult::Frozen { error, .. } => {
            panic!("old manager failed to retire: {error}")
        }
    };
    for handle in handles {
        handle.cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(10), handle.done)
            .await
            .expect("old runner stops")
            .expect("old runner signal");
    }
    let interrupted = reopened
        .task_detail(restart.id)
        .await
        .unwrap()
        .unwrap()
        .task;
    assert_eq!(interrupted.status, TaskStatus::Interrupted);
    assert_eq!(
        interrupted.delivery_readiness,
        DeliveryReadiness::Unreviewed
    );
    assert_eq!(
        interrupted
            .failure
            .as_ref()
            .map(|failure| failure.code.as_str()),
        Some("APP_RESTARTED")
    );
    let events = reopened
        .task_events_after(restart.id, EventCursor::ZERO, usize::MAX)
        .await
        .unwrap()
        .events;
    assert_eq!(
        events.last().map(|event| event.payload.kind()),
        Some(TaskEventKind::TaskInterrupted)
    );
    restart_fixture
        .assert_single_isolated_attempt(restart.id, INITIAL_SOURCE)
        .await;
}

fn provider_client(provider: &LoopbackProvider, journey: Journey) -> ChatCompletionsClient {
    let config = offline_provider_config(&provider.base_url())
        .expect("allow explicit IP-literal loopback HTTP in E2E");
    let timeout = if journey == Journey::Blocking {
        Duration::from_secs(20)
    } else {
        Duration::from_secs(10)
    };
    ChatCompletionsClient::new(
        config,
        ClientLimits::try_new(
            Duration::from_secs(2),
            timeout,
            512 * 1024,
            64 * 1024,
            8 * 1024 * 1024,
        )
        .expect("valid E2E provider limits"),
    )
    .expect("construct E2E provider client")
}

fn offline_provider_config(base_url: &str) -> Option<ProviderConfig> {
    let config =
        ProviderConfig::from_json_allow_loopback_http_for_test(&provider_config_json(base_url))
            .ok()?;
    let host = config
        .base_url()
        .host_str()?
        .trim_start_matches('[')
        .trim_end_matches(']');
    let address = host.parse::<std::net::IpAddr>().ok()?;
    address.is_loopback().then_some(config)
}

fn provider_config_json(base_url: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "base_url": base_url,
        "model": "offline-multi-role-model",
        "api_key": API_KEY,
        "tool_choice_compatibility": "strict"
    }))
    .unwrap()
}

fn provisioner_factory(
    toolchain: ToolchainPaths,
    artifact_root: PathBuf,
    runtime_directory: PathBuf,
) -> Arc<dyn RepositoryWorktreeProvisionerFactory> {
    Arc::new(
        move |repository: &Repository| -> Result<Arc<WorktreeProvisioner>, CodingAttemptError> {
            WorktreeProvisioner::from_trusted_paths(
                &toolchain,
                repository.git_root.as_path(),
                repository.cargo_workspace_root.as_path(),
                &artifact_root,
                &runtime_directory,
                ProcessLimits::try_new(
                    512 * 1024,
                    256 * 1024,
                    Duration::from_secs(2 * 60),
                    Duration::from_secs(5),
                )
                .expect("valid E2E process limits"),
                WorktreeLimits::try_new(Duration::from_secs(30))
                    .expect("valid E2E worktree limits"),
            )
            .map(Arc::new)
            .map_err(|error| CodingAttemptError::new(error.code(), false))
        },
    )
}

fn seed_repository(repository: &Path) {
    git_ok(repository, &["init", "--quiet"]);
    git_ok(repository, &["config", "user.name", "Offline E2E"]);
    git_ok(
        repository,
        &["config", "user.email", "offline-e2e@example.invalid"],
    );
    std::fs::write(
        repository.join("Cargo.toml"),
        b"[workspace]\n\n[package]\nname = \"offline_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .unwrap();
    std::fs::write(
        repository.join("Cargo.lock"),
        b"# This file is automatically @generated by Cargo.\n# It is not intended for manual editing.\nversion = 4\n\n[[package]]\nname = \"offline_fixture\"\nversion = \"0.1.0\"\n",
    )
    .unwrap();
    std::fs::write(repository.join(".gitignore"), b"/target/\n").unwrap();
    std::fs::write(repository.join("src/lib.rs"), INITIAL_SOURCE).unwrap();
    std::fs::write(
        repository.join("tests/answer.rs"),
        b"#[test]\nfn answer_is_forty_two() { assert_eq!(offline_fixture::answer(), 42); }\n",
    )
    .unwrap();
    git_ok(repository, &["add", "--all"]);
    git_ok(
        repository,
        &["commit", "--quiet", "--no-gpg-sign", "-m", "base"],
    );
}

fn canonical(path: &Path) -> CanonicalPath {
    CanonicalPath::try_from_canonical(path.canonicalize().expect("canonical fixture path"))
        .expect("domain canonical path")
}

fn git_ok(repository: &Path, arguments: &[&str]) {
    let output = git_output(repository, arguments);
    assert!(
        output.status.success(),
        "git {} failed: {}",
        arguments.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git_line(repository: &Path, arguments: &[&str]) -> String {
    let output = git_output(repository, arguments);
    assert!(
        output.status.success(),
        "git {} failed",
        arguments.join(" ")
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn git_output(repository: &Path, arguments: &[&str]) -> Output {
    support::command_output(
        Command::new(path_executable(if cfg!(windows) {
            "git.exe"
        } else {
            "git"
        }))
        .arg("-C")
        .arg(repository)
        .args(arguments),
    )
    .expect("run fixture Git")
}

fn concrete_rustc() -> PathBuf {
    let output = support::command_output(Command::new("rustc").args(["--print", "sysroot"]))
        .expect("query Rust sysroot");
    assert!(output.status.success());
    PathBuf::from(String::from_utf8(output.stdout).unwrap().trim())
        .join("bin")
        .join(if cfg!(windows) { "rustc.exe" } else { "rustc" })
        .canonicalize()
        .expect("canonical rustc")
}

fn path_executable(name: &str) -> PathBuf {
    let candidate = std::env::split_paths(&std::env::var_os("PATH").expect("host PATH"))
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .unwrap_or_else(|| panic!("{name} missing from PATH"))
        .canonicalize()
        .expect("canonical executable");
    #[cfg(windows)]
    if name.eq_ignore_ascii_case("git.exe")
        && candidate.parent().is_some_and(|parent| {
            parent
                .file_name()
                .is_some_and(|name| name.eq_ignore_ascii_case("cmd"))
        })
    {
        let installation = candidate.parent().unwrap().parent().unwrap();
        for architecture in ["mingw64", "mingw32"] {
            let concrete = installation.join(architecture).join("bin/git.exe");
            if concrete.is_file() {
                return concrete.canonicalize().expect("canonical concrete Git");
            }
        }
    }
    candidate
}
