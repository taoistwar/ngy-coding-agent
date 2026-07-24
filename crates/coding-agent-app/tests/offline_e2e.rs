mod support;

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_stream::stream;
use axum::Router;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::State;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{Request, Response, StatusCode};
use axum::routing::post;
use coding_agent_api::{
    LiveEventItem, LiveEventStream, ServiceStateControl, ServiceStateDto, ServiceStateStream,
    SseBackend, TaskEventDto,
};
use coding_agent_app::{
    CodingAgentRunner, CodingAgentRunnerConfig, CodingAttemptError, EventDispatcherHandle,
    Project2RuntimeSessionFactory, QuiesceResult, RepositoryWorktreeProvisionerFactory,
    ServiceState, ServiceStateController, StoreWriterHandle, SystemWallClock, TaskManagerHandle,
    WallClock, WorktreeCodingAgentAttemptFactory,
};
use coding_agent_domain::{
    CanonicalPath, EventCursor, NewRepository, Repository, Task, TaskEventKind, TaskFailure,
    TaskId, TaskStatus, TestStatus, UtcTimestamp,
};
use coding_agent_provider::{ChatCompletionsClient, ClientLimits, ProviderConfig};
use coding_agent_runtime::{
    ProcessLimits, ToolchainPaths, WorktreeLimits, WorktreeProvisioner, discover_toolchain,
};
use coding_agent_store::{AttemptArtifactState, RegisterRepositoryOutcome, Store};
use futures_util::stream as futures_stream;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio::time::Instant;

const API_KEY: &str = "offline-provider-key";
const PACKAGE: &str = "offline_fixture";
const CHANGED_SOURCE: &str = "pub fn answer() -> u32 { 42 }\n";
const POST_PASS_SOURCE: &str = "pub fn answer() -> u32 { 43 }\n";
const FAILED_SOURCE: &str = "pub fn answer() -> u32 { 0 }\n";

static E2E_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Success,
    RequiredAsRequiredSuccess,
    RequiredAsAutoSuccess,
    TestFailure,
    ReplaceAfterPass,
    PathEscape,
    Disconnect,
    Timeout,
    OutputFlood,
    Blocking,
}

struct ProviderState {
    scenario: Scenario,
    calls: AtomicUsize,
    requests: Mutex<Vec<serde_json::Value>>,
}

impl ProviderState {
    fn new(scenario: Scenario) -> Self {
        Self {
            scenario,
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<serde_json::Value> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

struct MockProvider {
    address: std::net::SocketAddr,
    state: Arc<ProviderState>,
    task: JoinHandle<()>,
}

impl MockProvider {
    async fn spawn(scenario: Scenario) -> Self {
        let state = Arc::new(ProviderState::new(scenario));
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind offline provider");
        let address = listener.local_addr().expect("offline provider address");
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

impl Drop for MockProvider {
    fn drop(&mut self) {
        self.task.abort();
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
        Some("Bearer offline-provider-key")
    );
    let encoded = to_bytes(request.into_body(), 512 * 1024)
        .await
        .expect("read bounded provider request");
    let body: serde_json::Value =
        serde_json::from_slice(&encoded).expect("provider request is JSON");
    assert_eq!(body["model"], "offline-model");
    assert_eq!(body["parallel_tool_calls"], false);
    assert_eq!(body["stream"], false);
    state
        .requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(body.clone());
    let call = state.calls.fetch_add(1, Ordering::AcqRel);

    match state.scenario {
        Scenario::Disconnect if call == 0 => disconnected_response(),
        Scenario::Timeout if call == 0 => {
            tokio::time::sleep(Duration::from_secs(30)).await;
            final_response("late")
        }
        Scenario::Blocking if call == 0 => {
            tokio::time::sleep(Duration::from_secs(30)).await;
            final_response("late")
        }
        Scenario::OutputFlood if call == 0 => output_flood_response(),
        _ => match request_role(&body) {
            WireRole::Planner => project_3_plan_response(),
            WireRole::Executor => {
                project_3_executor_response(state.scenario, &body, request_role_run(&body))
            }
            WireRole::Reviewer => project_3_reviewer_response(&body, request_role_run(&body)),
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WireRole {
    Planner,
    Executor,
    Reviewer,
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

fn request_role_run(body: &serde_json::Value) -> u8 {
    if request_role(body) == WireRole::Planner {
        return 1;
    }
    request_handoff(body)["role_run"]
        .as_u64()
        .and_then(|value| u8::try_from(value).ok())
        .expect("bounded role run")
}

fn request_handoff(body: &serde_json::Value) -> serde_json::Value {
    serde_json::from_str(
        body["messages"][1]["content"]
            .as_str()
            .expect("canonical role handoff"),
    )
    .expect("role handoff JSON")
}

fn project_3_plan_response() -> Response<Body> {
    tool_response(
        "planner-submit",
        "submit_plan",
        serde_json::json!({
            "summary": "Change and validate the offline answer",
            "steps": [{
                "title": "Implement and validate",
                "description": "Update the answer implementation and run its focused test.",
                "acceptance_criteria": ["The answer integration test passes."]
            }],
            "initial_required_checks": [{
                "kind": "cargo_test",
                "package": PACKAGE,
                "integration_test": "answer"
            }]
        }),
    )
}

fn project_3_executor_response(
    scenario: Scenario,
    body: &serde_json::Value,
    role_run: u8,
) -> Response<Body> {
    let history = tool_history(body);
    let compatibility_scenario = matches!(
        scenario,
        Scenario::RequiredAsRequiredSuccess | Scenario::RequiredAsAutoSuccess
    );
    if scenario == Scenario::PathEscape {
        if history.is_empty() {
            return tool_response(
                "executor-read-escape",
                "read_file",
                serde_json::json!({
                    "path": "../outside.txt",
                    "start_line": 1,
                    "end_line": 10
                }),
            );
        }
        assert_latest_tool_status(body, "failed");
        return tool_response(
            "executor-path-blocked",
            "report_blocked",
            serde_json::json!({
                "reason": "unsupported_scope",
                "summary": "the requested path is outside the isolated worktree"
            }),
        );
    }

    if !history.iter().any(|(_, name)| name == "read_file") {
        return tool_response(
            &format!("executor-{role_run}-read"),
            "read_file",
            serde_json::json!({
                "path": "src/lib.rs",
                "start_line": 1,
                "end_line": 20
            }),
        );
    }

    let replace_count = history
        .iter()
        .filter(|(_, name)| name == "replace_file")
        .count();
    let test_count = history
        .iter()
        .filter(|(_, name)| name == "cargo_test")
        .count();
    if replace_count == 0 {
        let result = latest_tool_payload(body);
        let digest = result["sha256"]
            .as_str()
            .expect("read_file result carries SHA-256");
        let content = if scenario == Scenario::TestFailure {
            FAILED_SOURCE
        } else {
            CHANGED_SOURCE
        };
        return tool_response(
            "executor-replace-source",
            "replace_file",
            serde_json::json!({
                "path": "src/lib.rs",
                "expected_sha256": digest,
                "content": content
            }),
        );
    }

    if compatibility_scenario
        && !history
            .iter()
            .any(|(_, name)| name == "update_plan_progress")
    {
        return tool_response(
            "executor-progress",
            "update_plan_progress",
            serde_json::json!({
                "updates": [{"step_id": "step-01", "status": "completed"}]
            }),
        );
    }

    if compatibility_scenario && test_count < replace_count {
        if request_exposes_only(body, "cargo_test") {
            return tool_response(
                &format!("executor-test-{test_count}"),
                "cargo_test",
                validation_arguments(body),
            );
        }
        let read_count = history
            .iter()
            .filter(|(_, name)| name == "read_file")
            .count();
        return tool_response(
            &format!("executor-compat-read-{read_count}"),
            "read_file",
            serde_json::json!({
                "path": "src/lib.rs",
                "start_line": 1,
                "end_line": 20
            }),
        );
    }

    if test_count < replace_count {
        return tool_response(
            &format!("executor-test-{test_count}"),
            "cargo_test",
            validation_arguments(body),
        );
    }

    if scenario == Scenario::TestFailure {
        assert_latest_tool_status(body, "failed");
        return tool_response(
            "executor-test-blocked",
            "report_blocked",
            serde_json::json!({
                "reason": "unsupported_scope",
                "summary": "the focused validation failed"
            }),
        );
    }

    if scenario == Scenario::ReplaceAfterPass && replace_count == 1 {
        assert_latest_tool_status(body, "succeeded");
        let digest = latest_successful_sha256_for_tool(body, "replace_file")
            .expect("post-pass replacement hash");
        return tool_response(
            "executor-replace-after-pass",
            "replace_file",
            serde_json::json!({
                "path": "src/lib.rs",
                "expected_sha256": digest,
                "content": POST_PASS_SOURCE
            }),
        );
    }

    if scenario == Scenario::ReplaceAfterPass && replace_count == 2 {
        assert_latest_tool_status(body, "failed");
        return tool_response(
            "executor-stale-test-blocked",
            "report_blocked",
            serde_json::json!({
                "reason": "unsupported_scope",
                "summary": "the post-pass mutation invalidated the required test"
            }),
        );
    }

    if role_run == 1
        && !history
            .iter()
            .any(|(_, name)| name == "update_plan_progress")
    {
        return tool_response(
            "executor-progress",
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
            "summary": "the offline change and current required test are complete"
        }),
    )
}

fn project_3_reviewer_response(body: &serde_json::Value, role_run: u8) -> Response<Body> {
    let history = tool_history(body);
    if !history
        .iter()
        .any(|(_, name)| name == "review_diff_manifest")
    {
        let manifest = function_properties(body, "review_diff_manifest");
        if manifest
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
                        "end_line": 20
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
                                "end_line": 20
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
            "summary": "the current validation and complete diff are approved",
            "findings": [],
            "add_required_checks": []
        }),
    )
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

fn validation_arguments(body: &serde_json::Value) -> serde_json::Value {
    let properties = function_properties(body, "cargo_test");
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
            .unwrap_or_else(|| serde_json::json!("answer")),
    );
    serde_json::Value::Object(arguments)
}

fn required_chunk_arguments(body: &serde_json::Value) -> Option<serde_json::Value> {
    let properties = function_properties_optional(body, "review_diff_chunks")?;
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
        "start_chunk": properties.get("start_chunk")?.get("const")?.clone(),
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

fn request_exposes_only(body: &serde_json::Value, name: &str) -> bool {
    body["tools"].as_array().is_some_and(|tools| {
        tools.len() == 1 && tools[0]["function"]["name"].as_str() == Some(name)
    })
}

fn latest_successful_sha256_for_tool(body: &serde_json::Value, name: &str) -> Option<String> {
    let history = tool_history(body);
    let call_id = history
        .iter()
        .rev()
        .find_map(|(id, candidate)| (candidate == name).then_some(id))?;
    body["messages"]
        .as_array()?
        .iter()
        .rev()
        .find(|message| message["role"] == "tool" && message["tool_call_id"] == call_id.as_str())
        .and_then(|message| message["content"].as_str())
        .and_then(|content| content.split_once('\n'))
        .and_then(|(_, payload)| serde_json::from_str::<serde_json::Value>(payload).ok())
        .and_then(|payload| payload["sha256"].as_str().map(str::to_owned))
}

fn latest_tool_content(request: &serde_json::Value) -> &str {
    request["messages"]
        .as_array()
        .expect("provider messages array")
        .iter()
        .rev()
        .find(|message| message["role"] == "tool")
        .and_then(|message| message["content"].as_str())
        .expect("latest tool result")
}

fn assert_latest_tool_status(request: &serde_json::Value, expected: &str) {
    let content = latest_tool_content(request);
    assert!(
        content.starts_with(&format!("[tool_status={expected};")),
        "expected a {expected} tool result, got: {content}"
    );
}

fn latest_tool_payload(request: &serde_json::Value) -> serde_json::Value {
    let content = latest_tool_content(request);
    assert!(
        content.starts_with("[tool_status=succeeded;"),
        "expected a successful tool result, got: {content}"
    );
    let (_, payload) = content.split_once('\n').expect("tool result envelope");
    serde_json::from_str(payload).expect("tool result JSON")
}

fn tool_response(id: &str, name: &str, arguments: serde_json::Value) -> Response<Body> {
    json_response(serde_json::json!({
        "id": format!("chatcmpl-{id}"),
        "object": "chat.completion",
        "created": 1,
        "model": "offline-model",
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
        "id": "chatcmpl-batch",
        "object": "chat.completion",
        "created": 1,
        "model": "offline-model",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "Applying the change and validating it.",
                "tool_calls": calls
            },
            "finish_reason": "tool_calls"
        }]
    }))
}

fn final_response(content: &str) -> Response<Body> {
    json_response(serde_json::json!({
        "id": "chatcmpl-final",
        "object": "chat.completion",
        "created": 1,
        "model": "offline-model",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": content},
            "finish_reason": "stop"
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
    let stream = futures_stream::iter([
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

fn output_flood_response() -> Response<Body> {
    let chunks = (0..128).map(|_| Ok::<Bytes, Infallible>(Bytes::from(vec![b'x'; 1024])));
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from_stream(futures_stream::iter(chunks)))
        .unwrap()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OriginalSnapshot {
    status: Vec<u8>,
    staged_blob: Vec<u8>,
    files: BTreeMap<PathBuf, Vec<u8>>,
}

impl OriginalSnapshot {
    fn capture(repository: &Path) -> Self {
        let files = ["src/lib.rs", "staged.txt", "unstaged.txt", "untracked.txt"]
            .into_iter()
            .map(|path| {
                let path = PathBuf::from(path);
                let bytes = std::fs::read(repository.join(&path)).expect("capture original file");
                (path, bytes)
            })
            .collect();
        Self {
            status: git_output(repository, &["status", "--porcelain=v1", "-z"]).stdout,
            staged_blob: git_output(repository, &["show", ":staged.txt"]).stdout,
            files,
        }
    }
}

struct E2eFixture {
    root: PathBuf,
    repository_path: PathBuf,
    runtime_directory: PathBuf,
    artifact_root: PathBuf,
    protected_toolchain_paths: Vec<PathBuf>,
    original: OriginalSnapshot,
    base_commit: String,
    outside_bytes: Vec<u8>,
    store: Store,
    repository: Repository,
    writer: StoreWriterHandle,
    dispatcher: EventDispatcherHandle,
    manager: TaskManagerHandle,
    provider: MockProvider,
    _temp: TempDir,
}

impl E2eFixture {
    async fn new(scenario: Scenario) -> Self {
        let temporary = tempfile::Builder::new()
            .prefix("offline-e2e-")
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
        dirty_original_repository(&repository_path);
        let original = OriginalSnapshot::capture(&repository_path);
        let outside = root.join("outside.txt");
        let outside_bytes = b"outside sentinel must survive\n".to_vec();
        std::fs::write(&outside, &outside_bytes).expect("write outside sentinel");

        let rustc = concrete_rustc();
        let git = path_executable(if cfg!(windows) { "git.exe" } else { "git" });
        let toolchain = discover_toolchain(
            &runtime_directory,
            Some(rustc.as_path()),
            Some(git.as_path()),
        )
        .await
        .expect("discover E2E toolchain");
        let mut protected_toolchain_paths = toolchain.search_directories().to_vec();
        protected_toolchain_paths.push(toolchain.cargo_home().to_owned());

        let database_path = root.join("store.sqlite3");
        let store = Store::open(database_path).await.expect("open E2E store");
        store.migrate().await.expect("migrate E2E store");
        let repository = match store
            .register_repository(NewRepository {
                selected_path: canonical(&repository_path),
                display_name: "offline-e2e".to_owned(),
                git_root: canonical(&repository_path),
                cargo_workspace_root: canonical(&repository_path),
            })
            .await
            .expect("register E2E repository")
        {
            RegisterRepositoryOutcome::Created(repository)
            | RegisterRepositoryOutcome::Existing(repository) => repository,
        };

        let provider = MockProvider::spawn(scenario).await;
        let provider_client = Arc::new(provider_client(&provider, scenario));
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 1_024)
            .await
            .expect("spawn E2E dispatcher");
        let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 128);
        let provisioners = provisioner_factory(
            toolchain.clone(),
            artifact_root.clone(),
            runtime_directory.clone(),
        );
        let runtimes = Arc::new(Project2RuntimeSessionFactory::project_2_defaults(
            toolchain,
            runtime_directory.clone(),
        ));
        let attempts = Arc::new(WorktreeCodingAgentAttemptFactory::new(
            provisioners,
            runtimes,
        ));
        let runner = Arc::new(CodingAgentRunner::new(
            writer.clone(),
            provider_client,
            attempts,
            Arc::new(SystemWallClock),
            CodingAgentRunnerConfig::try_new(Duration::from_secs(10), Duration::from_millis(10))
                .expect("valid E2E runner config"),
        ));
        let manager = TaskManagerHandle::spawn(
            store.clone(),
            writer.clone(),
            dispatcher.clone(),
            ServiceStateController::new(ServiceState::Ready),
            runner,
            1,
            128,
        );

        Self {
            root,
            repository_path,
            runtime_directory,
            artifact_root,
            protected_toolchain_paths,
            original,
            base_commit,
            outside_bytes,
            store,
            repository,
            writer,
            dispatcher,
            manager,
            provider,
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

    async fn wait_for_status(&self, task_id: TaskId, expected: TaskStatus) -> Task {
        tokio::time::timeout(Duration::from_secs(180), async {
            loop {
                let detail = self
                    .store
                    .task_detail(task_id)
                    .await
                    .expect("read E2E task")
                    .expect("E2E task exists");
                let task = detail.task;
                if task.status == expected {
                    return task;
                }
                if matches!(
                    task.status,
                    TaskStatus::Completed
                        | TaskStatus::Failed
                        | TaskStatus::Cancelled
                        | TaskStatus::Interrupted
                ) {
                    panic!(
                        "task reached {:?}, expected {expected:?}: {:?}; diff={:?}; tests={:?}",
                        task.status, task.failure, detail.diff, detail.tests
                    );
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("task {task_id} did not reach {expected:?}"))
    }

    async fn assert_isolation_invariants(&self, task_id: TaskId) {
        assert_eq!(
            OriginalSnapshot::capture(&self.repository_path),
            self.original,
            "isolated attempt changed the dirty original repository"
        );
        assert_eq!(
            std::fs::read(self.root.join("outside.txt")).expect("read outside sentinel"),
            self.outside_bytes
        );
        let artifact = self
            .store
            .load_attempt_artifact(task_id)
            .await
            .expect("load protected E2E artifact")
            .expect("E2E attempt artifact exists");
        let target_directory = artifact.worktree_path.as_path().join("target");
        let mut protected_paths = vec![
            self.root.clone(),
            self.repository_path.clone(),
            self.artifact_root.clone(),
            self.runtime_directory.clone(),
            artifact.worktree_path.as_path().to_owned(),
            target_directory,
        ];
        protected_paths.extend(self.protected_toolchain_paths.iter().cloned());
        let protected_paths = protected_paths
            .iter()
            .map(PathBuf::as_path)
            .collect::<Vec<_>>();
        for request in self.provider.state.requests() {
            assert_json_strings_hide_paths(&request, &protected_paths);
        }
    }
}

fn assert_json_strings_hide_paths(value: &serde_json::Value, protected_paths: &[&Path]) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                assert_json_strings_hide_paths(value, protected_paths);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                assert_json_strings_hide_paths(value, protected_paths);
            }
        }
        serde_json::Value::String(value) => {
            let value = normalized_path_text(value);
            for protected in protected_paths {
                let protected = normalized_path_text(&protected.to_string_lossy());
                assert!(
                    !value.contains(&protected),
                    "provider request exposed a protected absolute path"
                );
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
}

fn normalized_path_text(value: &str) -> String {
    let value = value.replace('\\', "/");
    #[cfg(windows)]
    return value.to_ascii_lowercase();
    #[cfg(not(windows))]
    value
}

fn provider_client(provider: &MockProvider, scenario: Scenario) -> ChatCompletionsClient {
    let tool_choice_compatibility = match scenario {
        Scenario::RequiredAsRequiredSuccess => "required_as_required",
        Scenario::RequiredAsAutoSuccess => "required_as_auto",
        _ => "strict",
    };
    let encoded = serde_json::to_vec(&serde_json::json!({
        "base_url": provider.base_url(),
        "model": "offline-model",
        "api_key": API_KEY,
        "tool_choice_compatibility": tool_choice_compatibility,
    }))
    .unwrap();
    let config = ProviderConfig::from_json_allow_loopback_http_for_test(&encoded)
        .expect("allow explicit loopback HTTP in E2E");
    let timeout = if scenario == Scenario::Timeout {
        Duration::from_millis(150)
    } else {
        Duration::from_secs(20)
    };
    ChatCompletionsClient::new(
        config,
        ClientLimits::try_new(
            Duration::from_secs(2),
            timeout,
            512 * 1024,
            32 * 1024,
            4 * 1024 * 1024,
        )
        .expect("valid E2E provider limits"),
    )
    .expect("construct E2E provider client")
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

struct StorePersistedEventReplay {
    store: Store,
    dispatcher: EventDispatcherHandle,
}

#[async_trait::async_trait]
impl SseBackend for StorePersistedEventReplay {
    fn subscribe_live(&self) -> LiveEventStream {
        let mut receiver = self.dispatcher.subscribe();
        Box::pin(stream! {
            loop {
                match receiver.recv().await {
                    Ok(event) => yield LiveEventItem::Event(event.into()),
                    Err(broadcast::error::RecvError::Lagged(_)) => yield LiveEventItem::Lagged,
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        })
    }

    fn subscribe_service_state(&self) -> ServiceStateStream {
        Box::pin(futures_stream::empty())
    }

    async fn current_service_state(&self) -> coding_agent_api::ApiResult<ServiceStateControl> {
        Ok(ServiceStateControl::new(ServiceStateDto::Ready, 0))
    }

    async fn latest_event_id(&self) -> coding_agent_api::ApiResult<i64> {
        Ok(self.store.latest_event_id().await.unwrap().get())
    }

    async fn events_between(
        &self,
        after: i64,
        through: i64,
        limit: usize,
    ) -> coding_agent_api::ApiResult<Vec<TaskEventDto>> {
        let after = EventCursor::new(after).unwrap();
        Ok(self
            .store
            .events_after(after, limit)
            .await
            .unwrap()
            .events
            .into_iter()
            .filter(|event| event.id.get() <= through)
            .map(Into::into)
            .collect())
    }
}

#[tokio::test]
async fn offline_success_runs_real_pipeline_and_persisted_event_dto_replay() {
    let _guard = E2E_LOCK.lock().await;
    let fixture = E2eFixture::new(Scenario::Success).await;
    let mut live = fixture.dispatcher.subscribe();
    let task = fixture
        .enqueue("make answer return forty two and verify it")
        .await;
    let completed = fixture
        .wait_for_status(task.id, TaskStatus::Completed)
        .await;
    assert!(completed.failure.is_none());

    let detail = fixture.store.task_detail(task.id).await.unwrap().unwrap();
    let diff = detail.diff.as_ref().expect("completed task diff");
    let tests = detail.tests.as_ref().expect("completed task tests");
    assert_eq!(diff.revision, 1);
    assert_eq!(tests.revision, diff.revision);
    assert_eq!(tests.status, TestStatus::Passed);
    assert_eq!(diff.files.len(), 1);
    assert_eq!(diff.files[0].path, "src/lib.rs");
    assert!(!diff.files[0].truncated);
    assert!(
        diff.files[0]
            .patch
            .contains("+pub fn answer() -> u32 { 42 }")
    );
    assert!(diff.files[0].patch.len() < 16 * 1024);

    let artifact = fixture
        .store
        .load_attempt_artifact(task.id)
        .await
        .unwrap()
        .expect("completed task artifact");
    assert_eq!(artifact.state, AttemptArtifactState::Ready);
    assert_eq!(artifact.identity.repository_id, fixture.repository.id);
    assert_eq!(artifact.identity.attempt, 1);
    assert_eq!(artifact.base_commit, fixture.base_commit);
    assert_eq!(
        artifact.branch_name,
        format!("codex/task-{}-attempt-1", task.id)
    );
    assert!(
        artifact
            .worktree_path
            .as_path()
            .starts_with(&fixture.artifact_root)
    );
    assert_eq!(
        std::fs::read_to_string(artifact.worktree_path.as_path().join("src/lib.rs")).unwrap(),
        CHANGED_SOURCE
    );
    assert_eq!(
        git_line(
            &fixture.repository_path,
            &["rev-parse", &format!("refs/heads/{}", artifact.branch_name)]
        ),
        fixture.base_commit,
        "worktree edits must not move the reserved branch without a commit"
    );
    fixture.assert_isolation_invariants(task.id).await;

    let page = fixture
        .store
        .task_events_after(task.id, EventCursor::ZERO, usize::MAX)
        .await
        .unwrap();
    fixture
        .dispatcher
        .flush_to(page.high_watermark)
        .await
        .expect("flush dispatcher to persisted cursor");
    let mut live_events = Vec::new();
    while let Ok(event) = live.try_recv() {
        if event.task_id == task.id {
            live_events.push(event);
        }
    }
    assert_eq!(
        live_events, page.events,
        "dispatcher replay diverged from SQLite"
    );
    assert_eq!(
        page.events.last().map(|event| event.payload.kind()),
        Some(TaskEventKind::TaskCompleted)
    );

    let replay = StorePersistedEventReplay {
        store: fixture.store.clone(),
        dispatcher: fixture.dispatcher.clone(),
    };
    let through = replay.latest_event_id().await.unwrap();
    let replayed_events = replay.events_between(0, through, usize::MAX).await.unwrap();
    let task_replay = replayed_events
        .into_iter()
        .filter(|event| serde_json::to_value(event).unwrap()["task_id"] == task.id.to_string())
        .collect::<Vec<_>>();
    let persisted_dto = page
        .events
        .iter()
        .cloned()
        .map(TaskEventDto::from)
        .collect::<Vec<_>>();
    assert_eq!(
        task_replay
            .iter()
            .map(|event| serde_json::to_value(event).unwrap())
            .collect::<Vec<_>>(),
        persisted_dto
            .iter()
            .map(|event| serde_json::to_value(event).unwrap())
            .collect::<Vec<_>>(),
        "persisted event DTO replay diverged from the task event stream"
    );
    assert_eq!(
        fixture.provider.state.calls.load(Ordering::Acquire),
        13,
        "Planner, Executor, and Reviewer must all use the same task-scoped provider ledger"
    );
}

#[tokio::test]
async fn compatibility_modes_force_one_visible_cargo_test_without_an_http_retry() {
    let _guard = E2E_LOCK.lock().await;
    for scenario in [
        Scenario::RequiredAsRequiredSuccess,
        Scenario::RequiredAsAutoSuccess,
    ] {
        let fixture = E2eFixture::new(scenario).await;
        let task = fixture
            .enqueue("change the answer and validate through the compatibility mode")
            .await;

        let completed = fixture
            .wait_for_status(task.id, TaskStatus::Completed)
            .await;
        assert!(completed.failure.is_none(), "scenario={scenario:?}");
        let detail = fixture.store.task_detail(task.id).await.unwrap().unwrap();
        assert_eq!(
            detail.diff.as_ref().expect("completed task diff").revision,
            1
        );
        assert_eq!(
            detail.tests.as_ref().expect("completed task tests").status,
            TestStatus::Passed
        );

        let requests = fixture.provider.state.requests();
        let forced_validation = requests
            .iter()
            .find(|request| {
                request["tools"].as_array().is_some_and(|tools| {
                    tools.len() == 1 && tools[0]["function"]["name"] == "cargo_test"
                })
            })
            .expect("one forced current Cargo test request");
        assert_eq!(
            forced_validation["tool_choice"],
            match scenario {
                Scenario::RequiredAsRequiredSuccess => serde_json::json!("required"),
                Scenario::RequiredAsAutoSuccess => serde_json::json!("auto"),
                _ => unreachable!(),
            }
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request_exposes_only(request, "cargo_test"))
                .count(),
            1,
            "compatibility mode must issue exactly one forced validation request; scenario={scenario:?}"
        );
        assert_eq!(
            requests.len(),
            28,
            "compatibility mode must not add an HTTP retry to the budget-forced role journey; scenario={scenario:?}"
        );
        assert_eq!(fixture.provider.state.calls.load(Ordering::Acquire), 28);
        fixture.assert_isolation_invariants(task.id).await;
    }
}

#[tokio::test]
async fn real_test_failure_replace_after_pass_and_path_escape_fail_closed() {
    let _guard = E2E_LOCK.lock().await;
    for (scenario, expected_revision) in [
        (Scenario::TestFailure, 1),
        (Scenario::ReplaceAfterPass, 2),
        (Scenario::PathEscape, 0),
    ] {
        let fixture = E2eFixture::new(scenario).await;
        let task = fixture.enqueue("exercise a fail-closed coding path").await;
        let failed = fixture.wait_for_status(task.id, TaskStatus::Failed).await;
        let failure = failed.failure.expect("failed task has failure");
        assert_eq!(
            failure.code, "EXECUTOR_BLOCKED_UNSUPPORTED_SCOPE",
            "scenario={scenario:?}"
        );
        assert!(!failure.retryable, "scenario={scenario:?}");
        let detail = fixture.store.task_detail(task.id).await.unwrap().unwrap();
        assert_eq!(
            detail.diff.as_ref().map_or(0, |diff| diff.revision),
            expected_revision,
            "scenario={scenario:?}"
        );
        match scenario {
            Scenario::TestFailure => {
                assert_eq!(detail.tests.unwrap().status, TestStatus::Failed);
            }
            Scenario::ReplaceAfterPass => {
                assert_eq!(detail.tests.unwrap().status, TestStatus::Failed);
                let requests = fixture.provider.state.requests();
                assert!(
                    requests.iter().any(|request| {
                        let history = tool_history(request);
                        history
                            .iter()
                            .any(|(id, _)| id == "executor-replace-after-pass")
                            && history
                                .iter()
                                .filter(|(_, name)| name == "cargo_test")
                                .count()
                                >= 2
                    }),
                    "post-pass mutation must be followed by a new failed validation"
                );
                assert_eq!(
                    std::fs::read_to_string(
                        fixture
                            .store
                            .load_attempt_artifact(task.id)
                            .await
                            .unwrap()
                            .unwrap()
                            .worktree_path
                            .as_path()
                            .join("src/lib.rs")
                    )
                    .unwrap(),
                    POST_PASS_SOURCE
                );
            }
            Scenario::PathEscape => {
                assert_ne!(
                    detail
                        .tests
                        .expect("cleanup publishes unverified tests")
                        .status,
                    TestStatus::Passed
                );
                let requests = fixture.provider.state.requests();
                assert!(latest_tool_content(&requests[2]).contains("COMMAND_NOT_ALLOWED"));
            }
            _ => unreachable!(),
        }
        fixture.assert_isolation_invariants(task.id).await;
    }
}

#[tokio::test]
async fn disconnect_timeout_and_output_flood_have_stable_bounded_failures() {
    let _guard = E2E_LOCK.lock().await;
    for (scenario, expected, retryable) in [
        (Scenario::Disconnect, "PLANNER_PROVIDER_FAILED", true),
        (Scenario::Timeout, "PLANNER_PROVIDER_FAILED", true),
        (
            Scenario::OutputFlood,
            "PLANNER_CONTEXT_LIMIT_REACHED",
            false,
        ),
    ] {
        let fixture = E2eFixture::new(scenario).await;
        let task = fixture
            .enqueue("exercise a provider transport failure")
            .await;
        let failed = fixture.wait_for_status(task.id, TaskStatus::Failed).await;
        let failure = failed.failure.expect("provider failure is persisted");
        assert_eq!(failure.code, expected, "scenario={scenario:?}");
        assert_eq!(failure.retryable, retryable, "scenario={scenario:?}");
        assert!(!failure.message.contains(API_KEY));
        assert!(fixture.provider.state.calls.load(Ordering::Acquire) <= 1);
        let artifact = fixture
            .store
            .load_attempt_artifact(task.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(artifact.state, AttemptArtifactState::Ready);
        fixture.assert_isolation_invariants(task.id).await;
    }
}

#[tokio::test]
async fn live_cancellation_and_reopened_store_restart_recovery_are_durable() {
    let _guard = E2E_LOCK.lock().await;

    let cancelled_fixture = E2eFixture::new(Scenario::Blocking).await;
    let cancelled_task = cancelled_fixture.enqueue("wait for cancellation").await;
    cancelled_fixture
        .wait_for_status(cancelled_task.id, TaskStatus::Running)
        .await;
    cancelled_fixture.provider.wait_for_calls(1).await;
    cancelled_fixture
        .manager
        .cancel(cancelled_task.id)
        .await
        .expect("cancel running E2E task");
    cancelled_fixture
        .wait_for_status(cancelled_task.id, TaskStatus::Cancelled)
        .await;
    cancelled_fixture
        .assert_isolation_invariants(cancelled_task.id)
        .await;

    let restart_fixture = E2eFixture::new(Scenario::Blocking).await;
    let restart_task = restart_fixture
        .enqueue("simulate restart interruption")
        .await;
    restart_fixture
        .wait_for_status(restart_task.id, TaskStatus::Running)
        .await;
    restart_fixture.provider.wait_for_calls(1).await;

    // Simulate the next process reopening SQLite before the abandoned runner
    // has had a chance to publish a terminal outcome. This is the same durable
    // recovery transaction used by primary startup, not orderly APP_SHUTDOWN
    // quiescing.
    let restarted_store = Store::open(restart_fixture.root.join("store.sqlite3"))
        .await
        .expect("reopen store after simulated restart");
    restarted_store
        .migrate()
        .await
        .expect("migrate reopened store");
    let restarted_at = UtcTimestamp::new(SystemWallClock.now_utc()).unwrap();
    let recovery = restarted_store
        .recover_incomplete(
            restarted_at,
            TaskFailure {
                code: "APP_RESTARTED".to_owned(),
                message: "task was interrupted because the application restarted".to_owned(),
                retryable: true,
            },
        )
        .await
        .expect("recover abandoned running task");
    assert_eq!(recovery.interrupted_count, 1);

    // Retire the simulated old process only after the new store has durably
    // recovered its task. Its later cancellation must not overwrite the
    // APP_RESTARTED terminal state.
    let quiesce = restart_fixture
        .manager
        .quiesce_and_interrupt(Instant::now() + Duration::from_secs(10))
        .await
        .expect("retire simulated pre-restart manager");
    let active = match quiesce {
        QuiesceResult::Durable { active, .. } => active,
        QuiesceResult::Frozen { error, .. } => panic!("old manager could not retire: {error}"),
    };
    for handle in active {
        handle.cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(10), handle.done)
            .await
            .expect("interrupted runner stopped")
            .expect("interrupted runner completion signal");
    }
    let interrupted = restarted_store
        .task_detail(restart_task.id)
        .await
        .unwrap()
        .unwrap()
        .task;
    assert_eq!(interrupted.status, TaskStatus::Interrupted);
    assert_eq!(
        interrupted.failure.expect("interruption failure").code,
        "APP_RESTARTED"
    );
    let events = restarted_store
        .task_events_after(restart_task.id, EventCursor::ZERO, usize::MAX)
        .await
        .unwrap()
        .events;
    assert_eq!(
        events.last().map(|event| event.payload.kind()),
        Some(TaskEventKind::TaskInterrupted)
    );
    restart_fixture
        .assert_isolation_invariants(restart_task.id)
        .await;
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
    std::fs::write(
        repository.join("src/lib.rs"),
        b"pub fn answer() -> u32 { 41 }\n",
    )
    .unwrap();
    std::fs::write(
        repository.join("tests/answer.rs"),
        b"#[test]\nfn answer_is_forty_two() { assert_eq!(offline_fixture::answer(), 42); }\n",
    )
    .unwrap();
    std::fs::write(repository.join("staged.txt"), b"committed staged bytes\n").unwrap();
    std::fs::write(
        repository.join("unstaged.txt"),
        b"committed unstaged bytes\n",
    )
    .unwrap();
    git_ok(repository, &["add", "--all"]);
    git_ok(
        repository,
        &["commit", "--quiet", "--no-gpg-sign", "-m", "base"],
    );
}

fn dirty_original_repository(repository: &Path) {
    std::fs::write(repository.join("staged.txt"), b"dirty staged bytes\n").unwrap();
    git_ok(repository, &["add", "--", "staged.txt"]);
    std::fs::write(repository.join("unstaged.txt"), b"dirty unstaged bytes\n").unwrap();
    std::fs::write(repository.join("untracked.txt"), b"dirty untracked bytes\n").unwrap();
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
