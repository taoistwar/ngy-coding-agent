use std::convert::Infallible;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::State;
use axum::http::header::{
    ACCEPT, ACCEPT_ENCODING, AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE, LOCATION,
};
use axum::http::{HeaderMap, Request, Response, StatusCode};
use axum::routing::{any, post};
use coding_agent_core::{
    ActionRequest, AllowedActions, ModelMessage, ModelProvider, ModelRequest, ModelResponse,
    ModelToolChoice, PreparedModelProvider, ProviderError, Role, RoleLoopError,
    RuntimeActionRequest, ToolCall, ToolCallBatch, ToolRequest,
};
use coding_agent_provider::{
    ChatCompletionsClient, ChatCompletionsProvider, ClientLimits, ClientLimitsError,
    PROVIDER_CANCELLED, PROVIDER_RATE_LIMITED, PROVIDER_REDIRECT_REJECTED,
    PROVIDER_REQUEST_BYTE_LIMIT_REACHED, PROVIDER_RESPONSE_FINISH_UNSUPPORTED,
    PROVIDER_RESPONSE_INVALID, PROVIDER_RESPONSE_ROLE_ACTION_NOT_ALLOWED,
    PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID, PROVIDER_RESPONSE_TOOL_CHOICE_VIOLATED,
    PROVIDER_TASK_BYTE_LIMIT_REACHED, PROVIDER_TRANSPORT_FAILED, PROVIDER_UNAUTHORIZED,
    PROVIDER_UNAVAILABLE, ProviderConfig, encode_chat_completions_request,
};
use futures_util::{StreamExt as _, stream};
use tokio::net::TcpListener;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const API_KEY: &str = "known-provider-secret";

fn request_fixture() -> ModelRequest {
    ModelRequest {
        messages: vec![
            ModelMessage::system("bounded policy"),
            ModelMessage::user("known-private-prompt"),
        ],
        allowed_actions: AllowedActions::legacy(),
        tool_choice: ModelToolChoice::Auto,
    }
}

fn executor_request_fixture() -> ModelRequest {
    ModelRequest {
        messages: vec![
            ModelMessage::system("executor policy"),
            ModelMessage::user("bounded executor input"),
        ],
        allowed_actions: AllowedActions::for_role(Role::Executor),
        tool_choice: ModelToolChoice::Auto,
    }
}

fn final_response(content: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "id": "chatcmpl-local",
        "object": "chat.completion",
        "created": 1,
        "model": "coding-model",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": content},
            "finish_reason": "stop"
        }]
    }))
    .unwrap()
}

fn role_tool_response(name: &str, arguments: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "role-call",
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
    .unwrap()
}

fn cargo_test_response() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "forced-test",
                    "type": "function",
                    "function": {
                        "name": "cargo_test",
                        "arguments": "{\"timeout_ms\":120000}"
                    }
                }]
            },
            "finish_reason": "tool_calls"
        }]
    }))
    .unwrap()
}

fn test_limits() -> ClientLimits {
    ClientLimits::try_new(
        Duration::from_secs(1),
        Duration::from_secs(2),
        256 * 1024,
        64 * 1024,
        1024 * 1024,
    )
    .unwrap()
}

fn provider(server: &MockServer, limits: ClientLimits) -> ChatCompletionsProvider {
    client(server, limits).start_task()
}

fn client(server: &MockServer, limits: ClientLimits) -> ChatCompletionsClient {
    client_with_options(server, limits, None, None)
}

fn client_with_thinking(
    server: &MockServer,
    limits: ClientLimits,
    thinking: Option<&str>,
) -> ChatCompletionsClient {
    client_with_options(server, limits, thinking, None)
}

fn client_with_options(
    server: &MockServer,
    limits: ClientLimits,
    thinking: Option<&str>,
    tool_choice_compatibility: Option<&str>,
) -> ChatCompletionsClient {
    let base_url = server.base_url();
    let parsed = url::Url::parse(&base_url).unwrap();
    assert_eq!(parsed.host_str(), Some("127.0.0.1"));
    let mut config = serde_json::json!({
        "base_url": base_url,
        "model": "coding-model",
        "api_key": API_KEY,
    });
    if let Some(thinking) = thinking {
        config["thinking"] = serde_json::Value::String(thinking.to_owned());
    }
    if let Some(tool_choice_compatibility) = tool_choice_compatibility {
        config["tool_choice_compatibility"] =
            serde_json::Value::String(tool_choice_compatibility.to_owned());
    }
    let encoded = serde_json::to_vec(&config).unwrap();
    let config = ProviderConfig::from_json_allow_loopback_http_for_test(&encoded).unwrap();
    ChatCompletionsClient::new(config, limits).expect("construct local provider client")
}

#[test]
fn production_https_rustls_client_constructs_without_contacting_a_real_provider() {
    let encoded = serde_json::to_vec(&serde_json::json!({
        "base_url": "https://provider.invalid",
        "model": "coding-model",
        "api_key": API_KEY,
    }))
    .unwrap();
    let config = ProviderConfig::from_json(&encoded).expect("production HTTPS config");

    let client = ChatCompletionsClient::new(config, test_limits())
        .expect("rustls-backed client constructs without native TLS");

    assert_eq!(client.limits(), test_limits());
    assert!(!format!("{client:?}").contains(API_KEY));
}

struct MockServer {
    address: std::net::SocketAddr,
    task: JoinHandle<()>,
}

impl MockServer {
    async fn spawn(router: Router) -> Self {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind local mock provider");
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        Self { address, task }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.address)
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct CapturedRequest {
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Clone)]
struct CaptureFixedState {
    sender: mpsc::UnboundedSender<CapturedRequest>,
    response_body: Bytes,
}

async fn capture_fixed_response(
    State(state): State<CaptureFixedState>,
    request: Request<Body>,
) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let body = to_bytes(body, 256 * 1024).await.unwrap();
    state
        .sender
        .send(CapturedRequest {
            headers: parts.headers,
            body,
        })
        .unwrap();
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(state.response_body))
        .unwrap()
}

async fn capture_success(
    State(sender): State<mpsc::UnboundedSender<CapturedRequest>>,
    request: Request<Body>,
) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let body = to_bytes(body, 256 * 1024).await.unwrap();
    sender
        .send(CapturedRequest {
            headers: parts.headers,
            body,
        })
        .unwrap();
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .header("x-request-id", "request-123")
        .body(Body::from(final_response("done")))
        .unwrap()
}

#[tokio::test]
async fn prepared_exchange_exposes_only_exact_lengths_and_sends_the_prepared_bytes_once() {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let server = MockServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(capture_success))
            .with_state(sender),
    )
    .await;
    let task_provider = provider(&server, test_limits());
    let role_view: Arc<dyn PreparedModelProvider> = Arc::new(task_provider.clone());
    let request = request_fixture();
    let expected_request = encode_chat_completions_request("coding-model", &request).unwrap();

    {
        let rejected_by_core_budget = task_provider.prepare(request.clone()).unwrap();
        assert_eq!(
            rejected_by_core_budget.encoded_len(),
            expected_request.len()
        );
        assert_eq!(
            rejected_by_core_budget.maximum_response_bytes(),
            test_limits().max_response_bytes()
        );
    }
    assert_eq!(task_provider.task_provider_bytes(), 0);
    assert!(
        receiver.try_recv().is_err(),
        "dropping a prepared request after core budget rejection must not contact the provider"
    );

    let prepared = role_view.prepare(request).unwrap();
    let raw = prepared.send(CancellationToken::new()).await.unwrap();
    let expected_response = final_response("done");
    assert_eq!(raw.encoded_len(), expected_response.len());
    let captured = receiver
        .recv()
        .await
        .expect("one prepared request was sent");
    assert_eq!(captured.body.as_ref(), expected_request.as_slice());
    assert_eq!(
        task_provider.task_provider_bytes(),
        expected_request.len() + expected_response.len()
    );
    assert_eq!(
        raw.decode().unwrap(),
        ModelResponse::Final {
            content: "done".to_owned()
        }
    );
}

#[tokio::test]
async fn cancellation_after_preflight_charges_the_prepared_request_without_provider_contact() {
    let hits = Arc::new(AtomicUsize::new(0));
    let server = MockServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(counted_success))
            .with_state(hits.clone()),
    )
    .await;
    let provider = provider(&server, test_limits());
    let request = request_fixture();
    let request_bytes = encode_chat_completions_request("coding-model", &request)
        .unwrap()
        .len();
    let prepared = provider.prepare(request).unwrap();
    let cancellation = CancellationToken::new();
    cancellation.cancel();

    let error = match prepared.send(cancellation).await {
        Ok(_) => panic!("a cancelled prepared request must not return a response"),
        Err(error) => error,
    };
    assert_eq!(error.code, PROVIDER_CANCELLED);
    assert_eq!(provider.task_provider_bytes(), request_bytes);
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(hits.load(Ordering::SeqCst), 0);
}

async fn fixed_final_success() -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(final_response("done")))
        .unwrap()
}

async fn fixed_cargo_test_success() -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(cargo_test_response()))
        .unwrap()
}

async fn fixed_body_success(State(body): State<Bytes>) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap()
}

async fn complete_with_body(
    body: Vec<u8>,
    tool_choice: ModelToolChoice,
) -> Result<ModelResponse, ProviderError> {
    let server = MockServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(fixed_body_success))
            .with_state(Bytes::from(body)),
    )
    .await;
    let mut request = request_fixture();
    request.tool_choice = tool_choice;
    provider(&server, test_limits())
        .complete(request, CancellationToken::new())
        .await
}

async fn decode_prepared_executor_response(body: Vec<u8>) -> Result<ModelResponse, ProviderError> {
    let server = MockServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(fixed_body_success))
            .with_state(Bytes::from(body)),
    )
    .await;
    let task_provider = provider(&server, test_limits());
    let prepared = task_provider.prepare(executor_request_fixture())?;
    let raw = prepared.send(CancellationToken::new()).await?;
    raw.decode()
}

#[tokio::test]
async fn prepared_executor_decode_preserves_role_failure_classification_for_core() {
    for (body, provider_code, executor_code) in [
        (
            final_response("ordinary final"),
            PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID,
            "EXECUTOR_INVALID_OUTPUT",
        ),
        (
            serde_json::to_vec(&serde_json::json!({
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": null,
                        "tool_calls": []
                    },
                    "finish_reason": "tool_calls"
                }]
            }))
            .unwrap(),
            PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID,
            "EXECUTOR_INVALID_OUTPUT",
        ),
        (
            role_tool_response("submit_execution", serde_json::json!({})),
            PROVIDER_RESPONSE_ROLE_OUTPUT_INVALID,
            "EXECUTOR_INVALID_OUTPUT",
        ),
        (
            role_tool_response("submit_plan", serde_json::json!({})),
            PROVIDER_RESPONSE_ROLE_ACTION_NOT_ALLOWED,
            "EXECUTOR_ACTION_NOT_ALLOWED",
        ),
        (
            role_tool_response("read_file", serde_json::json!({})),
            PROVIDER_RESPONSE_ROLE_ACTION_NOT_ALLOWED,
            "EXECUTOR_ACTION_NOT_ALLOWED",
        ),
        (
            b"{malformed".to_vec(),
            PROVIDER_RESPONSE_INVALID,
            "EXECUTOR_PROVIDER_FAILED",
        ),
    ] {
        let error = decode_prepared_executor_response(body).await.unwrap_err();
        assert_eq!(error.code, provider_code);
        assert_eq!(
            RoleLoopError::Provider(error).executor_failure_code(),
            Some(executor_code)
        );
    }
}

#[tokio::test]
async fn exact_post_body_bearer_and_safe_response_metadata() {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let server = MockServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(capture_success))
            .with_state(sender),
    )
    .await;
    let provider = provider(&server, test_limits());

    let response = provider
        .complete(request_fixture(), CancellationToken::new())
        .await
        .expect("local completion succeeds");
    assert_eq!(
        response,
        ModelResponse::Final {
            content: "done".to_owned()
        }
    );

    let captured = receiver.recv().await.expect("captured one request");
    assert_eq!(
        captured.headers[AUTHORIZATION],
        "Bearer known-provider-secret"
    );
    assert_eq!(captured.headers[CONTENT_TYPE], "application/json");
    assert_eq!(captured.headers[ACCEPT], "application/json");
    assert_eq!(captured.headers[ACCEPT_ENCODING], "identity");
    let body: serde_json::Value = serde_json::from_slice(&captured.body).unwrap();
    let keys = body
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        [
            "messages",
            "model",
            "parallel_tool_calls",
            "stream",
            "tool_choice",
            "tools"
        ]
    );
    assert_eq!(body["model"], "coding-model");
    assert_eq!(body["messages"][0]["role"], "system");
    assert_eq!(body["messages"][1]["content"], "known-private-prompt");
    assert_eq!(body["tools"].as_array().unwrap().len(), 8);
    assert_eq!(body["tool_choice"], "auto");
    assert_eq!(body["parallel_tool_calls"], false);
    assert_eq!(body["stream"], false);

    let metadata = provider
        .last_response_metadata()
        .expect("safe response metadata captured");
    assert_eq!(metadata.endpoint_origin(), server.base_url());
    assert_eq!(metadata.status(), 200);
    assert_eq!(metadata.request_id(), Some("request-123"));
    for rendered in [
        format!("{provider:?}"),
        format!("{metadata:?}"),
        format!("{metadata}"),
    ] {
        assert!(!rendered.contains(API_KEY));
        assert!(!rendered.contains("known-private-prompt"));
    }
}

#[tokio::test]
async fn client_enforces_none_and_required_cargo_test_response_contracts() {
    let final_server =
        MockServer::spawn(Router::new().route("/v1/chat/completions", post(fixed_final_success)))
            .await;
    let final_provider = provider(&final_server, test_limits());

    let mut none_request = request_fixture();
    none_request.tool_choice = ModelToolChoice::None;
    assert!(matches!(
        final_provider
            .complete(none_request, CancellationToken::new())
            .await,
        Ok(ModelResponse::Final { .. })
    ));

    let mut required_request = request_fixture();
    required_request.tool_choice = ModelToolChoice::RequiredCargoTest;
    let error = final_provider
        .complete(required_request, CancellationToken::new())
        .await
        .expect_err("a final response violates required cargo_test");
    assert_eq!(error.code, PROVIDER_RESPONSE_TOOL_CHOICE_VIOLATED);
    assert!(!error.retryable);

    let tool_server = MockServer::spawn(
        Router::new().route("/v1/chat/completions", post(fixed_cargo_test_success)),
    )
    .await;
    let tool_provider = provider(&tool_server, test_limits());
    let mut required_request = request_fixture();
    required_request.tool_choice = ModelToolChoice::RequiredCargoTest;
    assert!(matches!(
        tool_provider
            .complete(required_request, CancellationToken::new())
            .await,
        Ok(ModelResponse::ToolCalls(ToolCallBatch { calls, .. }))
            if matches!(
                calls.as_slice(),
                [ToolCall {
                    request: ActionRequest::Runtime(
                        RuntimeActionRequest::Tool(ToolRequest::CargoTest { .. })
                    ),
                    ..
                }]
            )
    ));

    let mut none_request = request_fixture();
    none_request.tool_choice = ModelToolChoice::None;
    let error = tool_provider
        .complete(none_request, CancellationToken::new())
        .await
        .expect_err("a tool response violates none");
    assert_eq!(error.code, PROVIDER_RESPONSE_TOOL_CHOICE_VIOLATED);
    assert!(!error.retryable);
}

#[tokio::test]
async fn required_as_required_uses_one_request_with_only_cargo_test_and_keeps_logical_validation() {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let response_body = cargo_test_response();
    let response_bytes = response_body.len();
    let server = MockServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(capture_fixed_response))
            .with_state(CaptureFixedState {
                sender,
                response_body: Bytes::from(response_body),
            }),
    )
    .await;
    let provider = client_with_options(
        &server,
        test_limits(),
        Some("disabled"),
        Some("required_as_required"),
    )
    .start_task();
    let mut request = request_fixture();
    request.tool_choice = ModelToolChoice::RequiredCargoTest;

    let response = provider
        .complete(request, CancellationToken::new())
        .await
        .expect("the compatibility wire still satisfies the logical required choice");
    assert!(matches!(
        response,
        ModelResponse::ToolCalls(ToolCallBatch { calls, .. })
            if matches!(
                calls.as_slice(),
                [ToolCall {
                    request: ActionRequest::Runtime(
                        RuntimeActionRequest::Tool(ToolRequest::CargoTest { .. })
                    ),
                    ..
                }]
            )
    ));

    let captured = receiver
        .recv()
        .await
        .expect("captured compatibility request");
    let request_bytes = captured.body.len();
    let body: serde_json::Value = serde_json::from_slice(&captured.body).unwrap();
    assert_eq!(body["tool_choice"], "required");
    assert_eq!(body["thinking"]["type"], "disabled");
    assert_eq!(body["parallel_tool_calls"], false);
    let tools = body["tools"].as_array().expect("compatibility tools");
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["function"]["name"], "cargo_test");
    assert_eq!(
        provider.task_provider_bytes(),
        request_bytes + response_bytes,
        "the single compatibility exchange is charged exactly once"
    );
    assert!(matches!(
        receiver.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn required_as_auto_uses_one_request_with_only_cargo_test_and_keeps_logical_validation() {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let response_body = cargo_test_response();
    let response_bytes = response_body.len();
    let server = MockServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(capture_fixed_response))
            .with_state(CaptureFixedState {
                sender,
                response_body: Bytes::from(response_body),
            }),
    )
    .await;
    let provider = client_with_options(
        &server,
        test_limits(),
        Some("enabled"),
        Some("required_as_auto"),
    )
    .start_task();
    let mut request = request_fixture();
    request.tool_choice = ModelToolChoice::RequiredCargoTest;

    let response = provider
        .complete(request, CancellationToken::new())
        .await
        .expect("the auto wire still satisfies the logical required choice");
    assert!(matches!(
        response,
        ModelResponse::ToolCalls(ToolCallBatch { calls, .. })
            if matches!(
                calls.as_slice(),
                [ToolCall {
                    request: ActionRequest::Runtime(
                        RuntimeActionRequest::Tool(ToolRequest::CargoTest { .. })
                    ),
                    ..
                }]
            )
    ));

    let captured = receiver
        .recv()
        .await
        .expect("captured compatibility request");
    let request_bytes = captured.body.len();
    let body: serde_json::Value = serde_json::from_slice(&captured.body).unwrap();
    assert_eq!(body["tool_choice"], "auto");
    assert_eq!(body["thinking"]["type"], "enabled");
    assert_eq!(body["parallel_tool_calls"], false);
    let tools = body["tools"].as_array().expect("compatibility tools");
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["function"]["name"], "cargo_test");
    assert_eq!(
        provider.task_provider_bytes(),
        request_bytes + response_bytes
    );
    assert!(matches!(
        receiver.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn compatibility_modes_do_not_retry_or_relax_a_secret_bearing_choice_violation() {
    for (compatibility, expected_wire) in [
        ("required_as_required", "required"),
        ("required_as_auto", "auto"),
    ] {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let server = MockServer::spawn(
            Router::new()
                .route("/v1/chat/completions", post(capture_fixed_response))
                .with_state(CaptureFixedState {
                    sender,
                    response_body: Bytes::from(final_response(API_KEY)),
                }),
        )
        .await;
        let provider =
            client_with_options(&server, test_limits(), None, Some(compatibility)).start_task();
        let mut request = request_fixture();
        request.tool_choice = ModelToolChoice::RequiredCargoTest;

        let error = provider
            .complete(request, CancellationToken::new())
            .await
            .expect_err("a final answer still violates the logical required choice");
        assert_eq!(error.code, PROVIDER_RESPONSE_TOOL_CHOICE_VIOLATED);
        assert!(!error.retryable);
        for rendered in [format!("{error}"), format!("{error:?}")] {
            assert!(!rendered.contains(API_KEY));
        }

        let captured = receiver.recv().await.expect("captured one request");
        let body: serde_json::Value = serde_json::from_slice(&captured.body).unwrap();
        assert_eq!(body["tool_choice"], expected_wire);
        assert_eq!(body["tools"].as_array().unwrap().len(), 1);
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }
}

#[tokio::test]
async fn compatibility_modes_reject_final_wrong_tool_and_multiple_tests_with_one_hit_each() {
    let tool_response = |calls: serde_json::Value| {
        serde_json::to_vec(&serde_json::json!({
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
        .unwrap()
    };
    let cargo_call = |id: &str| {
        serde_json::json!({
            "id": id,
            "type": "function",
            "function": {
                "name": "cargo_test",
                "arguments": "{\"timeout_ms\":120000}"
            }
        })
    };
    for (compatibility, expected_wire) in [
        ("required_as_required", "required"),
        ("required_as_auto", "auto"),
    ] {
        let cases = [
            final_response("done"),
            tool_response(serde_json::json!([{
                "id": "wrong-tool",
                "type": "function",
                "function": {"name": "git_status", "arguments": "{}"}
            }])),
            tool_response(serde_json::json!([
                cargo_call("cargo-one"),
                cargo_call("cargo-two")
            ])),
        ];

        for response_body in cases {
            let (sender, mut receiver) = mpsc::unbounded_channel();
            let server = MockServer::spawn(
                Router::new()
                    .route("/v1/chat/completions", post(capture_fixed_response))
                    .with_state(CaptureFixedState {
                        sender,
                        response_body: Bytes::from(response_body),
                    }),
            )
            .await;
            let provider =
                client_with_options(&server, test_limits(), None, Some(compatibility)).start_task();
            let mut request = request_fixture();
            request.tool_choice = ModelToolChoice::RequiredCargoTest;

            let error = provider
                .complete(request, CancellationToken::new())
                .await
                .expect_err("the logical required choice must remain exact");
            assert_eq!(error.code, PROVIDER_RESPONSE_TOOL_CHOICE_VIOLATED);
            assert!(!error.retryable);

            let captured = receiver.recv().await.expect("captured the only request");
            let body: serde_json::Value = serde_json::from_slice(&captured.body).unwrap();
            assert_eq!(body["tool_choice"], expected_wire);
            assert_eq!(body["tools"].as_array().unwrap().len(), 1);
            assert_eq!(body["tools"][0]["function"]["name"], "cargo_test");
            assert!(matches!(
                receiver.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ));
        }
    }
}

#[tokio::test]
async fn required_as_required_leaves_auto_and_none_wires_unchanged() {
    for (tool_choice, expected) in [
        (ModelToolChoice::Auto, serde_json::json!("auto")),
        (ModelToolChoice::None, serde_json::json!("none")),
    ] {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let server = MockServer::spawn(
            Router::new()
                .route("/v1/chat/completions", post(capture_fixed_response))
                .with_state(CaptureFixedState {
                    sender,
                    response_body: Bytes::from(final_response("done")),
                }),
        )
        .await;
        let provider =
            client_with_options(&server, test_limits(), None, Some("required_as_required"))
                .start_task();
        let mut request = request_fixture();
        request.tool_choice = tool_choice;

        assert!(matches!(
            provider.complete(request, CancellationToken::new()).await,
            Ok(ModelResponse::Final { .. })
        ));
        let captured = receiver.recv().await.expect("captured scoped-mode request");
        let body: serde_json::Value = serde_json::from_slice(&captured.body).unwrap();
        assert_eq!(body["tool_choice"], expected);
        assert_eq!(body["tools"].as_array().unwrap().len(), 8);
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }
}

#[tokio::test]
async fn explicit_tool_choice_violations_have_one_stable_safe_error_code() {
    let tool_response = |calls: serde_json::Value, finish_reason: &str| {
        serde_json::to_vec(&serde_json::json!({
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": calls
                },
                "finish_reason": finish_reason
            }]
        }))
        .unwrap()
    };
    let final_wire = |content: &str, finish_reason: &str| {
        serde_json::to_vec(&serde_json::json!({
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": finish_reason
            }]
        }))
        .unwrap()
    };
    let cargo_call = serde_json::json!({
        "id": "cargo",
        "type": "function",
        "function": {"name": "cargo_test", "arguments": "{\"timeout_ms\":120000}"}
    });
    let unknown_call = serde_json::json!({
        "id": "unknown",
        "type": "function",
        "function": {"name": "unknown_tool", "arguments": "known-provider-secret"}
    });
    let git_call = serde_json::json!({
        "id": "status",
        "type": "function",
        "function": {"name": "git_status", "arguments": "{}"}
    });

    let violations = [
        (
            ModelToolChoice::None,
            tool_response(serde_json::json!([cargo_call.clone()]), "stop"),
        ),
        (
            ModelToolChoice::None,
            tool_response(serde_json::json!([unknown_call.clone()]), "tool_calls"),
        ),
        (
            ModelToolChoice::RequiredCargoTest,
            final_wire("done", "stop"),
        ),
        (ModelToolChoice::RequiredCargoTest, final_wire("", "length")),
        (
            ModelToolChoice::RequiredCargoTest,
            tool_response(serde_json::json!([git_call]), "tool_calls"),
        ),
        (
            ModelToolChoice::RequiredCargoTest,
            tool_response(serde_json::json!([unknown_call.clone()]), "tool_calls"),
        ),
        (
            ModelToolChoice::RequiredCargoTest,
            tool_response(
                serde_json::json!([cargo_call.clone(), unknown_call]),
                "tool_calls",
            ),
        ),
    ];
    for (choice, body) in violations {
        let error = complete_with_body(body, choice)
            .await
            .expect_err("the response must violate the explicit tool choice");
        assert_eq!(error.code, PROVIDER_RESPONSE_TOOL_CHOICE_VIOLATED);
        assert!(!error.retryable);
        for rendered in [format!("{error}"), format!("{error:?}")] {
            assert!(!rendered.contains(API_KEY));
            assert!(!rendered.contains("unknown_tool"));
        }
    }

    let wrong_finish = tool_response(serde_json::json!([cargo_call.clone()]), "stop");
    let error = complete_with_body(wrong_finish, ModelToolChoice::RequiredCargoTest)
        .await
        .expect_err("a valid required tool with the wrong finish remains a finish error");
    assert_eq!(error.code, PROVIDER_RESPONSE_FINISH_UNSUPPORTED);

    let invalid_arguments = serde_json::json!({
        "id": "cargo",
        "type": "function",
        "function": {"name": "cargo_test", "arguments": "known-provider-secret"}
    });
    let error = complete_with_body(
        tool_response(serde_json::json!([invalid_arguments]), "tool_calls"),
        ModelToolChoice::RequiredCargoTest,
    )
    .await
    .expect_err("invalid required-tool arguments remain an invalid response");
    assert_eq!(error.code, PROVIDER_RESPONSE_INVALID);
    assert!(!format!("{error:?}").contains(API_KEY));
}

#[tokio::test]
async fn explicitly_disabled_thinking_is_sent_without_changing_the_default_contract() {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let server = MockServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(capture_success))
            .with_state(sender),
    )
    .await;
    let provider = client_with_thinking(&server, test_limits(), Some("disabled")).start_task();

    provider
        .complete(request_fixture(), CancellationToken::new())
        .await
        .expect("local completion succeeds");

    let captured = receiver.recv().await.expect("captured one request");
    let body: serde_json::Value = serde_json::from_slice(&captured.body).unwrap();
    assert_eq!(body["thinking"]["type"], "disabled");
}

async fn status_response(State(status): State<StatusCode>) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"error":"known-secret-upstream-body"}"#))
        .unwrap()
}

#[tokio::test]
async fn status_failures_are_static_secret_safe_and_retryable_only_when_transient() {
    for (status, code, retryable) in [
        (StatusCode::UNAUTHORIZED, PROVIDER_UNAUTHORIZED, false),
        (StatusCode::TOO_MANY_REQUESTS, PROVIDER_RATE_LIMITED, true),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            PROVIDER_UNAVAILABLE,
            true,
        ),
        (StatusCode::BAD_GATEWAY, PROVIDER_UNAVAILABLE, true),
    ] {
        let server = MockServer::spawn(
            Router::new()
                .route("/v1/chat/completions", post(status_response))
                .with_state(status),
        )
        .await;
        let error = provider(&server, test_limits())
            .complete(request_fixture(), CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error.code, code);
        assert_eq!(error.retryable, retryable);
        assert!(!format!("{error:?}").contains("known-secret-upstream-body"));
        assert!(!format!("{error}").contains("known-secret-upstream-body"));
    }
}

#[tokio::test]
async fn non_success_body_is_chargeable_before_static_status_error_is_decoded() {
    let server = MockServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(status_response))
            .with_state(StatusCode::UNAUTHORIZED),
    )
    .await;
    let provider = provider(&server, test_limits());
    let prepared = provider.prepare(request_fixture()).unwrap();
    let raw = prepared.send(CancellationToken::new()).await.unwrap();
    let response_bytes = br#"{"error":"known-secret-upstream-body"}"#.len();

    assert_eq!(raw.encoded_len(), response_bytes);
    let error = raw.decode().unwrap_err();
    assert_eq!(error.code, PROVIDER_UNAUTHORIZED);
    assert!(!format!("{error:?}").contains("known-secret-upstream-body"));
}

#[test]
fn every_client_limit_is_nonzero() {
    let valid = test_limits();
    assert_eq!(valid.connect_timeout(), Duration::from_secs(1));
    assert_eq!(valid.request_timeout(), Duration::from_secs(2));
    assert_eq!(valid.max_request_bytes(), 256 * 1024);
    assert_eq!(valid.max_response_bytes(), 64 * 1024);
    assert_eq!(valid.max_task_provider_bytes(), 1024 * 1024);

    for (connect, request_timeout, request, response, task, expected) in [
        (
            Duration::ZERO,
            Duration::from_secs(1),
            1,
            1,
            1,
            ClientLimitsError::ZeroConnectTimeout,
        ),
        (
            Duration::from_secs(1),
            Duration::ZERO,
            1,
            1,
            1,
            ClientLimitsError::ZeroRequestTimeout,
        ),
        (
            Duration::from_secs(1),
            Duration::from_secs(1),
            0,
            1,
            1,
            ClientLimitsError::ZeroRequestBytes,
        ),
        (
            Duration::from_secs(1),
            Duration::from_secs(1),
            1,
            0,
            1,
            ClientLimitsError::ZeroResponseBytes,
        ),
        (
            Duration::from_secs(1),
            Duration::from_secs(1),
            1,
            1,
            0,
            ClientLimitsError::ZeroTaskProviderBytes,
        ),
    ] {
        assert_eq!(
            ClientLimits::try_new(connect, request_timeout, request, response, task).unwrap_err(),
            expected
        );
    }
}

async fn delayed_response() -> Response<Body> {
    tokio::time::sleep(Duration::from_secs(30)).await;
    Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(final_response("late")))
        .unwrap()
}

#[tokio::test]
async fn total_request_timeout_maps_to_a_retryable_transport_failure() {
    let server =
        MockServer::spawn(Router::new().route("/v1/chat/completions", post(delayed_response)))
            .await;
    let limits = ClientLimits::try_new(
        Duration::from_secs(1),
        Duration::from_millis(30),
        256 * 1024,
        64 * 1024,
        1024 * 1024,
    )
    .unwrap();
    let error = provider(&server, limits)
        .complete(request_fixture(), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.code, PROVIDER_TRANSPORT_FAILED);
    assert!(error.retryable);
    assert_eq!(
        RoleLoopError::Provider(error).executor_failure_code(),
        Some("EXECUTOR_PROVIDER_FAILED")
    );
}

async fn disconnected_response(State(hits): State<Arc<AtomicUsize>>) -> Response<Body> {
    hits.fetch_add(1, Ordering::SeqCst);
    let chunks: Vec<Result<Bytes, io::Error>> = vec![
        Ok(Bytes::from_static(b"{\"choices\":[")),
        Err(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "known-secret-disconnect",
        )),
    ];
    Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from_stream(stream::iter(chunks)))
        .unwrap()
}

#[tokio::test]
async fn disconnect_maps_to_retryable_transport_error_without_echoing_the_cause() {
    let hits = Arc::new(AtomicUsize::new(0));
    let server = MockServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(disconnected_response))
            .with_state(hits.clone()),
    )
    .await;
    let error = provider(&server, test_limits())
        .complete(request_fixture(), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.code, PROVIDER_TRANSPORT_FAILED);
    assert!(error.retryable);
    assert!(!format!("{error:?}").contains("known-secret-disconnect"));
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "transport retries are disabled"
    );
}

async fn malformed_response() -> Response<Body> {
    Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .header("x-request-id", "malformed-123")
        .body(Body::from("{known-secret-malformed"))
        .unwrap()
}

#[tokio::test]
async fn malformed_json_is_fatal_but_keeps_safe_request_metadata() {
    let server =
        MockServer::spawn(Router::new().route("/v1/chat/completions", post(malformed_response)))
            .await;
    let provider = provider(&server, test_limits());
    let error = provider
        .complete(request_fixture(), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.code, PROVIDER_RESPONSE_INVALID);
    assert!(!error.retryable);
    assert!(!format!("{error:?}").contains("known-secret-malformed"));
    assert_eq!(
        provider.last_response_metadata().unwrap().request_id(),
        Some("malformed-123")
    );
}

#[tokio::test]
async fn malformed_body_is_available_for_exact_charge_before_decode_rejects_it() {
    let server =
        MockServer::spawn(Router::new().route("/v1/chat/completions", post(malformed_response)))
            .await;
    let provider = provider(&server, test_limits());
    let request = request_fixture();
    let request_bytes = encode_chat_completions_request("coding-model", &request)
        .unwrap()
        .len();
    let malformed_bytes = b"{known-secret-malformed".len();

    let prepared = provider.prepare(request).unwrap();
    let raw = prepared.send(CancellationToken::new()).await.unwrap();
    assert_eq!(raw.encoded_len(), malformed_bytes);
    assert_eq!(
        provider.task_provider_bytes(),
        request_bytes + malformed_bytes
    );

    let error = raw.decode().unwrap_err();
    assert_eq!(error.code, PROVIDER_RESPONSE_INVALID);
    assert!(!format!("{error:?}").contains("known-secret-malformed"));
}

async fn length_declared_oversized_response() -> Response<Body> {
    Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(final_response(&"x".repeat(1024))))
        .unwrap()
}

async fn chunk_flood_response() -> Response<Body> {
    let chunks = (0..64).map(|_| Ok::<_, Infallible>(Bytes::from_static(b"0123456789abcdef")));
    Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from_stream(stream::iter(chunks)))
        .unwrap()
}

#[tokio::test]
async fn declared_and_no_length_bodies_stop_at_the_streaming_response_cap() {
    for router in [
        Router::new().route(
            "/v1/chat/completions",
            post(length_declared_oversized_response),
        ),
        Router::new().route("/v1/chat/completions", post(chunk_flood_response)),
    ] {
        let server = MockServer::spawn(router).await;
        let limits = ClientLimits::try_new(
            Duration::from_secs(1),
            Duration::from_secs(2),
            256 * 1024,
            63,
            1024 * 1024,
        )
        .unwrap();
        let error = provider(&server, limits)
            .complete(request_fixture(), CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error.code, PROVIDER_RESPONSE_INVALID);
        assert!(!error.retryable);
    }
}

#[tokio::test]
async fn streaming_overflow_reports_the_exact_received_prefix_before_decode_failure() {
    let server =
        MockServer::spawn(Router::new().route("/v1/chat/completions", post(chunk_flood_response)))
            .await;
    let limits = ClientLimits::try_new(
        Duration::from_secs(1),
        Duration::from_secs(2),
        256 * 1024,
        63,
        1024 * 1024,
    )
    .unwrap();
    let provider = provider(&server, limits);
    let prepared = provider.prepare(request_fixture()).unwrap();
    let raw = prepared.send(CancellationToken::new()).await.unwrap();

    assert_eq!(
        raw.encoded_len(),
        64,
        "the fourth 16-byte chunk is observed before the hard cap stops the stream"
    );
    assert_eq!(raw.decode().unwrap_err().code, PROVIDER_RESPONSE_INVALID);
}

async fn encoded_response(
    State(sender): State<mpsc::UnboundedSender<HeaderMap>>,
    request: Request<Body>,
) -> Response<Body> {
    sender.send(request.headers().clone()).unwrap();
    Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .header(CONTENT_ENCODING, "gzip")
        .body(Body::from(Bytes::from_static(&[
            0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00,
        ])))
        .unwrap()
}

async fn wrong_content_type_response() -> Response<Body> {
    Response::builder()
        .header(CONTENT_TYPE, "text/plain")
        .body(Body::from("known-secret-wrong-content-type"))
        .unwrap()
}

#[tokio::test]
async fn unsupported_content_type_body_is_chargeable_but_never_exposed() {
    let server = MockServer::spawn(
        Router::new().route("/v1/chat/completions", post(wrong_content_type_response)),
    )
    .await;
    let provider = provider(&server, test_limits());
    let prepared = provider.prepare(request_fixture()).unwrap();
    let raw = prepared.send(CancellationToken::new()).await.unwrap();

    assert_eq!(raw.encoded_len(), "known-secret-wrong-content-type".len());
    let error = raw.decode().unwrap_err();
    assert_eq!(error.code, PROVIDER_RESPONSE_INVALID);
    assert!(!format!("{error:?}").contains("known-secret-wrong-content-type"));
}

#[tokio::test]
async fn compressed_bodies_are_rejected_without_automatic_decompression() {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let server = MockServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(encoded_response))
            .with_state(sender),
    )
    .await;
    let error = provider(&server, test_limits())
        .complete(request_fixture(), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.code, PROVIDER_RESPONSE_INVALID);
    assert!(!error.retryable);
    assert_eq!(receiver.recv().await.unwrap()[ACCEPT_ENCODING], "identity");
}

async fn redirect_target(
    State(hits): State<Arc<AtomicUsize>>,
    request: Request<Body>,
) -> StatusCode {
    hits.fetch_add(1, Ordering::SeqCst);
    assert!(request.headers().get(AUTHORIZATION).is_none());
    StatusCode::NO_CONTENT
}

#[tokio::test]
async fn redirects_are_returned_as_errors_and_authorization_never_reaches_the_target() {
    let target_hits = Arc::new(AtomicUsize::new(0));
    let target = MockServer::spawn(
        Router::new()
            .route("/stolen", any(redirect_target))
            .with_state(target_hits.clone()),
    )
    .await;
    let location = format!("{}/stolen", target.base_url());
    let source = MockServer::spawn(Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let location = location.clone();
            async move {
                Response::builder()
                    .status(StatusCode::TEMPORARY_REDIRECT)
                    .header(LOCATION, location)
                    .body(Body::empty())
                    .unwrap()
            }
        }),
    ))
    .await;

    let error = provider(&source, test_limits())
        .complete(request_fixture(), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.code, PROVIDER_REDIRECT_REJECTED);
    assert!(!error.retryable);
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(target_hits.load(Ordering::SeqCst), 0);
}

async fn counted_success(State(hits): State<Arc<AtomicUsize>>) -> Response<Body> {
    hits.fetch_add(1, Ordering::SeqCst);
    Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(final_response("done")))
        .unwrap()
}

#[tokio::test]
async fn oversized_request_fails_before_any_provider_contact() {
    let hits = Arc::new(AtomicUsize::new(0));
    let server = MockServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(counted_success))
            .with_state(hits.clone()),
    )
    .await;
    let limits = ClientLimits::try_new(
        Duration::from_secs(1),
        Duration::from_secs(2),
        1,
        64 * 1024,
        1024 * 1024,
    )
    .unwrap();
    let provider = provider(&server, limits);

    let error = provider
        .complete(request_fixture(), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.code, PROVIDER_REQUEST_BYTE_LIMIT_REACHED);
    assert!(!error.retryable);
    assert_eq!(provider.task_provider_bytes(), 0);
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cumulative_task_bytes_include_requests_and_responses_and_fail_before_next_contact() {
    let hits = Arc::new(AtomicUsize::new(0));
    let server = MockServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(counted_success))
            .with_state(hits.clone()),
    )
    .await;
    let request = request_fixture();
    let request_bytes = encode_chat_completions_request("coding-model", &request)
        .unwrap()
        .len();
    let response_bytes = final_response("done").len();
    let task_limit = request_bytes + response_bytes + request_bytes - 1;
    let limits = ClientLimits::try_new(
        Duration::from_secs(1),
        Duration::from_secs(2),
        request_bytes,
        64 * 1024,
        task_limit,
    )
    .unwrap();
    let provider = provider(&server, limits);

    provider
        .complete(request.clone(), CancellationToken::new())
        .await
        .expect("first request fits cumulative budget");
    assert_eq!(
        provider.task_provider_bytes(),
        request_bytes + response_bytes
    );
    let error = provider
        .complete(request, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.code, PROVIDER_TASK_BYTE_LIMIT_REACHED);
    assert!(!error.retryable);
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn clones_for_multiple_role_loops_share_one_task_provider_byte_ledger() {
    let hits = Arc::new(AtomicUsize::new(0));
    let server = MockServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(counted_success))
            .with_state(hits.clone()),
    )
    .await;
    let request = request_fixture();
    let request_bytes = encode_chat_completions_request("coding-model", &request)
        .unwrap()
        .len();
    let response_bytes = final_response("done").len();
    let exchange_bytes = request_bytes + response_bytes;
    let limits = ClientLimits::try_new(
        Duration::from_secs(1),
        Duration::from_secs(2),
        request_bytes,
        64 * 1024,
        exchange_bytes * 2,
    )
    .unwrap();
    let shared_task = provider(&server, limits);
    let planner_view = shared_task.clone();
    let reviewer_view = shared_task.clone();

    planner_view
        .complete(request.clone(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(reviewer_view.task_provider_bytes(), exchange_bytes);
    reviewer_view
        .complete(request, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(planner_view.task_provider_bytes(), exchange_bytes * 2);
    assert_eq!(shared_task.task_provider_bytes(), exchange_bytes * 2);
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn separate_task_sessions_have_independent_cumulative_budgets() {
    let hits = Arc::new(AtomicUsize::new(0));
    let server = MockServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(counted_success))
            .with_state(hits.clone()),
    )
    .await;
    let request = request_fixture();
    let request_bytes = encode_chat_completions_request("coding-model", &request)
        .unwrap()
        .len();
    let response_bytes = final_response("done").len();
    let limits = ClientLimits::try_new(
        Duration::from_secs(1),
        Duration::from_secs(2),
        request_bytes,
        64 * 1024,
        request_bytes + response_bytes,
    )
    .unwrap();
    let client = client(&server, limits);
    let context_redactor = client.context_redactor();
    assert_eq!(context_redactor.redact(API_KEY), "<redacted>");

    let first = client.start_task();
    first
        .complete(request.clone(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(first.task_provider_bytes(), request_bytes + response_bytes);

    let second = client.start_task();
    assert_eq!(second.task_provider_bytes(), 0);
    second
        .complete(request, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(second.task_provider_bytes(), request_bytes + response_bytes);
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn prepared_request_owns_and_charges_only_its_originating_task_session() {
    let hits = Arc::new(AtomicUsize::new(0));
    let server = MockServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(counted_success))
            .with_state(hits.clone()),
    )
    .await;
    let client = client(&server, test_limits());
    let first_task = client.start_task();
    let second_task = client.start_task();
    let prepared = first_task.prepare(request_fixture()).unwrap();

    let raw = prepared.send(CancellationToken::new()).await.unwrap();
    assert!(first_task.task_provider_bytes() > 0);
    assert_eq!(second_task.task_provider_bytes(), 0);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert!(matches!(raw.decode().unwrap(), ModelResponse::Final { .. }));
}

async fn wait_for_cancellation(State(started): State<Arc<Notify>>) -> Response<Body> {
    started.notify_one();
    std::future::pending().await
}

#[tokio::test]
async fn cancellation_wins_while_waiting_for_a_response_and_is_not_retryable() {
    let started = Arc::new(Notify::new());
    let server = MockServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(wait_for_cancellation))
            .with_state(started.clone()),
    )
    .await;
    let provider = provider(&server, test_limits());
    let cancellation = CancellationToken::new();
    let completion = tokio::spawn({
        let cancellation = cancellation.clone();
        async move { provider.complete(request_fixture(), cancellation).await }
    });
    started.notified().await;
    cancellation.cancel();
    let error = tokio::time::timeout(Duration::from_secs(1), completion)
        .await
        .expect("cancellation aborts promptly")
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code, PROVIDER_CANCELLED);
    assert!(!error.retryable);
}

async fn partial_body_then_stall(State(started): State<Arc<Notify>>) -> Response<Body> {
    let first = stream::once(async move {
        started.notify_one();
        Ok::<_, Infallible>(Bytes::from_static(b"{\"choices\":["))
    });
    let body = first.chain(stream::pending::<Result<Bytes, Infallible>>());
    Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from_stream(body))
        .unwrap()
}

#[tokio::test]
async fn cancellation_wins_while_reading_a_response_body_stream() {
    let started = Arc::new(Notify::new());
    let server = MockServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(partial_body_then_stall))
            .with_state(started.clone()),
    )
    .await;
    let provider = provider(&server, test_limits());
    let cancellation = CancellationToken::new();
    let completion = tokio::spawn({
        let cancellation = cancellation.clone();
        async move { provider.complete(request_fixture(), cancellation).await }
    });

    started.notified().await;
    cancellation.cancel();
    let error = tokio::time::timeout(Duration::from_secs(1), completion)
        .await
        .expect("stream cancellation aborts promptly")
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code, PROVIDER_CANCELLED);
    assert!(!error.retryable);
}

#[tokio::test]
async fn cancellation_after_receiving_body_returns_chargeable_raw_prefix() {
    let started = Arc::new(Notify::new());
    let server = MockServer::spawn(
        Router::new()
            .route("/v1/chat/completions", post(partial_body_then_stall))
            .with_state(started.clone()),
    )
    .await;
    let provider = provider(&server, test_limits());
    let request = request_fixture();
    let request_bytes = encode_chat_completions_request("coding-model", &request)
        .unwrap()
        .len();
    let prepared = provider.prepare(request).unwrap();
    let cancellation = CancellationToken::new();
    let completion = tokio::spawn({
        let cancellation = cancellation.clone();
        async move { prepared.send(cancellation).await }
    });

    started.notified().await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while provider.task_provider_bytes() == request_bytes {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the first response prefix is charged before cancellation");
    cancellation.cancel();
    let raw = completion.await.unwrap().unwrap();
    assert_eq!(raw.encoded_len(), b"{\"choices\":[".len());
    assert_eq!(raw.decode().unwrap_err().code, PROVIDER_CANCELLED);
}

async fn secret_request_id() -> Response<Body> {
    Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .header(
            "x-request-id",
            format!("known-second-call-secret-{}", "x".repeat(300)),
        )
        .body(Body::from(final_response("done")))
        .unwrap()
}

#[tokio::test]
async fn request_id_metadata_is_bounded_and_redacted_before_exposure() {
    let server =
        MockServer::spawn(Router::new().route("/v1/chat/completions", post(secret_request_id)))
            .await;
    let provider = provider(&server, test_limits());
    let mut request = request_fixture();
    request
        .messages
        .push(ModelMessage::AssistantToolCalls(ToolCallBatch {
            assistant_content: None,
            reasoning_content: None,
            calls: vec![
                ToolCall {
                    id: "safe-first".to_owned(),
                    request: ToolRequest::GitStatus.into(),
                },
                ToolCall {
                    id: "private-second".to_owned(),
                    request: ToolRequest::SearchText {
                        query: "known-second-call-secret".to_owned(),
                        path: ".".to_owned(),
                        glob: None,
                        limit: 10,
                    }
                    .into(),
                },
            ],
        }));
    request
        .messages
        .push(ModelMessage::tool_result("safe-first", "clean"));
    request
        .messages
        .push(ModelMessage::tool_result("private-second", "matches"));
    provider
        .complete(request, CancellationToken::new())
        .await
        .unwrap();
    let metadata = provider.last_response_metadata().unwrap();
    let request_id = metadata.request_id().unwrap();
    assert!(request_id.len() <= 256);
    assert!(request_id.starts_with("<redacted>-"));
    assert!(request_id.ends_with("<truncated>"));
    assert!(!format!("{metadata:?}").contains(API_KEY));
    assert!(!format!("{metadata:?}").contains("known-second-call-secret"));
}

async fn reasoning_secret_request_id() -> Response<Body> {
    Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .header("x-request-id", "known-reasoning-metadata-secret")
        .body(Body::from(final_response("done")))
        .unwrap()
}

#[tokio::test]
async fn reasoning_history_is_part_of_request_metadata_redaction() {
    let server = MockServer::spawn(
        Router::new().route("/v1/chat/completions", post(reasoning_secret_request_id)),
    )
    .await;
    let provider = client_with_thinking(&server, test_limits(), Some("enabled")).start_task();
    let mut request = request_fixture();
    request
        .messages
        .push(ModelMessage::AssistantToolCalls(ToolCallBatch {
            assistant_content: None,
            reasoning_content: Some("known-reasoning-metadata-secret".to_owned()),
            calls: vec![ToolCall {
                id: "reasoning-call".to_owned(),
                request: ToolRequest::GitStatus.into(),
            }],
        }));
    request
        .messages
        .push(ModelMessage::tool_result("reasoning-call", "clean"));

    provider
        .complete(request, CancellationToken::new())
        .await
        .expect("thinking history request succeeds");

    let metadata = provider.last_response_metadata().unwrap();
    assert_eq!(metadata.request_id(), Some("<redacted>"));
    assert!(!format!("{metadata:?}").contains("known-reasoning-metadata-secret"));
}

async fn api_key_final_response() -> Response<Body> {
    Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(final_response(API_KEY)))
        .unwrap()
}

async fn api_key_tool_argument_response() -> Response<Body> {
    let arguments = serde_json::to_string(&serde_json::json!({
        "path": "src/lib.rs",
        "expected_sha256": null,
        "content": format!("provider echoed {API_KEY}"),
    }))
    .unwrap();
    let response = serde_json::to_vec(&serde_json::json!({
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [
                    {
                        "index": 0,
                        "id": "call-safe-first",
                        "type": "function",
                        "function": {
                            "name": "git_status",
                            "arguments": "{}",
                        }
                    },
                    {
                        "index": 1,
                        "id": "call-secret-echo",
                        "type": "function",
                        "function": {
                            "name": "replace_file",
                            "arguments": arguments,
                        }
                    }
                ]
            },
            "finish_reason": "tool_calls"
        }]
    }))
    .unwrap();
    Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(response))
        .unwrap()
}

async fn api_key_assistant_tool_content_response() -> Response<Body> {
    let response = serde_json::to_vec(&serde_json::json!({
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": format!("I found {API_KEY}"),
                "reasoning_content": "",
                "tool_calls": [{
                    "index": 0,
                    "id": "call-secret-content",
                    "type": "function",
                    "function": {
                        "name": "git_status",
                        "arguments": "{}",
                    }
                }]
            },
            "finish_reason": "tool_calls"
        }]
    }))
    .unwrap();
    Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(response))
        .unwrap()
}

#[tokio::test]
async fn successful_responses_cannot_echo_the_configured_api_key_to_runtime() {
    for router in [
        Router::new().route("/v1/chat/completions", post(api_key_final_response)),
        Router::new().route("/v1/chat/completions", post(api_key_tool_argument_response)),
        Router::new().route(
            "/v1/chat/completions",
            post(api_key_assistant_tool_content_response),
        ),
    ] {
        let server = MockServer::spawn(router).await;
        let error = provider(&server, test_limits())
            .complete(request_fixture(), CancellationToken::new())
            .await
            .expect_err("a protected config secret must never reach runtime");

        assert_eq!(error.code, PROVIDER_RESPONSE_INVALID);
        assert!(!error.retryable);
        assert!(!format!("{error:?}").contains(API_KEY));
        assert!(!format!("{error}").contains(API_KEY));
    }
}
