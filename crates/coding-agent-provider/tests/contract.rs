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
use coding_agent_core::{ModelMessage, ModelProvider, ModelRequest, ModelResponse};
use coding_agent_provider::{
    ChatCompletionsClient, ChatCompletionsProvider, ClientLimits, ClientLimitsError,
    PROVIDER_CANCELLED, PROVIDER_RATE_LIMITED, PROVIDER_REDIRECT_REJECTED,
    PROVIDER_REQUEST_BYTE_LIMIT_REACHED, PROVIDER_RESPONSE_INVALID,
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
    let base_url = server.base_url();
    let parsed = url::Url::parse(&base_url).unwrap();
    assert_eq!(parsed.host_str(), Some("127.0.0.1"));
    let encoded = serde_json::to_vec(&serde_json::json!({
        "base_url": base_url,
        "model": "coding-model",
        "api_key": API_KEY,
    }))
    .unwrap();
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

async fn secret_request_id() -> Response<Body> {
    Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .header(
            "x-request-id",
            format!("known-private-prompt-{}", "x".repeat(300)),
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
    provider
        .complete(request_fixture(), CancellationToken::new())
        .await
        .unwrap();
    let metadata = provider.last_response_metadata().unwrap();
    let request_id = metadata.request_id().unwrap();
    assert!(request_id.len() <= 256);
    assert!(request_id.starts_with("<redacted>-"));
    assert!(request_id.ends_with("<truncated>"));
    assert!(!format!("{metadata:?}").contains(API_KEY));
    assert!(!format!("{metadata:?}").contains("known-private-prompt"));
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
                "tool_calls": [{
                    "id": "call-secret-echo",
                    "type": "function",
                    "function": {
                        "name": "replace_file",
                        "arguments": arguments,
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
