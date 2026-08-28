mod support;

use std::collections::BTreeSet;
use std::sync::Arc;

use coding_agent_api::{api_openapi, build_api_router};
use http::header::{ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_TYPE, HOST, RETRY_AFTER, SET_COOKIE};
use http::{Method, StatusCode};
use support::{CancelMode, PickMode, Ports, RetryMode};

fn router(ports: &Ports) -> axum::Router {
    build_api_router(
        ports.backend.clone(),
        ports.security.clone(),
        ports.sse.clone(),
    )
}

#[test]
fn openapi_paths_and_cancel_responses_are_exact() {
    let document = serde_json::to_value(api_openapi()).expect("serialize router OpenAPI");
    let paths = document["paths"]
        .as_object()
        .expect("OpenAPI paths")
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        paths,
        [
            "/api/app/quit",
            "/api/bootstrap",
            "/api/delivery-operations/{operation_id}",
            "/api/events",
            "/api/repositories",
            "/api/repositories/pick",
            "/api/session/exchange",
            "/api/tasks",
            "/api/tasks/{id}",
            "/api/tasks/{id}/cancel",
            "/api/tasks/{id}/events",
            "/api/tasks/{id}/retry",
            "/api/tasks/{task_id}/cleanup/branch",
            "/api/tasks/{task_id}/cleanup/worktree",
            "/api/tasks/{task_id}/delivery",
            "/api/tasks/{task_id}/merge",
            "/api/tasks/{task_id}/merge/preflight",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    );

    let response_cases: &[(&str, &str, &[&str])] = &[
        (
            "/api/session/exchange",
            "post",
            &["204", "400", "401", "403", "415", "500"],
        ),
        (
            "/api/bootstrap",
            "get",
            &["200", "400", "401", "403", "500", "503"],
        ),
        (
            "/api/repositories",
            "get",
            &["200", "400", "401", "403", "500", "503"],
        ),
        (
            "/api/repositories",
            "post",
            &[
                "200", "201", "400", "401", "403", "415", "422", "500", "503",
            ],
        ),
        (
            "/api/repositories/pick",
            "post",
            &[
                "200", "201", "204", "400", "401", "403", "409", "422", "500", "503",
            ],
        ),
        (
            "/api/tasks",
            "get",
            &["200", "400", "401", "403", "500", "503"],
        ),
        (
            "/api/tasks",
            "post",
            &[
                "200", "201", "400", "401", "403", "409", "415", "422", "429", "500", "503",
            ],
        ),
        (
            "/api/tasks/{id}",
            "get",
            &["200", "400", "401", "403", "404", "500", "503"],
        ),
        (
            "/api/tasks/{id}/cancel",
            "post",
            &[
                "200", "202", "400", "401", "403", "404", "409", "500", "503",
            ],
        ),
        (
            "/api/tasks/{id}/retry",
            "post",
            &[
                "200", "201", "400", "401", "403", "404", "409", "429", "500", "503",
            ],
        ),
        (
            "/api/tasks/{id}/events",
            "get",
            &["200", "400", "401", "403", "404", "500", "503"],
        ),
        ("/api/events", "get", &["200", "400", "401", "403", "500"]),
        (
            "/api/app/quit",
            "post",
            &["202", "400", "401", "403", "500", "503"],
        ),
        (
            "/api/tasks/{task_id}/delivery",
            "get",
            &["200", "400", "401", "403", "404", "500", "503"],
        ),
        (
            "/api/delivery-operations/{operation_id}",
            "get",
            &["200", "400", "401", "403", "404", "500", "503"],
        ),
        (
            "/api/tasks/{task_id}/merge/preflight",
            "post",
            &[
                "200", "201", "400", "401", "403", "404", "409", "415", "422", "500", "503", "504",
            ],
        ),
        (
            "/api/tasks/{task_id}/merge",
            "post",
            &[
                "200", "202", "400", "401", "403", "404", "409", "415", "422", "500", "503", "504",
            ],
        ),
        (
            "/api/tasks/{task_id}/cleanup/worktree",
            "post",
            &[
                "200", "202", "400", "401", "403", "404", "409", "415", "422", "500", "503", "504",
            ],
        ),
        (
            "/api/tasks/{task_id}/cleanup/branch",
            "post",
            &[
                "200", "202", "400", "401", "403", "404", "409", "415", "422", "500", "503", "504",
            ],
        ),
    ];
    for (path, method, expected) in response_cases {
        let actual = document["paths"][path][method]["responses"]
            .as_object()
            .unwrap_or_else(|| panic!("OpenAPI responses for {method} {path}"))
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let expected = expected
            .iter()
            .map(|status| (*status).to_owned())
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, expected, "OpenAPI statuses for {method} {path}");
    }

    let cancel = &document["paths"]["/api/tasks/{id}/cancel"]["post"]["responses"];
    assert_eq!(
        cancel["200"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/TaskDto"
    );
    assert_eq!(
        cancel["202"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/CancellationAcceptedResponse"
    );
}

#[tokio::test]
async fn exchange_requires_exact_boundary_and_returns_sensitive_cookie() {
    let ports = Ports::new();
    let request = support::request(Method::POST, "/api/session/exchange")
        .header(http::header::ORIGIN, support::ORIGIN_VALUE)
        .header(CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(r#"{"token":"launch-token"}"#))
        .unwrap();
    let response = support::send(router(&ports), request).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(response.headers().contains_key(SET_COOKIE));
    assert_eq!(response.headers()["x-request-id"], support::REQUEST_ID);
    assert!(!response.headers().contains_key(ACCESS_CONTROL_ALLOW_ORIGIN));

    for (header, value, code) in [
        (HOST, "evil.invalid", "SECURITY_INVALID_HOST"),
        (
            http::header::ORIGIN,
            "http://evil.invalid",
            "SECURITY_INVALID_ORIGIN",
        ),
    ] {
        let mut request = support::request(Method::POST, "/api/session/exchange")
            .header(http::header::ORIGIN, support::ORIGIN_VALUE)
            .header(CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(r#"{"token":"launch-token"}"#))
            .unwrap();
        request.headers_mut().insert(header, value.parse().unwrap());
        let response = support::send(router(&Ports::new()), request).await;
        let (status, _, body) = support::json(response).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body["code"], code);
    }

    let request = support::request(Method::POST, "/api/session/exchange")
        .header(http::header::ORIGIN, support::ORIGIN_VALUE)
        .header(CONTENT_TYPE, "text/plain")
        .body(axum::body::Body::from("launch-token"))
        .unwrap();
    assert_eq!(
        support::send(router(&Ports::new()), request).await.status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
}

#[tokio::test]
async fn every_read_route_requires_session_and_has_the_expected_media_type() {
    let ports = Ports::new();
    let task_id = ports.backend.task().id;
    let repository_id = ports.backend.repository().id;
    let cases = [
        ("/api/bootstrap".to_owned(), "application/json"),
        ("/api/repositories".to_owned(), "application/json"),
        (
            format!("/api/tasks?repository_id={repository_id}"),
            "application/json",
        ),
        (format!("/api/tasks/{task_id}"), "application/json"),
        (
            format!("/api/tasks/{task_id}/events?after=0"),
            "application/json",
        ),
        ("/api/events?after=0".to_owned(), "text/event-stream"),
    ];

    for (uri, media_type) in cases {
        let response =
            support::send(router(&ports), support::read_request(Method::GET, &uri)).await;
        assert_eq!(response.status(), StatusCode::OK, "GET {uri}");
        assert!(
            response.headers()[CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with(media_type),
            "GET {uri}"
        );
        assert_eq!(response.headers()["x-request-id"], support::REQUEST_ID);
        assert!(!response.headers().contains_key(ACCESS_CONTROL_ALLOW_ORIGIN));

        let request = support::request(Method::GET, &uri)
            .body(axum::body::Body::empty())
            .unwrap();
        let (status, _, body) = support::json(support::send(router(&ports), request).await).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "GET {uri}");
        assert_eq!(body["code"], "SECURITY_INVALID_SESSION");
    }
}

#[tokio::test]
async fn malformed_and_ambiguous_query_parameters_are_rejected() {
    let ports = Ports::new();
    let repository_id = ports.backend.repository().id;
    let task_id = ports.backend.task().id;
    let cases = [
        "/api/tasks?repository_id".to_owned(),
        "/api/tasks?repository_id=".to_owned(),
        format!("/api/tasks?repository_id={repository_id}&repository_id={repository_id}"),
        "/api/tasks?unknown".to_owned(),
        "/api/events?after".to_owned(),
        "/api/events?after=".to_owned(),
        "/api/events?after=0&after=1".to_owned(),
        format!("/api/tasks/{task_id}/events?after=0&after=1"),
    ];

    for uri in cases {
        let response =
            support::send(router(&ports), support::read_request(Method::GET, &uri)).await;
        let (status, _, body) = support::json(response).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "GET {uri}");
        assert_eq!(body["code"], "INVALID_QUERY", "GET {uri}");
    }

    for uri in ["/api/tasks?unknown=value", "/api/events?unknown=value"] {
        let response = support::send(router(&ports), support::read_request(Method::GET, uri)).await;
        assert_eq!(response.status(), StatusCode::OK, "GET {uri}");
    }
}

#[tokio::test]
async fn route_methods_are_closed_and_request_ids_cover_rejections() {
    let ports = Ports::new();
    for uri in [
        "/api/bootstrap",
        "/api/repositories/pick",
        "/api/tasks",
        "/api/events",
        "/api/app/quit",
    ] {
        let request = support::request(Method::DELETE, uri)
            .header(http::header::COOKIE, support::COOKIE_VALUE)
            .body(axum::body::Body::empty())
            .unwrap();
        let response = support::send(router(&ports), request).await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED, "{uri}");
        assert_eq!(response.headers()["x-request-id"], support::REQUEST_ID);
    }

    let request = support::request(Method::GET, "/does-not-exist")
        .body(axum::body::Body::empty())
        .unwrap();
    let response = support::send(router(&ports), request).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(response.headers()["x-request-id"], support::REQUEST_ID);

    let request = http::Request::builder()
        .uri("/api/bootstrap")
        .header(HOST, support::HOST_VALUE)
        .header(http::header::COOKIE, support::COOKIE_VALUE)
        .header("x-request-id", "not a valid request id\r\n")
        .body(axum::body::Body::empty());
    assert!(
        request.is_err(),
        "HTTP itself rejects CRLF header injection"
    );

    let request = http::Request::builder()
        .uri("/api/bootstrap")
        .header(HOST, support::HOST_VALUE)
        .header(http::header::COOKIE, support::COOKIE_VALUE)
        .header("x-request-id", "malformed-but-header-safe")
        .body(axum::body::Body::empty())
        .unwrap();
    let response = support::send(router(&ports), request).await;
    assert_eq!(response.status(), StatusCode::OK);
    let generated = response.headers()["x-request-id"].to_str().unwrap();
    assert_ne!(generated, "malformed-but-header-safe");
    uuid::Uuid::parse_str(generated).expect("server generated a UUID request ID");
}

#[tokio::test]
async fn repository_create_and_picker_status_matrix_is_exact() {
    let ports = Ports::new();
    for (path, expected, code) in [
        ("created", StatusCode::CREATED, None),
        ("existing", StatusCode::OK, None),
        ("busy", StatusCode::SERVICE_UNAVAILABLE, Some("STORE_BUSY")),
        (
            "degraded",
            StatusCode::SERVICE_UNAVAILABLE,
            Some("STORE_DEGRADED"),
        ),
        (
            "invalid",
            StatusCode::UNPROCESSABLE_ENTITY,
            Some("REPOSITORY_PATH_NOT_FOUND"),
        ),
    ] {
        let response = support::send(
            router(&ports),
            support::mutation_request("/api/repositories", serde_json::json!({"path": path})),
        )
        .await;
        let (status, _, body) = support::json(response).await;
        assert_eq!(status, expected, "path={path}");
        if let Some(code) = code {
            assert_eq!(body["code"], code);
        }
    }

    for (mode, expected) in [
        (PickMode::Created, StatusCode::CREATED),
        (PickMode::Existing, StatusCode::OK),
        (PickMode::Cancelled, StatusCode::NO_CONTENT),
        (PickMode::Busy, StatusCode::CONFLICT),
        (PickMode::Invalid, StatusCode::UNPROCESSABLE_ENTITY),
    ] {
        ports.backend.set_pick_mode(mode);
        let response = support::send(
            router(&ports),
            support::empty_mutation_request("/api/repositories/pick"),
        )
        .await;
        assert_eq!(response.status(), expected, "mode={mode:?}");
    }
}

#[tokio::test]
async fn task_create_validates_body_and_maps_idempotency_and_store_failures() {
    let ports = Ports::new();
    for (prompt, expected, code) in [
        ("created", StatusCode::CREATED, None),
        ("existing", StatusCode::OK, None),
        (
            "conflict",
            StatusCode::CONFLICT,
            Some("IDEMPOTENCY_CONFLICT"),
        ),
        (
            "queue-full",
            StatusCode::TOO_MANY_REQUESTS,
            Some("TASK_QUEUE_FULL"),
        ),
        ("busy", StatusCode::SERVICE_UNAVAILABLE, Some("STORE_BUSY")),
        (
            "degraded",
            StatusCode::SERVICE_UNAVAILABLE,
            Some("STORE_DEGRADED"),
        ),
        (
            "   ",
            StatusCode::UNPROCESSABLE_ENTITY,
            Some("INVALID_PROMPT"),
        ),
    ] {
        let response = support::send(
            router(&ports),
            support::mutation_request("/api/tasks", support::create_task_body(prompt)),
        )
        .await;
        let (status, _, body) = support::json(response).await;
        assert_eq!(status, expected, "prompt={prompt:?}");
        if let Some(code) = code {
            assert_eq!(body["code"], code);
        }
    }

    let over_limit = "界".repeat(50_001);
    let response = support::send(
        router(&ports),
        support::mutation_request("/api/tasks", support::create_task_body(&over_limit)),
    )
    .await;
    let (status, _, body) = support::json(response).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["code"], "INVALID_PROMPT");

    let request = support::request(Method::POST, "/api/tasks")
        .header(http::header::COOKIE, support::COOKIE_VALUE)
        .header(http::header::ORIGIN, support::ORIGIN_VALUE)
        .header("x-csrf-token", support::CSRF_VALUE)
        .header(CONTENT_TYPE, "text/plain")
        .body(axum::body::Body::from("not json"))
        .unwrap();
    assert_eq!(
        support::send(router(&ports), request).await.status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
}

#[tokio::test]
async fn queue_full_and_stop_winner_errors_have_exact_envelopes() {
    let ports = Ports::new();
    let create = support::send(
        router(&ports),
        support::mutation_request("/api/tasks", support::create_task_body("queue-full")),
    )
    .await;
    assert_queue_full_response(create).await;

    ports.backend.set_retry_mode(RetryMode::QueueFull);
    let task_id = ports.backend.task().id;
    let retry = support::send(
        router(&ports),
        support::empty_mutation_request(&format!("/api/tasks/{task_id}/retry")),
    )
    .await;
    assert_queue_full_response(retry).await;

    ports
        .backend
        .set_cancel_mode(CancelMode::StopAlreadyRequested);
    let cancel = support::send(
        router(&ports),
        support::empty_mutation_request(&format!("/api/tasks/{task_id}/cancel")),
    )
    .await;
    let (status, headers, body) = support::json(cancel).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(!headers.contains_key(RETRY_AFTER));
    assert_eq!(
        body,
        serde_json::json!({
            "code": "TASK_STOP_ALREADY_REQUESTED",
            "message": "another stop request already won for this task",
            "retryable": false,
            "request_id": support::REQUEST_ID,
            "details": {},
        })
    );
}

async fn assert_queue_full_response(response: axum::response::Response) {
    let (status, headers, body) = support::json(response).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(!headers.contains_key(RETRY_AFTER));
    assert_eq!(
        body,
        serde_json::json!({
            "code": "TASK_QUEUE_FULL",
            "message": "the task queue is full; retry after capacity becomes available",
            "retryable": true,
            "request_id": support::REQUEST_ID,
            "details": {
                "queued_tasks": 32,
                "max_queued_tasks": 32,
            },
        })
    );
}

#[tokio::test]
async fn cancel_retry_and_quit_status_matrix_is_exact() {
    let ports = Ports::new();
    let id = ports.backend.task().id;

    for (mode, expected) in [
        (CancelMode::Finished, StatusCode::OK),
        (CancelMode::Accepted, StatusCode::ACCEPTED),
        (CancelMode::Conflict, StatusCode::CONFLICT),
    ] {
        ports.backend.set_cancel_mode(mode);
        let response = support::send(
            router(&ports),
            support::empty_mutation_request(&format!("/api/tasks/{id}/cancel")),
        )
        .await;
        assert_eq!(response.status(), expected, "cancel={mode:?}");
    }

    for (mode, expected) in [
        (RetryMode::Created, StatusCode::CREATED),
        (RetryMode::Existing, StatusCode::OK),
        (RetryMode::Conflict, StatusCode::CONFLICT),
    ] {
        ports.backend.set_retry_mode(mode);
        let response = support::send(
            router(&ports),
            support::empty_mutation_request(&format!("/api/tasks/{id}/retry")),
        )
        .await;
        assert_eq!(response.status(), expected, "retry={mode:?}");
    }

    let response = support::send(
        router(&ports),
        support::empty_mutation_request("/api/app/quit"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert!(
        !ports.backend.quit_triggered(),
        "response body is not drained yet"
    );
    let (_, _, body) = support::json(response).await;
    assert_eq!(body, serde_json::json!({"status":"shutting_down"}));
    assert!(ports.backend.quit_triggered(), "EOS fires the quit trigger");
}

#[tokio::test]
async fn mutations_require_origin_and_csrf_before_calling_backend() {
    for omitted in [http::header::ORIGIN.as_str(), "x-csrf-token"] {
        let ports = Ports::new();
        let body = support::create_task_body("not-called");
        let mut request = support::mutation_request("/api/tasks", body);
        request.headers_mut().remove(omitted);
        let (status, _, _) = support::json(support::send(router(&ports), request).await).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(!ports.backend.calls().contains(&"create_task"));
    }
}

#[tokio::test]
async fn errors_and_panics_are_json_and_never_reflect_malformed_request_ids() {
    let ports = Ports::new();
    let mut request = support::mutation_request(
        "/api/tasks",
        support::create_task_body("panic-known-prompt"),
    );
    request
        .headers_mut()
        .insert("x-request-id", "known-invalid-request-id".parse().unwrap());
    let response = support::send(router(&ports), request).await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let generated = response.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_owned();
    uuid::Uuid::parse_str(&generated).expect("panic response request UUID");
    let (_, _, body) = support::json(response).await;
    assert_eq!(body["code"], "INTERNAL_ERROR");
    assert_eq!(body["request_id"], generated);
    assert!(!body.to_string().contains("panic-known-prompt"));
}

#[test]
fn fake_ports_are_shared_without_hiding_the_router_boundary() {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<Arc<support::FakeBackend>>();
}
