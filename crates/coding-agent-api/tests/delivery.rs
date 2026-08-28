mod support;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use coding_agent_api::{
    ApiError, ApiErrorResponse, ApiResult, AuthContext, DeliveryAllowedActionDto,
    DeliveryApiErrorKind, DeliveryBackend, DeliveryCleanupKindDto, DeliveryCleanupOperationDto,
    DeliveryCleanupStateDto, DeliveryCommandResponse, DeliveryDeleteBranchRequest,
    DeliveryEligibilityDto, DeliveryEvidenceSummaryDto, DeliveryMergeOperationDto,
    DeliveryMergeRequest, DeliveryMergeStateDto, DeliveryOperationDto, DeliveryPreflightRequest,
    DeliveryReceiptDispositionDto, DeliveryTargetObservationDto, DeliveryTaskDto,
    ValidatedDeliveryDeleteBranchCommand, ValidatedDeliveryMergeCommand,
    ValidatedDeliveryPreflightCommand, ValidatedDeliveryRemoveWorktreeCommand, build_api_router,
    build_api_router_with_delivery,
};
use coding_agent_domain::TaskId;
use http::header::{CONTENT_TYPE, COOKIE, ORIGIN};
use http::{Method, StatusCode};
use uuid::Uuid;

const CLIENT_ID: &str = "123e4567-e89b-42d3-a456-426614174101";
const OPERATION_ID: &str = "123e4567-e89b-42d3-a456-426614174102";
const MERGE_ID: &str = "123e4567-e89b-42d3-a456-426614174103";
const SHA1_A: &str = "1111111111111111111111111111111111111111";
const SHA1_B: &str = "2222222222222222222222222222222222222222";
const FINGERPRINT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

struct FakeDeliveryBackend {
    existing: AtomicBool,
    latest_cleanup: AtomicBool,
    calls: Mutex<Vec<String>>,
}

impl FakeDeliveryBackend {
    fn new() -> Self {
        Self {
            existing: AtomicBool::new(false),
            latest_cleanup: AtomicBool::new(false),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn set_existing(&self, existing: bool) {
        self.existing.store(existing, Ordering::SeqCst);
    }

    fn set_latest_cleanup(&self, latest_cleanup: bool) {
        self.latest_cleanup.store(latest_cleanup, Ordering::SeqCst);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("lock delivery calls").clone()
    }

    fn record(&self, call: String) {
        self.calls.lock().expect("lock delivery calls").push(call);
    }

    fn receipt(&self) -> DeliveryReceiptDispositionDto {
        if self.existing.load(Ordering::SeqCst) {
            DeliveryReceiptDispositionDto::Existing
        } else {
            DeliveryReceiptDispositionDto::Created
        }
    }
}

#[async_trait::async_trait]
impl DeliveryBackend for FakeDeliveryBackend {
    async fn task_delivery(&self, _: &AuthContext, task_id: TaskId) -> ApiResult<DeliveryTaskDto> {
        self.record(format!("get-task:{task_id}"));
        Ok(DeliveryTaskDto {
            task_id: task_id.as_uuid(),
            eligibility: DeliveryEligibilityDto::Eligible,
            reasons: Vec::new(),
            evidence: Some(DeliveryEvidenceSummaryDto {
                review_generation: 7,
                workspace_fingerprint: FINGERPRINT.to_owned(),
            }),
            target: DeliveryTargetObservationDto::available(
                "refs/heads/main".to_owned(),
                SHA1_A.to_owned(),
            ),
            source: None,
            latest_merge: None,
            latest_cleanup: self
                .latest_cleanup
                .load(Ordering::SeqCst)
                .then(|| cleanup_operation_dto(DeliveryCleanupKindDto::RemoveWorktree)),
            disposition: None,
            allowed_actions: vec![DeliveryAllowedActionDto::RunPreflight],
        })
    }

    async fn delivery_operation(
        &self,
        _: &AuthContext,
        operation_id: Uuid,
    ) -> ApiResult<DeliveryOperationDto> {
        self.record(format!("get-operation:{operation_id}"));
        Ok(merge_operation(operation_id))
    }

    async fn preflight(
        &self,
        _: &AuthContext,
        command: ValidatedDeliveryPreflightCommand,
    ) -> ApiResult<DeliveryCommandResponse> {
        self.record(format!(
            "preflight:{}:{}:{}:{}",
            command.task_id(),
            command.client_request_id(),
            command.target_branch(),
            command.expected_target_head()
        ));
        Ok(DeliveryCommandResponse {
            receipt: self.receipt(),
            operation: merge_operation(operation_id()),
        })
    }

    async fn accept_merge(
        &self,
        _: &AuthContext,
        command: ValidatedDeliveryMergeCommand,
    ) -> ApiResult<DeliveryCommandResponse> {
        self.record(format!(
            "merge:{}:{}:{}:{}:{}:{}:{}:{}",
            command.task_id(),
            command.client_request_id(),
            command.preflight_operation_id(),
            command.expected_operation_version(),
            command.expected_review_generation(),
            command.expected_workspace_fingerprint(),
            command.target_branch(),
            command.expected_target_head()
        ));
        Ok(DeliveryCommandResponse {
            receipt: self.receipt(),
            operation: merge_operation(operation_id()),
        })
    }

    async fn remove_worktree(
        &self,
        _: &AuthContext,
        command: ValidatedDeliveryRemoveWorktreeCommand,
    ) -> ApiResult<DeliveryCommandResponse> {
        self.record(format!(
            "worktree:{}:{}:{}:{}:{}:{}",
            command.task_id(),
            command.client_request_id(),
            command.expected_disposition_version(),
            command.expected_merge_operation_id(),
            command.expected_source_ref(),
            command.expected_source_oid()
        ));
        Ok(DeliveryCommandResponse {
            receipt: self.receipt(),
            operation: cleanup_operation(DeliveryCleanupKindDto::RemoveWorktree),
        })
    }

    async fn delete_branch(
        &self,
        _: &AuthContext,
        command: ValidatedDeliveryDeleteBranchCommand,
    ) -> ApiResult<DeliveryCommandResponse> {
        self.record(format!(
            "branch:{}:{}:{}:{}:{}:{}:{}:{}",
            command.task_id(),
            command.client_request_id(),
            command.expected_disposition_version(),
            command.expected_merge_operation_id(),
            command.expected_source_ref(),
            command.expected_source_oid(),
            command.target_branch(),
            command.target_head()
        ));
        Ok(DeliveryCommandResponse {
            receipt: self.receipt(),
            operation: cleanup_operation(DeliveryCleanupKindDto::DeleteBranch),
        })
    }
}

fn router(ports: &support::Ports, delivery: Arc<FakeDeliveryBackend>) -> axum::Router {
    build_api_router_with_delivery(
        ports.backend.clone(),
        ports.security.clone(),
        ports.sse.clone(),
        delivery,
    )
}

#[tokio::test]
async fn both_get_routes_require_session_and_return_exact_discriminated_projections() {
    let ports = support::Ports::new();
    let delivery = Arc::new(FakeDeliveryBackend::new());
    let task_id = ports.backend.task().id;

    let response = support::send(
        router(&ports, delivery.clone()),
        support::read_request(Method::GET, &format!("/api/tasks/{task_id}/delivery")),
    )
    .await;
    let (status, _, body) = support::json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["task_id"], task_id.to_string());
    assert_eq!(body["eligibility"], "eligible");
    assert_eq!(body["target"]["available"], true);
    assert_eq!(body["latest_cleanup"], serde_json::Value::Null);
    assert_eq!(
        body.as_object()
            .expect("delivery task projection object")
            .len(),
        10,
        "latest_cleanup is a required-nullable projection field"
    );
    assert_eq!(
        body["allowed_actions"],
        serde_json::json!(["run_preflight"])
    );

    delivery.set_latest_cleanup(true);
    let response = support::send(
        router(&ports, delivery.clone()),
        support::read_request(Method::GET, &format!("/api/tasks/{task_id}/delivery")),
    )
    .await;
    let (status, _, body) = support::json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["latest_cleanup"]["operation_id"], OPERATION_ID);
    assert_eq!(body["latest_cleanup"]["cleanup_kind"], "remove_worktree");
    assert!(
        body["latest_cleanup"].get("kind").is_none(),
        "task projection embeds the bare cleanup DTO, not an operation envelope"
    );

    let response = support::send(
        router(&ports, delivery.clone()),
        support::read_request(
            Method::GET,
            &format!("/api/delivery-operations/{}", operation_id()),
        ),
    )
    .await;
    let (status, _, body) = support::json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["kind"], "merge");
    assert_eq!(body["operation_id"], operation_id().to_string());

    for uri in [
        format!("/api/tasks/{task_id}/delivery"),
        format!("/api/delivery-operations/{}", operation_id()),
    ] {
        let request = support::request(Method::GET, &uri)
            .body(axum::body::Body::empty())
            .unwrap();
        let (status, _, body) =
            support::json(support::send(router(&ports, delivery.clone()), request).await).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["code"], "SECURITY_INVALID_SESSION");
    }
}

#[tokio::test]
async fn four_mutations_validate_and_delegate_every_exact_field() {
    let ports = support::Ports::new();
    let delivery = Arc::new(FakeDeliveryBackend::new());
    let task_id = ports.backend.task().id;
    let cases = [
        (
            format!("/api/tasks/{task_id}/merge/preflight"),
            serde_json::json!({
                "client_request_id": CLIENT_ID,
                "target_branch": "refs/heads/main",
                "expected_target_head": SHA1_A,
            }),
            StatusCode::CREATED,
            "merge",
            None,
        ),
        (
            format!("/api/tasks/{task_id}/merge"),
            serde_json::json!({
                "client_request_id": CLIENT_ID,
                "preflight_operation_id": OPERATION_ID,
                "expected_operation_version": 2,
                "expected_review_generation": 7,
                "expected_workspace_fingerprint": FINGERPRINT,
                "target_branch": "refs/heads/main",
                "expected_target_head": SHA1_A,
            }),
            StatusCode::ACCEPTED,
            "merge",
            None,
        ),
        (
            format!("/api/tasks/{task_id}/cleanup/worktree"),
            serde_json::json!({
                "client_request_id": CLIENT_ID,
                "expected_disposition_version": 1,
                "expected_merge_operation_id": MERGE_ID,
                "expected_source_ref": "refs/heads/coding-agent/task",
                "expected_source_oid": SHA1_B,
            }),
            StatusCode::ACCEPTED,
            "cleanup",
            Some("remove_worktree"),
        ),
        (
            format!("/api/tasks/{task_id}/cleanup/branch"),
            serde_json::json!({
                "client_request_id": CLIENT_ID,
                "expected_disposition_version": 2,
                "expected_merge_operation_id": MERGE_ID,
                "expected_source_ref": "refs/heads/coding-agent/task",
                "expected_source_oid": SHA1_B,
                "target_branch": "refs/heads/main",
                "target_head": SHA1_A,
            }),
            StatusCode::ACCEPTED,
            "cleanup",
            Some("delete_branch"),
        ),
    ];

    for (uri, body, expected_status, expected_kind, expected_cleanup_kind) in cases {
        let response = support::send(
            router(&ports, delivery.clone()),
            support::mutation_request(&uri, body),
        )
        .await;
        let (status, _, body) = support::json(response).await;
        assert_eq!(status, expected_status, "POST {uri}");
        assert_eq!(body["receipt"], "created");
        assert_eq!(body.as_object().expect("command response object").len(), 2);
        assert!(body.get("kind").is_none(), "operation must remain nested");
        assert_eq!(body["operation"]["kind"], expected_kind);
        if let Some(expected_cleanup_kind) = expected_cleanup_kind {
            assert_eq!(
                body["operation"]["cleanup_kind"], expected_cleanup_kind,
                "POST {uri}"
            );
        }
    }

    let calls = delivery.calls();
    assert_eq!(calls.len(), 4);
    assert!(calls[0].contains("refs/heads/main"));
    assert!(calls[1].contains(FINGERPRINT));
    assert!(calls[2].contains("refs/heads/coding-agent/task"));
    assert!(calls[3].contains(SHA1_A));
}

#[tokio::test]
async fn created_and_existing_receipts_drive_only_the_locked_http_status() {
    let ports = support::Ports::new();
    let delivery = Arc::new(FakeDeliveryBackend::new());
    delivery.set_existing(true);
    let task_id = ports.backend.task().id;

    for (suffix, body) in [
        (
            "merge/preflight",
            serde_json::json!({
                "client_request_id": CLIENT_ID,
                "target_branch": "refs/heads/main",
                "expected_target_head": SHA1_A,
            }),
        ),
        (
            "cleanup/worktree",
            serde_json::json!({
                "client_request_id": CLIENT_ID,
                "expected_disposition_version": 1,
                "expected_merge_operation_id": MERGE_ID,
                "expected_source_ref": "refs/heads/coding-agent/task",
                "expected_source_oid": SHA1_B,
            }),
        ),
    ] {
        let response = support::send(
            router(&ports, delivery.clone()),
            support::mutation_request(&format!("/api/tasks/{task_id}/{suffix}"), body),
        )
        .await;
        let (status, _, body) = support::json(response).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["receipt"], "existing");
    }
}

#[tokio::test]
async fn json_order_and_whitespace_produce_the_same_validated_command() {
    let ports = support::Ports::new();
    let delivery = Arc::new(FakeDeliveryBackend::new());
    let task_id = ports.backend.task().id;
    let uri = format!("/api/tasks/{task_id}/merge/preflight");
    let bodies = [
        format!(
            r#"{{"client_request_id":"{CLIENT_ID}","target_branch":"refs/heads/main","expected_target_head":"{SHA1_A}"}}"#
        ),
        format!(
            "{{ \n  \"expected_target_head\": \"{SHA1_A}\", \n  \"client_request_id\": \"{CLIENT_ID}\", \n  \"target_branch\": \"refs/heads/main\" \n}}"
        ),
    ];
    for body in bodies {
        let request = support::request(Method::POST, &uri)
            .header(COOKIE, support::COOKIE_VALUE)
            .header(ORIGIN, support::ORIGIN_VALUE)
            .header("x-csrf-token", support::CSRF_VALUE)
            .header(CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(body))
            .unwrap();
        assert_eq!(
            support::send(router(&ports, delivery.clone()), request)
                .await
                .status(),
            StatusCode::CREATED
        );
    }
    let calls = delivery.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0], calls[1]);
}

#[tokio::test]
async fn delivery_mutations_reuse_session_origin_csrf_media_and_bounded_json_guards() {
    let ports = support::Ports::new();
    let delivery = Arc::new(FakeDeliveryBackend::new());
    let task_id = ports.backend.task().id;
    let uri = format!("/api/tasks/{task_id}/merge/preflight");
    let body = serde_json::json!({
        "client_request_id": CLIENT_ID,
        "target_branch": "refs/heads/main",
        "expected_target_head": SHA1_A,
    });

    for (header, expected_status) in [
        (COOKIE, StatusCode::UNAUTHORIZED),
        (ORIGIN, StatusCode::FORBIDDEN),
        (
            http::HeaderName::from_static("x-csrf-token"),
            StatusCode::FORBIDDEN,
        ),
    ] {
        let mut request = support::mutation_request(&uri, body.clone());
        request.headers_mut().remove(header);
        assert_eq!(
            support::send(router(&ports, delivery.clone()), request)
                .await
                .status(),
            expected_status
        );
    }

    let request = support::request(Method::POST, &uri)
        .header(COOKIE, support::COOKIE_VALUE)
        .header(ORIGIN, support::ORIGIN_VALUE)
        .header("x-csrf-token", support::CSRF_VALUE)
        .header(CONTENT_TYPE, "text/plain")
        .body(axum::body::Body::from(body.to_string()))
        .unwrap();
    assert_eq!(
        support::send(router(&ports, delivery.clone()), request)
            .await
            .status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );

    let huge = format!(r#"{{"unknown":"{}"}}"#, "x".repeat(65_536));
    let request = support::request(Method::POST, &uri)
        .header(COOKIE, support::COOKIE_VALUE)
        .header(ORIGIN, support::ORIGIN_VALUE)
        .header("x-csrf-token", support::CSRF_VALUE)
        .header(CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(huge))
        .unwrap();
    let (status, _, body) =
        support::json(support::send(router(&ports, delivery), request).await).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["code"], "INVALID_REQUEST");
}

#[tokio::test]
async fn malformed_unknown_and_semantically_invalid_fields_are_separated() {
    let ports = support::Ports::new();
    let delivery = Arc::new(FakeDeliveryBackend::new());
    let task_id = ports.backend.task().id;
    let uri = format!("/api/tasks/{task_id}/merge/preflight");

    for body in [
        serde_json::json!({"client_request_id": CLIENT_ID}),
        serde_json::json!({
            "client_request_id": CLIENT_ID,
            "target_branch": "refs/heads/main",
            "expected_target_head": SHA1_A,
            "unknown": true,
        }),
    ] {
        let (status, _, body) = support::json(
            support::send(
                router(&ports, delivery.clone()),
                support::mutation_request(&uri, body),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "INVALID_JSON");
    }

    for (client_request_id, branch, oid) in [
        (
            "00000000-0000-0000-0000-000000000000",
            "refs/heads/main",
            SHA1_A,
        ),
        (CLIENT_ID, "main", SHA1_A),
        (
            CLIENT_ID,
            "refs/heads/main",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        ),
        (
            CLIENT_ID,
            "refs/heads/main",
            "0000000000000000000000000000000000000000",
        ),
    ] {
        let response = support::send(
            router(&ports, delivery.clone()),
            support::mutation_request(
                &uri,
                serde_json::json!({
                    "client_request_id": client_request_id,
                    "target_branch": branch,
                    "expected_target_head": oid,
                }),
            ),
        )
        .await;
        let (status, _, body) = support::json(response).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["code"], "INVALID_REQUEST");
    }
    assert!(delivery.calls().is_empty());
}

#[tokio::test]
async fn legacy_router_exposes_delivery_contract_but_fails_closed_without_an_injected_port() {
    let ports = support::Ports::new();
    let task_id = ports.backend.task().id;
    let router = build_api_router(
        ports.backend.clone(),
        ports.security.clone(),
        ports.sse.clone(),
    );
    let response = support::send(
        router,
        support::read_request(Method::GET, &format!("/api/tasks/{task_id}/delivery")),
    )
    .await;
    let (status, _, body) = support::json(response).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "STORE_DEGRADED");
}

#[test]
fn delivery_error_matrix_is_stable_and_redacted() {
    let cases = [
        (
            DeliveryApiErrorKind::TaskNotMergeEligible,
            "TASK_NOT_MERGE_ELIGIBLE",
            409,
            false,
        ),
        (
            DeliveryApiErrorKind::DeliveryEvidenceStale,
            "DELIVERY_EVIDENCE_STALE",
            409,
            false,
        ),
        (
            DeliveryApiErrorKind::DeliverySourceChanged,
            "DELIVERY_SOURCE_CHANGED",
            409,
            false,
        ),
        (
            DeliveryApiErrorKind::DeliverySourceInconsistent,
            "DELIVERY_SOURCE_INCONSISTENT",
            503,
            false,
        ),
        (
            DeliveryApiErrorKind::DeliveryPreflightStale,
            "DELIVERY_PREFLIGHT_STALE",
            409,
            false,
        ),
        (
            DeliveryApiErrorKind::DeliveryOperationInProgress,
            "DELIVERY_OPERATION_IN_PROGRESS",
            409,
            true,
        ),
        (
            DeliveryApiErrorKind::TargetBranchDetached,
            "TARGET_BRANCH_DETACHED",
            409,
            false,
        ),
        (
            DeliveryApiErrorKind::TargetBranchMismatch,
            "TARGET_BRANCH_MISMATCH",
            409,
            false,
        ),
        (
            DeliveryApiErrorKind::TargetHeadChanged,
            "TARGET_HEAD_CHANGED",
            409,
            false,
        ),
        (
            DeliveryApiErrorKind::TargetWorktreeDirty,
            "TARGET_WORKTREE_DIRTY",
            409,
            false,
        ),
        (
            DeliveryApiErrorKind::TargetIgnoredPathCollision,
            "TARGET_IGNORED_PATH_COLLISION",
            409,
            false,
        ),
        (
            DeliveryApiErrorKind::TargetGitOperationInProgress,
            "TARGET_GIT_OPERATION_IN_PROGRESS",
            409,
            true,
        ),
        (
            DeliveryApiErrorKind::UnsafeGitConfiguration,
            "UNSAFE_GIT_CONFIGURATION",
            409,
            false,
        ),
        (
            DeliveryApiErrorKind::UnsupportedGitAttributes,
            "UNSUPPORTED_GIT_ATTRIBUTES",
            409,
            false,
        ),
        (
            DeliveryApiErrorKind::MergeConflict,
            "MERGE_CONFLICT",
            409,
            false,
        ),
        (
            DeliveryApiErrorKind::SourceAlreadyInTarget,
            "SOURCE_ALREADY_IN_TARGET",
            409,
            false,
        ),
        (
            DeliveryApiErrorKind::DeliveryReconciliationRequired,
            "DELIVERY_RECONCILIATION_REQUIRED",
            503,
            false,
        ),
        (
            DeliveryApiErrorKind::ArtifactCleanupNotAllowed,
            "ARTIFACT_CLEANUP_NOT_ALLOWED",
            409,
            false,
        ),
        (
            DeliveryApiErrorKind::ArtifactProcessStillActive,
            "ARTIFACT_PROCESS_STILL_ACTIVE",
            409,
            true,
        ),
        (
            DeliveryApiErrorKind::WorktreeIdentityMismatch,
            "WORKTREE_IDENTITY_MISMATCH",
            409,
            false,
        ),
        (
            DeliveryApiErrorKind::SourceBranchNotMerged,
            "SOURCE_BRANCH_NOT_MERGED",
            409,
            false,
        ),
        (
            DeliveryApiErrorKind::IdempotencyConflict,
            "IDEMPOTENCY_CONFLICT",
            409,
            false,
        ),
        (
            DeliveryApiErrorKind::RepositoryControlBusy,
            "REPOSITORY_CONTROL_BUSY",
            503,
            true,
        ),
        (
            DeliveryApiErrorKind::RepositoryControlPoisoned,
            "REPOSITORY_CONTROL_POISONED",
            503,
            false,
        ),
        (
            DeliveryApiErrorKind::CommandTimedOut,
            "COMMAND_TIMED_OUT",
            504,
            true,
        ),
        (
            DeliveryApiErrorKind::ProcessTreeCleanupFailed,
            "PROCESS_TREE_CLEANUP_FAILED",
            503,
            false,
        ),
    ];
    for (kind, code, status, retryable) in cases {
        let error = ApiError::delivery(kind);
        assert_eq!(error.code, code);
        assert_eq!(error.status.as_u16(), status);
        assert_eq!(error.retryable, retryable);
        let response = ApiErrorResponse {
            code: error.code,
            message: error.message,
            retryable: error.retryable,
            request_id: "delivery-test-request".to_owned(),
            details: error.details,
        };
        let serialized = serde_json::to_string(&response).expect("serialize public error response");
        for forbidden in ["prompt", "diff", "stderr", "C:\\", "/tmp/"] {
            assert!(!serialized.contains(forbidden), "forbidden={forbidden}");
        }
        let message_and_details = format!("{} {:?}", response.message, response.details);
        for forbidden in ["GIT_", "SSH_", "TOKEN", "SECRET", "PASSWORD"] {
            assert!(
                !message_and_details.contains(forbidden),
                "stable codes may name Git, but message/details must not expose {forbidden} values"
            );
        }
    }
}

#[test]
fn wire_validation_locks_uuid_oid_ref_fingerprint_version_and_generation_bounds() {
    let task_id = TaskId::new();
    let max_branch = format!("refs/heads/{}", "a".repeat(4_085));
    for (branch, oid) in [
        ("refs/heads/main".to_owned(), SHA1_A.to_owned()),
        ("refs/heads/main".to_owned(), "b".repeat(64)),
        (max_branch.clone(), SHA1_A.to_owned()),
    ] {
        assert!(
            ValidatedDeliveryPreflightCommand::try_new(
                task_id,
                DeliveryPreflightRequest {
                    client_request_id: CLIENT_ID.to_owned(),
                    target_branch: branch,
                    expected_target_head: oid,
                },
            )
            .is_ok()
        );
    }

    for (client_request_id, branch, oid) in [
        (
            CLIENT_ID.to_uppercase(),
            "refs/heads/main".to_owned(),
            SHA1_A.to_owned(),
        ),
        (
            CLIENT_ID.to_owned(),
            format!("refs/heads/{}", "a".repeat(4_086)),
            SHA1_A.to_owned(),
        ),
        (
            CLIENT_ID.to_owned(),
            "refs/heads/main".to_owned(),
            "0".repeat(40),
        ),
        (
            CLIENT_ID.to_owned(),
            "refs/heads/main".to_owned(),
            "b".repeat(41),
        ),
    ] {
        assert_eq!(
            ValidatedDeliveryPreflightCommand::try_new(
                task_id,
                DeliveryPreflightRequest {
                    client_request_id,
                    target_branch: branch,
                    expected_target_head: oid,
                },
            )
            .unwrap_err()
            .status,
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }

    assert!(
        ValidatedDeliveryMergeCommand::try_new(
            task_id,
            DeliveryMergeRequest {
                client_request_id: CLIENT_ID.to_owned(),
                preflight_operation_id: OPERATION_ID.to_owned(),
                expected_operation_version: 9_007_199_254_740_991,
                expected_review_generation: 9_007_199_254_740_991,
                expected_workspace_fingerprint: FINGERPRINT.to_owned(),
                target_branch: "refs/heads/main".to_owned(),
                expected_target_head: "b".repeat(64),
            },
        )
        .is_ok()
    );
    for (version, generation, fingerprint) in [
        (0, 0, FINGERPRINT.to_owned()),
        (9_007_199_254_740_992, 0, FINGERPRINT.to_owned()),
        (1, 9_007_199_254_740_992, FINGERPRINT.to_owned()),
        (1, 0, "A".repeat(64)),
    ] {
        assert_eq!(
            ValidatedDeliveryMergeCommand::try_new(
                task_id,
                DeliveryMergeRequest {
                    client_request_id: CLIENT_ID.to_owned(),
                    preflight_operation_id: OPERATION_ID.to_owned(),
                    expected_operation_version: version,
                    expected_review_generation: generation,
                    expected_workspace_fingerprint: fingerprint,
                    target_branch: "refs/heads/main".to_owned(),
                    expected_target_head: SHA1_A.to_owned(),
                },
            )
            .unwrap_err()
            .status,
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }

    assert_eq!(
        ValidatedDeliveryDeleteBranchCommand::try_new(
            task_id,
            DeliveryDeleteBranchRequest {
                client_request_id: CLIENT_ID.to_owned(),
                expected_disposition_version: 1,
                expected_merge_operation_id: MERGE_ID.to_owned(),
                expected_source_ref: "refs/heads/coding-agent/task".to_owned(),
                expected_source_oid: SHA1_B.to_owned(),
                target_branch: "refs/heads/main".to_owned(),
                target_head: "b".repeat(64),
            },
        )
        .unwrap_err()
        .status,
        StatusCode::UNPROCESSABLE_ENTITY
    );
}

fn operation_id() -> Uuid {
    Uuid::parse_str(OPERATION_ID).unwrap()
}

fn merge_operation(operation_id: Uuid) -> DeliveryOperationDto {
    DeliveryOperationDto::Merge(DeliveryMergeOperationDto {
        operation_id,
        version: 2,
        state: DeliveryMergeStateDto::PreflightReady,
        review_generation: 7,
        workspace_fingerprint: FINGERPRINT.to_owned(),
        candidate_source_tree: Some(SHA1_B.to_owned()),
        preflight_source_commit: Some(SHA1_B.to_owned()),
        source_commit: None,
        target_branch: "refs/heads/main".to_owned(),
        target_head: SHA1_A.to_owned(),
        conflicts: None,
        failure: None,
    })
}

fn cleanup_operation(kind: DeliveryCleanupKindDto) -> DeliveryOperationDto {
    DeliveryOperationDto::Cleanup(cleanup_operation_dto(kind))
}

fn cleanup_operation_dto(kind: DeliveryCleanupKindDto) -> DeliveryCleanupOperationDto {
    DeliveryCleanupOperationDto {
        operation_id: operation_id(),
        cleanup_kind: kind,
        version: 1,
        state: match kind {
            DeliveryCleanupKindDto::RemoveWorktree => DeliveryCleanupStateDto::UnlockPending,
            DeliveryCleanupKindDto::DeleteBranch => DeliveryCleanupStateDto::DeletePending,
        },
        expected_disposition_version: 1,
        expected_merge_operation_id: Uuid::parse_str(MERGE_ID).unwrap(),
        expected_source_ref: "refs/heads/coding-agent/task".to_owned(),
        expected_source_oid: SHA1_B.to_owned(),
        target_branch: (kind == DeliveryCleanupKindDto::DeleteBranch)
            .then(|| "refs/heads/main".to_owned()),
        target_head: (kind == DeliveryCleanupKindDto::DeleteBranch).then(|| SHA1_A.to_owned()),
        failure: None,
    }
}
