#![cfg(feature = "test-support")]

#[allow(dead_code, unused_imports)]
mod delivery_merge_support;
mod support;

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use coding_agent_api::{RequestSecurity, SessionExchange};
use coding_agent_app::{
    ApplicationBackend, DeliveryAcceptAuthenticationError, DeliveryCommandConflict,
    DeliveryEligibilityReason, DeliveryManagerHandle, DeliveryPreflightBusyReason,
    DeliveryPreflightUnavailableReason, MutationGate, RepositoryDiscovery,
    build_application_api_router_with_delivery, map_delivery_busy_for_test,
    map_delivery_cleanup_eligibility_for_test, map_delivery_command_conflict_for_test,
    map_delivery_eligibility_for_test, map_delivery_unavailable_for_test,
};
use coding_agent_domain::ClientRequestId;
use coding_agent_store::{
    AcceptMergeCommandRequest, DeliveryCommand, DeliveryCommandLookup, MergeOperationState,
    PreflightRejectedReason, PreflightStaleReason,
};
use http::header::{CONTENT_TYPE, COOKIE, HOST, ORIGIN};
use http::{Method, Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{Value, json};
use tower::ServiceExt as _;

use delivery_merge_support::{DeliveryMergeFixture, LiveFault, LiveStage};

const CLIENT_REQUEST_ID: &str = "8f26bbff-268d-42a8-923e-6329dc02efc6";
const MERGE_OPERATION_ID: &str = "a32eaeb7-cbe2-4d60-b250-464c6cb9f27b";
const SHA1_A: &str = "1111111111111111111111111111111111111111";
const SHA1_B: &str = "2222222222222222222222222222222222222222";
const FINGERPRINT: &str = "3333333333333333333333333333333333333333333333333333333333333333";

#[test]
fn typed_manager_conflicts_have_exact_stable_http_contracts() {
    let cases = [
        (
            DeliveryCommandConflict::IdempotencyConflict,
            "IDEMPOTENCY_CONFLICT",
            false,
        ),
        (
            DeliveryCommandConflict::EvidenceStale,
            "DELIVERY_EVIDENCE_STALE",
            false,
        ),
        (
            DeliveryCommandConflict::SourceChanged,
            "DELIVERY_SOURCE_CHANGED",
            false,
        ),
        (
            DeliveryCommandConflict::PreflightStale,
            "DELIVERY_PREFLIGHT_STALE",
            false,
        ),
        (
            DeliveryCommandConflict::OperationInProgress,
            "DELIVERY_OPERATION_IN_PROGRESS",
            true,
        ),
        (
            DeliveryCommandConflict::TargetBranchMismatch,
            "TARGET_BRANCH_MISMATCH",
            false,
        ),
        (
            DeliveryCommandConflict::TargetHeadChanged,
            "TARGET_HEAD_CHANGED",
            false,
        ),
        (
            DeliveryCommandConflict::MergeConflict,
            "MERGE_CONFLICT",
            false,
        ),
        (
            DeliveryCommandConflict::ArtifactCleanupNotAllowed,
            "ARTIFACT_CLEANUP_NOT_ALLOWED",
            false,
        ),
        (
            DeliveryCommandConflict::ArtifactProcessStillActive,
            "ARTIFACT_PROCESS_STILL_ACTIVE",
            true,
        ),
        (
            DeliveryCommandConflict::WorktreeIdentityMismatch,
            "WORKTREE_IDENTITY_MISMATCH",
            false,
        ),
        (
            DeliveryCommandConflict::SourceBranchNotMerged,
            "SOURCE_BRANCH_NOT_MERGED",
            false,
        ),
    ];

    for (conflict, code, retryable) in cases {
        assert_error_contract(
            map_delivery_command_conflict_for_test(conflict),
            StatusCode::CONFLICT,
            code,
            retryable,
            conflict,
        );
    }
}

#[test]
fn typed_manager_non_conflict_outcomes_have_exact_stable_http_contracts() {
    let eligibility = [
        (
            DeliveryEligibilityReason::TaskNotFound,
            StatusCode::NOT_FOUND,
            "TASK_NOT_FOUND",
            false,
        ),
        (
            DeliveryEligibilityReason::TaskNotCompleted,
            StatusCode::CONFLICT,
            "TASK_NOT_MERGE_ELIGIBLE",
            false,
        ),
        (
            DeliveryEligibilityReason::ReviewNotApproved,
            StatusCode::CONFLICT,
            "TASK_NOT_MERGE_ELIGIBLE",
            false,
        ),
        (
            DeliveryEligibilityReason::ApprovedEvidenceMissing,
            StatusCode::CONFLICT,
            "TASK_NOT_MERGE_ELIGIBLE",
            false,
        ),
        (
            DeliveryEligibilityReason::AttemptArtifactMissing,
            StatusCode::CONFLICT,
            "TASK_NOT_MERGE_ELIGIBLE",
            false,
        ),
        (
            DeliveryEligibilityReason::AttemptArtifactNotReady,
            StatusCode::CONFLICT,
            "TASK_NOT_MERGE_ELIGIBLE",
            false,
        ),
        (
            DeliveryEligibilityReason::TaskActive,
            StatusCode::CONFLICT,
            "DELIVERY_OPERATION_IN_PROGRESS",
            true,
        ),
        (
            DeliveryEligibilityReason::ProcessCleanupUnproven,
            StatusCode::SERVICE_UNAVAILABLE,
            "PROCESS_TREE_CLEANUP_FAILED",
            false,
        ),
        (
            DeliveryEligibilityReason::TargetBranchDetached,
            StatusCode::CONFLICT,
            "TARGET_BRANCH_DETACHED",
            false,
        ),
        (
            DeliveryEligibilityReason::TargetBranchMismatch,
            StatusCode::CONFLICT,
            "TARGET_BRANCH_MISMATCH",
            false,
        ),
        (
            DeliveryEligibilityReason::TargetWorktreeDirty,
            StatusCode::CONFLICT,
            "TARGET_WORKTREE_DIRTY",
            false,
        ),
        (
            DeliveryEligibilityReason::TargetIgnoredPathCollision,
            StatusCode::CONFLICT,
            "TARGET_IGNORED_PATH_COLLISION",
            false,
        ),
        (
            DeliveryEligibilityReason::TargetGitOperationInProgress,
            StatusCode::CONFLICT,
            "TARGET_GIT_OPERATION_IN_PROGRESS",
            true,
        ),
        (
            DeliveryEligibilityReason::UnsafeGitConfiguration,
            StatusCode::CONFLICT,
            "UNSAFE_GIT_CONFIGURATION",
            false,
        ),
        (
            DeliveryEligibilityReason::UnsupportedGitAttributes,
            StatusCode::CONFLICT,
            "UNSUPPORTED_GIT_ATTRIBUTES",
            false,
        ),
        (
            DeliveryEligibilityReason::SourceAlreadyInTarget,
            StatusCode::CONFLICT,
            "SOURCE_ALREADY_IN_TARGET",
            false,
        ),
        (
            DeliveryEligibilityReason::RuntimeDrift,
            StatusCode::CONFLICT,
            "DELIVERY_EVIDENCE_STALE",
            false,
        ),
        (
            DeliveryEligibilityReason::DeliveryOwned,
            StatusCode::CONFLICT,
            "DELIVERY_OPERATION_IN_PROGRESS",
            true,
        ),
        (
            DeliveryEligibilityReason::AlreadyMerged,
            StatusCode::CONFLICT,
            "SOURCE_ALREADY_IN_TARGET",
            false,
        ),
        (
            DeliveryEligibilityReason::ReconciliationRequired,
            StatusCode::SERVICE_UNAVAILABLE,
            "DELIVERY_RECONCILIATION_REQUIRED",
            false,
        ),
        (
            DeliveryEligibilityReason::RepositoryBusy,
            StatusCode::SERVICE_UNAVAILABLE,
            "REPOSITORY_CONTROL_BUSY",
            true,
        ),
        (
            DeliveryEligibilityReason::RepositoryUnavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "REPOSITORY_CONTROL_POISONED",
            false,
        ),
        (
            DeliveryEligibilityReason::StoreUnavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "STORE_DEGRADED",
            true,
        ),
        (
            DeliveryEligibilityReason::RuntimeObservationUnavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "STORE_DEGRADED",
            true,
        ),
        (
            DeliveryEligibilityReason::ServiceNotReady,
            StatusCode::SERVICE_UNAVAILABLE,
            "STORE_DEGRADED",
            true,
        ),
    ];
    for (reason, status, code, retryable) in eligibility {
        assert_error_contract(
            map_delivery_eligibility_for_test(reason),
            status,
            code,
            retryable,
            reason,
        );
    }

    let cleanup_context = [
        (
            DeliveryEligibilityReason::AttemptArtifactMissing,
            "ARTIFACT_CLEANUP_NOT_ALLOWED",
            false,
        ),
        (
            DeliveryEligibilityReason::AlreadyMerged,
            "ARTIFACT_CLEANUP_NOT_ALLOWED",
            false,
        ),
        (
            DeliveryEligibilityReason::TaskActive,
            "ARTIFACT_PROCESS_STILL_ACTIVE",
            true,
        ),
        (
            DeliveryEligibilityReason::RuntimeDrift,
            "WORKTREE_IDENTITY_MISMATCH",
            false,
        ),
    ];
    for (reason, code, retryable) in cleanup_context {
        assert_error_contract(
            map_delivery_cleanup_eligibility_for_test(reason),
            StatusCode::CONFLICT,
            code,
            retryable,
            reason,
        );
    }

    for reason in [
        DeliveryPreflightBusyReason::RepositoryBusy,
        DeliveryPreflightBusyReason::WorkerQueueFull,
    ] {
        assert_error_contract(
            map_delivery_busy_for_test(reason),
            StatusCode::SERVICE_UNAVAILABLE,
            "REPOSITORY_CONTROL_BUSY",
            true,
            reason,
        );
    }

    let unavailable = [
        (
            DeliveryPreflightUnavailableReason::ManagerQuiescing,
            StatusCode::SERVICE_UNAVAILABLE,
            "STORE_DEGRADED",
            true,
        ),
        (
            DeliveryPreflightUnavailableReason::ServiceNotReady,
            StatusCode::SERVICE_UNAVAILABLE,
            "STORE_DEGRADED",
            true,
        ),
        (
            DeliveryPreflightUnavailableReason::RepositoryControlUnavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "REPOSITORY_CONTROL_POISONED",
            false,
        ),
        (
            DeliveryPreflightUnavailableReason::StoreUnavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "STORE_DEGRADED",
            true,
        ),
        (
            DeliveryPreflightUnavailableReason::RuntimeUnavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "DELIVERY_RECONCILIATION_REQUIRED",
            false,
        ),
        (
            DeliveryPreflightUnavailableReason::SourceInconsistent,
            StatusCode::SERVICE_UNAVAILABLE,
            "DELIVERY_SOURCE_INCONSISTENT",
            false,
        ),
        (
            DeliveryPreflightUnavailableReason::ProcessProofUnavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "PROCESS_TREE_CLEANUP_FAILED",
            false,
        ),
        (
            DeliveryPreflightUnavailableReason::CommandTimedOut,
            StatusCode::GATEWAY_TIMEOUT,
            "COMMAND_TIMED_OUT",
            true,
        ),
        (
            DeliveryPreflightUnavailableReason::OutcomeUnknown,
            StatusCode::SERVICE_UNAVAILABLE,
            "REPOSITORY_CONTROL_POISONED",
            false,
        ),
        (
            DeliveryPreflightUnavailableReason::OrchestrationUnavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            "STORE_DEGRADED",
            true,
        ),
    ];
    for (reason, status, code, retryable) in unavailable {
        assert_error_contract(
            map_delivery_unavailable_for_test(reason),
            status,
            code,
            retryable,
            reason,
        );
    }
}

#[tokio::test]
async fn fresh_accept_authentication_rejections_have_exact_http_and_no_accept_side_effects() {
    let delivery = DeliveryMergeFixture::new(None).await;
    let prepared = delivery.prepare_accept().await;
    let (router, backend, application, security, session) =
        fixture_with_delivery_manager(Some(delivery.manager().clone())).await;
    let cases = [
        (
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::TaskNotMergeEligible,
            ),
            StatusCode::CONFLICT,
            "TASK_NOT_MERGE_ELIGIBLE",
            false,
        ),
        (
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::TargetBranchDetached,
            ),
            StatusCode::CONFLICT,
            "TARGET_BRANCH_DETACHED",
            false,
        ),
        (
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::TargetBranchMismatch,
            ),
            StatusCode::CONFLICT,
            "TARGET_BRANCH_MISMATCH",
            false,
        ),
        (
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::TargetWorktreeDirty,
            ),
            StatusCode::CONFLICT,
            "TARGET_WORKTREE_DIRTY",
            false,
        ),
        (
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::TargetIgnoredPathCollision,
            ),
            StatusCode::CONFLICT,
            "TARGET_IGNORED_PATH_COLLISION",
            false,
        ),
        (
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::TargetGitOperationInProgress,
            ),
            StatusCode::CONFLICT,
            "TARGET_GIT_OPERATION_IN_PROGRESS",
            true,
        ),
        (
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::UnsafeGitConfiguration,
            ),
            StatusCode::CONFLICT,
            "UNSAFE_GIT_CONFIGURATION",
            false,
        ),
        (
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::UnsupportedGitAttributes,
            ),
            StatusCode::CONFLICT,
            "UNSUPPORTED_GIT_ATTRIBUTES",
            false,
        ),
        (
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::SourceAlreadyInTarget,
            ),
            StatusCode::CONFLICT,
            "SOURCE_ALREADY_IN_TARGET",
            false,
        ),
        (
            DeliveryAcceptAuthenticationError::MergeConflict,
            StatusCode::CONFLICT,
            "MERGE_CONFLICT",
            false,
        ),
        (
            DeliveryAcceptAuthenticationError::CommandTimedOut,
            StatusCode::GATEWAY_TIMEOUT,
            "COMMAND_TIMED_OUT",
            true,
        ),
    ];

    for (runtime_error, status, code, retryable) in cases {
        let command = rebuild_accept_command(&prepared.command);
        delivery.live_runtime.fail_once(
            LiveStage::AuthenticateAccept,
            LiveFault::Accept(runtime_error),
        );
        let response = router
            .clone()
            .oneshot(authenticated_post(
                &format!("/api/tasks/{}/merge", prepared.task.id),
                accept_body(&command),
                &security,
                &session,
            ))
            .await
            .expect("serve rejected accept command");
        assert_delivery_error(response, status, code, retryable).await;
        delivery
            .wait_repository_state(coding_agent_app::RepositoryControlState::Available)
            .await;
        assert!(matches!(
            delivery
                .base
                .store
                .lookup_delivery_command(&DeliveryCommand::AcceptMerge(command))
                .await
                .expect("lookup rejected HTTP accept command"),
            DeliveryCommandLookup::Missing
        ));
        let operation = delivery.operation(prepared.operation_id).await;
        assert_eq!(operation.state, MergeOperationState::PreflightReady);
        assert!(operation.accept_receipt_id.is_none());
        assert!(operation.delivery_source_task_id.is_none());
        assert!(delivery.source(prepared.task.id).await.is_none());
    }
    drop(router);
    drop(backend);
    drop(application);
    drop(security);
    drop(session);
    delivery.finish().await;
}

#[tokio::test]
async fn fresh_stale_accept_authentication_terminalizes_before_exact_http_conflict() {
    for (reason, code) in [
        (
            PreflightStaleReason::EvidenceStale,
            "DELIVERY_EVIDENCE_STALE",
        ),
        (
            PreflightStaleReason::TargetBranchChanged,
            "TARGET_BRANCH_MISMATCH",
        ),
        (
            PreflightStaleReason::TargetHeadChanged,
            "TARGET_HEAD_CHANGED",
        ),
        (
            PreflightStaleReason::SourceChanged,
            "DELIVERY_SOURCE_CHANGED",
        ),
    ] {
        assert_fresh_stale_accept_http(reason, code).await;
    }
}

async fn assert_fresh_stale_accept_http(reason: PreflightStaleReason, code: &'static str) {
    let delivery = DeliveryMergeFixture::new(None).await;
    let prepared = delivery.prepare_accept().await;
    let (router, backend, application, security, session) =
        fixture_with_delivery_manager(Some(delivery.manager().clone())).await;
    let command = rebuild_accept_command(&prepared.command);
    delivery.live_runtime.fail_once(
        LiveStage::AuthenticateAccept,
        LiveFault::Accept(DeliveryAcceptAuthenticationError::Stale(reason)),
    );

    let response = router
        .clone()
        .oneshot(authenticated_post(
            &format!("/api/tasks/{}/merge", prepared.task.id),
            accept_body(&command),
            &security,
            &session,
        ))
        .await
        .expect("serve stale accept command");
    assert_delivery_error(response, StatusCode::CONFLICT, code, false).await;
    delivery
        .wait_repository_state(coding_agent_app::RepositoryControlState::Available)
        .await;
    assert!(matches!(
        delivery
            .base
            .store
            .lookup_delivery_command(&DeliveryCommand::AcceptMerge(command))
            .await
            .expect("lookup stale HTTP accept command"),
        DeliveryCommandLookup::Missing
    ));
    let operation = delivery.operation(prepared.operation_id).await;
    assert_eq!(operation.state, MergeOperationState::Stale);
    assert_eq!(
        operation.version,
        prepared
            .command
            .expected_operation_version()
            .next()
            .expect("ready operation version has a successor")
    );
    assert_eq!(
        operation
            .failure_code
            .as_ref()
            .map(|failure| failure.as_str()),
        Some(reason.as_failure_code())
    );
    assert!(operation.accept_receipt_id.is_none());
    assert!(operation.delivery_source_task_id.is_none());
    assert!(delivery.source(prepared.task.id).await.is_none());

    drop(router);
    drop(backend);
    drop(application);
    drop(security);
    drop(session);
    delivery.finish().await;
}

fn rebuild_accept_command(original: &AcceptMergeCommandRequest) -> AcceptMergeCommandRequest {
    AcceptMergeCommandRequest::try_new(
        ClientRequestId::new(),
        original.task_id(),
        original.preflight_operation_id(),
        original.expected_operation_version(),
        original.expected_review_generation(),
        original.expected_workspace_fingerprint().clone(),
        original.target_branch().clone(),
        original.expected_target_head().clone(),
    )
    .expect("valid rebuilt HTTP accept command")
}

fn accept_body(command: &AcceptMergeCommandRequest) -> Value {
    json!({
        "client_request_id": command.client_request_id().to_string(),
        "preflight_operation_id": command.preflight_operation_id().to_string(),
        "expected_operation_version": command.expected_operation_version().get(),
        "expected_review_generation": command.expected_review_generation(),
        "expected_workspace_fingerprint": command.expected_workspace_fingerprint().as_str(),
        "target_branch": command.target_branch().as_str(),
        "expected_target_head": command.expected_target_head().as_str(),
    })
}

fn assert_error_contract(
    error: coding_agent_api::ApiError,
    status: StatusCode,
    code: &str,
    retryable: bool,
    source: impl std::fmt::Debug,
) {
    assert_eq!(error.status, status, "{source:?}");
    assert_eq!(error.code, code, "{source:?}");
    assert_eq!(error.retryable, retryable, "{source:?}");
    assert!(error.details.is_empty(), "{source:?}");
    for forbidden in ["prompt", "stderr", "C:\\", "\\\\"] {
        assert!(!error.message.contains(forbidden), "{source:?}");
    }
}

struct BrowserSession {
    cookie: String,
    csrf: String,
}

async fn fixture() -> (
    axum::Router,
    support::TaskManagerFixture,
    support::SecurityFixture,
    BrowserSession,
) {
    let (router, _backend, application, security, session) =
        fixture_with_delivery_manager(None).await;
    (router, application, security, session)
}

async fn fixture_with_delivery_manager(
    delivery_manager: Option<DeliveryManagerHandle>,
) -> (
    axum::Router,
    Arc<ApplicationBackend>,
    support::TaskManagerFixture,
    support::SecurityFixture,
    BrowserSession,
) {
    let application = support::task_manager_fixture(1).await;
    let security = support::SecurityFixture::production();
    let session = establish_session(&security).await;
    let gate = MutationGate::new(application.state.clone());
    let backend = Arc::new(ApplicationBackend::new_without_repository_runtime_for_test(
        application.store.clone(),
        application.writer.clone(),
        application.dispatcher.clone(),
        application.manager.clone(),
        RepositoryDiscovery::new_without_commands_for_test(std::env::temp_dir()),
        None,
        security.manager.clone(),
        application.state.clone(),
        gate,
        support::timestamp(),
        1,
        NonZeroU32::new(256).expect("positive queue limit"),
        Duration::from_secs(2),
        Arc::new(|| {}),
    ));
    let delivery_manager = match delivery_manager {
        Some(delivery_manager) => delivery_manager,
        None => {
            let (repository_control, _) =
                support::repository_control_fixture(&application.store).await;
            DeliveryManagerHandle::spawn_unavailable(
                repository_control,
                application.state.clone(),
                8,
            )
        }
    };
    let router = build_application_api_router_with_delivery(
        backend.clone(),
        Arc::new(security.manager.clone()),
        delivery_manager,
    );
    (router, backend, application, security, session)
}

#[tokio::test]
async fn production_router_injects_the_delivery_manager_for_all_six_routes() {
    let (router, application, security, session) = fixture().await;
    let task_id = application.repository.id;
    let task_id = application
        .store
        .create_task(support::new_task(task_id, "delivery HTTP fixture"))
        .await
        .expect("create delivery HTTP task")
        .task()
        .id;

    for uri in [
        format!("/api/tasks/{task_id}/delivery"),
        format!("/api/delivery-operations/{MERGE_OPERATION_ID}"),
    ] {
        let response = router
            .clone()
            .oneshot(authenticated_get(&uri, &security, &session))
            .await
            .expect("serve delivery GET");
        assert_error(response, StatusCode::SERVICE_UNAVAILABLE, "STORE_DEGRADED").await;
    }

    let commands = [
        (
            format!("/api/tasks/{task_id}/merge/preflight"),
            json!({
                "client_request_id": CLIENT_REQUEST_ID,
                "target_branch": "refs/heads/main",
                "expected_target_head": SHA1_A,
            }),
        ),
        (
            format!("/api/tasks/{task_id}/merge"),
            json!({
                "client_request_id": CLIENT_REQUEST_ID,
                "preflight_operation_id": MERGE_OPERATION_ID,
                "expected_operation_version": 3,
                "expected_review_generation": 1,
                "expected_workspace_fingerprint": FINGERPRINT,
                "target_branch": "refs/heads/main",
                "expected_target_head": SHA1_A,
            }),
        ),
        (
            format!("/api/tasks/{task_id}/cleanup/worktree"),
            json!({
                "client_request_id": CLIENT_REQUEST_ID,
                "expected_disposition_version": 1,
                "expected_merge_operation_id": MERGE_OPERATION_ID,
                "expected_source_ref": "refs/heads/coding-agent/task",
                "expected_source_oid": SHA1_B,
            }),
        ),
        (
            format!("/api/tasks/{task_id}/cleanup/branch"),
            json!({
                "client_request_id": CLIENT_REQUEST_ID,
                "expected_disposition_version": 2,
                "expected_merge_operation_id": MERGE_OPERATION_ID,
                "expected_source_ref": "refs/heads/coding-agent/task",
                "expected_source_oid": SHA1_B,
                "target_branch": "refs/heads/main",
                "target_head": SHA1_A,
            }),
        ),
    ];
    for (uri, body) in commands {
        let response = router
            .clone()
            .oneshot(authenticated_post(&uri, body, &security, &session))
            .await
            .expect("serve delivery POST");
        assert_error(response, StatusCode::SERVICE_UNAVAILABLE, "STORE_DEGRADED").await;
    }
}

#[tokio::test]
async fn delivery_routes_reuse_session_origin_csrf_and_mutation_gate_security() {
    let (router, application, security, session) = fixture().await;
    let task_id = application
        .store
        .create_task(support::new_task(
            application.repository.id,
            "delivery security fixture",
        ))
        .await
        .expect("create delivery security task")
        .task()
        .id;
    let get_uri = format!("/api/tasks/{task_id}/delivery");
    let post_uri = format!("/api/tasks/{task_id}/merge/preflight");
    let body = json!({
        "client_request_id": CLIENT_REQUEST_ID,
        "target_branch": "refs/heads/main",
        "expected_target_head": SHA1_A,
    });

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(&get_uri)
                .header(HOST, &security.expected_host)
                .body(Body::empty())
                .expect("build unauthenticated GET"),
        )
        .await
        .expect("serve unauthenticated GET");
    assert_error(
        response,
        StatusCode::UNAUTHORIZED,
        "SECURITY_INVALID_SESSION",
    )
    .await;

    for missing in [ORIGIN, http::HeaderName::from_static("x-csrf-token")] {
        let mut request = authenticated_post(&post_uri, body.clone(), &security, &session);
        request.headers_mut().remove(missing);
        let response = router
            .clone()
            .oneshot(request)
            .await
            .expect("serve rejected delivery mutation");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    assert!(
        application
            .state
            .set(coding_agent_app::ServiceState::Quiescing)
            .is_ok()
    );
    let response = router
        .oneshot(authenticated_post(&post_uri, body, &security, &session))
        .await
        .expect("serve quiescing delivery mutation");
    assert_error(
        response,
        StatusCode::SERVICE_UNAVAILABLE,
        "APP_SHUTTING_DOWN",
    )
    .await;
}

async fn establish_session(fixture: &support::SecurityFixture) -> BrowserSession {
    let parts = Request::builder()
        .method(Method::POST)
        .uri("/api/session/exchange")
        .header(HOST, &fixture.expected_host)
        .header(ORIGIN, &fixture.public_origin)
        .body(())
        .expect("build session exchange")
        .into_parts()
        .0;
    let exchange = RequestSecurity::exchange(
        &fixture.manager,
        &parts,
        fixture.initial_launch_token.as_str(),
    )
    .await
    .expect("exchange initial launch token");
    session_from_exchange(fixture, exchange)
}

fn session_from_exchange(
    fixture: &support::SecurityFixture,
    exchange: SessionExchange,
) -> BrowserSession {
    let cookie = exchange
        .set_cookie
        .to_str()
        .expect("ASCII cookie")
        .split(';')
        .next()
        .expect("session cookie pair")
        .to_owned();
    let parts = Request::builder()
        .method(Method::GET)
        .uri("/api/bootstrap")
        .header(HOST, &fixture.expected_host)
        .header(COOKIE, &cookie)
        .body(())
        .expect("build session read")
        .into_parts()
        .0;
    let auth = RequestSecurity::authorize_read(&fixture.manager, &parts)
        .expect("authorize established session");
    let csrf = fixture
        .manager
        .csrf_for_auth(&auth)
        .expect("resolve session CSRF");
    BrowserSession { cookie, csrf }
}

fn authenticated_get(
    uri: &str,
    fixture: &support::SecurityFixture,
    session: &BrowserSession,
) -> Request<Body> {
    Request::builder()
        .method(Method::GET)
        .uri(uri)
        .header(HOST, &fixture.expected_host)
        .header(COOKIE, &session.cookie)
        .body(Body::empty())
        .expect("build authenticated delivery GET")
}

fn authenticated_post(
    uri: &str,
    body: Value,
    fixture: &support::SecurityFixture,
    session: &BrowserSession,
) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(HOST, &fixture.expected_host)
        .header(COOKIE, &session.cookie)
        .header(ORIGIN, &fixture.public_origin)
        .header("x-csrf-token", &session.csrf)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("build authenticated delivery POST")
}

async fn assert_error(response: axum::response::Response, status: StatusCode, code: &str) {
    assert_eq!(response.status(), status);
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("collect JSON error")
        .to_bytes();
    let body: Value = serde_json::from_slice(&bytes).expect("decode JSON error");
    assert_eq!(body["code"], code);
    let text = String::from_utf8(bytes.to_vec()).expect("error is UTF-8");
    for forbidden in [
        "delivery HTTP fixture",
        "delivery security fixture",
        "\\",
        ":\\",
    ] {
        assert!(
            !text.contains(forbidden),
            "delivery error leaked {forbidden:?}"
        );
    }
}

async fn assert_delivery_error(
    response: axum::response::Response,
    status: StatusCode,
    code: &str,
    retryable: bool,
) {
    assert_eq!(response.status(), status);
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("collect delivery JSON error")
        .to_bytes();
    let body: Value = serde_json::from_slice(&bytes).expect("decode delivery JSON error");
    assert_eq!(body["code"], code);
    assert_eq!(body["retryable"], retryable);
    assert!(
        body["details"]
            .as_object()
            .is_some_and(|details| details.is_empty())
    );
    let text = String::from_utf8(bytes.to_vec()).expect("delivery error is UTF-8");
    for forbidden in ["prompt", "stderr", "C:\\", "\\\\"] {
        assert!(
            !text.contains(forbidden),
            "delivery error leaked {forbidden:?}"
        );
    }
}
