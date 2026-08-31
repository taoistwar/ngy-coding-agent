#![cfg(feature = "test-support")]

mod delivery_merge_support;
mod support;

use std::str::FromStr;
use std::sync::Arc;

use coding_agent_app::{
    DeliveryAcceptAuthenticationError, DeliveryCommandConflict, DeliveryEligibilityReason,
    DeliveryMergeAcceptanceOutcome, DeliveryMergeReceiptDisposition,
    DeliveryOperationRecoveryOutcome, DeliveryPreflightUnavailableReason, DeliveryProcessProof,
    RepositoryControlPoisonReason, RepositoryControlState, StoreWriterFaultPoint,
    StoreWriterFaultSpec, StoreWriterOperationKind, StoreWriterTestController,
};
use coding_agent_domain::ClientRequestId;
use coding_agent_store::{
    AcceptMergeCommandRequest, DeliveryCommand, DeliveryCommandLookup, DeliverySourceState,
    DeliveryVersion, MarkPreflightStaleOutcome, MarkPreflightStaleRequest, MergeOperationState,
    MergeReconciliationReason, PreflightRejectedReason, PreflightStaleReason,
};
use tokio::time::{Duration, timeout};

use delivery_merge_support::{DeliveryMergeFixture, LiveCall, LiveFault, LiveStage};

#[tokio::test]
async fn fresh_accept_authentication_rejections_write_no_accept_receipt_or_source() {
    let cases = [
        (
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::TaskNotMergeEligible,
            ),
            DeliveryMergeAcceptanceOutcome::Ineligible(vec![
                DeliveryEligibilityReason::TaskNotCompleted,
            ]),
        ),
        (
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::TargetBranchDetached,
            ),
            DeliveryMergeAcceptanceOutcome::Ineligible(vec![
                DeliveryEligibilityReason::TargetBranchDetached,
            ]),
        ),
        (
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::TargetBranchMismatch,
            ),
            DeliveryMergeAcceptanceOutcome::Ineligible(vec![
                DeliveryEligibilityReason::TargetBranchMismatch,
            ]),
        ),
        (
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::TargetWorktreeDirty,
            ),
            DeliveryMergeAcceptanceOutcome::Ineligible(vec![
                DeliveryEligibilityReason::TargetWorktreeDirty,
            ]),
        ),
        (
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::TargetIgnoredPathCollision,
            ),
            DeliveryMergeAcceptanceOutcome::Ineligible(vec![
                DeliveryEligibilityReason::TargetIgnoredPathCollision,
            ]),
        ),
        (
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::TargetGitOperationInProgress,
            ),
            DeliveryMergeAcceptanceOutcome::Ineligible(vec![
                DeliveryEligibilityReason::TargetGitOperationInProgress,
            ]),
        ),
        (
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::UnsafeGitConfiguration,
            ),
            DeliveryMergeAcceptanceOutcome::Ineligible(vec![
                DeliveryEligibilityReason::UnsafeGitConfiguration,
            ]),
        ),
        (
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::UnsupportedGitAttributes,
            ),
            DeliveryMergeAcceptanceOutcome::Ineligible(vec![
                DeliveryEligibilityReason::UnsupportedGitAttributes,
            ]),
        ),
        (
            DeliveryAcceptAuthenticationError::Rejected(
                PreflightRejectedReason::SourceAlreadyInTarget,
            ),
            DeliveryMergeAcceptanceOutcome::Ineligible(vec![
                DeliveryEligibilityReason::SourceAlreadyInTarget,
            ]),
        ),
        (
            DeliveryAcceptAuthenticationError::Stale(PreflightStaleReason::EvidenceStale),
            DeliveryMergeAcceptanceOutcome::Conflict(DeliveryCommandConflict::EvidenceStale),
        ),
        (
            DeliveryAcceptAuthenticationError::Stale(PreflightStaleReason::TargetBranchChanged),
            DeliveryMergeAcceptanceOutcome::Conflict(DeliveryCommandConflict::TargetBranchMismatch),
        ),
        (
            DeliveryAcceptAuthenticationError::Stale(PreflightStaleReason::TargetHeadChanged),
            DeliveryMergeAcceptanceOutcome::Conflict(DeliveryCommandConflict::TargetHeadChanged),
        ),
        (
            DeliveryAcceptAuthenticationError::Stale(PreflightStaleReason::SourceChanged),
            DeliveryMergeAcceptanceOutcome::Conflict(DeliveryCommandConflict::SourceChanged),
        ),
        (
            DeliveryAcceptAuthenticationError::MergeConflict,
            DeliveryMergeAcceptanceOutcome::Conflict(DeliveryCommandConflict::MergeConflict),
        ),
        (
            DeliveryAcceptAuthenticationError::CommandTimedOut,
            DeliveryMergeAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::CommandTimedOut,
            ),
        ),
    ];

    for (error, expected) in cases {
        assert_fresh_accept_rejection(error, expected).await;
    }
}

async fn assert_fresh_accept_rejection(
    error: DeliveryAcceptAuthenticationError,
    expected: DeliveryMergeAcceptanceOutcome,
) {
    let stale_reason = match error {
        DeliveryAcceptAuthenticationError::Stale(reason) => Some(reason),
        _ => None,
    };
    let fixture = DeliveryMergeFixture::new(None).await;
    let prepared = fixture.prepare_accept().await;
    fixture
        .live_runtime
        .fail_once(LiveStage::AuthenticateAccept, LiveFault::Accept(error));

    let outcome = fixture
        .manager()
        .accept_merge(prepared.request())
        .await
        .expect("accept manager remains open");
    assert_eq!(outcome, expected, "{error:?}");
    fixture
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    assert!(matches!(
        fixture
            .base
            .store
            .lookup_delivery_command(&DeliveryCommand::AcceptMerge(prepared.command.clone()))
            .await
            .expect("lookup rejected accept command"),
        DeliveryCommandLookup::Missing
    ));
    let operation = fixture.operation(prepared.operation_id).await;
    if let Some(reason) = stale_reason {
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
            operation.failure_code.as_ref().map(|code| code.as_str()),
            Some(reason.as_failure_code())
        );
    } else {
        assert_eq!(operation.state, MergeOperationState::PreflightReady);
        assert_eq!(
            operation.version,
            prepared.command.expected_operation_version()
        );
        assert!(operation.failure_code.is_none());
    }
    assert!(operation.accept_receipt_id.is_none());
    assert!(operation.delivery_source_task_id.is_none());
    assert!(fixture.source(prepared.task.id).await.is_none());
    assert_eq!(
        fixture.live_runtime.calls(),
        vec![LiveCall::AuthenticateAccept]
    );
    fixture.finish().await;
}

#[tokio::test]
async fn stale_accept_reply_loss_reconciles_the_exact_terminal_write() {
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            operation: Some(StoreWriterOperationKind::MarkMergePreflightStale),
            count: 1,
        }])
        .expect("valid stale reply-loss script"),
    );
    let fixture = DeliveryMergeFixture::new(Some(controller.clone())).await;
    let prepared = fixture.prepare_accept().await;
    fixture.live_runtime.fail_once(
        LiveStage::AuthenticateAccept,
        LiveFault::Accept(DeliveryAcceptAuthenticationError::Stale(
            PreflightStaleReason::TargetHeadChanged,
        )),
    );

    assert_eq!(
        fixture
            .manager()
            .accept_merge(prepared.request())
            .await
            .expect("accept manager remains open"),
        DeliveryMergeAcceptanceOutcome::Conflict(DeliveryCommandConflict::TargetHeadChanged)
    );
    fixture
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            StoreWriterOperationKind::MarkMergePreflightStale,
        ),
        1
    );
    let operation = fixture.operation(prepared.operation_id).await;
    assert_eq!(operation.state, MergeOperationState::Stale);
    assert_eq!(
        operation.failure_code.as_ref().map(|code| code.as_str()),
        Some("TARGET_HEAD_CHANGED")
    );
    assert!(operation.accept_receipt_id.is_none());
    assert!(fixture.source(prepared.task.id).await.is_none());
    fixture.finish().await;
}

#[tokio::test]
async fn unknown_stale_terminal_write_retains_repository_ownership() {
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            operation: Some(StoreWriterOperationKind::MarkMergePreflightStale),
            // One exact-write budget is four submissions. Each typed
            // StoreWriter submission performs the initial write plus its
            // query-first reconciliation after reply loss.
            count: 8,
        }])
        .expect("valid stale unknown-outcome script"),
    );
    let fixture = DeliveryMergeFixture::new(Some(controller.clone())).await;
    let prepared = fixture.prepare_accept().await;
    fixture.live_runtime.fail_once(
        LiveStage::AuthenticateAccept,
        LiveFault::Accept(DeliveryAcceptAuthenticationError::Stale(
            PreflightStaleReason::TargetHeadChanged,
        )),
    );

    assert_eq!(
        fixture
            .manager()
            .accept_merge(prepared.request())
            .await
            .expect("accept manager remains open"),
        DeliveryMergeAcceptanceOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::OutcomeUnknown
        )
    );
    fixture
        .wait_repository_state(RepositoryControlState::Busy)
        .await;
    assert_eq!(
        fixture
            .manager()
            .quiesce()
            .await
            .expect("quiesce stale-unknown manager")
            .in_flight_workers(),
        1
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            StoreWriterOperationKind::MarkMergePreflightStale,
        ),
        8
    );
    let operation = fixture.operation(prepared.operation_id).await;
    assert_eq!(operation.state, MergeOperationState::Stale);
    assert!(operation.accept_receipt_id.is_none());
    assert!(fixture.source(prepared.task.id).await.is_none());
    fixture.finish().await;
}

#[tokio::test]
async fn known_not_applied_stale_terminal_write_retains_repository_ownership() {
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::BusyBeforeExecute,
            operation: Some(StoreWriterOperationKind::MarkMergePreflightStale),
            // Exhaust the StoreWriter initial attempt and five bounded busy
            // retries so the manager observes a proven KnownNotApplied write.
            count: 6,
        }])
        .expect("valid stale known-not-applied script"),
    );
    let fixture = DeliveryMergeFixture::new(Some(controller.clone())).await;
    let prepared = fixture.prepare_accept().await;
    fixture.live_runtime.fail_once(
        LiveStage::AuthenticateAccept,
        LiveFault::Accept(DeliveryAcceptAuthenticationError::Stale(
            PreflightStaleReason::TargetHeadChanged,
        )),
    );

    assert_eq!(
        fixture
            .manager()
            .accept_merge(prepared.request())
            .await
            .expect("accept manager remains open"),
        DeliveryMergeAcceptanceOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::StoreUnavailable
        )
    );
    fixture
        .wait_repository_state(RepositoryControlState::Busy)
        .await;
    assert_eq!(
        fixture
            .manager()
            .quiesce()
            .await
            .expect("quiesce stale-known-not-applied manager")
            .in_flight_workers(),
        1
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::BusyBeforeExecute,
            StoreWriterOperationKind::MarkMergePreflightStale,
        ),
        6
    );
    let operation = fixture.operation(prepared.operation_id).await;
    assert_eq!(operation.state, MergeOperationState::PreflightReady);
    assert!(operation.failure_code.is_none());
    assert!(operation.accept_receipt_id.is_none());
    assert!(fixture.source(prepared.task.id).await.is_none());
    fixture.finish().await;
}

#[tokio::test]
async fn accept_admission_returns_exact_typed_client_conflicts() {
    let evidence_fixture = DeliveryMergeFixture::new(None).await;
    let evidence = evidence_fixture.prepare_accept().await;
    let outcome = evidence_fixture
        .manager()
        .accept_merge(coding_agent_app::DeliveryAcceptRequest::new(
            rebuild_accept_command(
                &evidence.command,
                ClientRequestId::new(),
                evidence.command.expected_operation_version(),
                evidence.command.expected_review_generation() + 1,
            ),
        ))
        .await
        .expect("accept manager remains open");
    assert_eq!(
        outcome,
        DeliveryMergeAcceptanceOutcome::Conflict(DeliveryCommandConflict::EvidenceStale)
    );
    evidence_fixture.finish().await;

    let version_fixture = DeliveryMergeFixture::new(None).await;
    let version = version_fixture.prepare_accept().await;
    let outcome = version_fixture
        .manager()
        .accept_merge(coding_agent_app::DeliveryAcceptRequest::new(
            rebuild_accept_command(
                &version.command,
                ClientRequestId::new(),
                version
                    .command
                    .expected_operation_version()
                    .next()
                    .expect("fixture version has a successor"),
                version.command.expected_review_generation(),
            ),
        ))
        .await
        .expect("accept manager remains open");
    assert_eq!(
        outcome,
        DeliveryMergeAcceptanceOutcome::Conflict(DeliveryCommandConflict::PreflightStale)
    );
    version_fixture.finish().await;

    let state_fixture = DeliveryMergeFixture::new(None).await;
    let state = state_fixture.prepare_accept().await;
    let stale = state_fixture
        .base
        .store
        .mark_merge_preflight_stale(
            MarkPreflightStaleRequest::try_new(
                state.task.id,
                state.operation_id,
                state.command.expected_operation_version(),
                PreflightStaleReason::EvidenceStale,
            )
            .expect("valid stale transition"),
        )
        .await
        .expect("persist stale transition");
    assert!(matches!(stale, MarkPreflightStaleOutcome::Applied { .. }));
    let current = state_fixture.operation(state.operation_id).await;
    let outcome = state_fixture
        .manager()
        .accept_merge(coding_agent_app::DeliveryAcceptRequest::new(
            rebuild_accept_command(
                &state.command,
                ClientRequestId::new(),
                current.version,
                state.command.expected_review_generation(),
            ),
        ))
        .await
        .expect("accept manager remains open");
    assert_eq!(
        outcome,
        DeliveryMergeAcceptanceOutcome::Conflict(DeliveryCommandConflict::PreflightStale)
    );
    state_fixture.finish().await;

    let idempotency_fixture = DeliveryMergeFixture::new(None).await;
    let idempotency = idempotency_fixture.prepare_accept().await;
    idempotency_fixture.accept(&idempotency).await;
    idempotency_fixture
        .wait_operation_state(idempotency.operation_id, MergeOperationState::Merged)
        .await;
    let reused_client_request_id =
        ClientRequestId::from_str(&idempotency.command.client_request_id().to_string())
            .expect("delivery command ID is a valid client request ID");
    let outcome = idempotency_fixture
        .manager()
        .accept_merge(coding_agent_app::DeliveryAcceptRequest::new(
            rebuild_accept_command(
                &idempotency.command,
                reused_client_request_id,
                idempotency.command.expected_operation_version(),
                idempotency.command.expected_review_generation() + 1,
            ),
        ))
        .await
        .expect("accept manager remains open");
    assert_eq!(
        outcome,
        DeliveryMergeAcceptanceOutcome::Conflict(DeliveryCommandConflict::IdempotencyConflict)
    );
    idempotency_fixture.finish().await;
}

fn rebuild_accept_command(
    original: &AcceptMergeCommandRequest,
    client_request_id: ClientRequestId,
    expected_operation_version: DeliveryVersion,
    expected_review_generation: u64,
) -> AcceptMergeCommandRequest {
    AcceptMergeCommandRequest::try_new(
        client_request_id,
        original.task_id(),
        original.preflight_operation_id(),
        expected_operation_version,
        expected_review_generation,
        original.expected_workspace_fingerprint().clone(),
        original.target_branch().clone(),
        original.expected_target_head().clone(),
    )
    .expect("valid rebuilt accept command")
}

#[tokio::test]
async fn receipt_first_accept_drives_exact_success_pipeline() {
    let fixture = DeliveryMergeFixture::new(None).await;
    let prepared = fixture.prepare_accept().await;
    let source_gate = fixture.live_runtime.install_gate(LiveStage::SourceObject);

    let acceptance = fixture.accept(&prepared).await;
    assert_eq!(acceptance.operation_id(), prepared.operation_id);
    assert_eq!(
        acceptance.receipt(),
        DeliveryMergeReceiptDisposition::Created
    );

    source_gate.wait_until_reached().await;
    let accepted = fixture.operation(prepared.operation_id).await;
    assert_eq!(accepted.state, MergeOperationState::Accepted);
    assert_eq!(
        fixture
            .source(prepared.task.id)
            .await
            .expect("receipt-first pipeline has created its source")
            .state,
        DeliverySourceState::ObjectPending
    );
    source_gate.release();

    let merged = fixture
        .wait_operation_state(prepared.operation_id, MergeOperationState::Merged)
        .await;
    assert!(merged.expected_merge_commit.is_some());
    assert!(merged.merged_disposition_task_id.is_some());
    fixture
        .wait_source_state(prepared.task.id, DeliverySourceState::Committed)
        .await;
    fixture
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    assert_eq!(
        fixture.live_runtime.calls(),
        vec![
            LiveCall::AuthenticateAccept,
            LiveCall::SourceObject,
            LiveCall::SourceCommit,
            LiveCall::ExpectedMerge,
            LiveCall::ActualMerge,
        ]
    );
    fixture.finish().await;
}

#[tokio::test]
async fn live_open_auth_and_merge_stages_may_exceed_orchestration_budget() {
    let fixture = DeliveryMergeFixture::new(None).await;
    let prepared = fixture.prepare_accept().await;
    let delays = [
        LiveStage::OpenSession,
        LiveStage::AuthenticateAccept,
        LiveStage::ExpectedMerge,
        LiveStage::ActualMerge,
    ]
    .map(|stage| {
        fixture
            .live_runtime
            .delay_once(stage, Duration::from_secs(31))
    });
    let advance_live_stages = async {
        for delay in delays {
            delay.wait_until_started().await;
            // Freezing earlier can auto-advance unrelated Store deadlines.
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(31)).await;
            tokio::time::resume();
        }
    };
    let (_, ()) = tokio::join!(fixture.accept(&prepared), advance_live_stages);
    let merged = timeout(Duration::from_secs(20 * 60), async {
        loop {
            let operation = fixture.operation(prepared.operation_id).await;
            if operation.state == MergeOperationState::Merged {
                return operation;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("valid slow live stages converge before the runtime budget");

    assert!(merged.expected_merge_commit.is_some());
    assert_eq!(
        fixture.live_runtime.calls(),
        vec![
            LiveCall::AuthenticateAccept,
            LiveCall::SourceObject,
            LiveCall::SourceCommit,
            LiveCall::ExpectedMerge,
            LiveCall::ActualMerge,
        ]
    );
    fixture.finish().await;
}

#[tokio::test]
async fn outer_runtime_timeout_during_actual_merge_retains_repository_ownership() {
    let fixture = DeliveryMergeFixture::new(None).await;
    let prepared = fixture.prepare_accept().await;
    let gate = fixture.live_runtime.install_gate(LiveStage::ActualMerge);

    fixture.accept(&prepared).await;
    gate.wait_until_reached().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(11 * 60 + 1)).await;
    tokio::time::resume();
    gate.wait_until_exited().await;

    assert_eq!(
        fixture.operation(prepared.operation_id).await.state,
        MergeOperationState::MergePending
    );
    assert_retained_worker(&fixture, "outer actual-merge runtime timeout").await;
    fixture.finish().await;
}

#[tokio::test]
async fn restart_recovers_every_durable_pending_stage() {
    assert_restart_recovery(LiveStage::SourceObject, false).await;
    assert_restart_recovery(LiveStage::SourceCommit, false).await;
    assert_restart_recovery(LiveStage::ActualMerge, false).await;
    assert_restart_recovery(LiveStage::Abort, true).await;
}

#[tokio::test]
async fn store_reply_loss_replays_the_exact_complete_command_once() {
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            operation: Some(StoreWriterOperationKind::CompleteMerge),
            count: 1,
        }])
        .expect("valid complete-merge reply-loss script"),
    );
    let fixture = DeliveryMergeFixture::new(Some(controller.clone())).await;
    let prepared = fixture.prepare_accept().await;

    let first = fixture.accept(&prepared).await;
    assert_eq!(first.receipt(), DeliveryMergeReceiptDisposition::Created);
    fixture
        .wait_operation_state(prepared.operation_id, MergeOperationState::Merged)
        .await;
    fixture
        .wait_repository_state(RepositoryControlState::Available)
        .await;

    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            StoreWriterOperationKind::CompleteMerge,
        ),
        1
    );
    assert_eq!(
        fixture
            .live_runtime
            .calls()
            .into_iter()
            .filter(|call| *call == LiveCall::ActualMerge)
            .count(),
        1,
        "Store reply loss must not rerun the Git merge"
    );
    let merged_journal_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND to_state = 'merged'",
    )
    .bind(prepared.operation_id.to_string())
    .fetch_one(fixture.base.store.pool())
    .await
    .expect("count merged journal rows");
    assert_eq!(merged_journal_rows, 1);

    let replay = match fixture
        .manager()
        .accept_merge(prepared.request())
        .await
        .expect("manager remains open for exact replay")
    {
        DeliveryMergeAcceptanceOutcome::Durable(acceptance) => acceptance,
        other => panic!("expected durable accept replay, got {other:?}"),
    };
    assert_eq!(replay.receipt(), DeliveryMergeReceiptDisposition::Existing);
    assert_eq!(replay.operation_id(), prepared.operation_id);
    fixture.finish().await;
}

#[tokio::test]
async fn known_not_applied_source_commit_retains_repository_ownership() {
    let controller = busy_controller(StoreWriterOperationKind::CommitDeliverySource, 18);
    let fixture = DeliveryMergeFixture::new(Some(controller.clone())).await;
    let prepared = fixture.prepare_accept().await;

    fixture.accept(&prepared).await;
    controller
        .wait_until_reached(StoreWriterFaultPoint::BusyBeforeExecute, 18)
        .await;

    assert_eq!(
        fixture.operation(prepared.operation_id).await.state,
        MergeOperationState::Accepted
    );
    assert_eq!(
        fixture
            .source(prepared.task.id)
            .await
            .expect("source commit intent remains durable")
            .state,
        DeliverySourceState::CommitPending
    );
    assert_retained_worker(&fixture, "source-commit known-not-applied").await;
    fixture.finish().await;
}

#[tokio::test]
async fn source_commit_kna_then_runtime_unavailable_keeps_retention_obligation() {
    let controller = busy_controller(StoreWriterOperationKind::CommitDeliverySource, 6);
    let fixture = DeliveryMergeFixture::new(Some(controller.clone())).await;
    let prepared = fixture.prepare_accept().await;

    fixture.accept(&prepared).await;
    controller
        .wait_until_reached(StoreWriterFaultPoint::BusyBeforeExecute, 6)
        .await;
    fixture
        .live_runtime
        .fail_once(LiveStage::SourceCommit, LiveFault::Unavailable);
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_retained_worker(
        &fixture,
        "source-commit KNA followed by runtime unavailable",
    )
    .await;
    assert_eq!(
        fixture
            .source(prepared.task.id)
            .await
            .expect("source commit intent remains durable")
            .state,
        DeliverySourceState::CommitPending
    );
    fixture.finish().await;
}

#[tokio::test]
async fn known_not_applied_merge_completion_retains_repository_ownership() {
    let controller = busy_controller(StoreWriterOperationKind::CompleteMerge, 18);
    let fixture = DeliveryMergeFixture::new(Some(controller.clone())).await;
    let prepared = fixture.prepare_accept().await;

    fixture.accept(&prepared).await;
    controller
        .wait_until_reached(StoreWriterFaultPoint::BusyBeforeExecute, 18)
        .await;

    assert_eq!(
        fixture.operation(prepared.operation_id).await.state,
        MergeOperationState::MergePending
    );
    assert_retained_worker(&fixture, "merge-completion known-not-applied").await;
    fixture.finish().await;
}

#[tokio::test]
async fn known_not_applied_begin_abort_retains_repository_ownership() {
    let controller = busy_controller(StoreWriterOperationKind::BeginMergeAbort, 18);
    let fixture = DeliveryMergeFixture::new(Some(controller.clone())).await;
    fixture.live_runtime.use_conflict();
    let prepared = fixture.prepare_accept().await;

    fixture.accept(&prepared).await;
    controller
        .wait_until_reached(StoreWriterFaultPoint::BusyBeforeExecute, 18)
        .await;

    assert_eq!(
        fixture.operation(prepared.operation_id).await.state,
        MergeOperationState::MergePending
    );
    assert_retained_worker(&fixture, "begin-abort known-not-applied").await;
    fixture.finish().await;
}

#[tokio::test]
async fn known_not_applied_abort_completion_retains_repository_ownership() {
    let controller = busy_controller(StoreWriterOperationKind::CompleteMergeAbort, 18);
    let fixture = DeliveryMergeFixture::new(Some(controller.clone())).await;
    fixture.live_runtime.use_conflict();
    let prepared = fixture.prepare_accept().await;

    fixture.accept(&prepared).await;
    controller
        .wait_until_reached(StoreWriterFaultPoint::BusyBeforeExecute, 18)
        .await;

    assert_eq!(
        fixture.operation(prepared.operation_id).await.state,
        MergeOperationState::AbortPending
    );
    assert_retained_worker(&fixture, "abort-completion known-not-applied").await;
    fixture.finish().await;
}

#[tokio::test]
async fn known_not_applied_fresh_accept_reconciliation_poisons_without_accept_side_effects() {
    let controller = busy_controller(StoreWriterOperationKind::ReconcileMerge, 6);
    let fixture = DeliveryMergeFixture::new(Some(controller.clone())).await;
    let prepared = fixture.prepare_accept().await;
    fixture.live_runtime.fail_once(
        LiveStage::AuthenticateAccept,
        LiveFault::Accept(DeliveryAcceptAuthenticationError::ReconciliationRequired(
            MergeReconciliationReason::DeliveryStateInconsistent,
        )),
    );

    assert_eq!(
        fixture
            .manager()
            .accept_merge(prepared.request())
            .await
            .expect("accept manager remains open"),
        DeliveryMergeAcceptanceOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::RuntimeUnavailable
        )
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::BusyBeforeExecute,
            StoreWriterOperationKind::ReconcileMerge,
        ),
        6
    );
    let operation = fixture.operation(prepared.operation_id).await;
    assert_eq!(operation.state, MergeOperationState::PreflightReady);
    assert!(operation.accept_receipt_id.is_none());
    assert!(operation.delivery_source_task_id.is_none());
    assert!(fixture.source(prepared.task.id).await.is_none());
    assert_poisoned_released_worker(&fixture, "fresh-accept reconciliation").await;
    fixture.finish().await;
}

#[tokio::test]
async fn known_not_applied_source_reconciliation_poisons_and_releases_worker() {
    let controller = busy_controller(StoreWriterOperationKind::ReconcileDeliverySource, 6);
    let fixture = DeliveryMergeFixture::new(Some(controller.clone())).await;
    fixture.live_runtime.fail_once(
        LiveStage::SourceCommit,
        LiveFault::ReconciliationRequired(MergeReconciliationReason::SourceInconsistent),
    );
    let prepared = fixture.prepare_accept().await;

    fixture.accept(&prepared).await;
    controller
        .wait_until_reached(StoreWriterFaultPoint::BusyBeforeExecute, 6)
        .await;

    assert_eq!(
        fixture.operation(prepared.operation_id).await.state,
        MergeOperationState::Accepted
    );
    assert_eq!(
        fixture
            .source(prepared.task.id)
            .await
            .expect("source reconciliation write was not applied")
            .state,
        DeliverySourceState::CommitPending
    );
    assert_poisoned_released_worker(&fixture, "source reconciliation").await;
    fixture.finish().await;
}

#[tokio::test]
async fn known_not_applied_source_retry_write_releases_without_side_effect_ownership() {
    let controller = busy_controller(StoreWriterOperationKind::RecordDeliverySourceRetry, 18);
    let fixture = DeliveryMergeFixture::new(Some(controller.clone())).await;
    fixture.live_runtime.source_known_not_applied_times(3);
    let prepared = fixture.prepare_accept().await;

    fixture.accept(&prepared).await;
    controller
        .wait_until_reached(StoreWriterFaultPoint::BusyBeforeExecute, 18)
        .await;
    fixture
        .wait_repository_state(RepositoryControlState::Available)
        .await;

    assert_eq!(
        fixture
            .source(prepared.task.id)
            .await
            .expect("source retry write was not applied")
            .state,
        DeliverySourceState::CommitPending
    );
    assert_eq!(
        fixture
            .manager()
            .quiesce()
            .await
            .expect("quiesce source-retry manager")
            .in_flight_workers(),
        0,
        "known-zero-effect source retry KNA may release its worker"
    );
    assert_eq!(
        fixture
            .coordinator
            .poison_reason(fixture.base.repository.id)
            .expect("fixture repository remains registered"),
        None
    );
    fixture.finish().await;
}

#[tokio::test]
async fn known_not_applied_merge_reconciliation_poisons_and_releases_worker() {
    let controller = busy_controller(StoreWriterOperationKind::ReconcileMerge, 6);
    let fixture = DeliveryMergeFixture::new(Some(controller.clone())).await;
    fixture.live_runtime.fail_once(
        LiveStage::ActualMerge,
        LiveFault::ReconciliationRequired(MergeReconciliationReason::WorktreeIdentityMismatch),
    );
    let prepared = fixture.prepare_accept().await;

    fixture.accept(&prepared).await;
    controller
        .wait_until_reached(StoreWriterFaultPoint::BusyBeforeExecute, 6)
        .await;

    assert_eq!(
        fixture.operation(prepared.operation_id).await.state,
        MergeOperationState::MergePending
    );
    assert_poisoned_released_worker(&fixture, "merge reconciliation").await;
    fixture.finish().await;
}

#[tokio::test]
async fn source_and_target_drift_persist_reconciliation_and_sticky_poison() {
    let source_fixture = DeliveryMergeFixture::new(None).await;
    source_fixture.live_runtime.fail_once(
        LiveStage::SourceCommit,
        LiveFault::ReconciliationRequired(MergeReconciliationReason::SourceInconsistent),
    );
    let source = source_fixture.prepare_accept().await;
    source_fixture.accept(&source).await;
    source_fixture
        .wait_operation_state(
            source.operation_id,
            MergeOperationState::ReconciliationRequired,
        )
        .await;
    source_fixture
        .wait_source_state(source.task.id, DeliverySourceState::ReconciliationRequired)
        .await;
    assert_sticky_poison(&source_fixture);
    source_fixture.finish().await;

    let target_fixture = DeliveryMergeFixture::new(None).await;
    target_fixture.live_runtime.fail_once(
        LiveStage::ExpectedMerge,
        LiveFault::ReconciliationRequired(MergeReconciliationReason::WorktreeIdentityMismatch),
    );
    let target = target_fixture.prepare_accept().await;
    target_fixture.accept(&target).await;
    target_fixture
        .wait_operation_state(
            target.operation_id,
            MergeOperationState::ReconciliationRequired,
        )
        .await;
    target_fixture
        .wait_source_state(target.task.id, DeliverySourceState::Committed)
        .await;
    assert_sticky_poison(&target_fixture);
    target_fixture.finish().await;
}

#[tokio::test]
async fn conflict_is_durable_abort_pending_before_runtime_abort() {
    let fixture = DeliveryMergeFixture::new(None).await;
    fixture.live_runtime.use_conflict();
    let prepared = fixture.prepare_accept().await;

    fixture.accept(&prepared).await;
    let conflict = fixture
        .wait_operation_state(prepared.operation_id, MergeOperationState::Conflict)
        .await;
    assert!(fixture.live_runtime.abort_observed_persisted_proof());
    assert!(conflict.abort_child_receipt_id.is_some());
    assert!(conflict.abort_merge_head.is_some());
    assert!(conflict.abort_index_stages_digest.is_some());
    assert!(conflict.abort_worktree_digest.is_some());
    assert_eq!(
        conflict.abort_merge_autostash_proof.as_deref(),
        Some("absent")
    );
    assert_eq!(conflict.conflicts.len(), 1);
    let terminal_states: Vec<String> = sqlx::query_scalar(
        "SELECT to_state FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? \
           AND to_state IN ('abort_pending', 'conflict') ORDER BY entity_version",
    )
    .bind(prepared.operation_id.to_string())
    .fetch_all(fixture.base.store.pool())
    .await
    .expect("load abort transition order");
    assert_eq!(terminal_states, vec!["abort_pending", "conflict"]);
    let calls = fixture.live_runtime.calls();
    assert!(
        calls.iter().position(|call| *call == LiveCall::ActualMerge)
            < calls.iter().position(|call| *call == LiveCall::Abort)
    );
    fixture.finish().await;
}

#[tokio::test]
async fn process_and_child_unknown_retain_lease_permit_and_actor_worker() {
    let process_fixture = DeliveryMergeFixture::new(None).await;
    let process = process_fixture.prepare_accept().await;
    process_fixture
        .process_proofs
        .push(DeliveryProcessProof::CleanupUnproven);
    let outcome = process_fixture
        .manager()
        .accept_merge(process.request())
        .await
        .expect("manager remains open");
    assert_eq!(
        outcome,
        DeliveryMergeAcceptanceOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::ProcessProofUnavailable
        )
    );
    process_fixture
        .wait_repository_state(RepositoryControlState::Busy)
        .await;
    assert_eq!(
        process_fixture
            .manager()
            .quiesce()
            .await
            .expect("quiesce process-unknown manager")
            .in_flight_workers(),
        1
    );
    assert!(matches!(
        process_fixture
            .base
            .store
            .lookup_delivery_command(&DeliveryCommand::AcceptMerge(process.command.clone()))
            .await
            .expect("query missing process-unknown receipt"),
        DeliveryCommandLookup::Missing
    ));
    process_fixture.finish().await;

    let child_fixture = DeliveryMergeFixture::new(None).await;
    child_fixture
        .live_runtime
        .fail_once(LiveStage::ActualMerge, LiveFault::ProcessCleanupUnproven);
    let child = child_fixture.prepare_accept().await;
    let accepted = child_fixture.accept(&child).await;
    assert_eq!(accepted.receipt(), DeliveryMergeReceiptDisposition::Created);
    child_fixture
        .wait_operation_state(child.operation_id, MergeOperationState::MergePending)
        .await;
    child_fixture
        .wait_repository_state(RepositoryControlState::Busy)
        .await;
    assert_eq!(
        child_fixture
            .manager()
            .quiesce()
            .await
            .expect("quiesce child-unknown manager")
            .in_flight_workers(),
        1
    );
    child_fixture.finish().await;
}

async fn assert_restart_recovery(stage: LiveStage, conflict: bool) {
    let mut fixture = DeliveryMergeFixture::new(None).await;
    if conflict {
        fixture.live_runtime.use_conflict();
    }
    fixture
        .live_runtime
        .fail_once(stage, LiveFault::Unavailable);
    let prepared = fixture.prepare_accept().await;
    let accepted = fixture.accept(&prepared).await;
    assert_eq!(accepted.receipt(), DeliveryMergeReceiptDisposition::Created);

    match stage {
        LiveStage::OpenSession | LiveStage::AuthenticateAccept => {
            panic!("live session admission is not a durable pending state")
        }
        LiveStage::SourceObject => {
            fixture
                .wait_source_state(prepared.task.id, DeliverySourceState::ObjectPending)
                .await;
            assert_eq!(
                fixture.operation(prepared.operation_id).await.state,
                MergeOperationState::Accepted
            );
        }
        LiveStage::SourceCommit => {
            fixture
                .wait_source_state(prepared.task.id, DeliverySourceState::CommitPending)
                .await;
            assert_eq!(
                fixture.operation(prepared.operation_id).await.state,
                MergeOperationState::Accepted
            );
        }
        LiveStage::ActualMerge => {
            fixture
                .wait_operation_state(prepared.operation_id, MergeOperationState::MergePending)
                .await;
        }
        LiveStage::Abort => {
            fixture
                .wait_operation_state(prepared.operation_id, MergeOperationState::AbortPending)
                .await;
        }
        LiveStage::ExpectedMerge => panic!("expected-merge is not a durable pending state"),
    }
    fixture
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    fixture.restart_manager().await;
    let recovery = fixture
        .manager()
        .recover_operation_for_test(prepared.operation_id)
        .await
        .expect("restarted manager remains open");
    assert_eq!(recovery, DeliveryOperationRecoveryOutcome::Converged);
    let expected = if conflict {
        MergeOperationState::Conflict
    } else {
        MergeOperationState::Merged
    };
    fixture
        .wait_operation_state(prepared.operation_id, expected)
        .await;
    fixture
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    fixture.finish().await;
}

fn assert_sticky_poison(fixture: &DeliveryMergeFixture) {
    assert_eq!(
        fixture
            .coordinator
            .control_state(fixture.base.repository.id)
            .expect("fixture repository remains registered"),
        RepositoryControlState::Poisoned
    );
    assert_eq!(
        fixture
            .coordinator
            .poison_reason(fixture.base.repository.id)
            .expect("load fixture poison reason"),
        Some(RepositoryControlPoisonReason::SideEffectIdentityMismatch)
    );
}

fn busy_controller(
    operation: StoreWriterOperationKind,
    count: u32,
) -> Arc<StoreWriterTestController> {
    Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::BusyBeforeExecute,
            operation: Some(operation),
            count,
        }])
        .expect("valid known-not-applied StoreWriter script"),
    )
}

async fn assert_retained_worker(fixture: &DeliveryMergeFixture, stage: &str) {
    fixture
        .wait_repository_state(RepositoryControlState::Busy)
        .await;
    assert_eq!(
        fixture
            .manager()
            .quiesce()
            .await
            .unwrap_or_else(|_| panic!("quiesce {stage} manager"))
            .in_flight_workers(),
        1,
        "{stage} must retain its lease, global permit, and actor worker"
    );
}

async fn assert_poisoned_released_worker(fixture: &DeliveryMergeFixture, stage: &str) {
    fixture
        .wait_repository_state(RepositoryControlState::Poisoned)
        .await;
    assert_eq!(
        fixture
            .manager()
            .quiesce()
            .await
            .unwrap_or_else(|_| panic!("quiesce {stage} manager"))
            .in_flight_workers(),
        0,
        "{stage} poison must release its permit and actor worker"
    );
}
