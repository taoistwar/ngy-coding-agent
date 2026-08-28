use std::str::FromStr;

use coding_agent_domain::{ClientRequestId, Task, TaskId};
use coding_agent_store::{
    AcceptMergeCommandRequest, AcceptMergeOutcome, CreatePreflightOutcome, CreatePreflightRequest,
    DeliveryOperationId, DeliveryVersion, DirectoryIdentity, GitBranchRef, GitCommitOid,
    GitTreeOid, MarkPreflightStaleOutcome, MarkPreflightStaleRequest, MergeConflictPaths,
    MergeOperationState, MergePreflightResult, PreflightCommandRequest, PreflightRejectedReason,
    PreflightStaleReason, RecordMergePreflightResultRequest, Sha256Digest, Store,
};

use crate::snapshot::CompatibilitySnapshot;
use crate::support::delivery::eligibility::{
    ADMIN_IDENTITY, CANDIDATE_TREE, COMMON_IDENTITY, CONFIG_DIGEST, MERGE_BASE, MERGE_TREE,
    PREFLIGHT_SOURCE, TARGET_CONFIG_DIGEST, TARGET_HEAD, TARGET_SECURITY_DIGEST,
};

use super::TARGET_BRANCH;
use super::helpers::{applied_merge, ownership};
use super::scenario;

pub struct AcceptedPreflight {
    pub operation_id: DeliveryOperationId,
    pub command: AcceptMergeCommandRequest,
    pub version: DeliveryVersion,
}

pub async fn close_conflicting_preflight(
    store: &Store,
    task: &Task,
    baseline: &CompatibilitySnapshot,
) {
    let operation_id = create_preflight(store, task).await;
    baseline
        .assert_unchanged(store, "create preflight for conflict")
        .await;
    record_conflict(store, task.id, operation_id).await;
    baseline
        .assert_unchanged(store, "preflight pending to conflict")
        .await;
}

pub async fn accept_ready_preflight(
    store: &Store,
    task: &Task,
    baseline: &CompatibilitySnapshot,
) -> AcceptedPreflight {
    let operation_id = create_preflight(store, task).await;
    baseline
        .assert_unchanged(store, "create preflight for delivery")
        .await;
    let ready_version = record_ready(store, task.id, operation_id).await;
    baseline
        .assert_unchanged(store, "preflight pending to ready")
        .await;
    let accepted = accept_merge(store, task, operation_id, ready_version).await;
    baseline
        .assert_unchanged(store, "preflight ready to accepted")
        .await;
    accepted
}

pub async fn exercise_terminal_preflight_transitions() {
    record_pending_terminal(
        MergePreflightResult::rejected(PreflightRejectedReason::TaskNotMergeEligible),
        MergeOperationState::Rejected,
        "preflight pending to rejected",
    )
    .await;
    record_pending_terminal(
        MergePreflightResult::stale(PreflightStaleReason::EvidenceStale),
        MergeOperationState::Stale,
        "preflight pending to stale",
    )
    .await;
    mark_ready_stale().await;
    supersede_ready_preflight().await;
}

pub async fn create_preflight(store: &Store, task: &Task) -> DeliveryOperationId {
    let command = PreflightCommandRequest::try_new(
        ClientRequestId::new(),
        task.id,
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
    )
    .unwrap();
    let request = CreatePreflightRequest::try_new(
        command,
        DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
        Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
        Sha256Digest::from_str(TARGET_CONFIG_DIGEST).unwrap(),
        Sha256Digest::from_str(TARGET_SECURITY_DIGEST).unwrap(),
    )
    .unwrap();
    let operation_id = match store.create_merge_preflight(request).await.unwrap() {
        CreatePreflightOutcome::Created(receipt) => receipt.operation_id,
        other => panic!("expected created preflight, got {other:?}"),
    };
    crate::support::delivery::merge::bind_preflight_inputs(
        store,
        task.id,
        operation_id,
        CANDIDATE_TREE,
        PREFLIGHT_SOURCE,
    )
    .await;
    operation_id
}

pub async fn record_conflict(store: &Store, task_id: TaskId, operation_id: DeliveryOperationId) {
    let result = MergePreflightResult::conflict(
        GitCommitOid::from_str(MERGE_BASE).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
        MergeConflictPaths::try_from_raw(vec![b"src/conflict.rs".to_vec()]).unwrap(),
    )
    .unwrap();
    applied_merge(
        store
            .record_merge_preflight_result(
                RecordMergePreflightResultRequest::try_new(
                    task_id,
                    operation_id,
                    DeliveryVersion::try_new(2).unwrap(),
                    result,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
    );
}

pub(super) async fn record_ready(
    store: &Store,
    task_id: TaskId,
    operation_id: DeliveryOperationId,
) -> DeliveryVersion {
    let result = MergePreflightResult::ready(
        GitCommitOid::from_str(MERGE_BASE).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
    )
    .unwrap();
    applied_merge(
        store
            .record_merge_preflight_result(
                RecordMergePreflightResultRequest::try_new(
                    task_id,
                    operation_id,
                    DeliveryVersion::try_new(2).unwrap(),
                    result,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
    )
    .version
}

async fn record_pending_terminal(
    result: MergePreflightResult,
    expected_state: MergeOperationState,
    label: &str,
) {
    let (fixture, baseline) = scenario::fresh().await;
    let operation_id = create_preflight(&fixture.store, &fixture.delivery_task).await;
    baseline
        .assert_unchanged(&fixture.store, "create terminal preflight")
        .await;
    applied_merge(
        fixture
            .store
            .record_merge_preflight_result(
                RecordMergePreflightResultRequest::try_new(
                    fixture.delivery_task.id,
                    operation_id,
                    DeliveryVersion::try_new(2).unwrap(),
                    result,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    baseline.assert_unchanged(&fixture.store, label).await;
    assert_operation_state(
        &fixture.store,
        fixture.delivery_task.id,
        operation_id,
        expected_state,
    )
    .await;
}

async fn mark_ready_stale() {
    let (fixture, baseline) = scenario::fresh().await;
    let operation_id = create_preflight(&fixture.store, &fixture.delivery_task).await;
    baseline
        .assert_unchanged(&fixture.store, "create preflight for ready stale")
        .await;
    let ready_version = record_ready(&fixture.store, fixture.delivery_task.id, operation_id).await;
    baseline
        .assert_unchanged(&fixture.store, "preflight pending to ready before stale")
        .await;
    assert!(matches!(
        fixture
            .store
            .mark_merge_preflight_stale(
                MarkPreflightStaleRequest::try_new(
                    fixture.delivery_task.id,
                    operation_id,
                    ready_version,
                    PreflightStaleReason::EvidenceStale,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        MarkPreflightStaleOutcome::Applied { .. }
    ));
    baseline
        .assert_unchanged(&fixture.store, "preflight ready to stale")
        .await;
    assert_operation_state(
        &fixture.store,
        fixture.delivery_task.id,
        operation_id,
        MergeOperationState::Stale,
    )
    .await;
}

async fn supersede_ready_preflight() {
    let (fixture, baseline) = scenario::fresh().await;
    let operation_id = create_preflight(&fixture.store, &fixture.delivery_task).await;
    baseline
        .assert_unchanged(&fixture.store, "create preflight for supersede")
        .await;
    record_ready(&fixture.store, fixture.delivery_task.id, operation_id).await;
    baseline
        .assert_unchanged(
            &fixture.store,
            "preflight pending to ready before supersede",
        )
        .await;
    let replacement = create_preflight(&fixture.store, &fixture.delivery_task).await;
    baseline
        .assert_unchanged(&fixture.store, "preflight ready to superseded")
        .await;
    assert_operation_state(
        &fixture.store,
        fixture.delivery_task.id,
        operation_id,
        MergeOperationState::Superseded,
    )
    .await;
    assert_operation_state(
        &fixture.store,
        fixture.delivery_task.id,
        replacement,
        MergeOperationState::PreflightPending,
    )
    .await;
}

async fn assert_operation_state(
    store: &Store,
    task_id: TaskId,
    operation_id: DeliveryOperationId,
    expected: MergeOperationState,
) {
    let operation = ownership(store, task_id)
        .await
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    assert_eq!(operation.state, expected);
}

async fn accept_merge(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
    ready_version: DeliveryVersion,
) -> AcceptedPreflight {
    let evidence = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .evidence_identity
        .unwrap();
    let command = AcceptMergeCommandRequest::try_new(
        ClientRequestId::new(),
        task.id,
        operation_id,
        ready_version,
        evidence.workspace_generation(),
        evidence.workspace_fingerprint().clone(),
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
    )
    .unwrap();
    let receipt = match store.accept_merge(command.clone()).await.unwrap() {
        AcceptMergeOutcome::Accepted(receipt) => receipt,
        other => panic!("expected accepted merge, got {other:?}"),
    };
    AcceptedPreflight {
        operation_id,
        command,
        version: receipt.accepted_operation_version,
    }
}
