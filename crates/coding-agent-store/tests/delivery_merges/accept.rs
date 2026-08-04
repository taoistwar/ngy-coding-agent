use std::str::FromStr;

use coding_agent_domain::{ClientRequestId, TaskId};
use coding_agent_store::{
    AcceptMergeCommandRequest, AcceptMergeOutcome, DeliveryAcceptedOperationState,
    DeliveryOperationId, DeliveryVersion, GitBranchRef, GitCommitOid, MergeConflictPaths,
    MergeOperationState, MergePreflightResult, PreflightRejectedReason, PreflightStaleReason,
    RecordMergePreflightResultRequest, Sha256Digest, Store,
};

use crate::support::delivery::eligibility::{MERGE_BASE, MERGE_TREE};

use super::fixtures::{TARGET_BRANCH, accept_command, create_pending_preflight, pending_preflight};

#[tokio::test]
async fn accept_receipt_and_ready_to_accepted_transition_commit_atomically() {
    let (store, task, operation_id) = pending_preflight().await;
    super::preflight_results::ready(&store, task.id, operation_id).await;
    let command = accept_command(&store, &task, operation_id, ClientRequestId::new()).await;

    let receipt = match store.accept_merge(command.clone()).await.unwrap() {
        AcceptMergeOutcome::Accepted(receipt) => receipt,
        other => panic!("expected accepted merge, got {other:?}"),
    };
    assert_eq!(
        receipt.accepted_operation_version,
        DeliveryVersion::try_new(3).unwrap()
    );
    assert_eq!(
        receipt.accepted_operation_state,
        DeliveryAcceptedOperationState::Accepted
    );

    let operation = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    assert_eq!(operation.state, MergeOperationState::Accepted);
    assert_eq!(operation.accept_receipt_id, Some(receipt.client_request_id));
    let metadata = operation.merge_metadata.unwrap();
    assert_eq!(
        metadata.message_bytes,
        format!(
            "coding-agent: merge task {} attempt {}\n",
            task.id, task.attempt
        )
        .as_bytes()
    );
    assert_eq!(metadata.author_date_bytes, metadata.committer_date_bytes);

    assert!(matches!(
        store.accept_merge(command).await.unwrap(),
        AcceptMergeOutcome::Existing(existing) if existing == receipt
    ));
}

#[tokio::test]
async fn rejected_conflict_stale_and_superseded_operations_cannot_be_accepted() {
    let (store, task, rejected_id) = pending_preflight().await;
    let rejected = RecordMergePreflightResultRequest::try_new(
        task.id,
        rejected_id,
        DeliveryVersion::initial(),
        MergePreflightResult::rejected(PreflightRejectedReason::TargetWorktreeDirty),
    )
    .unwrap();
    store.record_merge_preflight_result(rejected).await.unwrap();
    let rejected_accept = accept_command(&store, &task, rejected_id, ClientRequestId::new()).await;
    assert_accept_conflict_without_writes(&store, rejected_id, rejected_accept).await;

    let (store, task, conflict_id) = pending_preflight().await;
    let conflict = RecordMergePreflightResultRequest::try_new(
        task.id,
        conflict_id,
        DeliveryVersion::initial(),
        MergePreflightResult::conflict(
            GitCommitOid::from_str(MERGE_BASE).unwrap(),
            coding_agent_store::GitTreeOid::from_str(MERGE_TREE).unwrap(),
            MergeConflictPaths::try_from_raw(Vec::new()).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    store.record_merge_preflight_result(conflict).await.unwrap();
    let conflict_accept = accept_command(&store, &task, conflict_id, ClientRequestId::new()).await;
    assert_accept_conflict_without_writes(&store, conflict_id, conflict_accept).await;

    let (store, task, stale_id) = pending_preflight().await;
    let stale = RecordMergePreflightResultRequest::try_new(
        task.id,
        stale_id,
        DeliveryVersion::initial(),
        MergePreflightResult::stale(PreflightStaleReason::TargetHeadChanged),
    )
    .unwrap();
    store.record_merge_preflight_result(stale).await.unwrap();
    let stale_accept = accept_command(&store, &task, stale_id, ClientRequestId::new()).await;
    assert_accept_conflict_without_writes(&store, stale_id, stale_accept).await;

    let (store, task, superseded_id) = pending_preflight().await;
    super::preflight_results::ready(&store, task.id, superseded_id).await;
    create_pending_preflight(&store, &task).await;
    let superseded_accept =
        accept_command(&store, &task, superseded_id, ClientRequestId::new()).await;
    assert_accept_conflict_without_writes(&store, superseded_id, superseded_accept).await;
}

#[tokio::test]
async fn version_evidence_target_task_and_operation_mismatches_are_zero_write_conflicts() {
    let (store, task, operation_id) = pending_preflight().await;
    super::preflight_results::ready(&store, task.id, operation_id).await;
    let exact = accept_command(&store, &task, operation_id, ClientRequestId::new()).await;
    let requests = [
        AcceptMergeCommandRequest::try_new(
            ClientRequestId::new(),
            task.id,
            operation_id,
            DeliveryVersion::initial(),
            exact.expected_review_generation(),
            exact.expected_workspace_fingerprint().clone(),
            exact.target_branch().clone(),
            exact.expected_target_head().clone(),
        )
        .unwrap(),
        AcceptMergeCommandRequest::try_new(
            ClientRequestId::new(),
            task.id,
            operation_id,
            exact.expected_operation_version(),
            exact.expected_review_generation() + 1,
            exact.expected_workspace_fingerprint().clone(),
            exact.target_branch().clone(),
            exact.expected_target_head().clone(),
        )
        .unwrap(),
        AcceptMergeCommandRequest::try_new(
            ClientRequestId::new(),
            task.id,
            operation_id,
            exact.expected_operation_version(),
            exact.expected_review_generation(),
            Sha256Digest::from_str(
                "abababababababababababababababababababababababababababababababab",
            )
            .unwrap(),
            exact.target_branch().clone(),
            exact.expected_target_head().clone(),
        )
        .unwrap(),
        AcceptMergeCommandRequest::try_new(
            ClientRequestId::new(),
            task.id,
            operation_id,
            exact.expected_operation_version(),
            exact.expected_review_generation(),
            exact.expected_workspace_fingerprint().clone(),
            GitBranchRef::from_str("refs/heads/not-main").unwrap(),
            exact.expected_target_head().clone(),
        )
        .unwrap(),
        AcceptMergeCommandRequest::try_new(
            ClientRequestId::new(),
            task.id,
            operation_id,
            exact.expected_operation_version(),
            exact.expected_review_generation(),
            exact.expected_workspace_fingerprint().clone(),
            GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
            GitCommitOid::from_str("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
        )
        .unwrap(),
        AcceptMergeCommandRequest::try_new(
            ClientRequestId::new(),
            TaskId::new(),
            operation_id,
            exact.expected_operation_version(),
            exact.expected_review_generation(),
            exact.expected_workspace_fingerprint().clone(),
            exact.target_branch().clone(),
            exact.expected_target_head().clone(),
        )
        .unwrap(),
        AcceptMergeCommandRequest::try_new(
            ClientRequestId::new(),
            task.id,
            DeliveryOperationId::new(),
            exact.expected_operation_version(),
            exact.expected_review_generation(),
            exact.expected_workspace_fingerprint().clone(),
            exact.target_branch().clone(),
            exact.expected_target_head().clone(),
        )
        .unwrap(),
    ];
    for request in requests {
        assert_accept_conflict_without_writes(&store, operation_id, request).await;
    }
}

async fn assert_accept_conflict_without_writes(
    store: &Store,
    operation_id: DeliveryOperationId,
    request: AcceptMergeCommandRequest,
) {
    let before: (String, i64, Option<String>, i64, i64) = sqlx::query_as(
        "SELECT m.state, m.version, m.accept_receipt_id, \
                (SELECT COUNT(*) FROM task_delivery_operation_transitions t \
                 WHERE t.entity_kind = 'merge_operation' AND t.entity_id = m.operation_id), \
                (SELECT COUNT(*) FROM task_delivery_command_receipts r \
                 WHERE r.operation_id = m.operation_id AND r.command_kind = 'accept_merge') \
         FROM task_merge_operations m WHERE m.operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert!(matches!(
        store.accept_merge(request).await.unwrap(),
        AcceptMergeOutcome::Conflict
    ));
    let after: (String, i64, Option<String>, i64, i64) = sqlx::query_as(
        "SELECT m.state, m.version, m.accept_receipt_id, \
                (SELECT COUNT(*) FROM task_delivery_operation_transitions t \
                 WHERE t.entity_kind = 'merge_operation' AND t.entity_id = m.operation_id), \
                (SELECT COUNT(*) FROM task_delivery_command_receipts r \
                 WHERE r.operation_id = m.operation_id AND r.command_kind = 'accept_merge') \
         FROM task_merge_operations m WHERE m.operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(after, before);
}
