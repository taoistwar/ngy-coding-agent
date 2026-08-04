use coding_agent_store::{
    DeliverySourceAnchor, DeliverySourceReconciliationReason, DeliverySourceState,
    MergeKnownNotAppliedReason, MergeOperationState, MergeReconciliationReason,
    MergeTransitionOutcome, ReconcileDeliverySourceOutcome, ReconcileDeliverySourceRequest,
    ReconcileMergeRequest, RecordMergeKnownFailureRequest,
};

use crate::support::delivery::eligibility::SOURCE_COMMIT;

use super::fixtures::{accepted_with_committed_source, merge_pending};

#[tokio::test]
async fn accepted_known_zero_effect_failure_binds_only_the_committed_source() {
    let (store, task, operation_id, accepted_version) = accepted_with_committed_source().await;
    let request = RecordMergeKnownFailureRequest::try_new(
        task.id,
        operation_id,
        MergeOperationState::Accepted,
        accepted_version,
        MergeKnownNotAppliedReason::TargetHeadChanged,
    )
    .unwrap();
    assert!(matches!(
        store
            .record_merge_known_failure(request.clone())
            .await
            .unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));
    assert!(matches!(
        store.record_merge_known_failure(request).await.unwrap(),
        MergeTransitionOutcome::Existing(_)
    ));
    let operation = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    assert_eq!(operation.state, MergeOperationState::Failed);
    assert_eq!(operation.source_commit.unwrap().as_str(), SOURCE_COMMIT);
    assert!(operation.expected_merge_commit.is_none());
}

#[tokio::test]
async fn unknown_merge_pending_outcome_enters_allowlisted_reconciliation() {
    let (store, task, operation_id, pending_version) = merge_pending().await;
    let request = ReconcileMergeRequest::try_new(
        task.id,
        operation_id,
        MergeOperationState::MergePending,
        pending_version,
        MergeReconciliationReason::WorktreeIdentityMismatch,
    )
    .unwrap();
    assert!(matches!(
        store.reconcile_merge(request.clone()).await.unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));
    assert!(matches!(
        store.reconcile_merge(request).await.unwrap(),
        MergeTransitionOutcome::Existing(_)
    ));
    let operation = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    assert_eq!(operation.state, MergeOperationState::ReconciliationRequired);
    assert_eq!(
        operation.failure_code.unwrap().as_str(),
        "WORKTREE_IDENTITY_MISMATCH"
    );
}

#[tokio::test]
async fn accepted_source_inconsistent_requires_the_task5_paired_reconciliation_api() {
    let (store, task, operation_id, accepted_version) = accepted_with_committed_source().await;
    let merge_only = ReconcileMergeRequest::try_new(
        task.id,
        operation_id,
        MergeOperationState::Accepted,
        accepted_version,
        MergeReconciliationReason::SourceInconsistent,
    )
    .unwrap();
    assert!(matches!(
        store.reconcile_merge(merge_only).await.unwrap(),
        MergeTransitionOutcome::Conflict
    ));
    let ownership = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let source = ownership.source.unwrap();
    assert_eq!(source.state, DeliverySourceState::Committed);
    assert_eq!(
        ownership
            .merge_operations
            .iter()
            .find(|operation| operation.operation_id == operation_id)
            .unwrap()
            .state,
        MergeOperationState::Accepted
    );

    let paired = ReconcileDeliverySourceRequest::try_new(
        DeliverySourceAnchor::try_new(task.id, operation_id, accepted_version).unwrap(),
        DeliverySourceState::Committed,
        source.version,
        accepted_version,
        DeliverySourceReconciliationReason::SourceInconsistent,
    )
    .unwrap();
    assert!(matches!(
        store.reconcile_delivery_source(paired).await.unwrap(),
        ReconcileDeliverySourceOutcome::Applied(_)
    ));
}
