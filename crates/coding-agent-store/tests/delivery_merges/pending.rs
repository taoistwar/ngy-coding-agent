use std::str::FromStr;

use coding_agent_domain::ClientRequestId;
use coding_agent_store::{
    DeliverySourceAnchor, DeliverySourceReconciliationReason, DeliverySourceState,
    EnterMergePendingRequest, GitCommitOid, GitTreeOid, MergeCommitObjectProof,
    MergeKnownNotAppliedReason, MergeOperationState, MergeTransitionOutcome,
    ReconcileDeliverySourceOutcome, ReconcileDeliverySourceRequest, RecordMergeKnownFailureRequest,
};

use crate::support::delivery::eligibility::{MERGE_COMMIT, MERGE_TREE, SOURCE_COMMIT, TARGET_HEAD};

use super::fixtures::{
    accept_command, accepted_with_committed_source, create_pending_preflight_with_source,
};

#[tokio::test]
async fn merge_object_proof_rejects_degenerate_fixed_no_ff_identity() {
    let (store, task, operation_id, _accepted_version) = accepted_with_committed_source().await;
    let metadata = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap()
        .merge_metadata
        .unwrap();
    let target = GitCommitOid::from_str(TARGET_HEAD).unwrap();
    let source = GitCommitOid::from_str(SOURCE_COMMIT).unwrap();
    let merge = GitCommitOid::from_str(MERGE_COMMIT).unwrap();
    let tree = GitTreeOid::from_str(MERGE_TREE).unwrap();
    assert!(
        MergeCommitObjectProof::try_new(
            merge.clone(),
            tree.clone(),
            vec![target.clone(), source.clone()],
            metadata.clone(),
        )
        .is_ok()
    );
    assert!(
        MergeCommitObjectProof::try_new(
            merge,
            tree.clone(),
            vec![target.clone(), target.clone()],
            metadata.clone(),
        )
        .is_err()
    );
    assert!(
        MergeCommitObjectProof::try_new(
            target.clone(),
            tree.clone(),
            vec![target.clone(), source.clone()],
            metadata.clone(),
        )
        .is_err()
    );
    assert!(
        MergeCommitObjectProof::try_new(source.clone(), tree, vec![target, source], metadata,)
            .is_err()
    );
}

#[tokio::test]
async fn committed_source_and_exact_object_proof_advance_accepted_to_merge_pending() {
    let (store, task, operation_id, accepted_version) = accepted_with_committed_source().await;
    let accepted = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    let proof = MergeCommitObjectProof::try_new(
        GitCommitOid::from_str(MERGE_COMMIT).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
        vec![
            GitCommitOid::from_str(TARGET_HEAD).unwrap(),
            GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        ],
        accepted.merge_metadata.unwrap(),
    )
    .unwrap();
    let request =
        EnterMergePendingRequest::try_new(task.id, operation_id, accepted_version, proof).unwrap();

    let receipt = match store.enter_merge_pending(request.clone()).await.unwrap() {
        MergeTransitionOutcome::Applied(receipt) => receipt,
        other => panic!("expected applied merge intent, got {other:?}"),
    };
    assert_eq!(receipt.state, MergeOperationState::MergePending);
    assert!(matches!(
        store.enter_merge_pending(request).await.unwrap(),
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
    assert_eq!(operation.delivery_source_task_id, Some(task.id));
    assert_eq!(operation.source_commit.unwrap().as_str(), SOURCE_COMMIT);
    assert_eq!(
        operation.expected_merge_commit.unwrap().as_str(),
        MERGE_COMMIT
    );
}

#[tokio::test]
async fn merge_pending_replay_survives_a_later_exact_source_reconciliation_pair() {
    let (store, task, first_operation_id, accepted_version) =
        accepted_with_committed_source().await;
    let accepted = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == first_operation_id)
        .unwrap();
    let request = EnterMergePendingRequest::try_new(
        task.id,
        first_operation_id,
        accepted_version,
        MergeCommitObjectProof::try_new(
            GitCommitOid::from_str(MERGE_COMMIT).unwrap(),
            GitTreeOid::from_str(MERGE_TREE).unwrap(),
            vec![
                GitCommitOid::from_str(TARGET_HEAD).unwrap(),
                GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
            ],
            accepted.merge_metadata.unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let pending_version = match store.enter_merge_pending(request.clone()).await.unwrap() {
        MergeTransitionOutcome::Applied(receipt) => receipt.version,
        other => panic!("expected merge pending, got {other:?}"),
    };
    let failed = RecordMergeKnownFailureRequest::try_new(
        task.id,
        first_operation_id,
        MergeOperationState::MergePending,
        pending_version,
        MergeKnownNotAppliedReason::TargetHeadChanged,
    )
    .unwrap();
    assert!(matches!(
        store.record_merge_known_failure(failed).await.unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));

    let second_operation_id =
        create_pending_preflight_with_source(&store, &task, SOURCE_COMMIT).await;
    super::preflight_results::ready(&store, task.id, second_operation_id).await;
    let second_accept =
        accept_command(&store, &task, second_operation_id, ClientRequestId::new()).await;
    let second_receipt = match store.accept_merge(second_accept).await.unwrap() {
        coding_agent_store::AcceptMergeOutcome::Accepted(receipt) => receipt,
        other => panic!("expected second accepted merge, got {other:?}"),
    };
    let source = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .source
        .unwrap();
    assert_eq!(source.state, DeliverySourceState::Committed);
    let anchor = DeliverySourceAnchor::try_new(
        task.id,
        second_operation_id,
        second_receipt.accepted_operation_version,
    )
    .unwrap();
    let reconcile = ReconcileDeliverySourceRequest::try_new(
        anchor,
        DeliverySourceState::Committed,
        source.version,
        second_receipt.accepted_operation_version,
        DeliverySourceReconciliationReason::SourceInconsistent,
    )
    .unwrap();
    assert!(matches!(
        store.reconcile_delivery_source(reconcile).await.unwrap(),
        ReconcileDeliverySourceOutcome::Applied(_)
    ));

    assert!(matches!(
        store.enter_merge_pending(request).await.unwrap(),
        MergeTransitionOutcome::Existing(_)
    ));
}

#[tokio::test]
async fn future_expected_version_is_a_zero_write_pending_conflict() {
    let (store, task, operation_id, accepted_version) = accepted_with_committed_source().await;
    let accepted = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    let request = EnterMergePendingRequest::try_new(
        task.id,
        operation_id,
        accepted_version.next().unwrap(),
        MergeCommitObjectProof::try_new(
            GitCommitOid::from_str(MERGE_COMMIT).unwrap(),
            GitTreeOid::from_str(MERGE_TREE).unwrap(),
            vec![
                GitCommitOid::from_str(TARGET_HEAD).unwrap(),
                GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
            ],
            accepted.merge_metadata.unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        store.enter_merge_pending(request).await.unwrap(),
        MergeTransitionOutcome::Conflict
    ));
    let row: (String, i64, i64) = sqlx::query_as(
        "SELECT state, version, \
                (SELECT COUNT(*) FROM task_delivery_operation_transitions \
                 WHERE entity_kind = 'merge_operation' AND entity_id = ?) \
         FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(row, ("accepted".to_owned(), 3, 3));
}
