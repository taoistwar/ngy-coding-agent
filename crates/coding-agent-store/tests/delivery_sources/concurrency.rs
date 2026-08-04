use coding_agent_store::{
    AdvanceDeliverySourceObjectRequest, DeliverySourceReconciliationReason,
    DeliverySourceRetryReason, DeliverySourceState, DeliverySourceTransitionOutcome,
    DeliveryVersion, MergeOperationState, ReconcileDeliverySourceOutcome,
    ReconcileDeliverySourceRequest, RecordDeliverySourceRetryRequest,
};

use super::fixtures::{
    created_source, file_backed_accepted_fixture, object_proof, source_anchor, source_journal_count,
};

#[tokio::test]
async fn same_old_version_advance_retry_and_reconcile_have_one_atomic_winner() {
    let (fixture, command) = file_backed_accepted_fixture().await;
    let store = fixture.store.clone();
    let source = created_source(&store, command.clone()).await;
    let anchor = source_anchor(&command);
    let advance =
        AdvanceDeliverySourceObjectRequest::try_new(anchor, source.version, object_proof(&source))
            .unwrap();
    let retry = RecordDeliverySourceRetryRequest::try_new(
        anchor,
        DeliverySourceState::ObjectPending,
        source.version,
        DeliverySourceRetryReason::CommandTimedOut,
    )
    .unwrap();
    let reconcile = ReconcileDeliverySourceRequest::try_new(
        anchor,
        DeliverySourceState::ObjectPending,
        source.version,
        DeliveryVersion::try_new(3).unwrap(),
        DeliverySourceReconciliationReason::SourceInconsistent,
    )
    .unwrap();

    let advance_store = store.clone();
    let retry_store = store.clone();
    let reconcile_store = store.clone();
    let (advance_outcome, retry_outcome, reconcile_outcome) = tokio::join!(
        advance_store.advance_delivery_source_object(advance),
        retry_store.record_delivery_source_retry(retry),
        reconcile_store.reconcile_delivery_source(reconcile),
    );
    let advance_outcome = advance_outcome.unwrap();
    let retry_outcome = retry_outcome.unwrap();
    let reconcile_outcome = reconcile_outcome.unwrap();
    let applied = usize::from(matches!(
        advance_outcome,
        DeliverySourceTransitionOutcome::Applied(_)
    )) + usize::from(matches!(
        retry_outcome,
        DeliverySourceTransitionOutcome::Applied(_)
    )) + usize::from(matches!(
        reconcile_outcome,
        ReconcileDeliverySourceOutcome::Applied(_)
    ));
    let conflicts = usize::from(matches!(
        advance_outcome,
        DeliverySourceTransitionOutcome::Conflict
    )) + usize::from(matches!(
        retry_outcome,
        DeliverySourceTransitionOutcome::Conflict
    )) + usize::from(matches!(
        reconcile_outcome,
        ReconcileDeliverySourceOutcome::Conflict
    ));
    assert_eq!(applied, 1);
    assert_eq!(conflicts, 2);
    assert_eq!(source_journal_count(&store, command.task_id()).await, 2);

    let snapshot = store
        .delivery_ownership_snapshot(command.task_id())
        .await
        .unwrap()
        .unwrap();
    let current = snapshot.source.unwrap();
    assert_eq!(current.version, DeliveryVersion::try_new(2).unwrap());
    let operation = snapshot
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == command.preflight_operation_id())
        .unwrap();
    if current.state == DeliverySourceState::ReconciliationRequired {
        assert_eq!(operation.state, MergeOperationState::ReconciliationRequired);
        assert_eq!(current.failure_code, operation.failure_code);
        assert_eq!(current.updated_at, operation.updated_at);
        assert!(current.current_transition_id < operation.current_transition_id);
    } else {
        assert_eq!(operation.state, MergeOperationState::Accepted);
        assert_eq!(operation.version, DeliveryVersion::try_new(3).unwrap());
    }
}
