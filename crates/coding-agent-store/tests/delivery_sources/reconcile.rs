use coding_agent_store::{
    CommitDeliverySourceRequest, DeliverySourceReconciliationReason, DeliverySourceRetryReason,
    DeliverySourceState, DeliverySourceTransitionOutcome, DeliveryVersion,
    ReconcileDeliverySourceOutcome, ReconcileDeliverySourceRequest,
    RecordDeliverySourceRetryRequest,
};

use super::fixtures::{
    accepted_fixture, commit_pending_fixture, created_source, merge_journal_count, source_anchor,
    source_journal_count,
};

#[tokio::test]
async fn retry_and_reconcile_require_exact_pending_source_and_accepted_owner() {
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
    let anchor = source_anchor(&command);
    let retry = RecordDeliverySourceRetryRequest::try_new(
        anchor,
        DeliverySourceState::ObjectPending,
        source.version,
        DeliverySourceRetryReason::CommandTimedOut,
    )
    .unwrap();
    assert!(matches!(
        store.record_delivery_source_retry(retry).await.unwrap(),
        DeliverySourceTransitionOutcome::Applied(_)
    ));
    let retried = store
        .delivery_ownership_snapshot(command.task_id())
        .await
        .unwrap()
        .unwrap();
    let retried_source = retried.source.unwrap();
    assert_eq!(
        retried_source.failure_code.as_ref().unwrap().as_str(),
        "COMMAND_TIMED_OUT"
    );
    let retried_owner = retried
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == command.preflight_operation_id())
        .unwrap();
    assert_eq!(
        retried_owner.state,
        coding_agent_store::MergeOperationState::Accepted
    );
    assert_eq!(retried_owner.version, DeliveryVersion::try_new(3).unwrap());
    assert_eq!(retried_owner.failure_code, None);
    let reconcile = ReconcileDeliverySourceRequest::try_new(
        anchor,
        DeliverySourceState::ObjectPending,
        DeliveryVersion::try_new(2).unwrap(),
        DeliveryVersion::try_new(3).unwrap(),
        DeliverySourceReconciliationReason::SourceInconsistent,
    )
    .unwrap();
    let receipt = match store.reconcile_delivery_source(reconcile).await.unwrap() {
        ReconcileDeliverySourceOutcome::Applied(receipt) => receipt,
        other => panic!("expected reconciliation to apply, got {other:?}"),
    };
    assert_eq!(
        receipt.failure_code.as_str(),
        "DELIVERY_SOURCE_INCONSISTENT"
    );
    assert_eq!(
        receipt.source.failure_code.as_ref(),
        Some(&receipt.failure_code)
    );
    assert_eq!(receipt.source.transitioned_at, receipt.transitioned_at);
    let reconciled = store
        .delivery_ownership_snapshot(command.task_id())
        .await
        .unwrap()
        .unwrap();
    let reconciled_source = reconciled.source.unwrap();
    let reconciled_owner = reconciled
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == command.preflight_operation_id())
        .unwrap();
    assert_eq!(
        reconciled_source.failure_code,
        reconciled_owner.failure_code
    );
    assert_eq!(reconciled_source.updated_at, reconciled_owner.updated_at);
    assert!(reconciled_source.current_transition_id < reconciled_owner.current_transition_id);
    assert_eq!(source_journal_count(&store, command.task_id()).await, 3);
    assert_eq!(
        merge_journal_count(&store, command.preflight_operation_id()).await,
        4
    );
}

#[tokio::test]
async fn paired_reconcile_replay_and_wrong_expected_values_are_typed_conflicts() {
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
    let anchor = source_anchor(&command);
    let request = ReconcileDeliverySourceRequest::try_new(
        anchor,
        DeliverySourceState::ObjectPending,
        source.version,
        DeliveryVersion::try_new(3).unwrap(),
        DeliverySourceReconciliationReason::SourceInconsistent,
    )
    .unwrap();
    assert!(matches!(
        store
            .reconcile_delivery_source(request.clone())
            .await
            .unwrap(),
        ReconcileDeliverySourceOutcome::Applied(_)
    ));
    assert!(matches!(
        store.reconcile_delivery_source(request).await.unwrap(),
        ReconcileDeliverySourceOutcome::Existing(_)
    ));
    for conflict in [
        ReconcileDeliverySourceRequest::try_new(
            anchor,
            DeliverySourceState::ObjectPending,
            DeliveryVersion::try_new(2).unwrap(),
            DeliveryVersion::try_new(3).unwrap(),
            DeliverySourceReconciliationReason::SourceInconsistent,
        )
        .unwrap(),
        ReconcileDeliverySourceRequest::try_new(
            anchor,
            DeliverySourceState::ObjectPending,
            DeliveryVersion::initial(),
            DeliveryVersion::try_new(4).unwrap(),
            DeliverySourceReconciliationReason::SourceInconsistent,
        )
        .unwrap(),
        ReconcileDeliverySourceRequest::try_new(
            anchor,
            DeliverySourceState::ObjectPending,
            DeliveryVersion::initial(),
            DeliveryVersion::try_new(3).unwrap(),
            DeliverySourceReconciliationReason::ProcessTreeCleanupFailed,
        )
        .unwrap(),
    ] {
        assert!(matches!(
            store.reconcile_delivery_source(conflict).await.unwrap(),
            ReconcileDeliverySourceOutcome::Conflict
        ));
    }
    assert_eq!(source_journal_count(&store, command.task_id()).await, 2);
}

#[tokio::test]
async fn committed_source_can_reconcile_with_its_exact_still_accepted_owner() {
    let (store, command) = accepted_fixture().await;
    let (pending, anchor, _object, applied) = commit_pending_fixture(&store, &command).await;
    store
        .commit_delivery_source(
            CommitDeliverySourceRequest::try_new(anchor, pending.version, applied.clone()).unwrap(),
        )
        .await
        .unwrap();
    let request = ReconcileDeliverySourceRequest::try_new(
        anchor,
        DeliverySourceState::Committed,
        DeliveryVersion::try_new(3).unwrap(),
        DeliveryVersion::try_new(3).unwrap(),
        DeliverySourceReconciliationReason::SourceInconsistent,
    )
    .unwrap();
    assert!(matches!(
        store.reconcile_delivery_source(request).await.unwrap(),
        ReconcileDeliverySourceOutcome::Applied(_)
    ));
    let reconciled = store
        .delivery_ownership_snapshot(command.task_id())
        .await
        .unwrap()
        .unwrap();
    let source = reconciled.source.unwrap();
    let owner = reconciled
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == command.preflight_operation_id())
        .unwrap();
    assert!(owner.current_transition_id < source.current_transition_id);
    let commit_replay =
        CommitDeliverySourceRequest::try_new(anchor, DeliveryVersion::try_new(2).unwrap(), applied)
            .unwrap();
    assert!(matches!(
        store.commit_delivery_source(commit_replay).await.unwrap(),
        DeliverySourceTransitionOutcome::Existing(_)
    ));
}

#[tokio::test]
async fn committed_reconciliation_with_reversed_transition_order_fails_closed() {
    let (store, command) = accepted_fixture().await;
    let (pending, anchor, _object, applied) = commit_pending_fixture(&store, &command).await;
    store
        .commit_delivery_source(
            CommitDeliverySourceRequest::try_new(anchor, pending.version, applied).unwrap(),
        )
        .await
        .unwrap();
    let request = ReconcileDeliverySourceRequest::try_new(
        anchor,
        DeliverySourceState::Committed,
        DeliveryVersion::try_new(3).unwrap(),
        DeliveryVersion::try_new(3).unwrap(),
        DeliverySourceReconciliationReason::SourceInconsistent,
    )
    .unwrap();
    store.reconcile_delivery_source(request).await.unwrap();

    sqlx::raw_sql("DROP TRIGGER task_delivery_operation_transitions_no_update;")
        .execute(store.pool())
        .await
        .unwrap();
    let source_id: i64 = sqlx::query_scalar(
        "SELECT transition_id FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'delivery_source' AND entity_id = ? \
           AND to_state = 'reconciliation_required'",
    )
    .bind(command.task_id().to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    let merge_id: i64 = sqlx::query_scalar(
        "SELECT transition_id FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? \
           AND to_state = 'reconciliation_required'",
    )
    .bind(command.preflight_operation_id().to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert!(merge_id < source_id);
    sqlx::query(
        "UPDATE task_delivery_operation_transitions SET transition_id = -1 \
         WHERE transition_id = ?",
    )
    .bind(source_id)
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions SET transition_id = ? \
         WHERE transition_id = ?",
    )
    .bind(source_id)
    .bind(merge_id)
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions SET transition_id = ? \
         WHERE transition_id = -1",
    )
    .bind(merge_id)
    .execute(store.pool())
    .await
    .unwrap();

    assert!(matches!(
        store.delivery_ownership_snapshot(command.task_id()).await,
        Err(coding_agent_store::StoreError::InvariantViolation(_))
    ));
}
