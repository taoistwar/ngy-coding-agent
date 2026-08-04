use coding_agent_store::{
    CreateDeliverySourceOutcome, CreateDeliverySourceRequest, DeliveryRecoveryDisposition,
    DeliveryRecoveryQuery, DeliverySourceAnchor, DeliverySourceReconciliationReason,
    DeliverySourceState, ReconcileDeliverySourceOutcome, ReconcileDeliverySourceRequest,
};

use crate::authenticated_identity;
use crate::recovery_fixtures::{accepted, merged_task, pending_preflight};
use crate::support::delivery::eligibility::{
    COMMON_IDENTITY, create_worktree_cleanup, reconcile_worktree_cleanup,
};

#[tokio::test]
async fn legal_source_reconciliation_is_a_typed_blocker_at_origin_operation_order() {
    let store = crate::support::seeded_store().await;
    let (source_task, operation_id, accept_command) = accepted(
        &store,
        "codex/recovery-source-reconciliation",
        COMMON_IDENTITY,
    )
    .await;
    let (later_task, _) = pending_preflight(
        &store,
        "codex/recovery-after-source-origin",
        COMMON_IDENTITY,
    )
    .await;
    let source = match store
        .create_delivery_source(CreateDeliverySourceRequest::try_new(accept_command).unwrap())
        .await
        .unwrap()
    {
        CreateDeliverySourceOutcome::Created(source) => source,
        other => panic!("expected created source, got {other:?}"),
    };
    let anchor =
        DeliverySourceAnchor::try_new(source_task.id, operation_id, source.origin_accepted_version)
            .unwrap();
    let request = ReconcileDeliverySourceRequest::try_new(
        anchor,
        DeliverySourceState::ObjectPending,
        source.version,
        source.origin_accepted_version,
        DeliverySourceReconciliationReason::SourceInconsistent,
    )
    .unwrap();
    assert!(matches!(
        store.reconcile_delivery_source(request).await.unwrap(),
        ReconcileDeliverySourceOutcome::Applied(_)
    ));

    let batch = store
        .delivery_recovery_batch(&DeliveryRecoveryQuery::first(authenticated_identity()))
        .await
        .unwrap();
    assert_eq!(
        batch
            .entries
            .iter()
            .map(|entry| entry.identity.task_id())
            .collect::<Vec<_>>(),
        vec![source_task.id, later_task.id]
    );
    assert!(matches!(
        batch.entries[0].disposition,
        DeliveryRecoveryDisposition::ReconciliationRequired
    ));
    assert!(batch.entries[0].ownership.requires_reconciliation());
    let startup = store.startup_delivery_ownership().await.unwrap();
    assert!(
        startup
            .iter()
            .find(|entry| entry.identity.task_id() == source_task.id)
            .unwrap()
            .reconciliation_required
    );
}

#[tokio::test]
async fn cleanup_reconciliation_orders_by_cleanup_creation_not_earlier_disposition_fact() {
    let store = crate::support::seeded_store().await;
    let cleanup_task = merged_task(&store, "codex/recovery-cleanup-reconciliation").await;
    let (middle_task, _) = pending_preflight(
        &store,
        "codex/recovery-between-disposition-and-cleanup",
        COMMON_IDENTITY,
    )
    .await;
    let cleanup_id = create_worktree_cleanup(&store, &cleanup_task).await;
    reconcile_worktree_cleanup(&store, &cleanup_task, cleanup_id).await;

    let batch = store
        .delivery_recovery_batch(&DeliveryRecoveryQuery::first(authenticated_identity()))
        .await
        .unwrap();
    assert_eq!(
        batch
            .entries
            .iter()
            .map(|entry| entry.identity.task_id())
            .collect::<Vec<_>>(),
        vec![middle_task.id, cleanup_task.id]
    );
    assert!(matches!(
        batch.entries[1].disposition,
        DeliveryRecoveryDisposition::ReconciliationRequired
    ));
}
