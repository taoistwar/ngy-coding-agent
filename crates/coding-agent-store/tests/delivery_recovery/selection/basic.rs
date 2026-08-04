use coding_agent_store::{
    DeliveryOperationId, DeliveryRecoveryAction, DeliveryRecoveryDisposition,
    DeliveryRecoveryQuery, MergeOperationState,
};

use crate::authenticated_identity;
use crate::support::delivery::eligibility::{
    approved_task_on_store, approved_task_with_ready_artifact, finish_preflight_terminal,
    insert_preflight, mark_preflight_ready,
};

#[tokio::test]
async fn startup_ownership_and_recovery_return_a_pending_preflight() {
    let (store, task) = approved_task_with_ready_artifact("codex/recovery-preflight").await;
    let snapshot = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let operation_id = DeliveryOperationId::new();
    insert_preflight(
        &store,
        &task,
        snapshot.evidence_identity.as_ref().unwrap(),
        operation_id,
    )
    .await;

    let ownership = store.startup_delivery_ownership().await.unwrap();
    assert_eq!(ownership.len(), 1);
    assert_eq!(ownership[0].identity.task_id(), task.id);
    assert_eq!(
        ownership[0].expected_common_git_identity,
        authenticated_identity()
    );
    assert!(!ownership[0].reconciliation_required);

    let batch = store
        .delivery_recovery_batch(&DeliveryRecoveryQuery::first(authenticated_identity()))
        .await
        .unwrap();
    assert_eq!(batch.entries.len(), 1);
    assert!(batch.next_cursor.is_none());
    assert_eq!(batch.entries[0].identity.task_id(), task.id);
    assert!(matches!(
        batch.entries[0].disposition,
        DeliveryRecoveryDisposition::Recover(DeliveryRecoveryAction::PreflightPending {
            operation_id: recovered,
            ..
        }) if recovered == operation_id
    ));
}

#[tokio::test]
async fn creation_order_is_stable_and_ready_or_terminal_preflights_are_not_replayed() {
    let store = crate::support::seeded_store().await;
    let (_, first_task) = approved_task_on_store(store.clone(), "codex/recovery-first", 0).await;
    let first_snapshot = store
        .delivery_eligibility_snapshot(first_task.id)
        .await
        .unwrap()
        .unwrap();
    let first = DeliveryOperationId::new();
    insert_preflight(
        &store,
        &first_task,
        first_snapshot.evidence_identity.as_ref().unwrap(),
        first,
    )
    .await;

    let (_, ready_task) = approved_task_on_store(store.clone(), "codex/recovery-ready", 0).await;
    let ready_snapshot = store
        .delivery_eligibility_snapshot(ready_task.id)
        .await
        .unwrap()
        .unwrap();
    let ready = DeliveryOperationId::new();
    insert_preflight(
        &store,
        &ready_task,
        ready_snapshot.evidence_identity.as_ref().unwrap(),
        ready,
    )
    .await;
    mark_preflight_ready(&store, ready).await;

    let (_, terminal_task) =
        approved_task_on_store(store.clone(), "codex/recovery-terminal", 0).await;
    let terminal_snapshot = store
        .delivery_eligibility_snapshot(terminal_task.id)
        .await
        .unwrap()
        .unwrap();
    let terminal = DeliveryOperationId::new();
    insert_preflight(
        &store,
        &terminal_task,
        terminal_snapshot.evidence_identity.as_ref().unwrap(),
        terminal,
    )
    .await;
    finish_preflight_terminal(&store, terminal, MergeOperationState::Rejected).await;

    let (_, last_task) = approved_task_on_store(store.clone(), "codex/recovery-last", 0).await;
    let last_snapshot = store
        .delivery_eligibility_snapshot(last_task.id)
        .await
        .unwrap()
        .unwrap();
    let last = DeliveryOperationId::new();
    insert_preflight(
        &store,
        &last_task,
        last_snapshot.evidence_identity.as_ref().unwrap(),
        last,
    )
    .await;

    let query = DeliveryRecoveryQuery::first(authenticated_identity());
    let first_read = store.delivery_recovery_batch(&query).await.unwrap();
    let repeated = store.delivery_recovery_batch(&query).await.unwrap();
    assert_eq!(first_read, repeated);
    assert_eq!(
        first_read
            .entries
            .iter()
            .map(|entry| entry.identity.task_id())
            .collect::<Vec<_>>(),
        vec![first_task.id, last_task.id]
    );
}
