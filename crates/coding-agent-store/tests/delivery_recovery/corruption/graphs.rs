use coding_agent_store::{
    CreateDeliverySourceOutcome, CreateDeliverySourceRequest, DeliverySourceAnchor,
    DeliverySourceReconciliationReason, DeliverySourceState, MAX_DELIVERY_RECOVERY_BATCH,
    ReconcileDeliverySourceOutcome, ReconcileDeliverySourceRequest,
};

use crate::corruption_cases::assert_recovery_invariant;
use crate::recovery_fixtures::{accepted, merged_task, pending_preflight};
use crate::support::delivery::eligibility::{COMMON_IDENTITY, delete_artifact_parent};

#[tokio::test]
async fn missing_ownership_parent_current_journal_gap_and_receipt_mismatch_fail_closed() {
    let store = crate::support::seeded_store().await;
    let (task, _) =
        pending_preflight(&store, "codex/recovery-missing-parent", COMMON_IDENTITY).await;
    delete_artifact_parent(&store, &task).await;
    assert_recovery_invariant(&store).await;

    let store = crate::support::seeded_store().await;
    let (_, operation_id) =
        pending_preflight(&store, "codex/recovery-journal-gap", COMMON_IDENTITY).await;
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER task_delivery_operation_transitions_no_delete")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "DELETE FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 1",
    )
    .bind(operation_id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);
    assert_recovery_invariant(&store).await;

    let store = crate::support::seeded_store().await;
    let (_, operation_id) =
        pending_preflight(&store, "codex/recovery-receipt-mismatch", COMMON_IDENTITY).await;
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER task_delivery_command_receipts_no_update")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_delivery_command_receipts \
         SET command_kind = 'accept_merge', accepted_operation_state = 'accepted', \
             response_discriminator = 'merge_accepted' \
         WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);
    assert_recovery_invariant(&store).await;
}

#[tokio::test]
async fn merged_operation_without_its_disposition_fails_closed() {
    let store = crate::support::seeded_store().await;
    let task = merged_task(&store, "codex/recovery-missing-disposition").await;
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER task_artifact_dispositions_no_delete")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DELETE FROM task_artifact_dispositions WHERE task_id = ?")
        .bind(task.id.to_string())
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);
    assert_recovery_invariant(&store).await;
}

#[tokio::test]
async fn corruption_beyond_the_first_batch_is_a_global_failure_not_a_hidden_later_page() {
    let store = crate::support::seeded_store().await;
    let mut page_out_task = None;
    for index in 0..=MAX_DELIVERY_RECOVERY_BATCH {
        let branch = format!("codex/recovery-corrupt-page-{index:02}");
        let (task, _) = pending_preflight(&store, &branch, COMMON_IDENTITY).await;
        page_out_task = Some(task);
    }
    delete_artifact_parent(&store, &page_out_task.unwrap()).await;
    assert_recovery_invariant(&store).await;
}

#[tokio::test]
async fn contradictory_source_merge_reconciliation_pair_fails_closed() {
    let store = crate::support::seeded_store().await;
    let (task, operation_id, accept_command) = accepted(
        &store,
        "codex/recovery-contradictory-reconciliation",
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
        DeliverySourceAnchor::try_new(task.id, operation_id, source.origin_accepted_version)
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

    sqlx::raw_sql(
        "DROP TRIGGER task_merge_operations_immutable_on_update; \
         DROP TRIGGER task_merge_operations_transition_on_update; \
         DROP TRIGGER task_merge_operations_source_consistency_on_update; \
         DROP TRIGGER task_merge_operations_source_reconciliation_on_update; \
         DROP TRIGGER task_merge_operations_journal_on_update; \
         DROP TRIGGER task_delivery_operation_transitions_no_update;",
    )
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_merge_operations SET failure_code = 'PROCESS_TREE_CLEANUP_FAILED' \
         WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions \
         SET failure_code = 'PROCESS_TREE_CLEANUP_FAILED' \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 4",
    )
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    assert_recovery_invariant(&store).await;
}
