use coding_agent_store::{
    AcceptedDeliverySourceState, CreateDeliverySourceOutcome, CreateDeliverySourceRequest,
    DeliveryRecoveryAction, DeliveryRecoveryBatch, DeliveryRecoveryDisposition,
    DeliveryRecoveryQuery, Store, StoreError,
};

use crate::authenticated_identity;
use crate::recovery_fixtures::{accepted, pending_preflight};
use crate::support::delivery::eligibility::{
    COMMON_IDENTITY, DELIVERY_TIMESTAMP, MERGE_BASE, MERGE_TREE,
};

#[tokio::test]
async fn explicit_delivery_transaction_rollback_leaves_recovery_snapshot_unchanged() {
    let store = crate::support::seeded_store().await;
    let (_, operation_id) =
        pending_preflight(&store, "codex/recovery-explicit-rollback", COMMON_IDENTITY).await;
    let before = recovery_batch(&store).await;

    let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await.unwrap();
    let updated = sqlx::query(
        "UPDATE task_merge_operations SET state = 'preflight_ready', version = 2, \
             merge_base_oid = ?, candidate_merge_tree_oid = ?, updated_at = ? \
         WHERE operation_id = ?",
    )
    .bind(MERGE_BASE)
    .bind(MERGE_TREE)
    .bind(DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(&mut *transaction)
    .await
    .unwrap();
    assert_eq!(updated.rows_affected(), 1);
    let in_transaction_journal_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(&mut *transaction)
    .await
    .unwrap();
    assert_eq!(in_transaction_journal_count, 2);
    transaction.rollback().await.unwrap();

    assert_eq!(recovery_batch(&store).await, before);
    assert_merge_state_and_journal(&store, operation_id, "preflight_pending", 1, 1).await;
}

#[tokio::test]
async fn delivery_source_journal_fault_rolls_back_current_state_and_recovery_does_not_drift() {
    let store = crate::support::seeded_store().await;
    let (task, operation_id, command) = accepted(
        &store,
        "codex/recovery-source-journal-fault",
        COMMON_IDENTITY,
    )
    .await;
    let before = recovery_batch(&store).await;
    install_source_journal_fault(&store).await;

    let result = store
        .create_delivery_source(CreateDeliverySourceRequest::try_new(command).unwrap())
        .await;
    assert!(matches!(result, Err(StoreError::Database(_))));
    remove_source_journal_fault(&store).await;

    assert_eq!(recovery_batch(&store).await, before);
    assert_accepted_without_source(&store, task.id, operation_id).await;
}

#[tokio::test]
async fn real_database_busy_writes_nothing_and_recovery_does_not_drift() {
    let fixture = crate::support::file_store().await;
    fixture.store.migrate().await.unwrap();
    crate::support::register_repository(&fixture.store, "task8-recovery-busy").await;
    let store = fixture.store.clone();
    let (task, operation_id, command) =
        accepted(&store, "codex/recovery-real-busy", COMMON_IDENTITY).await;
    let request = CreateDeliverySourceRequest::try_new(command).unwrap();
    let before = recovery_batch(&store).await;

    let mut connections = Vec::new();
    for _ in 0..5 {
        connections.push(store.pool().acquire().await.unwrap());
    }
    for connection in &mut connections {
        sqlx::query("PRAGMA busy_timeout = 0")
            .execute(&mut **connection)
            .await
            .unwrap();
    }
    let mut writer = connections.pop().unwrap();
    drop(connections);
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *writer)
        .await
        .unwrap();

    assert!(matches!(
        store.create_delivery_source(request).await,
        Err(StoreError::Database(_))
    ));
    sqlx::query("ROLLBACK").execute(&mut *writer).await.unwrap();
    drop(writer);

    assert_eq!(recovery_batch(&store).await, before);
    assert_accepted_without_source(&store, task.id, operation_id).await;
}

#[tokio::test]
async fn lost_reply_after_commit_is_recovered_from_only_the_committed_stage() {
    let store = crate::support::seeded_store().await;
    let (task, operation_id, command) = accepted(
        &store,
        "codex/recovery-commit-before-reply",
        COMMON_IDENTITY,
    )
    .await;

    let committed_reply = store
        .create_delivery_source(CreateDeliverySourceRequest::try_new(command).unwrap())
        .await
        .unwrap();
    assert!(matches!(
        committed_reply,
        CreateDeliverySourceOutcome::Created(_)
    ));
    drop(committed_reply);

    let first = recovery_batch(&store).await;
    let repeated = recovery_batch(&store).await;
    assert_eq!(first, repeated);
    assert_eq!(first.entries.len(), 1);
    let entry = &first.entries[0];
    assert_eq!(entry.identity.task_id(), task.id);
    assert!(matches!(
        entry.disposition,
        DeliveryRecoveryDisposition::Recover(DeliveryRecoveryAction::Accepted {
            operation_id: recovered,
            source: AcceptedDeliverySourceState::ObjectPending { .. },
            ..
        }) if recovered == operation_id
    ));
    assert_merge_state_and_journal(&store, operation_id, "accepted", 3, 3).await;
    let source_journal_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'delivery_source' AND entity_id = ?",
    )
    .bind(task.id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(source_journal_count, 1);
}

async fn recovery_batch(store: &Store) -> DeliveryRecoveryBatch {
    store
        .delivery_recovery_batch(&DeliveryRecoveryQuery::first(authenticated_identity()))
        .await
        .unwrap()
}

async fn assert_accepted_without_source(
    store: &Store,
    task_id: coding_agent_domain::TaskId,
    operation_id: coding_agent_store::DeliveryOperationId,
) {
    assert_merge_state_and_journal(store, operation_id, "accepted", 3, 3).await;
    let source_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_sources \
         WHERE origin_accepted_operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(source_count, 0);
    let source_journal_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'delivery_source' AND entity_id = ?",
    )
    .bind(task_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(source_journal_count, 0);
}

async fn assert_merge_state_and_journal(
    store: &Store,
    operation_id: coding_agent_store::DeliveryOperationId,
    expected_state: &str,
    expected_version: i64,
    expected_journal_count: i64,
) {
    let current: (String, i64) =
        sqlx::query_as("SELECT state, version FROM task_merge_operations WHERE operation_id = ?")
            .bind(operation_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(current, (expected_state.to_owned(), expected_version));
    let journal_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(journal_count, expected_journal_count);
}

async fn install_source_journal_fault(store: &Store) {
    sqlx::raw_sql(
        "CREATE TRIGGER task8_recovery_source_journal_fault \
         BEFORE INSERT ON task_delivery_operation_transitions \
         WHEN NEW.entity_kind = 'delivery_source' AND NEW.entity_version = 1 \
         BEGIN SELECT RAISE(ABORT, 'task8 source journal fault'); END;",
    )
    .execute(store.pool())
    .await
    .unwrap();
}

async fn remove_source_journal_fault(store: &Store) {
    sqlx::raw_sql("DROP TRIGGER task8_recovery_source_journal_fault;")
        .execute(store.pool())
        .await
        .unwrap();
}
