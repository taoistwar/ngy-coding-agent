use coding_agent_domain::ClientRequestId;
use coding_agent_store::{
    AcceptMergeOutcome, BeginMergeAbortRequest, CompleteMergeAbortRequest, MergeConflictPaths,
    MergeTransitionOutcome, Store, StoreError,
};
use uuid::Uuid;

use super::fixtures::{accept_command, complete_merge_request, merge_pending, pending_preflight};

#[tokio::test]
async fn accept_rolls_back_when_the_transition_journal_insert_faults() {
    assert_accept_fault_rolls_back(
        "CREATE TRIGGER task6_accept_journal_fault \
         BEFORE INSERT ON task_delivery_operation_transitions \
         WHEN NEW.entity_kind = 'merge_operation' AND NEW.entity_version = 3 \
         BEGIN SELECT RAISE(ABORT, 'task6 accept journal fault'); END;",
        "DROP TRIGGER task6_accept_journal_fault;",
    )
    .await;
}

#[tokio::test]
async fn accept_rolls_back_when_the_command_receipt_insert_faults() {
    assert_accept_fault_rolls_back(
        "CREATE TRIGGER task6_accept_receipt_fault \
         BEFORE INSERT ON task_delivery_command_receipts \
         WHEN NEW.command_kind = 'accept_merge' \
         BEGIN SELECT RAISE(ABORT, 'task6 accept receipt fault'); END;",
        "DROP TRIGGER task6_accept_receipt_fault;",
    )
    .await;
}

#[tokio::test]
async fn accept_rolls_back_when_deferred_commit_validation_fails() {
    assert_accept_fault_rolls_back(
        "CREATE TABLE task6_accept_fault_parent (id INTEGER PRIMARY KEY) STRICT; \
         CREATE TABLE task6_accept_fault_child ( \
             parent_id INTEGER NOT NULL, \
             FOREIGN KEY (parent_id) REFERENCES task6_accept_fault_parent(id) \
                 DEFERRABLE INITIALLY DEFERRED \
         ) STRICT; \
         CREATE TRIGGER task6_accept_deferred_fault \
         AFTER INSERT ON task_delivery_command_receipts \
         WHEN NEW.command_kind = 'accept_merge' \
         BEGIN INSERT INTO task6_accept_fault_child(parent_id) VALUES (1); END;",
        "DROP TRIGGER task6_accept_deferred_fault; \
         DROP TABLE task6_accept_fault_child; \
         DROP TABLE task6_accept_fault_parent;",
    )
    .await;
}

#[tokio::test]
async fn merged_rolls_back_when_the_operation_update_faults() {
    assert_merged_fault_rolls_back(
        "CREATE TRIGGER task6_merged_update_fault \
         BEFORE UPDATE OF state ON task_merge_operations \
         WHEN NEW.state = 'merged' \
         BEGIN SELECT RAISE(ABORT, 'task6 merged update fault'); END;",
        "DROP TRIGGER task6_merged_update_fault;",
    )
    .await;
}

#[tokio::test]
async fn merged_rolls_back_when_the_merge_operation_journal_insert_faults() {
    assert_merged_fault_rolls_back(
        "CREATE TRIGGER task6_merged_operation_journal_fault \
         BEFORE INSERT ON task_delivery_operation_transitions \
         WHEN NEW.entity_kind = 'merge_operation' AND NEW.entity_version = 5 \
         BEGIN SELECT RAISE(ABORT, 'task6 merged operation journal fault'); END;",
        "DROP TRIGGER task6_merged_operation_journal_fault;",
    )
    .await;
}

#[tokio::test]
async fn merged_rolls_back_when_the_disposition_insert_faults() {
    assert_merged_fault_rolls_back(
        "CREATE TRIGGER task6_merged_disposition_fault \
         BEFORE INSERT ON task_artifact_dispositions \
         BEGIN SELECT RAISE(ABORT, 'task6 disposition fault'); END;",
        "DROP TRIGGER task6_merged_disposition_fault;",
    )
    .await;
}

#[tokio::test]
async fn merged_rolls_back_when_an_initial_disposition_journal_faults() {
    assert_merged_fault_rolls_back(
        "CREATE TRIGGER task6_merged_journal_fault \
         BEFORE INSERT ON task_delivery_operation_transitions \
         WHEN NEW.entity_kind = 'worktree_disposition' AND NEW.entity_version = 1 \
         BEGIN SELECT RAISE(ABORT, 'task6 disposition journal fault'); END;",
        "DROP TRIGGER task6_merged_journal_fault;",
    )
    .await;
}

#[tokio::test]
async fn merged_rolls_back_when_the_branch_disposition_journal_faults() {
    assert_merged_fault_rolls_back(
        "CREATE TRIGGER task6_merged_branch_journal_fault \
         BEFORE INSERT ON task_delivery_operation_transitions \
         WHEN NEW.entity_kind = 'branch_disposition' AND NEW.entity_version = 1 \
         BEGIN SELECT RAISE(ABORT, 'task6 branch disposition journal fault'); END;",
        "DROP TRIGGER task6_merged_branch_journal_fault;",
    )
    .await;
}

#[tokio::test]
async fn merged_rolls_back_when_deferred_commit_validation_fails() {
    assert_merged_fault_rolls_back(
        "CREATE TABLE task6_merged_fault_parent (id INTEGER PRIMARY KEY) STRICT; \
         CREATE TABLE task6_merged_fault_child ( \
             parent_id INTEGER NOT NULL, \
             FOREIGN KEY (parent_id) REFERENCES task6_merged_fault_parent(id) \
                 DEFERRABLE INITIALLY DEFERRED \
         ) STRICT; \
         CREATE TRIGGER task6_merged_deferred_fault \
         AFTER INSERT ON task_artifact_dispositions \
         BEGIN INSERT INTO task6_merged_fault_child(parent_id) VALUES (1); END;",
        "DROP TRIGGER task6_merged_deferred_fault; \
         DROP TABLE task6_merged_fault_child; \
         DROP TABLE task6_merged_fault_parent;",
    )
    .await;
}

#[tokio::test]
async fn abort_completion_rolls_back_when_a_conflict_child_insert_faults() {
    let (store, task, operation_id, abort_version) = super::abort::abort_pending_fixture().await;
    let request = CompleteMergeAbortRequest::try_new(
        task.id,
        operation_id,
        abort_version,
        super::abort::exact_abort_applied_proof(),
        MergeConflictPaths::try_from_raw(vec![
            b"src/first-conflict.rs".to_vec(),
            b"src/second-conflict.rs".to_vec(),
        ])
        .unwrap(),
    )
    .unwrap();
    sqlx::raw_sql(
        "CREATE TRIGGER task6_abort_conflict_child_fault \
         BEFORE INSERT ON task_merge_conflicts \
         WHEN NEW.ordinal = 1 \
         BEGIN SELECT RAISE(ABORT, 'task6 abort conflict child fault'); END;",
    )
    .execute(store.pool())
    .await
    .unwrap();

    assert!(matches!(
        store.complete_merge_abort(request.clone()).await,
        Err(StoreError::Database(_))
    ));
    assert_abort_pending_without_conflict_children(&store, operation_id).await;

    sqlx::raw_sql("DROP TRIGGER task6_abort_conflict_child_fault;")
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(
        store.complete_merge_abort(request).await.unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));
}

#[tokio::test]
async fn begin_abort_rolls_back_when_the_transition_journal_insert_faults() {
    let (store, task, operation_id, pending_version) = merge_pending().await;
    let request = BeginMergeAbortRequest::try_new(
        task.id,
        operation_id,
        pending_version,
        super::abort::exact_abort_begin_proof(Uuid::new_v4()),
    )
    .unwrap();
    sqlx::raw_sql(
        "CREATE TRIGGER task6_begin_abort_journal_fault \
         BEFORE INSERT ON task_delivery_operation_transitions \
         WHEN NEW.entity_kind = 'merge_operation' AND NEW.entity_version = 5 \
         BEGIN SELECT RAISE(ABORT, 'task6 begin abort journal fault'); END;",
    )
    .execute(store.pool())
    .await
    .unwrap();

    assert!(matches!(
        store.begin_merge_abort(request.clone()).await,
        Err(StoreError::Database(_))
    ));
    assert_merge_pending_without_abort_facts(&store, operation_id).await;

    sqlx::raw_sql("DROP TRIGGER task6_begin_abort_journal_fault;")
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(
        store.begin_merge_abort(request).await.unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));
}

async fn assert_accept_fault_rolls_back(fault_sql: &'static str, cleanup_sql: &'static str) {
    let (store, task, operation_id) = pending_preflight().await;
    super::preflight_results::ready(&store, task.id, operation_id).await;
    let request = accept_command(&store, &task, operation_id, ClientRequestId::new()).await;
    sqlx::raw_sql(fault_sql)
        .execute(store.pool())
        .await
        .unwrap();

    assert!(matches!(
        store.accept_merge(request.clone()).await,
        Err(StoreError::Database(_))
    ));
    assert_ready_without_accept_writes(&store, operation_id).await;

    sqlx::raw_sql(cleanup_sql)
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(
        store.accept_merge(request).await.unwrap(),
        AcceptMergeOutcome::Accepted(_)
    ));
}

async fn assert_ready_without_accept_writes(
    store: &Store,
    operation_id: coding_agent_store::DeliveryOperationId,
) {
    let row: (String, i64, Option<String>) = sqlx::query_as(
        "SELECT state, version, accept_receipt_id \
         FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(row, ("preflight_ready".to_owned(), 2, None));
    let journal_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(journal_count, 2);
    let receipt_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_command_receipts \
         WHERE operation_id = ? AND command_kind = 'accept_merge'",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(receipt_count, 0);
}

async fn assert_merged_fault_rolls_back(fault_sql: &'static str, cleanup_sql: &'static str) {
    let (store, task, operation_id, pending_version) = merge_pending().await;
    let request = complete_merge_request(&store, &task, operation_id, pending_version).await;
    sqlx::raw_sql(fault_sql)
        .execute(store.pool())
        .await
        .unwrap();

    assert!(matches!(
        store.complete_merge(request.clone()).await,
        Err(StoreError::Database(_))
    ));
    assert_merge_pending_without_disposition(&store, task.id, operation_id).await;

    sqlx::raw_sql(cleanup_sql)
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(
        store.complete_merge(request).await.unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));
}

async fn assert_merge_pending_without_disposition(
    store: &Store,
    task_id: coding_agent_domain::TaskId,
    operation_id: coding_agent_store::DeliveryOperationId,
) {
    let row: (String, i64, Option<String>) = sqlx::query_as(
        "SELECT state, version, merged_disposition_task_id \
         FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(row, ("merge_pending".to_owned(), 4, None));
    let journal_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(journal_count, 4);
    let disposition_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_artifact_dispositions WHERE task_id = ?")
            .bind(task_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(disposition_count, 0);
}

async fn assert_abort_pending_without_conflict_children(
    store: &Store,
    operation_id: coding_agent_store::DeliveryOperationId,
) {
    let row: (String, i64, Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT state, version, conflict_path_count, failure_code \
         FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(row, ("abort_pending".to_owned(), 5, None, None));
    let journal_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(journal_count, 5);
    let conflict_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_merge_conflicts WHERE operation_id = ?")
            .bind(operation_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(conflict_count, 0);
}

async fn assert_merge_pending_without_abort_facts(
    store: &Store,
    operation_id: coding_agent_store::DeliveryOperationId,
) {
    let row: (
        String,
        i64,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT state, version, abort_child_receipt_id, abort_merge_head_oid, \
                abort_index_stages_digest, abort_worktree_digest \
         FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(row, ("merge_pending".to_owned(), 4, None, None, None, None));
    let journal_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(journal_count, 4);
}
