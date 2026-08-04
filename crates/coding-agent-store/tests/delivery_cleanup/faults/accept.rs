use coding_agent_domain::{ClientRequestId, TaskId};
use coding_agent_store::{CleanupAcceptanceOutcome, Store, StoreError};

use super::super::fixtures::{
    delete_request, file_backed_merged_fixture, remove_request, remove_worktree_fully,
};
use super::snapshot::delivery_snapshot;

#[tokio::test]
async fn worktree_accept_rolls_back_when_the_cleanup_operation_insert_faults() {
    let (fixture, task, _) = file_backed_merged_fixture("codex/task7-accept-operation-fault").await;
    let request = remove_request(&fixture.store, &task, ClientRequestId::new()).await;
    assert_accept_fault_rolls_back(
        &fixture.store,
        task.id,
        request,
        "CREATE TRIGGER task7_accept_operation_fault \
         BEFORE INSERT ON task_cleanup_operations \
         WHEN NEW.kind = 'remove_worktree' \
         BEGIN SELECT RAISE(ABORT, 'task7 accept operation fault'); END;",
        "DROP TRIGGER task7_accept_operation_fault;",
    )
    .await;
}

#[tokio::test]
async fn branch_accept_rolls_back_when_the_cleanup_journal_insert_faults() {
    let (fixture, task, _) = file_backed_merged_fixture("codex/task7-accept-journal-fault").await;
    remove_worktree_fully(&fixture.store, &task).await;
    let request = delete_request(
        &fixture.store,
        &task,
        ClientRequestId::new(),
        "3333333333333333333333333333333333333333".parse().unwrap(),
    )
    .await;
    assert_branch_accept_fault_rolls_back(
        &fixture.store,
        task.id,
        request,
        "CREATE TRIGGER task7_accept_journal_fault \
         BEFORE INSERT ON task_delivery_operation_transitions \
         WHEN NEW.entity_kind = 'cleanup_operation' AND NEW.entity_version = 1 \
         BEGIN SELECT RAISE(ABORT, 'task7 accept journal fault'); END;",
        "DROP TRIGGER task7_accept_journal_fault;",
    )
    .await;
}

#[tokio::test]
async fn branch_accept_rolls_back_when_the_command_receipt_insert_faults() {
    let (fixture, task, _) = file_backed_merged_fixture("codex/task7-accept-receipt-fault").await;
    remove_worktree_fully(&fixture.store, &task).await;
    let request = delete_request(
        &fixture.store,
        &task,
        ClientRequestId::new(),
        "3333333333333333333333333333333333333333".parse().unwrap(),
    )
    .await;
    assert_branch_accept_fault_rolls_back(
        &fixture.store,
        task.id,
        request,
        "CREATE TRIGGER task7_accept_receipt_fault \
         BEFORE INSERT ON task_delivery_command_receipts \
         WHEN NEW.command_kind = 'delete_branch' \
         BEGIN SELECT RAISE(ABORT, 'task7 accept receipt fault'); END;",
        "DROP TRIGGER task7_accept_receipt_fault;",
    )
    .await;
}

async fn assert_accept_fault_rolls_back(
    store: &Store,
    task_id: TaskId,
    request: coding_agent_store::RemoveWorktreeCommandRequest,
    fault_sql: &'static str,
    cleanup_sql: &'static str,
) {
    let before = delivery_snapshot(store, task_id).await;
    sqlx::raw_sql(fault_sql)
        .execute(store.pool())
        .await
        .unwrap();

    assert!(matches!(
        store.accept_worktree_cleanup(request.clone()).await,
        Err(StoreError::Database(_))
    ));
    assert_eq!(delivery_snapshot(store, task_id).await, before);

    sqlx::raw_sql(cleanup_sql)
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(
        store.accept_worktree_cleanup(request).await.unwrap(),
        CleanupAcceptanceOutcome::Accepted(_)
    ));
}

async fn assert_branch_accept_fault_rolls_back(
    store: &Store,
    task_id: TaskId,
    request: coding_agent_store::DeleteBranchCommandRequest,
    fault_sql: &'static str,
    cleanup_sql: &'static str,
) {
    let before = delivery_snapshot(store, task_id).await;
    sqlx::raw_sql(fault_sql)
        .execute(store.pool())
        .await
        .unwrap();

    assert!(matches!(
        store.accept_branch_cleanup(request.clone()).await,
        Err(StoreError::Database(_))
    ));
    assert_eq!(delivery_snapshot(store, task_id).await, before);

    sqlx::raw_sql(cleanup_sql)
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(
        store.accept_branch_cleanup(request).await.unwrap(),
        CleanupAcceptanceOutcome::Accepted(_)
    ));
}
