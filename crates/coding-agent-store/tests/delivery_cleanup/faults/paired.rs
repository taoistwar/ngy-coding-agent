use coding_agent_store::{
    CleanupOperationAnchor, CleanupTransitionOutcome, CompleteBranchCleanupRequest,
    CompleteWorktreeCleanupRequest, StoreError,
};

use super::super::fixtures::{branch_pending_fixture, remove_pending_fixture};
use super::snapshot::delivery_snapshot;

#[tokio::test]
async fn paired_completion_rolls_back_when_the_disposition_update_faults() {
    assert_worktree_completion_fault_rolls_back(
        "codex/task7-disposition-update-fault",
        "CREATE TRIGGER task7_disposition_update_fault \
         BEFORE UPDATE OF worktree_state ON task_artifact_dispositions \
         WHEN NEW.worktree_state = 'removed' \
         BEGIN SELECT RAISE(ABORT, 'task7 disposition update fault'); END;",
        "DROP TRIGGER task7_disposition_update_fault;",
    )
    .await;
}

#[tokio::test]
async fn paired_completion_rolls_back_when_the_disposition_journal_insert_faults() {
    assert_worktree_completion_fault_rolls_back(
        "codex/task7-disposition-journal-fault",
        "CREATE TRIGGER task7_disposition_journal_fault \
         BEFORE INSERT ON task_delivery_operation_transitions \
         WHEN NEW.entity_kind = 'worktree_disposition' AND NEW.entity_version = 3 \
         BEGIN SELECT RAISE(ABORT, 'task7 disposition journal fault'); END;",
        "DROP TRIGGER task7_disposition_journal_fault;",
    )
    .await;
}

#[tokio::test]
async fn paired_completion_rolls_back_when_the_cleanup_current_update_faults() {
    assert_worktree_completion_fault_rolls_back(
        "codex/task7-cleanup-update-fault",
        "CREATE TRIGGER task7_cleanup_update_fault \
         BEFORE UPDATE OF state ON task_cleanup_operations \
         WHEN NEW.kind = 'remove_worktree' AND NEW.state = 'completed' \
         BEGIN SELECT RAISE(ABORT, 'task7 cleanup update fault'); END;",
        "DROP TRIGGER task7_cleanup_update_fault;",
    )
    .await;
}

#[tokio::test]
async fn paired_completion_rolls_back_when_the_cleanup_journal_insert_faults() {
    assert_worktree_completion_fault_rolls_back(
        "codex/task7-cleanup-journal-fault",
        "CREATE TRIGGER task7_cleanup_journal_fault \
         BEFORE INSERT ON task_delivery_operation_transitions \
         WHEN NEW.entity_kind = 'cleanup_operation' AND NEW.entity_version = 4 \
         BEGIN SELECT RAISE(ABORT, 'task7 cleanup journal fault'); END;",
        "DROP TRIGGER task7_cleanup_journal_fault;",
    )
    .await;
}

#[tokio::test]
async fn branch_completion_rolls_back_when_the_disposition_journal_insert_faults() {
    let (fixture, task, operation_id, pending_version, _) =
        branch_pending_fixture("codex/task7-branch-disposition-journal-fault").await;
    let request = CompleteBranchCleanupRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, operation_id, pending_version).unwrap(),
    )
    .unwrap();
    let before = delivery_snapshot(&fixture.store, task.id).await;
    sqlx::raw_sql(
        "CREATE TRIGGER task7_branch_disposition_journal_fault \
         BEFORE INSERT ON task_delivery_operation_transitions \
         WHEN NEW.entity_kind = 'branch_disposition' AND NEW.entity_version = 2 \
         BEGIN SELECT RAISE(ABORT, 'task7 branch disposition journal fault'); END;",
    )
    .execute(fixture.store.pool())
    .await
    .unwrap();

    assert!(matches!(
        fixture.store.complete_branch_cleanup(request.clone()).await,
        Err(StoreError::Database(_))
    ));
    assert_eq!(delivery_snapshot(&fixture.store, task.id).await, before);

    sqlx::raw_sql("DROP TRIGGER task7_branch_disposition_journal_fault;")
        .execute(fixture.store.pool())
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .complete_branch_cleanup(request)
            .await
            .unwrap(),
        CleanupTransitionOutcome::Applied(_)
    ));
}

#[tokio::test]
async fn paired_completion_rolls_back_when_deferred_commit_validation_fails() {
    assert_worktree_completion_fault_rolls_back(
        "codex/task7-deferred-commit-fault",
        "CREATE TABLE task7_fault_parent (id INTEGER PRIMARY KEY) STRICT; \
         CREATE TABLE task7_fault_child ( \
             parent_id INTEGER NOT NULL, \
             FOREIGN KEY (parent_id) REFERENCES task7_fault_parent(id) \
                 DEFERRABLE INITIALLY DEFERRED \
         ) STRICT; \
         CREATE TRIGGER task7_deferred_commit_fault \
         AFTER UPDATE OF state ON task_cleanup_operations \
         WHEN NEW.kind = 'remove_worktree' AND NEW.state = 'completed' \
         BEGIN INSERT INTO task7_fault_child(parent_id) VALUES (1); END;",
        "DROP TRIGGER task7_deferred_commit_fault; \
         DROP TABLE task7_fault_child; \
         DROP TABLE task7_fault_parent;",
    )
    .await;
}

async fn assert_worktree_completion_fault_rolls_back(
    branch: &str,
    fault_sql: &'static str,
    cleanup_sql: &'static str,
) {
    let (fixture, task, operation_id, pending_version) = remove_pending_fixture(branch).await;
    let request = CompleteWorktreeCleanupRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, operation_id, pending_version).unwrap(),
    )
    .unwrap();
    let before = delivery_snapshot(&fixture.store, task.id).await;
    sqlx::raw_sql(fault_sql)
        .execute(fixture.store.pool())
        .await
        .unwrap();

    assert!(matches!(
        fixture
            .store
            .complete_worktree_cleanup(request.clone())
            .await,
        Err(StoreError::Database(_))
    ));
    assert_eq!(delivery_snapshot(&fixture.store, task.id).await, before);

    sqlx::raw_sql(cleanup_sql)
        .execute(fixture.store.pool())
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .complete_worktree_cleanup(request)
            .await
            .unwrap(),
        CleanupTransitionOutcome::Applied(_)
    ));
}
