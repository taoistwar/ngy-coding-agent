use coding_agent_store::{
    CleanupOperationAnchor, CleanupTransitionOutcome, GitCommitOid,
    RefreshBranchCleanupTargetRequest, StoreError,
};

use super::super::fixtures::branch_pending_fixture;
use super::snapshot::delivery_snapshot;

#[tokio::test]
async fn branch_refresh_rolls_back_when_the_target_observation_insert_faults() {
    assert_branch_refresh_fault_rolls_back(
        "codex/task7-refresh-observation-fault",
        "CREATE TRIGGER task7_refresh_observation_fault \
         BEFORE INSERT ON task_cleanup_target_head_observations \
         WHEN NEW.operation_version = 2 \
         BEGIN SELECT RAISE(ABORT, 'task7 refresh observation fault'); END;",
        "DROP TRIGGER task7_refresh_observation_fault;",
    )
    .await;
}

#[tokio::test]
async fn branch_refresh_rolls_back_when_the_cleanup_journal_insert_faults() {
    assert_branch_refresh_fault_rolls_back(
        "codex/task7-refresh-journal-fault",
        "CREATE TRIGGER task7_refresh_journal_fault \
         BEFORE INSERT ON task_delivery_operation_transitions \
         WHEN NEW.entity_kind = 'cleanup_operation' AND NEW.entity_version = 2 \
         BEGIN SELECT RAISE(ABORT, 'task7 refresh journal fault'); END;",
        "DROP TRIGGER task7_refresh_journal_fault;",
    )
    .await;
}

async fn assert_branch_refresh_fault_rolls_back(
    branch: &str,
    fault_sql: &'static str,
    cleanup_sql: &'static str,
) {
    let (fixture, task, operation_id, version, expected_head) =
        branch_pending_fixture(branch).await;
    let request = RefreshBranchCleanupTargetRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, operation_id, version).unwrap(),
        expected_head,
        target_head('4'),
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
            .refresh_branch_cleanup_target(request.clone())
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
            .refresh_branch_cleanup_target(request)
            .await
            .unwrap(),
        CleanupTransitionOutcome::Applied(_)
    ));
}

fn target_head(digit: char) -> GitCommitOid {
    digit.to_string().repeat(40).parse().unwrap()
}
