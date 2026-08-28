use std::str::FromStr;

use coding_agent_domain::{ClientRequestId, Task};
use coding_agent_store::{
    AcceptMergeOutcome, BeginMergeAbortRequest, DirectoryIdentity, GitBranchRef, GitCommitOid,
    MergeAbortProof, MergeAutostashObservation, MergeConflictPaths, MergeTransitionOutcome,
    OtherGitOperationObservation, Sha256Digest, StoreError,
};
use uuid::Uuid;

use crate::support::delivery::eligibility::{
    ADMIN_IDENTITY, COMMON_IDENTITY, CONFIG_DIGEST, SOURCE_COMMIT, TARGET_HEAD,
    approved_task_on_store,
};

use super::fixtures::{
    TARGET_BRANCH, accept_command, file_backed_pending_preflight, merge_pending_on_store,
};

const INDEX_STAGES: &str = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";
const WORKTREE: &str = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2";

#[tokio::test]
async fn distinct_accept_ids_race_to_one_receipt_and_one_typed_conflict() {
    let (fixture, task, operation_id) = file_backed_pending_preflight().await;
    let store = fixture.store.clone();
    super::preflight_results::ready(&store, task.id, operation_id).await;
    let first = accept_command(&store, &task, operation_id, ClientRequestId::new()).await;
    let second = accept_command(&store, &task, operation_id, ClientRequestId::new()).await;

    let (first_outcome, second_outcome) =
        tokio::join!(store.accept_merge(first), store.accept_merge(second));
    let outcomes = [first_outcome.unwrap(), second_outcome.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AcceptMergeOutcome::Accepted(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AcceptMergeOutcome::Conflict))
            .count(),
        1
    );
    let receipt_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_command_receipts \
         WHERE operation_id = ? AND command_kind = 'accept_merge'",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(receipt_count, 1);
}

#[tokio::test]
async fn real_database_busy_during_accept_writes_nothing() {
    let (fixture, task, operation_id) = file_backed_pending_preflight().await;
    let store = fixture.store.clone();
    super::preflight_results::ready(&store, task.id, operation_id).await;
    let request = accept_command(&store, &task, operation_id, ClientRequestId::new()).await;

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
        store.accept_merge(request.clone()).await,
        Err(StoreError::Database(_))
    ));
    sqlx::query("ROLLBACK").execute(&mut *writer).await.unwrap();
    drop(writer);
    let row: (String, i64, Option<String>) = sqlx::query_as(
        "SELECT state, version, accept_receipt_id \
         FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(row, ("preflight_ready".to_owned(), 3, None));
    assert!(matches!(
        store.accept_merge(request).await.unwrap(),
        AcceptMergeOutcome::Accepted(_)
    ));
}

#[tokio::test]
async fn abort_child_receipt_is_globally_unique_under_race_and_raw_duplicates_fail_closed() {
    let fixture = crate::support::file_store().await;
    fixture.store.migrate().await.unwrap();
    crate::support::register_repository(&fixture.store, "task6-abort-race").await;
    let store = fixture.store.clone();
    let (_, first_task) = approved_task_on_store(store.clone(), "codex/task-abort-one", 0).await;
    let (_, second_task) = approved_task_on_store(store.clone(), "codex/task-abort-two", 0).await;
    let (first_operation, first_version) = merge_pending_on_store(&store, &first_task).await;
    let (second_operation, second_version) = merge_pending_on_store(&store, &second_task).await;
    let shared_child = Uuid::new_v4();
    let first = abort_request(
        &first_task,
        first_operation,
        first_version,
        "codex/task-abort-one",
        shared_child,
    );
    let second = abort_request(
        &second_task,
        second_operation,
        second_version,
        "codex/task-abort-two",
        shared_child,
    );

    let (first_outcome, second_outcome) = tokio::join!(
        store.begin_merge_abort(first.clone()),
        store.begin_merge_abort(second.clone())
    );
    let first_outcome = first_outcome.unwrap();
    let second_outcome = second_outcome.unwrap();
    let (winner, loser, loser_task, loser_operation, loser_version, loser_branch) =
        match (&first_outcome, &second_outcome) {
            (MergeTransitionOutcome::Applied(_), MergeTransitionOutcome::Conflict) => (
                first,
                second,
                &second_task,
                second_operation,
                second_version,
                "codex/task-abort-two",
            ),
            (MergeTransitionOutcome::Conflict, MergeTransitionOutcome::Applied(_)) => (
                second,
                first,
                &first_task,
                first_operation,
                first_version,
                "codex/task-abort-one",
            ),
            other => panic!("expected one abort winner and one conflict, got {other:?}"),
        };
    assert!(matches!(
        store.begin_merge_abort(winner.clone()).await.unwrap(),
        MergeTransitionOutcome::Existing(_)
    ));
    let loser_row: (String, i64, Option<String>) = sqlx::query_as(
        "SELECT state, version, abort_child_receipt_id \
         FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(loser_operation.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(loser_row, ("merge_pending".to_owned(), 5, None));
    assert!(matches!(
        store.begin_merge_abort(loser).await.unwrap(),
        MergeTransitionOutcome::Conflict
    ));

    let distinct = abort_request(
        loser_task,
        loser_operation,
        loser_version,
        loser_branch,
        Uuid::new_v4(),
    );
    assert!(matches!(
        store.begin_merge_abort(distinct).await.unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));
    sqlx::raw_sql(
        "DROP INDEX task_merge_operations_abort_child_receipt_unique; \
         DROP TRIGGER task_merge_operations_immutable_on_update; \
         DROP TRIGGER task_merge_operations_transition_on_update; \
         DROP TRIGGER task_merge_operations_source_consistency_on_update; \
         DROP TRIGGER task_merge_operations_source_reconciliation_on_update; \
         DROP TRIGGER task_merge_operations_journal_on_update;",
    )
    .execute(store.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_merge_operations SET abort_child_receipt_id = ? WHERE operation_id = ?",
    )
    .bind(shared_child.to_string())
    .bind(loser_operation.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    assert!(matches!(
        store.begin_merge_abort(winner).await,
        Err(StoreError::InvariantViolation(_))
    ));
}

fn abort_request(
    task: &Task,
    operation_id: coding_agent_store::DeliveryOperationId,
    version: coding_agent_store::DeliveryVersion,
    source_branch: &str,
    child_receipt_id: Uuid,
) -> BeginMergeAbortRequest {
    let proof = MergeAbortProof::try_new(
        child_receipt_id,
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
        GitBranchRef::from_str(&format!("refs/heads/{source_branch}")).unwrap(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
        "codex-reserved".to_owned(),
        Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
        Sha256Digest::from_str(INDEX_STAGES).unwrap(),
        Sha256Digest::from_str(WORKTREE).unwrap(),
        MergeAutostashObservation::Absent,
        OtherGitOperationObservation::Clear,
        MergeConflictPaths::try_from_raw(vec![b"src/conflicted.rs".to_vec()]).unwrap(),
    )
    .unwrap();
    BeginMergeAbortRequest::try_new(task.id, operation_id, version, proof).unwrap()
}
