use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use coding_agent_domain::ClientRequestId;
use coding_agent_store::{
    BranchCleanupKnownNotAppliedReason, CleanupAcceptanceOutcome, CleanupOperationAnchor,
    CleanupOperationState, CleanupReconciliationReason, CleanupTransitionOutcome,
    CompleteBranchCleanupRequest, CompleteWorktreeCleanupRequest, GitCommitOid,
    ReconcileBranchCleanupRequest, ReconcileWorktreeCleanupRequest,
    RecordBranchCleanupFailureRequest, RecordWorktreeCleanupFailureRequest,
    RefreshBranchCleanupTargetRequest, StoreError, WorktreeCleanupKnownNotAppliedReason,
};
use tokio::sync::Barrier;

use super::fixtures::{
    branch_pending_fixture, delete_request, file_backed_merged_fixture, remove_pending_fixture,
    remove_request, remove_worktree_fully,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_same_cleanup_receipt_is_one_accepted_one_existing() {
    let (fixture, task, _) =
        file_backed_merged_fixture("codex/task7-concurrent-same-receipt").await;
    let store = fixture.store.clone();
    let request = remove_request(&store, &task, ClientRequestId::new()).await;
    let before = cleanup_counts(&store, task.id).await;

    let mut contenders = independent_stores(&fixture.database_path, 2).await;
    let second_store = contenders.pop().unwrap();
    let first_store = contenders.pop().unwrap();
    let first_request = request.clone();
    let outcomes: Vec<_> = race(vec![
        Box::pin(async move {
            first_store
                .accept_worktree_cleanup(first_request)
                .await
                .unwrap()
        }),
        Box::pin(async move { second_store.accept_worktree_cleanup(request).await.unwrap() }),
    ])
    .await;
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, CleanupAcceptanceOutcome::Accepted(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, CleanupAcceptanceOutcome::Existing(_)))
            .count(),
        1
    );
    let accepted = outcomes
        .iter()
        .find_map(|outcome| match outcome {
            CleanupAcceptanceOutcome::Accepted(receipt) => Some(receipt),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        cleanup_counts(&store, task.id).await,
        (before.0 + 1, before.1 + 1, before.2)
    );
    assert_eq!(
        operation_history_counts(&store, accepted.operation_id).await,
        (1, 0)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_distinct_cleanup_receipts_are_one_accepted_one_conflict() {
    let (fixture, task, _) =
        file_backed_merged_fixture("codex/task7-concurrent-distinct-receipt").await;
    let store = fixture.store.clone();
    let first_request = remove_request(&store, &task, ClientRequestId::new()).await;
    let second_request = remove_request(&store, &task, ClientRequestId::new()).await;
    let before = cleanup_counts(&store, task.id).await;

    let mut contenders = independent_stores(&fixture.database_path, 2).await;
    let second_store = contenders.pop().unwrap();
    let first_store = contenders.pop().unwrap();
    let outcomes: Vec<_> = race(vec![
        Box::pin(async move {
            first_store
                .accept_worktree_cleanup(first_request)
                .await
                .unwrap()
        }),
        Box::pin(async move {
            second_store
                .accept_worktree_cleanup(second_request)
                .await
                .unwrap()
        }),
    ])
    .await;
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, CleanupAcceptanceOutcome::Accepted(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, CleanupAcceptanceOutcome::Conflict))
            .count(),
        1
    );
    let accepted = outcomes
        .iter()
        .find_map(|outcome| match outcome {
            CleanupAcceptanceOutcome::Accepted(receipt) => Some(receipt),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        cleanup_counts(&store, task.id).await,
        (before.0 + 1, before.1 + 1, before.2)
    );
    assert_eq!(
        operation_history_counts(&store, accepted.operation_id).await,
        (1, 0)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worktree_complete_failure_and_reconcile_race_to_one_winner() {
    let (fixture, task, operation_id, version) =
        remove_pending_fixture("codex/task7-concurrent-worktree-phase").await;
    let complete = CompleteWorktreeCleanupRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, operation_id, version).unwrap(),
    )
    .unwrap();
    let failure = RecordWorktreeCleanupFailureRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, operation_id, version).unwrap(),
        CleanupOperationState::RemovePending,
        WorktreeCleanupKnownNotAppliedReason::TargetWorktreeDirty,
    )
    .unwrap();
    let reconcile = ReconcileWorktreeCleanupRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, operation_id, version).unwrap(),
        CleanupOperationState::RemovePending,
        CleanupReconciliationReason::WorktreeIdentityMismatch,
    )
    .unwrap();

    let mut contenders = independent_stores(&fixture.database_path, 3).await;
    let reconcile_store = contenders.pop().unwrap();
    let failure_store = contenders.pop().unwrap();
    let complete_store = contenders.pop().unwrap();
    let outcomes = race(vec![
        Box::pin(async move {
            complete_store
                .complete_worktree_cleanup(complete)
                .await
                .unwrap()
        }),
        Box::pin(async move {
            failure_store
                .record_worktree_cleanup_failure(failure)
                .await
                .unwrap()
        }),
        Box::pin(async move {
            reconcile_store
                .reconcile_worktree_cleanup(reconcile)
                .await
                .unwrap()
        }),
    ])
    .await;
    assert_one_winner(&outcomes);
    assert_current_matches_winner(&fixture.store, task.id, operation_id, &outcomes).await;
    assert_eq!(
        operation_history_counts(&fixture.store, operation_id).await,
        (4, 0)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn branch_refresh_complete_failure_and_reconcile_race_to_one_winner() {
    let (fixture, task, operation_id, version, head) =
        branch_pending_fixture("codex/task7-concurrent-branch-phase").await;
    let refreshed_head =
        GitCommitOid::from_str("4444444444444444444444444444444444444444").unwrap();
    let refresh = RefreshBranchCleanupTargetRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, operation_id, version).unwrap(),
        head,
        refreshed_head,
    )
    .unwrap();
    let complete = CompleteBranchCleanupRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, operation_id, version).unwrap(),
    )
    .unwrap();
    let failure = RecordBranchCleanupFailureRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, operation_id, version).unwrap(),
        BranchCleanupKnownNotAppliedReason::SourceBranchNotMerged,
    )
    .unwrap();
    let reconcile = ReconcileBranchCleanupRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, operation_id, version).unwrap(),
        CleanupReconciliationReason::SourceInconsistent,
    )
    .unwrap();

    let mut contenders = independent_stores(&fixture.database_path, 4).await;
    let reconcile_store = contenders.pop().unwrap();
    let failure_store = contenders.pop().unwrap();
    let complete_store = contenders.pop().unwrap();
    let refresh_store = contenders.pop().unwrap();
    let outcomes = race(vec![
        Box::pin(async move {
            refresh_store
                .refresh_branch_cleanup_target(refresh)
                .await
                .unwrap()
        }),
        Box::pin(async move {
            complete_store
                .complete_branch_cleanup(complete)
                .await
                .unwrap()
        }),
        Box::pin(async move {
            failure_store
                .record_branch_cleanup_failure(failure)
                .await
                .unwrap()
        }),
        Box::pin(async move {
            reconcile_store
                .reconcile_branch_cleanup(reconcile)
                .await
                .unwrap()
        }),
    ])
    .await;
    assert_one_winner(&outcomes);
    assert_current_matches_winner(&fixture.store, task.id, operation_id, &outcomes).await;
    assert_eq!(
        operation_history_counts(&fixture.store, operation_id).await,
        (2, 2)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_exact_branch_refresh_is_one_applied_one_existing() {
    let (fixture, task, operation_id, version, head) =
        branch_pending_fixture("codex/task7-concurrent-exact-branch-refresh").await;
    let store = fixture.store.clone();
    let refreshed_head =
        GitCommitOid::from_str("4444444444444444444444444444444444444444").unwrap();
    let request = RefreshBranchCleanupTargetRequest::try_new(
        CleanupOperationAnchor::try_new(task.id, operation_id, version).unwrap(),
        head,
        refreshed_head,
    )
    .unwrap();

    let mut contenders = independent_stores(&fixture.database_path, 2).await;
    let second_store = contenders.pop().unwrap();
    let first_store = contenders.pop().unwrap();
    let first_request = request.clone();
    let outcomes: Vec<_> = race(vec![
        Box::pin(async move {
            first_store
                .refresh_branch_cleanup_target(first_request)
                .await
                .unwrap()
        }),
        Box::pin(async move {
            second_store
                .refresh_branch_cleanup_target(request)
                .await
                .unwrap()
        }),
    ])
    .await;
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, CleanupTransitionOutcome::Applied(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, CleanupTransitionOutcome::Existing(_)))
            .count(),
        1
    );
    assert_eq!(operation_history_counts(&store, operation_id).await, (2, 2));
}

#[tokio::test]
async fn real_database_busy_during_cleanup_acceptance_writes_nothing() {
    let (fixture, task, _) = file_backed_merged_fixture("codex/task7-cleanup-real-busy").await;
    let store = fixture.store.clone();
    remove_worktree_fully(&store, &task).await;
    let request = delete_request(
        &store,
        &task,
        ClientRequestId::new(),
        GitCommitOid::from_str("3333333333333333333333333333333333333333").unwrap(),
    )
    .await;
    let before = cleanup_counts(&store, task.id).await;

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
        store.accept_branch_cleanup(request.clone()).await,
        Err(StoreError::Database(_))
    ));
    sqlx::query("ROLLBACK").execute(&mut *writer).await.unwrap();
    drop(writer);
    assert_eq!(cleanup_counts(&store, task.id).await, before);
    assert!(matches!(
        store.accept_branch_cleanup(request).await.unwrap(),
        CleanupAcceptanceOutcome::Accepted(_)
    ));
}

fn assert_one_winner(outcomes: &[CleanupTransitionOutcome]) {
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, CleanupTransitionOutcome::Applied(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, CleanupTransitionOutcome::Conflict))
            .count(),
        outcomes.len() - 1
    );
}

type RaceFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

async fn race<T: Send + 'static>(contenders: Vec<RaceFuture<T>>) -> Vec<T> {
    let gate = Arc::new(Barrier::new(contenders.len() + 1));
    let handles: Vec<_> = contenders
        .into_iter()
        .map(|contender| {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                gate.wait().await;
                contender.await
            })
        })
        .collect();
    gate.wait().await;

    let mut outcomes = Vec::with_capacity(handles.len());
    for handle in handles {
        outcomes.push(handle.await.unwrap());
    }
    outcomes
}

async fn independent_stores(path: &Path, count: usize) -> Vec<coding_agent_store::Store> {
    let mut stores = Vec::with_capacity(count);
    for _ in 0..count {
        stores.push(coding_agent_store::Store::open(path).await.unwrap());
    }
    stores
}

async fn assert_current_matches_winner(
    store: &coding_agent_store::Store,
    task_id: coding_agent_domain::TaskId,
    operation_id: coding_agent_store::DeliveryOperationId,
    outcomes: &[CleanupTransitionOutcome],
) {
    let winner = outcomes
        .iter()
        .find_map(|outcome| match outcome {
            CleanupTransitionOutcome::Applied(receipt) => Some(receipt),
            _ => None,
        })
        .unwrap();
    let operation = store
        .delivery_ownership_snapshot(task_id)
        .await
        .unwrap()
        .unwrap()
        .cleanup_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    assert_eq!(operation.version, winner.version);
    assert_eq!(operation.state, winner.state);
    assert_eq!(operation.failure_code, winner.failure_code);
}

async fn cleanup_counts(
    store: &coding_agent_store::Store,
    task_id: coding_agent_domain::TaskId,
) -> (i64, i64, i64) {
    sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM task_cleanup_operations WHERE task_id = ?), \
                (SELECT COUNT(*) FROM task_delivery_command_receipts WHERE task_id = ?), \
                (SELECT COUNT(*) FROM task_cleanup_target_head_observations)",
    )
    .bind(task_id.to_string())
    .bind(task_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap()
}

async fn operation_history_counts(
    store: &coding_agent_store::Store,
    operation_id: coding_agent_store::DeliveryOperationId,
) -> (i64, i64) {
    sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM task_delivery_operation_transitions \
                 WHERE entity_kind = 'cleanup_operation' AND entity_id = ?), \
                (SELECT COUNT(*) FROM task_cleanup_target_head_observations \
                 WHERE cleanup_operation_id = ?)",
    )
    .bind(operation_id.to_string())
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap()
}
