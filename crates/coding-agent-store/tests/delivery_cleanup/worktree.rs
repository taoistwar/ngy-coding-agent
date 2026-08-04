use coding_agent_domain::ClientRequestId;
use coding_agent_store::{
    CleanupAcceptanceOutcome, CleanupOperationAnchor, CleanupOperationState,
    CleanupReconciliationReason, CleanupTransitionOutcome, CompleteWorktreeCleanupRequest,
    DeliveryVersion, EnterWorktreeRemovePendingRequest, ReconcileWorktreeCleanupRequest,
    RecordWorktreeCleanupFailureRequest, RecordWorktreeUnlockedRequest,
    WorktreeCleanupKnownNotAppliedReason, WorktreeDisposition,
};

use super::fixtures::{cleanup_operation, merged_fixture, remove_request};

#[path = "worktree/conflicts.rs"]
mod conflicts;
#[path = "worktree/failure.rs"]
mod failure;
#[path = "worktree/lifecycle.rs"]
mod lifecycle;
#[path = "worktree/reconciliation.rs"]
mod reconciliation;

fn applied(outcome: CleanupTransitionOutcome) -> coding_agent_store::CleanupTransitionReceipt {
    match outcome {
        CleanupTransitionOutcome::Applied(receipt) => receipt,
        other => panic!("expected applied cleanup transition, got {other:?}"),
    }
}

async fn load_disposition(
    store: &coding_agent_store::Store,
    task_id: coding_agent_domain::TaskId,
) -> coding_agent_store::ArtifactDispositionRecord {
    store
        .delivery_ownership_snapshot(task_id)
        .await
        .unwrap()
        .unwrap()
        .disposition
        .unwrap()
}

async fn journal_counts(
    store: &coding_agent_store::Store,
    operation_id: coding_agent_store::DeliveryOperationId,
    task_id: coding_agent_domain::TaskId,
) -> (i64, i64) {
    sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM task_delivery_operation_transitions \
                  WHERE entity_kind = 'cleanup_operation' AND entity_id = ?), \
                (SELECT COUNT(*) FROM task_delivery_operation_transitions \
                  WHERE entity_kind = 'worktree_disposition' AND entity_id = ?)",
    )
    .bind(operation_id.to_string())
    .bind(task_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap()
}

async fn all_delivery_counts(
    store: &coding_agent_store::Store,
    task_id: coding_agent_domain::TaskId,
) -> (i64, i64, i64) {
    sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM task_cleanup_operations WHERE task_id = ?), \
                (SELECT COUNT(*) FROM task_delivery_command_receipts WHERE task_id = ?), \
                (SELECT COUNT(*) FROM task_delivery_operation_transitions)",
    )
    .bind(task_id.to_string())
    .bind(task_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap()
}
