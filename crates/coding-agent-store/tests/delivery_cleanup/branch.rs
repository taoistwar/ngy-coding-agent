use std::str::FromStr;

use coding_agent_domain::{ClientRequestId, TaskId};
use coding_agent_store::{
    BranchCleanupKnownNotAppliedReason, BranchDisposition, CleanupAcceptanceOutcome,
    CleanupOperationAnchor, CleanupOperationState, CleanupReconciliationReason,
    CleanupTransitionOutcome, CompleteBranchCleanupRequest, DeliveryAcceptedOperationState,
    DeliveryVersion, GitCommitOid, ReconcileBranchCleanupRequest,
    RecordBranchCleanupFailureRequest, RefreshBranchCleanupTargetRequest, StoreError,
    WorktreeDisposition,
};

use super::fixtures::{
    cleanup_operation, delete_request, merged_fixture, remove_worktree_fully,
    remove_worktree_fully_with_client,
};

#[path = "branch/acceptance.rs"]
mod acceptance;
#[path = "branch/refresh.rs"]
mod refresh;
#[path = "branch/terminal.rs"]
mod terminal;

fn accepted(outcome: CleanupAcceptanceOutcome) -> coding_agent_store::DeliveryCommandReceipt {
    match outcome {
        CleanupAcceptanceOutcome::Accepted(receipt) => receipt,
        other => panic!("expected accepted cleanup, got {other:?}"),
    }
}

fn applied(outcome: CleanupTransitionOutcome) -> coding_agent_store::CleanupTransitionReceipt {
    match outcome {
        CleanupTransitionOutcome::Applied(receipt) => receipt,
        other => panic!("expected applied transition, got {other:?}"),
    }
}

fn target_head(value: &str) -> GitCommitOid {
    GitCommitOid::from_str(value).unwrap()
}

async fn load_disposition(
    store: &coding_agent_store::Store,
    task_id: TaskId,
) -> coding_agent_store::ArtifactDispositionRecord {
    store
        .delivery_ownership_snapshot(task_id)
        .await
        .unwrap()
        .unwrap()
        .disposition
        .unwrap()
}

async fn delivery_counts(store: &coding_agent_store::Store, task_id: TaskId) -> (i64, i64, i64) {
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
