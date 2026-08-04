use coding_agent_store::{
    BranchDisposition, CleanupKind, CleanupOperationState, DeliverySourceState,
    MergeOperationState, PersistentEligibilityBlocker, WorktreeDisposition,
};

use crate::support::delivery::eligibility::{
    approved_task_with_ready_artifact, complete_branch_cleanup, complete_worktree_cleanup,
    create_branch_cleanup, create_merged_delivery, create_worktree_cleanup, fail_branch_cleanup,
    fail_worktree_cleanup, reconcile_branch_cleanup, reconcile_worktree_cleanup,
};

#[tokio::test]
async fn merged_disposition_and_completed_cleanup_are_projected_without_stale_ownership() {
    let (store, task) = approved_task_with_ready_artifact("codex/task-cleanup").await;
    let eligible = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let merge_id =
        create_merged_delivery(&store, &task, eligible.evidence_identity.as_ref().unwrap()).await;

    let merged = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        merged.ownership.source.as_ref().unwrap().state,
        DeliverySourceState::Committed
    );
    assert_eq!(merged.ownership.merge_operations.len(), 1);
    assert_eq!(
        merged.ownership.merge_operations[0].state,
        MergeOperationState::Merged
    );
    let disposition = merged.ownership.disposition.as_ref().unwrap();
    assert_eq!(disposition.merged_operation_id, merge_id);
    assert_eq!(
        disposition.worktree_state,
        WorktreeDisposition::RetainedLocked
    );
    assert_eq!(disposition.branch_state, BranchDisposition::Retained);
    assert!(
        merged
            .persistent_blockers
            .contains(&PersistentEligibilityBlocker::AlreadyMerged)
    );
    assert!(
        !merged
            .persistent_blockers
            .contains(&PersistentEligibilityBlocker::DeliveryOwned)
    );

    let worktree_cleanup = create_worktree_cleanup(&store, &task).await;
    let pending = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.ownership.cleanup_operations.len(), 1);
    assert_eq!(
        pending.ownership.cleanup_operations[0].state,
        CleanupOperationState::UnlockPending
    );
    assert!(
        pending
            .persistent_blockers
            .contains(&PersistentEligibilityBlocker::DeliveryOwned)
    );

    complete_worktree_cleanup(&store, &task, worktree_cleanup).await;
    let removed = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        removed
            .ownership
            .disposition
            .as_ref()
            .unwrap()
            .worktree_state,
        WorktreeDisposition::Removed
    );
    assert_eq!(
        removed.ownership.cleanup_operations[0].state,
        CleanupOperationState::Completed
    );
    assert!(
        !removed
            .persistent_blockers
            .contains(&PersistentEligibilityBlocker::DeliveryOwned)
    );

    let advanced_target_head = "8".repeat(40);
    let branch_cleanup = create_branch_cleanup(&store, &task, &advanced_target_head).await;
    let delete_pending = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let projected_delete = delete_pending
        .ownership
        .cleanup_operations
        .iter()
        .find(|operation| operation.operation_id == branch_cleanup)
        .unwrap();
    assert_eq!(projected_delete.kind, CleanupKind::DeleteBranch);
    assert_eq!(
        projected_delete
            .expected_target_head
            .as_ref()
            .unwrap()
            .as_str(),
        advanced_target_head
    );
    assert!(
        delete_pending
            .persistent_blockers
            .contains(&PersistentEligibilityBlocker::DeliveryOwned)
    );

    complete_branch_cleanup(&store, &task, branch_cleanup).await;
    let deleted = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        deleted.ownership.disposition.as_ref().unwrap().branch_state,
        BranchDisposition::Deleted
    );
    assert_eq!(deleted.ownership.cleanup_operations.len(), 2);
    assert!(
        deleted
            .ownership
            .cleanup_operations
            .iter()
            .all(|operation| operation.state == CleanupOperationState::Completed)
    );
    assert!(
        !deleted
            .persistent_blockers
            .contains(&PersistentEligibilityBlocker::DeliveryOwned)
    );
}

#[tokio::test]
async fn cleanup_reconciliation_is_projected_and_blocks_delivery() {
    let (store, task) = approved_task_with_ready_artifact("codex/task-cleanup-reconcile").await;
    let eligible = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    create_merged_delivery(&store, &task, eligible.evidence_identity.as_ref().unwrap()).await;
    let cleanup_id = create_worktree_cleanup(&store, &task).await;
    reconcile_worktree_cleanup(&store, &task, cleanup_id).await;

    let snapshot = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        snapshot
            .ownership
            .disposition
            .as_ref()
            .unwrap()
            .worktree_state,
        WorktreeDisposition::ReconciliationRequired
    );
    assert_eq!(snapshot.ownership.cleanup_operations.len(), 1);
    assert_eq!(
        snapshot.ownership.cleanup_operations[0].state,
        CleanupOperationState::ReconciliationRequired
    );
    assert!(snapshot.ownership.requires_reconciliation());
    assert!(
        snapshot
            .persistent_blockers
            .contains(&PersistentEligibilityBlocker::ReconciliationRequired)
    );
}

#[tokio::test]
async fn old_failed_cleanup_is_validated_before_later_reconciliation() {
    let (store, task) = approved_task_with_ready_artifact("codex/task-cleanup-history").await;
    let eligible = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    create_merged_delivery(&store, &task, eligible.evidence_identity.as_ref().unwrap()).await;
    let old_failed = create_worktree_cleanup(&store, &task).await;
    fail_worktree_cleanup(&store, old_failed).await;
    let reconciliation = create_worktree_cleanup(&store, &task).await;
    reconcile_worktree_cleanup(&store, &task, reconciliation).await;

    let snapshot = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        snapshot
            .ownership
            .cleanup_operations
            .iter()
            .map(|operation| (operation.operation_id, operation.state))
            .collect::<Vec<_>>(),
        vec![
            (old_failed, CleanupOperationState::Failed),
            (
                reconciliation,
                CleanupOperationState::ReconciliationRequired,
            ),
        ]
    );
    assert!(
        snapshot.ownership.cleanup_operations[0].initial_transition_id
            < snapshot.ownership.cleanup_operations[1].initial_transition_id
    );
    assert!(
        snapshot
            .persistent_blockers
            .contains(&PersistentEligibilityBlocker::ReconciliationRequired)
    );
}

#[tokio::test]
async fn old_failed_delete_is_validated_before_later_branch_reconciliation() {
    let (store, task) = approved_task_with_ready_artifact("codex/task-delete-history").await;
    let eligible = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    create_merged_delivery(&store, &task, eligible.evidence_identity.as_ref().unwrap()).await;
    let remove = create_worktree_cleanup(&store, &task).await;
    complete_worktree_cleanup(&store, &task, remove).await;

    let old_failed = create_branch_cleanup(&store, &task, &"8".repeat(40)).await;
    fail_branch_cleanup(&store, old_failed).await;
    let reconciliation = create_branch_cleanup(&store, &task, &"9".repeat(40)).await;
    reconcile_branch_cleanup(&store, &task, reconciliation).await;

    let snapshot = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let delete_operations = snapshot
        .ownership
        .cleanup_operations
        .iter()
        .filter(|operation| operation.kind == CleanupKind::DeleteBranch)
        .map(|operation| (operation.operation_id, operation.state))
        .collect::<Vec<_>>();
    assert_eq!(
        delete_operations,
        vec![
            (old_failed, CleanupOperationState::Failed),
            (
                reconciliation,
                CleanupOperationState::ReconciliationRequired,
            ),
        ]
    );
    assert_eq!(
        snapshot
            .ownership
            .disposition
            .as_ref()
            .unwrap()
            .branch_state,
        BranchDisposition::ReconciliationRequired
    );
    assert!(
        snapshot
            .persistent_blockers
            .contains(&PersistentEligibilityBlocker::ReconciliationRequired)
    );
}
