use coding_agent_store::{
    AcceptedDeliverySourceState, DeliveryRecoveryAction, DeliveryRecoveryBatch,
    DeliveryRecoveryDisposition, DeliveryRecoveryQuery,
};

use crate::authenticated_identity;
use crate::recovery_fixtures::{
    abort_pending, accepted, commit_pending_source, committed_source, mark_remove_pending,
    mark_unlocked_pending_remove, merge_pending, merged_task, object_pending_source,
    pending_preflight, worktree_cleanup,
};
use crate::support::delivery::eligibility::{
    complete_worktree_cleanup, create_branch_cleanup, create_worktree_cleanup,
};

#[tokio::test]
async fn every_recoverable_merge_source_and_cleanup_phase_has_one_typed_entry() {
    let store = crate::support::seeded_store().await;

    let (preflight_task, preflight_id) = pending_preflight(
        &store,
        "codex/recovery-matrix-preflight",
        crate::support::delivery::eligibility::COMMON_IDENTITY,
    )
    .await;
    let (accepted_task, accepted_id, _) = accepted(
        &store,
        "codex/recovery-matrix-accepted",
        crate::support::delivery::eligibility::COMMON_IDENTITY,
    )
    .await;
    let (object_task, object_id) = object_pending_source(
        &store,
        "codex/recovery-matrix-object",
        crate::support::delivery::eligibility::COMMON_IDENTITY,
    )
    .await;
    let (commit_task, commit_id) = commit_pending_source(
        &store,
        "codex/recovery-matrix-commit",
        crate::support::delivery::eligibility::COMMON_IDENTITY,
    )
    .await;
    let (committed_task, committed_id) = committed_source(
        &store,
        "codex/recovery-matrix-committed",
        crate::support::delivery::eligibility::COMMON_IDENTITY,
    )
    .await;
    let (merge_task, merge_id, _) = merge_pending(
        &store,
        "codex/recovery-matrix-merge",
        crate::support::delivery::eligibility::COMMON_IDENTITY,
    )
    .await;
    let (abort_task, abort_id) = abort_pending(
        &store,
        "codex/recovery-matrix-abort",
        crate::support::delivery::eligibility::COMMON_IDENTITY,
    )
    .await;

    let (unlock_task, unlock_id) = worktree_cleanup(&store, "codex/recovery-matrix-unlock").await;
    let (unlocked_task, unlocked_id) =
        worktree_cleanup(&store, "codex/recovery-matrix-unlocked").await;
    mark_unlocked_pending_remove(&store, &unlocked_task, unlocked_id).await;
    let (remove_task, remove_id) = worktree_cleanup(&store, "codex/recovery-matrix-remove").await;
    mark_unlocked_pending_remove(&store, &remove_task, remove_id).await;
    mark_remove_pending(&store, remove_id).await;
    let delete_task = merged_task(&store, "codex/recovery-matrix-delete").await;
    let completed_worktree = create_worktree_cleanup(&store, &delete_task).await;
    complete_worktree_cleanup(&store, &delete_task, completed_worktree).await;
    let delete_id = create_branch_cleanup(
        &store,
        &delete_task,
        crate::support::delivery::eligibility::MERGE_COMMIT,
    )
    .await;

    let batch = store
        .delivery_recovery_batch(&DeliveryRecoveryQuery::first(authenticated_identity()))
        .await
        .unwrap();
    assert_eq!(batch.entries.len(), 11);
    assert!(matches!(
        action_for(&batch, preflight_task.id),
        DeliveryRecoveryAction::PreflightPending { operation_id, .. }
            if operation_id == preflight_id
    ));
    assert!(matches!(
        action_for(&batch, accepted_task.id),
        DeliveryRecoveryAction::Accepted {
            operation_id,
            source: AcceptedDeliverySourceState::Missing,
            ..
        } if operation_id == accepted_id
    ));
    assert!(matches!(
        action_for(&batch, object_task.id),
        DeliveryRecoveryAction::Accepted {
            operation_id,
            source: AcceptedDeliverySourceState::ObjectPending { .. },
            ..
        } if operation_id == object_id
    ));
    assert!(matches!(
        action_for(&batch, commit_task.id),
        DeliveryRecoveryAction::Accepted {
            operation_id,
            source: AcceptedDeliverySourceState::CommitPending { .. },
            ..
        } if operation_id == commit_id
    ));
    assert!(matches!(
        action_for(&batch, committed_task.id),
        DeliveryRecoveryAction::Accepted {
            operation_id,
            source: AcceptedDeliverySourceState::Committed { .. },
            ..
        } if operation_id == committed_id
    ));
    assert!(matches!(
        action_for(&batch, merge_task.id),
        DeliveryRecoveryAction::MergePending { operation_id, .. } if operation_id == merge_id
    ));
    assert!(matches!(
        action_for(&batch, abort_task.id),
        DeliveryRecoveryAction::AbortPending { operation_id, .. } if operation_id == abort_id
    ));
    assert!(matches!(
        action_for(&batch, unlock_task.id),
        DeliveryRecoveryAction::UnlockPending { operation_id, .. } if operation_id == unlock_id
    ));
    assert!(matches!(
        action_for(&batch, unlocked_task.id),
        DeliveryRecoveryAction::UnlockedPendingRemove { operation_id, .. }
            if operation_id == unlocked_id
    ));
    assert!(matches!(
        action_for(&batch, remove_task.id),
        DeliveryRecoveryAction::RemovePending { operation_id, .. } if operation_id == remove_id
    ));
    assert!(matches!(
        action_for(&batch, delete_task.id),
        DeliveryRecoveryAction::DeletePending { operation_id, .. } if operation_id == delete_id
    ));
}

fn action_for(
    batch: &DeliveryRecoveryBatch,
    task_id: coding_agent_domain::TaskId,
) -> DeliveryRecoveryAction {
    let entry = batch
        .entries
        .iter()
        .find(|entry| entry.identity.task_id() == task_id)
        .expect("task has a recovery entry");
    match entry.disposition {
        DeliveryRecoveryDisposition::Recover(action) => action,
        DeliveryRecoveryDisposition::ReconciliationRequired => {
            panic!("expected executable recovery entry")
        }
    }
}
