#![cfg(feature = "test-support")]

mod delivery_cleanup_support;
#[allow(dead_code, unused_imports)]
mod delivery_merge_support;
mod support;

use std::str::FromStr;
use std::sync::Arc;

use coding_agent_app::{
    DeliveryCleanupAcceptanceOutcome, DeliveryCleanupOperationKind, DeliveryCleanupOperationState,
    DeliveryCleanupReceiptDisposition, DeliveryCommandConflict, DeliveryDeleteBranchRequest,
    DeliveryEligibilityReason, DeliveryOperationRecoveryOutcome, DeliveryPreflightBusyReason,
    DeliveryPreflightUnavailableReason, DeliveryProcessProof, DeliveryRemoveWorktreeRequest,
    RepositoryControlPoisonReason, RepositoryControlState, StoreWriterFaultPoint,
    StoreWriterFaultSpec, StoreWriterOperationKind, StoreWriterTestController,
};
use coding_agent_domain::ClientRequestId;
use coding_agent_runtime::DeliveryRemovePendingDisposition;
use coding_agent_store::{
    BranchDisposition, CleanupOperationState, CleanupReconciliationReason,
    DeleteBranchCommandRequest, DeliveryCommand, DeliveryCommandLookup, GitBranchRef, GitCommitOid,
    RemoveWorktreeCommandRequest, WorktreeDisposition,
};

use delivery_cleanup_support::{
    BranchStep, CleanupCall, CleanupFault, CleanupStage, DeliveryCleanupFixture,
};
use delivery_merge_support::EXPECTED_MERGE_COMMIT;
use tokio::time::Duration;

const REFRESHED_TARGET_HEAD: &str = "89abcdef0123456789abcdef0123456789abcdef";

#[tokio::test]
async fn dirty_first_worktree_cleanup_is_ineligible_without_receipt_or_poison() {
    let fixture = DeliveryCleanupFixture::new(None).await;
    let request = fixture.remove_request().await;
    fixture.runtime.fail_once(
        CleanupStage::BindWorktree,
        CleanupFault::TargetWorktreeDirty,
    );

    let outcome = fixture
        .manager()
        .remove_worktree(DeliveryRemoveWorktreeRequest::new(request))
        .await
        .expect("cleanup manager remains open");

    assert_eq!(
        outcome,
        DeliveryCleanupAcceptanceOutcome::Ineligible(vec![
            DeliveryEligibilityReason::TargetWorktreeDirty,
        ])
    );
    let counts = sqlx::query_as::<_, (i64, i64)>(
        "SELECT (SELECT COUNT(*) FROM task_cleanup_operations WHERE task_id = ?), \
                (SELECT COUNT(*) FROM task_delivery_command_receipts \
                 WHERE task_id = ? AND command_kind = 'remove_worktree')",
    )
    .bind(fixture.task.id.to_string())
    .bind(fixture.task.id.to_string())
    .fetch_one(fixture.merge.base.store.pool())
    .await
    .expect("count rejected cleanup durable state");
    assert_eq!(
        counts,
        (0, 0),
        "dirty admission must write no operation or receipt"
    );
    fixture
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    assert_eq!(
        fixture
            .coordinator
            .poison_reason(fixture.task.repository_id)
            .expect("cleanup repository remains registered"),
        None,
        "a typed dirty rejection must not poison repository coordination"
    );
    fixture.finish().await;
}

#[tokio::test]
async fn cleanup_admission_returns_exact_typed_anchor_conflicts() {
    let version_fixture = DeliveryCleanupFixture::new(None).await;
    let version = version_fixture.remove_request().await;
    let stale_version = RemoveWorktreeCommandRequest::try_new(
        ClientRequestId::new(),
        version.task_id(),
        version
            .expected_disposition_version()
            .next()
            .expect("fixture disposition version has a successor"),
        version.expected_merge_operation_id(),
        version.expected_source_ref().clone(),
        version.expected_source_oid().clone(),
    )
    .expect("valid stale cleanup request");
    let outcome = version_fixture
        .manager()
        .remove_worktree(DeliveryRemoveWorktreeRequest::new(stale_version))
        .await
        .expect("cleanup manager remains open");
    assert_eq!(
        outcome,
        DeliveryCleanupAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::ArtifactCleanupNotAllowed
        )
    );
    version_fixture.finish().await;

    let identity_fixture = DeliveryCleanupFixture::new(None).await;
    let identity = identity_fixture.remove_request().await;
    let wrong_identity = RemoveWorktreeCommandRequest::try_new(
        ClientRequestId::new(),
        identity.task_id(),
        identity.expected_disposition_version(),
        identity.expected_merge_operation_id(),
        GitBranchRef::from_str("refs/heads/codex/not-this-worktree")
            .expect("valid alternate source branch"),
        identity.expected_source_oid().clone(),
    )
    .expect("valid identity-mismatched cleanup request");
    let outcome = identity_fixture
        .manager()
        .remove_worktree(DeliveryRemoveWorktreeRequest::new(wrong_identity))
        .await
        .expect("cleanup manager remains open");
    assert_eq!(
        outcome,
        DeliveryCleanupAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::WorktreeIdentityMismatch
        )
    );
    identity_fixture.finish().await;

    let target_fixture = DeliveryCleanupFixture::new(None).await;
    let worktree = target_fixture.remove().await;
    target_fixture
        .wait_operation_state(worktree.operation_id(), CleanupOperationState::Completed)
        .await;
    let target = target_fixture.delete_request(EXPECTED_MERGE_COMMIT).await;
    let changed_target = DeleteBranchCommandRequest::try_new(
        ClientRequestId::new(),
        target.task_id(),
        target.expected_disposition_version(),
        target.expected_merge_operation_id(),
        target.expected_source_ref().clone(),
        target.expected_source_oid().clone(),
        target.target_branch().clone(),
        GitCommitOid::from_str(REFRESHED_TARGET_HEAD).expect("valid changed target head"),
    )
    .expect("valid target-changed cleanup request");
    let outcome = target_fixture
        .manager()
        .delete_branch(DeliveryDeleteBranchRequest::new(changed_target))
        .await
        .expect("cleanup manager remains open");
    assert_eq!(
        outcome,
        DeliveryCleanupAcceptanceOutcome::Conflict(DeliveryCommandConflict::TargetHeadChanged)
    );
    target_fixture.finish().await;
}

#[tokio::test]
async fn worktree_success_does_not_chain_branch_and_explicit_branch_is_independent() {
    let fixture = DeliveryCleanupFixture::new(None).await;

    let worktree = fixture.remove().await;
    assert_eq!(
        worktree.receipt(),
        DeliveryCleanupReceiptDisposition::Created
    );
    assert_eq!(
        worktree.cleanup_kind(),
        DeliveryCleanupOperationKind::RemoveWorktree
    );
    assert_eq!(
        worktree.accepted_state(),
        DeliveryCleanupOperationState::UnlockPending
    );
    let completed_worktree = fixture
        .wait_operation_state(worktree.operation_id(), CleanupOperationState::Completed)
        .await;
    assert_eq!(
        completed_worktree.expected_merge_operation_id,
        fixture.merge_operation_id
    );
    fixture
        .wait_repository_state(RepositoryControlState::Available)
        .await;

    let disposition = fixture
        .merge
        .base
        .store
        .delivery_ownership_snapshot(fixture.task.id)
        .await
        .expect("load disposition after worktree cleanup")
        .expect("cleanup task exists")
        .disposition
        .expect("cleanup disposition exists");
    assert_eq!(disposition.worktree_state, WorktreeDisposition::Removed);
    assert_eq!(disposition.branch_state, BranchDisposition::Retained);
    assert!(disposition.branch_failure_code.is_none());
    assert!(
        fixture.runtime.calls().iter().all(|call| !matches!(
            call,
            CleanupCall::BindBranchAcceptance | CleanupCall::Delete(_)
        )),
        "worktree completion must never auto-delete the branch"
    );

    let branch = fixture.delete().await;
    assert_eq!(branch.receipt(), DeliveryCleanupReceiptDisposition::Created);
    assert_eq!(
        branch.cleanup_kind(),
        DeliveryCleanupOperationKind::DeleteBranch
    );
    assert_eq!(
        branch.accepted_state(),
        DeliveryCleanupOperationState::DeletePending
    );
    fixture
        .wait_operation_state(branch.operation_id(), CleanupOperationState::Completed)
        .await;
    fixture
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    let disposition = fixture
        .merge
        .base
        .store
        .delivery_ownership_snapshot(fixture.task.id)
        .await
        .expect("load terminal cleanup disposition")
        .expect("cleanup task exists")
        .disposition
        .expect("cleanup disposition exists");
    assert_eq!(disposition.worktree_state, WorktreeDisposition::Removed);
    assert_eq!(disposition.branch_state, BranchDisposition::Deleted);
    fixture.finish().await;
}

#[tokio::test]
async fn restart_recovers_all_three_worktree_pending_phases() {
    assert_worktree_restart(CleanupStage::Unlock, CleanupOperationState::UnlockPending).await;
    assert_worktree_restart(
        CleanupStage::EnterRemove,
        CleanupOperationState::UnlockedPendingRemove,
    )
    .await;
    assert_worktree_restart(CleanupStage::Remove, CleanupOperationState::RemovePending).await;
}

#[tokio::test]
async fn persisted_dirty_restart_uses_each_worktree_phase_matrix() {
    let mut remove_pending = DeliveryCleanupFixture::new(None).await;
    remove_pending
        .runtime
        .fail_once(CleanupStage::Remove, CleanupFault::Unavailable);
    let accepted = remove_pending.remove().await;
    remove_pending
        .wait_operation_state(
            accepted.operation_id(),
            CleanupOperationState::RemovePending,
        )
        .await;
    remove_pending
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    remove_pending
        .runtime
        .push_remove_step(DeliveryRemovePendingDisposition::KnownNotAppliedDirty);
    remove_pending.restart_manager().await;
    assert_eq!(
        remove_pending
            .manager()
            .recover_operation_for_test(accepted.operation_id())
            .await
            .expect("restarted RemovePending manager remains open"),
        DeliveryOperationRecoveryOutcome::Converged,
    );
    let failed = remove_pending
        .wait_operation_state(accepted.operation_id(), CleanupOperationState::Failed)
        .await;
    assert_eq!(
        failed.failure_code.as_ref().map(|failure| failure.as_str()),
        Some("TARGET_WORKTREE_DIRTY"),
    );
    let disposition = remove_pending
        .merge
        .base
        .store
        .delivery_ownership_snapshot(remove_pending.task.id)
        .await
        .expect("load recovered dirty disposition")
        .expect("dirty recovery task exists")
        .disposition
        .expect("dirty recovery disposition exists");
    assert_eq!(
        disposition.worktree_state,
        WorktreeDisposition::RetainedUnlocked
    );
    remove_pending
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    assert_eq!(
        remove_pending
            .coordinator
            .poison_reason(remove_pending.task.repository_id)
            .expect("dirty RemovePending repository remains registered"),
        None,
    );
    remove_pending.finish().await;

    let mut unlocked_pending = DeliveryCleanupFixture::new(None).await;
    unlocked_pending
        .runtime
        .fail_once(CleanupStage::EnterRemove, CleanupFault::Unavailable);
    let accepted = unlocked_pending.remove().await;
    unlocked_pending
        .wait_operation_state(
            accepted.operation_id(),
            CleanupOperationState::UnlockedPendingRemove,
        )
        .await;
    unlocked_pending
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    unlocked_pending.runtime.push_enter_remove_step(
        coding_agent_runtime::DeliveryUnlockedPendingRemoveDisposition::ReconciliationRequired,
    );
    unlocked_pending.restart_manager().await;
    assert_eq!(
        unlocked_pending
            .manager()
            .recover_operation_for_test(accepted.operation_id())
            .await
            .expect("restarted UnlockedPendingRemove manager remains open"),
        DeliveryOperationRecoveryOutcome::ReconciliationRequired,
    );
    unlocked_pending
        .wait_operation_state(
            accepted.operation_id(),
            CleanupOperationState::ReconciliationRequired,
        )
        .await;
    assert_poisoned(&unlocked_pending);
    assert_eq!(
        count_calls(&unlocked_pending, |call| matches!(
            call,
            CleanupCall::Remove(_)
        )),
        0,
    );
    unlocked_pending.finish().await;

    let mut unlock_pending = DeliveryCleanupFixture::new(None).await;
    unlock_pending
        .runtime
        .fail_once(CleanupStage::Unlock, CleanupFault::Unavailable);
    let accepted = unlock_pending.remove().await;
    unlock_pending
        .wait_operation_state(
            accepted.operation_id(),
            CleanupOperationState::UnlockPending,
        )
        .await;
    unlock_pending
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    unlock_pending.runtime.push_enter_remove_step(
        coding_agent_runtime::DeliveryUnlockedPendingRemoveDisposition::ReconciliationRequired,
    );
    unlock_pending.restart_manager().await;
    assert_eq!(
        unlock_pending
            .manager()
            .recover_operation_for_test(accepted.operation_id())
            .await
            .expect("restarted UnlockPending manager remains open"),
        DeliveryOperationRecoveryOutcome::ReconciliationRequired,
    );
    unlock_pending
        .wait_operation_state(
            accepted.operation_id(),
            CleanupOperationState::ReconciliationRequired,
        )
        .await;
    assert_poisoned(&unlock_pending);
    assert_eq!(
        count_calls(&unlock_pending, |call| matches!(
            call,
            CleanupCall::Unlock(_)
        )),
        1,
        "locked dirty recovery may execute only the exact unlock stage",
    );
    assert_eq!(
        count_calls(&unlock_pending, |call| matches!(
            call,
            CleanupCall::Remove(_)
        )),
        0,
    );
    unlock_pending.finish().await;
}

#[tokio::test]
async fn worktree_complete_reply_loss_replays_store_command_without_repeating_remove() {
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            operation: Some(StoreWriterOperationKind::CompleteWorktreeCleanup),
            count: 1,
        }])
        .expect("valid worktree completion reply-loss script"),
    );
    let fixture = DeliveryCleanupFixture::new(Some(controller.clone())).await;

    let accepted = fixture.remove().await;
    fixture
        .wait_operation_state(accepted.operation_id(), CleanupOperationState::Completed)
        .await;
    fixture
        .wait_repository_state(RepositoryControlState::Available)
        .await;

    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            StoreWriterOperationKind::CompleteWorktreeCleanup,
        ),
        1
    );
    assert_eq!(
        count_calls(&fixture, |call| matches!(call, CleanupCall::Remove(_))),
        1
    );
    assert_eq!(
        cleanup_transition_count(&fixture, accepted.operation_id(), "completed").await,
        1
    );
    fixture.finish().await;
}

#[tokio::test]
async fn dirty_worktree_is_durable_and_new_receipt_skips_unlock() {
    let fixture = DeliveryCleanupFixture::new(None).await;
    fixture
        .runtime
        .push_remove_step(DeliveryRemovePendingDisposition::KnownNotAppliedDirty);

    let first = fixture.remove().await;
    let failed = fixture
        .wait_operation_state(first.operation_id(), CleanupOperationState::Failed)
        .await;
    assert!(failed.failure_code.is_some());
    let disposition = fixture
        .merge
        .base
        .store
        .delivery_ownership_snapshot(fixture.task.id)
        .await
        .expect("load dirty disposition")
        .expect("cleanup task exists")
        .disposition
        .expect("cleanup disposition exists");
    assert_eq!(
        disposition.worktree_state,
        WorktreeDisposition::RetainedUnlocked
    );

    let second = fixture.remove().await;
    assert_ne!(second.operation_id(), first.operation_id());
    assert_eq!(
        second.accepted_state(),
        DeliveryCleanupOperationState::RemovePending
    );
    fixture
        .wait_operation_state(second.operation_id(), CleanupOperationState::Completed)
        .await;
    assert_eq!(
        count_calls(&fixture, |call| matches!(call, CleanupCall::Unlock(_))),
        1
    );
    assert_eq!(
        count_calls(&fixture, |call| matches!(call, CleanupCall::Remove(_))),
        2
    );
    fixture.finish().await;
}

#[tokio::test]
async fn worktree_identity_drift_is_durable_reconciliation_and_sticky_poison() {
    let fixture = DeliveryCleanupFixture::new(None).await;
    fixture.runtime.fail_once(
        CleanupStage::Unlock,
        CleanupFault::Reconcile(CleanupReconciliationReason::WorktreeIdentityMismatch),
    );

    let accepted = fixture.remove().await;
    fixture
        .wait_operation_state(
            accepted.operation_id(),
            CleanupOperationState::ReconciliationRequired,
        )
        .await;
    assert_poisoned(&fixture);
    fixture.finish().await;
}

#[tokio::test]
async fn branch_refresh_persists_next_version_and_revokes_old_capability() {
    let fixture = DeliveryCleanupFixture::new(None).await;
    let worktree = fixture.remove().await;
    fixture
        .wait_operation_state(worktree.operation_id(), CleanupOperationState::Completed)
        .await;
    let fresh = GitCommitOid::from_str(REFRESHED_TARGET_HEAD).expect("valid refreshed target");
    fixture
        .runtime
        .push_branch_step(BranchStep::Refresh(fresh.clone()));
    fixture.runtime.push_branch_step(BranchStep::Deleted);

    let branch = fixture.delete().await;
    let completed = fixture
        .wait_operation_state(branch.operation_id(), CleanupOperationState::Completed)
        .await;
    assert_eq!(completed.expected_target_head.as_ref(), Some(&fresh));
    assert_eq!(completed.target_head_observations.len(), 3);
    assert_eq!(completed.target_head_observations[1].target_head, fresh);
    assert_eq!(completed.target_head_observations[2].target_head, fresh);
    let delete_capabilities: Vec<u64> = fixture
        .runtime
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            CleanupCall::Delete(identity) => Some(identity),
            _ => None,
        })
        .collect();
    assert_eq!(delete_capabilities.len(), 2);
    assert_eq!(delete_capabilities[1], delete_capabilities[0] + 1);
    fixture.finish().await;
}

#[tokio::test]
async fn branch_complete_reply_loss_does_not_repeat_atomic_delete() {
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            operation: Some(StoreWriterOperationKind::CompleteBranchCleanup),
            count: 1,
        }])
        .expect("valid branch completion reply-loss script"),
    );
    let fixture = DeliveryCleanupFixture::new(Some(controller.clone())).await;
    let worktree = fixture.remove().await;
    fixture
        .wait_operation_state(worktree.operation_id(), CleanupOperationState::Completed)
        .await;

    let branch = fixture.delete().await;
    fixture
        .wait_operation_state(branch.operation_id(), CleanupOperationState::Completed)
        .await;
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            StoreWriterOperationKind::CompleteBranchCleanup,
        ),
        1
    );
    assert_eq!(
        count_calls(&fixture, |call| matches!(call, CleanupCall::Delete(_))),
        1
    );
    assert_eq!(
        cleanup_transition_count(&fixture, branch.operation_id(), "completed").await,
        1
    );
    fixture.finish().await;
}

#[tokio::test]
async fn known_not_applied_worktree_unlock_record_retains_repository_ownership() {
    let controller = busy_controller(StoreWriterOperationKind::RecordWorktreeUnlocked, 18);
    let fixture = DeliveryCleanupFixture::new(Some(controller.clone())).await;

    let accepted = fixture.remove().await;
    controller
        .wait_until_reached(StoreWriterFaultPoint::BusyBeforeExecute, 18)
        .await;

    assert_eq!(
        fixture.operation(accepted.operation_id()).await.state,
        CleanupOperationState::UnlockPending
    );
    assert_retained_cleanup_worker(&fixture, "worktree-unlock record").await;
    fixture.finish().await;
}

#[tokio::test]
async fn outer_runtime_timeout_during_worktree_unlock_retains_repository_ownership() {
    let fixture = DeliveryCleanupFixture::new(None).await;
    let gate = fixture.runtime.install_gate(CleanupStage::Unlock);

    let accepted = fixture.remove().await;
    gate.wait_until_reached().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(11 * 60 + 1)).await;
    tokio::time::resume();
    gate.wait_until_exited().await;

    assert_eq!(
        fixture.operation(accepted.operation_id()).await.state,
        CleanupOperationState::UnlockPending
    );
    assert_retained_cleanup_worker(&fixture, "outer worktree-unlock runtime timeout").await;
    fixture.finish().await;
}

#[tokio::test]
async fn worktree_unlock_kna_then_runtime_unavailable_keeps_retention_obligation() {
    let controller = busy_controller(StoreWriterOperationKind::RecordWorktreeUnlocked, 6);
    let fixture = DeliveryCleanupFixture::new(Some(controller.clone())).await;

    let accepted = fixture.remove().await;
    controller
        .wait_until_reached(StoreWriterFaultPoint::BusyBeforeExecute, 6)
        .await;
    fixture
        .runtime
        .fail_once(CleanupStage::Unlock, CleanupFault::Unavailable);
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_retained_cleanup_worker(
        &fixture,
        "worktree-unlock KNA followed by runtime unavailable",
    )
    .await;
    assert_eq!(
        fixture.operation(accepted.operation_id()).await.state,
        CleanupOperationState::UnlockPending
    );
    fixture.finish().await;
}

#[tokio::test]
async fn known_not_applied_worktree_completion_retains_repository_ownership() {
    let controller = busy_controller(StoreWriterOperationKind::CompleteWorktreeCleanup, 18);
    let fixture = DeliveryCleanupFixture::new(Some(controller.clone())).await;

    let accepted = fixture.remove().await;
    controller
        .wait_until_reached(StoreWriterFaultPoint::BusyBeforeExecute, 18)
        .await;

    assert_eq!(
        fixture.operation(accepted.operation_id()).await.state,
        CleanupOperationState::RemovePending
    );
    assert_retained_cleanup_worker(&fixture, "worktree completion").await;
    fixture.finish().await;
}

#[tokio::test]
async fn known_not_applied_branch_completion_retains_repository_ownership() {
    let controller = busy_controller(StoreWriterOperationKind::CompleteBranchCleanup, 18);
    let fixture = DeliveryCleanupFixture::new(Some(controller.clone())).await;
    let worktree = fixture.remove().await;
    fixture
        .wait_operation_state(worktree.operation_id(), CleanupOperationState::Completed)
        .await;

    let branch = fixture.delete().await;
    controller
        .wait_until_reached(StoreWriterFaultPoint::BusyBeforeExecute, 18)
        .await;

    assert_eq!(
        fixture.operation(branch.operation_id()).await.state,
        CleanupOperationState::DeletePending
    );
    assert_retained_cleanup_worker(&fixture, "branch completion").await;
    fixture.finish().await;
}

#[tokio::test]
async fn known_not_applied_worktree_reconciliation_poisons_and_releases_worker() {
    let controller = busy_controller(StoreWriterOperationKind::ReconcileWorktreeCleanup, 6);
    let fixture = DeliveryCleanupFixture::new(Some(controller.clone())).await;
    fixture.runtime.fail_once(
        CleanupStage::Unlock,
        CleanupFault::Reconcile(CleanupReconciliationReason::WorktreeIdentityMismatch),
    );

    let accepted = fixture.remove().await;
    controller
        .wait_until_reached(StoreWriterFaultPoint::BusyBeforeExecute, 6)
        .await;

    assert_eq!(
        fixture.operation(accepted.operation_id()).await.state,
        CleanupOperationState::UnlockPending
    );
    assert_poisoned_released_cleanup_worker(&fixture, "worktree reconciliation").await;
    fixture.finish().await;
}

#[tokio::test]
async fn known_not_applied_branch_reconciliation_poisons_and_releases_worker() {
    let controller = busy_controller(StoreWriterOperationKind::ReconcileBranchCleanup, 6);
    let fixture = DeliveryCleanupFixture::new(Some(controller.clone())).await;
    let worktree = fixture.remove().await;
    fixture
        .wait_operation_state(worktree.operation_id(), CleanupOperationState::Completed)
        .await;
    fixture.runtime.fail_once(
        CleanupStage::Delete,
        CleanupFault::Reconcile(CleanupReconciliationReason::SourceInconsistent),
    );

    let branch = fixture.delete().await;
    controller
        .wait_until_reached(StoreWriterFaultPoint::BusyBeforeExecute, 6)
        .await;

    assert_eq!(
        fixture.operation(branch.operation_id()).await.state,
        CleanupOperationState::DeletePending
    );
    assert_poisoned_released_cleanup_worker(&fixture, "branch reconciliation").await;
    fixture.finish().await;
}

#[tokio::test]
async fn exact_runtime_retries_are_bounded_per_cleanup_phase() {
    let fixture = DeliveryCleanupFixture::new(None).await;
    fixture
        .runtime
        .push_remove_step(DeliveryRemovePendingDisposition::RetryExactRemove);
    fixture
        .runtime
        .push_remove_step(DeliveryRemovePendingDisposition::Removed);
    let worktree = fixture.remove().await;
    fixture
        .wait_operation_state(worktree.operation_id(), CleanupOperationState::Completed)
        .await;
    assert_eq!(
        count_calls(&fixture, |call| matches!(call, CleanupCall::Remove(_))),
        2
    );

    fixture.runtime.push_branch_step(BranchStep::RetryExact);
    fixture.runtime.push_branch_step(BranchStep::Deleted);
    let branch = fixture.delete().await;
    fixture
        .wait_operation_state(branch.operation_id(), CleanupOperationState::Completed)
        .await;
    assert_eq!(
        count_calls(&fixture, |call| matches!(call, CleanupCall::Delete(_))),
        2
    );
    fixture.finish().await;
}

#[tokio::test]
async fn terminal_branch_receipt_replays_without_repeating_delete() {
    let fixture = DeliveryCleanupFixture::new(None).await;
    let worktree = fixture.remove().await;
    fixture
        .wait_operation_state(worktree.operation_id(), CleanupOperationState::Completed)
        .await;
    let request = fixture.delete_request(EXPECTED_MERGE_COMMIT).await;
    let created = match fixture
        .manager()
        .delete_branch(DeliveryDeleteBranchRequest::new(request.clone()))
        .await
        .expect("cleanup manager remains open")
    {
        DeliveryCleanupAcceptanceOutcome::Durable(acceptance) => acceptance,
        other => panic!("expected durable branch acceptance, got {other:?}"),
    };
    fixture
        .wait_operation_state(created.operation_id(), CleanupOperationState::Completed)
        .await;
    fixture
        .manager()
        .quiesce()
        .await
        .expect("quiesce cleanup manager");

    let existing = match fixture
        .manager()
        .delete_branch(DeliveryDeleteBranchRequest::new(request))
        .await
        .expect("quiesced manager remains open for receipt lookup")
    {
        DeliveryCleanupAcceptanceOutcome::Durable(acceptance) => acceptance,
        other => panic!("expected durable branch replay, got {other:?}"),
    };
    assert_eq!(existing.operation_id(), created.operation_id());
    assert_eq!(
        existing.receipt(),
        DeliveryCleanupReceiptDisposition::Existing
    );
    assert_eq!(
        count_calls(&fixture, |call| matches!(call, CleanupCall::Delete(_))),
        1
    );
    fixture.finish().await;
}

#[tokio::test]
async fn branch_known_failure_and_source_drift_take_distinct_durable_paths() {
    let known = DeliveryCleanupFixture::new(None).await;
    let worktree = known.remove().await;
    known
        .wait_operation_state(worktree.operation_id(), CleanupOperationState::Completed)
        .await;
    known.runtime.push_branch_step(BranchStep::SourceNotMerged);
    let branch = known.delete().await;
    let failed = known
        .wait_operation_state(branch.operation_id(), CleanupOperationState::Failed)
        .await;
    assert_eq!(
        failed.failure_code.as_ref().map(|failure| failure.as_str()),
        Some("SOURCE_BRANCH_NOT_MERGED")
    );
    let disposition = known
        .merge
        .base
        .store
        .delivery_ownership_snapshot(known.task.id)
        .await
        .expect("load known-failure disposition")
        .expect("cleanup task exists")
        .disposition
        .expect("cleanup disposition exists");
    assert_eq!(disposition.branch_state, BranchDisposition::Retained);
    assert!(disposition.branch_failure_code.is_none());
    known
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    known.finish().await;

    let drift = DeliveryCleanupFixture::new(None).await;
    let worktree = drift.remove().await;
    drift
        .wait_operation_state(worktree.operation_id(), CleanupOperationState::Completed)
        .await;
    drift.runtime.fail_once(
        CleanupStage::Delete,
        CleanupFault::Reconcile(CleanupReconciliationReason::SourceInconsistent),
    );
    let branch = drift.delete().await;
    drift
        .wait_operation_state(
            branch.operation_id(),
            CleanupOperationState::ReconciliationRequired,
        )
        .await;
    assert_poisoned(&drift);
    drift.finish().await;
}

#[tokio::test]
async fn branch_timeout_and_runtime_reconciliation_map_to_distinct_durable_paths() {
    let timed_out = DeliveryCleanupFixture::new(None).await;
    let worktree = timed_out.remove().await;
    timed_out
        .wait_operation_state(worktree.operation_id(), CleanupOperationState::Completed)
        .await;
    timed_out
        .runtime
        .push_branch_step(BranchStep::CommandTimedOut);
    let branch = timed_out.delete().await;
    let failed = timed_out
        .wait_operation_state(branch.operation_id(), CleanupOperationState::Failed)
        .await;
    assert_eq!(
        failed.failure_code.as_ref().map(|failure| failure.as_str()),
        Some("COMMAND_TIMED_OUT")
    );
    let disposition = timed_out
        .merge
        .base
        .store
        .delivery_ownership_snapshot(timed_out.task.id)
        .await
        .expect("load timeout disposition")
        .expect("cleanup task exists")
        .disposition
        .expect("cleanup disposition exists");
    assert_eq!(disposition.branch_state, BranchDisposition::Retained);
    assert!(disposition.branch_failure_code.is_none());
    timed_out
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    timed_out.finish().await;

    let reconciliation = DeliveryCleanupFixture::new(None).await;
    let worktree = reconciliation.remove().await;
    reconciliation
        .wait_operation_state(worktree.operation_id(), CleanupOperationState::Completed)
        .await;
    reconciliation
        .runtime
        .push_branch_step(BranchStep::ReconciliationRequired);
    let branch = reconciliation.delete().await;
    reconciliation
        .wait_operation_state(
            branch.operation_id(),
            CleanupOperationState::ReconciliationRequired,
        )
        .await;
    assert_poisoned(&reconciliation);
    reconciliation.finish().await;
}

#[tokio::test]
async fn ingress_is_receipt_first_and_busy_active_and_unknown_ownership_are_fail_closed() {
    let receipt_first = DeliveryCleanupFixture::new(None).await;
    let request = receipt_first.remove_request().await;
    let first = match receipt_first
        .manager()
        .remove_worktree(DeliveryRemoveWorktreeRequest::new(request.clone()))
        .await
        .expect("cleanup manager remains open")
    {
        DeliveryCleanupAcceptanceOutcome::Durable(acceptance) => acceptance,
        other => panic!("expected durable cleanup, got {other:?}"),
    };
    receipt_first
        .wait_operation_state(first.operation_id(), CleanupOperationState::Completed)
        .await;
    receipt_first
        .manager()
        .quiesce()
        .await
        .expect("quiesce cleanup manager");
    let replay = match receipt_first
        .manager()
        .remove_worktree(DeliveryRemoveWorktreeRequest::new(request))
        .await
        .expect("quiesced manager remains open for receipt lookup")
    {
        DeliveryCleanupAcceptanceOutcome::Durable(acceptance) => acceptance,
        other => panic!("expected durable receipt replay, got {other:?}"),
    };
    assert_eq!(
        replay.receipt(),
        DeliveryCleanupReceiptDisposition::Existing
    );
    receipt_first.finish().await;

    let busy = DeliveryCleanupFixture::new(None).await;
    let key = busy
        .coordinator
        .coordination_key(busy.task.repository_id)
        .expect("cleanup repository is registered");
    let lease = busy
        .coordinator
        .try_acquire(key)
        .expect("hold cleanup repository lease");
    let outcome = busy
        .manager()
        .remove_worktree(DeliveryRemoveWorktreeRequest::new(
            busy.remove_request().await,
        ))
        .await
        .expect("cleanup manager remains open");
    assert_eq!(
        outcome,
        DeliveryCleanupAcceptanceOutcome::Busy(DeliveryPreflightBusyReason::RepositoryBusy)
    );
    lease.clean_release().expect("release busy test lease");
    busy.finish().await;

    let active = DeliveryCleanupFixture::new(None).await;
    active.process_proofs.push(DeliveryProcessProof::Active);
    let outcome = active
        .manager()
        .remove_worktree(DeliveryRemoveWorktreeRequest::new(
            active.remove_request().await,
        ))
        .await
        .expect("cleanup manager remains open");
    assert_eq!(
        outcome,
        DeliveryCleanupAcceptanceOutcome::Conflict(
            DeliveryCommandConflict::ArtifactProcessStillActive
        )
    );
    active.finish().await;

    let unknown = DeliveryCleanupFixture::new(None).await;
    let command = unknown.remove_request().await;
    unknown
        .process_proofs
        .push(DeliveryProcessProof::CleanupUnproven);
    let outcome = unknown
        .manager()
        .remove_worktree(DeliveryRemoveWorktreeRequest::new(command.clone()))
        .await
        .expect("cleanup manager remains open");
    assert_eq!(
        outcome,
        DeliveryCleanupAcceptanceOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::ProcessProofUnavailable
        )
    );
    unknown
        .wait_repository_state(RepositoryControlState::Busy)
        .await;
    assert_eq!(
        unknown
            .manager()
            .quiesce()
            .await
            .expect("quiesce unknown-ownership manager")
            .in_flight_workers(),
        1
    );
    assert!(matches!(
        unknown
            .merge
            .base
            .store
            .lookup_delivery_command(&DeliveryCommand::RemoveWorktree(command))
            .await
            .expect("query missing unknown-ownership receipt"),
        DeliveryCommandLookup::Missing
    ));
    unknown.finish().await;
}

#[tokio::test]
async fn runtime_cleanup_unproven_retains_durable_pending_worker() {
    let fixture = DeliveryCleanupFixture::new(None).await;
    fixture
        .runtime
        .fail_once(CleanupStage::Unlock, CleanupFault::ProcessCleanupUnproven);

    let accepted = fixture.remove().await;
    fixture
        .wait_operation_state(
            accepted.operation_id(),
            CleanupOperationState::UnlockPending,
        )
        .await;
    fixture
        .wait_repository_state(RepositoryControlState::Busy)
        .await;
    assert_eq!(
        fixture
            .manager()
            .quiesce()
            .await
            .expect("quiesce child-unknown cleanup manager")
            .in_flight_workers(),
        1
    );
    fixture.finish().await;
}

async fn assert_worktree_restart(stage: CleanupStage, pending: CleanupOperationState) {
    let mut fixture = DeliveryCleanupFixture::new(None).await;
    fixture.runtime.fail_once(stage, CleanupFault::Unavailable);
    let accepted = fixture.remove().await;
    fixture
        .wait_operation_state(accepted.operation_id(), pending)
        .await;
    fixture
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    fixture.restart_manager().await;

    assert_eq!(
        fixture
            .manager()
            .recover_operation_for_test(accepted.operation_id())
            .await
            .expect("restarted cleanup manager remains open"),
        DeliveryOperationRecoveryOutcome::Converged
    );
    fixture
        .wait_operation_state(accepted.operation_id(), CleanupOperationState::Completed)
        .await;
    fixture
        .wait_repository_state(RepositoryControlState::Available)
        .await;
    fixture.finish().await;
}

fn count_calls(
    fixture: &DeliveryCleanupFixture,
    predicate: impl Fn(&CleanupCall) -> bool,
) -> usize {
    fixture
        .runtime
        .calls()
        .iter()
        .filter(|call| predicate(call))
        .count()
}

async fn cleanup_transition_count(
    fixture: &DeliveryCleanupFixture,
    operation_id: coding_agent_store::DeliveryOperationId,
    state: &str,
) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'cleanup_operation' AND entity_id = ? AND to_state = ?",
    )
    .bind(operation_id.to_string())
    .bind(state)
    .fetch_one(fixture.merge.base.store.pool())
    .await
    .expect("count cleanup transitions")
}

fn assert_poisoned(fixture: &DeliveryCleanupFixture) {
    assert_eq!(
        fixture
            .coordinator
            .control_state(fixture.task.repository_id)
            .expect("cleanup repository remains registered"),
        RepositoryControlState::Poisoned
    );
    assert_eq!(
        fixture
            .coordinator
            .poison_reason(fixture.task.repository_id)
            .expect("load cleanup poison reason"),
        Some(RepositoryControlPoisonReason::SideEffectIdentityMismatch)
    );
}

fn busy_controller(
    operation: StoreWriterOperationKind,
    count: u32,
) -> Arc<StoreWriterTestController> {
    Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::BusyBeforeExecute,
            operation: Some(operation),
            count,
        }])
        .expect("valid cleanup known-not-applied StoreWriter script"),
    )
}

async fn assert_retained_cleanup_worker(fixture: &DeliveryCleanupFixture, stage: &str) {
    fixture
        .wait_repository_state(RepositoryControlState::Busy)
        .await;
    assert_eq!(
        fixture
            .manager()
            .quiesce()
            .await
            .unwrap_or_else(|_| panic!("quiesce {stage} manager"))
            .in_flight_workers(),
        1,
        "{stage} must retain its lease, global permit, and actor worker"
    );
}

async fn assert_poisoned_released_cleanup_worker(fixture: &DeliveryCleanupFixture, stage: &str) {
    fixture
        .wait_repository_state(RepositoryControlState::Poisoned)
        .await;
    assert_eq!(
        fixture
            .manager()
            .quiesce()
            .await
            .unwrap_or_else(|_| panic!("quiesce {stage} manager"))
            .in_flight_workers(),
        0,
        "{stage} poison must release its permit and actor worker"
    );
}
