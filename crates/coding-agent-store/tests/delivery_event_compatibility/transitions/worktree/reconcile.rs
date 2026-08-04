use coding_agent_store::{
    CleanupOperationState, CleanupReconciliationReason, DeliveryVersion,
    ReconcileWorktreeCleanupRequest,
};

use super::super::helpers::applied_cleanup;
use super::super::scenario;
use super::{accept_cleanup, anchor, enter_remove_pending, record_unlocked};

#[derive(Clone, Copy)]
enum ActiveStage {
    UnlockPending,
    UnlockedPendingRemove,
    RemovePending,
}

pub async fn exercise_reconcile_transitions() {
    reconcile_from(ActiveStage::UnlockPending).await;
    reconcile_from(ActiveStage::UnlockedPendingRemove).await;
    reconcile_from(ActiveStage::RemovePending).await;
}

async fn reconcile_from(stage: ActiveStage) {
    let (fixture, baseline, accepted_merge) = scenario::merged().await;
    let accepted = accept_cleanup(
        &fixture.store,
        &fixture.delivery_task,
        accepted_merge.operation_id,
        &baseline,
    )
    .await;
    let (state, version) = prepare_stage(&fixture, &baseline, &accepted, stage).await;
    applied_cleanup(
        fixture
            .store
            .reconcile_worktree_cleanup(
                ReconcileWorktreeCleanupRequest::try_new(
                    anchor(&fixture.delivery_task, accepted.operation_id, version),
                    state,
                    CleanupReconciliationReason::WorktreeIdentityMismatch,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    baseline
        .assert_unchanged(&fixture.store, reconcile_label(stage))
        .await;
}

async fn prepare_stage(
    fixture: &crate::fixture::CompatibilityFixture,
    baseline: &crate::snapshot::CompatibilitySnapshot,
    accepted: &coding_agent_store::DeliveryCommandReceipt,
    stage: ActiveStage,
) -> (CleanupOperationState, DeliveryVersion) {
    if matches!(stage, ActiveStage::UnlockPending) {
        return (
            CleanupOperationState::UnlockPending,
            accepted.accepted_operation_version,
        );
    }
    let unlocked =
        record_unlocked(&fixture.store, &fixture.delivery_task, accepted, baseline).await;
    if matches!(stage, ActiveStage::UnlockedPendingRemove) {
        return (CleanupOperationState::UnlockedPendingRemove, unlocked);
    }
    let pending = enter_remove_pending(
        &fixture.store,
        &fixture.delivery_task,
        accepted,
        unlocked,
        baseline,
    )
    .await;
    (CleanupOperationState::RemovePending, pending)
}

const fn reconcile_label(stage: ActiveStage) -> &'static str {
    match stage {
        ActiveStage::UnlockPending => "worktree unlock pending to reconciliation required",
        ActiveStage::UnlockedPendingRemove => {
            "worktree unlocked pending remove to reconciliation required"
        }
        ActiveStage::RemovePending => "worktree remove pending to reconciliation required",
    }
}
