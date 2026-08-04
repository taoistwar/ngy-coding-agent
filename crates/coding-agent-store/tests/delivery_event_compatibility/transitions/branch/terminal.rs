use coding_agent_store::{
    BranchCleanupKnownNotAppliedReason, CleanupReconciliationReason, ReconcileBranchCleanupRequest,
    RecordBranchCleanupFailureRequest,
};

use super::super::helpers::applied_cleanup;
use super::super::{scenario, worktree};
use super::{accept_cleanup, anchor, target_head};

pub async fn exercise_failure_retry_and_reconcile_transitions() {
    fail_and_retry_delete_pending().await;
    reconcile_delete_pending().await;
}

async fn fail_and_retry_delete_pending() {
    let (fixture, baseline, merge) = scenario::merged().await;
    worktree::remove_worktree(
        &fixture.store,
        &fixture.delivery_task,
        merge.operation_id,
        &baseline,
    )
    .await;
    let first = accept_cleanup(
        &fixture.store,
        &fixture.delivery_task,
        merge.operation_id,
        target_head("3333333333333333333333333333333333333333"),
        &baseline,
    )
    .await;
    applied_cleanup(
        fixture
            .store
            .record_branch_cleanup_failure(
                RecordBranchCleanupFailureRequest::try_new(
                    anchor(
                        &fixture.delivery_task,
                        first.operation_id,
                        first.accepted_operation_version,
                    ),
                    BranchCleanupKnownNotAppliedReason::SourceBranchNotMerged,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    baseline
        .assert_unchanged(&fixture.store, "branch delete pending to failed")
        .await;
    let retry = accept_cleanup(
        &fixture.store,
        &fixture.delivery_task,
        merge.operation_id,
        target_head("4444444444444444444444444444444444444444"),
        &baseline,
    )
    .await;
    assert_ne!(retry.operation_id, first.operation_id);
}

async fn reconcile_delete_pending() {
    let (fixture, baseline, merge) = scenario::merged().await;
    worktree::remove_worktree(
        &fixture.store,
        &fixture.delivery_task,
        merge.operation_id,
        &baseline,
    )
    .await;
    let accepted = accept_cleanup(
        &fixture.store,
        &fixture.delivery_task,
        merge.operation_id,
        target_head("5555555555555555555555555555555555555555"),
        &baseline,
    )
    .await;
    applied_cleanup(
        fixture
            .store
            .reconcile_branch_cleanup(
                ReconcileBranchCleanupRequest::try_new(
                    anchor(
                        &fixture.delivery_task,
                        accepted.operation_id,
                        accepted.accepted_operation_version,
                    ),
                    CleanupReconciliationReason::SourceInconsistent,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    baseline
        .assert_unchanged(
            &fixture.store,
            "branch delete pending to reconciliation required",
        )
        .await;
}
