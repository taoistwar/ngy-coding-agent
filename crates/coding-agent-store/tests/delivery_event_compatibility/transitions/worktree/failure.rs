use coding_agent_store::{
    CleanupOperationState, RecordWorktreeCleanupFailureRequest,
    WorktreeCleanupKnownNotAppliedReason,
};

use super::super::helpers::applied_cleanup;
use super::super::scenario;
use super::{accept_cleanup, anchor, enter_remove_pending, record_unlocked};

pub async fn exercise_failure_and_retry_transitions() {
    fail_and_retry_unlock_pending().await;
    fail_and_retry_remove_pending().await;
}

async fn fail_and_retry_unlock_pending() {
    let (fixture, baseline, accepted_merge) = scenario::merged().await;
    let first = accept_cleanup(
        &fixture.store,
        &fixture.delivery_task,
        accepted_merge.operation_id,
        &baseline,
    )
    .await;
    applied_cleanup(
        fixture
            .store
            .record_worktree_cleanup_failure(
                RecordWorktreeCleanupFailureRequest::try_new(
                    anchor(
                        &fixture.delivery_task,
                        first.operation_id,
                        first.accepted_operation_version,
                    ),
                    CleanupOperationState::UnlockPending,
                    WorktreeCleanupKnownNotAppliedReason::CommandTimedOut,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    baseline
        .assert_unchanged(&fixture.store, "worktree unlock pending to failed")
        .await;
    let retry = accept_cleanup(
        &fixture.store,
        &fixture.delivery_task,
        accepted_merge.operation_id,
        &baseline,
    )
    .await;
    assert_ne!(retry.operation_id, first.operation_id);
}

async fn fail_and_retry_remove_pending() {
    let (fixture, baseline, accepted_merge) = scenario::merged().await;
    let first = accept_cleanup(
        &fixture.store,
        &fixture.delivery_task,
        accepted_merge.operation_id,
        &baseline,
    )
    .await;
    let unlocked = record_unlocked(&fixture.store, &fixture.delivery_task, &first, &baseline).await;
    let pending = enter_remove_pending(
        &fixture.store,
        &fixture.delivery_task,
        &first,
        unlocked,
        &baseline,
    )
    .await;
    applied_cleanup(
        fixture
            .store
            .record_worktree_cleanup_failure(
                RecordWorktreeCleanupFailureRequest::try_new(
                    anchor(&fixture.delivery_task, first.operation_id, pending),
                    CleanupOperationState::RemovePending,
                    WorktreeCleanupKnownNotAppliedReason::TargetWorktreeDirty,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    baseline
        .assert_unchanged(&fixture.store, "worktree remove pending to failed")
        .await;
    let retry = accept_cleanup(
        &fixture.store,
        &fixture.delivery_task,
        accepted_merge.operation_id,
        &baseline,
    )
    .await;
    assert_ne!(retry.operation_id, first.operation_id);
    assert_eq!(retry.accepted_operation_state.as_str(), "remove_pending");
}
