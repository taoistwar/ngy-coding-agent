use coding_agent_domain::Task;
use coding_agent_store::{
    CleanupAcceptanceOutcome, CleanupOperationAnchor, CompleteWorktreeCleanupRequest,
    DeliveryCommandReceipt, DeliveryOperationId, DeliveryVersion,
    EnterWorktreeRemovePendingRequest, RecordWorktreeUnlockedRequest, RemoveWorktreeCommandRequest,
    Store,
};

use crate::snapshot::CompatibilitySnapshot;

use super::helpers::{applied_cleanup, ownership};

mod failure;
mod reconcile;

pub async fn remove_worktree(
    store: &Store,
    task: &Task,
    merge_operation_id: DeliveryOperationId,
    baseline: &CompatibilitySnapshot,
) {
    let accepted = accept_cleanup(store, task, merge_operation_id, baseline).await;
    let unlocked = record_unlocked(store, task, &accepted, baseline).await;
    let pending = enter_remove_pending(store, task, &accepted, unlocked, baseline).await;
    complete_cleanup(store, task, &accepted, pending, baseline).await;
}

pub async fn exercise_failure_retry_and_reconcile_transitions() {
    failure::exercise_failure_and_retry_transitions().await;
    reconcile::exercise_reconcile_transitions().await;
}

pub(super) async fn accept_cleanup(
    store: &Store,
    task: &Task,
    merge_operation_id: DeliveryOperationId,
    baseline: &CompatibilitySnapshot,
) -> DeliveryCommandReceipt {
    let snapshot = ownership(store, task.id).await;
    let source = snapshot.source.unwrap();
    let disposition = snapshot.disposition.unwrap();
    let request = RemoveWorktreeCommandRequest::try_new(
        coding_agent_domain::ClientRequestId::new(),
        task.id,
        disposition.worktree_version,
        merge_operation_id,
        source.provenance.source_branch,
        source.expected_source_commit.unwrap(),
    )
    .unwrap();
    let accepted = match store.accept_worktree_cleanup(request).await.unwrap() {
        CleanupAcceptanceOutcome::Accepted(receipt) => receipt,
        other => panic!("expected accepted worktree cleanup, got {other:?}"),
    };
    baseline
        .assert_unchanged(store, "worktree cleanup accepted")
        .await;
    accepted
}

pub(super) async fn record_unlocked(
    store: &Store,
    task: &Task,
    accepted: &DeliveryCommandReceipt,
    baseline: &CompatibilitySnapshot,
) -> DeliveryVersion {
    let receipt = applied_cleanup(
        store
            .record_worktree_unlocked(
                RecordWorktreeUnlockedRequest::try_new(anchor(
                    task,
                    accepted.operation_id,
                    accepted.accepted_operation_version,
                ))
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    baseline
        .assert_unchanged(store, "worktree unlock pending to unlocked pending remove")
        .await;
    receipt.version
}

pub(super) async fn enter_remove_pending(
    store: &Store,
    task: &Task,
    accepted: &DeliveryCommandReceipt,
    version: DeliveryVersion,
    baseline: &CompatibilitySnapshot,
) -> DeliveryVersion {
    let receipt = applied_cleanup(
        store
            .enter_worktree_remove_pending(
                EnterWorktreeRemovePendingRequest::try_new(anchor(
                    task,
                    accepted.operation_id,
                    version,
                ))
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    baseline
        .assert_unchanged(store, "worktree unlocked pending remove to remove pending")
        .await;
    receipt.version
}

async fn complete_cleanup(
    store: &Store,
    task: &Task,
    accepted: &DeliveryCommandReceipt,
    version: DeliveryVersion,
    baseline: &CompatibilitySnapshot,
) {
    applied_cleanup(
        store
            .complete_worktree_cleanup(
                CompleteWorktreeCleanupRequest::try_new(anchor(
                    task,
                    accepted.operation_id,
                    version,
                ))
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    baseline
        .assert_unchanged(store, "worktree remove pending to removed")
        .await;
}

pub(super) fn anchor(
    task: &Task,
    operation_id: DeliveryOperationId,
    version: DeliveryVersion,
) -> CleanupOperationAnchor {
    CleanupOperationAnchor::try_new(task.id, operation_id, version).unwrap()
}
