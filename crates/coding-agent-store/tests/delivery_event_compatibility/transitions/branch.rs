use std::str::FromStr;

use coding_agent_domain::{ClientRequestId, Task};
use coding_agent_store::{
    CleanupAcceptanceOutcome, CleanupOperationAnchor, CompleteBranchCleanupRequest,
    DeleteBranchCommandRequest, DeliveryCommandReceipt, DeliveryOperationId, DeliveryVersion,
    GitCommitOid, RefreshBranchCleanupTargetRequest, Store,
};

use crate::snapshot::CompatibilitySnapshot;

use super::helpers::{applied_cleanup, ownership};

mod terminal;

pub async fn delete_branch(
    store: &Store,
    task: &Task,
    merge_operation_id: DeliveryOperationId,
    baseline: &CompatibilitySnapshot,
) {
    let initial_head = target_head("3333333333333333333333333333333333333333");
    let accepted = accept_cleanup(
        store,
        task,
        merge_operation_id,
        initial_head.clone(),
        baseline,
    )
    .await;
    let fresh_head = target_head("4444444444444444444444444444444444444444");
    let refreshed = applied_cleanup(
        store
            .refresh_branch_cleanup_target(
                RefreshBranchCleanupTargetRequest::try_new(
                    anchor(
                        task,
                        accepted.operation_id,
                        accepted.accepted_operation_version,
                    ),
                    initial_head,
                    fresh_head,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    baseline
        .assert_unchanged(store, "branch delete target refresh")
        .await;
    applied_cleanup(
        store
            .complete_branch_cleanup(
                CompleteBranchCleanupRequest::try_new(anchor(
                    task,
                    accepted.operation_id,
                    refreshed.version,
                ))
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    baseline
        .assert_unchanged(store, "branch delete pending to deleted")
        .await;
}

pub async fn exercise_failure_retry_and_reconcile_transitions() {
    terminal::exercise_failure_retry_and_reconcile_transitions().await;
}

pub(super) async fn accept_cleanup(
    store: &Store,
    task: &Task,
    merge_operation_id: DeliveryOperationId,
    initial_head: GitCommitOid,
    baseline: &CompatibilitySnapshot,
) -> DeliveryCommandReceipt {
    let snapshot = ownership(store, task.id).await;
    let source = snapshot.source.unwrap();
    let disposition = snapshot.disposition.unwrap();
    let merged = snapshot
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == merge_operation_id)
        .unwrap();
    let request = DeleteBranchCommandRequest::try_new(
        ClientRequestId::new(),
        task.id,
        disposition.branch_version,
        merge_operation_id,
        source.provenance.source_branch,
        source.expected_source_commit.unwrap(),
        merged.target_branch,
        initial_head,
    )
    .unwrap();
    let accepted = match store.accept_branch_cleanup(request).await.unwrap() {
        CleanupAcceptanceOutcome::Accepted(receipt) => receipt,
        other => panic!("expected accepted branch cleanup, got {other:?}"),
    };
    baseline
        .assert_unchanged(store, "branch cleanup accepted")
        .await;
    accepted
}

pub(super) fn target_head(value: &str) -> GitCommitOid {
    GitCommitOid::from_str(value).unwrap()
}

pub(super) fn anchor(
    task: &Task,
    operation_id: DeliveryOperationId,
    version: DeliveryVersion,
) -> CleanupOperationAnchor {
    CleanupOperationAnchor::try_new(task.id, operation_id, version).unwrap()
}
