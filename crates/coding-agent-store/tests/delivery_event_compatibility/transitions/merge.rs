use std::str::FromStr;

use coding_agent_domain::{Task, TaskId};
use coding_agent_store::{
    CompleteMergeRequest, EnterMergePendingRequest, GitBranchRef, GitCommitOid, GitTreeOid,
    MergeAppliedProof, MergeAutostashObservation, MergeCommitObjectProof,
    OtherGitOperationObservation, Store,
};

use crate::snapshot::CompatibilitySnapshot;
use crate::support::delivery::eligibility::{MERGE_COMMIT, MERGE_TREE, SOURCE_COMMIT, TARGET_HEAD};

use super::TARGET_BRANCH;
use super::helpers::{applied_merge, ownership};
use super::preflight::AcceptedPreflight;

mod abort;
mod reconcile;
mod terminal;

pub async fn complete_delivery_merge(
    store: &Store,
    task: &Task,
    accepted: &AcceptedPreflight,
    baseline: &CompatibilitySnapshot,
) {
    let object = merge_object(store, task.id, accepted).await;
    let pending_version =
        enter_merge_pending(store, task, accepted, object.clone(), baseline).await;

    let source = ownership(store, task.id).await.source.unwrap();
    let proof = MergeAppliedProof::try_new(
        object,
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(MERGE_COMMIT).unwrap(),
        source.provenance.source_branch.clone(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        source.provenance.common_git_identity.clone(),
        source.provenance.worktree_admin_identity.clone(),
        source.provenance.fixed_lock_reason.clone(),
        source.provenance.config_attributes_digest.clone(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
        0,
        0,
        0,
        0,
        None,
        MergeAutostashObservation::Absent,
        OtherGitOperationObservation::Clear,
    )
    .unwrap();
    applied_merge(
        store
            .complete_merge(
                CompleteMergeRequest::try_new(
                    task.id,
                    accepted.operation_id,
                    pending_version,
                    proof,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    baseline
        .assert_unchanged(store, "merge pending to merged")
        .await;
}

pub async fn exercise_failure_abort_and_reconcile_transitions() {
    terminal::exercise_known_failure_transitions().await;
    abort::exercise_abort_transitions().await;
    reconcile::exercise_reconcile_transitions().await;
}

pub(super) async fn enter_pending(
    store: &Store,
    task: &Task,
    accepted: &AcceptedPreflight,
    baseline: &CompatibilitySnapshot,
) -> coding_agent_store::DeliveryVersion {
    let object = merge_object(store, task.id, accepted).await;
    enter_merge_pending(store, task, accepted, object, baseline).await
}

async fn enter_merge_pending(
    store: &Store,
    task: &Task,
    accepted: &AcceptedPreflight,
    object: MergeCommitObjectProof,
    baseline: &CompatibilitySnapshot,
) -> coding_agent_store::DeliveryVersion {
    let pending = applied_merge(
        store
            .enter_merge_pending(
                EnterMergePendingRequest::try_new(
                    task.id,
                    accepted.operation_id,
                    accepted.version,
                    object.clone(),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    baseline
        .assert_unchanged(store, "accepted to merge pending")
        .await;
    pending.version
}

async fn merge_object(
    store: &Store,
    task_id: TaskId,
    accepted: &AcceptedPreflight,
) -> MergeCommitObjectProof {
    let operation = ownership(store, task_id)
        .await
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == accepted.operation_id)
        .unwrap();
    MergeCommitObjectProof::try_new(
        GitCommitOid::from_str(MERGE_COMMIT).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
        vec![
            GitCommitOid::from_str(TARGET_HEAD).unwrap(),
            GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        ],
        operation.merge_metadata.unwrap(),
    )
    .unwrap()
}
