use std::str::FromStr;

use coding_agent_domain::{Task, TaskId};
use coding_agent_store::{
    BeginMergeAbortRequest, CompleteMergeAbortRequest, DeliveryOperationId, DeliveryVersion,
    GitBranchRef, GitCommitOid, MergeAbortAppliedProof, MergeAbortProof, MergeAutostashObservation,
    MergeConflictPaths, OtherGitOperationObservation, Sha256Digest, Store,
};
use uuid::Uuid;

use crate::snapshot::CompatibilitySnapshot;
use crate::support::delivery::eligibility::{SOURCE_COMMIT, TARGET_HEAD};

use super::super::TARGET_BRANCH;
use super::super::helpers::{applied_merge, ownership};
use super::super::scenario;
use super::enter_pending;

const INDEX_STAGES: &str = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";
const WORKTREE: &str = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2";

pub async fn exercise_abort_transitions() {
    let (fixture, baseline, accepted) = scenario::committed_source().await;
    let pending_version =
        enter_pending(&fixture.store, &fixture.delivery_task, &accepted, &baseline).await;
    let abort_version = begin_abort(
        &fixture.store,
        &fixture.delivery_task,
        accepted.operation_id,
        pending_version,
        &baseline,
    )
    .await;
    complete_abort(
        &fixture.store,
        &fixture.delivery_task,
        accepted.operation_id,
        abort_version,
        &baseline,
    )
    .await;
}

pub(super) async fn begin_abort(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
    pending_version: DeliveryVersion,
    baseline: &CompatibilitySnapshot,
) -> DeliveryVersion {
    let proof = abort_begin_proof(store, task.id).await;
    let receipt = applied_merge(
        store
            .begin_merge_abort(
                BeginMergeAbortRequest::try_new(task.id, operation_id, pending_version, proof)
                    .unwrap(),
            )
            .await
            .unwrap(),
    );
    baseline
        .assert_unchanged(store, "merge pending to abort pending")
        .await;
    receipt.version
}

async fn complete_abort(
    store: &Store,
    task: &Task,
    operation_id: DeliveryOperationId,
    abort_version: DeliveryVersion,
    baseline: &CompatibilitySnapshot,
) {
    let proof = abort_applied_proof(store, task.id).await;
    applied_merge(
        store
            .complete_merge_abort(
                CompleteMergeAbortRequest::try_new(
                    task.id,
                    operation_id,
                    abort_version,
                    proof,
                    MergeConflictPaths::try_from_raw(vec![b"src/conflicted.rs".to_vec()]).unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    baseline
        .assert_unchanged(store, "abort pending to conflict")
        .await;
}

async fn abort_begin_proof(store: &Store, task_id: TaskId) -> MergeAbortProof {
    let source = ownership(store, task_id).await.source.unwrap();
    MergeAbortProof::try_new(
        Uuid::new_v4(),
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
        source.provenance.source_branch,
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        source.provenance.common_git_identity,
        source.provenance.worktree_admin_identity,
        source.provenance.fixed_lock_reason,
        source.provenance.config_attributes_digest,
        Sha256Digest::from_str(INDEX_STAGES).unwrap(),
        Sha256Digest::from_str(WORKTREE).unwrap(),
        MergeAutostashObservation::Absent,
        OtherGitOperationObservation::Clear,
    )
    .unwrap()
}

async fn abort_applied_proof(store: &Store, task_id: TaskId) -> MergeAbortAppliedProof {
    let source = ownership(store, task_id).await.source.unwrap();
    MergeAbortAppliedProof::try_new(
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
        source.provenance.source_branch,
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        source.provenance.common_git_identity,
        source.provenance.worktree_admin_identity,
        source.provenance.fixed_lock_reason,
        source.provenance.config_attributes_digest,
        0,
        0,
        0,
        0,
        None,
        MergeAutostashObservation::Absent,
        OtherGitOperationObservation::Clear,
    )
    .unwrap()
}
