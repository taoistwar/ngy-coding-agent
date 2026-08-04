use std::str::FromStr;

use coding_agent_store::{
    BranchDisposition, CompleteMergeRequest, DirectoryIdentity, GitBranchRef, GitCommitOid,
    GitTreeOid, MergeAppliedProof, MergeAutostashObservation, MergeCommitObjectProof,
    MergeOperationState, MergeTransitionOutcome, OtherGitOperationObservation, Sha256Digest,
    WorktreeDisposition,
};

use crate::support::delivery::eligibility::{
    ADMIN_IDENTITY, COMMON_IDENTITY, CONFIG_DIGEST, MERGE_COMMIT, MERGE_TREE, SOURCE_COMMIT,
    TARGET_HEAD,
};

use super::fixtures::{TARGET_BRANCH, merge_pending};

#[tokio::test]
async fn exact_applied_merge_and_initial_dispositions_commit_atomically() {
    let (store, task, operation_id, pending_version) = merge_pending().await;
    let operation = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    let object = MergeCommitObjectProof::try_new(
        GitCommitOid::from_str(MERGE_COMMIT).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
        vec![
            GitCommitOid::from_str(TARGET_HEAD).unwrap(),
            GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        ],
        operation.merge_metadata.unwrap(),
    )
    .unwrap();
    let proof = MergeAppliedProof::try_new(
        object,
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(MERGE_COMMIT).unwrap(),
        GitBranchRef::from_str("refs/heads/codex/task-merge-store").unwrap(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
        "codex-reserved".to_owned(),
        Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
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
    let request =
        CompleteMergeRequest::try_new(task.id, operation_id, pending_version, proof).unwrap();

    assert!(matches!(
        store.complete_merge(request.clone()).await.unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));
    assert!(matches!(
        store.complete_merge(request).await.unwrap(),
        MergeTransitionOutcome::Existing(_)
    ));
    let ownership = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let merged = ownership
        .merge_operations
        .iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    let disposition = ownership.disposition.unwrap();
    assert_eq!(merged.state, MergeOperationState::Merged);
    assert_eq!(disposition.merged_operation_id, operation_id);
    assert_eq!(
        disposition.worktree_state,
        WorktreeDisposition::RetainedLocked
    );
    assert_eq!(disposition.branch_state, BranchDisposition::Retained);
    assert!(merged.current_transition_id < disposition.worktree_initial_transition_id);
    assert!(merged.current_transition_id < disposition.branch_initial_transition_id);
    assert_eq!(merged.updated_at, disposition.created_at);
    assert_eq!(merged.updated_at, disposition.worktree_updated_at);
    assert_eq!(merged.updated_at, disposition.branch_updated_at);
}
