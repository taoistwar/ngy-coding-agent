use std::str::FromStr;

use coding_agent_store::{
    BeginMergeAbortRequest, CompleteMergeAbortRequest, DeliveryError, DirectoryIdentity,
    GitBranchRef, GitCommitOid, MergeAbortAppliedProof, MergeAbortProof, MergeAutostashObservation,
    MergeConflictPathEncoding, MergeConflictPaths, MergeOperationState, MergeTransitionOutcome,
    OtherGitOperationObservation, Sha256Digest,
};
use uuid::Uuid;

use super::fixtures::TARGET_BRANCH;
use crate::support::delivery::eligibility::{
    ADMIN_IDENTITY, COMMON_IDENTITY, CONFIG_DIGEST, SOURCE_COMMIT, TARGET_HEAD,
};

use super::fixtures::merge_pending;

const INDEX_STAGES: &str = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";
const WORKTREE: &str = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2";

#[test]
fn abort_proofs_require_explicit_autostash_and_operation_marker_absence() {
    assert!(
        MergeAbortProof::try_new(
            Uuid::new_v4(),
            GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
            GitCommitOid::from_str(TARGET_HEAD).unwrap(),
            GitBranchRef::from_str("refs/heads/codex/task-merge-store").unwrap(),
            GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
            GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
            DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
            DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
            "codex-reserved".to_owned(),
            Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
            Sha256Digest::from_str(INDEX_STAGES).unwrap(),
            Sha256Digest::from_str(WORKTREE).unwrap(),
            MergeAutostashObservation::Present,
            OtherGitOperationObservation::Clear,
            MergeConflictPaths::try_from_raw(vec![b"src/conflicted.rs".to_vec()]).unwrap(),
        )
        .is_err()
    );
    assert!(
        MergeAbortAppliedProof::try_new(
            GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
            GitCommitOid::from_str(TARGET_HEAD).unwrap(),
            GitBranchRef::from_str("refs/heads/codex/task-merge-store").unwrap(),
            GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
            DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
            DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
            "codex-reserved".to_owned(),
            Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
            0,
            0,
            0,
            0,
            None,
            MergeAutostashObservation::Absent,
            OtherGitOperationObservation::Present,
        )
        .is_err()
    );
    assert!(
        abort_begin_proof_with_paths_result(Uuid::new_v4(), Vec::new()).is_err(),
        "a known conflict must bind at least one durable path"
    );
}

#[tokio::test]
async fn known_conflict_proof_is_durable_before_abort_can_run() {
    let (store, task, operation_id, pending_version) = merge_pending().await;
    let child_receipt_id = Uuid::new_v4();
    let proof = MergeAbortProof::try_new(
        child_receipt_id,
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
        GitBranchRef::from_str("refs/heads/codex/task-merge-store").unwrap(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
        "codex-reserved".to_owned(),
        Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
        Sha256Digest::from_str(INDEX_STAGES).unwrap(),
        Sha256Digest::from_str(WORKTREE).unwrap(),
        MergeAutostashObservation::Absent,
        OtherGitOperationObservation::Clear,
        MergeConflictPaths::try_from_raw(vec![
            b"src/conflicted.rs".to_vec(),
            b"src/\xff.bin".to_vec(),
        ])
        .unwrap(),
    )
    .unwrap();
    let proof_debug = format!("{proof:?}");
    assert!(!proof_debug.contains("src/conflicted.rs"));
    let request =
        BeginMergeAbortRequest::try_new(task.id, operation_id, pending_version, proof).unwrap();

    assert!(matches!(
        store.begin_merge_abort(request.clone()).await.unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));
    assert!(matches!(
        store.begin_merge_abort(request).await.unwrap(),
        MergeTransitionOutcome::Existing(_)
    ));
    let operation = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    assert_eq!(operation.state, MergeOperationState::AbortPending);
    assert_eq!(operation.abort_child_receipt_id, Some(child_receipt_id));
    assert_eq!(operation.abort_merge_head.unwrap().as_str(), SOURCE_COMMIT);
    assert_eq!(
        operation.abort_merge_autostash_proof.as_deref(),
        Some("absent")
    );
    assert_eq!(operation.conflict_path_count, Some(2));
    assert_eq!(operation.conflicts.len(), 2);
    assert_eq!(
        operation.conflicts[0].path_encoding,
        MergeConflictPathEncoding::Utf8
    );
    assert_eq!(
        operation.conflicts[1].path_encoding,
        MergeConflictPathEncoding::Base64Url
    );
}

#[tokio::test]
async fn exact_abort_postcondition_advances_abort_pending_to_conflict_with_paths() {
    let (store, task, operation_id, pending_version) = merge_pending().await;
    let begin = BeginMergeAbortRequest::try_new(
        task.id,
        operation_id,
        pending_version,
        MergeAbortProof::try_new(
            Uuid::new_v4(),
            GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
            GitCommitOid::from_str(TARGET_HEAD).unwrap(),
            GitBranchRef::from_str("refs/heads/codex/task-merge-store").unwrap(),
            GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
            GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
            DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
            DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
            "codex-reserved".to_owned(),
            Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
            Sha256Digest::from_str(INDEX_STAGES).unwrap(),
            Sha256Digest::from_str(WORKTREE).unwrap(),
            MergeAutostashObservation::Absent,
            OtherGitOperationObservation::Clear,
            MergeConflictPaths::try_from_raw(vec![b"src/conflicted.rs".to_vec()]).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let abort_version = match store.begin_merge_abort(begin).await.unwrap() {
        MergeTransitionOutcome::Applied(receipt) => receipt.version,
        other => panic!("expected abort pending, got {other:?}"),
    };
    let proof = MergeAbortAppliedProof::try_new(
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
        GitBranchRef::from_str("refs/heads/codex/task-merge-store").unwrap(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
        "codex-reserved".to_owned(),
        Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
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
        CompleteMergeAbortRequest::try_new(task.id, operation_id, abort_version, proof).unwrap();

    assert!(matches!(
        store.complete_merge_abort(request.clone()).await.unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));
    assert!(matches!(
        store.complete_merge_abort(request).await.unwrap(),
        MergeTransitionOutcome::Existing(_)
    ));
    let operation = store
        .delivery_ownership_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap();
    assert_eq!(operation.state, MergeOperationState::Conflict);
    assert_eq!(operation.failure_code.unwrap().as_str(), "MERGE_CONFLICT");
    assert_eq!(operation.conflicts.len(), 1);
}

pub(super) async fn abort_pending_fixture() -> (
    coding_agent_store::Store,
    coding_agent_domain::Task,
    coding_agent_store::DeliveryOperationId,
    coding_agent_store::DeliveryVersion,
) {
    abort_pending_fixture_with_paths(vec![b"src/conflicted.rs".to_vec()]).await
}

pub(super) async fn abort_pending_fixture_with_paths(
    raw_paths: Vec<Vec<u8>>,
) -> (
    coding_agent_store::Store,
    coding_agent_domain::Task,
    coding_agent_store::DeliveryOperationId,
    coding_agent_store::DeliveryVersion,
) {
    let (store, task, operation_id, pending_version) = merge_pending().await;
    let request = BeginMergeAbortRequest::try_new(
        task.id,
        operation_id,
        pending_version,
        exact_abort_begin_proof_with_paths(Uuid::new_v4(), raw_paths),
    )
    .unwrap();
    let abort_version = match store.begin_merge_abort(request).await.unwrap() {
        MergeTransitionOutcome::Applied(receipt) => receipt.version,
        other => panic!("expected abort pending, got {other:?}"),
    };
    (store, task, operation_id, abort_version)
}

pub(super) fn exact_abort_begin_proof(child_receipt_id: Uuid) -> MergeAbortProof {
    exact_abort_begin_proof_with_paths(child_receipt_id, vec![b"src/conflicted.rs".to_vec()])
}

pub(super) fn exact_abort_begin_proof_with_paths(
    child_receipt_id: Uuid,
    raw_paths: Vec<Vec<u8>>,
) -> MergeAbortProof {
    abort_begin_proof_with_paths_result(child_receipt_id, raw_paths).unwrap()
}

fn abort_begin_proof_with_paths_result(
    child_receipt_id: Uuid,
    raw_paths: Vec<Vec<u8>>,
) -> Result<MergeAbortProof, DeliveryError> {
    MergeAbortProof::try_new(
        child_receipt_id,
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
        GitBranchRef::from_str("refs/heads/codex/task-merge-store").unwrap(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
        "codex-reserved".to_owned(),
        Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
        Sha256Digest::from_str(INDEX_STAGES).unwrap(),
        Sha256Digest::from_str(WORKTREE).unwrap(),
        MergeAutostashObservation::Absent,
        OtherGitOperationObservation::Clear,
        MergeConflictPaths::try_from_raw(raw_paths)?,
    )
}
