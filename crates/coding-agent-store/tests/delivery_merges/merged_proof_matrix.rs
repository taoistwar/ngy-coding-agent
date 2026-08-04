use std::str::FromStr;

use coding_agent_store::{
    CompleteMergeRequest, DeliveryCommitMetadata, DeliveryError, DirectoryIdentity, GitBranchRef,
    GitCommitOid, GitTreeOid, MergeAppliedProof, MergeAutostashObservation, MergeCommitObjectProof,
    MergeTransitionOutcome, OtherGitOperationObservation, Sha256Digest, Store,
};

use crate::support::delivery::eligibility::{
    ADMIN_IDENTITY, COMMON_IDENTITY, CONFIG_DIGEST, MERGE_COMMIT, MERGE_TREE, SOURCE_COMMIT,
    TARGET_HEAD,
};

use super::fixtures::TARGET_BRANCH;

const SOURCE_BRANCH: &str = "refs/heads/codex/task-merge-store";
const ALT_TARGET_BRANCH: &str = "refs/heads/release";
const ALT_SOURCE_BRANCH: &str = "refs/heads/codex/other-source";
const ALT_MERGE_COMMIT: &str = "abababababababababababababababababababab";
const ALT_PARENT: &str = "bcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbc";
const ALT_TREE: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
const ALT_COMMON_IDENTITY: &str =
    "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1";
const ALT_ADMIN_IDENTITY: &str = "f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2";
const ALT_CONFIG_DIGEST: &str = "f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3";

#[derive(Debug, Clone, Copy)]
enum ProofMutation {
    Exact,
    ExpectedMergeCommit,
    ObjectTree,
    TargetParent,
    SourceParent,
    Metadata,
    TargetBranch,
    TargetHead,
    SourceBranch,
    SourceOid,
    CommonIdentity,
    AdminIdentity,
    ConfigDigest,
}

const MUTATIONS: [ProofMutation; 12] = [
    ProofMutation::ExpectedMergeCommit,
    ProofMutation::ObjectTree,
    ProofMutation::TargetParent,
    ProofMutation::SourceParent,
    ProofMutation::Metadata,
    ProofMutation::TargetBranch,
    ProofMutation::TargetHead,
    ProofMutation::SourceBranch,
    ProofMutation::SourceOid,
    ProofMutation::CommonIdentity,
    ProofMutation::AdminIdentity,
    ProofMutation::ConfigDigest,
];

#[tokio::test]
async fn complete_merge_rejects_every_fresh_proof_binding_mismatch_without_writes() {
    for mutation in MUTATIONS {
        let (store, task, operation_id, pending_version) = super::fixtures::merge_pending().await;
        let metadata = merge_metadata(&store, task.id, operation_id).await;
        let request = CompleteMergeRequest::try_new(
            task.id,
            operation_id,
            pending_version,
            applied_proof(metadata, mutation),
        )
        .unwrap();
        let outcome = store.complete_merge(request).await.unwrap();
        assert!(
            matches!(outcome, MergeTransitionOutcome::Conflict),
            "{mutation:?} produced {outcome:?}"
        );
        assert_merge_pending_without_disposition(&store, task.id, operation_id).await;
    }
}

#[tokio::test]
async fn complete_merge_changed_proof_replay_is_conflict_and_preserves_the_disposition() {
    let (store, task, operation_id, pending_version) = super::fixtures::merge_pending().await;
    let metadata = merge_metadata(&store, task.id, operation_id).await;
    let exact = CompleteMergeRequest::try_new(
        task.id,
        operation_id,
        pending_version,
        applied_proof(metadata.clone(), ProofMutation::Exact),
    )
    .unwrap();
    assert!(matches!(
        store.complete_merge(exact).await.unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));

    for mutation in MUTATIONS {
        let changed = CompleteMergeRequest::try_new(
            task.id,
            operation_id,
            pending_version,
            applied_proof(metadata.clone(), mutation),
        )
        .unwrap();
        let outcome = store.complete_merge(changed).await.unwrap();
        assert!(
            matches!(outcome, MergeTransitionOutcome::Conflict),
            "{mutation:?} produced {outcome:?}"
        );
    }
    let row: (String, i64, Option<String>) = sqlx::query_as(
        "SELECT state, version, merged_disposition_task_id \
         FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(row, ("merged".to_owned(), 5, Some(task.id.to_string())));
    let dispositions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_artifact_dispositions WHERE task_id = ?")
            .bind(task.id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(dispositions, 1);
    assert_eq!(merge_journal_count(&store, operation_id).await, 5);
}

#[test]
fn merge_applied_proof_constructor_rejects_dirty_or_incoherent_postconditions() {
    let metadata = test_metadata();
    assert!(
        raw_applied_proof(
            metadata.clone(),
            ALT_TREE,
            MERGE_TREE,
            (0, 0, 0, 0),
            None,
            "codex-reserved",
            MergeAutostashObservation::Absent,
            OtherGitOperationObservation::Clear
        )
        .is_err()
    );
    assert!(
        raw_applied_proof(
            metadata.clone(),
            MERGE_TREE,
            ALT_TREE,
            (0, 0, 0, 0),
            None,
            "codex-reserved",
            MergeAutostashObservation::Absent,
            OtherGitOperationObservation::Clear
        )
        .is_err()
    );
    for counts in [(1, 0, 0, 0), (0, 1, 0, 0), (0, 0, 1, 0), (0, 0, 0, 1)] {
        assert!(
            raw_applied_proof(
                metadata.clone(),
                MERGE_TREE,
                MERGE_TREE,
                counts,
                None,
                "codex-reserved",
                MergeAutostashObservation::Absent,
                OtherGitOperationObservation::Clear
            )
            .is_err()
        );
    }
    assert!(
        raw_applied_proof(
            metadata.clone(),
            MERGE_TREE,
            MERGE_TREE,
            (0, 0, 0, 0),
            Some(SOURCE_COMMIT),
            "codex-reserved",
            MergeAutostashObservation::Absent,
            OtherGitOperationObservation::Clear
        )
        .is_err()
    );
    assert!(
        raw_applied_proof(
            metadata.clone(),
            MERGE_TREE,
            MERGE_TREE,
            (0, 0, 0, 0),
            None,
            "other-lock",
            MergeAutostashObservation::Absent,
            OtherGitOperationObservation::Clear
        )
        .is_err()
    );
    assert!(
        raw_applied_proof(
            metadata.clone(),
            MERGE_TREE,
            MERGE_TREE,
            (0, 0, 0, 0),
            None,
            "codex-reserved",
            MergeAutostashObservation::Present,
            OtherGitOperationObservation::Clear
        )
        .is_err()
    );
    assert!(
        raw_applied_proof(
            metadata.clone(),
            MERGE_TREE,
            MERGE_TREE,
            (0, 0, 0, 0),
            None,
            "codex-reserved",
            MergeAutostashObservation::Unobservable,
            OtherGitOperationObservation::Clear
        )
        .is_err()
    );
    assert!(
        raw_applied_proof(
            metadata.clone(),
            MERGE_TREE,
            MERGE_TREE,
            (0, 0, 0, 0),
            None,
            "codex-reserved",
            MergeAutostashObservation::Absent,
            OtherGitOperationObservation::Present
        )
        .is_err()
    );
    assert!(
        raw_applied_proof(
            metadata,
            MERGE_TREE,
            MERGE_TREE,
            (0, 0, 0, 0),
            None,
            "codex-reserved",
            MergeAutostashObservation::Absent,
            OtherGitOperationObservation::Unobservable
        )
        .is_err()
    );
}

fn applied_proof(
    mut metadata: DeliveryCommitMetadata,
    mutation: ProofMutation,
) -> MergeAppliedProof {
    let expected_merge = if matches!(mutation, ProofMutation::ExpectedMergeCommit) {
        ALT_MERGE_COMMIT
    } else {
        MERGE_COMMIT
    };
    let tree = if matches!(mutation, ProofMutation::ObjectTree) {
        ALT_TREE
    } else {
        MERGE_TREE
    };
    let target_parent = if matches!(mutation, ProofMutation::TargetParent) {
        ALT_PARENT
    } else {
        TARGET_HEAD
    };
    let source_parent = if matches!(mutation, ProofMutation::SourceParent) {
        ALT_PARENT
    } else {
        SOURCE_COMMIT
    };
    if matches!(mutation, ProofMutation::Metadata) {
        metadata.author_name.push_str(" changed");
    }
    let object = MergeCommitObjectProof::try_new(
        GitCommitOid::from_str(expected_merge).unwrap(),
        GitTreeOid::from_str(tree).unwrap(),
        vec![
            GitCommitOid::from_str(target_parent).unwrap(),
            GitCommitOid::from_str(source_parent).unwrap(),
        ],
        metadata,
    )
    .unwrap();
    let target_branch = if matches!(mutation, ProofMutation::TargetBranch) {
        ALT_TARGET_BRANCH
    } else {
        TARGET_BRANCH
    };
    let target_head = if matches!(mutation, ProofMutation::TargetHead) {
        ALT_MERGE_COMMIT
    } else {
        MERGE_COMMIT
    };
    let source_branch = if matches!(mutation, ProofMutation::SourceBranch) {
        ALT_SOURCE_BRANCH
    } else {
        SOURCE_BRANCH
    };
    let source_oid = if matches!(mutation, ProofMutation::SourceOid) {
        ALT_MERGE_COMMIT
    } else {
        SOURCE_COMMIT
    };
    let common = if matches!(mutation, ProofMutation::CommonIdentity) {
        ALT_COMMON_IDENTITY
    } else {
        COMMON_IDENTITY
    };
    let admin = if matches!(mutation, ProofMutation::AdminIdentity) {
        ALT_ADMIN_IDENTITY
    } else {
        ADMIN_IDENTITY
    };
    let config = if matches!(mutation, ProofMutation::ConfigDigest) {
        ALT_CONFIG_DIGEST
    } else {
        CONFIG_DIGEST
    };
    MergeAppliedProof::try_new(
        object,
        GitBranchRef::from_str(target_branch).unwrap(),
        GitCommitOid::from_str(target_head).unwrap(),
        GitBranchRef::from_str(source_branch).unwrap(),
        GitCommitOid::from_str(source_oid).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", common).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", admin).unwrap(),
        "codex-reserved".to_owned(),
        Sha256Digest::from_str(config).unwrap(),
        GitTreeOid::from_str(tree).unwrap(),
        GitTreeOid::from_str(tree).unwrap(),
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

#[allow(clippy::too_many_arguments)]
fn raw_applied_proof(
    metadata: DeliveryCommitMetadata,
    index_tree: &str,
    worktree_tree: &str,
    counts: (u32, u32, u32, u32),
    merge_head: Option<&str>,
    fixed_lock_reason: &str,
    autostash: MergeAutostashObservation,
    other: OtherGitOperationObservation,
) -> Result<MergeAppliedProof, DeliveryError> {
    let object = MergeCommitObjectProof::try_new(
        GitCommitOid::from_str(MERGE_COMMIT).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
        vec![
            GitCommitOid::from_str(TARGET_HEAD).unwrap(),
            GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        ],
        metadata,
    )
    .unwrap();
    MergeAppliedProof::try_new(
        object,
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(MERGE_COMMIT).unwrap(),
        GitBranchRef::from_str(SOURCE_BRANCH).unwrap(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
        fixed_lock_reason.to_owned(),
        Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
        GitTreeOid::from_str(index_tree).unwrap(),
        GitTreeOid::from_str(worktree_tree).unwrap(),
        counts.0,
        counts.1,
        counts.2,
        counts.3,
        merge_head.map(|value| GitCommitOid::from_str(value).unwrap()),
        autostash,
        other,
    )
}

async fn merge_metadata(
    store: &Store,
    task_id: coding_agent_domain::TaskId,
    operation_id: coding_agent_store::DeliveryOperationId,
) -> DeliveryCommitMetadata {
    store
        .delivery_ownership_snapshot(task_id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap()
        .merge_metadata
        .unwrap()
}

async fn assert_merge_pending_without_disposition(
    store: &Store,
    task_id: coding_agent_domain::TaskId,
    operation_id: coding_agent_store::DeliveryOperationId,
) {
    let row: (String, i64, Option<String>) = sqlx::query_as(
        "SELECT state, version, merged_disposition_task_id \
         FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(row, ("merge_pending".to_owned(), 4, None));
    let dispositions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_artifact_dispositions WHERE task_id = ?")
            .bind(task_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(dispositions, 0);
    assert_eq!(merge_journal_count(store, operation_id).await, 4);
}

async fn merge_journal_count(
    store: &Store,
    operation_id: coding_agent_store::DeliveryOperationId,
) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap()
}

fn test_metadata() -> DeliveryCommitMetadata {
    DeliveryCommitMetadata {
        author_name: "Coding Agent".to_owned(),
        author_email: "coding-agent@localhost".to_owned(),
        committer_name: "Coding Agent".to_owned(),
        committer_email: "coding-agent@localhost".to_owned(),
        author_date_bytes: "1785801600 +0000".to_owned(),
        committer_date_bytes: "1785801600 +0000".to_owned(),
        message_template_version: 1,
        message_bytes: b"coding-agent test merge\n".to_vec(),
    }
}
