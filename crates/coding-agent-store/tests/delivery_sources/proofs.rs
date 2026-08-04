use std::str::FromStr;

use coding_agent_store::{
    AdvanceDeliverySourceObjectRequest, CommitDeliverySourceRequest, DeliveryCommitMetadata,
    DeliverySourceAppliedProof, DeliverySourceObjectProof, DeliverySourceTransitionOutcome,
    DirectoryIdentity, GitBranchRef, GitCommitOid, GitTreeOid, Sha256Digest, SourceWorktreeProof,
};

use super::fixtures::{
    accepted_fixture, commit_pending_fixture, created_source, source_anchor, source_journal_count,
};

const ALT_COMMIT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ALT_PARENT: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const ALT_TREE: &str = "cccccccccccccccccccccccccccccccccccccccc";
const SHA256_COMMIT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SHA256_TREE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const SHA256_PARENT: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const ALT_DIRECTORY: &str = "abababababababababababababababababababababababababababababababab";
const ALT_DIGEST: &str = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

#[derive(Debug, Clone, Copy)]
enum ObjectMismatch {
    Tree,
    Parent,
    Algorithm,
    AuthorName,
    AuthorEmail,
    CommitterName,
    CommitterEmail,
    AuthorDate,
    CommitterDate,
    TemplateVersion,
    Message,
}

impl ObjectMismatch {
    const ALL: [Self; 11] = [
        Self::Tree,
        Self::Parent,
        Self::Algorithm,
        Self::AuthorName,
        Self::AuthorEmail,
        Self::CommitterName,
        Self::CommitterEmail,
        Self::AuthorDate,
        Self::CommitterDate,
        Self::TemplateVersion,
        Self::Message,
    ];
}

#[tokio::test]
async fn replayed_object_transition_rejects_a_different_discovered_commit() {
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
    let anchor = source_anchor(&command);
    let exact = super::fixtures::object_proof(&source);
    let request =
        AdvanceDeliverySourceObjectRequest::try_new(anchor, source.version, exact).unwrap();
    assert!(matches!(
        store.advance_delivery_source_object(request).await.unwrap(),
        DeliverySourceTransitionOutcome::Applied(_)
    ));
    let changed_commit = DeliverySourceObjectProof::try_new(
        GitCommitOid::from_str(ALT_COMMIT).unwrap(),
        source.candidate_tree.clone(),
        vec![source.expected_parent.clone()],
        source.commit_metadata.clone(),
    )
    .unwrap();
    let replay =
        AdvanceDeliverySourceObjectRequest::try_new(anchor, source.version, changed_commit)
            .unwrap();
    assert!(matches!(
        store.advance_delivery_source_object(replay).await.unwrap(),
        DeliverySourceTransitionOutcome::Conflict
    ));
    assert_eq!(source_journal_count(&store, command.task_id()).await, 2);
}

#[tokio::test]
async fn object_proof_mismatch_matrix_is_conflict_without_writes() {
    let (store, command) = accepted_fixture().await;
    let source = created_source(&store, command.clone()).await;
    let anchor = source_anchor(&command);
    for mismatch in ObjectMismatch::ALL {
        let proof = mismatched_object_proof(&source, mismatch);
        let outcome = store
            .advance_delivery_source_object(
                AdvanceDeliverySourceObjectRequest::try_new(anchor, source.version, proof).unwrap(),
            )
            .await
            .unwrap();
        assert!(
            matches!(outcome, DeliverySourceTransitionOutcome::Conflict),
            "{mismatch:?} was accepted"
        );
        assert_eq!(source_journal_count(&store, command.task_id()).await, 1);
    }
}

#[test]
fn object_proof_constructor_rejects_parent_cardinality_and_mixed_algorithms() {
    let metadata = sample_metadata();
    let commit = GitCommitOid::from_str(ALT_COMMIT).unwrap();
    let tree = GitTreeOid::from_str(ALT_TREE).unwrap();
    let parent = GitCommitOid::from_str(ALT_PARENT).unwrap();
    assert!(
        DeliverySourceObjectProof::try_new(
            commit.clone(),
            tree.clone(),
            Vec::new(),
            metadata.clone(),
        )
        .is_err()
    );
    assert!(
        DeliverySourceObjectProof::try_new(
            commit.clone(),
            tree,
            vec![parent.clone(), parent],
            metadata.clone(),
        )
        .is_err()
    );
    assert!(
        DeliverySourceObjectProof::try_new(
            commit,
            GitTreeOid::from_str(SHA256_TREE).unwrap(),
            vec![GitCommitOid::from_str(SHA256_PARENT).unwrap()],
            metadata,
        )
        .is_err()
    );
}

#[derive(Debug, Clone, Copy)]
enum AppliedMismatch {
    Object,
    SymbolicRef,
    SourceRefOid,
    HeadOid,
    IndexTree,
    WorktreeTree,
    StagedCount,
    UnstagedCount,
    UntrackedCount,
    UnmergedCount,
    CommonIdentity,
    AdminIdentity,
    FixedLock,
    ConfigDigest,
}

impl AppliedMismatch {
    const ALL: [Self; 14] = [
        Self::Object,
        Self::SymbolicRef,
        Self::SourceRefOid,
        Self::HeadOid,
        Self::IndexTree,
        Self::WorktreeTree,
        Self::StagedCount,
        Self::UnstagedCount,
        Self::UntrackedCount,
        Self::UnmergedCount,
        Self::CommonIdentity,
        Self::AdminIdentity,
        Self::FixedLock,
        Self::ConfigDigest,
    ];
}

#[tokio::test]
async fn applied_source_proof_mismatch_matrix_is_conflict_without_writes() {
    let (store, command) = accepted_fixture().await;
    let (source, anchor, object, _) = commit_pending_fixture(&store, &command).await;
    for mismatch in AppliedMismatch::ALL {
        let proof = mismatched_applied_proof(&source, &object, mismatch);
        let outcome = store
            .commit_delivery_source(
                CommitDeliverySourceRequest::try_new(anchor, source.version, proof).unwrap(),
            )
            .await
            .unwrap();
        assert!(
            matches!(outcome, DeliverySourceTransitionOutcome::Conflict),
            "{mismatch:?} was accepted"
        );
        assert_eq!(source_journal_count(&store, command.task_id()).await, 2);
    }
}

fn mismatched_object_proof(
    source: &coding_agent_store::DeliverySourceRecord,
    mismatch: ObjectMismatch,
) -> DeliverySourceObjectProof {
    let mut commit =
        GitCommitOid::from_str(crate::support::delivery::eligibility::SOURCE_COMMIT).unwrap();
    let mut tree = source.candidate_tree.clone();
    let mut parent = source.expected_parent.clone();
    let mut metadata = source.commit_metadata.clone();
    match mismatch {
        ObjectMismatch::Tree => {
            tree = GitTreeOid::from_str(ALT_TREE).unwrap();
        }
        ObjectMismatch::Parent => {
            parent = GitCommitOid::from_str(ALT_PARENT).unwrap();
        }
        ObjectMismatch::Algorithm => {
            commit = GitCommitOid::from_str(SHA256_COMMIT).unwrap();
            tree = GitTreeOid::from_str(SHA256_TREE).unwrap();
            parent = GitCommitOid::from_str(SHA256_PARENT).unwrap();
        }
        ObjectMismatch::AuthorName => metadata.author_name = "Different Author".to_owned(),
        ObjectMismatch::AuthorEmail => {
            metadata.author_email = "different-author@example.com".to_owned()
        }
        ObjectMismatch::CommitterName => metadata.committer_name = "Different Committer".to_owned(),
        ObjectMismatch::CommitterEmail => {
            metadata.committer_email = "different-committer@example.com".to_owned()
        }
        ObjectMismatch::AuthorDate => metadata.author_date_bytes = "1 +0000".to_owned(),
        ObjectMismatch::CommitterDate => metadata.committer_date_bytes = "1 +0000".to_owned(),
        ObjectMismatch::TemplateVersion => metadata.message_template_version = 2,
        ObjectMismatch::Message => {
            metadata.message_bytes = b"different source commit message\n".to_vec()
        }
    }
    DeliverySourceObjectProof::try_new(commit, tree, vec![parent], metadata).unwrap()
}

fn mismatched_applied_proof(
    source: &coding_agent_store::DeliverySourceRecord,
    exact_object: &DeliverySourceObjectProof,
    mismatch: AppliedMismatch,
) -> DeliverySourceAppliedProof {
    let mut object = exact_object.clone();
    let mut source_ref = source.provenance.source_branch.clone();
    let expected_commit = source.expected_source_commit.clone().unwrap();
    let mut ref_oid = expected_commit.clone();
    let mut head_oid = expected_commit;
    let mut index_tree = source.candidate_tree.clone();
    let mut worktree_tree = source.candidate_tree.clone();
    let (mut staged, mut unstaged, mut untracked, mut unmerged) = (0, 0, 0, 0);
    let mut common = source.provenance.common_git_identity.clone();
    let mut admin = source.provenance.worktree_admin_identity.clone();
    let mut lock = source.provenance.fixed_lock_reason.clone();
    let mut config = source.provenance.config_attributes_digest.clone();
    match mismatch {
        AppliedMismatch::Object => {
            object = DeliverySourceObjectProof::try_new(
                GitCommitOid::from_str(ALT_COMMIT).unwrap(),
                source.candidate_tree.clone(),
                vec![source.expected_parent.clone()],
                source.commit_metadata.clone(),
            )
            .unwrap();
        }
        AppliedMismatch::SymbolicRef => {
            source_ref = GitBranchRef::from_str("refs/heads/not-the-source").unwrap();
        }
        AppliedMismatch::SourceRefOid => ref_oid = GitCommitOid::from_str(ALT_COMMIT).unwrap(),
        AppliedMismatch::HeadOid => head_oid = GitCommitOid::from_str(ALT_COMMIT).unwrap(),
        AppliedMismatch::IndexTree => index_tree = GitTreeOid::from_str(ALT_TREE).unwrap(),
        AppliedMismatch::WorktreeTree => worktree_tree = GitTreeOid::from_str(ALT_TREE).unwrap(),
        AppliedMismatch::StagedCount => staged = 1,
        AppliedMismatch::UnstagedCount => unstaged = 1,
        AppliedMismatch::UntrackedCount => untracked = 1,
        AppliedMismatch::UnmergedCount => unmerged = 1,
        AppliedMismatch::CommonIdentity => {
            common = DirectoryIdentity::try_new("directory_identity_v1", ALT_DIRECTORY).unwrap()
        }
        AppliedMismatch::AdminIdentity => {
            admin = DirectoryIdentity::try_new("directory_identity_v1", ALT_DIRECTORY).unwrap()
        }
        AppliedMismatch::FixedLock => lock = "wrong-lock-reason".to_owned(),
        AppliedMismatch::ConfigDigest => config = Sha256Digest::from_str(ALT_DIGEST).unwrap(),
    }
    let worktree = SourceWorktreeProof::try_new(
        index_tree,
        worktree_tree,
        staged,
        unstaged,
        untracked,
        unmerged,
    )
    .unwrap();
    DeliverySourceAppliedProof::try_new(
        object, source_ref, ref_oid, head_oid, worktree, common, admin, lock, config,
    )
    .unwrap()
}

fn sample_metadata() -> DeliveryCommitMetadata {
    DeliveryCommitMetadata {
        author_name: "Coding Agent".to_owned(),
        author_email: "coding-agent@localhost".to_owned(),
        committer_name: "Coding Agent".to_owned(),
        committer_email: "coding-agent@localhost".to_owned(),
        author_date_bytes: "1785801600 +0000".to_owned(),
        committer_date_bytes: "1785801600 +0000".to_owned(),
        message_template_version: 1,
        message_bytes: b"coding-agent: deliver task sample attempt 1\n".to_vec(),
    }
}
