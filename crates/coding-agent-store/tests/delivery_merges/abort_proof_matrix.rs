use std::str::FromStr;

use coding_agent_store::{
    BeginMergeAbortRequest, CompleteMergeAbortRequest, DeliveryError, DirectoryIdentity,
    GitBranchRef, GitCommitOid, MergeAbortAppliedProof, MergeAbortProof, MergeAutostashObservation,
    MergeConflictPaths, MergeTransitionOutcome, OtherGitOperationObservation, Sha256Digest, Store,
};
use uuid::Uuid;

use crate::support::delivery::eligibility::{
    ADMIN_IDENTITY, COMMON_IDENTITY, CONFIG_DIGEST, SOURCE_COMMIT, TARGET_HEAD,
};

use super::fixtures::TARGET_BRANCH;

const SOURCE_BRANCH: &str = "refs/heads/codex/task-merge-store";
const ALT_TARGET_BRANCH: &str = "refs/heads/release";
const ALT_SOURCE_BRANCH: &str = "refs/heads/codex/other-source";
const ALT_OID: &str = "abababababababababababababababababababab";
const ALT_COMMON_IDENTITY: &str =
    "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1";
const ALT_ADMIN_IDENTITY: &str = "f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2";
const ALT_CONFIG_DIGEST: &str = "f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3";
const INDEX_STAGES: &str = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";
const ALT_INDEX_STAGES: &str = "a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2";
const WORKTREE: &str = "b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1";
const ALT_WORKTREE: &str = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2";

#[derive(Debug, Clone, Copy)]
enum BeginMutation {
    Exact,
    ChildReceipt,
    TargetBranch,
    TargetHead,
    SourceBranch,
    SourceOid,
    CommonIdentity,
    AdminIdentity,
    ConfigDigest,
    IndexStages,
    Worktree,
    ConflictPaths,
}

#[derive(Debug, Clone, Copy)]
enum CompleteMutation {
    Exact,
    TargetBranch,
    TargetHead,
    SourceBranch,
    SourceOid,
    CommonIdentity,
    AdminIdentity,
    ConfigDigest,
}

#[tokio::test]
async fn begin_abort_rejects_every_fresh_binding_mismatch_without_writes() {
    for mutation in [
        BeginMutation::TargetBranch,
        BeginMutation::TargetHead,
        BeginMutation::SourceBranch,
        BeginMutation::SourceOid,
        BeginMutation::CommonIdentity,
        BeginMutation::AdminIdentity,
        BeginMutation::ConfigDigest,
    ] {
        let (store, task, operation_id, pending_version) = super::fixtures::merge_pending().await;
        let request = BeginMergeAbortRequest::try_new(
            task.id,
            operation_id,
            pending_version,
            begin_proof(Uuid::new_v4(), mutation),
        )
        .unwrap();
        let outcome = store.begin_merge_abort(request).await.unwrap();
        assert!(
            matches!(outcome, MergeTransitionOutcome::Conflict),
            "{mutation:?} produced {outcome:?}"
        );
        assert_merge_pending_without_abort_facts(&store, operation_id).await;
    }
}

#[tokio::test]
async fn begin_abort_changed_proof_replay_is_conflict_and_preserves_the_sealed_facts() {
    let (store, task, operation_id, pending_version) = super::fixtures::merge_pending().await;
    let child = Uuid::new_v4();
    let exact = BeginMergeAbortRequest::try_new(
        task.id,
        operation_id,
        pending_version,
        begin_proof(child, BeginMutation::Exact),
    )
    .unwrap();
    assert!(matches!(
        store.begin_merge_abort(exact).await.unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));

    for mutation in [
        BeginMutation::ChildReceipt,
        BeginMutation::TargetBranch,
        BeginMutation::TargetHead,
        BeginMutation::SourceBranch,
        BeginMutation::SourceOid,
        BeginMutation::CommonIdentity,
        BeginMutation::AdminIdentity,
        BeginMutation::ConfigDigest,
        BeginMutation::IndexStages,
        BeginMutation::Worktree,
        BeginMutation::ConflictPaths,
    ] {
        let request = BeginMergeAbortRequest::try_new(
            task.id,
            operation_id,
            pending_version,
            begin_proof(child, mutation),
        )
        .unwrap();
        let outcome = store.begin_merge_abort(request).await.unwrap();
        assert!(
            matches!(outcome, MergeTransitionOutcome::Conflict),
            "{mutation:?} produced {outcome:?}"
        );
    }
    assert_abort_pending_facts(&store, operation_id, child).await;
}

#[tokio::test]
async fn complete_abort_rejects_every_fresh_binding_mismatch_without_writes() {
    for mutation in [
        CompleteMutation::TargetBranch,
        CompleteMutation::TargetHead,
        CompleteMutation::SourceBranch,
        CompleteMutation::SourceOid,
        CompleteMutation::CommonIdentity,
        CompleteMutation::AdminIdentity,
        CompleteMutation::ConfigDigest,
    ] {
        let (store, task, operation_id, abort_version) =
            super::abort::abort_pending_fixture().await;
        let request = CompleteMergeAbortRequest::try_new(
            task.id,
            operation_id,
            abort_version,
            complete_proof(mutation),
        )
        .unwrap();
        let outcome = store.complete_merge_abort(request).await.unwrap();
        assert!(
            matches!(outcome, MergeTransitionOutcome::Conflict),
            "{mutation:?} produced {outcome:?}"
        );
        assert_abort_pending_with_conflict_facts(&store, operation_id, "src/conflicted.rs").await;
    }
}

#[tokio::test]
async fn complete_abort_reuses_durable_paths_across_reply_lost_replay() {
    let (store, task, operation_id, abort_version) =
        super::abort::abort_pending_fixture_with_paths(vec![b"src/first.rs".to_vec()]).await;
    let first = CompleteMergeAbortRequest::try_new(
        task.id,
        operation_id,
        abort_version,
        complete_proof(CompleteMutation::Exact),
    )
    .unwrap();
    assert!(matches!(
        store.complete_merge_abort(first).await.unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));
    let replay = CompleteMergeAbortRequest::try_new(
        task.id,
        operation_id,
        abort_version,
        complete_proof(CompleteMutation::Exact),
    )
    .unwrap();
    assert!(matches!(
        store.complete_merge_abort(replay).await.unwrap(),
        MergeTransitionOutcome::Existing(_)
    ));

    let row: (String, i64, i64) = sqlx::query_as(
        "SELECT state, version, conflict_path_count FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(row, ("conflict".to_owned(), 7, 1));
    let path: (String, String) = sqlx::query_as(
        "SELECT path_encoding, path_value FROM task_merge_conflicts WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(path, ("utf8".to_owned(), "src/first.rs".to_owned()));
    assert_eq!(merge_journal_count(&store, operation_id).await, 7);
}

#[tokio::test]
async fn complete_abort_changed_binding_replay_is_conflict_and_preserves_the_sealed_set() {
    let (store, task, operation_id, abort_version) =
        super::abort::abort_pending_fixture_with_paths(vec![b"src/sealed.rs".to_vec()]).await;
    let exact = CompleteMergeAbortRequest::try_new(
        task.id,
        operation_id,
        abort_version,
        complete_proof(CompleteMutation::Exact),
    )
    .unwrap();
    assert!(matches!(
        store.complete_merge_abort(exact).await.unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));

    for mutation in [
        CompleteMutation::TargetBranch,
        CompleteMutation::TargetHead,
        CompleteMutation::SourceBranch,
        CompleteMutation::SourceOid,
        CompleteMutation::CommonIdentity,
        CompleteMutation::AdminIdentity,
        CompleteMutation::ConfigDigest,
    ] {
        let changed = CompleteMergeAbortRequest::try_new(
            task.id,
            operation_id,
            abort_version,
            complete_proof(mutation),
        )
        .unwrap();
        let outcome = store.complete_merge_abort(changed).await.unwrap();
        assert!(
            matches!(outcome, MergeTransitionOutcome::Conflict),
            "{mutation:?} produced {outcome:?}"
        );
    }

    let row: (String, i64, i64) = sqlx::query_as(
        "SELECT state, version, conflict_path_count FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(row, ("conflict".to_owned(), 7, 1));
    let path: String =
        sqlx::query_scalar("SELECT path_value FROM task_merge_conflicts WHERE operation_id = ?")
            .bind(operation_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(path, "src/sealed.rs");
    assert_eq!(merge_journal_count(&store, operation_id).await, 7);
}

#[test]
fn abort_proof_constructors_reject_every_unobservable_or_nonzero_postcondition() {
    assert!(
        begin_proof_result(
            Uuid::nil(),
            "codex-reserved",
            MergeAutostashObservation::Absent,
            OtherGitOperationObservation::Clear,
            SOURCE_COMMIT,
            SOURCE_COMMIT
        )
        .is_err()
    );
    assert!(
        begin_proof_result(
            Uuid::new_v4(),
            "other-lock",
            MergeAutostashObservation::Absent,
            OtherGitOperationObservation::Clear,
            SOURCE_COMMIT,
            SOURCE_COMMIT
        )
        .is_err()
    );
    assert!(
        begin_proof_result(
            Uuid::new_v4(),
            "codex-reserved",
            MergeAutostashObservation::Present,
            OtherGitOperationObservation::Clear,
            SOURCE_COMMIT,
            SOURCE_COMMIT
        )
        .is_err()
    );
    assert!(
        begin_proof_result(
            Uuid::new_v4(),
            "codex-reserved",
            MergeAutostashObservation::Unobservable,
            OtherGitOperationObservation::Clear,
            SOURCE_COMMIT,
            SOURCE_COMMIT
        )
        .is_err()
    );
    assert!(
        begin_proof_result(
            Uuid::new_v4(),
            "codex-reserved",
            MergeAutostashObservation::Absent,
            OtherGitOperationObservation::Present,
            SOURCE_COMMIT,
            SOURCE_COMMIT
        )
        .is_err()
    );
    assert!(
        begin_proof_result(
            Uuid::new_v4(),
            "codex-reserved",
            MergeAutostashObservation::Absent,
            OtherGitOperationObservation::Unobservable,
            SOURCE_COMMIT,
            SOURCE_COMMIT
        )
        .is_err()
    );
    assert!(
        begin_proof_result(
            Uuid::new_v4(),
            "codex-reserved",
            MergeAutostashObservation::Absent,
            OtherGitOperationObservation::Clear,
            SOURCE_COMMIT,
            ALT_OID
        )
        .is_err()
    );

    for counts in [(1, 0, 0, 0), (0, 1, 0, 0), (0, 0, 1, 0), (0, 0, 0, 1)] {
        assert!(
            complete_proof_result(
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
        complete_proof_result(
            (0, 0, 0, 0),
            Some(SOURCE_COMMIT),
            "codex-reserved",
            MergeAutostashObservation::Absent,
            OtherGitOperationObservation::Clear
        )
        .is_err()
    );
    assert!(
        complete_proof_result(
            (0, 0, 0, 0),
            None,
            "other-lock",
            MergeAutostashObservation::Absent,
            OtherGitOperationObservation::Clear
        )
        .is_err()
    );
    assert!(
        complete_proof_result(
            (0, 0, 0, 0),
            None,
            "codex-reserved",
            MergeAutostashObservation::Present,
            OtherGitOperationObservation::Clear
        )
        .is_err()
    );
    assert!(
        complete_proof_result(
            (0, 0, 0, 0),
            None,
            "codex-reserved",
            MergeAutostashObservation::Unobservable,
            OtherGitOperationObservation::Clear
        )
        .is_err()
    );
    assert!(
        complete_proof_result(
            (0, 0, 0, 0),
            None,
            "codex-reserved",
            MergeAutostashObservation::Absent,
            OtherGitOperationObservation::Present
        )
        .is_err()
    );
    assert!(
        complete_proof_result(
            (0, 0, 0, 0),
            None,
            "codex-reserved",
            MergeAutostashObservation::Absent,
            OtherGitOperationObservation::Unobservable
        )
        .is_err()
    );
}

fn begin_proof(child: Uuid, mutation: BeginMutation) -> MergeAbortProof {
    let child = if matches!(mutation, BeginMutation::ChildReceipt) {
        Uuid::new_v4()
    } else {
        child
    };
    let target_branch = if matches!(mutation, BeginMutation::TargetBranch) {
        ALT_TARGET_BRANCH
    } else {
        TARGET_BRANCH
    };
    let target_head = if matches!(mutation, BeginMutation::TargetHead) {
        ALT_OID
    } else {
        TARGET_HEAD
    };
    let source_branch = if matches!(mutation, BeginMutation::SourceBranch) {
        ALT_SOURCE_BRANCH
    } else {
        SOURCE_BRANCH
    };
    let source_oid = if matches!(mutation, BeginMutation::SourceOid) {
        ALT_OID
    } else {
        SOURCE_COMMIT
    };
    let common = if matches!(mutation, BeginMutation::CommonIdentity) {
        ALT_COMMON_IDENTITY
    } else {
        COMMON_IDENTITY
    };
    let admin = if matches!(mutation, BeginMutation::AdminIdentity) {
        ALT_ADMIN_IDENTITY
    } else {
        ADMIN_IDENTITY
    };
    let config = if matches!(mutation, BeginMutation::ConfigDigest) {
        ALT_CONFIG_DIGEST
    } else {
        CONFIG_DIGEST
    };
    let index = if matches!(mutation, BeginMutation::IndexStages) {
        ALT_INDEX_STAGES
    } else {
        INDEX_STAGES
    };
    let worktree = if matches!(mutation, BeginMutation::Worktree) {
        ALT_WORKTREE
    } else {
        WORKTREE
    };
    let conflict_paths = if matches!(mutation, BeginMutation::ConflictPaths) {
        vec![b"src/changed.rs".to_vec()]
    } else {
        vec![b"src/conflicted.rs".to_vec()]
    };
    MergeAbortProof::try_new(
        child,
        GitBranchRef::from_str(target_branch).unwrap(),
        GitCommitOid::from_str(target_head).unwrap(),
        GitBranchRef::from_str(source_branch).unwrap(),
        GitCommitOid::from_str(source_oid).unwrap(),
        GitCommitOid::from_str(source_oid).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", common).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", admin).unwrap(),
        "codex-reserved".to_owned(),
        Sha256Digest::from_str(config).unwrap(),
        Sha256Digest::from_str(index).unwrap(),
        Sha256Digest::from_str(worktree).unwrap(),
        MergeAutostashObservation::Absent,
        OtherGitOperationObservation::Clear,
        MergeConflictPaths::try_from_raw(conflict_paths).unwrap(),
    )
    .unwrap()
}

fn complete_proof(mutation: CompleteMutation) -> MergeAbortAppliedProof {
    let target_branch = if matches!(mutation, CompleteMutation::TargetBranch) {
        ALT_TARGET_BRANCH
    } else {
        TARGET_BRANCH
    };
    let target_head = if matches!(mutation, CompleteMutation::TargetHead) {
        ALT_OID
    } else {
        TARGET_HEAD
    };
    let source_branch = if matches!(mutation, CompleteMutation::SourceBranch) {
        ALT_SOURCE_BRANCH
    } else {
        SOURCE_BRANCH
    };
    let source_oid = if matches!(mutation, CompleteMutation::SourceOid) {
        ALT_OID
    } else {
        SOURCE_COMMIT
    };
    let common = if matches!(mutation, CompleteMutation::CommonIdentity) {
        ALT_COMMON_IDENTITY
    } else {
        COMMON_IDENTITY
    };
    let admin = if matches!(mutation, CompleteMutation::AdminIdentity) {
        ALT_ADMIN_IDENTITY
    } else {
        ADMIN_IDENTITY
    };
    let config = if matches!(mutation, CompleteMutation::ConfigDigest) {
        ALT_CONFIG_DIGEST
    } else {
        CONFIG_DIGEST
    };
    MergeAbortAppliedProof::try_new(
        GitBranchRef::from_str(target_branch).unwrap(),
        GitCommitOid::from_str(target_head).unwrap(),
        GitBranchRef::from_str(source_branch).unwrap(),
        GitCommitOid::from_str(source_oid).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", common).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", admin).unwrap(),
        "codex-reserved".to_owned(),
        Sha256Digest::from_str(config).unwrap(),
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

fn begin_proof_result(
    child: Uuid,
    fixed_lock_reason: &str,
    autostash: MergeAutostashObservation,
    other: OtherGitOperationObservation,
    source_oid: &str,
    merge_head: &str,
) -> Result<MergeAbortProof, DeliveryError> {
    MergeAbortProof::try_new(
        child,
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
        GitBranchRef::from_str(SOURCE_BRANCH).unwrap(),
        GitCommitOid::from_str(source_oid).unwrap(),
        GitCommitOid::from_str(merge_head).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
        fixed_lock_reason.to_owned(),
        Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
        Sha256Digest::from_str(INDEX_STAGES).unwrap(),
        Sha256Digest::from_str(WORKTREE).unwrap(),
        autostash,
        other,
        MergeConflictPaths::try_from_raw(vec![b"src/conflicted.rs".to_vec()]).unwrap(),
    )
}

fn complete_proof_result(
    counts: (u32, u32, u32, u32),
    merge_head: Option<&str>,
    fixed_lock_reason: &str,
    autostash: MergeAutostashObservation,
    other: OtherGitOperationObservation,
) -> Result<MergeAbortAppliedProof, DeliveryError> {
    MergeAbortAppliedProof::try_new(
        GitBranchRef::from_str(TARGET_BRANCH).unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
        GitBranchRef::from_str(SOURCE_BRANCH).unwrap(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
        fixed_lock_reason.to_owned(),
        Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
        counts.0,
        counts.1,
        counts.2,
        counts.3,
        merge_head.map(|value| GitCommitOid::from_str(value).unwrap()),
        autostash,
        other,
    )
}

async fn assert_merge_pending_without_abort_facts(
    store: &Store,
    operation_id: coding_agent_store::DeliveryOperationId,
) {
    type OptionalAbortFactsRow = (
        String,
        i64,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
    );
    let row: OptionalAbortFactsRow = sqlx::query_as(
        "SELECT state, version, abort_child_receipt_id, abort_merge_head_oid, \
                    abort_index_stages_digest, abort_worktree_digest, conflict_path_count \
             FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(
        row,
        ("merge_pending".to_owned(), 5, None, None, None, None, None)
    );
    let conflict_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_merge_conflicts WHERE operation_id = ?")
            .bind(operation_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(conflict_count, 0);
    assert_eq!(merge_journal_count(store, operation_id).await, 5);
}

async fn assert_abort_pending_facts(
    store: &Store,
    operation_id: coding_agent_store::DeliveryOperationId,
    child: Uuid,
) {
    let row: (String, i64, String, String, String, String, i64) = sqlx::query_as(
        "SELECT state, version, abort_child_receipt_id, abort_merge_head_oid, \
                abort_index_stages_digest, abort_worktree_digest, conflict_path_count \
         FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(
        row,
        (
            "abort_pending".to_owned(),
            6,
            child.to_string(),
            SOURCE_COMMIT.to_owned(),
            INDEX_STAGES.to_owned(),
            WORKTREE.to_owned(),
            1,
        )
    );
    let path: String =
        sqlx::query_scalar("SELECT path_value FROM task_merge_conflicts WHERE operation_id = ?")
            .bind(operation_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(path, "src/conflicted.rs");
    assert_eq!(merge_journal_count(store, operation_id).await, 6);
}

async fn assert_abort_pending_with_conflict_facts(
    store: &Store,
    operation_id: coding_agent_store::DeliveryOperationId,
    expected_path: &str,
) {
    let row: (String, i64, i64, Option<String>) = sqlx::query_as(
        "SELECT state, version, conflict_path_count, failure_code \
         FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(row, ("abort_pending".to_owned(), 6, 1, None));
    let path: String =
        sqlx::query_scalar("SELECT path_value FROM task_merge_conflicts WHERE operation_id = ?")
            .bind(operation_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(path, expected_path);
    assert_eq!(merge_journal_count(store, operation_id).await, 6);
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
