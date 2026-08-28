mod delivery_source_support;

use std::collections::BTreeSet;
#[cfg(feature = "test-support")]
use std::sync::{Arc, Mutex};

use coding_agent_runtime::{
    DeliverySourceCommitInput, DeliverySourcePendingState, DeliverySourceRecoveryDisposition,
    DeliverySourceRecoveryIntent,
};
use delivery_source_support::{Fixture, git_line, git_ok};
#[cfg(feature = "test-support")]
use delivery_source_support::{RepositorySnapshot, ReviewedDirtySource, snapshot_paths};
use tokio_util::sync::CancellationToken;

/// Durable CommitPending input prepared on the normal pre-stage path. Tests
/// deliberately drop that capability before opening the recovery capability.
#[cfg(feature = "test-support")]
struct PreparedCommitPending {
    fixture: Fixture,
    source: ReviewedDirtySource,
    intent: DeliverySourceRecoveryIntent,
    expected_object_id: String,
}

#[cfg(feature = "test-support")]
impl PreparedCommitPending {
    async fn new(name: &str, task_id: &str) -> Self {
        let fixture = Fixture::new(name).await;
        let source = fixture.reviewed_dirty_source(task_id).await;
        let provisioner = fixture.delivery_source(&source.worktrees).unwrap();
        let opened = provisioner
            .open_delivery_source(
                &source.reservation,
                source.approved_fingerprint,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let candidate = provisioner
            .build_candidate_tree(&opened, CancellationToken::new())
            .await
            .unwrap();
        let input = DeliverySourceCommitInput::try_new(task_id, 1, 1_700_000_000).unwrap();
        let expected = provisioner
            .build_source_commit(&opened, &candidate, &input, CancellationToken::new())
            .await
            .unwrap();
        let intent = DeliverySourceRecoveryIntent::from_source(
            DeliverySourcePendingState::CommitPending,
            &opened,
            &candidate,
            Some(&expected),
            input,
        )
        .unwrap();
        let expected_object_id = expected.object_id().to_owned();

        // Recovery must be reopened from the captured opaque intent, never from the
        // pre-crash capability that built the candidate and object.
        drop(opened);
        drop(provisioner);

        Self {
            fixture,
            source,
            intent,
            expected_object_id,
        }
    }

    async fn open_recovery(
        &self,
    ) -> (
        coding_agent_runtime::DeliverySourceProvisioner,
        coding_agent_runtime::DeliverySourceRecoveryCapability,
    ) {
        let provisioner = self
            .fixture
            .delivery_source(&self.source.worktrees)
            .unwrap();
        let recovery = provisioner
            .open_delivery_source_for_recovery(
                &self.source.reservation,
                &self.intent,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        (provisioner, recovery)
    }

    fn snapshot(&self) -> RepositorySnapshot {
        self.source.snapshot(&self.fixture.repository)
    }

    fn source_ref(&self) -> String {
        format!("refs/heads/{}", self.source.reservation.branch_name())
    }
}

#[tokio::test]
async fn candidate_tree_contains_exact_reviewed_changes_without_mutating_the_real_index_or_refs() {
    let fixture = Fixture::new("candidate-tree-exact").await;
    std::fs::write(
        fixture.repository.join(".gitignore"),
        b"ignored-review-output/\n",
    )
    .unwrap();
    git_ok(&fixture.repository, &["add", "--", ".gitignore"]);
    git_ok(
        &fixture.repository,
        &[
            "commit",
            "--quiet",
            "--no-gpg-sign",
            "-m",
            "ignore local review output",
        ],
    );

    let source = fixture.reviewed_dirty_source("candidate-tree-exact").await;
    let ignored = source.worktree_path().join("ignored-review-output");
    std::fs::create_dir_all(&ignored).unwrap();
    std::fs::write(
        ignored.join("result.txt"),
        b"must not enter the candidate tree\n",
    )
    .unwrap();

    let provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let opened = provisioner
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let before = source.snapshot(&fixture.repository);

    let candidate = provisioner
        .build_candidate_tree(&opened, CancellationToken::new())
        .await
        .unwrap_or_else(|error| panic!("candidate tree failed with {}", error.code()));

    let tree_paths = git_line(
        &fixture.repository,
        &["ls-tree", "--name-only", "-r", candidate.object_id()],
    )
    .lines()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    let expected_paths = BTreeSet::from([
        ".gitignore".to_owned(),
        "nested/rust/Cargo.lock".to_owned(),
        "nested/rust/Cargo.toml".to_owned(),
        "nested/rust/src/lib.rs".to_owned(),
        "review-note.txt".to_owned(),
        "sequence.txt".to_owned(),
        "tracked.txt".to_owned(),
    ]);
    assert_eq!(tree_paths, expected_paths);
    assert!(!tree_paths.contains("ignored-review-output/result.txt"));

    let tracked_spec = format!("{}:tracked.txt", candidate.object_id());
    assert_eq!(
        git_line(&fixture.repository, &["show", &tracked_spec]),
        "tracked approved change"
    );
    let untracked_spec = format!("{}:review-note.txt", candidate.object_id());
    assert_eq!(
        git_line(&fixture.repository, &["show", &untracked_spec]),
        "approved untracked change"
    );
    assert_eq!(source.snapshot(&fixture.repository), before);
}

#[tokio::test]
async fn candidate_tree_applies_a_staged_delete_without_mutating_the_real_source_state() {
    let fixture = Fixture::new("candidate-tree-staged-delete").await;
    let source = fixture
        .reviewed_dirty_source("candidate-tree-staged-delete")
        .await;
    git_ok(
        source.worktree_path(),
        &["rm", "--force", "--", "tracked.txt"],
    );
    let approved_fingerprint = fixture.current_fingerprint(&source).await;
    assert_ne!(approved_fingerprint, source.approved_fingerprint);

    let provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let opened = provisioner
        .open_delivery_source(
            &source.reservation,
            approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let before = source.snapshot(&fixture.repository);

    let candidate = provisioner
        .build_candidate_tree(&opened, CancellationToken::new())
        .await
        .unwrap_or_else(|error| panic!("candidate tree failed with {}", error.code()));

    let tree_paths = git_line(
        &fixture.repository,
        &["ls-tree", "--name-only", "-r", candidate.object_id()],
    )
    .lines()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    assert!(!tree_paths.contains("tracked.txt"));
    assert_eq!(source.snapshot(&fixture.repository), before);
}

#[tokio::test]
async fn candidate_tree_applies_a_staged_rename_as_delete_and_add_without_mutating_real_state() {
    let fixture = Fixture::new("candidate-tree-staged-rename").await;
    let source = fixture
        .reviewed_dirty_source("candidate-tree-staged-rename")
        .await;
    git_ok(
        source.worktree_path(),
        &["mv", "--force", "tracked.txt", "renamed-tracked.txt"],
    );
    let approved_fingerprint = fixture.current_fingerprint(&source).await;
    assert_ne!(approved_fingerprint, source.approved_fingerprint);

    let provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let opened = provisioner
        .open_delivery_source(
            &source.reservation,
            approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let before = source.snapshot(&fixture.repository);

    let candidate = provisioner
        .build_candidate_tree(&opened, CancellationToken::new())
        .await
        .unwrap_or_else(|error| panic!("candidate tree failed with {}", error.code()));

    let tree_paths = git_line(
        &fixture.repository,
        &["ls-tree", "--name-only", "-r", candidate.object_id()],
    )
    .lines()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    assert!(!tree_paths.contains("tracked.txt"));
    assert!(tree_paths.contains("renamed-tracked.txt"));
    let renamed_spec = format!("{}:renamed-tracked.txt", candidate.object_id());
    assert_eq!(
        git_line(&fixture.repository, &["show", &renamed_spec]),
        "tracked approved change"
    );
    assert_eq!(source.snapshot(&fixture.repository), before);
}

#[tokio::test]
async fn candidate_tree_preserves_a_visible_untracked_readd_after_staged_delete() {
    let fixture = Fixture::new("candidate-tree-staged-delete-visible-readd").await;
    let source = fixture
        .reviewed_dirty_source("candidate-tree-staged-delete-visible-readd")
        .await;
    git_ok(
        source.worktree_path(),
        &["rm", "--cached", "--force", "--", "tracked.txt"],
    );
    std::fs::write(
        source.worktree_path().join("tracked.txt"),
        b"visible untracked replacement\n",
    )
    .unwrap();
    let approved_fingerprint = fixture.current_fingerprint(&source).await;
    assert_ne!(approved_fingerprint, source.approved_fingerprint);

    let provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let opened = provisioner
        .open_delivery_source(
            &source.reservation,
            approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let before = source.snapshot(&fixture.repository);

    let candidate = provisioner
        .build_candidate_tree(&opened, CancellationToken::new())
        .await
        .unwrap_or_else(|error| panic!("candidate tree failed with {}", error.code()));

    let tracked_spec = format!("{}:tracked.txt", candidate.object_id());
    assert_eq!(
        git_line(&fixture.repository, &["show", &tracked_spec]),
        "visible untracked replacement"
    );
    assert_eq!(source.snapshot(&fixture.repository), before);
}

#[tokio::test]
async fn deterministic_source_commit_is_exact_and_leaves_real_state_unchanged() {
    let fixture = Fixture::new("deterministic-source-commit").await;
    let source = fixture
        .reviewed_dirty_source("123e4567-e89b-12d3-a456-426614174000")
        .await;
    let provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let opened = provisioner
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let before = source.snapshot(&fixture.repository);
    let candidate = provisioner
        .build_candidate_tree(&opened, CancellationToken::new())
        .await
        .unwrap_or_else(|error| panic!("candidate tree failed with {}", error.code()));
    let metadata = DeliverySourceCommitInput::try_new(
        "123e4567-e89b-12d3-a456-426614174000",
        1,
        1_700_000_000,
    )
    .unwrap();

    let mismatched_metadata = DeliverySourceCommitInput::try_new(
        "123e4567-e89b-12d3-a456-426614174001",
        1,
        1_700_000_000,
    )
    .unwrap();
    let mismatch = provisioner
        .build_source_commit(
            &opened,
            &candidate,
            &mismatched_metadata,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(mismatch.code(), "DELIVERY_SOURCE_CHANGED");
    assert_eq!(source.snapshot(&fixture.repository), before);

    let mismatched_attempt = DeliverySourceCommitInput::try_new(
        "123e4567-e89b-12d3-a456-426614174000",
        2,
        1_700_000_000,
    )
    .unwrap();
    let mismatch = provisioner
        .build_source_commit(
            &opened,
            &candidate,
            &mismatched_attempt,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(mismatch.code(), "DELIVERY_SOURCE_CHANGED");
    assert_eq!(source.snapshot(&fixture.repository), before);

    let first = provisioner
        .build_source_commit(&opened, &candidate, &metadata, CancellationToken::new())
        .await
        .unwrap();
    let second = provisioner
        .build_source_commit(&opened, &candidate, &metadata, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(first.object_id(), second.object_id());
    let raw = git_line(&fixture.repository, &["cat-file", "-p", first.object_id()]);
    assert_eq!(
        raw,
        format!(
            "tree {}\nparent {}\nauthor Coding Agent <coding-agent@localhost> 1700000000 +0000\ncommitter Coding Agent <coding-agent@localhost> 1700000000 +0000\n\ncoding-agent: deliver task 123e4567-e89b-12d3-a456-426614174000 attempt 1",
            candidate.object_id(),
            source.reservation.base_commit(),
        )
    );
    assert_eq!(metadata.message_template_version(), 1);
    assert_eq!(source.snapshot(&fixture.repository), before);
}

#[tokio::test]
async fn source_commit_rejects_a_candidate_tree_bound_to_another_task_attempt() {
    let fixture = Fixture::new("candidate-tree-provenance").await;
    let source_a = fixture
        .reviewed_dirty_source("123e4567-e89b-12d3-a456-426614174001")
        .await;
    let source_b = fixture
        .reviewed_dirty_source("123e4567-e89b-12d3-a456-426614174002")
        .await;

    let provisioner_a = fixture.delivery_source(&source_a.worktrees).unwrap();
    let opened_a = provisioner_a
        .open_delivery_source(
            &source_a.reservation,
            source_a.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let candidate_a = provisioner_a
        .build_candidate_tree(&opened_a, CancellationToken::new())
        .await
        .unwrap();

    let provisioner_b = fixture.delivery_source(&source_b.worktrees).unwrap();
    let opened_b = provisioner_b
        .open_delivery_source(
            &source_b.reservation,
            source_b.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let before_b = source_b.snapshot(&fixture.repository);
    let metadata_b = DeliverySourceCommitInput::try_new(
        "123e4567-e89b-12d3-a456-426614174002",
        1,
        1_700_000_000,
    )
    .unwrap();

    let error = provisioner_b
        .build_source_commit(
            &opened_b,
            &candidate_a,
            &metadata_b,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "DELIVERY_SOURCE_CHANGED");
    assert_eq!(source_b.snapshot(&fixture.repository), before_b);
}

#[tokio::test]
async fn source_commit_rejects_a_candidate_tree_when_reopened_evidence_changes() {
    let fixture = Fixture::new("candidate-tree-evidence-provenance").await;
    let source = fixture
        .reviewed_dirty_source("123e4567-e89b-12d3-a456-426614174004")
        .await;
    let provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let original = provisioner
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let candidate = provisioner
        .build_candidate_tree(&original, CancellationToken::new())
        .await
        .unwrap();
    let metadata = DeliverySourceCommitInput::try_new(
        "123e4567-e89b-12d3-a456-426614174004",
        1,
        1_700_000_000,
    )
    .unwrap();

    std::fs::write(
        source.worktree_path().join("tracked.txt"),
        b"reviewed fingerprint evidence drift\n",
    )
    .unwrap();
    let changed_fingerprint = fixture.current_fingerprint(&source).await;
    assert_ne!(changed_fingerprint, source.approved_fingerprint);
    let fingerprint_changed = provisioner
        .open_delivery_source(
            &source.reservation,
            changed_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let external_fingerprint_state = source.snapshot(&fixture.repository);
    let error = provisioner
        .build_source_commit(
            &fingerprint_changed,
            &candidate,
            &metadata,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "DELIVERY_SOURCE_CHANGED");
    assert_eq!(
        source.snapshot(&fixture.repository),
        external_fingerprint_state
    );

    std::fs::write(
        source.worktree_path().join("tracked.txt"),
        b"tracked approved change\n",
    )
    .unwrap();
    assert_eq!(
        fixture.current_fingerprint(&source).await,
        source.approved_fingerprint
    );
    git_ok(
        &fixture.repository,
        &["config", "--local", "core.filemode", "true"],
    );
    let digest_changed = provisioner
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let external_digest_state = source.snapshot(&fixture.repository);
    let error = provisioner
        .build_source_commit(
            &digest_changed,
            &candidate,
            &metadata,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "DELIVERY_SOURCE_CHANGED");
    assert_eq!(source.snapshot(&fixture.repository), external_digest_state);
}

#[tokio::test]
async fn candidate_tree_rejects_a_gitlink_added_after_source_open_without_touching_real_state() {
    let fixture = Fixture::new("candidate-tree-gitlink-drift").await;
    let source = fixture
        .reviewed_dirty_source("candidate-tree-gitlink-drift")
        .await;
    let provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let opened = provisioner
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();

    let gitlink = format!(
        "160000,{},reviewed-submodule",
        source.reservation.base_commit()
    );
    git_ok(
        source.worktree_path(),
        &["update-index", "--add", "--cacheinfo", &gitlink],
    );
    let external_state = source.snapshot(&fixture.repository);

    let error = provisioner
        .build_candidate_tree(&opened, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.code(), "DELIVERY_SOURCE_CHANGED");
    assert_eq!(source.snapshot(&fixture.repository), external_state);
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn candidate_tree_rechecks_the_approved_fingerprint_after_write_tree() {
    let fixture = Fixture::new("candidate-tree-post-write-drift").await;
    let source = fixture
        .reviewed_dirty_source("candidate-tree-post-write-drift")
        .await;
    let mut provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let opened = provisioner
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();

    let repository = fixture.repository.clone();
    let admin_directory = source.admin_directory.clone();
    let worktree = source.worktree_path().to_path_buf();
    let observed_external_state = Arc::new(Mutex::new(None));
    provisioner.set_authentication_boundary_hook_for_tests({
        let observed_external_state = Arc::clone(&observed_external_state);
        move |phase| {
            if phase != "after-write-tree-before-fresh-fingerprint" {
                return;
            }
            let changed = worktree.join("after-tree.txt");
            std::fs::write(&changed, b"external post-write change\n").unwrap();
            let snapshot = snapshot_paths(&repository, &admin_directory, &worktree);
            let mut slot = observed_external_state.lock().unwrap();
            assert!(slot.replace(snapshot).is_none());
        }
    });

    let error = provisioner
        .build_candidate_tree(&opened, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.code(), "DELIVERY_SOURCE_CHANGED");
    let external_state = observed_external_state
        .lock()
        .unwrap()
        .take()
        .expect("post-write boundary hook should run exactly once");
    assert_eq!(source.snapshot(&fixture.repository), external_state);
    assert_eq!(
        std::fs::read(source.worktree_path().join("after-tree.txt")).unwrap(),
        b"external post-write change\n"
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn candidate_tree_rechecks_the_approved_fingerprint_after_write_tree_index_drift() {
    let fixture = Fixture::new("candidate-tree-post-write-index-drift").await;
    let source = fixture
        .reviewed_dirty_source("candidate-tree-post-write-index-drift")
        .await;
    let mut provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let opened = provisioner
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();

    let repository = fixture.repository.clone();
    let admin_directory = source.admin_directory.clone();
    let worktree = source.worktree_path().to_path_buf();
    let observed_external_state = Arc::new(Mutex::new(None));
    provisioner.set_authentication_boundary_hook_for_tests({
        let observed_external_state = Arc::clone(&observed_external_state);
        move |phase| {
            if phase != "after-write-tree-before-fresh-fingerprint" {
                return;
            }
            let changed = worktree.join("after-tree-index.txt");
            std::fs::write(&changed, b"externally staged after write-tree\n").unwrap();
            git_ok(&worktree, &["add", "--", "after-tree-index.txt"]);
            let snapshot = snapshot_paths(&repository, &admin_directory, &worktree);
            let mut slot = observed_external_state.lock().unwrap();
            assert!(slot.replace(snapshot).is_none());
        }
    });

    let error = provisioner
        .build_candidate_tree(&opened, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.code(), "DELIVERY_SOURCE_CHANGED");
    let external_state = observed_external_state
        .lock()
        .unwrap()
        .take()
        .expect("post-write boundary hook should run exactly once");
    assert_eq!(source.snapshot(&fixture.repository), external_state);
    assert_eq!(
        std::fs::read(source.worktree_path().join("after-tree-index.txt")).unwrap(),
        b"externally staged after write-tree\n"
    );
}

#[tokio::test]
async fn apply_source_commit_advances_the_authenticated_source_to_the_exact_expected_object() {
    let fixture = Fixture::new("apply-source-commit").await;
    let source = fixture
        .reviewed_dirty_source("123e4567-e89b-12d3-a456-426614174010")
        .await;
    let provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let opened = provisioner
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let candidate = provisioner
        .build_candidate_tree(&opened, CancellationToken::new())
        .await
        .unwrap();
    let input = DeliverySourceCommitInput::try_new(
        "123e4567-e89b-12d3-a456-426614174010",
        1,
        1_700_000_000,
    )
    .unwrap();
    let expected = provisioner
        .build_source_commit(&opened, &candidate, &input, CancellationToken::new())
        .await
        .unwrap();
    let recovery_intent = DeliverySourceRecoveryIntent::from_source(
        DeliverySourcePendingState::CommitPending,
        &opened,
        &candidate,
        Some(&expected),
        input.clone(),
    )
    .unwrap();

    // Commit application must not inherit the pre-crash capability. Re-open
    // the captured intent through a fresh provisioner. Store rehydration is a
    // later application-layer boundary.
    drop(opened);
    drop(provisioner);
    let recovery_provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let recovery = recovery_provisioner
        .open_delivery_source_for_recovery(
            &source.reservation,
            &recovery_intent,
            CancellationToken::new(),
        )
        .await
        .unwrap();

    let outcome = recovery_provisioner
        .apply_source_commit(&recovery, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(outcome, DeliverySourceRecoveryDisposition::Applied);
    let source_ref = format!("refs/heads/{}", source.reservation.branch_name());
    assert_eq!(
        git_line(source.worktree_path(), &["symbolic-ref", "HEAD"]),
        source_ref
    );
    assert_eq!(
        git_line(source.worktree_path(), &["rev-parse", "HEAD"]),
        expected.object_id()
    );
    assert_eq!(
        git_line(source.worktree_path(), &["rev-parse", &source_ref]),
        expected.object_id()
    );
    assert_eq!(
        git_line(source.worktree_path(), &["rev-parse", "HEAD^{tree}"]),
        candidate.object_id()
    );
    git_ok(
        source.worktree_path(),
        &[
            "diff-index",
            "--cached",
            "--quiet",
            candidate.object_id(),
            "--",
        ],
    );
    git_ok(source.worktree_path(), &["diff-files", "--quiet", "--"]);
    assert!(
        git_line(source.worktree_path(), &["status", "--porcelain=v1"]).is_empty(),
        "the applied source must be clean"
    );
}

#[tokio::test]
async fn apply_source_commit_recovers_and_applies_a_staged_delete_without_unrelated_side_effects() {
    let fixture = Fixture::new("apply-source-commit-staged-delete").await;
    let source = fixture
        .reviewed_dirty_source("123e4567-e89b-12d3-a456-426614174015")
        .await;
    git_ok(
        source.worktree_path(),
        &["rm", "--force", "--", "tracked.txt"],
    );
    let approved_fingerprint = fixture.current_fingerprint(&source).await;
    assert_ne!(approved_fingerprint, source.approved_fingerprint);
    let primary_head_before = git_line(&fixture.repository, &["rev-parse", "HEAD"]);

    let provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let opened = provisioner
        .open_delivery_source(
            &source.reservation,
            approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let before_candidate = source.snapshot(&fixture.repository);
    let candidate = provisioner
        .build_candidate_tree(&opened, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(source.snapshot(&fixture.repository), before_candidate);

    let candidate_paths = git_line(
        &fixture.repository,
        &["ls-tree", "--name-only", "-r", candidate.object_id()],
    )
    .lines()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    assert!(!candidate_paths.contains("tracked.txt"));
    assert!(candidate_paths.contains("review-note.txt"));
    assert!(candidate_paths.contains("sequence.txt"));

    let input = DeliverySourceCommitInput::try_new(
        "123e4567-e89b-12d3-a456-426614174015",
        1,
        1_700_000_000,
    )
    .unwrap();
    let expected = provisioner
        .build_source_commit(&opened, &candidate, &input, CancellationToken::new())
        .await
        .unwrap();
    let recovery_intent = DeliverySourceRecoveryIntent::from_source(
        DeliverySourcePendingState::CommitPending,
        &opened,
        &candidate,
        Some(&expected),
        input,
    )
    .unwrap();

    // Recovery has to re-open the captured CommitPending intent instead of
    // retaining authority from the pre-crash source capability.
    drop(opened);
    drop(provisioner);
    let recovery_provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let recovery = recovery_provisioner
        .open_delivery_source_for_recovery(
            &source.reservation,
            &recovery_intent,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let before_classification = source.snapshot(&fixture.repository);
    assert_eq!(
        recovery_provisioner
            .classify_source_recovery(&recovery, CancellationToken::new())
            .await
            .unwrap(),
        DeliverySourceRecoveryDisposition::Continue,
    );
    assert_eq!(source.snapshot(&fixture.repository), before_classification);

    let outcome = recovery_provisioner
        .apply_source_commit(&recovery, CancellationToken::new())
        .await
        .unwrap_or_else(|error| panic!("staged-delete apply failed with {}", error.code()));

    assert_eq!(outcome, DeliverySourceRecoveryDisposition::Applied);
    let source_ref = format!("refs/heads/{}", source.reservation.branch_name());
    assert_eq!(
        git_line(source.worktree_path(), &["rev-parse", "HEAD"]),
        expected.object_id()
    );
    assert_eq!(
        git_line(source.worktree_path(), &["rev-parse", &source_ref]),
        expected.object_id()
    );
    assert_eq!(
        git_line(source.worktree_path(), &["rev-parse", "HEAD^{tree}"]),
        candidate.object_id()
    );
    assert_eq!(
        git_line(&fixture.repository, &["rev-parse", "HEAD"]),
        primary_head_before,
        "applying the delivery source must not advance the primary repository HEAD",
    );
    assert!(
        !source.worktree_path().join("tracked.txt").exists(),
        "the staged deletion must be materialized in the final worktree",
    );
    git_ok(
        source.worktree_path(),
        &[
            "diff-index",
            "--cached",
            "--quiet",
            candidate.object_id(),
            "--",
        ],
    );
    git_ok(source.worktree_path(), &["diff-files", "--quiet", "--"]);
    assert!(
        git_line(source.worktree_path(), &["status", "--porcelain=v1"]).is_empty(),
        "the applied source must be clean",
    );
}

#[tokio::test]
async fn apply_source_commit_requires_reconciliation_for_pre_stage_drift_without_staging_or_advancing_the_source_ref()
 {
    let fixture = Fixture::new("apply-source-commit-pre-stage-drift").await;
    let source = fixture
        .reviewed_dirty_source("123e4567-e89b-12d3-a456-426614174011")
        .await;
    let provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let opened = provisioner
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let candidate = provisioner
        .build_candidate_tree(&opened, CancellationToken::new())
        .await
        .unwrap();
    let input = DeliverySourceCommitInput::try_new(
        "123e4567-e89b-12d3-a456-426614174011",
        1,
        1_700_000_000,
    )
    .unwrap();
    let expected = provisioner
        .build_source_commit(&opened, &candidate, &input, CancellationToken::new())
        .await
        .unwrap();
    let recovery_intent = DeliverySourceRecoveryIntent::from_source(
        DeliverySourcePendingState::CommitPending,
        &opened,
        &candidate,
        Some(&expected),
        input.clone(),
    )
    .unwrap();
    drop(opened);
    drop(provisioner);

    std::fs::write(
        source.worktree_path().join("external-pre-stage-drift.txt"),
        b"must remain un-staged after rejection\n",
    )
    .unwrap();
    let before_apply = source.snapshot(&fixture.repository);
    let recovery_provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let recovery = recovery_provisioner
        .open_delivery_source_for_recovery(
            &source.reservation,
            &recovery_intent,
            CancellationToken::new(),
        )
        .await
        .unwrap();

    let outcome = recovery_provisioner
        .apply_source_commit(&recovery, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(
        outcome,
        DeliverySourceRecoveryDisposition::ReconciliationRequired
    );
    assert_eq!(source.snapshot(&fixture.repository), before_apply);
    assert_eq!(
        git_line(source.worktree_path(), &["rev-parse", "HEAD"]),
        source.reservation.base_commit()
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn apply_source_commit_requires_reconciliation_when_external_staging_changes_the_tree_after_real_add()
 {
    let prepared = PreparedCommitPending::new(
        "apply-source-commit-after-real-index-stage",
        "123e4567-e89b-12d3-a456-426614174012",
    )
    .await;
    let (mut provisioner, recovery) = prepared.open_recovery().await;
    let repository = prepared.fixture.repository.clone();
    let admin_directory = prepared.source.admin_directory.clone();
    let worktree = prepared.source.worktree_path().to_path_buf();
    let observed_external_state = Arc::new(Mutex::new(None));
    provisioner.set_authentication_boundary_hook_for_tests({
        let observed_external_state = Arc::clone(&observed_external_state);
        move |phase| {
            if phase != "after-real-index-stage-before-source-object-reverify" {
                return;
            }
            let external_file = worktree.join("external-after-real-index-stage.txt");
            std::fs::write(&external_file, b"externally staged after real add\n").unwrap();
            git_ok(
                &worktree,
                &["add", "--", "external-after-real-index-stage.txt"],
            );
            let snapshot = snapshot_paths(&repository, &admin_directory, &worktree);
            assert!(
                observed_external_state
                    .lock()
                    .unwrap()
                    .replace(snapshot)
                    .is_none()
            );
        }
    });

    let error = provisioner
        .apply_source_commit(&recovery, CancellationToken::new())
        .await
        .unwrap_err();

    assert_eq!(error.code(), "DELIVERY_RECONCILIATION_REQUIRED");
    let external_state = observed_external_state
        .lock()
        .unwrap()
        .take()
        .expect("after-real-index-stage hook should run exactly once");
    assert_eq!(prepared.snapshot(), external_state);
    assert_eq!(
        std::fs::read(
            prepared
                .source
                .worktree_path()
                .join("external-after-real-index-stage.txt"),
        )
        .unwrap(),
        b"externally staged after real add\n"
    );
    let source_ref = prepared.source_ref();
    assert_eq!(
        git_line(prepared.source.worktree_path(), &["rev-parse", "HEAD"]),
        prepared.source.reservation.base_commit()
    );
    assert_eq!(
        git_line(prepared.source.worktree_path(), &["rev-parse", &source_ref],),
        prepared.source.reservation.base_commit()
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn apply_source_commit_requires_reconciliation_when_an_external_expected_ref_wins_before_cas()
{
    let prepared = PreparedCommitPending::new(
        "apply-source-commit-external-ref-before-cas",
        "123e4567-e89b-12d3-a456-426614174013",
    )
    .await;
    let (mut provisioner, recovery) = prepared.open_recovery().await;
    let repository = prepared.fixture.repository.clone();
    let admin_directory = prepared.source.admin_directory.clone();
    let worktree = prepared.source.worktree_path().to_path_buf();
    let source_ref = prepared.source_ref();
    let expected_object_id = prepared.expected_object_id.clone();
    let base_commit = prepared.source.reservation.base_commit().to_owned();
    let observed_external_state = Arc::new(Mutex::new(None));
    provisioner.set_authentication_boundary_hook_for_tests({
        let observed_external_state = Arc::clone(&observed_external_state);
        let source_ref = source_ref.clone();
        let expected_object_id = expected_object_id.clone();
        move |phase| {
            if phase != "after-source-object-reverify-before-cas" {
                return;
            }
            git_ok(
                &worktree,
                &[
                    "update-ref",
                    "--no-deref",
                    &source_ref,
                    &expected_object_id,
                    &base_commit,
                ],
            );
            let snapshot = snapshot_paths(&repository, &admin_directory, &worktree);
            assert!(
                observed_external_state
                    .lock()
                    .unwrap()
                    .replace(snapshot)
                    .is_none()
            );
        }
    });

    let error = provisioner
        .apply_source_commit(&recovery, CancellationToken::new())
        .await
        .unwrap_err();

    assert_eq!(error.code(), "DELIVERY_RECONCILIATION_REQUIRED");
    let external_state = observed_external_state
        .lock()
        .unwrap()
        .take()
        .expect("before-CAS hook should run exactly once");
    assert_eq!(prepared.snapshot(), external_state);
    assert_eq!(
        git_line(prepared.source.worktree_path(), &["rev-parse", "HEAD"]),
        expected_object_id
    );
    assert_eq!(
        git_line(prepared.source.worktree_path(), &["rev-parse", &source_ref],),
        expected_object_id
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn apply_source_commit_requires_reconciliation_and_preserves_untracked_external_state_after_cas()
 {
    let prepared = PreparedCommitPending::new(
        "apply-source-commit-untracked-after-cas",
        "123e4567-e89b-12d3-a456-426614174014",
    )
    .await;
    let (mut provisioner, recovery) = prepared.open_recovery().await;
    let repository = prepared.fixture.repository.clone();
    let admin_directory = prepared.source.admin_directory.clone();
    let worktree = prepared.source.worktree_path().to_path_buf();
    let observed_external_state = Arc::new(Mutex::new(None));
    provisioner.set_authentication_boundary_hook_for_tests({
        let observed_external_state = Arc::clone(&observed_external_state);
        move |phase| {
            if phase != "after-source-cas-before-postverify" {
                return;
            }
            let external_file = worktree.join("external-after-cas.txt");
            std::fs::write(&external_file, b"untracked external post-CAS change\n").unwrap();
            let snapshot = snapshot_paths(&repository, &admin_directory, &worktree);
            assert!(
                observed_external_state
                    .lock()
                    .unwrap()
                    .replace(snapshot)
                    .is_none()
            );
        }
    });

    let error = provisioner
        .apply_source_commit(&recovery, CancellationToken::new())
        .await
        .unwrap_err();

    assert_eq!(error.code(), "DELIVERY_RECONCILIATION_REQUIRED");
    let external_state = observed_external_state
        .lock()
        .unwrap()
        .take()
        .expect("after-CAS hook should run exactly once");
    assert_eq!(prepared.snapshot(), external_state);
    assert_eq!(
        std::fs::read(
            prepared
                .source
                .worktree_path()
                .join("external-after-cas.txt"),
        )
        .unwrap(),
        b"untracked external post-CAS change\n"
    );
    let source_ref = prepared.source_ref();
    assert_eq!(
        git_line(prepared.source.worktree_path(), &["rev-parse", "HEAD"]),
        prepared.expected_object_id
    );
    assert_eq!(
        git_line(prepared.source.worktree_path(), &["rev-parse", &source_ref],),
        prepared.expected_object_id
    );
}
