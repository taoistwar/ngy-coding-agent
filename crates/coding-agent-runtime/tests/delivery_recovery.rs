mod delivery_source_support;

use coding_agent_runtime::{
    DeliveryCandidateTree, DeliverySourceCommit, DeliverySourceCommitInput, DeliverySourceError,
    DeliverySourcePendingState, DeliverySourceProvisioner, DeliverySourceRecoveryDisposition,
    DeliverySourceRecoveryIntent,
};
use delivery_source_support::{Fixture, RepositorySnapshot, ReviewedDirtySource, git_line, git_ok};
use tokio_util::sync::CancellationToken;

/// Task 12 source metadata captured through the normal pre-stage path.
/// The original in-memory capability is deliberately dropped before any
/// recovery observation, so every assertion below has to re-authenticate a
/// fresh capability from the captured, opaque runtime intent. Store-to-runtime
/// rehydration is deliberately outside this runtime-only task.
struct PreparedSourceCommit {
    fixture: Fixture,
    source: ReviewedDirtySource,
    candidate: DeliveryCandidateTree,
    expected: DeliverySourceCommit,
    object_intent: DeliverySourceRecoveryIntent,
    commit_intent: DeliverySourceRecoveryIntent,
}

impl PreparedSourceCommit {
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
        let object_intent = DeliverySourceRecoveryIntent::from_source(
            DeliverySourcePendingState::ObjectPending,
            &opened,
            &candidate,
            None,
            input.clone(),
        )
        .unwrap();
        let commit_intent = DeliverySourceRecoveryIntent::from_source(
            DeliverySourcePendingState::CommitPending,
            &opened,
            &candidate,
            Some(&expected),
            input.clone(),
        )
        .unwrap();

        // This is the fresh-capability boundary under test: `opened` has no
        // authority in the subsequent recovery flow.
        drop(opened);
        drop(provisioner);

        Self {
            fixture,
            source,
            candidate,
            expected,
            object_intent,
            commit_intent,
        }
    }

    fn snapshot(&self) -> RepositorySnapshot {
        self.source.snapshot(&self.fixture.repository)
    }

    fn recovery_intent(&self, pending: DeliverySourcePendingState) -> DeliverySourceRecoveryIntent {
        match pending {
            DeliverySourcePendingState::ObjectPending => self.object_intent.clone(),
            DeliverySourcePendingState::CommitPending => self.commit_intent.clone(),
        }
    }

    async fn open_recovery(
        &self,
        pending: DeliverySourcePendingState,
    ) -> (
        DeliverySourceProvisioner,
        coding_agent_runtime::DeliverySourceRecoveryCapability,
    ) {
        let intent = self.recovery_intent(pending);
        let provisioner = self
            .fixture
            .delivery_source(&self.source.worktrees)
            .unwrap();
        let recovery = provisioner
            .open_delivery_source_for_recovery(
                &self.source.reservation,
                &intent,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        (provisioner, recovery)
    }

    fn stage_candidate(&self) {
        git_ok(self.source.worktree_path(), &["add", "--all"]);
        self.assert_candidate_index_and_worktree_are_clean();
    }

    /// Models a crash after Task 12's `read-tree --reset <candidate>` succeeds
    /// but before the fixed stat-cache refresh can run.  The raw index is
    /// deliberately retained exactly as Git left it; recovery must observe it
    /// without trying to "help" by refreshing the real index.
    fn stage_candidate_without_stat_refresh(&self) {
        git_ok(
            self.source.worktree_path(),
            &["read-tree", "--reset", self.candidate.object_id()],
        );
    }

    fn advance_source_ref_to_expected(&self) {
        let source_ref = format!("refs/heads/{}", self.source.reservation.branch_name());
        git_ok(
            self.source.worktree_path(),
            &[
                "update-ref",
                &source_ref,
                self.expected.object_id(),
                self.source.reservation.base_commit(),
            ],
        );
        assert_eq!(
            git_line(self.source.worktree_path(), &["rev-parse", "HEAD"]),
            self.expected.object_id()
        );
        self.assert_candidate_index_and_worktree_are_clean();
    }

    fn assert_candidate_index_and_worktree_are_clean(&self) {
        git_ok(
            self.source.worktree_path(),
            &[
                "diff-index",
                "--cached",
                "--quiet",
                self.candidate.object_id(),
                "--",
            ],
        );
        git_ok(
            self.source.worktree_path(),
            &["diff-files", "--quiet", "--"],
        );
    }

    async fn assert_classification_is_pure(
        &self,
        pending: DeliverySourcePendingState,
        expected: DeliverySourceRecoveryDisposition,
    ) {
        let before = self.snapshot();
        let (provisioner, recovery) = self.open_recovery(pending).await;
        let observed = provisioner
            .classify_source_recovery(&recovery, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(observed, expected);
        drop(recovery);
        drop(provisioner);
        assert_eq!(self.snapshot(), before);
    }
}

#[tokio::test]
async fn object_pending_replays_only_the_approved_pre_stage_source_without_mutating_it() {
    let prepared = PreparedSourceCommit::new(
        "object-pending-recovery",
        "123e4567-e89b-12d3-a456-426614174020",
    )
    .await;

    prepared
        .assert_classification_is_pure(
            DeliverySourcePendingState::ObjectPending,
            DeliverySourceRecoveryDisposition::ReplayObject,
        )
        .await;

    // A crash after the real index reaches the candidate must not let an
    // ObjectPending replay reinterpret that later state as safe.
    prepared.stage_candidate();
    prepared
        .assert_classification_is_pure(
            DeliverySourcePendingState::ObjectPending,
            DeliverySourceRecoveryDisposition::ReconciliationRequired,
        )
        .await;

    // Likewise, a crash after the source CAS must remain a reconciliation
    // problem until the durable state says CommitPending.
    prepared.advance_source_ref_to_expected();
    prepared
        .assert_classification_is_pure(
            DeliverySourcePendingState::ObjectPending,
            DeliverySourceRecoveryDisposition::ReconciliationRequired,
        )
        .await;
}

#[tokio::test]
async fn commit_pending_accepts_exactly_its_three_recovery_states_without_mutation() {
    let prepared = PreparedSourceCommit::new(
        "commit-pending-recovery",
        "123e4567-e89b-12d3-a456-426614174021",
    )
    .await;

    prepared
        .assert_classification_is_pure(
            DeliverySourcePendingState::CommitPending,
            DeliverySourceRecoveryDisposition::Continue,
        )
        .await;

    prepared.stage_candidate();
    prepared
        .assert_classification_is_pure(
            DeliverySourcePendingState::CommitPending,
            DeliverySourceRecoveryDisposition::StageComplete,
        )
        .await;

    prepared.advance_source_ref_to_expected();
    prepared
        .assert_classification_is_pure(
            DeliverySourcePendingState::CommitPending,
            DeliverySourceRecoveryDisposition::Applied,
        )
        .await;

    // Expected source ref alone is insufficient once the worktree drifts; the
    // classifier must leave the external file untouched and require recovery
    // at a higher layer rather than reset/clean/checkout it.
    std::fs::write(
        prepared
            .source
            .worktree_path()
            .join("external-post-cas-drift.txt"),
        b"must survive recovery observation\n",
    )
    .unwrap();
    prepared
        .assert_classification_is_pure(
            DeliverySourcePendingState::CommitPending,
            DeliverySourceRecoveryDisposition::ReconciliationRequired,
        )
        .await;
}

#[tokio::test]
async fn recovery_conservatively_reconciles_when_stage_crashes_before_stat_refresh() {
    let prepared = PreparedSourceCommit::new(
        "recovery-stage-before-stat-refresh",
        "123e4567-e89b-12d3-a456-426614174026",
    )
    .await;

    prepared.stage_candidate_without_stat_refresh();
    prepared
        .assert_classification_is_pure(
            DeliverySourcePendingState::CommitPending,
            DeliverySourceRecoveryDisposition::ReconciliationRequired,
        )
        .await;
}

#[tokio::test]
async fn captured_recovery_intent_reauthenticates_with_a_new_provisioner() {
    let prepared = PreparedSourceCommit::new(
        "recovery-fresh-provisioner",
        "123e4567-e89b-12d3-a456-426614174022",
    )
    .await;
    let before = prepared.snapshot();

    let (first_provisioner, first_recovery) = prepared
        .open_recovery(DeliverySourcePendingState::CommitPending)
        .await;
    assert_eq!(
        first_provisioner
            .classify_source_recovery(&first_recovery, CancellationToken::new())
            .await
            .unwrap(),
        DeliverySourceRecoveryDisposition::Continue,
    );
    drop(first_recovery);
    drop(first_provisioner);

    // A new provisioner has no in-memory authority from the first one. It
    // must authenticate the same captured intent again and observe the same
    // state without changing any repository object, index, or worktree file.
    let (second_provisioner, second_recovery) = prepared
        .open_recovery(DeliverySourcePendingState::CommitPending)
        .await;
    assert_eq!(
        second_provisioner
            .classify_source_recovery(&second_recovery, CancellationToken::new())
            .await
            .unwrap(),
        DeliverySourceRecoveryDisposition::Continue,
    );
    drop(second_recovery);
    drop(second_provisioner);
    assert_eq!(prepared.snapshot(), before);
}

#[tokio::test]
async fn recovery_rejects_a_logically_identical_replaced_common_git_directory() {
    let prepared = PreparedSourceCommit::new(
        "recovery-common-provenance",
        "123e4567-e89b-12d3-a456-426614174024",
    )
    .await;
    let intent = prepared.recovery_intent(DeliverySourcePendingState::CommitPending);
    let before = prepared.snapshot();
    let common_git_directory = prepared.fixture.repository.join(".git");
    let retained_original = prepared.fixture.root.join("retained-original-common-git");

    replace_directory_with_logically_identical_copy(&common_git_directory, &retained_original);
    assert_eq!(
        git_line(prepared.source.worktree_path(), &["rev-parse", "HEAD"]),
        prepared.source.reservation.base_commit(),
        "replacement must retain the approved base commit",
    );
    assert_eq!(
        git_line(
            prepared.source.worktree_path(),
            &["symbolic-ref", "--quiet", "HEAD"],
        ),
        format!("refs/heads/{}", prepared.source.reservation.branch_name()),
        "replacement must retain the reserved source branch",
    );
    assert_eq!(prepared.snapshot(), before);

    // Use a new authenticator and restored reservation bound to the copied
    // common directory.  That keeps the test focused on the persisted
    // recovery intent's common/admin provenance rather than the original
    // worktree provisioner's in-memory common-directory capability.
    let fresh_worktrees = prepared.fixture.fresh_worktree_provisioner();
    let restored_reservation = fresh_worktrees
        .restore_reservation(
            prepared.source.reservation.identity().clone(),
            prepared.source.reservation.base_commit(),
            prepared.source.reservation.branch_name(),
            prepared.source.reservation.worktree_path(),
        )
        .unwrap();
    let fresh_provisioner = prepared.fixture.delivery_source(&fresh_worktrees).unwrap();
    let error = fresh_provisioner
        .open_delivery_source_for_recovery(&restored_reservation, &intent, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error, DeliverySourceError::AuthenticationChanged);
    assert_eq!(error.code(), "DELIVERY_SOURCE_CHANGED");
    drop(fresh_provisioner);
    drop(fresh_worktrees);

    // Rejection is observational even when the copied common directory is
    // otherwise a valid Git control plane.
    assert_eq!(prepared.snapshot(), before);
}

#[tokio::test]
async fn recovery_rejects_a_logically_identical_replaced_linked_admin_directory() {
    let prepared = PreparedSourceCommit::new(
        "recovery-admin-provenance",
        "123e4567-e89b-12d3-a456-426614174023",
    )
    .await;
    // `PreparedSourceCommit::new` creates this intent with
    // `DeliverySourceRecoveryIntent::from_source` from the opened source,
    // candidate, and expected commit, then drops that capability before
    // returning. The recovery open below therefore has no in-memory
    // authorization to carry across the replacement.
    let intent = prepared.recovery_intent(DeliverySourcePendingState::CommitPending);
    let before = prepared.snapshot();
    let retained_original = prepared.fixture.root.join("retained-original-admin");

    replace_directory_with_logically_identical_copy(
        &prepared.source.admin_directory,
        &retained_original,
    );
    assert_linked_admin_control_files_are_identical(
        &retained_original,
        &prepared.source.admin_directory,
    );
    assert_eq!(
        git_line(prepared.source.worktree_path(), &["rev-parse", "HEAD"]),
        prepared.source.reservation.base_commit(),
        "replacement must retain the approved base commit",
    );
    assert_eq!(
        git_line(
            prepared.source.worktree_path(),
            &["symbolic-ref", "--quiet", "HEAD"],
        ),
        format!("refs/heads/{}", prepared.source.reservation.branch_name()),
        "replacement must retain the reserved source branch",
    );
    assert_eq!(prepared.snapshot(), before);

    // Construct a fresh delivery provisioner after the replacement. The
    // copied control files still prove the same branch, base, lock, gitdir,
    // and commondir, so a successful open would show that captured recovery
    // provenance failed to distinguish the replacement administration object.
    let fresh_provisioner = prepared
        .fixture
        .delivery_source(&prepared.source.worktrees)
        .unwrap();
    let error = fresh_provisioner
        .open_delivery_source_for_recovery(
            &prepared.source.reservation,
            &intent,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error, DeliverySourceError::AuthenticationChanged);
    assert_eq!(error.code(), "DELIVERY_SOURCE_CHANGED");
    drop(fresh_provisioner);

    // Re-authentication is observational: it may reject the copied control
    // plane, but it must not rewrite the source ref, real index, or worktree.
    assert_eq!(prepared.snapshot(), before);
}

#[tokio::test]
async fn recovery_intent_debug_is_redacted() {
    let prepared = PreparedSourceCommit::new(
        "recovery-intent-debug",
        "123e4567-e89b-12d3-a456-426614174025",
    )
    .await;
    let intent = prepared.recovery_intent(DeliverySourcePendingState::CommitPending);

    assert_eq!(
        format!("{intent:?}"),
        "DeliverySourceRecoveryIntent(<validated>)",
    );
}

fn replace_directory_with_logically_identical_copy(
    directory: &std::path::Path,
    retained_original: &std::path::Path,
) {
    assert!(
        !retained_original.exists(),
        "test replacement destination must be unique"
    );
    std::fs::rename(directory, retained_original)
        .expect("move the original linked-worktree administration directory");
    copy_directory_tree(retained_original, directory);
}

fn copy_directory_tree(source: &std::path::Path, destination: &std::path::Path) {
    std::fs::create_dir(destination).expect("create replacement administration directory");
    let mut entries = std::fs::read_dir(source)
        .expect("enumerate original linked-worktree administration directory")
        .map(|entry| entry.expect("read administration directory entry"))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let file_type = entry.file_type().expect("read administration entry type");
        let replacement = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_directory_tree(&entry.path(), &replacement);
        } else {
            assert!(
                file_type.is_file(),
                "linked-worktree administration fixtures must not contain special entries"
            );
            std::fs::copy(entry.path(), replacement)
                .expect("copy linked-worktree administration control file");
        }
    }
}

fn assert_linked_admin_control_files_are_identical(
    original: &std::path::Path,
    replacement: &std::path::Path,
) {
    for control_file in ["HEAD", "locked", "gitdir", "commondir"] {
        assert_eq!(
            std::fs::read(original.join(control_file))
                .expect("read original linked-worktree control file"),
            std::fs::read(replacement.join(control_file))
                .expect("read copied linked-worktree control file"),
            "replacement must preserve {control_file}",
        );
    }
}
