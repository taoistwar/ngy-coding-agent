mod delivery_source_support;

use coding_agent_runtime::{WorktreeError, WorktreeObservation};
use tokio_util::sync::CancellationToken;

use delivery_source_support::{
    Fixture, assert_read_only_failure, git_line, git_ok, git_with_stdin,
    near_zero_timeout_delivery_source_limits, tiny_delivery_source_limits,
    write_non_utf8_symbolic_head,
};

#[tokio::test]
async fn exact_reviewed_dirty_source_opens_without_weakening_p4a_open_ready() {
    let fixture = Fixture::new("reviewed-dirty").await;
    let source = fixture.reviewed_dirty_source("reviewed-dirty").await;
    let before = source.snapshot(&fixture.repository);

    assert_ne!(
        source
            .worktrees
            .observe(&source.reservation, CancellationToken::new())
            .await,
        WorktreeObservation::Ready,
        "P4-A must continue rejecting a dirty completed worktree"
    );
    assert!(matches!(
        source
            .worktrees
            .open_ready(&source.reservation, CancellationToken::new())
            .await,
        Err(WorktreeError::InconsistentArtifact)
    ));

    let opened = fixture
        .delivery_source(&source.worktrees)
        .unwrap()
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();

    assert_eq!(opened.identity(), source.reservation.identity());
    assert_eq!(opened.base_commit(), source.reservation.base_commit());
    assert_eq!(opened.branch_name(), source.reservation.branch_name());
    assert_eq!(format!("{opened:?}"), "DeliverySourceCapability(<opaque>)");
    assert_read_only_failure(before, &source, &fixture.repository);
}

#[tokio::test]
async fn stale_extra_and_index_drift_are_delivery_source_changed_and_read_only() {
    let fixture = Fixture::new("source-drift").await;

    for drift in [SourceDrift::Tracked, SourceDrift::Extra, SourceDrift::Index] {
        let task_id = match drift {
            SourceDrift::Tracked => "tracked-drift",
            SourceDrift::Extra => "extra-drift",
            SourceDrift::Index => "index-drift",
        };
        let source = fixture.reviewed_dirty_source(task_id).await;
        drift.apply(&source);
        let before = source.snapshot(&fixture.repository);

        let error = fixture
            .delivery_source(&source.worktrees)
            .unwrap()
            .open_delivery_source(
                &source.reservation,
                source.approved_fingerprint,
                CancellationToken::new(),
            )
            .await
            .unwrap_err();

        assert_eq!(error.code(), "DELIVERY_SOURCE_CHANGED", "{drift:?}");
        assert_read_only_failure(before, &source, &fixture.repository);
    }
}

#[tokio::test]
async fn branch_base_admin_path_and_fixed_lock_mismatches_are_rejected_read_only() {
    for mismatch in [
        ControlMismatch::Branch,
        ControlMismatch::BaseHead,
        ControlMismatch::AdminPath,
        ControlMismatch::LockReason,
    ] {
        let fixture = Fixture::new(mismatch.name()).await;
        let source = fixture.reviewed_dirty_source(mismatch.name()).await;
        mismatch.apply(&fixture, &source);
        let before = source.snapshot(&fixture.repository);

        let result = fixture
            .delivery_source(&source.worktrees)
            .unwrap()
            .open_delivery_source(
                &source.reservation,
                source.approved_fingerprint,
                CancellationToken::new(),
            )
            .await;

        assert!(result.is_err(), "{mismatch:?} was accepted");
        assert_read_only_failure(before, &source, &fixture.repository);
    }
}

#[tokio::test]
async fn unmerged_index_and_gitlink_entries_are_rejected_without_side_effects() {
    let fixture = Fixture::new("unsafe-index").await;

    let unmerged = fixture.reviewed_dirty_source("unmerged-index").await;
    let blob = git_line(unmerged.worktree_path(), &["rev-parse", "HEAD:tracked.txt"]);
    let zero = "0".repeat(blob.len());
    git_with_stdin(
        unmerged.worktree_path(),
        &["update-index", "--index-info"],
        format!(
            "0 {zero}\ttracked.txt\n100644 {blob} 1\ttracked.txt\n100644 {blob} 2\ttracked.txt\n100644 {blob} 3\ttracked.txt\n"
        )
        .as_bytes(),
    );
    assert_rejected_read_only(&fixture, &unmerged).await;

    let gitlink = fixture.reviewed_dirty_source("gitlink-index").await;
    let commit = git_line(gitlink.worktree_path(), &["rev-parse", "HEAD"]);
    git_ok(
        gitlink.worktree_path(),
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("160000,{commit},vendor-link"),
        ],
    );
    assert_rejected_read_only(&fixture, &gitlink).await;
}

#[tokio::test]
async fn malformed_non_utf8_symbolic_ref_is_rejected_without_side_effects() {
    let fixture = Fixture::new("non-utf8-ref").await;
    let source = fixture.reviewed_dirty_source("non-utf8-ref").await;
    write_non_utf8_symbolic_head(&source);
    assert_rejected_read_only(&fixture, &source).await;
}

#[tokio::test]
async fn cancellation_bounds_and_timeout_are_typed_and_read_only() {
    let fixture = Fixture::new("bounded-failures").await;
    let source = fixture.reviewed_dirty_source("bounded-failures").await;

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let before_cancel = source.snapshot(&fixture.repository);
    let cancelled = fixture
        .delivery_source(&source.worktrees)
        .unwrap()
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            cancellation,
        )
        .await
        .unwrap_err();
    assert_eq!(cancelled.code(), "COMMAND_CANCELLED");
    assert_read_only_failure(before_cancel, &source, &fixture.repository);

    let before_bounds = source.snapshot(&fixture.repository);
    let bounded = fixture
        .delivery_source_with_limits(&source.worktrees, tiny_delivery_source_limits())
        .unwrap()
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(bounded.code(), "DELIVERY_SOURCE_BOUNDS_EXCEEDED");
    assert_read_only_failure(before_bounds, &source, &fixture.repository);

    let before_timeout = source.snapshot(&fixture.repository);
    let timed_out = fixture
        .delivery_source_with_limits(
            &source.worktrees,
            near_zero_timeout_delivery_source_limits(),
        )
        .unwrap()
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(timed_out.code(), "COMMAND_TIMED_OUT");
    assert_read_only_failure(before_timeout, &source, &fixture.repository);
}

async fn assert_rejected_read_only(
    fixture: &Fixture,
    source: &delivery_source_support::ReviewedDirtySource,
) {
    let before = source.snapshot(&fixture.repository);
    let result = fixture
        .delivery_source(&source.worktrees)
        .unwrap()
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await;
    assert!(result.is_err());
    assert_read_only_failure(before, source, &fixture.repository);
}

#[derive(Debug, Clone, Copy)]
enum SourceDrift {
    Tracked,
    Extra,
    Index,
}

impl SourceDrift {
    fn apply(self, source: &delivery_source_support::ReviewedDirtySource) {
        match self {
            Self::Tracked => std::fs::write(
                source.worktree_path().join("tracked.txt"),
                b"changed after approval\n",
            )
            .unwrap(),
            Self::Extra => std::fs::write(
                source.worktree_path().join("not-reviewed.txt"),
                b"not reviewed\n",
            )
            .unwrap(),
            Self::Index => git_ok(source.worktree_path(), &["add", "--", "tracked.txt"]),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ControlMismatch {
    Branch,
    BaseHead,
    AdminPath,
    LockReason,
}

impl ControlMismatch {
    const fn name(self) -> &'static str {
        match self {
            Self::Branch => "branch-mismatch",
            Self::BaseHead => "base-mismatch",
            Self::AdminPath => "admin-path-mismatch",
            Self::LockReason => "lock-reason-mismatch",
        }
    }

    fn apply(self, fixture: &Fixture, source: &delivery_source_support::ReviewedDirtySource) {
        match self {
            Self::Branch => {
                git_ok(
                    source.worktree_path(),
                    &["branch", "other-source", source.reservation.base_commit()],
                );
                git_ok(
                    source.worktree_path(),
                    &["symbolic-ref", "HEAD", "refs/heads/other-source"],
                );
            }
            Self::BaseHead => {
                let prior = git_line(source.worktree_path(), &["rev-parse", "HEAD^"]);
                git_ok(
                    source.worktree_path(),
                    &[
                        "update-ref",
                        &format!("refs/heads/{}", source.reservation.branch_name()),
                        &prior,
                        source.reservation.base_commit(),
                    ],
                );
            }
            Self::AdminPath => {
                let wrong_pointer = fixture.root.join("wrong-worktree/.git");
                std::fs::write(
                    source.admin_directory.join("gitdir"),
                    format!("{}\n", wrong_pointer.to_string_lossy().replace('\\', "/")),
                )
                .unwrap();
            }
            Self::LockReason => {
                let worktree = source.worktree_path().to_string_lossy().into_owned();
                git_ok(
                    &fixture.repository,
                    &["worktree", "unlock", "--", &worktree],
                );
                git_ok(
                    &fixture.repository,
                    &[
                        "worktree",
                        "lock",
                        "--reason=external-owner",
                        "--",
                        &worktree,
                    ],
                );
            }
        }
    }
}
