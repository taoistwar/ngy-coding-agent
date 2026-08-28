mod delivery_source_support;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use coding_agent_runtime::{
    DeliveryCandidateTree, DeliveryPreflightError, DeliveryPreflightSource,
    DeliverySourceCapability, DeliverySourceCommit, DeliverySourceCommitInput, DeliverySourceError,
    DeliverySourcePendingState, DeliverySourceProvisioner, DeliverySourceRecoveryDisposition,
    DeliverySourceRecoveryIntent, DeliveryTargetProvisioner, DeliveryTargetRequest,
    PreparedDeliveryPreflightSource, ProcessLimits, WorktreeProvisioner, preflight_delivery_merge,
    preflight_prepared_delivery_merge,
};
use delivery_source_support::{
    Fixture, RepositorySnapshot, ReviewedDirtySource, delivery_source_limits, git_line, git_ok,
};
use tokio_util::sync::CancellationToken;

struct PreparedCandidate {
    source: ReviewedDirtySource,
    provisioner: DeliverySourceProvisioner,
    opened: DeliverySourceCapability,
    candidate: DeliveryCandidateTree,
}

impl PreparedCandidate {
    async fn new(fixture: &Fixture, task_id: &str) -> Self {
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

        Self {
            source,
            provisioner,
            opened,
            candidate,
        }
    }
}

struct PreparedCommittedSource {
    source: ReviewedDirtySource,
    provisioner: DeliverySourceProvisioner,
    opened: DeliverySourceCapability,
    candidate: DeliveryCandidateTree,
    commit: DeliverySourceCommit,
    input: DeliverySourceCommitInput,
}

impl PreparedCommittedSource {
    async fn new(fixture: &Fixture, task_id: &str) -> Self {
        let prepared = PreparedCandidate::new(fixture, task_id).await;
        let input = DeliverySourceCommitInput::try_new(task_id, 1, 1_700_000_000).unwrap();
        let commit = prepared
            .provisioner
            .build_source_commit(
                &prepared.opened,
                &prepared.candidate,
                &input,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let intent = DeliverySourceRecoveryIntent::from_source(
            DeliverySourcePendingState::CommitPending,
            &prepared.opened,
            &prepared.candidate,
            Some(&commit),
            input.clone(),
        )
        .unwrap();
        let recovery = prepared
            .provisioner
            .open_delivery_source_for_recovery(
                &prepared.source.reservation,
                &intent,
                CancellationToken::new(),
            )
            .await
            .unwrap();

        assert_eq!(
            prepared
                .provisioner
                .apply_source_commit(&recovery, CancellationToken::new())
                .await
                .unwrap(),
            DeliverySourceRecoveryDisposition::Applied,
        );
        drop(recovery);
        assert_eq!(
            git_line(prepared.source.worktree_path(), &["rev-parse", "HEAD"]),
            commit.object_id(),
        );

        Self {
            source: prepared.source,
            provisioner: prepared.provisioner,
            opened: prepared.opened,
            candidate: prepared.candidate,
            commit,
            input,
        }
    }
}

struct PreparedPendingPreflight {
    source: ReviewedDirtySource,
    provisioner: DeliverySourceProvisioner,
    opened: DeliverySourceCapability,
    prepared: PreparedDeliveryPreflightSource,
}

impl PreparedPendingPreflight {
    async fn new(fixture: &Fixture, task_id: &str) -> Self {
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
        // The Store's durable PreflightPending transition is the application
        // boundary immediately before this runtime method.
        let prepared = provisioner
            .prepare_delivery_preflight_source(&opened, CancellationToken::new())
            .await
            .unwrap();
        Self {
            source,
            provisioner,
            opened,
            prepared,
        }
    }
}

#[tokio::test]
async fn prepared_preflight_ids_are_deterministic_opaque_and_checkout_read_only() {
    let fixture = Fixture::new("prepared-preflight-deterministic").await;
    let source = fixture
        .reviewed_dirty_source("123e4567-e89b-12d3-a456-426614174110")
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
    let before = source_checkout_snapshot(&source, &fixture.repository);

    let first = provisioner
        .prepare_delivery_preflight_source(&opened, CancellationToken::new())
        .await
        .unwrap();
    let objects_after_first = object_inventory(&fixture.repository);
    let second = provisioner
        .prepare_delivery_preflight_source(&opened, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(first.candidate_tree_id(), second.candidate_tree_id());
    assert_eq!(first.source_commit_id(), second.source_commit_id());
    assert_eq!(object_inventory(&fixture.repository), objects_after_first);
    assert_eq!(
        source_checkout_snapshot(&source, &fixture.repository),
        before
    );
    let debug = format!("{first:?}");
    assert_eq!(debug, "PreparedDeliveryPreflightSource(<opaque>)");
    assert!(!debug.contains(first.candidate_tree_id()));
    assert!(!debug.contains(first.source_commit_id()));
}

#[tokio::test]
async fn prepared_preflight_runs_from_bound_ids_and_keeps_both_checkouts_read_only() {
    let fixture = Fixture::new("prepared-preflight-ready").await;
    let prepared =
        PreparedPendingPreflight::new(&fixture, "123e4567-e89b-12d3-a456-426614174111").await;
    let target_provisioner = target_provisioner(&fixture, &prepared.source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    let before_source = source_checkout_snapshot(&prepared.source, &fixture.repository);
    let before_target = target_checkout_snapshot(&fixture);

    let result = preflight_prepared_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &target,
        &prepared.opened,
        &prepared.prepared,
        CancellationToken::new(),
    )
    .await
    .unwrap_or_else(|error| panic!("prepared preflight failed: {}", error.code()));

    assert!(result.is_ready());
    assert_eq!(
        result.source_commit_id(),
        prepared.prepared.source_commit_id()
    );
    assert_eq!(
        source_checkout_snapshot(&prepared.source, &fixture.repository),
        before_source,
    );
    assert_eq!(target_checkout_snapshot(&fixture), before_target);
}

#[tokio::test]
async fn prepared_preflight_rejects_a_different_source_capability_before_merge_tree() {
    let fixture = Fixture::new("prepared-preflight-bound-source").await;
    let prepared =
        PreparedPendingPreflight::new(&fixture, "123e4567-e89b-12d3-a456-426614174112").await;
    let target_provisioner = target_provisioner(&fixture, &prepared.source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();

    let foreign_fixture = Fixture::new("prepared-preflight-foreign-source").await;
    let foreign_source = foreign_fixture
        .reviewed_dirty_source("123e4567-e89b-12d3-a456-426614174113")
        .await;
    let foreign_provisioner = foreign_fixture
        .delivery_source(&foreign_source.worktrees)
        .unwrap();
    let foreign_opened = foreign_provisioner
        .open_delivery_source(
            &foreign_source.reservation,
            foreign_source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let before_source = source_checkout_snapshot(&prepared.source, &fixture.repository);
    let before_foreign = source_checkout_snapshot(&foreign_source, &foreign_fixture.repository);
    let before_target = target_checkout_snapshot(&fixture);
    let before_objects = object_inventory(&fixture.repository);
    let before_foreign_objects = object_inventory(&foreign_fixture.repository);

    let error = preflight_prepared_delivery_merge(
        &foreign_provisioner,
        &target_provisioner,
        &target,
        &foreign_opened,
        &prepared.prepared,
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        DeliveryPreflightError::Source(DeliverySourceError::AuthenticationChanged)
    ));
    assert_eq!(error.code(), "WORKTREE_IDENTITY_MISMATCH");
    assert_eq!(object_inventory(&fixture.repository), before_objects);
    assert_eq!(
        object_inventory(&foreign_fixture.repository),
        before_foreign_objects
    );
    assert_eq!(
        source_checkout_snapshot(&prepared.source, &fixture.repository),
        before_source,
    );
    assert_eq!(
        source_checkout_snapshot(&foreign_source, &foreign_fixture.repository),
        before_foreign,
    );
    assert_eq!(target_checkout_snapshot(&fixture), before_target);
}

#[tokio::test]
async fn prepared_preflight_rejects_a_foreign_target_before_merge_tree() {
    let source_fixture = Fixture::new("prepared-preflight-source-repository").await;
    let prepared =
        PreparedPendingPreflight::new(&source_fixture, "123e4567-e89b-12d3-a456-426614174114")
            .await;
    let target_fixture = Fixture::new("prepared-preflight-target-repository").await;
    let target_worktrees = target_fixture.fresh_worktree_provisioner();
    let target_provisioner = target_provisioner(&target_fixture, &target_worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&target_fixture), CancellationToken::new())
        .await
        .unwrap();
    let before_source = source_checkout_snapshot(&prepared.source, &source_fixture.repository);
    let before_target = target_checkout_snapshot(&target_fixture);
    let before_source_objects = object_inventory(&source_fixture.repository);
    let before_target_objects = object_inventory(&target_fixture.repository);

    let error = preflight_prepared_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &target,
        &prepared.opened,
        &prepared.prepared,
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), "WORKTREE_IDENTITY_MISMATCH");
    assert_eq!(
        object_inventory(&source_fixture.repository),
        before_source_objects
    );
    assert_eq!(
        object_inventory(&target_fixture.repository),
        before_target_objects
    );
    assert_eq!(
        source_checkout_snapshot(&prepared.source, &source_fixture.repository),
        before_source,
    );
    assert_eq!(target_checkout_snapshot(&target_fixture), before_target);
}

#[tokio::test]
async fn prepared_preflight_source_drift_stops_before_merge_tree_without_checkout_side_effects() {
    let fixture = Fixture::new("prepared-preflight-source-drift").await;
    let prepared =
        PreparedPendingPreflight::new(&fixture, "123e4567-e89b-12d3-a456-426614174115").await;
    let target_provisioner = target_provisioner(&fixture, &prepared.source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    std::fs::write(
        prepared.source.worktree_path().join("tracked.txt"),
        b"drift after durable preflight inputs were bound\n",
    )
    .unwrap();
    let before_source = source_checkout_snapshot(&prepared.source, &fixture.repository);
    let before_target = target_checkout_snapshot(&fixture);
    let before_objects = object_inventory(&fixture.repository);

    let error = preflight_prepared_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &target,
        &prepared.opened,
        &prepared.prepared,
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), "DELIVERY_SOURCE_CHANGED");
    assert_eq!(object_inventory(&fixture.repository), before_objects);
    assert_eq!(
        source_checkout_snapshot(&prepared.source, &fixture.repository),
        before_source,
    );
    assert_eq!(target_checkout_snapshot(&fixture), before_target);
}

#[tokio::test]
async fn committed_source_preflight_uses_the_exact_persisted_commit_and_keeps_both_checkouts_read_only()
 {
    let fixture = Fixture::new("preflight-committed-exact").await;
    let prepared =
        PreparedCommittedSource::new(&fixture, "123e4567-e89b-12d3-a456-426614174101").await;
    let target_provisioner = target_provisioner(&fixture, &prepared.source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    let before_source = source_checkout_snapshot(&prepared.source, &fixture.repository);
    let before_target = target_checkout_snapshot(&fixture);

    let result = preflight_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &target,
        DeliveryPreflightSource::committed(
            &prepared.opened,
            &prepared.candidate,
            &prepared.commit,
            &prepared.input,
        ),
        CancellationToken::new(),
    )
    .await
    .unwrap_or_else(|error| panic!("committed preflight failed: {}", error.code()));

    assert!(result.is_ready());
    assert_eq!(result.source_commit_id(), prepared.commit.object_id());
    assert_eq!(
        source_checkout_snapshot(&prepared.source, &fixture.repository),
        before_source,
    );
    assert_eq!(target_checkout_snapshot(&fixture), before_target);
}

#[tokio::test]
async fn committed_source_already_merged_into_target_is_rejected_without_checkout_mutation() {
    let fixture = Fixture::new("preflight-source-already-target").await;
    let prepared =
        PreparedCommittedSource::new(&fixture, "123e4567-e89b-12d3-a456-426614174102").await;
    git_ok(
        &fixture.repository,
        &[
            "merge",
            "--ff-only",
            "--no-gpg-sign",
            prepared.source.reservation.branch_name(),
        ],
    );
    assert_eq!(
        git_line(&fixture.repository, &["rev-parse", "HEAD"]),
        prepared.commit.object_id(),
    );
    let target_provisioner = target_provisioner(&fixture, &prepared.source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    let before_source = source_checkout_snapshot(&prepared.source, &fixture.repository);
    let before_target = target_checkout_snapshot(&fixture);

    let error = preflight_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &target,
        DeliveryPreflightSource::committed(
            &prepared.opened,
            &prepared.candidate,
            &prepared.commit,
            &prepared.input,
        ),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), "SOURCE_ALREADY_IN_TARGET");
    assert_eq!(
        source_checkout_snapshot(&prepared.source, &fixture.repository),
        before_source,
    );
    assert_eq!(target_checkout_snapshot(&fixture), before_target);
}

#[tokio::test]
async fn same_branch_pairing_is_rejected_before_creating_a_candidate_source_commit() {
    let fixture = Fixture::new("preflight-same-branch").await;
    let prepared = PreparedCandidate::new(&fixture, "123e4567-e89b-12d3-a456-426614174103").await;
    git_ok(
        &fixture.repository,
        &[
            "checkout",
            "--ignore-other-worktrees",
            "--quiet",
            prepared.source.reservation.branch_name(),
        ],
    );
    let target_provisioner = target_provisioner(&fixture, &prepared.source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    let before_source = source_checkout_snapshot(&prepared.source, &fixture.repository);
    let before_target = target_checkout_snapshot(&fixture);
    let before_objects = object_inventory(&fixture.repository);

    let error = preflight_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &target,
        DeliveryPreflightSource::candidate(&prepared.opened, &prepared.candidate),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), "TARGET_BRANCH_MISMATCH");
    assert_eq!(object_inventory(&fixture.repository), before_objects);
    assert_eq!(
        source_checkout_snapshot(&prepared.source, &fixture.repository),
        before_source,
    );
    assert_eq!(target_checkout_snapshot(&fixture), before_target);
}

#[tokio::test]
async fn cross_repository_pairing_is_rejected_before_creating_a_source_object() {
    let source_fixture = Fixture::new("preflight-cross-source").await;
    let prepared =
        PreparedCandidate::new(&source_fixture, "123e4567-e89b-12d3-a456-426614174104").await;
    let target_fixture = Fixture::new("preflight-cross-target").await;
    let target_worktrees = target_fixture.fresh_worktree_provisioner();
    let target_provisioner = target_provisioner(&target_fixture, &target_worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&target_fixture), CancellationToken::new())
        .await
        .unwrap();
    let before_source = source_checkout_snapshot(&prepared.source, &source_fixture.repository);
    let before_target = target_checkout_snapshot(&target_fixture);
    let before_source_objects = object_inventory(&source_fixture.repository);
    let before_target_objects = object_inventory(&target_fixture.repository);

    let error = preflight_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &target,
        DeliveryPreflightSource::candidate(&prepared.opened, &prepared.candidate),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), "WORKTREE_IDENTITY_MISMATCH");
    assert_eq!(
        object_inventory(&source_fixture.repository),
        before_source_objects
    );
    assert_eq!(
        object_inventory(&target_fixture.repository),
        before_target_objects
    );
    assert_eq!(
        source_checkout_snapshot(&prepared.source, &source_fixture.repository),
        before_source,
    );
    assert_eq!(target_checkout_snapshot(&target_fixture), before_target);
}

#[tokio::test]
async fn source_fingerprint_drift_after_capabilities_open_rejects_preflight_without_checkout_mutation()
 {
    let fixture = Fixture::new("preflight-source-fingerprint-drift").await;
    let prepared = PreparedCandidate::new(&fixture, "123e4567-e89b-12d3-a456-426614174105").await;
    let target_provisioner = target_provisioner(&fixture, &prepared.source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();

    std::fs::write(
        prepared.source.worktree_path().join("tracked.txt"),
        b"external source fingerprint drift\n",
    )
    .unwrap();
    assert_ne!(
        fixture.current_fingerprint(&prepared.source).await,
        prepared.source.approved_fingerprint,
    );
    let before_source = source_checkout_snapshot(&prepared.source, &fixture.repository);
    let before_target = target_checkout_snapshot(&fixture);
    let before_objects = object_inventory(&fixture.repository);

    let error = preflight_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &target,
        DeliveryPreflightSource::candidate(&prepared.opened, &prepared.candidate),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), "DELIVERY_SOURCE_CHANGED");
    assert_eq!(object_inventory(&fixture.repository), before_objects);
    assert_eq!(
        source_checkout_snapshot(&prepared.source, &fixture.repository),
        before_source,
    );
    assert_eq!(target_checkout_snapshot(&fixture), before_target);
}

#[tokio::test]
async fn target_head_drift_after_capability_open_rejects_preflight_without_checkout_mutation() {
    let fixture = Fixture::new("preflight-target-head-drift").await;
    let prepared = PreparedCandidate::new(&fixture, "123e4567-e89b-12d3-a456-426614174106").await;
    let target_provisioner = target_provisioner(&fixture, &prepared.source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();

    std::fs::write(
        fixture.repository.join("target-head-drift.txt"),
        b"external target HEAD drift\n",
    )
    .unwrap();
    git_ok(&fixture.repository, &["add", "--", "target-head-drift.txt"]);
    git_ok(
        &fixture.repository,
        &[
            "commit",
            "--quiet",
            "--no-gpg-sign",
            "-m",
            "external target HEAD drift",
        ],
    );
    let before_source = source_checkout_snapshot(&prepared.source, &fixture.repository);
    let before_target = target_checkout_snapshot(&fixture);
    let before_objects = object_inventory(&fixture.repository);

    let error = preflight_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &target,
        DeliveryPreflightSource::candidate(&prepared.opened, &prepared.candidate),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), "TARGET_HEAD_CHANGED");
    assert_eq!(object_inventory(&fixture.repository), before_objects);
    assert_eq!(
        source_checkout_snapshot(&prepared.source, &fixture.repository),
        before_source,
    );
    assert_eq!(target_checkout_snapshot(&fixture), before_target);
}

fn target_provisioner(
    fixture: &Fixture,
    worktrees: &WorktreeProvisioner,
) -> DeliveryTargetProvisioner {
    DeliveryTargetProvisioner::from_worktree_provisioner(
        worktrees,
        Arc::clone(&fixture.delivery_git),
        &fixture.runtime_directory,
        fixture.task_process_scope(),
        process_limits(),
        delivery_source_limits(),
    )
    .unwrap()
}

fn process_limits() -> ProcessLimits {
    ProcessLimits::try_new(
        512 * 1024,
        512 * 1024,
        std::time::Duration::from_secs(30),
        std::time::Duration::from_secs(5),
    )
    .unwrap()
}

fn target_request(fixture: &Fixture) -> DeliveryTargetRequest {
    DeliveryTargetRequest::try_new(
        git_line(&fixture.repository, &["symbolic-ref", "--short", "HEAD"]),
        git_line(&fixture.repository, &["rev-parse", "HEAD"]),
    )
    .unwrap()
}

#[derive(Debug, PartialEq, Eq)]
struct SourceCheckoutSnapshot {
    repository: RepositorySnapshot,
    head: Vec<u8>,
    status: Vec<u8>,
}

fn source_checkout_snapshot(
    source: &ReviewedDirtySource,
    repository: &Path,
) -> SourceCheckoutSnapshot {
    SourceCheckoutSnapshot {
        repository: source.snapshot(repository),
        head: std::fs::read(source.admin_directory.join("HEAD")).unwrap(),
        status: git_command_output(
            source.worktree_path(),
            &["status", "--porcelain=v2", "-z", "--untracked-files=all"],
        ),
    }
}

#[derive(Debug, PartialEq, Eq)]
struct TargetCheckoutSnapshot {
    refs: Vec<u8>,
    index: Vec<u8>,
    head: Vec<u8>,
    status: Vec<u8>,
    worktree: BTreeMap<PathBuf, Option<Vec<u8>>>,
}

fn target_checkout_snapshot(fixture: &Fixture) -> TargetCheckoutSnapshot {
    TargetCheckoutSnapshot {
        refs: git_command_output(
            &fixture.repository,
            &[
                "for-each-ref",
                "--format=%(refname)%00%(objectname)",
                "refs",
            ],
        ),
        index: std::fs::read(fixture.repository.join(".git/index")).unwrap(),
        head: std::fs::read(fixture.repository.join(".git/HEAD")).unwrap(),
        status: git_command_output(
            &fixture.repository,
            &["status", "--porcelain=v2", "-z", "--untracked-files=all"],
        ),
        worktree: snapshot_target_worktree(&fixture.repository),
    }
}

fn snapshot_target_worktree(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn visit(root: &Path, directory: &Path, entries: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
        let mut children = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap())
            .collect::<Vec<_>>();
        children.sort_by_key(|entry| entry.file_name());
        for entry in children {
            if entry.file_name() == ".git" {
                continue;
            }
            let path = entry.path();
            let relative = path.strip_prefix(root).unwrap().to_owned();
            if entry.file_type().unwrap().is_dir() {
                entries.insert(relative, None);
                visit(root, &path, entries);
            } else {
                entries.insert(relative, Some(std::fs::read(path).unwrap()));
            }
        }
    }

    let mut entries = BTreeMap::new();
    visit(root, root, &mut entries);
    entries
}

fn object_inventory(repository: &Path) -> Vec<Vec<u8>> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(repository)
        .args([
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objectname) %(objecttype)",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "object inventory failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut objects = output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    objects.sort();
    objects
}

fn git_command_output(repository: &Path, arguments: &[&str]) -> Vec<u8> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git fixture command failed: git -C {} {}\nstdout: {}\nstderr: {}",
        repository.display(),
        arguments.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}
