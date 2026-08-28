mod delivery_source_support;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use coding_agent_runtime::{
    DeliveryCandidateTree, DeliveryMergeInput, DeliveryMergeOutcome, DeliveryPreflightSource,
    DeliverySourceCapability, DeliverySourceCommit, DeliverySourceCommitInput,
    DeliverySourcePendingState, DeliverySourceProvisioner, DeliverySourceRecoveryDisposition,
    DeliverySourceRecoveryIntent, DeliveryTargetProvisioner, DeliveryTargetRequest, ProcessLimits,
    WorktreeProvisioner, apply_expected_delivery_merge, build_expected_delivery_merge,
    preflight_delivery_merge,
};
use delivery_source_support::{
    Fixture, ReviewedDirtySource, delivery_source_limits, git_line, git_ok,
};
use tokio_util::sync::CancellationToken;

const TASK_ID: &str = "123e4567-e89b-12d3-a456-426614174201";
const EPOCH_SECONDS: i64 = 1_700_000_001;

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
        let input = DeliverySourceCommitInput::try_new(task_id, 1, EPOCH_SECONDS).unwrap();
        let commit = provisioner
            .build_source_commit(&opened, &candidate, &input, CancellationToken::new())
            .await
            .unwrap();
        let intent = DeliverySourceRecoveryIntent::from_source(
            DeliverySourcePendingState::CommitPending,
            &opened,
            &candidate,
            Some(&commit),
            input.clone(),
        )
        .unwrap();
        let recovery = provisioner
            .open_delivery_source_for_recovery(
                &source.reservation,
                &intent,
                CancellationToken::new(),
            )
            .await
            .unwrap();

        assert_eq!(
            provisioner
                .apply_source_commit(&recovery, CancellationToken::new())
                .await
                .unwrap(),
            DeliverySourceRecoveryDisposition::Applied,
        );
        drop(recovery);

        Self {
            source,
            provisioner,
            opened,
            candidate,
            commit,
            input,
        }
    }
}

#[tokio::test]
async fn expected_merge_object_is_deterministic_with_exact_tree_ordered_parents_and_metadata() {
    let fixture = Fixture::new("merge-expected-deterministic").await;
    let prepared = PreparedCommittedSource::new(&fixture, TASK_ID).await;
    let target_provisioner = target_provisioner(&fixture, &prepared.source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    let target_head = target.head_id().to_owned();
    let preflight = committed_preflight(&prepared, &target_provisioner, &target)
        .await
        .unwrap();
    let input = DeliveryMergeInput::try_new(TASK_ID, 1, EPOCH_SECONDS).unwrap();
    let before_source = source_checkout_snapshot(&prepared.source);
    let before_target = target_checkout_snapshot(&fixture);

    let expected = build_expected_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &prepared.opened,
        &target,
        &prepared.candidate,
        &prepared.commit,
        &prepared.input,
        &preflight,
        &input,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let replayed = build_expected_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &prepared.opened,
        &target,
        &prepared.candidate,
        &prepared.commit,
        &prepared.input,
        &preflight,
        &input,
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(expected.object_id(), replayed.object_id());
    assert_eq!(
        git_line(
            &fixture.repository,
            &["show", "--no-patch", "--format=%T", expected.object_id()],
        ),
        preflight.candidate_merge_tree_id(),
    );
    assert_eq!(
        git_line(
            &fixture.repository,
            &["rev-list", "--parents", "-n", "1", expected.object_id()],
        ),
        format!(
            "{} {} {}",
            expected.object_id(),
            target_head,
            prepared.commit.object_id()
        ),
    );
    assert_eq!(
        git_commit_payload(&fixture.repository, expected.object_id()),
        format!(
            "tree {}\nparent {}\nparent {}\nauthor Coding Agent <coding-agent@localhost> {EPOCH_SECONDS} +0000\ncommitter Coding Agent <coding-agent@localhost> {EPOCH_SECONDS} +0000\n\ncoding-agent: merge task {TASK_ID} attempt 1\n",
            preflight.candidate_merge_tree_id(),
            target_head,
            prepared.commit.object_id(),
        )
        .into_bytes(),
    );
    assert_eq!(source_checkout_snapshot(&prepared.source), before_source,);
    assert_eq!(target_checkout_snapshot(&fixture), before_target);
}

#[tokio::test]
async fn actual_no_ff_merge_advances_target_to_the_prebuilt_expected_object_and_leaves_it_clean() {
    let fixture = Fixture::new("merge-actual-exact").await;
    let prepared = PreparedCommittedSource::new(&fixture, TASK_ID).await;
    let target_provisioner = target_provisioner(&fixture, &prepared.source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    let target_head = target.head_id().to_owned();
    let preflight = committed_preflight(&prepared, &target_provisioner, &target)
        .await
        .unwrap();
    let input = DeliveryMergeInput::try_new(TASK_ID, 1, EPOCH_SECONDS).unwrap();
    let expected = build_expected_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &prepared.opened,
        &target,
        &prepared.candidate,
        &prepared.commit,
        &prepared.input,
        &preflight,
        &input,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let before_source = source_checkout_snapshot(&prepared.source);

    assert_eq!(
        apply_expected_delivery_merge(
            &prepared.provisioner,
            &target_provisioner,
            &prepared.opened,
            &target,
            &prepared.candidate,
            &prepared.commit,
            &prepared.input,
            &preflight,
            &expected,
            CancellationToken::new(),
        )
        .await
        .unwrap(),
        DeliveryMergeOutcome::Applied,
    );

    assert_eq!(
        git_line(&fixture.repository, &["rev-parse", "HEAD"]),
        expected.object_id(),
    );
    assert_eq!(
        git_line(&fixture.repository, &["rev-parse", "HEAD^{tree}"]),
        preflight.candidate_merge_tree_id(),
    );
    assert_eq!(
        git_line(
            &fixture.repository,
            &["rev-list", "--parents", "-n", "1", "HEAD"],
        ),
        format!(
            "{} {} {}",
            expected.object_id(),
            target_head,
            prepared.commit.object_id()
        ),
    );
    assert_target_is_clean_without_merge_state(&fixture);
    assert_eq!(source_checkout_snapshot(&prepared.source), before_source,);
}

#[tokio::test]
async fn actual_merge_overrides_accepted_merge_verify_signatures_configuration() {
    let fixture = Fixture::new("merge-verify-signatures-override").await;
    git_ok(
        &fixture.repository,
        &["config", "--local", "merge.verifySignatures", "true"],
    );
    assert_eq!(
        git_line(
            &fixture.repository,
            &["config", "--local", "--get", "merge.verifySignatures"],
        ),
        "true"
    );

    let prepared = PreparedCommittedSource::new(&fixture, TASK_ID).await;
    let target_provisioner = target_provisioner(&fixture, &prepared.source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    let preflight = committed_preflight(&prepared, &target_provisioner, &target)
        .await
        .unwrap();
    let input = DeliveryMergeInput::try_new(TASK_ID, 1, EPOCH_SECONDS).unwrap();
    let expected = build_expected_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &prepared.opened,
        &target,
        &prepared.candidate,
        &prepared.commit,
        &prepared.input,
        &preflight,
        &input,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let before_source = source_checkout_snapshot(&prepared.source);

    assert_eq!(
        apply_expected_delivery_merge(
            &prepared.provisioner,
            &target_provisioner,
            &prepared.opened,
            &target,
            &prepared.candidate,
            &prepared.commit,
            &prepared.input,
            &preflight,
            &expected,
            CancellationToken::new(),
        )
        .await
        .unwrap(),
        DeliveryMergeOutcome::Applied,
    );

    assert_eq!(
        git_line(&fixture.repository, &["rev-parse", "HEAD"]),
        expected.object_id(),
        "the fixed merge policy must override the accepted local verification setting"
    );
    assert_target_is_clean_without_merge_state(&fixture);
    assert_eq!(source_checkout_snapshot(&prepared.source), before_source);
}

#[tokio::test]
async fn actual_merge_does_not_invoke_repository_merge_hooks() {
    let fixture = Fixture::new("merge-hooks-disabled").await;
    let sentinel = fixture.root.join("repository-merge-hook-ran");
    for hook in [
        "pre-merge-commit",
        "prepare-commit-msg",
        "commit-msg",
        "post-merge",
    ] {
        install_hook(&fixture.repository.join(".git/hooks").join(hook), &sentinel);
    }

    let prepared = PreparedCommittedSource::new(&fixture, TASK_ID).await;
    let target_provisioner = target_provisioner(&fixture, &prepared.source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    let preflight = committed_preflight(&prepared, &target_provisioner, &target)
        .await
        .unwrap();
    let input = DeliveryMergeInput::try_new(TASK_ID, 1, EPOCH_SECONDS).unwrap();
    let expected = build_expected_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &prepared.opened,
        &target,
        &prepared.candidate,
        &prepared.commit,
        &prepared.input,
        &preflight,
        &input,
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(
        apply_expected_delivery_merge(
            &prepared.provisioner,
            &target_provisioner,
            &prepared.opened,
            &target,
            &prepared.candidate,
            &prepared.commit,
            &prepared.input,
            &preflight,
            &expected,
            CancellationToken::new(),
        )
        .await
        .unwrap(),
        DeliveryMergeOutcome::Applied,
    );
    assert!(
        !sentinel.exists(),
        "the fixed delivery hooksPath must prevent repository merge hooks from running"
    );
    assert_target_is_clean_without_merge_state(&fixture);
}

#[tokio::test]
async fn source_already_in_target_is_rejected_before_expected_merge_or_actual_merge_side_effects() {
    let fixture = Fixture::new("merge-source-already-target").await;
    let prepared = PreparedCommittedSource::new(&fixture, TASK_ID).await;
    git_ok(
        &fixture.repository,
        &[
            "merge",
            "--ff-only",
            "--no-gpg-sign",
            prepared.source.reservation.branch_name(),
        ],
    );
    let target_provisioner = target_provisioner(&fixture, &prepared.source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    let before_source = source_checkout_snapshot(&prepared.source);
    let before_target = target_checkout_snapshot(&fixture);
    let before_objects = object_inventory(&fixture.repository);

    let error = committed_preflight(&prepared, &target_provisioner, &target)
        .await
        .unwrap_err();

    assert_eq!(error.code(), "SOURCE_ALREADY_IN_TARGET");
    assert_eq!(object_inventory(&fixture.repository), before_objects);
    assert_eq!(source_checkout_snapshot(&prepared.source), before_source,);
    assert_eq!(target_checkout_snapshot(&fixture), before_target);
}

#[tokio::test]
async fn target_head_drift_after_expected_object_is_rejected_before_actual_merge_without_new_effects()
 {
    let fixture = Fixture::new("merge-pre-spawn-target-drift").await;
    let prepared = PreparedCommittedSource::new(&fixture, TASK_ID).await;
    let target_provisioner = target_provisioner(&fixture, &prepared.source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    let preflight = committed_preflight(&prepared, &target_provisioner, &target)
        .await
        .unwrap();
    let input = DeliveryMergeInput::try_new(TASK_ID, 1, EPOCH_SECONDS).unwrap();
    let expected = build_expected_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &prepared.opened,
        &target,
        &prepared.candidate,
        &prepared.commit,
        &prepared.input,
        &preflight,
        &input,
        CancellationToken::new(),
    )
    .await
    .unwrap();

    std::fs::write(
        fixture
            .repository
            .join("external-pre-spawn-target-drift.txt"),
        b"external target change before actual merge\n",
    )
    .unwrap();
    git_ok(
        &fixture.repository,
        &["add", "--", "external-pre-spawn-target-drift.txt"],
    );
    git_ok(
        &fixture.repository,
        &[
            "commit",
            "--quiet",
            "--no-gpg-sign",
            "-m",
            "external target change before actual merge",
        ],
    );
    let before_source = source_checkout_snapshot(&prepared.source);
    let before_target = target_checkout_snapshot(&fixture);
    let before_objects = object_inventory(&fixture.repository);

    let error = apply_expected_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &prepared.opened,
        &target,
        &prepared.candidate,
        &prepared.commit,
        &prepared.input,
        &preflight,
        &expected,
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), "TARGET_HEAD_CHANGED");
    assert_ne!(
        git_line(&fixture.repository, &["rev-parse", "HEAD"]),
        expected.object_id(),
        "a stale expected object must never be applied after target drift"
    );
    assert_eq!(object_inventory(&fixture.repository), before_objects);
    assert_eq!(source_checkout_snapshot(&prepared.source), before_source);
    assert_eq!(target_checkout_snapshot(&fixture), before_target);
    assert_target_is_clean_without_merge_state(&fixture);
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn target_drift_after_expected_object_inspection_is_rejected_before_actual_merge_spawn() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    let fixture = Fixture::new("merge-after-inspect-target-drift").await;
    let prepared = PreparedCommittedSource::new(&fixture, TASK_ID).await;
    let mut target_provisioner = target_provisioner(&fixture, &prepared.source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    let preflight = committed_preflight(&prepared, &target_provisioner, &target)
        .await
        .unwrap();
    let input = DeliveryMergeInput::try_new(TASK_ID, 1, EPOCH_SECONDS).unwrap();
    let expected = build_expected_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &prepared.opened,
        &target,
        &prepared.candidate,
        &prepared.commit,
        &prepared.input,
        &preflight,
        &input,
        CancellationToken::new(),
    )
    .await
    .unwrap();

    let injected = Arc::new(AtomicBool::new(false));
    let observed_head = Arc::new(Mutex::new(None));
    let repository = fixture.repository.clone();
    target_provisioner.set_actual_merge_boundary_hook_for_tests({
        let injected = Arc::clone(&injected);
        let observed_head = Arc::clone(&observed_head);
        move |phase| {
            if phase != "after-expected-merge-object-before-final-preflight"
                || injected.swap(true, Ordering::SeqCst)
            {
                return;
            }
            std::fs::write(
                repository.join("external-after-inspect-drift.txt"),
                b"external target change after expected object inspection\n",
            )
            .unwrap();
            git_ok(
                &repository,
                &["add", "--", "external-after-inspect-drift.txt"],
            );
            git_ok(
                &repository,
                &[
                    "commit",
                    "--quiet",
                    "--no-gpg-sign",
                    "-m",
                    "external target change after expected object inspection",
                ],
            );
            *observed_head.lock().unwrap() = Some(git_line(&repository, &["rev-parse", "HEAD"]));
        }
    });
    let before_source = source_checkout_snapshot(&prepared.source);

    let error = apply_expected_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &prepared.opened,
        &target,
        &prepared.candidate,
        &prepared.commit,
        &prepared.input,
        &preflight,
        &expected,
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert!(injected.load(Ordering::SeqCst));
    assert_eq!(error.code(), "TARGET_HEAD_CHANGED");
    assert_eq!(
        git_line(&fixture.repository, &["rev-parse", "HEAD"]),
        observed_head.lock().unwrap().as_deref().unwrap()
    );
    assert_ne!(
        git_line(&fixture.repository, &["rev-parse", "HEAD"]),
        expected.object_id()
    );
    assert_eq!(source_checkout_snapshot(&prepared.source), before_source);
    assert_target_is_clean_without_merge_state(&fixture);
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn ignored_path_introduced_after_final_scan_is_rejected_before_actual_merge_spawn() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    let fixture = Fixture::new("merge-ignored-race").await;
    let prepared = PreparedCommittedSource::new(&fixture, TASK_ID).await;
    std::fs::write(fixture.repository.join(".gitignore"), b"review-note.txt\n").unwrap();
    git_ok(&fixture.repository, &["add", "--", ".gitignore"]);
    git_ok(
        &fixture.repository,
        &[
            "commit",
            "--quiet",
            "--no-gpg-sign",
            "-m",
            "target ignores the source-only review note",
        ],
    );

    let mut target_provisioner = target_provisioner(&fixture, &prepared.source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    let preflight = committed_preflight(&prepared, &target_provisioner, &target)
        .await
        .unwrap();
    let input = DeliveryMergeInput::try_new(TASK_ID, 1, EPOCH_SECONDS).unwrap();
    let expected = build_expected_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &prepared.opened,
        &target,
        &prepared.candidate,
        &prepared.commit,
        &prepared.input,
        &preflight,
        &input,
        CancellationToken::new(),
    )
    .await
    .unwrap();

    let injected = Arc::new(AtomicBool::new(false));
    let injected_snapshot = Arc::new(Mutex::new(None));
    let repository = fixture.repository.clone();
    target_provisioner.set_actual_merge_boundary_hook_for_tests({
        let injected = Arc::clone(&injected);
        let injected_snapshot = Arc::clone(&injected_snapshot);
        move |phase| {
            if phase != "after-final-preflight-before-last-collision-recheck"
                || injected.swap(true, Ordering::SeqCst)
            {
                return;
            }
            std::fs::write(
                repository.join("review-note.txt"),
                b"externally introduced ignored collision\n",
            )
            .unwrap();
            *injected_snapshot.lock().unwrap() = Some(target_checkout_snapshot_at(&repository));
        }
    });
    let before_source = source_checkout_snapshot(&prepared.source);
    let before_objects = object_inventory(&fixture.repository);

    let error = apply_expected_delivery_merge(
        &prepared.provisioner,
        &target_provisioner,
        &prepared.opened,
        &target,
        &prepared.candidate,
        &prepared.commit,
        &prepared.input,
        &preflight,
        &expected,
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert!(injected.load(Ordering::SeqCst));
    assert_eq!(error.code(), "TARGET_IGNORED_PATH_COLLISION");
    assert_eq!(object_inventory(&fixture.repository), before_objects);
    assert_eq!(source_checkout_snapshot(&prepared.source), before_source);
    assert_eq!(
        target_checkout_snapshot(&fixture),
        injected_snapshot.lock().unwrap().as_ref().unwrap().clone(),
        "the fixed --no-overwrite-ignore guard must leave the injected ignored path and all target state intact"
    );
    assert_eq!(
        std::fs::read(fixture.repository.join("review-note.txt")).unwrap(),
        b"externally introduced ignored collision\n"
    );
    assert_target_is_clean_without_merge_state(&fixture);
}

async fn committed_preflight(
    prepared: &PreparedCommittedSource,
    target_provisioner: &DeliveryTargetProvisioner,
    target: &coding_agent_runtime::DeliveryTargetCapability,
) -> Result<
    coding_agent_runtime::DeliveryPreflightResult,
    coding_agent_runtime::DeliveryPreflightError,
> {
    preflight_delivery_merge(
        &prepared.provisioner,
        target_provisioner,
        target,
        DeliveryPreflightSource::committed(
            &prepared.opened,
            &prepared.candidate,
            &prepared.commit,
            &prepared.input,
        ),
        CancellationToken::new(),
    )
    .await
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

fn install_hook(hook: &Path, sentinel: &Path) {
    std::fs::write(
        hook,
        format!("#!/bin/sh\n{}\n", shell_probe_command(sentinel)),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(hook, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

fn shell_probe_command(sentinel: &Path) -> String {
    let path = sentinel.to_string_lossy().replace('\\', "/");
    if cfg!(windows) {
        format!("cmd.exe /C echo executed>{path}")
    } else {
        format!("touch '{}'", path.replace('\'', "'\"'\"'"))
    }
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
    index: Vec<u8>,
    head: Vec<u8>,
    status: Vec<u8>,
}

fn source_checkout_snapshot(source: &ReviewedDirtySource) -> SourceCheckoutSnapshot {
    SourceCheckoutSnapshot {
        index: std::fs::read(source.admin_directory.join("index")).unwrap(),
        head: std::fs::read(source.admin_directory.join("HEAD")).unwrap(),
        status: git_command_output(
            source.worktree_path(),
            &["status", "--porcelain=v2", "-z", "--untracked-files=all"],
        ),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TargetCheckoutSnapshot {
    refs: Vec<u8>,
    index: Vec<u8>,
    head: Vec<u8>,
    status: Vec<u8>,
    worktree: BTreeMap<PathBuf, Option<Vec<u8>>>,
}

fn target_checkout_snapshot(fixture: &Fixture) -> TargetCheckoutSnapshot {
    target_checkout_snapshot_at(&fixture.repository)
}

fn target_checkout_snapshot_at(repository: &Path) -> TargetCheckoutSnapshot {
    TargetCheckoutSnapshot {
        refs: git_command_output(
            repository,
            &[
                "for-each-ref",
                "--format=%(refname)%00%(objectname)",
                "refs",
            ],
        ),
        index: std::fs::read(repository.join(".git/index")).unwrap(),
        head: std::fs::read(repository.join(".git/HEAD")).unwrap(),
        status: git_command_output(
            repository,
            &["status", "--porcelain=v2", "-z", "--untracked-files=all"],
        ),
        worktree: snapshot_target_worktree(repository),
    }
}

fn assert_target_is_clean_without_merge_state(fixture: &Fixture) {
    assert!(
        git_command_output(
            &fixture.repository,
            &["status", "--porcelain=v2", "-z", "--untracked-files=all"],
        )
        .is_empty()
    );
    for entry in [
        "AUTO_MERGE",
        "MERGE_AUTOSTASH",
        "MERGE_HEAD",
        "MERGE_MODE",
        "MERGE_MSG",
    ] {
        assert!(
            !fixture.repository.join(".git").join(entry).exists(),
            "unexpected merge state entry {entry}"
        );
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

fn git_commit_payload(repository: &Path, object_id: &str) -> Vec<u8> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(["cat-file", "commit", object_id])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "commit payload lookup failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
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
