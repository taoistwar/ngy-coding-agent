mod delivery_source_support;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use coding_agent_runtime::{
    DeliveryGitObjectFormat, DeliveryPreflightSource, DeliverySourceError,
    DeliveryTargetProvisioner, DeliveryTargetRequest, ProcessLimits, preflight_delivery_merge,
};
use delivery_source_support::{Fixture, delivery_source_limits, git_line, git_ok, git_with_stdin};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn registered_target_discovery_returns_exact_runtime_facts_without_caller_ref_input() {
    let fixture = Fixture::new("target-discovery").await;
    let worktrees = fixture.fresh_worktree_provisioner();
    let target = target_provisioner(&fixture, &worktrees);
    let expected_branch = git_line(&fixture.repository, &["symbolic-ref", "--short", "HEAD"]);
    let expected_head = git_line(&fixture.repository, &["rev-parse", "HEAD"]);
    let before = target_snapshot(&fixture);

    let observed = target
        .observe_registered_delivery_target(CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(observed.branch_name(), expected_branch);
    assert_eq!(observed.head_id(), expected_head);
    assert_eq!(observed.object_format(), DeliveryGitObjectFormat::Sha1);
    assert_eq!(
        format!("{observed:?}"),
        "RegisteredDeliveryTargetObservation(<opaque>)"
    );
    let debug = format!("{observed:?}");
    assert!(!debug.contains(fixture.root.to_string_lossy().as_ref()));
    assert!(!debug.contains(observed.branch_name()));
    assert!(!debug.contains(observed.head_id()));
    assert_eq!(observed.capability().branch_name(), expected_branch);
    assert_eq!(target_snapshot(&fixture), before);
}

#[tokio::test]
async fn registered_target_discovery_rejects_detached_and_dirty_scenes_read_only() {
    for (name, state, expected_code) in [
        (
            "target-discovery-detached",
            TargetState::Detached,
            "TARGET_BRANCH_DETACHED",
        ),
        (
            "target-discovery-dirty",
            TargetState::Dirty,
            "TARGET_WORKTREE_DIRTY",
        ),
    ] {
        let fixture = Fixture::new(name).await;
        let worktrees = fixture.fresh_worktree_provisioner();
        let target = target_provisioner(&fixture, &worktrees);
        state.apply(&fixture);
        let before = target_snapshot(&fixture);

        let error = target
            .observe_registered_delivery_target(CancellationToken::new())
            .await
            .unwrap_err();

        assert_eq!(error.code(), expected_code);
        assert_eq!(target_snapshot(&fixture), before);
    }
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn registered_target_discovery_rejects_config_drift_across_its_ab_boundary() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let fixture = Fixture::new("target-discovery-config-drift").await;
    let worktrees = fixture.fresh_worktree_provisioner();
    let mut target = target_provisioner(&fixture, &worktrees);
    let repository = fixture.repository.clone();
    let injected = Arc::new(AtomicBool::new(false));
    let hook_injected = Arc::clone(&injected);
    target.set_registered_observation_boundary_hook_for_tests(move |phase| {
        if phase == "after-identity-discovery" && !hook_injected.swap(true, Ordering::SeqCst) {
            git_ok(&repository, &["config", "user.name", "Drifted Target User"]);
        }
    });

    let error = target
        .observe_registered_delivery_target(CancellationToken::new())
        .await
        .unwrap_err();

    assert!(injected.load(Ordering::SeqCst));
    assert_eq!(error.code(), "UNSAFE_GIT_CONFIGURATION");
}

#[tokio::test]
async fn registered_target_discovery_rejects_same_path_common_git_replacement() {
    let fixture = Fixture::new("target-discovery-common-replacement").await;
    let worktrees = fixture.fresh_worktree_provisioner();
    let target = target_provisioner(&fixture, &worktrees);
    let common = fixture.repository.join(".git");
    let retained = fixture.root.join("retained-target-common-git");
    replace_directory_with_logically_identical_copy(&common, &retained);

    let error = target
        .observe_registered_delivery_target(CancellationToken::new())
        .await
        .unwrap_err();

    assert_eq!(error.code(), "WORKTREE_IDENTITY_MISMATCH");
}

#[tokio::test]
async fn persistence_binding_is_redacted_exact_and_rejects_cross_repository_capabilities() {
    let fixture = Fixture::new("persistence-binding-source").await;
    let source = fixture.reviewed_dirty_source("persistence-binding").await;
    let source_provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let source_capability = source_provisioner
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let provisioner = target_provisioner(&fixture, &source.worktrees);
    let target_observation = provisioner
        .observe_registered_delivery_target(CancellationToken::new())
        .await
        .unwrap();
    let binding = source_capability
        .persistence_binding_for_target(target_observation.capability())
        .unwrap();

    assert_eq!(binding.object_format(), target_observation.object_format());
    assert_eq!(binding.source_identity(), source.reservation.identity());
    assert_eq!(
        binding.source_branch(),
        format!("refs/heads/{}", source.reservation.branch_name())
    );
    assert_eq!(
        binding.source_base_commit(),
        source.reservation.base_commit()
    );
    assert_eq!(binding.approved_fingerprint(), source.approved_fingerprint);
    assert_eq!(
        binding.common_git_identity_algorithm(),
        "directory_identity_v1"
    );
    assert_eq!(
        binding.worktree_admin_identity_algorithm(),
        "directory_identity_v1"
    );
    for digest in [
        binding.common_git_identity_digest(),
        binding.worktree_admin_identity_digest(),
        binding.source_config_attributes_digest(),
        binding.target_config_attributes_digest(),
        binding.target_security_digest(),
    ] {
        assert_eq!(digest.len(), 64);
        assert!(
            digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        );
    }
    assert_eq!(
        binding.target_branch(),
        format!("refs/heads/{}", target_observation.branch_name())
    );
    assert_eq!(binding.expected_target_head(), target_observation.head_id());
    let debug = format!("{binding:?}");
    assert_eq!(debug, "DeliveryPersistenceBinding(<redacted>)");
    let root = fixture.root.to_string_lossy().into_owned();
    for secret in [
        root.as_str(),
        binding.source_branch(),
        binding.source_base_commit(),
        binding.common_git_identity_digest(),
        binding.worktree_admin_identity_digest(),
        binding.target_security_digest(),
        binding.target_branch(),
        binding.expected_target_head(),
    ] {
        assert!(!debug.contains(secret));
    }

    let foreign_fixture = Fixture::new("persistence-binding-foreign-target").await;
    let foreign_worktrees = foreign_fixture.fresh_worktree_provisioner();
    let foreign_target = target_provisioner(&foreign_fixture, &foreign_worktrees)
        .observe_registered_delivery_target(CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        source_capability
            .persistence_binding_for_target(foreign_target.capability())
            .unwrap_err(),
        DeliverySourceError::AuthenticationChanged
    );
}

#[tokio::test]
async fn registered_primary_checkout_target_opens_read_only_at_exact_branch_and_head() {
    let fixture = Fixture::new("target-open").await;
    let worktrees = fixture.fresh_worktree_provisioner();
    let target = target_provisioner(&fixture, &worktrees);
    let request = target_request(&fixture);
    let before = target_snapshot(&fixture);

    let opened = target
        .open_delivery_target(&request, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(opened.branch_name(), request.branch_name());
    assert_eq!(opened.head_id(), request.expected_head());
    assert_eq!(format!("{opened:?}"), "DeliveryTargetCapability(<opaque>)");
    assert_eq!(target_snapshot(&fixture), before);
}

#[tokio::test]
async fn target_rejects_detached_branch_mismatch_head_drift_dirty_and_operation_states_read_only() {
    for state in [
        TargetState::Detached,
        TargetState::BranchMismatch,
        TargetState::HeadDrift,
        TargetState::Dirty,
        TargetState::UnmergedIndex,
        TargetState::Operation,
        TargetState::RebaseHead,
        TargetState::RebaseApply,
        TargetState::RebaseMerge,
        TargetState::CherryPick,
        TargetState::Revert,
        TargetState::BisectLog,
        TargetState::BisectStart,
        TargetState::Sequencer,
    ] {
        let fixture = Fixture::new(state.name()).await;
        let worktrees = fixture.fresh_worktree_provisioner();
        let target = target_provisioner(&fixture, &worktrees);
        let request = target_request(&fixture);
        state.apply(&fixture);
        let before = target_snapshot(&fixture);
        if let Some(sentinel) = state.git_state_sentinel() {
            assert!(
                before
                    .git_state
                    .get(&PathBuf::from(sentinel))
                    .is_some_and(|entry| !matches!(entry, GitStateEntry::Absent)),
                "{state:?} sentinel was not captured"
            );
        }

        let error = target
            .open_delivery_target(&request, CancellationToken::new())
            .await
            .unwrap_err();

        assert_eq!(error.code(), state.code(), "{state:?}");
        assert_eq!(target_snapshot(&fixture), before, "{state:?}");
    }
}

#[tokio::test]
async fn clean_candidate_preflight_creates_only_unreachable_objects_and_keeps_target_exact() {
    let fixture = Fixture::new("preflight-clean").await;
    let source = fixture.reviewed_dirty_source("preflight-clean").await;
    let source_provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let opened_source = source_provisioner
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let candidate = source_provisioner
        .build_candidate_tree(&opened_source, CancellationToken::new())
        .await
        .unwrap();
    let target_provisioner = target_provisioner(&fixture, &source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    let before_target = target_snapshot(&fixture);
    let before_source = source.snapshot(&fixture.repository);

    let result = preflight_delivery_merge(
        &source_provisioner,
        &target_provisioner,
        &target,
        DeliveryPreflightSource::candidate(&opened_source, &candidate),
        CancellationToken::new(),
    )
    .await
    .unwrap_or_else(|error| panic!("preflight failed: {}", error.code()));

    assert!(result.is_ready());
    assert!(result.conflict_paths().is_none());
    assert_eq!(target_snapshot(&fixture), before_target);
    assert_eq!(source.snapshot(&fixture.repository), before_source);
}

#[tokio::test]
async fn conflict_preflight_returns_bounded_paths_without_mutating_either_checkout() {
    let fixture = Fixture::new("preflight-conflict").await;
    let source = fixture.reviewed_dirty_source("preflight-conflict").await;
    let source_provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let opened_source = source_provisioner
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let candidate = source_provisioner
        .build_candidate_tree(&opened_source, CancellationToken::new())
        .await
        .unwrap();
    std::fs::write(
        fixture.repository.join("tracked.txt"),
        b"target conflicting change\n",
    )
    .unwrap();
    git_ok(&fixture.repository, &["add", "--", "tracked.txt"]);
    git_ok(
        &fixture.repository,
        &[
            "commit",
            "--quiet",
            "--no-gpg-sign",
            "-m",
            "target conflict",
        ],
    );
    let target_provisioner = target_provisioner(&fixture, &source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    let before_target = target_snapshot(&fixture);
    let before_source = source.snapshot(&fixture.repository);

    let result = preflight_delivery_merge(
        &source_provisioner,
        &target_provisioner,
        &target,
        DeliveryPreflightSource::candidate(&opened_source, &candidate),
        CancellationToken::new(),
    )
    .await
    .unwrap_or_else(|error| panic!("preflight failed: {}", error.code()));

    assert!(result.is_conflict());
    assert!(
        result
            .conflict_paths()
            .is_some_and(|paths| !paths.is_empty())
    );
    assert_eq!(target_snapshot(&fixture), before_target);
    assert_eq!(source.snapshot(&fixture.repository), before_source);
}

#[tokio::test]
async fn ignored_target_collision_is_rejected_without_overwriting_the_ignored_file() {
    let fixture = Fixture::new("preflight-ignored-collision").await;
    let mut source = fixture
        .reviewed_dirty_source("preflight-ignored-collision")
        .await;
    std::fs::write(
        source.worktree_path().join("collision.txt"),
        b"approved source value\n",
    )
    .unwrap();
    source.approved_fingerprint = fixture.current_fingerprint(&source).await;
    std::fs::write(fixture.repository.join(".gitignore"), b"collision.txt\n").unwrap();
    git_ok(&fixture.repository, &["add", "--", ".gitignore"]);
    git_ok(
        &fixture.repository,
        &[
            "commit",
            "--quiet",
            "--no-gpg-sign",
            "-m",
            "ignore collision",
        ],
    );
    std::fs::write(
        fixture.repository.join("collision.txt"),
        b"target ignored value\n",
    )
    .unwrap();

    let source_provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let opened_source = source_provisioner
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let candidate = source_provisioner
        .build_candidate_tree(&opened_source, CancellationToken::new())
        .await
        .unwrap();
    let target_provisioner = target_provisioner(&fixture, &source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    let before_target = target_snapshot(&fixture);
    let before_source = source.snapshot(&fixture.repository);

    let error = preflight_delivery_merge(
        &source_provisioner,
        &target_provisioner,
        &target,
        DeliveryPreflightSource::candidate(&opened_source, &candidate),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), "TARGET_IGNORED_PATH_COLLISION");
    assert_eq!(target_snapshot(&fixture), before_target);
    assert_eq!(source.snapshot(&fixture.repository), before_source);
    assert_eq!(
        std::fs::read(fixture.repository.join("collision.txt")).unwrap(),
        b"target ignored value\n"
    );
}

#[tokio::test]
async fn ignored_target_directory_ancestor_collision_is_rejected_without_mutation() {
    let fixture = Fixture::new("preflight-ignored-directory-collision").await;
    let mut source = fixture
        .reviewed_dirty_source("preflight-ignored-directory-collision")
        .await;
    let source_collision = source.worktree_path().join("collision-directory/new.txt");
    std::fs::create_dir_all(source_collision.parent().unwrap()).unwrap();
    std::fs::write(&source_collision, b"approved nested source value\n").unwrap();
    source.approved_fingerprint = fixture.current_fingerprint(&source).await;

    std::fs::write(
        fixture.repository.join(".gitignore"),
        b"collision-directory/\n",
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
            "ignore directory collision",
        ],
    );
    let ignored_directory = fixture.repository.join("collision-directory");
    let ignored_child = ignored_directory.join("preserve.txt");
    std::fs::create_dir_all(&ignored_directory).unwrap();
    std::fs::write(&ignored_child, b"target ignored child\n").unwrap();

    let source_provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let opened_source = source_provisioner
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let candidate = source_provisioner
        .build_candidate_tree(&opened_source, CancellationToken::new())
        .await
        .unwrap();
    let target_provisioner = target_provisioner(&fixture, &source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    let before_target = target_snapshot(&fixture);
    let before_source = source.snapshot(&fixture.repository);

    let error = preflight_delivery_merge(
        &source_provisioner,
        &target_provisioner,
        &target,
        DeliveryPreflightSource::candidate(&opened_source, &candidate),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), "TARGET_IGNORED_PATH_COLLISION");
    assert_eq!(target_snapshot(&fixture), before_target);
    assert_eq!(source.snapshot(&fixture.repository), before_source);
    assert!(ignored_directory.is_dir());
    assert_eq!(
        std::fs::read(&ignored_child).unwrap(),
        b"target ignored child\n"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn ignored_target_symlink_collision_is_rejected_without_mutation() {
    use std::os::unix::fs::symlink;

    let fixture = Fixture::new("preflight-ignored-symlink-collision").await;
    let mut source = fixture
        .reviewed_dirty_source("preflight-ignored-symlink-collision")
        .await;
    std::fs::write(
        source.worktree_path().join("collision-link"),
        b"approved source replacement\n",
    )
    .unwrap();
    source.approved_fingerprint = fixture.current_fingerprint(&source).await;

    let link_target = fixture.repository.join("tracked-link-target.txt");
    std::fs::write(&link_target, b"target link destination\n").unwrap();
    std::fs::write(fixture.repository.join(".gitignore"), b"collision-link\n").unwrap();
    git_ok(
        &fixture.repository,
        &["add", "--", ".gitignore", "tracked-link-target.txt"],
    );
    git_ok(
        &fixture.repository,
        &[
            "commit",
            "--quiet",
            "--no-gpg-sign",
            "-m",
            "ignore symlink collision",
        ],
    );
    let ignored_link = fixture.repository.join("collision-link");
    symlink("tracked-link-target.txt", &ignored_link).unwrap();
    let expected_link_target = std::fs::read_link(&ignored_link).unwrap();

    let source_provisioner = fixture.delivery_source(&source.worktrees).unwrap();
    let opened_source = source_provisioner
        .open_delivery_source(
            &source.reservation,
            source.approved_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let candidate = source_provisioner
        .build_candidate_tree(&opened_source, CancellationToken::new())
        .await
        .unwrap();
    let target_provisioner = target_provisioner(&fixture, &source.worktrees);
    let target = target_provisioner
        .open_delivery_target(&target_request(&fixture), CancellationToken::new())
        .await
        .unwrap();
    let before_target = target_snapshot(&fixture);
    let before_source = source.snapshot(&fixture.repository);

    let error = preflight_delivery_merge(
        &source_provisioner,
        &target_provisioner,
        &target,
        DeliveryPreflightSource::candidate(&opened_source, &candidate),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert_eq!(error.code(), "TARGET_IGNORED_PATH_COLLISION");
    assert_eq!(target_snapshot(&fixture), before_target);
    assert_eq!(source.snapshot(&fixture.repository), before_source);
    assert_eq!(
        std::fs::read_link(&ignored_link).unwrap(),
        expected_link_target
    );
    assert_eq!(
        std::fs::read(&link_target).unwrap(),
        b"target link destination\n"
    );
}

fn target_provisioner(
    fixture: &Fixture,
    worktrees: &coding_agent_runtime::WorktreeProvisioner,
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
    let branch = git_line(&fixture.repository, &["symbolic-ref", "--short", "HEAD"]);
    let head = git_line(&fixture.repository, &["rev-parse", "HEAD"]);
    DeliveryTargetRequest::try_new(branch, head).unwrap()
}

#[derive(Debug, Clone, Copy)]
enum TargetState {
    Detached,
    BranchMismatch,
    HeadDrift,
    Dirty,
    UnmergedIndex,
    Operation,
    RebaseHead,
    RebaseApply,
    RebaseMerge,
    CherryPick,
    Revert,
    BisectLog,
    BisectStart,
    Sequencer,
}

impl TargetState {
    fn name(self) -> &'static str {
        match self {
            Self::Detached => "target-detached",
            Self::BranchMismatch => "target-branch",
            Self::HeadDrift => "target-head",
            Self::Dirty => "target-dirty",
            Self::UnmergedIndex => "target-unmerged-index",
            Self::Operation => "target-operation",
            Self::RebaseHead => "target-rebase-head",
            Self::RebaseApply => "target-rebase-apply",
            Self::RebaseMerge => "target-rebase-merge",
            Self::CherryPick => "target-cherry-pick",
            Self::Revert => "target-revert",
            Self::BisectLog => "target-bisect-log",
            Self::BisectStart => "target-bisect-start",
            Self::Sequencer => "target-sequencer",
        }
    }

    fn code(self) -> &'static str {
        match self {
            Self::Detached => "TARGET_BRANCH_DETACHED",
            Self::BranchMismatch => "TARGET_BRANCH_MISMATCH",
            Self::HeadDrift => "TARGET_HEAD_CHANGED",
            Self::Dirty | Self::UnmergedIndex => "TARGET_WORKTREE_DIRTY",
            Self::Operation
            | Self::RebaseHead
            | Self::RebaseApply
            | Self::RebaseMerge
            | Self::CherryPick
            | Self::Revert
            | Self::BisectLog
            | Self::BisectStart
            | Self::Sequencer => "TARGET_GIT_OPERATION_IN_PROGRESS",
        }
    }

    fn apply(self, fixture: &Fixture) {
        match self {
            Self::Detached => git_ok(&fixture.repository, &["checkout", "--detach", "--quiet"]),
            Self::BranchMismatch => git_ok(
                &fixture.repository,
                &["checkout", "--quiet", "-b", "other-target-branch"],
            ),
            Self::HeadDrift => {
                std::fs::write(fixture.repository.join("target-drift.txt"), b"drift\n").unwrap();
                git_ok(&fixture.repository, &["add", "--", "target-drift.txt"]);
                git_ok(
                    &fixture.repository,
                    &["commit", "--quiet", "--no-gpg-sign", "-m", "target drift"],
                );
            }
            Self::Dirty => {
                std::fs::write(fixture.repository.join("untracked-target.txt"), b"dirty\n")
                    .unwrap();
            }
            Self::UnmergedIndex => create_unmerged_index(fixture),
            Self::Operation => create_git_state_file(fixture, "MERGE_HEAD"),
            Self::RebaseHead => create_git_state_file(fixture, "REBASE_HEAD"),
            Self::RebaseApply => create_git_state_directory(fixture, "rebase-apply"),
            Self::RebaseMerge => create_git_state_directory(fixture, "rebase-merge"),
            Self::CherryPick => create_git_state_file(fixture, "CHERRY_PICK_HEAD"),
            Self::Revert => create_git_state_file(fixture, "REVERT_HEAD"),
            Self::BisectLog => create_git_state_file(fixture, "BISECT_LOG"),
            Self::BisectStart => create_git_state_file(fixture, "BISECT_START"),
            Self::Sequencer => create_git_state_directory(fixture, "sequencer"),
        }
    }

    fn git_state_sentinel(self) -> Option<&'static str> {
        match self {
            Self::Operation => Some("MERGE_HEAD"),
            Self::RebaseHead => Some("REBASE_HEAD"),
            Self::RebaseApply => Some("rebase-apply"),
            Self::RebaseMerge => Some("rebase-merge"),
            Self::CherryPick => Some("CHERRY_PICK_HEAD"),
            Self::Revert => Some("REVERT_HEAD"),
            Self::BisectLog => Some("BISECT_LOG"),
            Self::BisectStart => Some("BISECT_START"),
            Self::Sequencer => Some("sequencer"),
            Self::Detached
            | Self::BranchMismatch
            | Self::HeadDrift
            | Self::Dirty
            | Self::UnmergedIndex => None,
        }
    }
}

fn create_unmerged_index(fixture: &Fixture) {
    let blob = git_line(&fixture.repository, &["rev-parse", "HEAD:tracked.txt"]);
    let zero = "0".repeat(blob.len());
    git_with_stdin(
        &fixture.repository,
        &["update-index", "--index-info"],
        format!(
            "0 {zero}\ttracked.txt\n100644 {blob} 1\ttracked.txt\n100644 {blob} 2\ttracked.txt\n100644 {blob} 3\ttracked.txt\n"
        )
        .as_bytes(),
    );
    assert!(
        !git_command_output(&fixture.repository, &["ls-files", "--unmerged", "-z", "--"])
            .is_empty()
    );
}

fn create_git_state_file(fixture: &Fixture, name: &str) {
    std::fs::write(
        git_directory(fixture).join(name),
        b"target operation sentinel\n",
    )
    .unwrap();
}

fn create_git_state_directory(fixture: &Fixture, name: &str) {
    let directory = git_directory(fixture).join(name);
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("state"), b"target operation sentinel\n").unwrap();
}

fn git_directory(fixture: &Fixture) -> PathBuf {
    fixture.repository.join(".git")
}

fn replace_directory_with_logically_identical_copy(directory: &Path, retained_original: &Path) {
    assert!(
        !retained_original.exists(),
        "test replacement destination must be unique"
    );
    std::fs::rename(directory, retained_original).expect("move the original common Git directory");
    copy_directory_tree(retained_original, directory);
}

fn copy_directory_tree(source: &Path, destination: &Path) {
    std::fs::create_dir(destination).expect("create replacement common Git directory");
    let mut entries = std::fs::read_dir(source)
        .expect("enumerate original common Git directory")
        .map(|entry| entry.expect("read common Git directory entry"))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let file_type = entry.file_type().expect("read common Git entry type");
        let replacement = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_directory_tree(&entry.path(), &replacement);
        } else {
            assert!(
                file_type.is_file(),
                "common Git fixtures must not contain special entries"
            );
            std::fs::copy(entry.path(), replacement)
                .expect("copy common Git control file or object");
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct TargetSnapshot {
    refs: Vec<u8>,
    index: Vec<u8>,
    head: Vec<u8>,
    status: GitCommandSnapshot,
    git_state: BTreeMap<PathBuf, GitStateEntry>,
    worktree: BTreeMap<PathBuf, Option<Vec<u8>>>,
}

#[derive(Debug, PartialEq, Eq)]
struct GitCommandSnapshot {
    exit_code: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

const TARGET_GIT_OPERATION_SENTINELS: [&str; 16] = [
    "AUTO_MERGE",
    "BISECT_LOG",
    "BISECT_START",
    "CHERRY_PICK_HEAD",
    "MERGE_AUTOSTASH",
    "MERGE_HEAD",
    "MERGE_MODE",
    "MERGE_MSG",
    "MERGE_RR",
    "REBASE_HEAD",
    "REVERT_HEAD",
    "SQUASH_MSG",
    "index.lock",
    "rebase-apply",
    "rebase-merge",
    "sequencer",
];

#[derive(Debug, PartialEq, Eq)]
enum GitStateEntry {
    Absent,
    File(Vec<u8>),
    Directory(BTreeMap<PathBuf, GitStateEntry>),
    Symlink(PathBuf),
    Other,
}

fn target_snapshot(fixture: &Fixture) -> TargetSnapshot {
    TargetSnapshot {
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
        status: git_command_snapshot(
            &fixture.repository,
            &["status", "--porcelain=v2", "-z", "--untracked-files=all"],
        ),
        git_state: snapshot_target_git_state(&git_directory(fixture)),
        worktree: snapshot_target_worktree(&fixture.repository),
    }
}

fn snapshot_target_git_state(git_directory: &Path) -> BTreeMap<PathBuf, GitStateEntry> {
    TARGET_GIT_OPERATION_SENTINELS
        .into_iter()
        .map(|name| {
            let relative = PathBuf::from(name);
            let state = snapshot_git_state_entry(&git_directory.join(&relative));
            (relative, state)
        })
        .collect()
}

fn snapshot_git_state_entry(path: &Path) -> GitStateEntry {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return GitStateEntry::Absent;
        }
        Err(error) => panic!("read target Git state metadata: {error}"),
    };
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        return GitStateEntry::Symlink(std::fs::read_link(path).unwrap());
    }
    if file_type.is_file() {
        return GitStateEntry::File(std::fs::read(path).unwrap());
    }
    if !file_type.is_dir() {
        return GitStateEntry::Other;
    }

    // `symlink_metadata` above proves this directory is not a symlink. Each
    // child repeats that check, so the snapshot never follows a sentinel link.
    let mut entries = BTreeMap::new();
    for child in std::fs::read_dir(path).unwrap() {
        let child = child.unwrap();
        entries.insert(
            PathBuf::from(child.file_name()),
            snapshot_git_state_entry(&child.path()),
        );
    }
    GitStateEntry::Directory(entries)
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

fn git_command_output(repository: &std::path::Path, arguments: &[&str]) -> Vec<u8> {
    let output = std::process::Command::new("git")
        .args(["-C", repository.to_str().unwrap()])
        .args(arguments)
        .output()
        .unwrap();
    assert!(output.status.success());
    output.stdout
}

fn git_command_snapshot(repository: &Path, arguments: &[&str]) -> GitCommandSnapshot {
    let output = std::process::Command::new("git")
        .args(["-C", repository.to_str().unwrap()])
        .args(arguments)
        .output()
        .unwrap();
    GitCommandSnapshot {
        exit_code: output.status.code(),
        stdout: output.stdout,
        stderr: output.stderr,
    }
}
