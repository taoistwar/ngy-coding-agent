use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Duration;

use coding_agent_runtime::{
    CargoToolLimits, CargoTools, GitRunStatus, GitToolLimits, ProcessLimits, RuntimeSession,
    RuntimeSessionLimits, ToolchainPaths, WorktreeArtifactState, WorktreeError, WorktreeIdentity,
    WorktreeLimits, WorktreeObservation, WorktreeProvisioner, WorktreeReservation,
    discover_toolchain,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const REPOSITORY_ID: &str = "repository-1";
const TASK_ID: &str = "task-1";

#[test]
fn zero_is_not_a_valid_attempt_identity() {
    let error = WorktreeIdentity::try_new(REPOSITORY_ID, TASK_ID, 0).unwrap_err();
    assert!(matches!(error, WorktreeError::InvalidIdentity));
}

#[tokio::test]
async fn unborn_head_has_no_git_or_worktree_side_effect() {
    let fixture = Fixture::new(false).await;
    let identity = fixture.reserve(TASK_ID, 1);
    let target = fixture.target(&identity);
    let refs_before = git_stdout(
        &fixture.repository,
        &["for-each-ref", "--format=%(refname)", "refs/heads"],
    );
    let admin_before = worktree_admin_entries(&fixture.repository);

    let error = fixture
        .provisioner()
        .prepare(identity.clone(), CancellationToken::new())
        .await
        .unwrap_err();

    assert!(matches!(error, WorktreeError::UnbornHead), "{error:?}");
    assert_eq!(error.code(), "GIT_HEAD_UNBORN");
    assert!(!target.exists());
    assert_eq!(
        git_stdout(
            &fixture.repository,
            &["for-each-ref", "--format=%(refname)", "refs/heads"],
        ),
        refs_before
    );
    assert_eq!(worktree_admin_entries(&fixture.repository), admin_before);
    assert!(!branch_exists(&fixture.repository, &identity.branch_name()));
}

#[tokio::test]
async fn redirected_artifact_parent_is_rejected_without_creating_anything_outside_root() {
    let fixture = Fixture::new(true).await;
    let provisioner = fixture.provisioner();
    let identity = WorktreeIdentity::try_new(REPOSITORY_ID, TASK_ID, 1).unwrap();
    let reservation = provisioner
        .prepare(identity.clone(), CancellationToken::new())
        .await
        .unwrap();
    let outside = fixture.root.join("outside-artifact-root");
    std::fs::create_dir(&outside).unwrap();
    create_directory_redirect(&outside, &fixture.artifact_root.join("worktrees")).unwrap();
    let refs_before = git_stdout(
        &fixture.repository,
        &["for-each-ref", "--format=%(refname)", "refs/heads"],
    );
    let admin_before = worktree_admin_entries(&fixture.repository);

    let error = provisioner
        .provision_reserved(reservation, CancellationToken::new())
        .await
        .unwrap_err();

    assert!(
        matches!(error.cause(), WorktreeError::ArtifactPathInvalid),
        "{error:?}"
    );
    assert_eq!(error.code(), "WORKTREE_PATH_ESCAPE");
    assert_eq!(error.artifact_state(), WorktreeArtifactState::Absent);
    assert_eq!(
        std::fs::read_dir(&outside).unwrap().count(),
        0,
        "artifact path validation wrote through a directory redirect"
    );
    assert_eq!(
        git_stdout(
            &fixture.repository,
            &["for-each-ref", "--format=%(refname)", "refs/heads"],
        ),
        refs_before
    );
    assert_eq!(worktree_admin_entries(&fixture.repository), admin_before);
    assert!(!branch_exists(&fixture.repository, &identity.branch_name()));
}

#[tokio::test]
async fn prepare_is_deterministic_and_has_no_artifact_or_git_side_effect() {
    let fixture = Fixture::new(true).await;
    let provisioner = fixture.provisioner();
    let identity = WorktreeIdentity::try_new(REPOSITORY_ID, "prepared-task", 7).unwrap();
    let target = fixture.target(&identity);
    let base_commit = git_line(&fixture.repository, &["rev-parse", "HEAD"]);
    let refs_before = git_stdout(
        &fixture.repository,
        &["for-each-ref", "--format=%(refname)", "refs/heads"],
    );
    let admin_before = worktree_admin_entries(&fixture.repository);
    assert!(!fixture.artifact_root.join("worktrees").exists());

    let first = provisioner
        .prepare(identity.clone(), CancellationToken::new())
        .await
        .unwrap();
    let second = provisioner
        .prepare(identity.clone(), CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(first, second);
    assert_eq!(first.identity(), &identity);
    assert_eq!(first.base_commit(), base_commit);
    assert_eq!(first.branch_name(), identity.branch_name());
    assert_eq!(first.worktree_path(), target);
    assert!(!target.exists());
    assert!(
        !fixture.artifact_root.join("worktrees").exists(),
        "prepare created an artifact directory"
    );
    assert_eq!(
        git_stdout(
            &fixture.repository,
            &["for-each-ref", "--format=%(refname)", "refs/heads"],
        ),
        refs_before
    );
    assert_eq!(worktree_admin_entries(&fixture.repository), admin_before);
    assert!(!branch_exists(&fixture.repository, first.branch_name()));

    let restored = provisioner
        .restore_reservation(
            identity.clone(),
            first.base_commit(),
            first.branch_name(),
            first.worktree_path(),
        )
        .unwrap();
    assert_eq!(restored, first);
    let provisioned = provisioner
        .provision_reserved(first, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(provisioned.identity(), &identity);
    assert_eq!(provisioned.base_commit(), base_commit);
    assert_eq!(provisioned.branch_name(), identity.branch_name());
    assert_eq!(provisioned.worktree_path(), target);
    assert!(target.is_dir());
    assert!(
        provisioned
            .target_directory_matches(target.join("nested/rust/target"))
            .unwrap(),
        "returned target-directory capability has the wrong identity"
    );
    let cargo = CargoTools::from_trusted_capabilities(
        &fixture.toolchain,
        provisioned.cargo_workspace(),
        provisioned.target_directory(),
        &fixture.runtime_directory,
        process_limits(),
        CargoToolLimits::try_new(Duration::from_secs(10), 8, 32, 128).unwrap(),
    )
    .unwrap();
    let catalog = cargo.catalog(CancellationToken::new()).await.unwrap();
    assert_eq!(catalog.packages()[0].name(), "worktree_fixture");
    assert_eq!(
        catalog.packages()[0].integration_tests(),
        &["workspace_smoke".to_owned()]
    );
    assert_eq!(provisioned.cargo_catalog(), &catalog);
    let repository_context = provisioned.repository_context();
    assert_eq!(
        repository_context,
        "Cargo workspace selectors:\npackage=worktree_fixture; integration_tests=workspace_smoke\n"
    );
    assert!(!repository_context.contains(&fixture.root.to_string_lossy().to_string()));
    let session = RuntimeSession::from_provisioned_worktree(
        &provisioned,
        &fixture.toolchain,
        &fixture.runtime_directory,
        RuntimeSessionLimits::project_2_defaults(),
    )
    .unwrap();
    assert_eq!(session.cargo_catalog(), &catalog);
    assert_eq!(session.repository_context(), repository_context);
    assert_eq!(
        provisioner
            .observe(&restored, CancellationToken::new())
            .await,
        WorktreeObservation::Ready
    );
    let reopened = provisioner
        .open_ready(&restored, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(reopened.identity(), &identity);
    assert_eq!(reopened.base_commit(), base_commit);
    assert_eq!(reopened.branch_name(), identity.branch_name());
    assert_eq!(reopened.worktree_path(), target);
    assert_eq!(reopened.cargo_catalog(), &catalog);
    assert_eq!(reopened.repository_context(), repository_context);
    assert!(
        reopened
            .target_directory_matches(target.join("nested/rust/target"))
            .unwrap(),
        "recovered target-directory capability has the wrong identity"
    );
}

#[tokio::test]
async fn observation_and_recovery_never_recreate_an_ignored_missing_cargo_lock() {
    let fixture = Fixture::new(true).await;
    std::fs::remove_file(fixture.cargo_workspace.join("Cargo.lock")).unwrap();
    std::fs::write(
        fixture.cargo_workspace.join("Cargo.toml"),
        b"[workspace]\n\n[package]\nname = \"worktree_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[dependencies]\nfixture_dependency = \"0.1.0\"\n",
    )
    .unwrap();
    std::fs::create_dir_all(fixture.cargo_workspace.join(".cargo")).unwrap();
    std::fs::write(
        fixture.cargo_workspace.join(".cargo/config.toml"),
        b"[source.crates-io]\nreplace-with = \"vendored-sources\"\n\n[source.vendored-sources]\ndirectory = \"vendor\"\n",
    )
    .unwrap();
    let dependency_root = fixture
        .cargo_workspace
        .join("vendor/fixture_dependency-0.1.0");
    std::fs::create_dir_all(dependency_root.join("src")).unwrap();
    let dependency_manifest =
        b"[package]\nname = \"fixture_dependency\"\nversion = \"0.1.0\"\nedition = \"2024\"\n";
    let dependency_source = b"pub fn dependency() {}\n";
    std::fs::write(dependency_root.join("Cargo.toml"), dependency_manifest).unwrap();
    std::fs::write(dependency_root.join("src/lib.rs"), dependency_source).unwrap();
    let checksums = serde_json::json!({
        "files": {
            "Cargo.toml": sha256_hex(dependency_manifest),
            "src/lib.rs": sha256_hex(dependency_source),
        },
        "package": serde_json::Value::Null,
    });
    std::fs::write(
        dependency_root.join(".cargo-checksum.json"),
        serde_json::to_vec(&checksums).unwrap(),
    )
    .unwrap();
    std::fs::write(
        fixture.cargo_workspace.join(".gitignore"),
        b"Cargo.lock\ntarget/\n",
    )
    .unwrap();
    git_ok(&fixture.repository, &["add", "--all"]);
    git_ok(
        &fixture.repository,
        &[
            "commit",
            "--quiet",
            "--no-gpg-sign",
            "-m",
            "ignore generated Cargo state",
        ],
    );

    let provisioner = fixture.provisioner();
    let identity = fixture.reserve("read-only-recovery", 1);
    let reservation = provisioner
        .prepare(identity, CancellationToken::new())
        .await
        .unwrap();
    let provisioned = provisioner
        .provision_reserved(reservation.clone(), CancellationToken::new())
        .await
        .unwrap();
    let lockfile = provisioned.cargo_workspace_path().join("Cargo.lock");
    std::fs::write(&lockfile, b"ignored lockfile sentinel\n").unwrap();
    std::fs::remove_file(&lockfile).unwrap();
    let before = snapshot_tree_bytes(provisioned.worktree_path());

    assert_eq!(
        provisioner
            .observe(&reservation, CancellationToken::new())
            .await,
        WorktreeObservation::Ready
    );
    assert_eq!(snapshot_tree_bytes(provisioned.worktree_path()), before);
    provisioner
        .open_ready(&reservation, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(snapshot_tree_bytes(provisioned.worktree_path()), before);
    assert!(
        !lockfile.exists(),
        "recovery recreated an ignored Cargo.lock"
    );
}

#[tokio::test]
async fn reservation_cannot_be_replayed_against_another_source_repository() {
    let fixture = Fixture::new(true).await;
    let identity = fixture.reserve("cross-source", 1);
    let target = fixture.target(&identity);
    let source = fixture.provisioner();
    let reservation = source
        .prepare(identity.clone(), CancellationToken::new())
        .await
        .unwrap();

    let other_repository = fixture.root.join("other-repository");
    let other_workspace = other_repository.join("nested/rust");
    std::fs::create_dir_all(other_workspace.join("src")).unwrap();
    git_ok(&other_repository, &["init", "--quiet"]);
    git_ok(
        &other_repository,
        &["config", "user.name", "Other Worktree Test"],
    );
    git_ok(
        &other_repository,
        &["config", "user.email", "other-worktree@example.invalid"],
    );
    std::fs::write(
        other_workspace.join("Cargo.toml"),
        b"[package]\nname = \"other_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .unwrap();
    std::fs::write(
        other_workspace.join("src/lib.rs"),
        b"pub fn belongs_to_other_repository() {}\n",
    )
    .unwrap();
    git_ok(&other_repository, &["add", "--all"]);
    git_ok(
        &other_repository,
        &["commit", "--quiet", "--no-gpg-sign", "-m", "other"],
    );
    let other = WorktreeProvisioner::from_trusted_paths(
        &fixture.toolchain,
        &other_repository,
        &other_workspace,
        &fixture.artifact_root,
        &fixture.runtime_directory,
        process_limits(),
        WorktreeLimits::try_new(Duration::from_secs(15)).unwrap(),
    )
    .unwrap();
    let source_admin_before = worktree_admin_entries(&fixture.repository);
    let other_admin_before = worktree_admin_entries(&other_repository);

    let error = other
        .provision_reserved(reservation, CancellationToken::new())
        .await
        .unwrap_err();

    assert!(matches!(error.cause(), WorktreeError::InvalidReservation));
    assert_eq!(error.code(), "WORKTREE_STATE_INCONSISTENT");
    assert_eq!(error.artifact_state(), WorktreeArtifactState::Absent);
    assert!(!target.exists());
    assert!(!branch_exists(&fixture.repository, &identity.branch_name()));
    assert!(!branch_exists(&other_repository, &identity.branch_name()));
    assert_eq!(
        worktree_admin_entries(&fixture.repository),
        source_admin_before
    );
    assert_eq!(
        worktree_admin_entries(&other_repository),
        other_admin_before
    );
}

#[tokio::test]
async fn observation_classifies_every_creation_crash_point_without_deleting_the_scene() {
    let fixture = Fixture::new(true).await;
    let provisioner = fixture.provisioner();

    let absent = prepare_crash_reservation(&fixture, &provisioner, 1).await;
    let absent_admin = worktree_admin_entries(&fixture.repository);
    assert_eq!(
        provisioner.observe(&absent, CancellationToken::new()).await,
        WorktreeObservation::Absent
    );
    assert!(!absent.worktree_path().exists());
    assert!(!branch_exists(&fixture.repository, absent.branch_name()));
    assert_eq!(worktree_admin_entries(&fixture.repository), absent_admin);

    let branch_only = prepare_crash_reservation(&fixture, &provisioner, 2).await;
    git_ok(
        &fixture.repository,
        &[
            "branch",
            branch_only.branch_name(),
            branch_only.base_commit(),
        ],
    );
    let branch_only_oid = git_line(
        &fixture.repository,
        &[
            "rev-parse",
            &format!("refs/heads/{}", branch_only.branch_name()),
        ],
    );
    let branch_only_admin = worktree_admin_entries(&fixture.repository);
    assert_eq!(
        provisioner
            .observe(&branch_only, CancellationToken::new())
            .await,
        WorktreeObservation::BranchOnly
    );
    assert_eq!(
        git_line(
            &fixture.repository,
            &[
                "rev-parse",
                &format!("refs/heads/{}", branch_only.branch_name()),
            ],
        ),
        branch_only_oid
    );
    assert!(!branch_only.worktree_path().exists());
    assert_eq!(
        worktree_admin_entries(&fixture.repository),
        branch_only_admin
    );

    let administrative = prepare_crash_reservation(&fixture, &provisioner, 3).await;
    let administrative_git_dir = raw_worktree_add(&fixture.repository, &administrative);
    let administrative_pointer =
        std::fs::read(administrative.worktree_path().join(".git")).unwrap();
    let administrative_head = std::fs::read(administrative_git_dir.join("HEAD")).unwrap();
    let administrative_locked = std::fs::read(administrative_git_dir.join("locked")).unwrap();
    assert_eq!(
        provisioner
            .observe(&administrative, CancellationToken::new())
            .await,
        WorktreeObservation::AdministrativeCreated
    );
    assert_eq!(
        std::fs::read(administrative.worktree_path().join(".git")).unwrap(),
        administrative_pointer
    );
    assert_eq!(
        std::fs::read(administrative_git_dir.join("HEAD")).unwrap(),
        administrative_head
    );
    assert_eq!(
        std::fs::read(administrative_git_dir.join("locked")).unwrap(),
        administrative_locked
    );

    let partial = prepare_crash_reservation(&fixture, &provisioner, 4).await;
    let partial_git_dir = raw_worktree_add(&fixture.repository, &partial);
    let partial_sentinel = partial.worktree_path().join("partial-checkout-sentinel");
    std::fs::write(&partial_sentinel, b"preserve partial checkout\n").unwrap();
    assert_eq!(
        provisioner
            .observe(&partial, CancellationToken::new())
            .await,
        WorktreeObservation::CheckoutPartial
    );
    assert_eq!(
        std::fs::read(&partial_sentinel).unwrap(),
        b"preserve partial checkout\n"
    );
    assert!(partial_git_dir.is_dir());
    assert!(branch_exists(&fixture.repository, partial.branch_name()));

    let ready = prepare_crash_reservation(&fixture, &provisioner, 5).await;
    let ready_git_dir = raw_worktree_add(&fixture.repository, &ready);
    raw_worktree_reset(&ready);
    std::fs::create_dir_all(ready.worktree_path().join("nested/rust/target")).unwrap();
    assert_eq!(
        provisioner.observe(&ready, CancellationToken::new()).await,
        WorktreeObservation::Ready
    );
    assert!(
        ready
            .worktree_path()
            .join("nested/rust/Cargo.toml")
            .is_file()
    );
    assert!(ready.worktree_path().join("nested/rust/target").is_dir());
    assert!(ready_git_dir.is_dir());
    assert!(branch_exists(&fixture.repository, ready.branch_name()));

    let mismatch = prepare_crash_reservation(&fixture, &provisioner, 6).await;
    let mismatch_git_dir = raw_worktree_add(&fixture.repository, &mismatch);
    std::fs::write(mismatch_git_dir.join("locked"), b"foreign-owner\n").unwrap();
    let mismatch_pointer = std::fs::read(mismatch.worktree_path().join(".git")).unwrap();
    assert_eq!(
        provisioner
            .observe(&mismatch, CancellationToken::new())
            .await,
        WorktreeObservation::Inconsistent
    );
    assert_eq!(
        std::fs::read(mismatch_git_dir.join("locked")).unwrap(),
        b"foreign-owner\n"
    );
    assert_eq!(
        std::fs::read(mismatch.worktree_path().join(".git")).unwrap(),
        mismatch_pointer
    );
    assert!(branch_exists(&fixture.repository, mismatch.branch_name()));
}

#[tokio::test]
async fn missing_or_invalid_committed_cargo_manifest_is_never_observed_as_ready() {
    let fixture = Fixture::new(true).await;
    let provisioner = fixture.provisioner();

    std::fs::remove_file(fixture.cargo_workspace.join("Cargo.toml")).unwrap();
    git_ok(&fixture.repository, &["add", "--all"]);
    git_ok(
        &fixture.repository,
        &[
            "commit",
            "--quiet",
            "--no-gpg-sign",
            "-m",
            "missing cargo manifest",
        ],
    );
    let missing = prepare_named_reservation(&fixture, &provisioner, "cargo-state", 1).await;
    let missing_git_dir = raw_worktree_add(&fixture.repository, &missing);
    raw_worktree_reset(&missing);
    std::fs::create_dir_all(missing.worktree_path().join("nested/rust/target")).unwrap();
    let missing_observation = provisioner
        .observe(&missing, CancellationToken::new())
        .await;
    assert_ne!(missing_observation, WorktreeObservation::Ready);
    assert!(missing.worktree_path().is_dir());
    assert!(missing_git_dir.is_dir());
    let missing_provision =
        prepare_named_reservation(&fixture, &provisioner, "cargo-state", 2).await;
    let missing_provision_observation = missing_provision.clone();
    let missing_error = provisioner
        .provision_reserved(missing_provision, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(missing_error.code(), "WORKTREE_CREATE_FAILED");
    assert_eq!(
        missing_error.artifact_state(),
        WorktreeArtifactState::Partial
    );
    assert_eq!(
        missing_error.observation(),
        WorktreeObservation::CheckoutPartial
    );
    assert!(missing_provision_observation.worktree_path().is_dir());
    assert!(
        linked_admin_directory(missing_provision_observation.worktree_path()).is_dir(),
        "failed missing-manifest provision removed common admin metadata"
    );
    assert!(branch_exists(
        &fixture.repository,
        missing_provision_observation.branch_name()
    ));
    assert_ne!(
        provisioner
            .observe(&missing_provision_observation, CancellationToken::new())
            .await,
        WorktreeObservation::Ready
    );

    std::fs::write(
        fixture.cargo_workspace.join("Cargo.toml"),
        b"this is not valid Cargo TOML = [\n",
    )
    .unwrap();
    git_ok(&fixture.repository, &["add", "--all"]);
    git_ok(
        &fixture.repository,
        &[
            "commit",
            "--quiet",
            "--no-gpg-sign",
            "-m",
            "invalid cargo manifest",
        ],
    );
    let invalid = prepare_named_reservation(&fixture, &provisioner, "cargo-state", 3).await;
    let invalid_git_dir = raw_worktree_add(&fixture.repository, &invalid);
    raw_worktree_reset(&invalid);
    std::fs::create_dir_all(invalid.worktree_path().join("nested/rust/target")).unwrap();
    let invalid_observation = provisioner
        .observe(&invalid, CancellationToken::new())
        .await;
    assert_ne!(invalid_observation, WorktreeObservation::Ready);
    assert_eq!(
        std::fs::read(invalid.worktree_path().join("nested/rust/Cargo.toml")).unwrap(),
        b"this is not valid Cargo TOML = [\n"
    );
    assert!(invalid_git_dir.is_dir());
    let invalid_provision =
        prepare_named_reservation(&fixture, &provisioner, "cargo-state", 4).await;
    let invalid_provision_observation = invalid_provision.clone();
    let invalid_error = provisioner
        .provision_reserved(invalid_provision, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(invalid_error.code(), "WORKTREE_CREATE_FAILED");
    assert_eq!(
        invalid_error.artifact_state(),
        WorktreeArtifactState::Partial
    );
    assert_eq!(
        invalid_error.observation(),
        WorktreeObservation::CheckoutPartial
    );
    assert!(invalid_provision_observation.worktree_path().is_dir());
    assert!(
        linked_admin_directory(invalid_provision_observation.worktree_path()).is_dir(),
        "failed invalid-manifest provision removed common admin metadata"
    );
    assert!(branch_exists(
        &fixture.repository,
        invalid_provision_observation.branch_name()
    ));
    assert_ne!(
        provisioner
            .observe(&invalid_provision_observation, CancellationToken::new())
            .await,
        WorktreeObservation::Ready
    );
}

#[tokio::test]
async fn committed_head_isolated_from_dirty_original_and_attempts_and_conflicts_do_not_overwrite() {
    let fixture = Fixture::new(true).await;
    std::fs::write(fixture.repository.join("staged.txt"), b"staged dirty\n").unwrap();
    git_ok(&fixture.repository, &["add", "--", "staged.txt"]);
    std::fs::write(fixture.repository.join("unstaged.txt"), b"unstaged dirty\n").unwrap();
    std::fs::write(
        fixture.repository.join("untracked.txt"),
        b"untracked dirty\n",
    )
    .unwrap();

    let original_status = git_stdout(
        &fixture.repository,
        &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
    );
    let original_staged_worktree = std::fs::read(fixture.repository.join("staged.txt")).unwrap();
    let original_staged_index = git_stdout(&fixture.repository, &["show", ":staged.txt"]);
    let original_unstaged = std::fs::read(fixture.repository.join("unstaged.txt")).unwrap();
    let original_untracked = std::fs::read(fixture.repository.join("untracked.txt")).unwrap();
    let base_commit = git_line(&fixture.repository, &["rev-parse", "HEAD"]);

    let first_identity = fixture.reserve(TASK_ID, 1);
    let second_identity = fixture.reserve(TASK_ID, 2);
    let provisioner = fixture.provisioner();
    let first_reservation = provisioner
        .prepare(first_identity.clone(), CancellationToken::new())
        .await
        .unwrap();
    let first = provisioner
        .provision_reserved(first_reservation, CancellationToken::new())
        .await
        .unwrap();
    let second_reservation = provisioner
        .prepare(second_identity.clone(), CancellationToken::new())
        .await
        .unwrap();
    let second = provisioner
        .provision_reserved(second_reservation, CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(first.base_commit(), base_commit);
    assert_eq!(second.base_commit(), base_commit);
    assert_eq!(first.branch_name(), first_identity.branch_name());
    assert_eq!(second.branch_name(), second_identity.branch_name());
    assert_ne!(first.branch_name(), second.branch_name());
    assert_ne!(first.worktree_path(), second.worktree_path());
    assert_eq!(first.worktree_path(), fixture.target(&first_identity));
    assert_eq!(second.worktree_path(), fixture.target(&second_identity));
    assert_eq!(
        first.cargo_workspace_path(),
        first.worktree_path().join("nested/rust")
    );
    assert_eq!(
        second.cargo_workspace_path(),
        second.worktree_path().join("nested/rust")
    );
    assert!(first.cargo_workspace_path().join("Cargo.toml").is_file());
    assert!(second.cargo_workspace_path().join("src/lib.rs").is_file());

    for worktree in [first.worktree_path(), second.worktree_path()] {
        assert_eq!(
            std::fs::read(worktree.join("staged.txt")).unwrap(),
            b"staged base\n"
        );
        assert_eq!(
            std::fs::read(worktree.join("unstaged.txt")).unwrap(),
            b"unstaged base\n"
        );
        assert!(!worktree.join("untracked.txt").exists());
        assert!(
            git_stdout(
                worktree,
                &["status", "--porcelain=v1", "-z", "--untracked-files=all"]
            )
            .is_empty()
        );
    }

    std::fs::write(first.worktree_path().join("attempt-only.txt"), b"first\n").unwrap();
    assert!(!second.worktree_path().join("attempt-only.txt").exists());
    assert_eq!(
        git_line(
            &fixture.repository,
            &["rev-parse", &format!("refs/heads/{}", first.branch_name())]
        ),
        base_commit
    );
    assert_eq!(
        git_line(
            &fixture.repository,
            &["rev-parse", &format!("refs/heads/{}", second.branch_name())]
        ),
        base_commit
    );

    assert_eq!(
        git_stdout(
            &fixture.repository,
            &["status", "--porcelain=v1", "-z", "--untracked-files=all"]
        ),
        original_status
    );
    assert_eq!(
        std::fs::read(fixture.repository.join("staged.txt")).unwrap(),
        original_staged_worktree
    );
    assert_eq!(
        git_stdout(&fixture.repository, &["show", ":staged.txt"]),
        original_staged_index
    );
    assert_eq!(
        std::fs::read(fixture.repository.join("unstaged.txt")).unwrap(),
        original_unstaged
    );
    assert_eq!(
        std::fs::read(fixture.repository.join("untracked.txt")).unwrap(),
        original_untracked
    );

    let branch_conflict = fixture.reserve(TASK_ID, 3);
    git_ok(
        &fixture.repository,
        &["branch", &branch_conflict.branch_name(), &base_commit],
    );
    let branch_error = provisioner
        .prepare(branch_conflict.clone(), CancellationToken::new())
        .await
        .unwrap_err();
    assert!(
        matches!(branch_error, WorktreeError::BranchConflict),
        "{branch_error:?}"
    );
    assert_eq!(branch_error.code(), "WORKTREE_CREATE_FAILED");
    assert!(!fixture.target(&branch_conflict).exists());
    assert_eq!(
        git_line(
            &fixture.repository,
            &[
                "rev-parse",
                &format!("refs/heads/{}", branch_conflict.branch_name())
            ]
        ),
        base_commit
    );

    let path_conflict = fixture.reserve(TASK_ID, 4);
    let conflicting_target = fixture.target(&path_conflict);
    std::fs::create_dir(&conflicting_target).unwrap();
    std::fs::write(
        conflicting_target.join("sentinel.txt"),
        b"do not overwrite\n",
    )
    .unwrap();
    let path_error = provisioner
        .prepare(path_conflict.clone(), CancellationToken::new())
        .await
        .unwrap_err();
    assert!(
        matches!(path_error, WorktreeError::DestinationConflict),
        "{path_error:?}"
    );
    assert_eq!(path_error.code(), "WORKTREE_CREATE_FAILED");
    assert_eq!(
        std::fs::read(conflicting_target.join("sentinel.txt")).unwrap(),
        b"do not overwrite\n"
    );
    assert!(!branch_exists(
        &fixture.repository,
        &path_conflict.branch_name()
    ));
}

#[tokio::test]
async fn executable_filters_includes_and_worktree_config_are_rejected_before_checkout() {
    let fixture = Fixture::new(true).await;
    let sentinel = fixture.root.join("unsafe-config-ran");
    let command = shell_probe_command(&sentinel);
    let provisioner = fixture.provisioner();

    git_ok(
        &fixture.repository,
        &["config", "--local", "filter.codex-probe.smudge", &command],
    );
    assert_unsafe_before_checkout(&fixture, &provisioner, TASK_ID, 1, &sentinel).await;
    git_ok(
        &fixture.repository,
        &[
            "config",
            "--local",
            "--unset-all",
            "filter.codex-probe.smudge",
        ],
    );

    let included_config = fixture.root.join("included-config");
    std::fs::write(
        &included_config,
        format!("[filter \"codex-probe\"]\n\tsmudge = {command}\n"),
    )
    .unwrap();
    let include_key = "includeIf.gitdir:**.path";
    let included_config_value = path_for_git(&included_config);
    git_ok(
        &fixture.repository,
        &["config", "--local", include_key, &included_config_value],
    );
    assert_unsafe_before_checkout(&fixture, &provisioner, TASK_ID, 2, &sentinel).await;
    git_ok(
        &fixture.repository,
        &["config", "--local", "--unset-all", include_key],
    );

    std::fs::write(
        fixture.repository.join(".git/config.worktree"),
        format!("[filter \"codex-probe\"]\n\tsmudge = {command}\n"),
    )
    .unwrap();
    git_ok(
        &fixture.repository,
        &["config", "--local", "extensions.worktreeConfig", "true"],
    );
    assert_unsafe_before_checkout(&fixture, &provisioner, TASK_ID, 3, &sentinel).await;
}

#[tokio::test]
async fn hooks_fsmonitor_external_diff_and_textconv_never_run_and_linked_git_pointer_is_not_authority()
 {
    let fixture = Fixture::new(true).await;
    let hook_sentinel = fixture.root.join("hook-ran");
    let reference_transaction_sentinel = fixture.root.join("reference-transaction-ran");
    let post_index_change_sentinel = fixture.root.join("post-index-change-ran");
    let fsmonitor_sentinel = fixture.root.join("fsmonitor-ran");
    let external_diff_sentinel = fixture.root.join("external-diff-ran");
    let textconv_sentinel = fixture.root.join("textconv-ran");

    install_hook(&fixture.repository, "post-checkout", &hook_sentinel);
    install_hook(
        &fixture.repository,
        "reference-transaction",
        &reference_transaction_sentinel,
    );
    install_hook(
        &fixture.repository,
        "post-index-change",
        &post_index_change_sentinel,
    );
    git_ok(
        &fixture.repository,
        &[
            "config",
            "--local",
            "core.fsmonitor",
            &shell_probe_command(&fsmonitor_sentinel),
        ],
    );
    git_ok(
        &fixture.repository,
        &[
            "config",
            "--local",
            "diff.external",
            &shell_probe_command(&external_diff_sentinel),
        ],
    );
    git_ok(
        &fixture.repository,
        &[
            "config",
            "--local",
            "diff.codex-probe.textconv",
            &shell_probe_command(&textconv_sentinel),
        ],
    );

    let identity = fixture.reserve(TASK_ID, 1);
    let provisioner = fixture.provisioner();
    let reservation = provisioner
        .prepare(identity, CancellationToken::new())
        .await
        .unwrap();
    let provisioned = provisioner
        .provision_reserved(reservation, CancellationToken::new())
        .await
        .unwrap();
    assert_probe_sentinels_absent([
        &hook_sentinel,
        &reference_transaction_sentinel,
        &post_index_change_sentinel,
        &fsmonitor_sentinel,
        &external_diff_sentinel,
        &textconv_sentinel,
    ]);

    std::fs::write(
        provisioned.worktree_path().join("tracked.txt"),
        b"changed in trusted worktree\n",
    )
    .unwrap();
    let tools = provisioned
        .bind_git_tools(
            &fixture.toolchain,
            &fixture.runtime_directory,
            process_limits(),
            GitToolLimits::try_new(Duration::from_secs(10), Duration::from_secs(10)).unwrap(),
        )
        .unwrap();

    let evil_repository = fixture.root.join("evil-repository");
    std::fs::create_dir(&evil_repository).unwrap();
    git_ok(&evil_repository, &["init", "--quiet"]);
    git_ok(&evil_repository, &["config", "user.name", "Worktree Test"]);
    git_ok(
        &evil_repository,
        &["config", "user.email", "worktree@example.invalid"],
    );
    std::fs::write(evil_repository.join("evil.txt"), b"evil\n").unwrap();
    git_ok(&evil_repository, &["add", "--", "evil.txt"]);
    git_ok(
        &evil_repository,
        &["commit", "--quiet", "--no-gpg-sign", "-m", "evil"],
    );

    let linked_pointer = provisioned.worktree_path().join(".git");
    std::fs::write(
        &linked_pointer,
        format!("gitdir: {}\n", path_for_git(&evil_repository.join(".git"))),
    )
    .unwrap();

    let status = tools.status(CancellationToken::new()).await.unwrap();
    assert_eq!(status.status, GitRunStatus::Succeeded, "{status:?}");
    let status_output = complete_stdout(&status.command.stdout);
    assert_contains(&status_output, b"tracked.txt");
    assert!(
        !status_output
            .windows(b"evil.txt".len())
            .any(|part| part == b"evil.txt")
    );

    let diff = tools.diff(CancellationToken::new()).await.unwrap();
    assert_eq!(diff.status, GitRunStatus::Succeeded, "{diff:?}");
    let diff_output = complete_stdout(&diff.command.stdout);
    assert_contains(&diff_output, b"+changed in trusted worktree");
    assert!(
        !diff_output
            .windows(b"evil.txt".len())
            .any(|part| part == b"evil.txt")
    );
    assert_probe_sentinels_absent([
        &hook_sentinel,
        &reference_transaction_sentinel,
        &post_index_change_sentinel,
        &fsmonitor_sentinel,
        &external_diff_sentinel,
        &textconv_sentinel,
    ]);
    assert_contains(
        &std::fs::read(&linked_pointer).unwrap(),
        path_for_git(&evil_repository.join(".git")).as_bytes(),
    );
}

async fn assert_unsafe_before_checkout(
    fixture: &Fixture,
    provisioner: &WorktreeProvisioner,
    task_id: &str,
    attempt: u32,
    sentinel: &Path,
) {
    let identity = fixture.reserve(task_id, attempt);
    let target = fixture.target(&identity);
    let error = provisioner
        .prepare(identity.clone(), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.code(), "UNSAFE_GIT_CONFIGURATION", "{error:?}");
    assert!(!target.exists(), "unsafe config reached worktree creation");
    assert!(
        !branch_exists(&fixture.repository, &identity.branch_name()),
        "unsafe config reached branch creation"
    );
    assert!(!sentinel.exists(), "unsafe config command executed");
}

struct Fixture {
    _temporary: TempDir,
    root: PathBuf,
    runtime_directory: PathBuf,
    repository: PathBuf,
    cargo_workspace: PathBuf,
    artifact_root: PathBuf,
    toolchain: ToolchainPaths,
}

impl Fixture {
    async fn new(commit: bool) -> Self {
        let test_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target");
        std::fs::create_dir_all(&test_root).unwrap();
        let temporary = tempfile::Builder::new()
            .prefix("worktree-")
            .tempdir_in(test_root)
            .unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let runtime_directory = root.join("runtime");
        let repository = root.join("repository");
        let cargo_workspace = repository.join("nested/rust");
        let artifact_root = root.join("artifacts");
        for directory in [
            &runtime_directory,
            &cargo_workspace.join("src"),
            &cargo_workspace.join("tests"),
            &artifact_root,
        ] {
            std::fs::create_dir_all(directory).unwrap();
        }

        git_ok(&repository, &["init", "--quiet"]);
        git_ok(&repository, &["config", "user.name", "Worktree Test"]);
        git_ok(
            &repository,
            &["config", "user.email", "worktree@example.invalid"],
        );
        std::fs::write(repository.join("tracked.txt"), b"tracked base\n").unwrap();
        std::fs::write(repository.join("staged.txt"), b"staged base\n").unwrap();
        std::fs::write(repository.join("unstaged.txt"), b"unstaged base\n").unwrap();
        std::fs::write(
            repository.join(".gitattributes"),
            b"tracked.txt diff=codex-probe\ntracked.txt filter=codex-probe\n",
        )
        .unwrap();
        std::fs::write(
            cargo_workspace.join("Cargo.toml"),
            b"[workspace]\n\n[package]\nname = \"worktree_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
        )
        .unwrap();
        std::fs::write(
            cargo_workspace.join("src/lib.rs"),
            b"pub fn committed() -> bool { true }\n",
        )
        .unwrap();
        std::fs::write(
            cargo_workspace.join("tests/workspace_smoke.rs"),
            b"#[test]\nfn workspace_smoke() { assert!(worktree_fixture::committed()); }\n",
        )
        .unwrap();
        std::fs::write(
            cargo_workspace.join("Cargo.lock"),
            b"# This file is automatically @generated by Cargo.\nversion = 4\n\n[[package]]\nname = \"worktree_fixture\"\nversion = \"0.1.0\"\n",
        )
        .unwrap();
        if commit {
            git_ok(&repository, &["add", "--all"]);
            git_ok(
                &repository,
                &["commit", "--quiet", "--no-gpg-sign", "-m", "base"],
            );
        }

        let toolchain = discover_toolchain(
            &runtime_directory,
            Some(&concrete_rustc()),
            Some(&path_executable(if cfg!(windows) {
                "git.exe"
            } else {
                "git"
            })),
        )
        .await
        .unwrap();
        Self {
            _temporary: temporary,
            root,
            runtime_directory,
            repository,
            cargo_workspace,
            artifact_root,
            toolchain,
        }
    }

    fn reserve(&self, task_id: &str, attempt: u32) -> WorktreeIdentity {
        let identity = WorktreeIdentity::try_new(REPOSITORY_ID, task_id, attempt).unwrap();
        std::fs::create_dir_all(
            self.artifact_root
                .join(identity.relative_path())
                .parent()
                .unwrap(),
        )
        .unwrap();
        identity
    }

    fn target(&self, identity: &WorktreeIdentity) -> PathBuf {
        self.artifact_root.join(identity.relative_path())
    }

    fn provisioner(&self) -> WorktreeProvisioner {
        WorktreeProvisioner::from_trusted_paths(
            &self.toolchain,
            &self.repository,
            &self.cargo_workspace,
            &self.artifact_root,
            &self.runtime_directory,
            process_limits(),
            WorktreeLimits::try_new(Duration::from_secs(15)).unwrap(),
        )
        .unwrap()
    }
}

async fn prepare_crash_reservation(
    fixture: &Fixture,
    provisioner: &WorktreeProvisioner,
    attempt: u32,
) -> WorktreeReservation {
    prepare_named_reservation(fixture, provisioner, "crash-points", attempt).await
}

async fn prepare_named_reservation(
    fixture: &Fixture,
    provisioner: &WorktreeProvisioner,
    task_id: &str,
    attempt: u32,
) -> WorktreeReservation {
    let identity = fixture.reserve(task_id, attempt);
    provisioner
        .prepare(identity, CancellationToken::new())
        .await
        .unwrap()
}

fn process_limits() -> ProcessLimits {
    ProcessLimits::try_new(
        512 * 1024,
        512 * 1024,
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .unwrap()
}

fn snapshot_tree_bytes(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn visit(root: &Path, directory: &Path, entries: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
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

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn git_ok(repository: &Path, arguments: &[&str]) {
    let output = git_output(repository, arguments);
    assert!(
        output.status.success(),
        "git fixture command failed: git -C {} {}\nstdout: {}\nstderr: {}",
        repository.display(),
        arguments.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git_stdout(repository: &Path, arguments: &[&str]) -> Vec<u8> {
    let output = git_output(repository, arguments);
    assert!(
        output.status.success(),
        "git fixture command failed: git -C {} {}\nstderr: {}",
        repository.display(),
        arguments.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn git_line(repository: &Path, arguments: &[&str]) -> String {
    String::from_utf8(git_stdout(repository, arguments))
        .unwrap()
        .trim()
        .to_owned()
}

fn git_output(repository: &Path, arguments: &[&str]) -> Output {
    Command::new("git")
        .arg("--no-pager")
        .arg("-c")
        .arg(git_hooks_path_configuration())
        .arg("-c")
        .arg("commit.gpgSign=false")
        .arg("-C")
        .arg(repository)
        .args(arguments)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", null_device())
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .unwrap()
}

fn raw_worktree_add(repository: &Path, reservation: &WorktreeReservation) -> PathBuf {
    let before = worktree_admin_entries(repository);
    let target = path_for_git(reservation.worktree_path());
    git_ok(
        repository,
        &[
            "worktree",
            "add",
            "--no-checkout",
            "--lock",
            "--reason=codex-reserved",
            "--no-track",
            "--no-guess-remote",
            "--no-relative-paths",
            "-b",
            reservation.branch_name(),
            "--",
            &target,
            reservation.base_commit(),
        ],
    );
    let after = worktree_admin_entries(repository);
    let added = after
        .iter()
        .filter(|name| !before.contains(name))
        .collect::<Vec<_>>();
    assert_eq!(
        added.len(),
        1,
        "worktree add did not create one admin entry"
    );
    repository.join(".git/worktrees").join(added[0])
}

fn raw_worktree_reset(reservation: &WorktreeReservation) {
    git_ok(
        reservation.worktree_path(),
        &[
            "reset",
            "--hard",
            "--no-recurse-submodules",
            reservation.base_commit(),
        ],
    );
}

fn linked_admin_directory(worktree: &Path) -> PathBuf {
    let pointer = std::fs::read_to_string(worktree.join(".git")).unwrap();
    PathBuf::from(pointer.trim().strip_prefix("gitdir: ").unwrap())
}

#[cfg(unix)]
fn child_visible_path(path: &Path) -> PathBuf {
    path.to_owned()
}

#[cfg(windows)]
fn child_visible_path(path: &Path) -> PathBuf {
    use std::path::{Component, Prefix};

    let mut components = path.components();
    let Some(Component::Prefix(prefix)) = components.next() else {
        return path.to_owned();
    };
    let Prefix::VerbatimDisk(drive) = prefix.kind() else {
        return path.to_owned();
    };
    if !matches!(components.next(), Some(Component::RootDir)) {
        return path.to_owned();
    }
    let mut visible = PathBuf::from(format!("{}:\\", char::from(drive)));
    for component in components {
        if let Component::Normal(name) = component {
            visible.push(name);
        }
    }
    visible
}

fn branch_exists(repository: &Path, branch_name: &str) -> bool {
    git_output(
        repository,
        &[
            "show-ref",
            "--verify",
            "--quiet",
            &format!("refs/heads/{branch_name}"),
        ],
    )
    .status
    .success()
}

fn worktree_admin_entries(repository: &Path) -> Vec<String> {
    let directory = repository.join(".git/worktrees");
    let Ok(entries) = std::fs::read_dir(directory) else {
        return Vec::new();
    };
    let mut entries = entries
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    entries.sort();
    entries
}

fn install_hook(repository: &Path, name: &str, sentinel: &Path) {
    let hook = repository.join(".git/hooks").join(name);
    std::fs::write(
        &hook,
        format!(
            "#!/bin/sh\necho executed > {}\n",
            shell_quote(&path_for_git(sentinel))
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

fn shell_probe_command(sentinel: &Path) -> String {
    format!("echo executed > {}", shell_quote(&path_for_git(sentinel)))
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn path_for_git(path: &Path) -> String {
    child_visible_path(path)
        .to_string_lossy()
        .replace('\\', "/")
}

#[cfg(unix)]
fn create_directory_redirect(target: &Path, redirect: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, redirect)
}

#[cfg(windows)]
fn create_directory_redirect(target: &Path, redirect: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_dir(target, redirect)
}

fn assert_probe_sentinels_absent<const N: usize>(sentinels: [&Path; N]) {
    for sentinel in sentinels {
        assert!(!sentinel.exists(), "probe executed: {}", sentinel.display());
    }
}

fn complete_stdout(stream: &coding_agent_runtime::CapturedStream) -> Vec<u8> {
    assert!(stream.complete);
    assert!(!stream.truncated);
    let mut output = stream.head.clone();
    output.extend_from_slice(&stream.tail);
    output
}

fn assert_contains(haystack: &[u8], needle: &[u8]) {
    assert!(
        haystack.windows(needle.len()).any(|part| part == needle),
        "missing {:?} in {}",
        String::from_utf8_lossy(needle),
        String::from_utf8_lossy(haystack)
    );
}

fn concrete_rustc() -> PathBuf {
    let output = Command::new("rustc")
        .args(["--print", "sysroot"])
        .output()
        .unwrap();
    assert!(output.status.success());
    PathBuf::from(String::from_utf8(output.stdout).unwrap().trim())
        .join("bin")
        .join(if cfg!(windows) { "rustc.exe" } else { "rustc" })
        .canonicalize()
        .unwrap()
}

fn path_executable(name: &str) -> PathBuf {
    std::env::split_paths(&std::env::var_os("PATH").unwrap())
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .unwrap()
        .canonicalize()
        .unwrap()
}

#[cfg(windows)]
fn git_hooks_path_configuration() -> &'static str {
    "core.hooksPath=NUL"
}

#[cfg(unix)]
fn git_hooks_path_configuration() -> &'static str {
    "core.hooksPath=/dev/null"
}

#[cfg(windows)]
fn null_device() -> &'static str {
    "NUL"
}

#[cfg(unix)]
fn null_device() -> &'static str {
    "/dev/null"
}
