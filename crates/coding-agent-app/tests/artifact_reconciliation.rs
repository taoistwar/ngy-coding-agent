mod support;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use coding_agent_app::{
    AttemptArtifactObserver, RestartArtifactObservation, StoreWriterHandle,
    WorktreeArtifactObserver, reconcile_restart_artifacts,
};
use coding_agent_domain::{CanonicalPath, NewRepository};
use coding_agent_runtime::{
    ProcessLimits, WorktreeIdentity, WorktreeLimits, WorktreeProvisioner, discover_toolchain,
};
use coding_agent_store::{
    AttemptArtifactIdentity, AttemptArtifactState, RegisterRepositoryOutcome,
    ReserveAttemptArtifact, ReserveAttemptArtifactOutcome, Store, TaskAttemptArtifact,
};
use tokio_util::sync::CancellationToken;

struct ScriptedObserver {
    states: HashMap<String, RestartArtifactObservation>,
}

#[async_trait::async_trait]
impl AttemptArtifactObserver for ScriptedObserver {
    async fn observe(&self, artifact: &TaskAttemptArtifact) -> RestartArtifactObservation {
        self.states[&artifact.branch_name]
    }
}

struct DelayedObserver {
    delay: Duration,
    observation: RestartArtifactObservation,
}

#[async_trait::async_trait]
impl AttemptArtifactObserver for DelayedObserver {
    async fn observe(&self, _: &TaskAttemptArtifact) -> RestartArtifactObservation {
        tokio::time::sleep(self.delay).await;
        self.observation
    }
}

#[tokio::test]
async fn restart_marks_absent_partial_and_mismatched_inconsistent_but_valid_ready() {
    let fixture = support::writer_fixture().await;
    let cases = [
        ("absent", RestartArtifactObservation::Absent),
        ("valid", RestartArtifactObservation::Ready),
        ("partial", RestartArtifactObservation::Partial),
        ("mismatched", RestartArtifactObservation::Inconsistent),
    ];
    let mut states = HashMap::new();
    let mut task_ids = HashMap::new();
    for (name, observation) in cases {
        let task = fixture
            .writer
            .create_task(
                support::new_task(fixture.repository.id, name),
                support::deadline(),
            )
            .await
            .unwrap()
            .value
            .task()
            .clone();
        let reservation = reservation(&fixture.repository, &task, name);
        states.insert(reservation.branch_name.clone(), observation);
        task_ids.insert(name, task.id);
        fixture
            .writer
            .reserve_attempt_artifact(reservation, support::deadline())
            .await
            .unwrap();
    }

    let summary = reconcile_restart_artifacts(
        &fixture.store,
        &fixture.writer,
        &ScriptedObserver { states },
        Duration::from_secs(2),
    )
    .await
    .unwrap();

    assert_eq!(summary.examined, 4);
    assert_eq!(summary.marked_ready, 1);
    assert_eq!(summary.marked_inconsistent, 3);
    let valid = fixture
        .store
        .load_attempt_artifact(task_ids["valid"])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(valid.state, AttemptArtifactState::Ready);
    assert!(valid.failure_code.is_none());
    let absent = fixture
        .store
        .load_attempt_artifact(task_ids["absent"])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(absent.state, AttemptArtifactState::Inconsistent);
    assert_eq!(
        absent.failure_code.as_deref(),
        Some("WORKTREE_RESERVATION_ABANDONED")
    );
    for name in ["partial", "mismatched"] {
        let artifact = fixture
            .store
            .load_attempt_artifact(task_ids[name])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(artifact.state, AttemptArtifactState::Inconsistent);
        assert_eq!(
            artifact.failure_code.as_deref(),
            Some("WORKTREE_STATE_INCONSISTENT")
        );
    }
    assert!(
        fixture
            .store
            .list_reserved_attempt_artifacts()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn store_writer_deadline_starts_after_slow_artifact_observation() {
    let fixture = support::writer_fixture().await;
    let task = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "slow observation"),
            support::deadline(),
        )
        .await
        .unwrap()
        .value
        .task()
        .clone();
    fixture
        .writer
        .reserve_attempt_artifact(
            reservation(&fixture.repository, &task, "slow-observation"),
            support::deadline(),
        )
        .await
        .unwrap();

    let summary = reconcile_restart_artifacts(
        &fixture.store,
        &fixture.writer,
        &DelayedObserver {
            delay: Duration::from_secs(2),
            observation: RestartArtifactObservation::Ready,
        },
        Duration::from_secs(1),
    )
    .await
    .expect("observation time must not consume the StoreWriter budget");

    assert_eq!(summary.examined, 1);
    assert_eq!(summary.marked_ready, 1);
    assert_eq!(summary.marked_inconsistent, 0);
    assert_eq!(
        fixture
            .store
            .load_attempt_artifact(task.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        AttemptArtifactState::Ready
    );
}

#[tokio::test]
async fn same_run_identical_reservation_remains_reserved_for_safe_reentry() {
    let fixture = support::writer_fixture().await;
    let task = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "same run"),
            support::deadline(),
        )
        .await
        .unwrap()
        .value
        .task()
        .clone();
    let input = reservation(&fixture.repository, &task, "same-run");
    fixture
        .writer
        .reserve_attempt_artifact(input.clone(), support::deadline())
        .await
        .unwrap();
    let replay = fixture
        .writer
        .reserve_attempt_artifact(input, support::deadline())
        .await
        .unwrap();

    assert!(matches!(
        replay.value,
        ReserveAttemptArtifactOutcome::Existing(ref artifact)
            if artifact.state == AttemptArtifactState::Reserved
    ));
    assert_eq!(
        fixture
            .store
            .list_reserved_attempt_artifacts()
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn restart_reconciliation_observes_real_git_and_disk_state() {
    let test_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/reconcile-tests");
    std::fs::create_dir_all(&test_root).unwrap();
    let temporary = tempfile::Builder::new()
        .prefix("restart-real-")
        .tempdir_in(test_root)
        .unwrap();
    let root = temporary.path().canonicalize().unwrap();
    let repository_path = root.join("repository");
    let cargo_workspace = repository_path.join("nested/rust");
    let artifact_root = root.join("artifacts");
    let runtime_directory = root.join("runtime");
    for directory in [
        cargo_workspace.join("src"),
        artifact_root.clone(),
        runtime_directory.clone(),
    ] {
        std::fs::create_dir_all(directory).unwrap();
    }
    git(&repository_path, &["init", "--quiet"]);
    git(&repository_path, &["config", "user.name", "Reconcile Test"]);
    git(
        &repository_path,
        &["config", "user.email", "reconcile@example.invalid"],
    );
    std::fs::write(repository_path.join("tracked.txt"), b"first\n").unwrap();
    std::fs::write(
        cargo_workspace.join("Cargo.toml"),
        b"[workspace]\n\n[package]\nname = \"reconcile_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .unwrap();
    std::fs::write(
        cargo_workspace.join("Cargo.lock"),
        b"# This file is automatically @generated by Cargo.\nversion = 4\n\n[[package]]\nname = \"reconcile_fixture\"\nversion = \"0.1.0\"\n",
    )
    .unwrap();
    std::fs::write(cargo_workspace.join("src/lib.rs"), b"pub fn first() {}\n").unwrap();
    git(&repository_path, &["add", "--all"]);
    git(
        &repository_path,
        &["commit", "--quiet", "--no-gpg-sign", "-m", "first"],
    );
    std::fs::write(repository_path.join("tracked.txt"), b"second\n").unwrap();
    git(&repository_path, &["add", "--all"]);
    git(
        &repository_path,
        &["commit", "--quiet", "--no-gpg-sign", "-m", "second"],
    );

    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    let repository = match store
        .register_repository(NewRepository {
            selected_path: canonical(&repository_path),
            display_name: "restart-real".to_owned(),
            git_root: canonical(&repository_path),
            cargo_workspace_root: canonical(&cargo_workspace),
        })
        .await
        .unwrap()
    {
        RegisterRepositoryOutcome::Created(repository)
        | RegisterRepositoryOutcome::Existing(repository) => repository,
    };
    let writer = StoreWriterHandle::spawn(
        store.clone(),
        Arc::new(support::CountingWake::default()),
        16,
    );
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
    let provisioner = Arc::new(
        WorktreeProvisioner::from_trusted_paths(
            &toolchain,
            &repository_path,
            &cargo_workspace,
            &artifact_root,
            &runtime_directory,
            process_limits(),
            WorktreeLimits::try_new(Duration::from_secs(15)).unwrap(),
        )
        .unwrap(),
    );

    let mut task_ids = HashMap::new();
    let mut reservations = HashMap::new();
    for name in ["absent", "ready", "partial", "mismatched"] {
        let task = writer
            .create_task(support::new_task(repository.id, name), support::deadline())
            .await
            .unwrap()
            .value
            .task()
            .clone();
        let identity =
            WorktreeIdentity::try_new(repository.id.to_string(), task.id.to_string(), task.attempt)
                .unwrap();
        let reserved = provisioner
            .prepare(identity, CancellationToken::new())
            .await
            .unwrap();
        writer
            .reserve_attempt_artifact(
                ReserveAttemptArtifact {
                    identity: AttemptArtifactIdentity {
                        task_id: task.id,
                        repository_id: repository.id,
                        attempt: task.attempt,
                    },
                    base_commit: reserved.base_commit().to_owned(),
                    branch_name: reserved.branch_name().to_owned(),
                    worktree_path: CanonicalPath::try_from_canonical(
                        reserved.worktree_path().to_owned(),
                    )
                    .unwrap(),
                },
                support::deadline(),
            )
            .await
            .unwrap();
        task_ids.insert(name, task.id);
        reservations.insert(name, reserved);
    }
    provisioner
        .provision_reserved(
            reservations.remove("ready").unwrap(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let partial = &reservations["partial"];
    git(
        &repository_path,
        &["branch", partial.branch_name(), partial.base_commit()],
    );
    let mismatched = &reservations["mismatched"];
    git(
        &repository_path,
        &["branch", mismatched.branch_name(), "HEAD^"],
    );

    let observer = WorktreeArtifactObserver::new([(repository.id, provisioner)]);
    let summary = reconcile_restart_artifacts(&store, &writer, &observer, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(summary.examined, 4);
    assert_eq!(summary.marked_ready, 1);
    assert_eq!(summary.marked_inconsistent, 3);
    assert_eq!(
        store
            .load_attempt_artifact(task_ids["ready"])
            .await
            .unwrap()
            .unwrap()
            .state,
        AttemptArtifactState::Ready
    );
    for name in ["absent", "partial", "mismatched"] {
        assert_eq!(
            store
                .load_attempt_artifact(task_ids[name])
                .await
                .unwrap()
                .unwrap()
                .state,
            AttemptArtifactState::Inconsistent
        );
    }
}

fn reservation(
    repository: &coding_agent_domain::Repository,
    task: &coding_agent_domain::Task,
    name: &str,
) -> ReserveAttemptArtifact {
    ReserveAttemptArtifact {
        identity: AttemptArtifactIdentity {
            task_id: task.id,
            repository_id: repository.id,
            attempt: task.attempt,
        },
        base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
        branch_name: format!("codex/{name}-{}", task.id),
        worktree_path: CanonicalPath::try_from_canonical(
            repository
                .git_root
                .as_path()
                .join("restart-artifacts")
                .join(task.id.to_string()),
        )
        .unwrap(),
    }
}

fn canonical(path: &Path) -> CanonicalPath {
    CanonicalPath::try_from_canonical(path.canonicalize().unwrap()).unwrap()
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

fn git(repository: &Path, arguments: &[&str]) {
    let output = Command::new(path_executable(if cfg!(windows) {
        "git.exe"
    } else {
        "git"
    }))
    .arg("-C")
    .arg(repository)
    .args(arguments)
    .output()
    .unwrap();
    assert!(
        output.status.success(),
        "git {} failed: {}",
        arguments.join(" "),
        String::from_utf8_lossy(&output.stderr)
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
