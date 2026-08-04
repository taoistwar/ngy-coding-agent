mod support;

use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use coding_agent_core::{
    CheckEvidenceStatus, MAX_VALIDATION_MODEL_RESULT_BYTES, RepositoryCheckCatalog, RequiredCheck,
    RequiredCheckKind, RequiredCheckSelector, ValidationRuntime,
};
use coding_agent_runtime::{
    ProcessLimits, ProvisionedWorktree, RuntimeSession, RuntimeSessionLimits, ToolchainPaths,
    WorktreeIdentity, WorktreeLimits, WorktreeProvisioner, discover_toolchain,
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const PACKAGE: &str = "quality_fixture";

#[tokio::test]
async fn typed_quality_runtime_covers_catalog_status_bounds_and_stability() {
    let fixture = Fixture::new().await;
    let provisioned = fixture.provision().await;
    let workspace = provisioned.cargo_workspace_path().to_owned();
    let session = RuntimeSession::from_provisioned_worktree(
        &provisioned,
        &fixture.toolchain,
        &fixture.runtime_directory,
        support::task_process_scope(&fixture.runtime_directory),
        cargo_jobs_per_task(),
        RuntimeSessionLimits::project_2_defaults()
            .try_with_validation_timeout(Duration::from_secs(30))
            .unwrap(),
    )
    .unwrap();

    let initial_context = session.repository_context();
    let selectors = session
        .required_check_selectors(CancellationToken::new())
        .await
        .unwrap();
    assert!(contains_selector(
        &selectors,
        RequiredCheckKind::CargoCheck,
        None,
        None
    ));
    assert!(contains_selector(
        &selectors,
        RequiredCheckKind::CargoTest,
        None,
        None
    ));
    assert!(contains_selector(
        &selectors,
        RequiredCheckKind::CargoTest,
        Some(PACKAGE),
        Some("passing")
    ));

    let workspace_observation = session
        .run_validation(
            RequiredCheck::try_cargo_test("workspace-test", None, None).unwrap(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(workspace_observation.status(), CheckEvidenceStatus::Passed);
    assert_eq!(workspace_observation.check().package(), None);

    let check_observation = session
        .run_validation(
            RequiredCheck::try_cargo_check("workspace-check", None).unwrap(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(check_observation.status(), CheckEvidenceStatus::Passed);
    assert_eq!(
        check_observation.check().selector().kind(),
        RequiredCheckKind::CargoCheck
    );

    let lock_before_catalog_refresh = std::fs::read(workspace.join("Cargo.lock")).unwrap();
    write_integration(
        &workspace,
        "failure",
        "#[test]\nfn fails() { assert_eq!(2 + 2, 5); }\n",
    );
    assert!(
        !initial_context.contains("failure"),
        "the provision-time model context unexpectedly changed"
    );
    let refreshed = session
        .required_check_selectors(CancellationToken::new())
        .await
        .unwrap();
    assert!(contains_selector(
        &refreshed,
        RequiredCheckKind::CargoTest,
        Some(PACKAGE),
        Some("failure")
    ));
    assert_eq!(
        std::fs::read(workspace.join("Cargo.lock")).unwrap(),
        lock_before_catalog_refresh,
        "typed catalog refresh mutated Cargo.lock"
    );

    let failed_check = RequiredCheck::try_cargo_test(
        "failed-test",
        Some(PACKAGE.to_owned()),
        Some("failure".to_owned()),
    )
    .unwrap();
    let failed = session
        .run_validation(failed_check.clone(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(failed.check(), &failed_check);
    assert_eq!(failed.status(), CheckEvidenceStatus::Failed);
    assert!(failed.model_result().content().contains("duration_ms:"));
    assert!(
        !failed
            .model_result()
            .content()
            .contains(&workspace.to_string_lossy().to_string())
    );

    write_integration(
        &workspace,
        "noisy",
        "#[test]\nfn noisy() {\n    eprintln!(\"{}\", \"界\".repeat(20_000));\n    panic!(\"show output\");\n}\n",
    );
    let noisy = session
        .run_validation(
            RequiredCheck::try_cargo_test(
                "noisy-test",
                Some(PACKAGE.to_owned()),
                Some("noisy".to_owned()),
            )
            .unwrap(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(noisy.status(), CheckEvidenceStatus::Failed);
    assert!(noisy.truncated());
    assert!(noisy.model_result().truncated());
    assert!(noisy.model_result().content().len() <= MAX_VALIDATION_MODEL_RESULT_BYTES);
    assert!(std::str::from_utf8(noisy.model_result().content().as_bytes()).is_ok());

    let unknown = session
        .run_validation(
            RequiredCheck::try_cargo_test(
                "unknown-test",
                Some(PACKAGE.to_owned()),
                Some("not_in_metadata".to_owned()),
            )
            .unwrap(),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(unknown.code, "COMMAND_NOT_ALLOWED");
    assert!(
        RequiredCheck::try_cargo_test("invalid-test", None, Some("passing".to_owned())).is_err()
    );

    write_integration(
        &workspace,
        "slow",
        "#[test]\nfn slow() { std::thread::sleep(std::time::Duration::from_secs(30)); }\n",
    );
    let timeout_session = RuntimeSession::from_provisioned_worktree(
        &provisioned,
        &fixture.toolchain,
        &fixture.runtime_directory,
        support::task_process_scope(&fixture.runtime_directory),
        cargo_jobs_per_task(),
        RuntimeSessionLimits::project_2_defaults()
            .try_with_validation_timeout(Duration::from_secs(2))
            .unwrap(),
    )
    .unwrap();
    let timeout_check = RequiredCheck::try_cargo_test(
        "timeout-test",
        Some(PACKAGE.to_owned()),
        Some("slow".to_owned()),
    )
    .unwrap();
    let timed_out = timeout_session
        .run_validation(timeout_check.clone(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(timed_out.check(), &timeout_check);
    assert_eq!(timed_out.status(), CheckEvidenceStatus::Failed);
    assert!(
        timed_out
            .model_result()
            .content()
            .contains("status: timed_out")
    );

    let cancellation = CancellationToken::new();
    let cancel_signal = cancellation.clone();
    let cancel_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        cancel_signal.cancel();
    });
    let cancelled = session
        .run_validation(
            RequiredCheck::try_cargo_test(
                "cancelled-test",
                Some(PACKAGE.to_owned()),
                Some("slow".to_owned()),
            )
            .unwrap(),
            cancellation,
        )
        .await
        .unwrap_err();
    cancel_task.await.unwrap();
    assert_eq!(cancelled.code, "COMMAND_CANCELLED");

    write_integration(
        &workspace,
        "mutates",
        "#[test]\nfn mutates() { std::fs::write(\"workspace-mutated.txt\", b\"changed\").unwrap(); }\n",
    );
    let changed = session
        .run_validation(
            RequiredCheck::try_cargo_test(
                "mutation-test",
                Some(PACKAGE.to_owned()),
                Some("mutates".to_owned()),
            )
            .unwrap(),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(changed.code, "WORKSPACE_CHANGED");
    assert!(workspace.join("workspace-mutated.txt").is_file());
}

fn contains_selector(
    selectors: &[RequiredCheckSelector],
    kind: RequiredCheckKind,
    package: Option<&str>,
    integration_test: Option<&str>,
) -> bool {
    selectors.iter().any(|selector| {
        selector.kind() == kind
            && selector.package() == package
            && selector.integration_test() == integration_test
    })
}

fn cargo_jobs_per_task() -> NonZeroU32 {
    NonZeroU32::new(3).expect("test Cargo jobs are nonzero")
}

fn write_integration(workspace: &Path, name: &str, source: &str) {
    std::fs::write(workspace.join("tests").join(format!("{name}.rs")), source).unwrap();
}

struct Fixture {
    _temporary: TempDir,
    runtime_directory: PathBuf,
    repository: PathBuf,
    artifact_root: PathBuf,
    toolchain: ToolchainPaths,
}

impl Fixture {
    async fn new() -> Self {
        let test_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target");
        std::fs::create_dir_all(&test_root).unwrap();
        let temporary = tempfile::Builder::new()
            .prefix("quality-runtime-")
            .tempdir_in(test_root)
            .unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let runtime_directory = root.join("runtime");
        let repository = root.join("repository");
        let artifact_root = root.join("artifacts");
        for directory in [
            &runtime_directory,
            &repository.join("src"),
            &repository.join("tests"),
            &artifact_root,
        ] {
            std::fs::create_dir_all(directory).unwrap();
        }

        git_ok(&repository, &["init", "--quiet"]);
        git_ok(
            &repository,
            &["config", "user.name", "Quality Runtime Test"],
        );
        git_ok(
            &repository,
            &["config", "user.email", "quality-runtime@example.invalid"],
        );
        std::fs::write(repository.join(".gitignore"), b"/target/\n").unwrap();
        std::fs::write(
            repository.join("Cargo.toml"),
            format!(
                "[workspace]\n\n[package]\nname = \"{PACKAGE}\"\nversion = \"0.1.0\"\nedition = \"2024\"\n"
            ),
        )
        .unwrap();
        std::fs::write(
            repository.join("Cargo.lock"),
            format!(
                "# This file is automatically @generated by Cargo.\n# It is not intended for manual editing.\nversion = 4\n\n[[package]]\nname = \"{PACKAGE}\"\nversion = \"0.1.0\"\n"
            ),
        )
        .unwrap();
        std::fs::write(
            repository.join("src/lib.rs"),
            b"pub fn ready() -> bool { true }\n",
        )
        .unwrap();
        write_integration(
            &repository,
            "passing",
            "#[test]\nfn passing() { assert!(quality_fixture::ready()); }\n",
        );
        git_ok(&repository, &["add", "--all"]);
        git_ok(
            &repository,
            &["commit", "--quiet", "--no-gpg-sign", "-m", "base"],
        );

        let toolchain = discover_toolchain(
            &runtime_directory,
            support::instance_process_scope(&runtime_directory),
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
            runtime_directory,
            repository,
            artifact_root,
            toolchain,
        }
    }

    async fn provision(&self) -> ProvisionedWorktree {
        let identity = WorktreeIdentity::try_new("repository-1", "quality-task", 1).unwrap();
        std::fs::create_dir_all(
            self.artifact_root
                .join(identity.relative_path())
                .parent()
                .unwrap(),
        )
        .unwrap();
        let provisioner = WorktreeProvisioner::from_trusted_paths(
            &self.toolchain,
            "repository-1",
            &self.repository,
            &self.repository,
            &self.artifact_root,
            &self.runtime_directory,
            support::task_process_scope(&self.runtime_directory),
            process_limits(),
            WorktreeLimits::try_new(Duration::from_secs(15)).unwrap(),
        )
        .unwrap();
        let reservation = provisioner
            .prepare(identity, CancellationToken::new())
            .await
            .unwrap();
        provisioner
            .provision_reserved(reservation, CancellationToken::new())
            .await
            .unwrap()
    }
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

fn concrete_rustc() -> PathBuf {
    let output =
        support::command_output(Command::new("rustc").args(["--print", "sysroot"])).unwrap();
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

fn git_ok(repository: &Path, arguments: &[&str]) {
    let mut command = Command::new("git");
    command.current_dir(repository).args(arguments);
    let status = support::command_status(&mut command).unwrap();
    assert!(status.success(), "git {arguments:?} failed");
}
