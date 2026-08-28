// Each integration-test binary imports this shared fixture module directly,
// so any one binary intentionally uses only part of the support surface.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fmt;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use std::time::Duration;

use coding_agent_core::WorkspaceFingerprint;
use coding_agent_runtime::{
    DeliverySourceError, DeliverySourceLimits, DeliverySourceProvisioner, ExecutionDirectory,
    FingerprintLimits, ProbedDeliveryGit, ProcessLimits, ProcessLivenessScope, ProvisionedWorktree,
    ToolchainPaths, WorkspaceFingerprinter, WorktreeIdentity, WorktreeLimits, WorktreeProvisioner,
    WorktreeReservation, discover_toolchain, probe_delivery_git,
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

#[path = "../support/mod.rs"]
mod runtime_support;

pub(crate) fn task_process_scope(
    runtime_directory: &Path,
) -> coding_agent_runtime::ProcessLivenessScope {
    runtime_support::task_process_scope(runtime_directory)
}

const REPOSITORY_ID: &str = "delivery-source-repository";

pub struct Fixture {
    // Keep this first so its Drop proof runs while the private runtime still
    // exists and before TempDir removes the fixture tree.
    process_scopes: runtime_support::ProcessScopeTracker,
    _temporary: TempDir,
    pub root: PathBuf,
    pub runtime_directory: PathBuf,
    pub repository: PathBuf,
    cargo_workspace: PathBuf,
    artifact_root: PathBuf,
    pub toolchain: ToolchainPaths,
    pub delivery_git: Arc<ProbedDeliveryGit>,
}

pub struct Sha256RepositoryUnavailable {
    status: Option<i32>,
    stderr: String,
}

impl fmt::Display for Sha256RepositoryUnavailable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "git init --object-format=sha256 exited with status {:?}",
            self.status
        )?;
        if !self.stderr.is_empty() {
            write!(formatter, ": {}", self.stderr)?;
        }
        Ok(())
    }
}

impl Sha256RepositoryUnavailable {
    /// A dynamic skip is allowed only when Git itself explicitly rejects the
    /// SHA-256 capability. Other initialization failures must remain test
    /// failures instead of being mistaken for an unsupported toolchain.
    pub fn explicitly_reports_unsupported_object_format(&self) -> bool {
        let stderr = self.stderr.to_ascii_lowercase();
        let names_object_format =
            stderr.contains("object-format") || stderr.contains("object format");
        names_object_format
            && [
                "unknown option",
                "unrecognized option",
                "unsupported",
                "not supported",
                "invalid object format",
            ]
            .iter()
            .any(|diagnostic| stderr.contains(diagnostic))
    }
}

impl Fixture {
    pub fn artifact_root(&self) -> &Path {
        &self.artifact_root
    }

    pub async fn new(name: &str) -> Self {
        Self::new_with_init_arguments(name, &["init", "--quiet"])
            .await
            .unwrap_or_else(|error| panic!("default Git repository initialization failed: {error}"))
    }

    pub async fn try_new_sha256(name: &str) -> Result<Self, Sha256RepositoryUnavailable> {
        Self::new_with_init_arguments(name, &["init", "--quiet", "--object-format=sha256"]).await
    }

    async fn new_with_init_arguments(
        name: &str,
        init_arguments: &[&str],
    ) -> Result<Self, Sha256RepositoryUnavailable> {
        let test_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target");
        std::fs::create_dir_all(&test_root).unwrap();
        let temporary = tempfile::Builder::new()
            .prefix(&format!("delivery-source-{name}-"))
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
            &artifact_root,
        ] {
            std::fs::create_dir_all(directory).unwrap();
        }

        let init = git_output(&repository, init_arguments);
        if !init.status.success() {
            return Err(Sha256RepositoryUnavailable {
                status: init.status.code(),
                stderr: String::from_utf8_lossy(&init.stderr).trim().to_owned(),
            });
        }
        git_ok(
            &repository,
            &["config", "user.name", "Delivery Source Test"],
        );
        git_ok(
            &repository,
            &["config", "user.email", "delivery-source@example.invalid"],
        );
        std::fs::write(repository.join("tracked.txt"), b"tracked first\n").unwrap();
        std::fs::write(repository.join("sequence.txt"), b"first\n").unwrap();
        std::fs::write(
            cargo_workspace.join("Cargo.toml"),
            b"[workspace]\n\n[package]\nname = \"delivery_source_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
        )
        .unwrap();
        std::fs::write(
            cargo_workspace.join("src/lib.rs"),
            b"pub fn committed() -> bool { true }\n",
        )
        .unwrap();
        std::fs::write(
            cargo_workspace.join("Cargo.lock"),
            b"# This file is automatically @generated by Cargo.\nversion = 4\n\n[[package]]\nname = \"delivery_source_fixture\"\nversion = \"0.1.0\"\n",
        )
        .unwrap();
        git_ok(&repository, &["add", "--all"]);
        git_ok(
            &repository,
            &["commit", "--quiet", "--no-gpg-sign", "-m", "first base"],
        );
        std::fs::write(repository.join("sequence.txt"), b"second\n").unwrap();
        git_ok(&repository, &["add", "--", "sequence.txt"]);
        git_ok(
            &repository,
            &["commit", "--quiet", "--no-gpg-sign", "-m", "review base"],
        );

        let process_scopes = runtime_support::ProcessScopeTracker::default();
        let toolchain = discover_toolchain(
            &runtime_directory,
            process_scopes.track(runtime_support::instance_process_scope(&runtime_directory)),
            Some(&concrete_rustc()),
            Some(&path_executable(if cfg!(windows) {
                "git.exe"
            } else {
                "git"
            })),
        )
        .await
        .unwrap();
        let private_runtime = Arc::new(ExecutionDirectory::open(&runtime_directory).unwrap());
        let delivery_git = Arc::new(
            probe_delivery_git(
                toolchain.git(),
                private_runtime,
                process_scopes.track(runtime_support::task_process_scope(&runtime_directory)),
                process_limits(),
                Duration::from_secs(10),
                CancellationToken::new(),
            )
            .await
            .unwrap(),
        );

        Ok(Self {
            process_scopes,
            _temporary: temporary,
            root,
            runtime_directory,
            repository,
            cargo_workspace,
            artifact_root,
            toolchain,
            delivery_git,
        })
    }

    pub async fn reviewed_dirty_source(&self, task_id: &str) -> ReviewedDirtySource {
        let worker_process_scope = runtime_support::task_process_scope(&self.runtime_directory);
        let worktrees = self.worktree_provisioner_with_scope(worker_process_scope.clone());
        let identity = WorktreeIdentity::try_new(REPOSITORY_ID, task_id, 1).unwrap();
        std::fs::create_dir_all(
            self.artifact_root
                .join(identity.relative_path())
                .parent()
                .unwrap(),
        )
        .unwrap();
        let reservation = worktrees
            .prepare(identity, CancellationToken::new())
            .await
            .unwrap();
        let provisioned = worktrees
            .provision_reserved(reservation.clone(), CancellationToken::new())
            .await
            .unwrap();

        std::fs::write(
            provisioned.worktree_path().join("tracked.txt"),
            b"tracked approved change\n",
        )
        .unwrap();
        std::fs::write(
            provisioned.worktree_path().join("review-note.txt"),
            b"approved untracked change\n",
        )
        .unwrap();
        let approved_fingerprint = fingerprint(self, &provisioned).await;
        let admin_directory = linked_admin_directory(provisioned.worktree_path());

        ReviewedDirtySource {
            worktrees,
            worker_process_scope,
            reservation,
            approved_fingerprint,
            admin_directory,
        }
    }

    pub async fn current_fingerprint(&self, source: &ReviewedDirtySource) -> WorkspaceFingerprint {
        let git_directory = Arc::new(ExecutionDirectory::open(&source.admin_directory).unwrap());
        let work_tree = Arc::new(ExecutionDirectory::open(source.worktree_path()).unwrap());
        WorkspaceFingerprinter::from_trusted_capabilities(
            &self.toolchain,
            git_directory,
            work_tree,
            &self.runtime_directory,
            self.task_process_scope(),
            process_limits(),
            fingerprint_limits(),
        )
        .unwrap()
        .collect(CancellationToken::new())
        .await
        .unwrap()
    }

    pub async fn fingerprint_for_provisioned(
        &self,
        source: &ProvisionedWorktree,
    ) -> WorkspaceFingerprint {
        fingerprint(self, source).await
    }

    pub fn delivery_source(
        &self,
        worktrees: &WorktreeProvisioner,
    ) -> Result<DeliverySourceProvisioner, DeliverySourceError> {
        self.delivery_source_with_limits(worktrees, delivery_source_limits())
    }

    pub fn delivery_source_with_limits(
        &self,
        worktrees: &WorktreeProvisioner,
        limits: DeliverySourceLimits,
    ) -> Result<DeliverySourceProvisioner, DeliverySourceError> {
        DeliverySourceProvisioner::from_worktree_provisioner(
            worktrees,
            Arc::clone(&self.delivery_git),
            &self.runtime_directory,
            self.task_process_scope(),
            process_limits(),
            limits,
            fingerprint_limits(),
        )
    }

    /// Reopens the repository after a test deliberately replaces its common
    /// Git directory.  This gives recovery tests an authenticator with the
    /// replacement directory's own identity instead of relying on a stale
    /// in-memory capability to reject it first.
    pub fn fresh_worktree_provisioner(&self) -> WorktreeProvisioner {
        self.worktree_provisioner()
    }

    fn worktree_provisioner(&self) -> WorktreeProvisioner {
        self.worktree_provisioner_with_scope(self.task_process_scope())
    }

    pub(crate) fn task_process_scope(&self) -> ProcessLivenessScope {
        self.process_scopes
            .track(runtime_support::task_process_scope(&self.runtime_directory))
    }

    pub(crate) fn worktree_provisioner_with_scope(
        &self,
        process_liveness_scope: ProcessLivenessScope,
    ) -> WorktreeProvisioner {
        self.worktree_provisioner_for_repository_with_scope(REPOSITORY_ID, process_liveness_scope)
    }

    pub fn worktree_provisioner_for_repository_with_scope(
        &self,
        repository_id: &str,
        process_liveness_scope: ProcessLivenessScope,
    ) -> WorktreeProvisioner {
        let process_liveness_scope = self.process_scopes.track(process_liveness_scope);
        WorktreeProvisioner::from_trusted_paths(
            &self.toolchain,
            repository_id,
            &self.repository,
            &self.cargo_workspace,
            &self.artifact_root,
            &self.runtime_directory,
            process_liveness_scope,
            process_limits(),
            WorktreeLimits::try_new(Duration::from_secs(15)).unwrap(),
        )
        .unwrap()
    }
}

pub struct ReviewedDirtySource {
    pub worktrees: WorktreeProvisioner,
    pub worker_process_scope: ProcessLivenessScope,
    pub reservation: WorktreeReservation,
    pub approved_fingerprint: WorkspaceFingerprint,
    pub admin_directory: PathBuf,
}

impl ReviewedDirtySource {
    pub fn worktree_path(&self) -> &Path {
        self.reservation.worktree_path()
    }

    pub fn snapshot(&self, repository: &Path) -> RepositorySnapshot {
        snapshot_paths(repository, &self.admin_directory, self.worktree_path())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RepositorySnapshot {
    refs: Vec<u8>,
    index: Vec<u8>,
    worktree: BTreeMap<PathBuf, Option<Vec<u8>>>,
}

pub fn snapshot_paths(
    repository: &Path,
    admin_directory: &Path,
    worktree: &Path,
) -> RepositorySnapshot {
    RepositorySnapshot {
        refs: git_stdout(
            repository,
            &[
                "for-each-ref",
                "--format=%(refname)%00%(objectname)",
                "refs",
            ],
        ),
        index: std::fs::read(admin_directory.join("index")).unwrap(),
        worktree: snapshot_tree(worktree),
    }
}

pub fn delivery_source_limits() -> DeliverySourceLimits {
    DeliverySourceLimits::try_new(
        Duration::from_secs(10),
        512 * 1024,
        64 * 1024,
        64 * 1024,
        4_096,
    )
    .unwrap()
}

pub fn tiny_delivery_source_limits() -> DeliverySourceLimits {
    DeliverySourceLimits::try_new(Duration::from_secs(10), 64, 64, 64, 1).unwrap()
}

pub fn near_zero_timeout_delivery_source_limits() -> DeliverySourceLimits {
    DeliverySourceLimits::try_new(
        Duration::from_nanos(1),
        512 * 1024,
        64 * 1024,
        64 * 1024,
        4_096,
    )
    .unwrap()
}

pub fn assert_read_only_failure(
    before: RepositorySnapshot,
    source: &ReviewedDirtySource,
    repository: &Path,
) {
    assert_eq!(source.snapshot(repository), before);
}

pub fn git_ok(repository: &Path, arguments: &[&str]) {
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

pub fn git_line(repository: &Path, arguments: &[&str]) -> String {
    String::from_utf8(git_stdout(repository, arguments))
        .unwrap()
        .trim()
        .to_owned()
}

pub fn git_with_stdin(repository: &Path, arguments: &[&str], input: &[u8]) {
    let mut command = git_command(repository, arguments);
    command.stdin(Stdio::piped());
    let mut child = command.spawn().unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "git fixture command failed: git -C {} {}\nstderr: {}",
        repository.display(),
        arguments.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
}

pub fn write_non_utf8_symbolic_head(source: &ReviewedDirtySource) {
    std::fs::write(
        source.admin_directory.join("HEAD"),
        b"ref: refs/heads/delivery-source/invalid-\xff\n",
    )
    .unwrap();
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

fn fingerprint_limits() -> FingerprintLimits {
    FingerprintLimits::try_new(
        Duration::from_secs(10),
        4_096,
        2 * 1024 * 1024,
        32 * 1024 * 1024,
    )
    .unwrap()
}

async fn fingerprint(fixture: &Fixture, worktree: &ProvisionedWorktree) -> WorkspaceFingerprint {
    WorkspaceFingerprinter::from_trusted_capabilities(
        &fixture.toolchain,
        worktree.git_directory(),
        worktree.work_tree(),
        &fixture.runtime_directory,
        fixture.task_process_scope(),
        process_limits(),
        fingerprint_limits(),
    )
    .unwrap()
    .collect(CancellationToken::new())
    .await
    .unwrap()
}

fn snapshot_tree(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn visit(root: &Path, directory: &Path, entries: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
        let mut children = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap())
            .collect::<Vec<_>>();
        children.sort_by_key(|entry| entry.file_name());
        for entry in children {
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

fn linked_admin_directory(worktree: &Path) -> PathBuf {
    let pointer = std::fs::read_to_string(worktree.join(".git")).unwrap();
    PathBuf::from(pointer.trim().strip_prefix("gitdir: ").unwrap())
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

fn git_output(repository: &Path, arguments: &[&str]) -> Output {
    runtime_support::command_output(&mut git_command(repository, arguments)).unwrap()
}

fn git_command(repository: &Path, arguments: &[&str]) -> Command {
    let mut command = Command::new("git");
    command
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
        .env("LC_ALL", "C")
        .env("LANG", "C");
    command
}

fn concrete_rustc() -> PathBuf {
    let output =
        runtime_support::command_output(Command::new("rustc").args(["--print", "sysroot"]))
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
fn git_hooks_path_configuration() -> &'static OsStr {
    OsStr::new("core.hooksPath=NUL")
}

#[cfg(unix)]
fn git_hooks_path_configuration() -> &'static OsStr {
    OsStr::new("core.hooksPath=/dev/null")
}

#[cfg(windows)]
fn null_device() -> &'static OsStr {
    OsStr::new("NUL")
}

#[cfg(unix)]
fn null_device() -> &'static OsStr {
    OsStr::new("/dev/null")
}
