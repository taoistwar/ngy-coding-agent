mod support;

use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use coding_agent_core::{
    AgentRuntime, ContextRedactor, ReviewDiffCheckpoint, ReviewDiffChunkRequest, ReviewDiffRuntime,
    ToolRequest, ToolRuntime, WorkspaceCheckpoint,
};
use coding_agent_runtime::{
    ProcessLimits, ProvisionedWorktree, RuntimeSession, RuntimeSessionLimits, ToolchainPaths,
    WorktreeIdentity, WorktreeLimits, WorktreeProvisioner, discover_toolchain,
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const PACKAGE: &str = "review_diff_fixture";

struct IdentityRedactor;

impl ContextRedactor for IdentityRedactor {
    fn redact(&self, content: &str) -> String {
        content.to_owned()
    }
}

fn identity_redactor() -> Arc<dyn ContextRedactor> {
    Arc::new(IdentityRedactor)
}

struct SecretRedactor;

impl ContextRedactor for SecretRedactor {
    fn redact(&self, content: &str) -> String {
        content.replace("TOP_SECRET_VALUE", "[REDACTED]")
    }
}

#[tokio::test]
async fn reviewer_manifest_chunks_cache_and_terminal_recollection_are_authoritative() {
    let fixture = Fixture::new("authoritative").await;
    let provisioned = fixture.provision("review-task").await;
    let workspace = provisioned.cargo_workspace_path().to_owned();
    let session = fixture.session(&provisioned);
    let redactor = identity_redactor();

    std::fs::write(
        workspace.join("src/lib.rs"),
        b"pub fn answer() -> u32 { 43 }\n",
    )
    .unwrap();
    std::fs::write(workspace.join("zeta.txt"), b"zeta\n").unwrap();
    std::fs::write(workspace.join("alpha.txt"), b"alpha\n").unwrap();

    let checkpoint = trusted_checkpoint(&session, 1).await;
    let manifest = session
        .review_diff_manifest(
            checkpoint.clone(),
            Arc::clone(&redactor),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        manifest
            .files()
            .iter()
            .map(|file| file.path())
            .collect::<Vec<_>>(),
        vec!["alpha.txt", "src/lib.rs", "zeta.txt"]
    );
    let cached = session
        .review_diff_manifest(
            checkpoint.clone(),
            Arc::clone(&redactor),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(cached, manifest);

    let mut content = String::new();
    for start in (0..manifest.chunk_count()).step_by(2) {
        let count = (manifest.chunk_count() - start).min(2);
        let request = ReviewDiffChunkRequest::for_manifest(&manifest, start, count).unwrap();
        let batch = session
            .review_diff_chunks(request, CancellationToken::new())
            .await
            .unwrap();
        assert!(batch.chunks().len() <= 2);
        for chunk in batch.chunks() {
            content.push_str(chunk.content());
        }
    }
    assert!(content.contains("path: alpha.txt"));
    assert!(content.contains("path: src/lib.rs"));
    assert!(content.contains("+pub fn answer() -> u32 { 43 }"));

    // An ordinary Git ToolResult contains no typed coverage authority.
    let ordinary_only = fixture.session(&provisioned);
    ordinary_only
        .invoke(ToolRequest::GitDiff, CancellationToken::new())
        .await
        .unwrap();
    ordinary_only
        .invoke(
            ToolRequest::ReadFile {
                path: "src/lib.rs".to_owned(),
                start_line: 1,
                end_line: 10,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let first_request =
        ReviewDiffChunkRequest::for_manifest(&manifest, 0, manifest.chunk_count().min(1)).unwrap();
    let error = ordinary_only
        .review_diff_chunks(first_request, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.code, "REVIEW_DIFF_CACHE_MISS");

    // A changed workspace invalidates and clears the immutable cache. Returning
    // to the old bytes cannot replay it without a fresh manifest collection.
    std::fs::write(
        workspace.join("src/lib.rs"),
        b"pub fn answer() -> u32 { 44 }\n",
    )
    .unwrap();
    let request =
        ReviewDiffChunkRequest::for_manifest(&manifest, 0, manifest.chunk_count().min(1)).unwrap();
    let error = session
        .review_diff_chunks(request, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.code, "WORKSPACE_CHANGED");
    std::fs::write(
        workspace.join("src/lib.rs"),
        b"pub fn answer() -> u32 { 43 }\n",
    )
    .unwrap();
    let request =
        ReviewDiffChunkRequest::for_manifest(&manifest, 0, manifest.chunk_count().min(1)).unwrap();
    let error = session
        .review_diff_chunks(request, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.code, "REVIEW_DIFF_CACHE_MISS");

    let refreshed = session
        .review_diff_manifest(
            checkpoint.clone(),
            Arc::clone(&redactor),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(refreshed, manifest);
    let other_redactor = identity_redactor();
    let rebuilt_for_other_redactor = session
        .review_diff_manifest(
            checkpoint.clone(),
            Arc::clone(&other_redactor),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(rebuilt_for_other_redactor, manifest);
    assert_eq!(
        session
            .terminal_review_diff_manifest(
                checkpoint.clone(),
                Arc::clone(&redactor),
                CancellationToken::new(),
            )
            .await
            .unwrap_err()
            .code,
        "REVIEW_DIFF_REDACTOR_MISMATCH",
        "a different Arc redactor must replace, rather than reuse, the cache"
    );
    let refreshed = session
        .review_diff_manifest(
            checkpoint.clone(),
            Arc::clone(&redactor),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(refreshed, manifest);
    let terminal = session
        .terminal_review_diff_manifest(checkpoint, Arc::clone(&redactor), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(terminal, manifest);
    let request =
        ReviewDiffChunkRequest::for_manifest(&manifest, 0, manifest.chunk_count().min(1)).unwrap();
    assert_eq!(
        session
            .review_diff_chunks(request, CancellationToken::new())
            .await
            .unwrap_err()
            .code,
        "REVIEW_DIFF_CACHE_MISS",
        "fresh terminal collection must not leave the Reviewer cache reusable"
    );
}

#[tokio::test]
async fn binary_non_utf8_and_oversized_diffs_fail_closed() {
    let fixture = Fixture::new("fail-closed").await;
    let provisioned = fixture.provision("limit-task").await;
    let workspace = provisioned.cargo_workspace_path().to_owned();
    let session = fixture.session(&provisioned);
    let redactor = identity_redactor();

    std::fs::write(workspace.join("binary.bin"), [0, 1, 2, 3]).unwrap();
    let checkpoint = trusted_checkpoint(&session, 1).await;
    assert_eq!(
        session
            .review_diff_manifest(checkpoint, Arc::clone(&redactor), CancellationToken::new(),)
            .await
            .unwrap_err()
            .code,
        "REVIEW_DIFF_COVERAGE_LIMIT"
    );
    std::fs::remove_file(workspace.join("binary.bin")).unwrap();

    #[cfg(all(unix, not(target_vendor = "apple")))]
    {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;
        let path = OsString::from_vec(b"non-utf8-\xff.txt".to_vec());
        std::fs::write(workspace.join(&path), b"text\n").unwrap();
        let checkpoint = trusted_checkpoint(&session, 2).await;
        assert_eq!(
            session
                .review_diff_manifest(checkpoint, Arc::clone(&redactor), CancellationToken::new(),)
                .await
                .unwrap_err()
                .code,
            "REVIEW_DIFF_COVERAGE_LIMIT"
        );
        std::fs::remove_file(workspace.join(path)).unwrap();
    }

    std::fs::write(workspace.join("huge.txt"), "x".repeat(140 * 1024)).unwrap();
    let checkpoint = trusted_checkpoint(&session, 3).await;
    assert_eq!(
        session
            .review_diff_manifest(checkpoint, Arc::clone(&redactor), CancellationToken::new(),)
            .await
            .unwrap_err()
            .code,
        "REVIEW_DIFF_COVERAGE_LIMIT"
    );
    std::fs::remove_file(workspace.join("huge.txt")).unwrap();

    // This crosses the Project 2 per-file patch projection bound, proving an
    // available-but-truncated display DiffEvent cannot become coverage.
    std::fs::write(workspace.join("truncated.txt"), "x".repeat(300 * 1024)).unwrap();
    let checkpoint = trusted_checkpoint(&session, 4).await;
    assert_eq!(
        session
            .review_diff_manifest(checkpoint, Arc::clone(&redactor), CancellationToken::new(),)
            .await
            .unwrap_err()
            .code,
        "REVIEW_DIFF_COVERAGE_LIMIT"
    );
}

#[tokio::test]
async fn task_redactor_precedes_manifest_hash_chunks_cache_and_terminal_recollection() {
    let fixture = Fixture::new("redacted").await;
    let provisioned = fixture.provision("redacted-task").await;
    let workspace = provisioned.cargo_workspace_path().to_owned();
    let session = fixture.session(&provisioned);
    let redactor: Arc<dyn ContextRedactor> = Arc::new(SecretRedactor);
    std::fs::write(
        workspace.join("src/lib.rs"),
        b"pub const VALUE: &str = \"TOP_SECRET_VALUE\";\n",
    )
    .unwrap();

    let checkpoint = trusted_checkpoint(&session, 1).await;
    let manifest = session
        .review_diff_manifest(
            checkpoint.clone(),
            Arc::clone(&redactor),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(!String::from_utf8_lossy(manifest.canonical_bytes()).contains("TOP_SECRET_VALUE"));
    let mut visible = String::new();
    for start in (0..manifest.chunk_count()).step_by(2) {
        let count = (manifest.chunk_count() - start).min(2);
        let request = ReviewDiffChunkRequest::for_manifest(&manifest, start, count).unwrap();
        for chunk in session
            .review_diff_chunks(request, CancellationToken::new())
            .await
            .unwrap()
            .chunks()
        {
            visible.push_str(chunk.content());
        }
    }
    assert!(!visible.contains("TOP_SECRET_VALUE"));
    assert!(visible.contains("[REDACTED]"));

    let terminal = session
        .terminal_review_diff_manifest(checkpoint, redactor, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(terminal, manifest);
}

async fn trusted_checkpoint(session: &RuntimeSession, generation: u64) -> ReviewDiffCheckpoint {
    let fingerprint = session
        .workspace_fingerprint(CancellationToken::new())
        .await
        .unwrap();
    let checkpoint = WorkspaceCheckpoint::try_at_generation(generation, fingerprint).unwrap();
    ReviewDiffCheckpoint::from_workspace_checkpoint(&checkpoint)
}

struct Fixture {
    _temporary: TempDir,
    runtime_directory: PathBuf,
    repository: PathBuf,
    artifact_root: PathBuf,
    toolchain: ToolchainPaths,
}

impl Fixture {
    async fn new(name: &str) -> Self {
        let test_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target");
        std::fs::create_dir_all(&test_root).unwrap();
        let temporary = tempfile::Builder::new()
            .prefix(&format!("review-diff-{name}-"))
            .tempdir_in(test_root)
            .unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let runtime_directory = root.join("runtime");
        let repository = root.join("repository");
        let artifact_root = root.join("artifacts");
        for directory in [&runtime_directory, &repository.join("src"), &artifact_root] {
            std::fs::create_dir_all(directory).unwrap();
        }

        git_ok(&repository, &["init", "--quiet"]);
        git_ok(&repository, &["config", "user.name", "Review Diff Test"]);
        git_ok(
            &repository,
            &["config", "user.email", "review-diff@example.invalid"],
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
            b"pub fn answer() -> u32 { 42 }\n",
        )
        .unwrap();
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

    async fn provision(&self, task: &str) -> ProvisionedWorktree {
        let identity = WorktreeIdentity::try_new("repository-1", task, 1).unwrap();
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

    fn session(&self, provisioned: &ProvisionedWorktree) -> RuntimeSession {
        RuntimeSession::from_provisioned_worktree(
            provisioned,
            &self.toolchain,
            &self.runtime_directory,
            support::task_process_scope(&self.runtime_directory),
            NonZeroU32::new(1).expect("test Cargo jobs are nonzero"),
            RuntimeSessionLimits::project_2_defaults(),
        )
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
