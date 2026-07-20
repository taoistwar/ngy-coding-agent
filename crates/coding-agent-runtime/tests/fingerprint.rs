use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use coding_agent_runtime::{
    ExecutionDirectory, FingerprintError, FingerprintLimits, ProcessLimits, WorkspaceFingerprinter,
    discover_toolchain,
};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn tracked_untracked_index_state_and_ignored_outputs_are_deterministic() {
    let fixture = RepositoryFixture::new("coverage");
    fixture.write(".gitignore", b"target/\n");
    fixture.write("tracked.txt", b"base\n");
    fixture.commit_all("base");
    let collector = fixture.collector(32, 1024, 4096).await;

    let baseline = collector.collect(CancellationToken::new()).await.unwrap();
    assert_eq!(
        baseline,
        collector.collect(CancellationToken::new()).await.unwrap()
    );

    fixture.write("target/generated.bin", b"ignored build output");
    assert_eq!(
        baseline,
        collector.collect(CancellationToken::new()).await.unwrap(),
        "ignored target output must not invalidate source tests"
    );

    fixture.write("tracked.txt", b"modified\n");
    let unstaged = collector.collect(CancellationToken::new()).await.unwrap();
    assert_ne!(baseline, unstaged);
    fixture.git(&["add", "--", "tracked.txt"]);
    let staged = collector.collect(CancellationToken::new()).await.unwrap();
    assert_ne!(
        unstaged, staged,
        "index identity/status is part of the fingerprint"
    );

    fixture.write("z-untracked.txt", b"z\n");
    fixture.write("a-untracked.txt", b"a\n");
    let creation_order_one = collector.collect(CancellationToken::new()).await.unwrap();
    std::fs::remove_file(fixture.repository.join("z-untracked.txt")).unwrap();
    std::fs::remove_file(fixture.repository.join("a-untracked.txt")).unwrap();
    fixture.write("a-untracked.txt", b"a\n");
    fixture.write("z-untracked.txt", b"z\n");
    let creation_order_two = collector.collect(CancellationToken::new()).await.unwrap();
    assert_eq!(creation_order_one, creation_order_two);

    // Apple VFS rejects malformed UTF-8 names with EILSEQ; Linux CI covers byte paths.
    #[cfg(all(unix, not(target_vendor = "apple")))]
    {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;
        let raw_name = OsString::from_vec(b"raw-\xff.txt".to_vec());
        std::fs::write(fixture.repository.join(&raw_name), b"raw\n").unwrap();
        let raw_one = collector.collect(CancellationToken::new()).await.unwrap();
        let raw_two = collector.collect(CancellationToken::new()).await.unwrap();
        assert_eq!(raw_one, raw_two);
        std::fs::write(fixture.repository.join(raw_name), b"changed\n").unwrap();
        assert_ne!(
            raw_one,
            collector.collect(CancellationToken::new()).await.unwrap()
        );
    }
}

#[tokio::test]
async fn every_file_count_per_file_and_total_cap_fails_closed() {
    let fixture = RepositoryFixture::new("caps");
    fixture.write("one.txt", b"1111");
    fixture.write("two.txt", b"2222");
    fixture.commit_all("base");

    let too_many = fixture.collector(1, 16, 32).await;
    assert!(matches!(
        too_many.collect(CancellationToken::new()).await,
        Err(FingerprintError::TooManyFiles)
    ));

    let per_file = fixture.collector(4, 3, 32).await;
    assert!(matches!(
        per_file.collect(CancellationToken::new()).await,
        Err(FingerprintError::FileTooLarge)
    ));

    let total = fixture.collector(4, 4, 7).await;
    assert!(matches!(
        total.collect(CancellationToken::new()).await,
        Err(FingerprintError::TotalTooLarge)
    ));
}

#[tokio::test]
async fn symlink_gitlink_and_index_shortcuts_are_rejected() {
    let fixture = RepositoryFixture::new("unsafe");
    fixture.write("tracked.txt", b"tracked\n");
    fixture.commit_all("base");
    let collector = fixture.collector(16, 1024, 4096).await;

    fixture.git(&["update-index", "--assume-unchanged", "tracked.txt"]);
    assert!(matches!(
        collector.collect(CancellationToken::new()).await,
        Err(FingerprintError::UnsupportedEntry)
    ));
    fixture.git(&["update-index", "--no-assume-unchanged", "tracked.txt"]);
    fixture.git(&["update-index", "--skip-worktree", "tracked.txt"]);
    assert!(matches!(
        collector.collect(CancellationToken::new()).await,
        Err(FingerprintError::UnsupportedEntry)
    ));
    fixture.git(&["update-index", "--no-skip-worktree", "tracked.txt"]);

    let head = fixture.git_output(&["rev-parse", "HEAD"]);
    fixture.git(&[
        "update-index",
        "--add",
        "--cacheinfo",
        &format!("160000,{head},submodule"),
    ]);
    assert!(matches!(
        collector.collect(CancellationToken::new()).await,
        Err(FingerprintError::UnsupportedEntry)
    ));
    fixture.git(&["update-index", "--force-remove", "submodule"]);

    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(
            fixture.root.join("outside.txt"),
            fixture.repository.join("escape-link"),
        )
        .unwrap();
        assert!(matches!(
            collector.collect(CancellationToken::new()).await,
            Err(FingerprintError::UnsafeEntry(_))
        ));
    }
    #[cfg(windows)]
    {
        let target = fixture.root.join("outside.txt");
        std::fs::write(&target, b"outside").unwrap();
        if std::os::windows::fs::symlink_file(&target, fixture.repository.join("escape-link"))
            .is_ok()
        {
            assert!(matches!(
                collector.collect(CancellationToken::new()).await,
                Err(FingerprintError::UnsafeEntry(_))
            ));
        }
    }
}

#[tokio::test]
async fn unmerged_multistage_index_conflict_has_no_fingerprint() {
    let fixture = RepositoryFixture::new("conflict");
    fixture.write("conflicted.txt", b"base\n");
    fixture.commit_all("base");
    let base_branch = fixture.git_output(&["branch", "--show-current"]);
    fixture.git(&["checkout", "--quiet", "-b", "fingerprint-conflict"]);
    fixture.write("conflicted.txt", b"other\n");
    fixture.commit_all("other");
    fixture.git(&["checkout", "--quiet", &base_branch]);
    fixture.write("conflicted.txt", b"ours\n");
    fixture.commit_all("ours");
    fixture.git_expect_failure(&["merge", "--no-edit", "fingerprint-conflict"]);

    let collector = fixture.collector(16, 1024, 4096).await;
    assert!(matches!(
        collector.collect(CancellationToken::new()).await,
        Err(FingerprintError::UnsupportedEntry)
    ));
}

#[tokio::test]
async fn pre_cancelled_fingerprint_does_not_start_git() {
    let fixture = RepositoryFixture::new("cancel");
    fixture.write("tracked.txt", b"tracked\n");
    fixture.commit_all("base");
    let collector = fixture.collector(4, 1024, 4096).await;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        collector.collect(cancellation).await,
        Err(FingerprintError::Cancelled)
    ));
}

#[tokio::test]
async fn external_excludes_cannot_hide_untracked_deliverables() {
    let fixture = RepositoryFixture::new("external-excludes");
    fixture.write("tracked.txt", b"tracked\n");
    fixture.commit_all("base");
    let external = fixture.root.join("host-global-excludes");
    std::fs::write(&external, b"externally-hidden.txt\n").unwrap();
    fixture.git(&["config", "core.excludesFile", external.to_str().unwrap()]);
    let collector = fixture.collector(8, 1024, 4096).await;
    let before = collector.collect(CancellationToken::new()).await.unwrap();

    fixture.write("externally-hidden.txt", b"must be delivered\n");
    assert!(
        fixture
            .git_output(&["status", "--porcelain", "--untracked-files=all"])
            .is_empty(),
        "vanilla Git demonstrates that the external excludes file is active"
    );
    assert_ne!(
        before,
        collector.collect(CancellationToken::new()).await.unwrap(),
        "the bound fingerprint command must disable host-global excludes"
    );
}

struct RepositoryFixture {
    _temporary: tempfile::TempDir,
    root: PathBuf,
    runtime_directory: PathBuf,
    repository: PathBuf,
}

impl RepositoryFixture {
    fn new(name: &str) -> Self {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap();
        let test_root = workspace_root.join("target/fingerprint-tests");
        std::fs::create_dir_all(&test_root).unwrap();
        let temporary = tempfile::Builder::new()
            .prefix(&format!("{name}-"))
            .tempdir_in(test_root)
            .unwrap();
        let root = temporary.path().to_path_buf();
        let runtime_directory = root.join("runtime");
        let repository = root.join("repository");
        std::fs::create_dir(&runtime_directory).unwrap();
        std::fs::create_dir(&repository).unwrap();
        run_git(&repository, &["init", "--quiet"]);
        run_git(&repository, &["config", "user.name", "Fingerprint Test"]);
        run_git(
            &repository,
            &["config", "user.email", "fingerprint-test@example.invalid"],
        );
        Self {
            _temporary: temporary,
            root,
            runtime_directory,
            repository,
        }
    }

    fn write(&self, path: &str, content: &[u8]) {
        let path = self.repository.join(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, content).unwrap();
    }

    fn git(&self, arguments: &[&str]) {
        run_git(&self.repository, arguments);
    }

    fn git_output(&self, arguments: &[&str]) -> String {
        let output = Command::new("git")
            .arg("--no-pager")
            .arg("-C")
            .arg(&self.repository)
            .args(arguments)
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    fn git_expect_failure(&self, arguments: &[&str]) {
        let status = Command::new("git")
            .arg("--no-pager")
            .arg("-c")
            .arg(git_hooks_path_configuration())
            .arg("-C")
            .arg(&self.repository)
            .args(arguments)
            .status()
            .unwrap();
        assert!(
            !status.success(),
            "Git command unexpectedly passed: {arguments:?}"
        );
    }

    fn commit_all(&self, message: &str) {
        self.git(&["add", "--all"]);
        self.git(&["commit", "--quiet", "--no-gpg-sign", "-m", message]);
    }

    async fn collector(
        &self,
        max_files: usize,
        max_file_bytes: u64,
        max_total_bytes: u64,
    ) -> WorkspaceFingerprinter {
        let rustc = concrete_rustc();
        let git = path_executable(if cfg!(windows) { "git.exe" } else { "git" });
        let toolchain = discover_toolchain(&self.runtime_directory, Some(&rustc), Some(&git))
            .await
            .unwrap();
        WorkspaceFingerprinter::from_trusted_capabilities(
            &toolchain,
            Arc::new(
                ExecutionDirectory::open(self.repository.join(".git").canonicalize().unwrap())
                    .unwrap(),
            ),
            Arc::new(ExecutionDirectory::open(self.repository.canonicalize().unwrap()).unwrap()),
            &self.runtime_directory,
            ProcessLimits::try_new(
                256 * 1024,
                64 * 1024,
                Duration::from_secs(10),
                Duration::from_secs(3),
            )
            .unwrap(),
            FingerprintLimits::try_new(
                Duration::from_secs(5),
                max_files,
                max_file_bytes,
                max_total_bytes,
            )
            .unwrap(),
        )
        .unwrap()
    }
}

fn run_git(repository: &Path, arguments: &[&str]) {
    let status = Command::new("git")
        .arg("--no-pager")
        .arg("-c")
        .arg(git_hooks_path_configuration())
        .arg("-C")
        .arg(repository)
        .args(arguments)
        .status()
        .unwrap();
    assert!(status.success(), "Git command failed: {arguments:?}");
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
