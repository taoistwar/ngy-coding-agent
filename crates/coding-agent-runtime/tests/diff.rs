use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use coding_agent_core::DiffFileStatus;
use coding_agent_runtime::{
    DiffCollector, DiffError, DiffLimits, ExecutionDirectory, ProcessLimits, discover_toolchain,
};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn collects_added_modified_deleted_binary_untracked_and_non_utf8_paths_deterministically() {
    let fixture = RepositoryFixture::new("complete");
    fixture.write(".gitattributes", b"modified.txt diff=hostile\n");
    fixture.write("modified.txt", b"old\nkeep\n");
    fixture.write("deleted.txt", b"gone\nsecond\n");
    fixture.write("binary.bin", &[0, 1, 2, 3]);
    fixture.write("renamed-old.txt", b"renamed\n");
    fixture.write("copied-source.txt", b"copied\n");
    fixture.write("external-attribute.txt", b"before\n");
    // Apple VFS rejects malformed UTF-8 names with EILSEQ; Linux CI covers byte paths.
    #[cfg(all(unix, not(target_vendor = "apple")))]
    {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;
        let name = OsString::from_vec(b"tracked-nonutf8-\xff.txt".to_vec());
        std::fs::write(fixture.repository.join(name), b"before\n").unwrap();
    }
    fixture.commit_all("base");
    let external_excludes = fixture.root.join("host-global-excludes");
    std::fs::write(&external_excludes, b"externally-hidden.txt\n").unwrap();
    fixture.git(&[
        "config",
        "core.excludesFile",
        external_excludes.to_str().unwrap(),
    ]);
    let external_attributes = fixture.root.join("host-global-attributes");
    std::fs::write(&external_attributes, b"external-attribute.txt binary\n").unwrap();
    fixture.git(&[
        "config",
        "core.attributesFile",
        external_attributes.to_str().unwrap(),
    ]);
    fixture.git(&[
        "config",
        "diff.external",
        "definitely-not-a-real-diff-command",
    ]);
    fixture.git(&[
        "config",
        "diff.hostile.textconv",
        "definitely-not-a-real-textconv-command",
    ]);
    fixture.git(&["config", "status.renames", "copies"]);
    fixture.git(&["config", "diff.renames", "copies"]);
    fixture.git(&["config", "core.quotePath", "false"]);
    fixture.git(&["config", "color.ui", "always"]);
    fixture.git(&["config", "color.diff", "always"]);

    fixture.write("modified.txt", b"new\nkeep\nplus\n");
    std::fs::remove_file(fixture.repository.join("deleted.txt")).unwrap();
    fixture.write("binary.bin", &[0, 9, 8, 7, 6]);
    fixture.write("added.txt", b"first\nsecond\n");
    fixture.write("untracked.bin", &[0, 0xff, 0x10]);
    fixture.write("staged.txt", b"staged\n");
    fixture.write("externally-hidden.txt", b"must be delivered\n");
    fixture.write("external-attribute.txt", b"after\nplus\n");
    fixture.git(&["mv", "--", "renamed-old.txt", "renamed-new.txt"]);
    fixture.write("copied-new.txt", b"copied\n");
    fixture.git(&["add", "--", "copied-new.txt"]);
    fixture.git(&["add", "--", "staged.txt"]);
    assert!(
        git_output(
            &fixture.repository,
            &["status", "--porcelain", "--untracked-files=all"]
        )
        .lines()
        .all(|line| !line.contains("externally-hidden.txt")),
        "vanilla Git demonstrates that the external excludes file is active"
    );
    assert!(
        git_output(
            &fixture.repository,
            &["diff", "--numstat", "HEAD", "--", "external-attribute.txt"]
        )
        .starts_with("-\t-\t"),
        "vanilla Git demonstrates that the external attributes file is active"
    );

    #[cfg(all(unix, not(target_vendor = "apple")))]
    {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;
        let tracked = OsString::from_vec(b"tracked-nonutf8-\xff.txt".to_vec());
        std::fs::write(fixture.repository.join(tracked), b"after\nplus\n").unwrap();
        let name = OsString::from_vec(b"nonutf8-\xff.txt".to_vec());
        std::fs::write(fixture.repository.join(name), b"escaped\n").unwrap();
    }

    let collector = fixture.collector(32, 8 * 1024, 2 * 1024 * 1024).await;
    let first = collector
        .collect(7, CancellationToken::new())
        .await
        .unwrap();
    let second = collector
        .collect(7, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.revision, 7);

    let paths = first
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<Vec<_>>();
    let mut sorted = paths.clone();
    sorted.sort_unstable();
    assert_eq!(
        paths, sorted,
        "diff files must have deterministic path order"
    );
    assert!(paths.contains(&"added.txt"));
    assert!(paths.contains(&"binary.bin"));
    assert!(paths.contains(&"copied-new.txt"));
    assert!(paths.contains(&"deleted.txt"));
    assert!(paths.contains(&"external-attribute.txt"));
    assert!(paths.contains(&"externally-hidden.txt"));
    assert!(paths.contains(&"modified.txt"));
    assert!(paths.contains(&"renamed-new.txt"));
    assert!(paths.contains(&"renamed-old.txt"));
    assert!(paths.contains(&"staged.txt"));
    assert!(paths.contains(&"untracked.bin"));
    assert!(!paths.contains(&"copied-source.txt"));
    #[cfg(all(unix, not(target_vendor = "apple")))]
    {
        assert!(paths.contains(&"nonutf8-%FF.txt"));
        assert!(paths.contains(&"tracked-nonutf8-%FF.txt"));
        let tracked_non_utf8 = file(&first, "tracked-nonutf8-%FF.txt");
        assert_eq!(tracked_non_utf8.status, DiffFileStatus::Modified);
        assert_eq!(
            (tracked_non_utf8.additions, tracked_non_utf8.deletions),
            (2, 1)
        );
        assert!(!tracked_non_utf8.truncated);
        assert!(!tracked_non_utf8.patch.contains('\u{fffd}'));
    }

    let added = file(&first, "added.txt");
    assert_eq!(added.status, DiffFileStatus::Added);
    assert_eq!((added.additions, added.deletions), (2, 0));
    assert!(!added.truncated);
    assert!(added.patch.contains("new file mode"));
    assert!(added.patch.contains("+first\n+second\n"));

    let staged = file(&first, "staged.txt");
    assert_eq!(staged.status, DiffFileStatus::Added);
    assert_eq!((staged.additions, staged.deletions), (1, 0));

    let copied = file(&first, "copied-new.txt");
    assert_eq!(copied.status, DiffFileStatus::Added);
    assert_eq!((copied.additions, copied.deletions), (1, 0));

    let renamed_new = file(&first, "renamed-new.txt");
    assert_eq!(renamed_new.status, DiffFileStatus::Added);
    assert_eq!((renamed_new.additions, renamed_new.deletions), (1, 0));
    let renamed_old = file(&first, "renamed-old.txt");
    assert_eq!(renamed_old.status, DiffFileStatus::Deleted);
    assert_eq!((renamed_old.additions, renamed_old.deletions), (0, 1));

    let modified = file(&first, "modified.txt");
    assert_eq!(modified.status, DiffFileStatus::Modified);
    assert_eq!((modified.additions, modified.deletions), (2, 1));
    assert!(modified.patch.contains("+new"));
    assert!(modified.patch.contains("-old"));
    assert!(!modified.patch.contains('\u{1b}'));

    let externally_attributed = file(&first, "external-attribute.txt");
    assert_eq!(
        (
            externally_attributed.additions,
            externally_attributed.deletions
        ),
        (2, 1)
    );
    assert!(!externally_attributed.truncated);
    assert!(externally_attributed.patch.contains("+after"));
    assert_eq!(
        file(&first, "externally-hidden.txt").status,
        DiffFileStatus::Added
    );

    let deleted = file(&first, "deleted.txt");
    assert_eq!(deleted.status, DiffFileStatus::Deleted);
    assert_eq!((deleted.additions, deleted.deletions), (0, 2));

    for binary_path in ["binary.bin", "untracked.bin"] {
        let binary = file(&first, binary_path);
        assert_eq!((binary.additions, binary.deletions), (0, 0));
        assert!(
            binary.truncated,
            "binary content must be intentionally omitted"
        );
        assert!(binary.patch.contains("Binary"));
        assert!(!binary.patch.contains('\0'));
    }

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let error = collector.collect(8, cancellation).await.unwrap_err();
    assert_eq!(error.code(), "COMMAND_CANCELLED");
}

#[tokio::test]
async fn patch_caps_keep_counts_truthful_and_file_caps_fail_closed() {
    let fixture = RepositoryFixture::new("bounds");
    let before = (0..100)
        .map(|index| format!("before-{index}\n"))
        .collect::<String>();
    fixture.write("large.txt", before.as_bytes());
    fixture.commit_all("base");
    let after = (0..100)
        .map(|index| format!("after-{index}\n"))
        .collect::<String>();
    fixture.write("large.txt", after.as_bytes());

    let collector = fixture.collector(4, 96, 2 * 1024 * 1024).await;
    let snapshot = collector
        .collect(1, CancellationToken::new())
        .await
        .unwrap();
    let large = file(&snapshot, "large.txt");
    assert_eq!((large.additions, large.deletions), (100, 100));
    assert!(large.truncated);
    assert!(large.patch.len() <= 96);

    fixture.write("another.txt", b"another\n");
    let capped = fixture.collector(1, 96, 2 * 1024 * 1024).await;
    let error = capped
        .collect(2, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(error, DiffError::TooManyFiles));
    assert_eq!(error.code(), "DIFF_TOO_LARGE");
}

#[tokio::test]
async fn process_caps_keep_only_a_patch_prefix_and_reject_incomplete_status() {
    let fixture = RepositoryFixture::new("process-bounds");
    fixture.write("process.txt", b"before\n");
    fixture.commit_all("base");
    let after = (0..200)
        .map(|index| format!("after-{index:03}-{}\n", "x".repeat(64)))
        .collect::<String>();
    fixture.write("process.txt", after.as_bytes());

    let git_directory = fixture.repository.join(".git").canonicalize().unwrap();
    let work_tree = fixture.repository.canonicalize().unwrap();
    let limits = DiffLimits::try_new(
        Duration::from_secs(5),
        Duration::from_secs(5),
        4,
        4 * 1024,
        64 * 1024,
        64 * 1024,
    )
    .unwrap();
    let bounded = collector_for_with_process_limits(
        &fixture.runtime_directory,
        &git_directory,
        &work_tree,
        ProcessLimits::try_new(
            512,
            4 * 1024,
            Duration::from_secs(10),
            Duration::from_secs(3),
        )
        .unwrap(),
        limits,
    )
    .await;
    let snapshot = bounded.collect(9, CancellationToken::new()).await.unwrap();
    let changed = file(&snapshot, "process.txt");
    assert_eq!((changed.additions, changed.deletions), (200, 1));
    assert!(changed.truncated);
    assert!(changed.patch.starts_with("diff --git"));
    assert!(changed.patch.len() <= 256);
    assert!(!changed.patch.contains("after-199"));

    let status_limited = collector_for_with_process_limits(
        &fixture.runtime_directory,
        &git_directory,
        &work_tree,
        ProcessLimits::try_new(
            64,
            4 * 1024,
            Duration::from_secs(10),
            Duration::from_secs(3),
        )
        .unwrap(),
        limits,
    )
    .await;
    let error = status_limited
        .collect(10, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(error, DiffError::StatusOutputIncomplete));
    assert_eq!(error.code(), "DIFF_GIT_FAILED");
}

#[tokio::test]
async fn untracked_reads_are_capability_bounded_and_ignore_the_linked_git_pointer() {
    let fixture = RepositoryFixture::new("linked");
    fixture.write("tracked.txt", b"before\n");
    fixture.commit_all("base");

    let linked = fixture.root.join("linked-worktree");
    fixture.git(&[
        "worktree",
        "add",
        "--detach",
        linked.to_str().unwrap(),
        "HEAD",
    ]);
    let linked = linked.canonicalize().unwrap();
    let git_directory = git_output(&linked, &["rev-parse", "--absolute-git-dir"]);
    let git_directory = PathBuf::from(git_directory.trim()).canonicalize().unwrap();
    let runtime_directory = fixture.root.join("linked-runtime");
    std::fs::create_dir(&runtime_directory).unwrap();

    std::fs::write(linked.join("tracked.txt"), b"after\n").unwrap();
    std::fs::write(linked.join("plain.txt"), b"plain\n").unwrap();
    // The collector already has the trusted admin-directory capability. A
    // poisoned model-hidden pointer must neither redirect nor break it.
    std::fs::write(linked.join(".git"), b"gitdir: definitely-missing\n").unwrap();

    let collector = collector_for(
        &runtime_directory,
        &git_directory,
        &linked,
        DiffLimits::try_new(
            Duration::from_secs(5),
            Duration::from_secs(5),
            8,
            4 * 1024,
            16,
            64,
        )
        .unwrap(),
    )
    .await;
    let snapshot = collector
        .collect(3, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        file(&snapshot, "tracked.txt").status,
        DiffFileStatus::Modified
    );
    assert_eq!(file(&snapshot, "plain.txt").status, DiffFileStatus::Added);

    std::fs::write(linked.join("oversized.txt"), vec![b'x'; 17]).unwrap();
    let error = collector
        .collect(4, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(error, DiffError::UntrackedFileTooLarge));
    assert_eq!(error.code(), "DIFF_TOO_LARGE");
}

fn file<'a>(
    event: &'a coding_agent_core::DiffEvent,
    path: &str,
) -> &'a coding_agent_core::DiffFile {
    event
        .files
        .iter()
        .find(|file| file.path == path)
        .unwrap_or_else(|| panic!("missing diff file {path}"))
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
        let test_root = workspace_root.join("target/diff-tests");
        std::fs::create_dir_all(&test_root).unwrap();
        let temporary = tempfile::Builder::new()
            .prefix(&format!("{name}-"))
            .tempdir_in(test_root)
            .unwrap();
        // Keep the ordinary absolute spelling for commands invoked by the
        // fixture. On Windows, `canonicalize` adds a `\\?\` prefix that Git
        // for Windows does not accept as a worktree-add destination.
        let root = temporary.path().to_path_buf();
        assert!(root.is_absolute());
        let runtime_directory = root.join("runtime");
        let repository = root.join("repository");
        std::fs::create_dir(&runtime_directory).unwrap();
        std::fs::create_dir(&repository).unwrap();
        run_git(&repository, &["init", "--quiet"]);
        run_git(&repository, &["config", "user.name", "Diff Test"]);
        run_git(
            &repository,
            &["config", "user.email", "diff-test@example.invalid"],
        );
        Self {
            _temporary: temporary,
            root,
            runtime_directory,
            repository,
        }
    }

    fn write(&self, path: &str, content: &[u8]) {
        std::fs::write(self.repository.join(path), content).unwrap();
    }

    fn git(&self, arguments: &[&str]) {
        run_git(&self.repository, arguments);
    }

    fn commit_all(&self, message: &str) {
        self.git(&["add", "--all"]);
        self.git(&["commit", "--quiet", "--no-gpg-sign", "-m", message]);
    }

    async fn collector(
        &self,
        max_files: usize,
        max_patch_bytes: usize,
        max_untracked_file_bytes: u64,
    ) -> DiffCollector {
        let git_directory = self.repository.join(".git").canonicalize().unwrap();
        collector_for(
            &self.runtime_directory,
            &git_directory,
            &self.repository.canonicalize().unwrap(),
            DiffLimits::try_new(
                Duration::from_secs(5),
                Duration::from_secs(5),
                max_files,
                max_patch_bytes,
                max_untracked_file_bytes,
                max_untracked_file_bytes.saturating_mul(max_files as u64),
            )
            .unwrap(),
        )
        .await
    }
}

async fn collector_for(
    runtime_directory: &Path,
    git_directory: &Path,
    work_tree: &Path,
    limits: DiffLimits,
) -> DiffCollector {
    collector_for_with_process_limits(
        runtime_directory,
        git_directory,
        work_tree,
        ProcessLimits::try_new(
            256 * 1024,
            64 * 1024,
            Duration::from_secs(10),
            Duration::from_secs(3),
        )
        .unwrap(),
        limits,
    )
    .await
}

async fn collector_for_with_process_limits(
    runtime_directory: &Path,
    git_directory: &Path,
    work_tree: &Path,
    process_limits: ProcessLimits,
    limits: DiffLimits,
) -> DiffCollector {
    let rustc = concrete_rustc();
    let git = path_executable(if cfg!(windows) { "git.exe" } else { "git" });
    let toolchain = discover_toolchain(runtime_directory, Some(&rustc), Some(&git))
        .await
        .unwrap();
    DiffCollector::from_trusted_capabilities(
        &toolchain,
        Arc::new(ExecutionDirectory::open(git_directory).unwrap()),
        Arc::new(ExecutionDirectory::open(work_tree).unwrap()),
        runtime_directory,
        process_limits,
        limits,
    )
    .unwrap()
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
    assert!(
        status.success(),
        "fixture git command failed: {arguments:?}"
    );
}

fn git_output(repository: &Path, arguments: &[&str]) -> String {
    let output = Command::new("git")
        .arg("--no-pager")
        .arg("-C")
        .arg(repository)
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "fixture git command failed: {arguments:?}"
    );
    String::from_utf8(output.stdout).unwrap()
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
