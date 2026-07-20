mod support;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use coding_agent_runtime::{
    ExecutionDirectory, GitRunStatus, GitToolLimits, GitTools, ProcessLimits, discover_toolchain,
};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn typed_status_and_diff_observe_a_real_repository_without_call_arguments() {
    let test_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target");
    let temporary = tempfile::Builder::new()
        .prefix("typed-git-")
        .tempdir_in(test_root)
        .unwrap();
    let root = temporary.path().canonicalize().unwrap();
    let runtime_directory = root.join("runtime");
    let repository = root.join("repository");
    std::fs::create_dir(&runtime_directory).unwrap();
    std::fs::create_dir(&repository).unwrap();

    git(&repository, &["init", "--quiet"]);
    git(&repository, &["config", "user.name", "Typed Git Test"]);
    git(
        &repository,
        &["config", "user.email", "typed-git@example.invalid"],
    );
    std::fs::write(repository.join("tracked.txt"), b"before\n").unwrap();
    git(&repository, &["add", "--", "tracked.txt"]);
    git(
        &repository,
        &["commit", "--quiet", "--no-gpg-sign", "-m", "base"],
    );

    std::fs::write(repository.join("tracked.txt"), b"after\n").unwrap();
    std::fs::write(repository.join("untracked.txt"), b"new\n").unwrap();

    let repository = repository.canonicalize().unwrap();
    let git_directory = repository.join(".git").canonicalize().unwrap();
    let rustc = concrete_rustc();
    let git_executable = path_executable(if cfg!(windows) { "git.exe" } else { "git" });
    let toolchain = discover_toolchain(&runtime_directory, Some(&rustc), Some(&git_executable))
        .await
        .unwrap();
    let tools = GitTools::from_trusted_capabilities(
        &toolchain,
        Arc::new(ExecutionDirectory::open(git_directory).unwrap()),
        Arc::new(ExecutionDirectory::open(&repository).unwrap()),
        &runtime_directory,
        ProcessLimits::try_new(
            128 * 1024,
            128 * 1024,
            Duration::from_secs(10),
            Duration::from_secs(3),
        )
        .unwrap(),
        GitToolLimits::try_new(Duration::from_secs(5), Duration::from_secs(5)).unwrap(),
    )
    .unwrap();

    let status = tools.status(CancellationToken::new()).await.unwrap();
    assert_eq!(
        status.status,
        GitRunStatus::Succeeded,
        "git status stderr: {}{}",
        String::from_utf8_lossy(&status.command.stderr.head),
        String::from_utf8_lossy(&status.command.stderr.tail)
    );
    assert!(!status.command.stdout.truncated);
    assert!(
        status
            .command
            .stdout
            .head
            .windows(b"tracked.txt".len())
            .any(|window| window == b"tracked.txt")
    );
    assert!(
        status
            .command
            .stdout
            .head
            .windows(b"untracked.txt".len())
            .any(|window| window == b"untracked.txt")
    );

    let diff = tools.diff(CancellationToken::new()).await.unwrap();
    assert_eq!(
        diff.status,
        GitRunStatus::Succeeded,
        "git diff stderr: {}{}",
        String::from_utf8_lossy(&diff.command.stderr.head),
        String::from_utf8_lossy(&diff.command.stderr.tail)
    );
    assert!(!diff.command.stdout.truncated);
    assert!(
        diff.command
            .stdout
            .head
            .windows(b"+after".len())
            .any(|window| window == b"+after")
    );
    assert!(
        !diff
            .command
            .stdout
            .head
            .windows(b"untracked.txt".len())
            .any(|window| window == b"untracked.txt")
    );

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = tools.status(cancellation).await.unwrap();
    assert_eq!(cancelled.status, GitRunStatus::Cancelled);
    assert!(cancelled.command.cancelled);
}

fn git(repository: &Path, arguments: &[&str]) {
    let status = support::command_status(
        Command::new("git")
            .arg("--no-pager")
            .arg("-c")
            .arg(git_hooks_path_configuration())
            .arg("-C")
            .arg(repository)
            .args(arguments),
    )
    .unwrap();
    assert!(status.success(), "git fixture command failed");
}

fn concrete_rustc() -> PathBuf {
    let output =
        support::command_output(Command::new("rustc").args(["--print", "sysroot"])).unwrap();
    assert!(output.status.success());
    let sysroot = String::from_utf8(output.stdout).unwrap();
    PathBuf::from(sysroot.trim())
        .join("bin")
        .join(if cfg!(windows) { "rustc.exe" } else { "rustc" })
        .canonicalize()
        .unwrap()
}

fn path_executable(name: &str) -> PathBuf {
    let path = std::env::var_os("PATH").unwrap();
    std::env::split_paths(&path)
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
