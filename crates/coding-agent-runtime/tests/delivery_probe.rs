mod support;

use std::path::{Path, PathBuf};
#[cfg(feature = "test-support")]
use std::process::Command;
use std::sync::Arc;
#[cfg(feature = "test-support")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[cfg(feature = "test-support")]
use coding_agent_runtime::probe_delivery_git_with_after_initialize_hook_for_test;
use coding_agent_runtime::{
    DeliveryGitObjectFormat, ExecutionDirectory, PinnedExecutable, ProbedDeliveryGit,
    ProcessLimits, probe_delivery_git,
};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn real_private_repository_probe_returns_an_opaque_bound_handle_and_cleans_up() {
    let fixture = ProbeFixture::new();
    let handle = fixture.probe(CancellationToken::new()).await.unwrap();

    assert_send_sync::<ProbedDeliveryGit>();
    assert!(handle.version().is_at_least(2, 45));
    assert!(matches!(
        handle.object_format(),
        DeliveryGitObjectFormat::Sha1 | DeliveryGitObjectFormat::Sha256
    ));
    assert!(handle.supports_required_merge_options());
    assert!(handle.supports_merge_tree());
    assert!(handle.supports_atomic_ref_transaction());
    handle.verify_current_executable().unwrap();
    fixture.assert_probe_root_empty();

    let rendered = format!("{handle:?}");
    assert_eq!(rendered, "ProbedDeliveryGit(<opaque>)");
    assert!(!rendered.contains(&fixture.git_path.to_string_lossy().to_string()));
}

#[tokio::test]
async fn a_non_git_executable_fails_closed_without_leaving_the_probe_repository() {
    let mut fixture = ProbeFixture::new();
    fixture.git_path = std::env::current_exe().unwrap().canonicalize().unwrap();
    fixture.git = Arc::new(PinnedExecutable::open(&fixture.git_path).unwrap());

    let error = fixture.probe(CancellationToken::new()).await.unwrap_err();
    assert_eq!(error.code(), "DELIVERY_GIT_CAPABILITY_UNAVAILABLE");
    fixture.assert_probe_root_empty();
}

#[tokio::test]
async fn cancellation_before_spawn_fails_closed_and_cleans_the_private_directory() {
    let fixture = ProbeFixture::new();
    let cancellation = CancellationToken::new();
    cancellation.cancel();

    let error = fixture.probe(cancellation).await.unwrap_err();
    assert_eq!(error.code(), "DELIVERY_GIT_PROBE_CANCELLED");
    fixture.assert_probe_root_empty();
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn real_probe_merge_ignores_an_injected_repository_post_merge_hook() {
    let fixture = ProbeFixture::new();
    let sentinel = fixture.probe_root.join("post-merge-hook-ran");
    assert_post_merge_hook_is_executable(&fixture);
    let injected = Arc::new(AtomicBool::new(false));
    let injected_by_hook = Arc::clone(&injected);
    let hook_sentinel = sentinel.clone();

    let handle = fixture
        .probe_with_after_initialize_hook(CancellationToken::new(), move |repository| {
            install_post_merge_hook(
                &repository.join(".git").join("hooks").join("post-merge"),
                &hook_sentinel,
            );
            injected_by_hook.store(true, Ordering::SeqCst);
        })
        .await
        .unwrap();

    assert!(injected.load(Ordering::SeqCst));
    assert!(handle.supports_required_merge_options());
    assert!(
        !sentinel.exists(),
        "the repository post-merge hook executed despite the fixed hooks path"
    );
    fixture.assert_probe_root_empty();
}

fn assert_send_sync<T: Send + Sync>() {}

struct ProbeFixture {
    _temporary: tempfile::TempDir,
    runtime: PathBuf,
    probe_root: PathBuf,
    git_path: PathBuf,
    git: Arc<PinnedExecutable>,
    directory: Arc<ExecutionDirectory>,
}

impl ProbeFixture {
    fn new() -> Self {
        let target = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target");
        let temporary = tempfile::Builder::new()
            .prefix("delivery-git-probe-")
            .tempdir_in(target)
            .unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let runtime = root.join("runtime");
        let probe_root = root.join("probe");
        std::fs::create_dir(&runtime).unwrap();
        std::fs::create_dir(&probe_root).unwrap();
        let directory = Arc::new(ExecutionDirectory::open(&probe_root).unwrap());
        let git_path = path_executable(if cfg!(windows) { "git.exe" } else { "git" });
        let git = Arc::new(PinnedExecutable::open(&git_path).unwrap());
        Self {
            _temporary: temporary,
            runtime,
            probe_root,
            git_path,
            git,
            directory,
        }
    }

    async fn probe(
        &self,
        cancellation: CancellationToken,
    ) -> Result<ProbedDeliveryGit, coding_agent_runtime::DeliveryGitProbeError> {
        probe_delivery_git(
            Arc::clone(&self.git),
            Arc::clone(&self.directory),
            support::task_process_scope(&self.runtime),
            ProcessLimits::try_new(
                64 * 1024,
                64 * 1024,
                Duration::from_secs(30),
                Duration::from_secs(5),
            )
            .unwrap(),
            Duration::from_secs(15),
            cancellation,
        )
        .await
    }

    #[cfg(feature = "test-support")]
    async fn probe_with_after_initialize_hook(
        &self,
        cancellation: CancellationToken,
        after_initialize: impl Fn(&Path) + Send + Sync + 'static,
    ) -> Result<ProbedDeliveryGit, coding_agent_runtime::DeliveryGitProbeError> {
        probe_delivery_git_with_after_initialize_hook_for_test(
            Arc::clone(&self.git),
            Arc::clone(&self.directory),
            support::task_process_scope(&self.runtime),
            ProcessLimits::try_new(
                64 * 1024,
                64 * 1024,
                Duration::from_secs(30),
                Duration::from_secs(5),
            )
            .unwrap(),
            Duration::from_secs(15),
            cancellation,
            after_initialize,
        )
        .await
    }

    fn assert_probe_root_empty(&self) {
        assert_eq!(std::fs::read_dir(&self.probe_root).unwrap().count(), 0);
    }
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

#[cfg(feature = "test-support")]
fn install_post_merge_hook(hook: &Path, sentinel: &Path) {
    std::fs::create_dir_all(hook.parent().unwrap()).unwrap();
    let sentinel_name = sentinel.file_name().unwrap().to_string_lossy();
    std::fs::write(
        hook,
        format!("#!/bin/sh\nprintf 'executed\\n' > ../{sentinel_name}\n"),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        std::fs::set_permissions(hook, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

#[cfg(feature = "test-support")]
fn assert_post_merge_hook_is_executable(fixture: &ProbeFixture) {
    let repository = fixture._temporary.path().join("post-merge-hook-viability");
    let sentinel = fixture
        ._temporary
        .path()
        .join("post-merge-hook-viability-ran");
    std::fs::create_dir(&repository).unwrap();

    run_fixture_git(
        fixture,
        &repository,
        ["init", "--quiet", "--initial-branch=main"],
    );
    std::fs::write(repository.join("base.txt"), b"base\n").unwrap();
    run_fixture_git(fixture, &repository, ["add", "--all"]);
    commit_fixture_git(fixture, &repository, "base");
    run_fixture_git(
        fixture,
        &repository,
        ["checkout", "--quiet", "-b", "source"],
    );
    std::fs::write(repository.join("source.txt"), b"source\n").unwrap();
    run_fixture_git(fixture, &repository, ["add", "--all"]);
    commit_fixture_git(fixture, &repository, "source");
    run_fixture_git(fixture, &repository, ["checkout", "--quiet", "main"]);
    std::fs::write(repository.join("target.txt"), b"target\n").unwrap();
    run_fixture_git(fixture, &repository, ["add", "--all"]);
    commit_fixture_git(fixture, &repository, "target");

    install_post_merge_hook(&repository.join(".git/hooks/post-merge"), &sentinel);
    run_fixture_git(
        fixture,
        &repository,
        [
            "-c",
            "user.name=Coding Agent Hook Test",
            "-c",
            "user.email=hook-test@example.invalid",
            "merge",
            "--no-ff",
            "--no-edit",
            "source",
        ],
    );
    assert!(
        sentinel.exists(),
        "the injected post-merge fixture must be executable without the fixed hooks path"
    );
    std::fs::remove_file(sentinel).unwrap();
}

#[cfg(feature = "test-support")]
fn commit_fixture_git(fixture: &ProbeFixture, repository: &Path, message: &str) {
    run_fixture_git(
        fixture,
        repository,
        [
            "-c",
            "user.name=Coding Agent Hook Test",
            "-c",
            "user.email=hook-test@example.invalid",
            "commit",
            "--quiet",
            "--no-gpg-sign",
            "-m",
            message,
        ],
    );
}

#[cfg(feature = "test-support")]
fn run_fixture_git<const N: usize>(
    fixture: &ProbeFixture,
    repository: &Path,
    arguments: [&str; N],
) {
    let mut command = Command::new(&fixture.git_path);
    command
        .args(arguments)
        .current_dir(repository)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_SYSTEM", empty_git_config_endpoint())
        .env("GIT_CONFIG_GLOBAL", empty_git_config_endpoint());
    for key in [
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_KEY_0",
        "GIT_CONFIG_VALUE_0",
        "GIT_CONFIG_PARAMETERS",
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
    ] {
        command.env_remove(key);
    }
    let output = support::command_output(&mut command).unwrap();
    assert!(
        output.status.success(),
        "fixture git command {arguments:?} failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(feature = "test-support")]
const fn empty_git_config_endpoint() -> &'static str {
    if cfg!(windows) { "NUL" } else { "/dev/null" }
}
