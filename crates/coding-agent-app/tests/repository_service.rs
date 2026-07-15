mod support;

use std::ffi::OsString;
use std::io;
use std::path::Path;
use std::sync::Arc;

use coding_agent_app::{PickerError, RepositoryDiscovery};
use support::{LockState, RealRepositoryFixture, RecordingRunner};

#[tokio::test]
async fn invalid_selected_paths_fail_with_stable_codes_before_any_command() {
    let temp = tempfile::tempdir().expect("create invalid-path fixture");
    let runtime = temp.path().join("runtime");
    std::fs::create_dir(&runtime).expect("create neutral runtime directory");
    let runner = Arc::new(RecordingRunner::default());
    let discovery = RepositoryDiscovery::with_runner(runtime, runner.clone());

    let missing = discovery
        .discover(&temp.path().join("missing"))
        .await
        .expect_err("missing selection fails");
    assert_eq!(missing.code(), "REPOSITORY_PATH_NOT_FOUND");

    let file = temp.path().join("ordinary-file");
    std::fs::write(&file, b"not a directory").expect("create ordinary file");
    let not_directory = discovery
        .discover(&file)
        .await
        .expect_err("file selection fails");
    assert_eq!(not_directory.code(), "REPOSITORY_PATH_NOT_DIRECTORY");
    assert!(
        runner.calls().is_empty(),
        "invalid paths must invoke no commands"
    );
}

#[tokio::test]
async fn discovery_runs_only_approved_commands_and_uses_the_nearest_manifest() {
    let temp = tempfile::tempdir().expect("create scripted discovery fixture");
    let git_root = temp.path().join("repository");
    let workspace = git_root.join("nested-workspace");
    let selected = workspace.join("member").join("deep");
    let runtime = temp.path().join("runtime");
    std::fs::create_dir_all(&selected).expect("create selected directory");
    std::fs::create_dir(&runtime).expect("create neutral runtime directory");
    std::fs::write(git_root.join("Cargo.toml"), b"[workspace]\n").expect("write outer manifest");
    std::fs::write(workspace.join("Cargo.toml"), b"[workspace]\n").expect("write nearest manifest");
    let runner = Arc::new(RecordingRunner::scripted([
        Ok(format!("{}\n", git_root.display()).into_bytes()),
        Ok(format!("{}\n", workspace.join("Cargo.toml").display()).into_bytes()),
    ]));
    let discovery = RepositoryDiscovery::with_runner(runtime.clone(), runner.clone());

    let found = discovery
        .discover(&selected)
        .await
        .expect("discover repository");

    let selected = selected.canonicalize().unwrap();
    let workspace = workspace.canonicalize().unwrap();
    assert_eq!(found.selected_path, selected);
    assert_eq!(found.git_root, git_root.canonicalize().unwrap());
    assert_eq!(found.cargo_workspace_root, workspace);
    assert_eq!(found.display_name, "repository");
    let calls = runner.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].program, "git");
    assert_eq!(
        calls[0].args,
        os_args([
            "-C",
            selected.to_str().unwrap(),
            "rev-parse",
            "--show-toplevel",
        ])
    );
    assert_eq!(calls[1].program, "cargo");
    assert_eq!(calls[1].current_dir, runtime);
    assert_eq!(
        calls[1].args,
        os_args([
            "locate-project",
            "--workspace",
            "--manifest-path",
            workspace.join("Cargo.toml").to_str().unwrap(),
            "--message-format",
            "plain",
        ])
    );
}

#[tokio::test]
async fn a_symlinked_selection_is_normalized_before_commands_and_results() {
    let temp = tempfile::tempdir().expect("create symlinked-selection fixture");
    let git_root = temp.path().join("repository");
    let workspace = git_root.join("workspace");
    let selected_target = workspace.join("selected");
    let selected_link = temp.path().join("selected-link");
    let runtime = temp.path().join("runtime");
    std::fs::create_dir_all(&selected_target).expect("create selected target");
    std::fs::create_dir(&runtime).expect("create runtime directory");
    std::fs::write(workspace.join("Cargo.toml"), b"[workspace]\n")
        .expect("write workspace manifest");
    create_directory_symlink(&selected_target, &selected_link)
        .expect("create selected-directory symlink");
    let runner = Arc::new(RecordingRunner::scripted([
        Ok(format!("{}\n", git_root.display()).into_bytes()),
        Ok(format!("{}\n", workspace.join("Cargo.toml").display()).into_bytes()),
    ]));
    let discovery = RepositoryDiscovery::with_runner(runtime, runner.clone());

    let found = discovery
        .discover(&selected_link)
        .await
        .expect("discover through symlink");
    let normalized = selected_target.canonicalize().unwrap();
    assert_eq!(found.selected_path, normalized);
    assert_eq!(
        runner.calls()[0].args[1],
        normalized.as_os_str(),
        "Git receives the normalized target rather than the symlink spelling"
    );
}

#[tokio::test]
async fn missing_manifest_outside_workspace_and_command_failures_have_safe_stable_codes() {
    let temp = tempfile::tempdir().expect("create discovery-error fixture");
    let git_root = temp.path().join("repository");
    let selected = git_root.join("selected");
    let runtime = temp.path().join("runtime");
    std::fs::create_dir_all(&selected).expect("create selected directory");
    std::fs::create_dir(&runtime).expect("create runtime directory");

    let missing_runner = Arc::new(RecordingRunner::scripted([Ok(format!(
        "{}\n",
        git_root.display()
    )
    .into_bytes())]));
    let missing = RepositoryDiscovery::with_runner(runtime.clone(), missing_runner.clone())
        .discover(&selected)
        .await
        .expect_err("missing manifest fails");
    assert_eq!(missing.code(), "CARGO_WORKSPACE_NOT_FOUND");
    assert_eq!(
        missing_runner.calls().len(),
        1,
        "Cargo is not run without a manifest"
    );

    std::fs::write(git_root.join("Cargo.toml"), b"[workspace]\n")
        .expect("write repository manifest");
    let outside = temp.path().join("outside");
    std::fs::create_dir(&outside).expect("create outside workspace");
    std::fs::write(outside.join("Cargo.toml"), b"[workspace]\n").expect("write outside manifest");
    let outside_runner = Arc::new(RecordingRunner::scripted([
        Ok(format!("{}\n", git_root.display()).into_bytes()),
        Ok(format!("{}\n", outside.join("Cargo.toml").display()).into_bytes()),
    ]));
    let outside_error = RepositoryDiscovery::with_runner(runtime.clone(), outside_runner)
        .discover(&selected)
        .await
        .expect_err("outside workspace fails");
    assert_eq!(outside_error.code(), "CARGO_WORKSPACE_OUTSIDE_GIT_ROOT");

    let command_runner = Arc::new(RecordingRunner::scripted([Err(io::ErrorKind::NotFound)]));
    let command_error = RepositoryDiscovery::with_runner(runtime.clone(), command_runner)
        .discover(&selected)
        .await
        .expect_err("spawn failure is mapped");
    assert_eq!(command_error.code(), "REPOSITORY_COMMAND_FAILED");
    assert!(!command_error.to_string().contains("No such file"));

    let utf8_runner = Arc::new(RecordingRunner::scripted([Ok(vec![0xff, 0xfe])]));
    let utf8_error = RepositoryDiscovery::with_runner(runtime, utf8_runner)
        .discover(&selected)
        .await
        .expect_err("invalid UTF-8 is mapped");
    assert_eq!(utf8_error.code(), "REPOSITORY_COMMAND_FAILED");
}

#[tokio::test]
async fn real_discovery_is_read_only_for_lock_states_and_ignores_unavailable_toolchain() {
    for lock_state in [LockState::Missing, LockState::Stale, LockState::Dirty] {
        let fixture = RealRepositoryFixture::new(lock_state);
        let before = fixture.fingerprint();

        let found = RepositoryDiscovery::new(fixture.runtime.clone())
            .discover(&fixture.selected)
            .await
            .unwrap_or_else(|error| panic!("discover {lock_state:?} fixture: {error}"));

        assert_eq!(found.git_root, fixture.git_root.canonicalize().unwrap());
        assert_eq!(
            found.cargo_workspace_root,
            fixture.workspace.canonicalize().unwrap()
        );
        assert_eq!(
            before,
            fixture.fingerprint(),
            "discovery changed {lock_state:?} fixture"
        );
    }
}

#[test]
fn picker_error_code_is_stable() {
    assert_eq!(PickerError::AlreadyOpen.code(), "PICKER_ALREADY_OPEN");
}

fn os_args<const N: usize>(values: [&str; N]) -> Vec<OsString> {
    values.into_iter().map(OsString::from).collect()
}

#[cfg(unix)]
fn create_directory_symlink(target: &Path, link: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_directory_symlink(target: &Path, link: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    use windows_sys::Win32::Storage::FileSystem::{
        CreateSymbolicLinkW, SYMBOLIC_LINK_FLAG_ALLOW_UNPRIVILEGED_CREATE,
        SYMBOLIC_LINK_FLAG_DIRECTORY,
    };

    let link = link
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let target = target
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    if unsafe {
        CreateSymbolicLinkW(
            link.as_ptr(),
            target.as_ptr(),
            SYMBOLIC_LINK_FLAG_DIRECTORY | SYMBOLIC_LINK_FLAG_ALLOW_UNPRIVILEGED_CREATE,
        )
    } {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
