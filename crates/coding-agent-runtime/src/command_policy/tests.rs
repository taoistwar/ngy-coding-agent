use std::num::NonZeroU32;

use super::*;
use crate::process_supervisor::{ProcessError, ProcessLimits, ProcessSupervisor};
use tokio_util::sync::CancellationToken;

fn canonical_test_root(temporary: &tempfile::TempDir) -> PathBuf {
    std::fs::canonicalize(temporary.path()).unwrap()
}

fn command_fixture() -> (
    tempfile::TempDir,
    Arc<PinnedExecutable>,
    Arc<ExecutionDirectory>,
) {
    let temporary = tempfile::tempdir().unwrap();
    let root = canonical_test_root(&temporary);
    let tool = root.join(if cfg!(windows) { "tool.exe" } else { "tool" });
    std::fs::copy(std::env::current_exe().unwrap(), &tool).unwrap();
    make_executable(&tool);
    let executable = Arc::new(PinnedExecutable::open(&tool).unwrap());
    let directory = Arc::new(ExecutionDirectory::open(root).unwrap());
    (temporary, executable, directory)
}

fn cargo_jobs_per_task() -> NonZeroU32 {
    NonZeroU32::new(3).expect("test Cargo jobs are nonzero")
}

#[cfg(unix)]
fn make_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[cfg(windows)]
fn make_executable(_: &Path) {}

fn arguments(command: &ValidatedCommand) -> Vec<String> {
    command
        .arguments()
        .iter()
        .map(|argument| argument.to_string_lossy().into_owned())
        .collect()
}

#[test]
fn pinned_executable_rejects_relative_directory_and_non_executable_paths() {
    let temporary = tempfile::tempdir().unwrap();
    let root = canonical_test_root(&temporary);
    assert!(matches!(
        PinnedExecutable::open(Path::new("tool")),
        Err(CommandPolicyError::RelativePath)
    ));
    assert!(PinnedExecutable::open(&root).is_err());

    let non_executable = root.join("plain.txt");
    std::fs::write(&non_executable, b"not an executable").unwrap();
    assert!(matches!(
        PinnedExecutable::open(non_executable),
        Err(CommandPolicyError::NotExecutable)
    ));

    #[cfg(windows)]
    {
        let fake_image = root.join("plain.exe");
        std::fs::write(&fake_image, b"not a PE image").unwrap();
        assert!(matches!(
            PinnedExecutable::open(fake_image),
            Err(CommandPolicyError::NotExecutable)
        ));
    }
}

#[test]
fn execution_directory_rejects_relative_and_file_paths() {
    let temporary = tempfile::tempdir().unwrap();
    let root = canonical_test_root(&temporary);
    assert!(matches!(
        ExecutionDirectory::open(Path::new("relative")),
        Err(CommandPolicyError::RelativePath)
    ));
    let file = root.join("file");
    std::fs::write(&file, b"file").unwrap();
    assert!(ExecutionDirectory::open(file).is_err());
}

#[test]
fn retained_execution_directory_requires_the_expected_namespace_identity() {
    let original = tempfile::tempdir().unwrap();
    let replacement = tempfile::tempdir().unwrap();
    let original_path = canonical_test_root(&original);
    let replacement_path = canonical_test_root(&replacement);
    let retained = RootCapability::open(&original_path)
        .unwrap()
        .try_clone_root()
        .unwrap();

    assert!(matches!(
        ExecutionDirectory::from_retained_directory(&replacement_path, retained),
        Err(CommandPolicyError::IdentityChanged)
    ));
}

#[cfg(unix)]
#[test]
fn retained_execution_directory_rejects_a_namespace_replacement_during_handoff() {
    let temporary = tempfile::tempdir().unwrap();
    let original = canonical_test_root(&temporary);
    let retained = std::fs::File::open(&original).unwrap();
    let held = original.with_extension("held");
    let replacement = original.with_extension("replacement");
    std::fs::create_dir(&replacement).unwrap();
    std::fs::rename(&original, &held).unwrap();
    std::fs::rename(&replacement, &original).unwrap();

    assert!(matches!(
        ExecutionDirectory::from_retained_directory(&original, retained),
        Err(CommandPolicyError::IdentityChanged)
    ));
}

#[test]
fn executable_and_directory_namespace_replacement_is_detected() {
    let (_temporary, executable, directory) = command_fixture();
    let executable_path = executable.path().to_owned();
    #[cfg(unix)]
    {
        let held_executable = directory.path().join("held");
        std::fs::rename(&executable_path, &held_executable).unwrap();
        std::fs::copy(std::env::current_exe().unwrap(), &executable_path).unwrap();
        make_executable(&executable_path);
        assert!(matches!(
            executable.revalidate(),
            Err(CommandPolicyError::IdentityChanged)
        ));
        assert!(matches!(
            ValidatedCommand::cargo_metadata(
                Arc::clone(&executable),
                Arc::clone(&directory),
                ChildEnvironment::default(),
                Duration::from_secs(10),
            ),
            Err(CommandPolicyError::IdentityChanged)
        ));
    }
    #[cfg(windows)]
    {
        let held_executable = directory.path().join("held.exe");
        assert!(std::fs::rename(&executable_path, &held_executable).is_err());
        assert!(
            std::fs::OpenOptions::new()
                .write(true)
                .open(&executable_path)
                .is_err()
        );
        assert!(executable.revalidate().is_ok());
    }

    let directory_fixture = tempfile::tempdir().unwrap();
    let original_directory_path = canonical_test_root(&directory_fixture);
    let directory = ExecutionDirectory::open(&original_directory_path).unwrap();
    let replacement = original_directory_path.with_extension("replacement");
    std::fs::create_dir(&replacement).unwrap();
    let held_directory = original_directory_path.with_extension("held");
    std::fs::rename(&original_directory_path, &held_directory).unwrap();
    std::fs::rename(&replacement, &original_directory_path).unwrap();
    assert!(matches!(
        directory.revalidate(),
        Err(CommandPolicyError::IdentityChanged)
    ));

    std::fs::remove_dir(&original_directory_path).unwrap();
    std::fs::rename(&held_directory, &original_directory_path).unwrap();
}

#[cfg(unix)]
#[test]
fn executable_in_place_rewrite_is_detected_by_snapshot_and_digest() {
    let (_temporary, executable, _directory) = command_fixture();
    let mut image = std::fs::read(executable.path()).unwrap();
    let last = image.last_mut().unwrap();
    *last ^= 1;
    std::fs::write(executable.path(), image).unwrap();
    make_executable(executable.path());

    assert!(matches!(
        executable.revalidate(),
        Err(CommandPolicyError::IdentityChanged)
    ));
}

#[test]
fn rustc_sysroot_argv_is_fixed() {
    let (_temporary, executable, directory) = command_fixture();
    let command = ValidatedCommand::rustc_sysroot(
        executable,
        directory,
        ChildEnvironment::default(),
        Duration::from_secs(15),
    )
    .unwrap();

    assert_eq!(arguments(&command), ["--print", "sysroot"]);
    #[cfg(unix)]
    assert_eq!(command.unix_argv0(), Some(std::ffi::OsStr::new("rustc")));
}

#[test]
fn git_version_argv_is_fixed() {
    let (_temporary, executable, directory) = command_fixture();
    let command = ValidatedCommand::git_version(
        executable,
        directory,
        ChildEnvironment::default(),
        Duration::from_secs(15),
    )
    .unwrap();

    assert_eq!(arguments(&command), ["--version"]);
}

#[test]
fn cargo_argv_is_fixed_and_selectors_cannot_inject_options_or_paths() {
    let (_temporary, executable, directory) = command_fixture();
    let metadata = ValidatedCommand::cargo_metadata(
        Arc::clone(&executable),
        Arc::clone(&directory),
        ChildEnvironment::default(),
        Duration::from_secs(10),
    )
    .unwrap();
    assert_eq!(
        arguments(&metadata),
        ["metadata", "--format-version=1", "--no-deps", "--offline"]
    );
    let read_only_metadata = ValidatedCommand::cargo_metadata_read_only(
        Arc::clone(&executable),
        Arc::clone(&directory),
        ChildEnvironment::default(),
        Duration::from_secs(10),
    )
    .unwrap();
    assert_eq!(
        arguments(&read_only_metadata),
        ["metadata", "--format-version=1", "--no-deps", "--frozen"]
    );

    let check = ValidatedCommand::cargo_check(
        Arc::clone(&executable),
        Arc::clone(&directory),
        ChildEnvironment::default(),
        cargo_jobs_per_task(),
        Some("safe-package"),
        Duration::from_secs(10),
    )
    .unwrap();
    assert_eq!(
        arguments(&check),
        [
            "check",
            "--offline",
            "--color=never",
            "--message-format=json-render-diagnostics",
            "--jobs=3",
            "--package",
            "safe-package",
        ]
    );
    let workspace_check = ValidatedCommand::cargo_check(
        Arc::clone(&executable),
        Arc::clone(&directory),
        ChildEnvironment::default(),
        cargo_jobs_per_task(),
        None,
        Duration::from_secs(10),
    )
    .unwrap();
    assert_eq!(
        arguments(&workspace_check),
        [
            "check",
            "--offline",
            "--color=never",
            "--message-format=json-render-diagnostics",
            "--jobs=3",
            "--workspace",
        ]
    );

    let test = ValidatedCommand::cargo_test(
        Arc::clone(&executable),
        Arc::clone(&directory),
        ChildEnvironment::default(),
        cargo_jobs_per_task(),
        Some("safe-package"),
        Some("integration_test"),
        Duration::from_secs(10),
    )
    .unwrap();
    assert_eq!(
        arguments(&test),
        [
            "test",
            "--offline",
            "--color=never",
            "--no-fail-fast",
            "--message-format=json-render-diagnostics",
            "--jobs=3",
            "--package",
            "safe-package",
            "--test",
            "integration_test",
        ]
    );
    let workspace_test = ValidatedCommand::cargo_test(
        Arc::clone(&executable),
        Arc::clone(&directory),
        ChildEnvironment::default(),
        cargo_jobs_per_task(),
        None,
        None,
        Duration::from_secs(10),
    )
    .unwrap();
    assert_eq!(
        arguments(&workspace_test),
        [
            "test",
            "--offline",
            "--color=never",
            "--no-fail-fast",
            "--message-format=json-render-diagnostics",
            "--jobs=3",
            "--workspace",
        ]
    );
    for command in [&check, &workspace_check, &test, &workspace_test] {
        assert_eq!(
            arguments(command)
                .into_iter()
                .filter(|argument| argument.starts_with("--jobs="))
                .collect::<Vec<_>>(),
            ["--jobs=3"]
        );
        assert!(
            !command
                .environment()
                .entries()
                .contains_key(&OsString::from("CARGO_BUILD_JOBS"))
        );
        assert!(
            !command
                .environment()
                .entries()
                .contains_key(&OsString::from("RUST_TEST_THREADS"))
        );
    }
    assert!(matches!(
        ValidatedCommand::cargo_test(
            Arc::clone(&executable),
            Arc::clone(&directory),
            ChildEnvironment::default(),
            cargo_jobs_per_task(),
            None,
            Some("integration_test"),
            Duration::from_secs(10),
        ),
        Err(CommandPolicyError::InvalidCargoSelection)
    ));

    for invalid in [
        "",
        "--config",
        "--jobs=999",
        "../escape",
        "name=value",
        "with space",
        "unicode-工具",
    ] {
        assert!(matches!(
            ValidatedCommand::cargo_check(
                Arc::clone(&executable),
                Arc::clone(&directory),
                ChildEnvironment::default(),
                cargo_jobs_per_task(),
                Some(invalid),
                Duration::from_secs(10),
            ),
            Err(CommandPolicyError::InvalidCargoSelection)
        ));
    }
    assert!(matches!(
        ValidatedCommand::cargo_test(
            Arc::clone(&executable),
            Arc::clone(&directory),
            ChildEnvironment::default(),
            cargo_jobs_per_task(),
            Some("safe-package"),
            Some("--manifest-path"),
            Duration::from_secs(10),
        ),
        Err(CommandPolicyError::InvalidCargoSelection)
    ));

    for key in [
        "CARGO_BUILD_JOBS",
        "cargo_build_jobs",
        "RUST_TEST_THREADS",
        "rust_test_threads",
    ] {
        let environment =
            ChildEnvironment::from_entries([(OsString::from(key), OsString::from("999"))]);
        assert!(matches!(
            ValidatedCommand::cargo_test(
                Arc::clone(&executable),
                Arc::clone(&directory),
                environment,
                cargo_jobs_per_task(),
                None,
                None,
                Duration::from_secs(10),
            ),
            Err(CommandPolicyError::InvalidCargoEnvironment)
        ));
    }

    for existing in ["--jobs", "--jobs=9", "-j", "-j9", "-j=9"] {
        let mut arguments = vec![OsString::from("test"), OsString::from(existing)];
        assert!(matches!(
            append_trusted_cargo_jobs(&mut arguments, cargo_jobs_per_task()),
            Err(CommandPolicyError::InvalidCargoSelection)
        ));
    }
}

#[test]
fn git_argv_uses_only_prebuilt_bindings_and_fixed_read_only_operations() {
    let (_temporary, executable, work_tree) = command_fixture();
    let git_directory_path = work_tree.path().join("git-metadata");
    std::fs::create_dir(&git_directory_path).unwrap();
    let git_directory = Arc::new(ExecutionDirectory::open(&git_directory_path).unwrap());
    assert!(matches!(
        GitCommandBinding::try_new(Arc::clone(&work_tree), Arc::clone(&work_tree)),
        Err(CommandPolicyError::InvalidGitBinding)
    ));
    let binding = GitCommandBinding::try_new(git_directory, Arc::clone(&work_tree)).unwrap();

    let status = ValidatedCommand::git_status(
        Arc::clone(&executable),
        &binding,
        ChildEnvironment::default(),
        Duration::from_secs(10),
    )
    .unwrap();
    let status_arguments = arguments(&status);
    assert_eq!(
        &status_arguments[..5],
        [
            "--no-pager",
            "--literal-pathspecs",
            "--no-optional-locks",
            "--no-replace-objects",
            "--no-lazy-fetch",
        ]
    );
    assert_eq!(
        status_arguments[5],
        format!(
            "--git-dir={}",
            child_visible_path(&git_directory_path).display()
        )
    );
    assert_eq!(
        status_arguments[6],
        format!(
            "--work-tree={}",
            child_visible_path(work_tree.path()).display()
        )
    );
    assert_eq!(
        &status_arguments[7..],
        [
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.untrackedCache=false",
            "-c",
            "submodule.recurse=false",
            "-c",
            "core.sparseCheckout=false",
            "-c",
            "core.sparseCheckoutCone=false",
            "-c",
            "worktree.useRelativePaths=false",
            "-c",
            "gc.auto=0",
            "-c",
            "maintenance.auto=false",
            "-c",
            "core.excludesFile=",
            "-c",
            "core.attributesFile=",
            "-c",
            git_hooks_path_configuration(),
            "-c",
            "diff.external=",
            "-c",
            "diff.renames=false",
            "status",
            "--porcelain=v2",
            "--untracked-files=all",
            "--ignore-submodules=all",
            "--no-renames",
            "-z",
        ]
    );
    assert_eq!(status.working_directory().path(), work_tree.path());

    let diff = ValidatedCommand::git_diff(
        executable,
        &binding,
        ChildEnvironment::default(),
        Duration::from_secs(10),
    )
    .unwrap();
    assert_eq!(
        &arguments(&diff)[33..],
        [
            "-c",
            "core.quotePath=true",
            "diff",
            "--no-color",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            "--ignore-submodules=all",
            "--binary",
            "--full-index",
            "--src-prefix=a/",
            "--dst-prefix=b/",
            "HEAD",
            "--",
        ]
    );
}

#[cfg(unix)]
#[test]
fn delivery_descriptor_slots_are_fixed_without_a_mutable_hooks_binding() {
    let (_temporary, executable, work_tree) = command_fixture();
    let git_directory_path = work_tree.path().join("delivery-git-directory");
    let common_git_path = work_tree.path().join("delivery-common-git");
    for directory in [&git_directory_path, &common_git_path] {
        std::fs::create_dir(directory).unwrap();
    }
    let git_directory = Arc::new(ExecutionDirectory::open(&git_directory_path).unwrap());
    let common_git = Arc::new(ExecutionDirectory::open(&common_git_path).unwrap());
    let binding =
        GitCommandBinding::try_new(Arc::clone(&git_directory), Arc::clone(&work_tree)).unwrap();
    let mut command_arguments = binding.delivery_fixed_arguments();
    command_arguments.push(OsString::from("status"));
    let command = ValidatedCommand::build_git(
        executable,
        &binding,
        command_arguments,
        ChildEnvironment::default(),
        Duration::from_secs(10),
    )
    .unwrap()
    .with_dependent_directories(vec![
        Arc::clone(&git_directory),
        Arc::clone(&work_tree),
        Arc::clone(&common_git),
    ])
    .unwrap();

    let command = command
        .with_delivery_unix_directory_bindings(UnixDeliveryDirectoryBindings::repository(
            Arc::clone(&git_directory),
            Arc::clone(&work_tree),
            Arc::clone(&common_git),
        ))
        .unwrap();
    let roles = command
        .unix_delivery_directory_bindings()
        .unwrap()
        .bindings()
        .iter()
        .map(UnixDeliveryDirectoryBinding::role)
        .collect::<Vec<_>>();
    assert_eq!(
        roles,
        vec![
            UnixDeliveryDirectoryRole::GitDirectory {
                argument_index: DELIVERY_GIT_DIRECTORY_ARGUMENT_INDEX,
            },
            UnixDeliveryDirectoryRole::WorkTree {
                argument_index: DELIVERY_WORK_TREE_ARGUMENT_INDEX,
            },
            UnixDeliveryDirectoryRole::CommonGitEnvironment,
        ]
    );
}

#[tokio::test]
async fn git_directory_replacement_after_construction_is_rejected_before_spawn() {
    let (_temporary, executable, work_tree) = command_fixture();
    let git_directory_path = work_tree.path().join("git-metadata-replaced");
    std::fs::create_dir(&git_directory_path).unwrap();
    let git_directory = Arc::new(ExecutionDirectory::open(&git_directory_path).unwrap());
    let binding =
        GitCommandBinding::try_new(Arc::clone(&git_directory), Arc::clone(&work_tree)).unwrap();
    let command = ValidatedCommand::git_status(
        executable,
        &binding,
        ChildEnvironment::default(),
        Duration::from_secs(10),
    )
    .unwrap();
    assert_eq!(command.dependent_directories().len(), 2);

    let held_directory_path = work_tree.path().join("git-metadata-held");
    std::fs::rename(&git_directory_path, &held_directory_path).unwrap();
    std::fs::create_dir(&git_directory_path).unwrap();

    let limits = ProcessLimits::try_new(
        4 * 1024,
        4 * 1024,
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .unwrap();
    let error = ProcessSupervisor::new(limits, crate::process_liveness::test_process_scope())
        .run(command, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ProcessError::CommandPolicy(CommandPolicyError::IdentityChanged)
    ));

    std::fs::remove_dir(&git_directory_path).unwrap();
    std::fs::rename(&held_directory_path, &git_directory_path).unwrap();
}

#[test]
fn per_path_git_diff_commands_are_literal_read_only_and_path_validated() {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap();
    let test_root = workspace_root.join("target/command-policy-diff-tests");
    std::fs::create_dir_all(&test_root).unwrap();
    let temporary = tempfile::Builder::new()
        .prefix("literal-path-")
        .tempdir_in(test_root)
        .unwrap();
    let root = canonical_test_root(&temporary);
    let tool = root.join(if cfg!(windows) { "tool.exe" } else { "tool" });
    std::fs::copy(std::env::current_exe().unwrap(), &tool).unwrap();
    make_executable(&tool);
    let executable = Arc::new(PinnedExecutable::open(&tool).unwrap());
    let work_tree = Arc::new(ExecutionDirectory::open(root).unwrap());
    let git_directory_path = work_tree.path().join("git-metadata-path-diff");
    std::fs::create_dir(&git_directory_path).unwrap();
    let git_directory = Arc::new(ExecutionDirectory::open(&git_directory_path).unwrap());
    let binding = GitCommandBinding::try_new(git_directory, work_tree).unwrap();
    let path = OsStr::new("sub dir/-option.txt");

    let numstat = ValidatedCommand::git_diff_numstat_path(
        Arc::clone(&executable),
        &binding,
        ChildEnvironment::default(),
        path,
        Duration::from_secs(10),
    )
    .unwrap();
    let numstat_arguments = arguments(&numstat);
    assert_eq!(
        &numstat_arguments[numstat_arguments.len() - 11..],
        [
            "diff",
            "--no-color",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            "--ignore-submodules=all",
            "--numstat",
            "-z",
            "HEAD",
            "--",
            "sub dir/-option.txt",
        ]
    );

    let patch = ValidatedCommand::git_diff_patch_path(
        Arc::clone(&executable),
        &binding,
        ChildEnvironment::default(),
        path,
        Duration::from_secs(10),
    )
    .unwrap();
    let patch_arguments = arguments(&patch);
    assert_eq!(
        &patch_arguments[patch_arguments.len() - 15..],
        [
            "-c",
            "core.quotePath=true",
            "diff",
            "--no-color",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            "--ignore-submodules=all",
            "--binary",
            "--full-index",
            "--src-prefix=a/",
            "--dst-prefix=b/",
            "HEAD",
            "--",
            "sub dir/-option.txt",
        ]
    );

    let mut invalid_paths = vec!["", "../escape", ".GIT/config", "dir//file"];
    if cfg!(windows) {
        invalid_paths.extend(["C:/escape", "file:stream"]);
    } else {
        invalid_paths.push("/absolute");
    }
    for invalid in invalid_paths {
        assert!(matches!(
            ValidatedCommand::git_diff_numstat_path(
                Arc::clone(&executable),
                &binding,
                ChildEnvironment::default(),
                OsStr::new(invalid),
                Duration::from_secs(10),
            ),
            Err(CommandPolicyError::InvalidGitPath)
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::ffi::{OsStrExt, OsStringExt};

        let non_utf8 = OsString::from_vec(b"nonutf8-\xff.txt".to_vec());
        let command = ValidatedCommand::git_diff_numstat_path(
            executable,
            &binding,
            ChildEnvironment::default(),
            &non_utf8,
            Duration::from_secs(10),
        )
        .unwrap();
        assert_eq!(
            command.arguments().last().unwrap().as_bytes(),
            non_utf8.as_bytes()
        );
    }
}

#[test]
fn zero_timeout_is_never_a_validated_command() {
    let (_temporary, executable, directory) = command_fixture();
    assert!(matches!(
        ValidatedCommand::cargo_metadata(
            executable,
            directory,
            ChildEnvironment::default(),
            Duration::ZERO,
        ),
        Err(CommandPolicyError::InvalidTimeout)
    ));
}

#[cfg(unix)]
#[test]
fn symlinked_executable_and_directory_are_rejected() {
    let temporary = tempfile::tempdir().unwrap();
    let root = canonical_test_root(&temporary);
    let executable_target = root.join("target");
    std::fs::copy(std::env::current_exe().unwrap(), &executable_target).unwrap();
    make_executable(&executable_target);
    let executable_link = root.join("link");
    std::os::unix::fs::symlink(&executable_target, &executable_link).unwrap();
    assert!(PinnedExecutable::open(executable_link).is_err());

    let directory_target = root.join("directory-target");
    std::fs::create_dir(&directory_target).unwrap();
    let directory_link = root.join("directory-link");
    std::os::unix::fs::symlink(&directory_target, &directory_link).unwrap();
    assert!(ExecutionDirectory::open(directory_link).is_err());
}

#[cfg(windows)]
#[test]
fn reparse_point_executable_and_directory_are_rejected_when_links_are_available() {
    let temporary = tempfile::tempdir().unwrap();
    let root = canonical_test_root(&temporary);
    let executable_target = root.join("target.exe");
    std::fs::copy(std::env::current_exe().unwrap(), &executable_target).unwrap();
    let executable_link = root.join("link.exe");
    if std::os::windows::fs::symlink_file(&executable_target, &executable_link).is_ok() {
        assert!(PinnedExecutable::open(executable_link).is_err());
    }

    let directory_target = root.join("directory-target");
    std::fs::create_dir(&directory_target).unwrap();
    let directory_link = root.join("directory-link");
    if std::os::windows::fs::symlink_dir(&directory_target, &directory_link).is_ok() {
        assert!(ExecutionDirectory::open(directory_link).is_err());
    }
}

#[cfg(windows)]
#[test]
fn spawn_directory_lease_blocks_ancestor_rebinding_only_while_held() {
    let temporary = tempfile::tempdir().unwrap();
    let ancestor_path = canonical_test_root(&temporary).join("ancestor");
    let directory_path = ancestor_path.join("working");
    std::fs::create_dir_all(&directory_path).unwrap();
    let directory = ExecutionDirectory::open(&directory_path).unwrap();
    let held = ancestor_path.with_extension("held");

    let leases = directory.acquire_spawn_path_leases().unwrap();
    drop(directory);
    assert!(std::fs::rename(&ancestor_path, &held).is_err());
    drop(leases);
    std::fs::rename(&ancestor_path, &held).unwrap();
    std::fs::rename(&held, &ancestor_path).unwrap();
}
