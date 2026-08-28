use std::collections::BTreeMap;
use std::ffi::OsStr;

use super::*;

struct ReadOnlyBindingFixture {
    _temporary: tempfile::TempDir,
    binding: DeliveryGitReadOnlyBinding,
    #[cfg(unix)]
    git_directory_path: std::path::PathBuf,
    #[cfg(unix)]
    work_tree_path: std::path::PathBuf,
}

impl ReadOnlyBindingFixture {
    fn new() -> Self {
        let target_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target");
        std::fs::create_dir_all(&target_root).unwrap();
        let temporary = tempfile::Builder::new()
            .prefix("delivery-git-policy-")
            .tempdir_in(target_root)
            .unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let git_directory_path = root.join("git-directory");
        let work_tree_path = root.join("work-tree");
        let common_git_path = root.join("common-git");
        let sandbox_path = root.join("sandbox");
        for directory in [
            &git_directory_path,
            &work_tree_path,
            &common_git_path,
            &sandbox_path,
        ] {
            std::fs::create_dir(directory).unwrap();
        }
        let git_directory = Arc::new(ExecutionDirectory::open(&git_directory_path).unwrap());
        let work_tree = Arc::new(ExecutionDirectory::open(&work_tree_path).unwrap());
        let repository = GitCommandBinding::try_new(git_directory, Arc::clone(&work_tree)).unwrap();
        let sandbox = Arc::new(ExecutionDirectory::open(&sandbox_path).unwrap());
        #[cfg(unix)]
        let config = {
            let config_path = sandbox_path.join(".coding-agent-empty-gitconfig");
            std::fs::write(&config_path, b"").unwrap();
            let config_file = std::fs::File::open(&config_path).unwrap();
            std::fs::remove_file(&config_path).unwrap();
            Arc::new(
                super::super::DeliveryGitEmptyConfig::from_retained_sandbox_file(
                    Arc::clone(&sandbox),
                    config_file,
                )
                .unwrap(),
            )
        };
        #[cfg(windows)]
        let config = Arc::new(super::super::DeliveryGitEmptyConfig::windows_nul());
        let mut environment_entries = BTreeMap::new();
        config
            .apply_delivery_git_environment(&mut environment_entries)
            .unwrap();
        Self {
            _temporary: temporary,
            binding: DeliveryGitReadOnlyBinding {
                git: Arc::new(PinnedExecutable::open(std::env::current_exe().unwrap()).unwrap()),
                repository,
                common_git: Arc::new(ExecutionDirectory::open(&common_git_path).unwrap()),
                sandbox,
                config,
                environment: ChildEnvironment::from_entries(environment_entries),
                timeout: Duration::from_secs(10),
            },
            #[cfg(unix)]
            git_directory_path,
            #[cfg(unix)]
            work_tree_path,
        }
    }

    fn source_mutations(&self, object_id_length: usize) -> DeliveryGitSourceMutationBinding {
        DeliveryGitSourceMutationBinding::try_new(
            DeliveryGitMutationCommandFactory {
                git: Arc::clone(&self.binding.git),
            },
            &self.binding,
            object_id_length,
        )
        .unwrap()
    }

    fn target_mutations(&self, object_id_length: usize) -> DeliveryGitTargetMutationBinding {
        DeliveryGitTargetMutationBinding::from_read_only_for_test(
            DeliveryGitMutationCommandFactory {
                git: Arc::clone(&self.binding.git),
            },
            &self.binding,
            object_id_length,
        )
        .unwrap()
    }

    fn temporary_index(&self) -> DeliveryGitTemporaryIndexEnvironment {
        let path = self
            ._temporary
            .path()
            .canonicalize()
            .unwrap()
            .join("temporary-index");
        std::fs::create_dir(&path).unwrap();
        DeliveryGitTemporaryIndexEnvironment::try_new(Arc::new(
            ExecutionDirectory::open(&path).unwrap(),
        ))
        .unwrap()
    }
}

fn assert_command_tail(command: &ValidatedCommand, expected: &[&str]) {
    let arguments = command
        .arguments()
        .iter()
        .map(|argument| argument.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let expected = expected
        .iter()
        .map(|argument| (*argument).to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        &arguments[arguments.len() - expected.len()..],
        expected.as_slice()
    );
}

fn cleanup_target_authority<'fixture>(
    fixture: &'fixture ReadOnlyBindingFixture,
    path: &'fixture Path,
) -> DeliveryCleanupTargetAuthority<'fixture> {
    DeliveryCleanupTargetAuthority {
        git: &fixture.binding.git,
        binding: &fixture.binding.repository,
        path,
    }
}

fn build_cleanup_command(
    fixture: &ReadOnlyBindingFixture,
    target: DeliveryCleanupTargetAuthority<'_>,
    operation: DeliveryCleanupCommand,
) -> Result<ValidatedCommand, CommandPolicyError> {
    build_delivery_cleanup_command_with_authority(
        &DeliveryGitMutationCommandFactory {
            git: Arc::clone(&fixture.binding.git),
        },
        target,
        Arc::clone(&fixture.binding.sandbox),
        Arc::clone(&fixture.binding.config),
        fixture.binding.environment.clone(),
        fixture.binding.timeout,
        operation,
    )
}

#[test]
fn cleanup_commands_have_exact_primary_bound_argv_and_no_removal_leases() {
    let fixture = ReadOnlyBindingFixture::new();
    let root = fixture._temporary.path().canonicalize().unwrap();
    let target_path = root.join("cleanup-target");
    let admin_path = root.join("cleanup-admin");
    std::fs::create_dir(&target_path).unwrap();
    std::fs::create_dir(&admin_path).unwrap();
    let target = cleanup_target_authority(&fixture, &target_path);
    let source_branch = "codex/task-task_16-attempt-7";
    let source_ref = DeliveryGitSourceRef::try_new(source_branch).unwrap();
    let source_commit = source_ref.as_str().to_owned();

    let mut resolve = build_cleanup_command(
        &fixture,
        target,
        DeliveryCleanupCommand::ResolveSourceRef(source_commit.clone()),
    )
    .unwrap();
    let mut symbolic = build_cleanup_command(
        &fixture,
        target,
        DeliveryCleanupCommand::SourceRefSymbolic(source_commit.clone()),
    )
    .unwrap();
    let mut unlock =
        build_cleanup_command(&fixture, target, DeliveryCleanupCommand::Unlock).unwrap();
    let mut remove =
        build_cleanup_command(&fixture, target, DeliveryCleanupCommand::Remove).unwrap();

    let mut expected_prefix = fixture.binding.repository.delivery_fixed_arguments();
    append_delivery_read_only_configuration(&mut expected_prefix);
    let mut expected_resolve = expected_prefix.clone();
    expected_resolve.extend([
        OsString::from("rev-parse"),
        OsString::from("--verify"),
        OsString::from("--quiet"),
        OsString::from("--end-of-options"),
        OsString::from(&source_commit),
    ]);
    let mut expected_symbolic = expected_prefix.clone();
    expected_symbolic.extend([
        OsString::from("symbolic-ref"),
        OsString::from("--quiet"),
        OsString::from("--no-recurse"),
        OsString::from("--"),
        OsString::from(&source_commit),
    ]);
    let mut expected_unlock = expected_prefix.clone();
    expected_unlock.extend([
        OsString::from("worktree"),
        OsString::from("unlock"),
        OsString::from("--"),
        child_visible_path(&target_path).into_os_string(),
    ]);
    let mut expected_remove = expected_prefix;
    expected_remove.extend([
        OsString::from("worktree"),
        OsString::from("remove"),
        OsString::from("--"),
        child_visible_path(&target_path).into_os_string(),
    ]);
    assert_eq!(resolve.arguments(), expected_resolve);
    assert_eq!(symbolic.arguments(), expected_symbolic);
    assert_eq!(unlock.arguments(), expected_unlock);
    assert_eq!(remove.arguments(), expected_remove);

    for command in [&mut resolve, &mut symbolic, &mut unlock, &mut remove] {
        assert!(command.take_exact_input().is_none());
        assert!(command.delivery_git_empty_config().is_some());
        assert!(
            command
                .working_directory()
                .has_same_identity(fixture.binding.repository.work_tree())
        );
        assert_eq!(command.dependent_directories().len(), 3);
        assert!(command.dependent_directories().iter().all(|directory| {
            directory.path() != target_path && directory.path() != admin_path
        }));
        for forbidden in ["--force", "-f"] {
            assert!(
                !command
                    .arguments()
                    .iter()
                    .any(|argument| argument == OsStr::new(forbidden))
            );
        }
        for forbidden in [
            "GIT_INDEX_FILE",
            "GIT_AUTHOR_NAME",
            "GIT_AUTHOR_EMAIL",
            "GIT_AUTHOR_DATE",
            "GIT_COMMITTER_NAME",
            "GIT_COMMITTER_EMAIL",
            "GIT_COMMITTER_DATE",
        ] {
            assert!(
                !command
                    .environment()
                    .entries()
                    .contains_key(OsStr::new(forbidden))
            );
        }
        #[cfg(unix)]
        assert!(
            command
                .unix_delivery_directory_bindings()
                .unwrap()
                .bindings()
                .iter()
                .all(|binding| {
                    binding.directory().path() != target_path
                        && binding.directory().path() != admin_path
                })
        );
    }
}

#[test]
fn cleanup_source_ref_accepts_only_the_canonical_task_branch_namespace() {
    assert!(
        ValidatedCommand::validate_delivery_cleanup_source_branch("codex/task-task_16-attempt-7")
            .is_ok()
    );
    for invalid in [
        "refs/heads/codex/task-task_16-attempt-7",
        "main",
        "--help",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "codex/task-task_16-attempt-0",
        "codex/task-task_16-attempt-01",
        "codex/task-task_16-attempt-4294967296",
        "codex/task-Task16-attempt-7",
    ] {
        assert!(matches!(
            ValidatedCommand::validate_delivery_cleanup_source_branch(invalid),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
    }
}

#[test]
fn cleanup_commands_reject_invalid_or_unbound_targets() {
    let fixture = ReadOnlyBindingFixture::new();
    let relative = Path::new("relative-cleanup-target");
    assert!(matches!(
        build_cleanup_command(
            &fixture,
            cleanup_target_authority(&fixture, relative),
            DeliveryCleanupCommand::Unlock,
        ),
        Err(CommandPolicyError::InvalidGitBinding)
    ));

    let aliased = fixture.binding.repository.work_tree().path();
    assert!(matches!(
        build_cleanup_command(
            &fixture,
            cleanup_target_authority(&fixture, aliased),
            DeliveryCleanupCommand::Remove,
        ),
        Err(CommandPolicyError::InvalidGitBinding)
    ));

    let target_path = fixture
        ._temporary
        .path()
        .canonicalize()
        .unwrap()
        .join("unbound-target");
    std::fs::create_dir(&target_path).unwrap();
    let other_git = Arc::new(PinnedExecutable::open(std::env::current_exe().unwrap()).unwrap());
    let unbound = DeliveryCleanupTargetAuthority {
        git: &other_git,
        binding: &fixture.binding.repository,
        path: &target_path,
    };
    assert!(matches!(
        build_cleanup_command(&fixture, unbound, DeliveryCleanupCommand::Unlock),
        Err(CommandPolicyError::InvalidGitBinding)
    ));
}

fn branch_cleanup_binding(
    fixture: &ReadOnlyBindingFixture,
    source_branch: &str,
    target_branch: &str,
    expected_source: &str,
    expected_target: &str,
) -> DeliveryGitBranchCleanupBinding {
    let format = crate::delivery::DeliveryGitObjectFormat::Sha1;
    let source = crate::delivery::DeliveryCommitOid::try_new(expected_source, format).unwrap();
    let target = crate::delivery::DeliveryCommitOid::try_new(expected_target, format).unwrap();
    DeliveryGitBranchCleanupBinding::try_new(
        fixture.target_mutations(40),
        source_branch,
        target_branch,
        &source,
        &target,
    )
    .unwrap()
}

#[test]
fn branch_cleanup_commands_have_exact_primary_bound_argv() {
    let fixture = ReadOnlyBindingFixture::new();
    let source_branch = "codex/task-task_17-attempt-3";
    let target_branch = "release/v4";
    let source = "1".repeat(40);
    let target = "2".repeat(40);
    let fresh_target = "3".repeat(40);
    let fresh_target_oid = crate::delivery::DeliveryCommitOid::try_new(
        &fresh_target,
        crate::delivery::DeliveryGitObjectFormat::Sha1,
    )
    .unwrap();
    let binding = branch_cleanup_binding(&fixture, source_branch, target_branch, &source, &target);

    let mut source_symbolic =
        ValidatedCommand::delivery_branch_cleanup_source_ref_symbolic(&binding).unwrap();
    let mut target_symbolic =
        ValidatedCommand::delivery_branch_cleanup_target_ref_symbolic(&binding).unwrap();
    let mut source_ref =
        ValidatedCommand::delivery_branch_cleanup_resolve_source_ref(&binding).unwrap();
    let mut target_ref =
        ValidatedCommand::delivery_branch_cleanup_resolve_target_ref(&binding).unwrap();
    let mut source_commit =
        ValidatedCommand::delivery_branch_cleanup_expected_source_commit(&binding).unwrap();
    let mut target_commit =
        ValidatedCommand::delivery_branch_cleanup_expected_target_commit(&binding).unwrap();
    let mut fresh_commit =
        ValidatedCommand::delivery_branch_cleanup_fresh_target_commit(&binding, &fresh_target_oid)
            .unwrap();
    let mut source_ancestry =
        ValidatedCommand::delivery_branch_cleanup_source_is_ancestor(&binding, &fresh_target_oid)
            .unwrap();
    let mut target_ancestry =
        ValidatedCommand::delivery_branch_cleanup_target_is_ancestor(&binding, &fresh_target_oid)
            .unwrap();
    let mut worktrees = ValidatedCommand::delivery_branch_cleanup_worktree_list(&binding).unwrap();
    let mut deletion = ValidatedCommand::delivery_branch_cleanup_delete_source(&binding).unwrap();

    let source_ref_name = format!("refs/heads/{source_branch}");
    let target_ref_name = format!("refs/heads/{target_branch}");
    let mut expected_prefix = fixture.binding.repository.delivery_fixed_arguments();
    append_delivery_read_only_configuration(&mut expected_prefix);
    let expected = |tail: &[&str]| {
        let mut arguments = expected_prefix.clone();
        arguments.extend(tail.iter().map(OsString::from));
        arguments
    };

    assert_eq!(
        source_symbolic.arguments(),
        expected(&[
            "symbolic-ref",
            "--quiet",
            "--no-recurse",
            "--",
            &source_ref_name,
        ])
    );
    assert_eq!(
        target_symbolic.arguments(),
        expected(&[
            "symbolic-ref",
            "--quiet",
            "--no-recurse",
            "--",
            &target_ref_name,
        ])
    );
    assert_eq!(
        source_ref.arguments(),
        expected(&[
            "rev-parse",
            "--verify",
            "--quiet",
            "--end-of-options",
            &source_ref_name,
        ])
    );
    assert_eq!(
        target_ref.arguments(),
        expected(&[
            "rev-parse",
            "--verify",
            "--quiet",
            "--end-of-options",
            &target_ref_name,
        ])
    );
    assert_eq!(
        source_commit.arguments(),
        expected(&["cat-file", "--batch"])
    );
    assert_eq!(
        target_commit.arguments(),
        expected(&["cat-file", "--batch"])
    );
    assert_eq!(fresh_commit.arguments(), expected(&["cat-file", "--batch"]));
    assert_eq!(
        source_ancestry.arguments(),
        expected(&["merge-base", "--is-ancestor", &source, &fresh_target])
    );
    assert_eq!(
        target_ancestry.arguments(),
        expected(&["merge-base", "--is-ancestor", &target, &fresh_target])
    );
    assert_eq!(
        worktrees.arguments(),
        expected(&["worktree", "list", "--porcelain", "-z"])
    );
    assert_eq!(
        deletion.arguments(),
        expected(&["update-ref", "--no-deref", "--stdin"])
    );

    for command in [
        &mut source_symbolic,
        &mut target_symbolic,
        &mut source_ref,
        &mut target_ref,
        &mut source_ancestry,
        &mut target_ancestry,
        &mut worktrees,
    ] {
        assert!(command.take_exact_input().is_none());
    }
    for command in [&mut source_commit, &mut target_commit, &mut fresh_commit] {
        assert_eq!(
            format!("{:?}", command.take_exact_input().unwrap()),
            "ExactChildInput(<redacted>)"
        );
    }
    assert_eq!(
        format!("{:?}", deletion.take_exact_input().unwrap()),
        "ExactChildInput(<redacted>)"
    );
    assert_eq!(deletion.dependent_directories().len(), 4);
    assert!(
        deletion
            .working_directory()
            .has_same_identity(fixture.binding.repository.work_tree())
    );
    assert!(deletion.delivery_git_empty_config().is_some());
}

#[test]
fn branch_cleanup_transaction_is_byte_exact_and_fully_redacted() {
    let fixture = ReadOnlyBindingFixture::new();
    let source = "a".repeat(40);
    let target = "b".repeat(40);
    let binding = branch_cleanup_binding(
        &fixture,
        "codex/task-task_17-attempt-9",
        "main",
        &source,
        &target,
    );
    let expected = format!(
            "start\nverify refs/heads/main {target}\ndelete refs/heads/codex/task-task_17-attempt-9 {source}\nprepare\ncommit\n"
        )
        .into_bytes();
    assert_eq!(
        delivery_branch_cleanup_transaction_input(&binding),
        expected
    );

    let mut deletion = ValidatedCommand::delivery_branch_cleanup_delete_source(&binding).unwrap();
    let input_debug = format!("{:?}", deletion.take_exact_input().unwrap());
    let binding_debug = format!("{binding:?}");
    let command_debug = format!("{deletion:?}");
    assert_eq!(input_debug, "ExactChildInput(<redacted>)");
    assert_eq!(binding_debug, "DeliveryGitBranchCleanupBinding(<opaque>)");
    assert_eq!(command_debug, "ValidatedCommand(<opaque>)");
    for secret in [
        source.as_str(),
        target.as_str(),
        "refs/heads/main",
        "refs/heads/codex/task-task_17-attempt-9",
    ] {
        assert!(!input_debug.contains(secret));
        assert!(!binding_debug.contains(secret));
        assert!(!command_debug.contains(secret));
    }
}

#[test]
fn branch_cleanup_binding_rejects_ref_and_object_injection() {
    let fixture = ReadOnlyBindingFixture::new();
    let source = "1".repeat(40);
    let target = "2".repeat(40);
    let format = crate::delivery::DeliveryGitObjectFormat::Sha1;
    let source_oid = crate::delivery::DeliveryCommitOid::try_new(&source, format).unwrap();
    let target_oid = crate::delivery::DeliveryCommitOid::try_new(&target, format).unwrap();
    for invalid_target in [
        "",
        "-main",
        ".hidden",
        "feature//main",
        "feature/.hidden",
        "feature/main.lock",
        "feature/main\ncommit",
        "feature/@{main",
        "codex/task-task_17-attempt-3",
    ] {
        assert!(matches!(
            DeliveryGitBranchCleanupBinding::try_new(
                fixture.target_mutations(40),
                "codex/task-task_17-attempt-3",
                invalid_target,
                &source_oid,
                &target_oid,
            ),
            Err(crate::delivery::DeliverySourceError::AuthenticationChanged)
        ));
    }

    let sha256_source = crate::delivery::DeliveryCommitOid::try_new(
        &"3".repeat(64),
        crate::delivery::DeliveryGitObjectFormat::Sha256,
    )
    .unwrap();
    assert!(matches!(
        DeliveryGitBranchCleanupBinding::try_new(
            fixture.target_mutations(40),
            "codex/task-task_17-attempt-3",
            "main",
            &sha256_source,
            &target_oid,
        ),
        Err(crate::delivery::DeliverySourceError::AuthenticationChanged)
    ));
    assert!(matches!(
        ValidatedCommand::validate_delivery_branch_cleanup_binding(
            &fixture.target_mutations(40),
            "refs/heads/codex/task-task_17-attempt-03",
            "refs/heads/main",
            &source_oid,
            &target_oid,
        ),
        Err(CommandPolicyError::InvalidGitBinding)
    ));
}

#[test]
fn required_merge_arguments_are_exact_and_ordered() {
    let source = ProbeGitObjectId::try_new(&"a".repeat(40), 40).unwrap();
    let expected = [
        "merge",
        "--no-ff",
        "--strategy=ort",
        "--no-edit",
        "--no-verify",
        "--no-verify-signatures",
        "--no-gpg-sign",
        "--no-autostash",
        "--no-rerere-autoupdate",
        "--no-overwrite-ignore",
        "--no-log",
        "--no-stat",
        "--cleanup=verbatim",
        "-m",
        "coding-agent: delivery capability probe",
        "--",
        source.as_str(),
    ]
    .into_iter()
    .map(OsString::from)
    .collect::<Vec<_>>();
    assert_eq!(merge_arguments(&source), expected);
}

#[test]
fn ref_transaction_is_byte_exact_and_its_input_debug_is_redacted() {
    let target = ProbeGitObjectId::try_new(&"1".repeat(40), 40).unwrap();
    let source = ProbeGitObjectId::try_new(&"2".repeat(40), 40).unwrap();
    let input = delete_source_transaction_input(&target, &source);
    assert_eq!(
            input,
            b"start\nverify refs/heads/main 1111111111111111111111111111111111111111\ndelete refs/heads/coding-agent-probe-source 2222222222222222222222222222222222222222\nprepare\ncommit\n"
        );
    let exact = ExactChildInput::try_new(input).unwrap();
    assert_eq!(format!("{exact:?}"), "ExactChildInput(<redacted>)");
}

#[test]
fn mutation_factory_requires_the_exact_authorized_arc() {
    let authorized = Arc::new(PinnedExecutable::open(std::env::current_exe().unwrap()).unwrap());
    let independently_opened =
        Arc::new(PinnedExecutable::open(std::env::current_exe().unwrap()).unwrap());
    let factory = DeliveryGitMutationCommandFactory {
        git: Arc::clone(&authorized),
    };

    factory.revalidate_for(&authorized).unwrap();
    assert!(matches!(
        factory.require_same_executable(&independently_opened),
        Err(CommandPolicyError::InvalidGitBinding)
    ));
}

#[test]
fn source_mutation_commands_have_fixed_no_filter_argv_and_retained_temp_index_authority() {
    let fixture = ReadOnlyBindingFixture::new();
    let commands = fixture.source_mutations(40);
    let temporary_index = fixture.temporary_index();
    let tree = "b".repeat(40);
    let base = "d".repeat(40);

    let mut hash_snapshot = commands
        .hash_snapshot_file(
            ExactChildInput::try_new(b"exact no-follow snapshot bytes".to_vec()).unwrap(),
        )
        .unwrap();
    let mut update_index = commands
        .update_index_info(
            &temporary_index,
            ExactChildInput::try_new(
                format!("100644 {}\ttracked.txt\0", "c".repeat(40)).into_bytes(),
            )
            .unwrap(),
        )
        .unwrap();
    let write_tree = commands.write_tree(&temporary_index).unwrap();

    assert_command_tail(
        &hash_snapshot,
        &["hash-object", "-w", "--no-filters", "--stdin"],
    );
    assert_command_tail(
        &update_index,
        &["update-index", "--add", "--replace", "-z", "--index-info"],
    );
    assert_command_tail(&write_tree, &["write-tree"]);
    assert!(hash_snapshot.take_exact_input().is_some());
    assert!(update_index.take_exact_input().is_some());

    #[cfg(unix)]
    let expected_index = OsString::from(DELIVERY_GIT_TEMPORARY_INDEX_SENTINEL);
    #[cfg(windows)]
    let expected_index = temporary_index.index_file_path();
    for command in [&update_index, &write_tree] {
        assert_eq!(
            command
                .environment()
                .entries()
                .get(OsStr::new("GIT_INDEX_FILE"))
                .map(OsString::as_os_str),
            Some(expected_index.as_os_str())
        );
        assert!(
            command
                .dependent_directories()
                .iter()
                .any(|directory| Arc::ptr_eq(directory, temporary_index.directory()))
        );
        assert!(Arc::ptr_eq(
            command
                .delivery_git_empty_config()
                .expect("source mutation retains empty config authority"),
            &fixture.binding.config,
        ));
        assert!(
            command
                .arguments()
                .iter()
                .all(|argument| argument != &expected_index)
        );
        #[cfg(unix)]
        assert!(
            command
                .unix_delivery_directory_bindings()
                .expect("temporary-index command has descriptor bindings")
                .bindings()
                .iter()
                .any(|binding| matches!(
                    binding.role(),
                    super::super::UnixDeliveryDirectoryRole::TemporaryIndexEnvironment
                ))
        );
    }
    assert!(
        !hash_snapshot
            .environment()
            .entries()
            .contains_key(OsStr::new("GIT_INDEX_FILE"))
    );
    assert!(
        hash_snapshot
            .dependent_directories()
            .iter()
            .all(|directory| !Arc::ptr_eq(directory, temporary_index.directory()))
    );

    let input = ExactChildInput::try_new(b"subject\n".to_vec()).unwrap();
    let metadata = DeliveryGitCommitEnvironment::try_new(1_700_000_000).unwrap();
    let mut commit_tree = commands
        .commit_tree(&tree, &base, input, &metadata)
        .unwrap();
    assert_command_tail(
        &commit_tree,
        &["commit-tree", "--no-gpg-sign", &tree, "-p", &base],
    );
    assert!(commit_tree.take_exact_input().is_some());
    assert!(
        !commit_tree
            .environment()
            .entries()
            .contains_key(OsStr::new("GIT_INDEX_FILE"))
    );
    for (key, value) in [
        ("GIT_AUTHOR_NAME", "Coding Agent"),
        ("GIT_AUTHOR_EMAIL", "coding-agent@localhost"),
        ("GIT_AUTHOR_DATE", "1700000000 +0000"),
        ("GIT_COMMITTER_NAME", "Coding Agent"),
        ("GIT_COMMITTER_EMAIL", "coding-agent@localhost"),
        ("GIT_COMMITTER_DATE", "1700000000 +0000"),
    ] {
        assert_eq!(
            commit_tree
                .environment()
                .entries()
                .get(OsStr::new(key))
                .map(OsString::as_os_str),
            Some(OsStr::new(value))
        );
    }

    assert_eq!(
        cat_file_batch_input(&tree),
        format!("{tree}\n").into_bytes()
    );
    let mut cat_file = commands.cat_file_commit(&tree).unwrap();
    assert_command_tail(&cat_file, &["cat-file", "--batch"]);
    assert!(cat_file.take_exact_input().is_some());
}

#[test]
fn target_mutation_commands_have_exact_double_parent_and_actual_merge_argv() {
    let fixture = ReadOnlyBindingFixture::new();
    let commands = fixture.target_mutations(40);
    let source = "a".repeat(40);
    let target = "b".repeat(40);
    let tree = "c".repeat(40);
    let message = "coding-agent: merge task 123e4567-e89b-12d3-a456-426614174000 attempt 7\n";
    let metadata = DeliveryGitCommitEnvironment::try_new(1_700_000_000).unwrap();

    let mut expected = commands
        .commit_merge_tree(
            &tree,
            &target,
            &source,
            ExactChildInput::try_new(message.as_bytes().to_vec()).unwrap(),
            &metadata,
        )
        .unwrap();
    let mut inspect = commands.cat_file_commit(&target).unwrap();
    let mut actual = commands.merge(&source, message, &metadata).unwrap();

    assert_command_tail(
        &expected,
        &[
            "commit-tree",
            "--no-gpg-sign",
            &tree,
            "-p",
            &target,
            "-p",
            &source,
        ],
    );
    assert_command_tail(&inspect, &["cat-file", "--batch"]);
    assert_command_tail(
        &actual,
        &[
            "merge",
            "--no-ff",
            "--strategy=ort",
            "--no-edit",
            "--no-verify",
            "--no-verify-signatures",
            "--no-gpg-sign",
            "--no-autostash",
            "--no-rerere-autoupdate",
            "--no-overwrite-ignore",
            "--no-log",
            "--no-stat",
            "--cleanup=verbatim",
            "-m",
            message,
            "--",
            &source,
        ],
    );
    assert!(expected.take_exact_input().is_some());
    assert!(inspect.take_exact_input().is_some());
    assert!(actual.take_exact_input().is_none());

    for command in [&expected, &inspect, &actual] {
        assert!(
            !command
                .environment()
                .entries()
                .contains_key(OsStr::new("GIT_INDEX_FILE"))
        );
        assert!(Arc::ptr_eq(
            command
                .delivery_git_empty_config()
                .expect("target mutation retains empty config authority"),
            &fixture.binding.config,
        ));
        #[cfg(unix)]
        assert!(
            command
                .unix_delivery_directory_bindings()
                .expect("target mutation has repository descriptor bindings")
                .bindings()
                .iter()
                .all(|binding| !matches!(
                    binding.role(),
                    super::super::UnixDeliveryDirectoryRole::TemporaryIndexEnvironment
                ))
        );
    }
    for command in [&expected, &actual] {
        for (key, value) in [
            ("GIT_AUTHOR_NAME", "Coding Agent"),
            ("GIT_AUTHOR_EMAIL", "coding-agent@localhost"),
            ("GIT_AUTHOR_DATE", "1700000000 +0000"),
            ("GIT_COMMITTER_NAME", "Coding Agent"),
            ("GIT_COMMITTER_EMAIL", "coding-agent@localhost"),
            ("GIT_COMMITTER_DATE", "1700000000 +0000"),
        ] {
            assert_eq!(
                command
                    .environment()
                    .entries()
                    .get(OsStr::new(key))
                    .map(OsString::as_os_str),
                Some(OsStr::new(value))
            );
        }
    }

    for invalid in ["a".repeat(39), "A".repeat(40), "0".repeat(40)] {
        assert!(matches!(
            commands.merge(&invalid, message, &metadata),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
        assert!(matches!(
            commands.cat_file_commit(&invalid),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
    }
    assert!(matches!(
        commands.commit_merge_tree(
            &tree,
            &target,
            &target,
            ExactChildInput::try_new(b"subject\n".to_vec()).unwrap(),
            &metadata,
        ),
        Err(CommandPolicyError::InvalidGitBinding)
    ));
    for invalid_message in [
        "coding-agent: merge task 123e4567-e89b-12d3-a456-426614174000 attempt 0",
        "coding-agent: merge task 123e4567-e89b-12d3-a456-426614174000 attempt 07",
        "coding-agent: merge task 123e4567-e89b-12d3-a456-426614174000 attempt 4294967296",
        "coding-agent: merge task 123E4567-e89b-12d3-a456-426614174000 attempt 7",
        "coding-agent: merge task 123e4567-e89b-12d3-a456-426614174000 attempt 7",
        "coding-agent: merge task 123e4567-e89b-12d3-a456-426614174000 attempt 7\nextra",
    ] {
        assert!(matches!(
            commands.merge(&source, invalid_message, &metadata),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
    }
}

#[test]
fn target_merge_abort_has_fixed_zero_input_argv_and_retained_authority() {
    let fixture = ReadOnlyBindingFixture::new();
    let commands = fixture.target_mutations(40);
    let mut abort = commands.merge_abort().unwrap();

    assert_command_tail(&abort, &["merge", "--abort"]);
    assert!(abort.take_exact_input().is_none());
    assert!(
        !abort
            .environment()
            .entries()
            .contains_key(OsStr::new("GIT_INDEX_FILE"))
    );
    assert!(Arc::ptr_eq(
        abort
            .delivery_git_empty_config()
            .expect("target merge abort retains empty config authority"),
        &fixture.binding.config,
    ));
    #[cfg(unix)]
    assert!(
        abort
            .unix_delivery_directory_bindings()
            .expect("target merge abort has repository descriptor bindings")
            .bindings()
            .iter()
            .all(|binding| !matches!(
                binding.role(),
                super::super::UnixDeliveryDirectoryRole::TemporaryIndexEnvironment
            ))
    );
    for key in [
        "GIT_AUTHOR_NAME",
        "GIT_AUTHOR_EMAIL",
        "GIT_AUTHOR_DATE",
        "GIT_COMMITTER_NAME",
        "GIT_COMMITTER_EMAIL",
        "GIT_COMMITTER_DATE",
    ] {
        assert!(!abort.environment().entries().contains_key(OsStr::new(key)));
    }
}

#[test]
fn deleted_base_paths_has_fixed_no_rename_no_external_diff_argv_without_stdin() {
    let fixture = ReadOnlyBindingFixture::new();
    let commands = fixture.source_mutations(40);
    let base = "d".repeat(40);
    let mut command = commands.deleted_base_paths(&base).unwrap();

    assert_command_tail(
        &command,
        &[
            "diff-index",
            "--cached",
            "--no-renames",
            "--no-ext-diff",
            "--diff-filter=D",
            "--name-only",
            "-z",
            &base,
            "--",
        ],
    );
    assert!(
        command
            .arguments()
            .iter()
            .all(|argument| argument != OsStr::new("--stdin"))
    );
    assert!(command.take_exact_input().is_none());
    assert!(
        !command
            .environment()
            .entries()
            .contains_key(OsStr::new("GIT_INDEX_FILE"))
    );
    assert!(Arc::ptr_eq(
        command
            .delivery_git_empty_config()
            .expect("deleted-path command retains empty config authority"),
        &fixture.binding.config,
    ));

    for invalid in [
        "d".repeat(39),
        "d".repeat(41),
        "D".repeat(40),
        format!("{}-", "d".repeat(39)),
        "0".repeat(40),
    ] {
        assert!(matches!(
            commands.deleted_base_paths(&invalid),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
    }
}

#[test]
fn target_preflight_commands_have_exact_fixed_argv_and_typed_object_inputs() {
    let fixture = ReadOnlyBindingFixture::new();
    let source = "a".repeat(40);
    let target = "b".repeat(40);
    let merged_tree = "c".repeat(40);

    let status = ValidatedCommand::delivery_target_status(&fixture.binding).unwrap();
    let unmerged = ValidatedCommand::delivery_target_unmerged_entries(&fixture.binding).unwrap();
    let tracked = ValidatedCommand::delivery_target_tracked_paths(&fixture.binding).unwrap();
    let ignored =
        ValidatedCommand::delivery_target_ignored_untracked_paths(&fixture.binding).unwrap();
    let ancestor = ValidatedCommand::delivery_source_is_ancestor_of_target(
        &fixture.binding,
        &source,
        &target,
        40,
    )
    .unwrap();
    let merge_base =
        ValidatedCommand::delivery_merge_base(&fixture.binding, &target, &source, 40).unwrap();
    let merge_tree =
        ValidatedCommand::delivery_merge_tree(&fixture.binding, &target, &source, 40).unwrap();
    let write_set =
        ValidatedCommand::delivery_merge_write_set(&fixture.binding, &target, &merged_tree, 40)
            .unwrap();
    let raw_write_set = ValidatedCommand::delivery_expected_merge_raw_diff(
        &fixture.binding,
        &target,
        &merged_tree,
        40,
    )
    .unwrap();
    let mut conflict_entries = ValidatedCommand::delivery_expected_conflict_tree_entries(
        &fixture.binding,
        &source,
        &["dir/conflict file", "added.txt"],
        40,
    )
    .unwrap();
    let source_sha256 = "d".repeat(64);
    let conflict_entries_sha256 = ValidatedCommand::delivery_expected_conflict_tree_entries(
        &fixture.binding,
        &source_sha256,
        &["conflict.txt"],
        64,
    )
    .unwrap();

    assert_command_tail(
        &status,
        &[
            "status",
            "--porcelain=v2",
            "-z",
            "--untracked-files=all",
            "--ignore-submodules=none",
            "--no-renames",
            "--",
        ],
    );
    assert_command_tail(&unmerged, &["ls-files", "--unmerged", "-z", "--"]);
    assert_command_tail(&tracked, &["ls-files", "--cached", "-z", "--"]);
    assert_command_tail(
        &ignored,
        &[
            "ls-files",
            "--others",
            "--ignored",
            "--exclude-standard",
            "--directory",
            "-z",
            "--",
        ],
    );
    assert_command_tail(
        &ancestor,
        &["merge-base", "--is-ancestor", &source, &target],
    );
    assert_command_tail(&merge_base, &["merge-base", "--all", &target, &source]);
    assert_command_tail(
        &merge_tree,
        &[
            "merge-tree",
            "--write-tree",
            "--messages",
            "--name-only",
            "-z",
            &target,
            &source,
        ],
    );
    assert_command_tail(
        &write_set,
        &[
            "diff-tree",
            "--no-commit-id",
            "--name-only",
            "-r",
            "-z",
            "--no-renames",
            "--no-ext-diff",
            &target,
            &merged_tree,
            "--",
        ],
    );
    assert_command_tail(
        &raw_write_set,
        &[
            "diff-tree",
            "--no-commit-id",
            "--raw",
            "--abbrev=40",
            "-r",
            "-z",
            "--no-renames",
            "--no-ext-diff",
            &target,
            &merged_tree,
            "--",
        ],
    );
    assert_command_tail(
        &conflict_entries,
        &[
            "ls-tree",
            "-z",
            "--full-tree",
            "--abbrev=40",
            &source,
            "--",
            "dir/conflict file",
            "added.txt",
        ],
    );
    assert_command_tail(
        &conflict_entries_sha256,
        &[
            "ls-tree",
            "-z",
            "--full-tree",
            "--abbrev=64",
            &source_sha256,
            "--",
            "conflict.txt",
        ],
    );
    assert!(
        conflict_entries
            .arguments()
            .iter()
            .any(|argument| argument.as_os_str() == OsStr::new("--literal-pathspecs"))
    );
    assert!(conflict_entries.take_exact_input().is_none());

    for command in [
        &status,
        &unmerged,
        &tracked,
        &ignored,
        &ancestor,
        &merge_base,
        &merge_tree,
        &write_set,
        &raw_write_set,
        &conflict_entries,
        &conflict_entries_sha256,
    ] {
        assert!(
            !command
                .environment()
                .entries()
                .contains_key(OsStr::new("GIT_INDEX_FILE"))
        );
        assert!(Arc::ptr_eq(
            command
                .delivery_git_empty_config()
                .expect("target command retains empty config authority"),
            &fixture.binding.config,
        ));
        #[cfg(unix)]
        assert!(
            command
                .unix_delivery_directory_bindings()
                .expect("target command has repository descriptor bindings")
                .bindings()
                .iter()
                .all(|binding| !matches!(
                    binding.role(),
                    super::super::UnixDeliveryDirectoryRole::TemporaryIndexEnvironment
                ))
        );
    }

    for invalid in ["a".repeat(39), "A".repeat(40), "0".repeat(40)] {
        assert!(matches!(
            ValidatedCommand::delivery_source_is_ancestor_of_target(
                &fixture.binding,
                &invalid,
                &target,
                40,
            ),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
        assert!(matches!(
            ValidatedCommand::delivery_merge_tree(&fixture.binding, &target, &invalid, 40),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
        assert!(matches!(
            ValidatedCommand::delivery_merge_base(&fixture.binding, &target, &invalid, 40),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
        assert!(matches!(
            ValidatedCommand::delivery_merge_write_set(&fixture.binding, &target, &invalid, 40,),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
        assert!(matches!(
            ValidatedCommand::delivery_expected_merge_raw_diff(
                &fixture.binding,
                &target,
                &invalid,
                40,
            ),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
        assert!(matches!(
            ValidatedCommand::delivery_merge_base(&fixture.binding, &invalid, &source, 40),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
    }
    let unsupported_length = "a".repeat(41);
    assert!(matches!(
        ValidatedCommand::delivery_expected_merge_raw_diff(
            &fixture.binding,
            &unsupported_length,
            &unsupported_length,
            41,
        ),
        Err(CommandPolicyError::InvalidGitBinding)
    ));
    assert!(matches!(
        ValidatedCommand::delivery_expected_conflict_tree_entries(
            &fixture.binding,
            &unsupported_length,
            &["conflict.txt"],
            41,
        ),
        Err(CommandPolicyError::InvalidGitBinding)
    ));

    for invalid in ["a".repeat(39), "A".repeat(40), "0".repeat(40)] {
        assert!(matches!(
            ValidatedCommand::delivery_expected_conflict_tree_entries(
                &fixture.binding,
                &invalid,
                &["conflict.txt"],
                40,
            ),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
    }
    for invalid_paths in [Vec::<&str>::new(), vec![""], vec!["../escape"]] {
        assert!(matches!(
            ValidatedCommand::delivery_expected_conflict_tree_entries(
                &fixture.binding,
                &source,
                &invalid_paths,
                40,
            ),
            Err(CommandPolicyError::InvalidGitPath)
        ));
    }
    assert!(matches!(
        ValidatedCommand::delivery_expected_conflict_tree_entries(
            &fixture.binding,
            &source,
            &["same.txt", "same.txt"],
            40,
        ),
        Err(CommandPolicyError::InvalidGitPath)
    ));

    let too_many = (0..=MAX_MERGE_CONFLICT_PATHS)
        .map(|index| format!("path-{index}.txt"))
        .collect::<Vec<_>>();
    let too_many = too_many.iter().map(String::as_str).collect::<Vec<_>>();
    assert!(matches!(
        ValidatedCommand::delivery_expected_conflict_tree_entries(
            &fixture.binding,
            &source,
            &too_many,
            40,
        ),
        Err(CommandPolicyError::InvalidGitPath)
    ));

    let long_prefix = format!("{}/", "x".repeat(200)).repeat(19);
    let oversized_payload = (0..18)
        .map(|index| format!("{long_prefix}p{index}"))
        .collect::<Vec<_>>();
    let oversized_payload = oversized_payload
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    assert!(
        oversized_payload
            .iter()
            .all(|path| path.len() <= MAX_MERGE_CONFLICT_PATH_BYTES)
    );
    assert!(matches!(
        ValidatedCommand::delivery_expected_conflict_tree_entries(
            &fixture.binding,
            &source,
            &oversized_payload,
            40,
        ),
        Err(CommandPolicyError::InvalidGitPath)
    ));
    let oversized_path = "x".repeat(MAX_MERGE_CONFLICT_PATH_BYTES + 1);
    assert!(matches!(
        ValidatedCommand::delivery_expected_conflict_tree_entries(
            &fixture.binding,
            &source,
            &[oversized_path.as_str()],
            40,
        ),
        Err(CommandPolicyError::InvalidGitPath)
    ));
}

#[test]
fn real_index_binding_has_fixed_source_cas_and_predicate_argv() {
    let fixture = ReadOnlyBindingFixture::new();
    let source_objects = fixture.source_mutations(40);
    let expected = "a".repeat(40);
    let base = "b".repeat(40);
    let candidate = "c".repeat(40);

    assert!(matches!(
        source_objects.stage_candidate_in_real_index(&candidate),
        Err(CommandPolicyError::InvalidGitBinding)
    ));
    assert!(matches!(
        source_objects.refresh_real_index_stat(),
        Err(CommandPolicyError::InvalidGitBinding)
    ));
    assert!(matches!(
        source_objects.real_index_binding("refs/heads/main"),
        Err(CommandPolicyError::InvalidGitBinding)
    ));
    for invalid_branch in [
        "codex/task-task-attempt-0",
        "codex/task-task-attempt-01",
        "codex/task-task-attempt-4294967296",
    ] {
        assert!(matches!(
            source_objects.real_index_binding(invalid_branch),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
    }

    let real_index = source_objects
        .real_index_binding("codex/task-task-attempt-1")
        .unwrap();
    let stage_candidate = real_index
        .stage_candidate_in_real_index(&candidate)
        .unwrap();
    let refresh_stat = real_index.refresh_real_index_stat().unwrap();
    let mut verify_tree = real_index.cat_file_candidate_type(&candidate).unwrap();
    let update_ref = real_index.update_source_ref_cas(&expected, &base).unwrap();
    let index_matches = real_index.index_matches_tree(&candidate).unwrap();
    let worktree_matches = real_index.worktree_matches_index().unwrap();

    assert_command_tail(&stage_candidate, &["read-tree", "--reset", &candidate]);
    assert_command_tail(&refresh_stat, &["update-index", "--refresh", "-q"]);
    assert_command_tail(&verify_tree, &["cat-file", "-t", &candidate]);
    assert!(verify_tree.take_exact_input().is_none());
    assert_command_tail(
        &update_ref,
        &[
            "update-ref",
            "--no-deref",
            "refs/heads/codex/task-task-attempt-1",
            &expected,
            &base,
        ],
    );
    assert_command_tail(
        &index_matches,
        &["diff-index", "--cached", "--quiet", &candidate, "--"],
    );
    assert_command_tail(&worktree_matches, &["diff-files", "--quiet", "--"]);

    for command in [
        &stage_candidate,
        &refresh_stat,
        &verify_tree,
        &update_ref,
        &index_matches,
        &worktree_matches,
    ] {
        assert!(
            !command
                .environment()
                .entries()
                .contains_key(OsStr::new("GIT_INDEX_FILE"))
        );
        assert!(Arc::ptr_eq(
            command
                .delivery_git_empty_config()
                .expect("real-index command retains empty config authority"),
            &fixture.binding.config,
        ));
        #[cfg(unix)]
        assert!(
            command
                .unix_delivery_directory_bindings()
                .expect("real-index command has repository descriptor bindings")
                .bindings()
                .iter()
                .all(|binding| !matches!(
                    binding.role(),
                    super::super::UnixDeliveryDirectoryRole::TemporaryIndexEnvironment
                ))
        );
    }

    for invalid in ["a".repeat(39), "A".repeat(40), "0".repeat(40)] {
        assert!(matches!(
            real_index.cat_file_candidate_type(&invalid),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
        assert!(matches!(
            real_index.update_source_ref_cas(&invalid, &base),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
        assert!(matches!(
            real_index.index_matches_tree(&invalid),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
    }
    assert!(matches!(
        real_index.update_source_ref_cas(&expected, &"0".repeat(40)),
        Err(CommandPolicyError::InvalidGitBinding)
    ));
    assert_eq!(
        format!("{real_index:?}"),
        "DeliveryGitSourceMutationBinding(<opaque>)"
    );
}

#[test]
fn source_mutation_binding_rejects_invalid_object_ids_and_unsafe_temp_directories() {
    let fixture = ReadOnlyBindingFixture::new();
    assert!(matches!(
        DeliveryGitSourceMutationBinding::try_new(
            DeliveryGitMutationCommandFactory {
                git: Arc::clone(&fixture.binding.git),
            },
            &fixture.binding,
            41,
        ),
        Err(CommandPolicyError::InvalidGitBinding)
    ));

    let commands = fixture.source_mutations(40);
    let temporary_index = fixture.temporary_index();
    let valid = "b".repeat(40);
    let metadata = DeliveryGitCommitEnvironment::try_new(1_700_000_000).unwrap();
    for object in [
        "a".repeat(39),
        "a".repeat(41),
        "A".repeat(40),
        format!("{}-", "a".repeat(39)),
        "0".repeat(40),
    ] {
        assert!(matches!(
            commands.read_tree(&temporary_index, &object),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
        assert!(matches!(
            commands.cat_file_commit(&object),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
        assert!(matches!(
            commands.commit_tree(
                &object,
                &valid,
                ExactChildInput::try_new(b"subject\n".to_vec()).unwrap(),
                &metadata,
            ),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
        assert!(matches!(
            commands.commit_tree(
                &valid,
                &object,
                ExactChildInput::try_new(b"subject\n".to_vec()).unwrap(),
                &metadata,
            ),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
    }

    let unsafe_temporary_index =
        DeliveryGitTemporaryIndexEnvironment::try_new(Arc::clone(&fixture.binding.sandbox))
            .unwrap();
    assert!(matches!(
        commands.update_index_info(
            &unsafe_temporary_index,
            ExactChildInput::try_new(
                format!("100644 {}\ttracked.txt\0", "c".repeat(40)).into_bytes(),
            )
            .unwrap(),
        ),
        Err(CommandPolicyError::InvalidGitBinding)
    ));
    assert_eq!(
        format!("{temporary_index:?}"),
        "DeliveryGitTemporaryIndexEnvironment(<opaque>)"
    );
    assert_eq!(
        format!("{commands:?}"),
        "DeliveryGitSourceMutationBinding(<opaque>)"
    );
}

#[test]
fn commit_environment_is_fixed_and_rejects_ambient_author_metadata() {
    for epoch_seconds in [-1, 0] {
        assert!(matches!(
            DeliveryGitCommitEnvironment::try_new(epoch_seconds),
            Err(CommandPolicyError::InvalidGitBinding)
        ));
    }
    let metadata = DeliveryGitCommitEnvironment::try_new(42).unwrap();
    assert_eq!(
        format!("{metadata:?}"),
        "DeliveryGitCommitEnvironment(<opaque>)"
    );

    let environment = metadata
        .child_environment(&ChildEnvironment::default())
        .unwrap();
    assert_eq!(
        environment
            .entries()
            .get(OsStr::new("GIT_AUTHOR_DATE"))
            .map(OsString::as_os_str),
        Some(OsStr::new("42 +0000"))
    );
    assert_eq!(
        environment
            .entries()
            .get(OsStr::new("GIT_COMMITTER_EMAIL"))
            .map(OsString::as_os_str),
        Some(OsStr::new("coding-agent@localhost"))
    );

    let duplicate_author = ChildEnvironment::from_entries([(
        OsString::from("GIT_AUTHOR_NAME"),
        OsString::from("ambient identity"),
    )]);
    assert!(matches!(
        metadata.child_environment(&duplicate_author),
        Err(CommandPolicyError::InvalidGitBinding)
    ));
}

#[test]
fn delivery_read_only_configuration_disables_every_executable_mechanism() {
    let mut arguments = Vec::new();
    append_delivery_read_only_configuration(&mut arguments);
    let rendered = arguments
        .iter()
        .map(|argument| argument.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    for required in [
        "core.fsmonitor=false",
        "core.untrackedCache=false",
        "submodule.recurse=false",
        "extensions.worktreeConfig=false",
        "commit.gpgSign=false",
        "merge.verifySignatures=false",
        "merge.autoStash=false",
        "rerere.enabled=false",
        "credential.helper=",
        "core.askPass=",
        "core.attributesFile=",
        "core.excludesFile=",
        "i18n.commitEncoding=UTF-8",
        "diff.external=",
    ] {
        assert!(
            rendered.windows(2).any(|pair| pair == ["-c", required]),
            "delivery command must explicitly set {required}",
        );
    }
    assert_eq!(
        rendered.last().map(String::as_str),
        Some(crate::command_policy::git_hooks_path_configuration())
    );
}

#[test]
fn preinitialization_probe_configuration_disables_external_attributes_and_ignores() {
    let arguments = unbound_probe_arguments();
    let rendered = arguments
        .iter()
        .map(|argument| argument.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    for required in ["core.attributesFile=", "core.excludesFile="] {
        assert!(
            rendered.windows(2).any(|pair| pair == ["-c", required]),
            "probe command must explicitly disable {required}",
        );
    }
}

#[test]
fn every_probe_command_retains_the_exact_empty_config_authority() {
    let fixture = ReadOnlyBindingFixture::new();
    let commands = DeliveryGitProbeCommands::try_new(
        Arc::clone(&fixture.binding.git),
        Arc::clone(&fixture.binding.repository.work_tree),
        Arc::clone(&fixture.binding.config),
        fixture.binding.environment.clone(),
        Duration::from_secs(10),
    )
    .unwrap();
    let git_directory_path = fixture.binding.repository.work_tree.path().join(".git");
    std::fs::create_dir(&git_directory_path).unwrap();
    let repository_commands = commands
        .bind_repository(Arc::new(
            ExecutionDirectory::open(&git_directory_path).unwrap(),
        ))
        .unwrap();
    let tree = ProbeGitObjectId::try_new(&"1".repeat(40), 40).unwrap();
    let target = ProbeGitObjectId::try_new(&"2".repeat(40), 40).unwrap();
    let source = ProbeGitObjectId::try_new(&"3".repeat(40), 40).unwrap();

    for command in [
        commands.version().unwrap(),
        commands.initialize_repository().unwrap(),
        repository_commands.object_format().unwrap(),
        repository_commands.empty_tree().unwrap(),
        repository_commands.probe_blob().unwrap(),
        repository_commands.source_tree(&tree).unwrap(),
        repository_commands.base_commit(&tree).unwrap(),
        repository_commands.target_commit(&tree, &target).unwrap(),
        repository_commands.source_commit(&tree, &source).unwrap(),
        repository_commands.set_target_ref(&target).unwrap(),
        repository_commands.set_source_ref(&source).unwrap(),
        repository_commands.merge_tree(&target, &source).unwrap(),
        repository_commands.merge(&source).unwrap(),
        repository_commands.resolve_head().unwrap(),
        repository_commands
            .delete_source_transaction(&target, &source)
            .unwrap(),
        repository_commands.source_ref_exists().unwrap(),
    ] {
        assert!(Arc::ptr_eq(
            command
                .delivery_git_empty_config()
                .expect("probe command retains empty config authority"),
            &fixture.binding.config,
        ));
    }
}

#[test]
fn checked_attributes_include_working_tree_encoding() {
    let fixture = ReadOnlyBindingFixture::new();
    let command =
        ValidatedCommand::delivery_check_attributes(&fixture.binding, Vec::new()).unwrap();
    let arguments = command
        .arguments()
        .iter()
        .map(|argument| argument.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    assert!(arguments.ends_with(&[
        "check-attr".to_owned(),
        "-z".to_owned(),
        "--stdin".to_owned(),
        "filter".to_owned(),
        "diff".to_owned(),
        "merge".to_owned(),
        "working-tree-encoding".to_owned(),
    ]));
}

#[test]
fn every_delivery_read_command_retains_the_exact_empty_config_authority() {
    let fixture = ReadOnlyBindingFixture::new();
    let command = ValidatedCommand::delivery_resolve_head(&fixture.binding).unwrap();

    assert!(Arc::ptr_eq(
        command
            .delivery_git_empty_config()
            .expect("delivery read command retains empty config authority"),
        &fixture.binding.config,
    ));
}

#[cfg(unix)]
#[test]
fn delivery_argv_never_contains_repository_namespace_paths() {
    let fixture = ReadOnlyBindingFixture::new();
    let command = ValidatedCommand::delivery_resolve_head(&fixture.binding).unwrap();
    let arguments = command.arguments();

    for forbidden in [&fixture.git_directory_path, &fixture.work_tree_path] {
        let forbidden = forbidden.to_string_lossy();
        assert!(
            arguments
                .iter()
                .all(|argument| !argument.to_string_lossy().contains(forbidden.as_ref())),
            "delivery argv retained a namespace path"
        );
    }
}
