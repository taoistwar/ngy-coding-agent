mod support;

#[path = "delivery_security/fixture.rs"]
mod fixture;

#[cfg(feature = "test-support")]
use std::collections::BTreeMap;
use std::path::Path;
#[cfg(feature = "test-support")]
use std::path::PathBuf;
use std::process::Command;

use coding_agent_runtime::DeliverySourceCommitInput;
use fixture::{SecurityFixture, delivery_source_limits_with};
use tokio_util::sync::CancellationToken;

const NORMAL_STATUS_BYTES: usize = 256 * 1024;
const NORMAL_CONFIG_BYTES: usize = 64 * 1024;
const NORMAL_ATTRIBUTES_BYTES: usize = 64 * 1024;
const NORMAL_PATHS: usize = 4_096;

#[tokio::test]
async fn unsafe_common_config_is_rejected_before_any_helper_can_execute() {
    let fixture = SecurityFixture::new().await;
    let approved = fixture.fingerprint().await;
    let source = fixture.source_provisioner().unwrap();
    let sentinel = fixture.root.join("unsafe-common-config-helper-ran");
    let helper = shell_probe_command(&sentinel);
    let included = fixture.root.join("included-config");
    std::fs::write(
        &included,
        format!("[filter \"delivery-probe\"]\n\tprocess = {helper}\n"),
    )
    .unwrap();
    let included_value = path_for_git(&included);
    let branch_merge_options = format!("branch.{}.mergeOptions", fixture.reservation.branch_name());
    let cases = [
        ("include.path", included_value.as_str()),
        ("includeIf.gitdir:**.path", included_value.as_str()),
        ("filter.delivery-probe.clean", helper.as_str()),
        ("filter.delivery-probe.smudge", helper.as_str()),
        ("filter.delivery-probe.process", helper.as_str()),
        ("diff.delivery-probe.command", helper.as_str()),
        ("diff.delivery-probe.textconv", helper.as_str()),
        ("merge.delivery-probe.driver", helper.as_str()),
        (branch_merge_options.as_str(), "--squash"),
        ("core.hooksPath", included_value.as_str()),
        ("merge.autoStash", "true"),
        ("rerere.enabled", "true"),
        ("commit.gpgSign", "true"),
    ];

    for (key, value) in cases {
        fixture.set_common_config(key, value);
        let error = source
            .open_delivery_source(&fixture.reservation, approved, CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            "UNSAFE_GIT_CONFIGURATION",
            "key={key}: {error:?}"
        );
        assert_redacted_error(&error, &fixture, &sentinel);
        assert!(!sentinel.exists(), "unsafe helper executed for {key}");
        fixture.unset_common_config(key);
    }
}

#[tokio::test]
async fn merge_verify_signatures_is_accepted_under_the_fixed_delivery_policy() {
    let fixture = SecurityFixture::new().await;
    fixture.set_common_config("merge.verifySignatures", "true");
    let approved = fixture.fingerprint().await;
    let source = fixture.source_provisioner().unwrap();

    let _capability = source
        .open_delivery_source(&fixture.reservation, approved, CancellationToken::new())
        .await
        .unwrap();
}

#[tokio::test]
async fn filter_worktree_attribute_is_rejected_without_executing_repository_code() {
    assert_dangerous_worktree_attribute("filter", "Cargo.toml filter=delivery-probe\n").await;
}

#[tokio::test]
async fn diff_worktree_attribute_is_rejected_without_executing_repository_code() {
    assert_dangerous_worktree_attribute("diff", "Cargo.toml diff=delivery-probe\n").await;
}

#[tokio::test]
async fn merge_worktree_attribute_is_rejected_without_executing_repository_code() {
    assert_dangerous_worktree_attribute("merge", "Cargo.toml merge=delivery-probe\n").await;
}

#[tokio::test]
async fn working_tree_encoding_attribute_is_rejected_without_executing_repository_code() {
    assert_dangerous_worktree_attribute(
        "working-tree-encoding",
        "Cargo.toml working-tree-encoding=UTF-16LE\n",
    )
    .await;
}

#[tokio::test]
async fn resolved_worktree_attributes_are_bound_into_the_capability_digest() {
    let fixture = SecurityFixture::new().await;
    let attributes = fixture.worktree.join(".gitattributes");

    std::fs::write(&attributes, "Cargo.toml -filter\n").unwrap();
    let first_fingerprint = fixture.fingerprint().await;
    let first_capability = fixture
        .source_provisioner()
        .unwrap()
        .open_delivery_source(
            &fixture.reservation,
            first_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let first_digest = *first_capability.config_attributes_digest();
    drop(first_capability);

    std::fs::write(&attributes, "Cargo.toml !filter\n").unwrap();
    let second_fingerprint = fixture.fingerprint().await;
    let second_capability = fixture
        .source_provisioner()
        .unwrap()
        .open_delivery_source(
            &fixture.reservation,
            second_fingerprint,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let second_digest = *second_capability.config_attributes_digest();

    assert_ne!(first_fingerprint, second_fingerprint);
    assert_ne!(first_digest, second_digest);
}

async fn assert_dangerous_worktree_attribute(attribute: &str, contents: &str) {
    let fixture = SecurityFixture::new().await;
    let sentinel = fixture.root.join("worktree-attribute-helper-ran");
    install_hook(
        &fixture.repository.join(".git/hooks/post-index-change"),
        &sentinel,
    );
    std::fs::write(fixture.worktree.join(".gitattributes"), contents).unwrap();
    let approved = fixture.fingerprint().await;
    let source = fixture.source_provisioner().unwrap();

    let result = source
        .open_delivery_source(&fixture.reservation, approved, CancellationToken::new())
        .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("{attribute} attribute unexpectedly minted a delivery capability"),
    };
    assert_eq!(
        error.code(),
        "UNSAFE_GIT_CONFIGURATION",
        "attribute={attribute}: {error:?}"
    );
    assert_redacted_error(&error, &fixture, &sentinel);
    assert!(
        !sentinel.exists(),
        "repository code executed for {attribute} attribute"
    );
}

#[tokio::test]
async fn status_output_limit_is_independently_enforced() {
    assert_delivery_source_bound(
        "status",
        delivery_source_limits_with(
            1,
            NORMAL_CONFIG_BYTES,
            NORMAL_ATTRIBUTES_BYTES,
            NORMAL_PATHS,
        ),
    )
    .await;
}

#[tokio::test]
async fn config_input_limit_is_independently_enforced() {
    assert_delivery_source_bound(
        "config",
        delivery_source_limits_with(
            NORMAL_STATUS_BYTES,
            1,
            NORMAL_ATTRIBUTES_BYTES,
            NORMAL_PATHS,
        ),
    )
    .await;
}

#[tokio::test]
async fn checked_attributes_output_limit_is_independently_enforced() {
    assert_delivery_source_bound(
        "attributes",
        delivery_source_limits_with(NORMAL_STATUS_BYTES, NORMAL_CONFIG_BYTES, 1, NORMAL_PATHS),
    )
    .await;
}

#[tokio::test]
async fn observed_path_count_limit_is_independently_enforced() {
    assert_delivery_source_bound(
        "paths",
        delivery_source_limits_with(
            NORMAL_STATUS_BYTES,
            NORMAL_CONFIG_BYTES,
            NORMAL_ATTRIBUTES_BYTES,
            1,
        ),
    )
    .await;
}

#[tokio::test]
async fn config_worktree_and_extension_switch_are_rejected_from_the_admin_domain() {
    let fixture = SecurityFixture::new().await;
    let approved = fixture.fingerprint().await;
    let source = fixture.source_provisioner().unwrap();
    let sentinel = fixture.root.join("unsafe-admin-config-helper-ran");
    let helper = shell_probe_command(&sentinel);

    std::fs::write(
        fixture.config_worktree(),
        format!("[filter \"delivery-probe\"]\n\tprocess = {helper}\n"),
    )
    .unwrap();
    assert_unsafe_configuration(&fixture, &source, approved, &sentinel).await;
    std::fs::remove_file(fixture.config_worktree()).unwrap();

    fixture.set_common_config("extensions.worktreeConfig", "true");
    assert_unsafe_configuration(&fixture, &source, approved, &sentinel).await;
    fixture.unset_common_config("extensions.worktreeConfig");

    std::fs::create_dir(fixture.config_worktree()).unwrap();
    assert_unsafe_configuration(&fixture, &source, approved, &sentinel).await;
}

#[cfg(unix)]
#[tokio::test]
async fn config_worktree_symlink_is_rejected_without_following_or_running_it() {
    let fixture = SecurityFixture::new().await;
    let approved = fixture.fingerprint().await;
    let source = fixture.source_provisioner().unwrap();
    let sentinel = fixture.root.join("symlink-config-helper-ran");
    let outside = fixture.root.join("outside-config");
    std::fs::write(
        &outside,
        format!(
            "[filter \"delivery-probe\"]\n\tprocess = {}\n",
            shell_probe_command(&sentinel)
        ),
    )
    .unwrap();
    std::os::unix::fs::symlink(&outside, fixture.config_worktree()).unwrap();

    assert_unsafe_configuration(&fixture, &source, approved, &sentinel).await;
}

#[test]
fn source_object_commands_use_fixed_cwd_binding_and_a_clean_environment() {
    let output = support::command_output(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "poisoned_source_object_environment_child",
                "--nocapture",
            ])
            .env("CODING_AGENT_DELIVERY_SECURITY_CHILD", "1"),
    )
    .unwrap();
    assert!(
        output.status.success(),
        "isolated command-contract child failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn poisoned_source_object_environment_child() {
    if std::env::var_os("CODING_AGENT_DELIVERY_SECURITY_CHILD").is_none() {
        return;
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let fixture = SecurityFixture::new().await;
            let approved = fixture.fingerprint().await;
            let sentinel = fixture.root.join("injected-environment-helper-ran");
            let hook_sentinel = fixture.root.join("repository-hook-ran");
            for hook in [
                "pre-commit",
                "commit-msg",
                "post-commit",
                "post-index-change",
            ] {
                install_hook(
                    &fixture.repository.join(".git/hooks").join(hook),
                    &hook_sentinel,
                );
            }
            fixture.set_common_config("core.fsmonitor", &shell_probe_command(&sentinel));

            let evil = fixture.root.join("evil-cwd");
            std::fs::create_dir(&evil).unwrap();
            let init = support::command_output(
                Command::new("git")
                    .args(["init", "--quiet", "--"])
                    .current_dir(&evil),
            )
            .unwrap();
            assert!(init.status.success());
            std::env::set_current_dir(&evil).unwrap();
            unsafe {
                std::env::set_var("GIT_DIR", evil.join(".git"));
                std::env::set_var("GIT_WORK_TREE", &evil);
                std::env::set_var("GIT_CONFIG_COUNT", "1");
                std::env::set_var("GIT_CONFIG_KEY_0", "filter.delivery-probe.process");
                std::env::set_var("GIT_CONFIG_VALUE_0", shell_probe_command(&sentinel));
                std::env::set_var("GIT_EDITOR", shell_probe_command(&sentinel));
                std::env::set_var("GIT_SEQUENCE_EDITOR", shell_probe_command(&sentinel));
                std::env::set_var("EDITOR", shell_probe_command(&sentinel));
                std::env::set_var("VISUAL", shell_probe_command(&sentinel));
                std::env::set_var("GIT_TEMPLATE_DIR", &evil);
                std::env::set_var("GIT_ASKPASS", shell_probe_command(&sentinel));
                std::env::set_var("SSH_ASKPASS", shell_probe_command(&sentinel));
            }

            let source = fixture.source_provisioner().unwrap();
            let capability = source
                .open_delivery_source(&fixture.reservation, approved, CancellationToken::new())
                .await
                .unwrap();
            let candidate = source
                .build_candidate_tree(&capability, CancellationToken::new())
                .await
                .unwrap();
            let metadata = DeliverySourceCommitInput::try_new(
                capability.identity().task_id(),
                u64::from(capability.identity().attempt()),
                1_700_000_000,
            )
            .unwrap();
            let source_commit = source
                .build_source_commit(&capability, &candidate, &metadata, CancellationToken::new())
                .await
                .unwrap();
            let sensitive_root = fixture.root.to_string_lossy().into_owned();
            let sensitive_sentinel = sentinel.to_string_lossy().into_owned();
            let sensitive_helper = shell_probe_command(&sentinel);

            assert_eq!(capability.base_commit(), fixture.reservation.base_commit());
            assert_eq!(capability.branch_name(), fixture.reservation.branch_name());
            assert_eq!(source_commit.object_id().len(), 40);
            assert_eq!(
                format!("{capability:?}"),
                "DeliverySourceCapability(<opaque>)"
            );
            let rendered_success = format!("{capability:?} {candidate:?} {source_commit:?}");
            for sensitive in [&sensitive_root, &sensitive_sentinel, &sensitive_helper] {
                assert!(!rendered_success.contains(sensitive.as_str()));
            }

            let cancelled = CancellationToken::new();
            cancelled.cancel();
            let error = source
                .build_candidate_tree(&capability, cancelled)
                .await
                .unwrap_err();
            let rendered_error = format!("{error:?} {error}");
            for sensitive in [&sensitive_root, &sensitive_sentinel, &sensitive_helper] {
                assert!(!rendered_error.contains(sensitive.as_str()));
            }
            assert!(!sentinel.exists(), "config injection or fsmonitor executed");
            assert!(!hook_sentinel.exists(), "repository hook executed");
        });
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn config_created_after_the_first_scan_is_caught_before_command_spawn() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let fixture = SecurityFixture::new().await;
    let approved = fixture.fingerprint().await;
    let sentinel = fixture.root.join("toctou-config-helper-ran");
    let config_worktree = fixture.config_worktree();
    let helper = shell_probe_command(&sentinel);
    let injected = Arc::new(AtomicBool::new(false));
    let hook_injected = Arc::clone(&injected);
    let mut source = fixture.source_provisioner().unwrap();
    source.set_authentication_boundary_hook_for_tests(move |boundary| {
        if boundary == "after-config-authentication" && !hook_injected.swap(true, Ordering::SeqCst)
        {
            std::fs::write(
                &config_worktree,
                format!("[filter \"delivery-probe\"]\n\tprocess = {helper}\n"),
            )
            .unwrap();
        }
    });

    let error = source
        .open_delivery_source(&fixture.reservation, approved, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(injected.load(Ordering::SeqCst));
    assert_eq!(error.code(), "UNSAFE_GIT_CONFIGURATION");
    assert!(!sentinel.exists(), "TOCTOU helper executed");
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn candidate_tree_rejects_post_revalidation_filter_injection_without_helper_execution() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    let fixture = SecurityFixture::new().await;
    let common_info = fixture.repository.join(".git").join("info");
    std::fs::create_dir_all(&common_info).unwrap();
    // Keep the injected worktree attributes outside the approved deliverable
    // set. They still affect a path-based `git add`, which is the behavior
    // this regression guards against.
    std::fs::write(common_info.join("exclude"), b".gitattributes\n").unwrap();

    let approved = fixture.fingerprint().await;
    let mut source = fixture.source_provisioner().unwrap();
    let capability = source
        .open_delivery_source(&fixture.reservation, approved, CancellationToken::new())
        .await
        .unwrap();
    let sentinel = fixture.root.join("post-revalidation-filter-helper-ran");
    let helper = shell_probe_command(&sentinel);
    let injected = Arc::new(AtomicBool::new(false));
    let observed_source_state = Arc::new(Mutex::new(None));
    let hook_repository = fixture.repository.clone();
    let hook_admin_directory = fixture.admin_directory.clone();
    let hook_worktree = fixture.worktree.clone();
    let hook_common_attributes = common_info.join("attributes");
    let hook_worktree_attributes = hook_worktree.join(".gitattributes");
    let hook_helper = helper.clone();
    let hook_injected = Arc::clone(&injected);
    let hook_observed_source_state = Arc::clone(&observed_source_state);
    source.set_authentication_boundary_hook_for_tests(move |boundary| {
        if boundary != "after-candidate-revalidation-before-tree-build"
            || hook_injected.swap(true, Ordering::SeqCst)
        {
            return;
        }

        set_local_filter_process(&hook_repository, &hook_helper);
        std::fs::write(
            &hook_common_attributes,
            b"tracked.txt filter=delivery-probe\n",
        )
        .unwrap();
        std::fs::write(
            &hook_worktree_attributes,
            b"tracked.txt filter=delivery-probe\n",
        )
        .unwrap();

        let snapshot =
            injected_source_state(&hook_repository, &hook_admin_directory, &hook_worktree);
        let mut slot = hook_observed_source_state.lock().unwrap();
        assert!(slot.replace(snapshot).is_none(), "hook must run only once");
    });

    let error = source
        .build_candidate_tree(&capability, CancellationToken::new())
        .await
        .unwrap_err();

    assert!(injected.load(Ordering::SeqCst));
    assert_eq!(error.code(), "UNSAFE_GIT_CONFIGURATION", "{error:?}");
    assert_redacted_error(&error, &fixture, &sentinel);
    assert!(
        !sentinel.exists(),
        "post-revalidation filter helper must never execute"
    );

    let expected_source_state = observed_source_state
        .lock()
        .unwrap()
        .take()
        .expect("candidate-tree boundary hook should run exactly once");
    assert_eq!(
        injected_source_state(
            &fixture.repository,
            &fixture.admin_directory,
            &fixture.worktree,
        ),
        expected_source_state,
        "candidate construction must not rewrite the injected source state"
    );
}

#[tokio::test]
async fn public_constructor_rejects_an_independently_probed_git_capability() {
    let fixture = SecurityFixture::new().await;
    let independently_probed_git = fixture.independently_probed_git().await;
    independently_probed_git
        .verify_current_executable()
        .unwrap();
    assert!(independently_probed_git.supports_required_merge_options());

    let error = fixture
        .source_provisioner_with_probe(independently_probed_git)
        .unwrap_err();
    assert_eq!(error.code(), "DELIVERY_SOURCE_CHANGED");
    assert_eq!(format!("{error:?}"), "DeliverySourceError(<redacted>)");
}

#[tokio::test]
async fn public_constructor_rejects_a_runtime_not_bound_to_the_probe() {
    let fixture = SecurityFixture::new().await;
    let error = fixture
        .source_provisioner_with_temporary_directory(&fixture.runtime)
        .unwrap_err();

    assert_eq!(error.code(), "DELIVERY_SOURCE_CHANGED");
    assert_eq!(format!("{error:?}"), "DeliverySourceError(<redacted>)");
}

async fn assert_delivery_source_bound(
    dimension: &str,
    limits: coding_agent_runtime::DeliverySourceLimits,
) {
    let fixture = SecurityFixture::new().await;
    let approved = fixture.fingerprint().await;
    let source = fixture.source_provisioner_with_limits(limits).unwrap();
    let error = source
        .open_delivery_source(&fixture.reservation, approved, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        "DELIVERY_SOURCE_BOUNDS_EXCEEDED",
        "dimension={dimension}: {error:?}"
    );
}

async fn assert_unsafe_configuration(
    fixture: &SecurityFixture,
    source: &coding_agent_runtime::DeliverySourceProvisioner,
    approved: coding_agent_core::WorkspaceFingerprint,
    sentinel: &Path,
) {
    let error = source
        .open_delivery_source(&fixture.reservation, approved, CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.code(), "UNSAFE_GIT_CONFIGURATION", "{error:?}");
    assert_redacted_error(&error, fixture, sentinel);
    assert!(!sentinel.exists(), "unsafe helper executed");
}

fn assert_redacted_error(
    error: &coding_agent_runtime::DeliverySourceError,
    fixture: &SecurityFixture,
    sentinel: &Path,
) {
    let rendered = format!("{error:?} {}", error);
    assert!(!rendered.contains(&fixture.root.to_string_lossy().to_string()));
    assert!(!rendered.contains(&sentinel.to_string_lossy().to_string()));
    assert!(!rendered.contains("delivery-probe"));
}

fn shell_probe_command(sentinel: &Path) -> String {
    if cfg!(windows) {
        format!("cmd.exe /C echo executed>{}", path_for_git(sentinel))
    } else {
        format!("touch {}", shell_quote(&path_for_git(sentinel)))
    }
}

fn install_hook(hook: &Path, sentinel: &Path) {
    std::fs::write(
        hook,
        format!("#!/bin/sh\n{}\n", shell_probe_command(sentinel)),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(hook, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn path_for_git(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

#[cfg(feature = "test-support")]
#[derive(Debug, PartialEq, Eq)]
struct InjectedSourceState {
    refs: BTreeMap<PathBuf, SourcePathState>,
    packed_refs: Option<Vec<u8>>,
    linked_index: Option<Vec<u8>>,
    worktree: BTreeMap<PathBuf, SourcePathState>,
    common_config: Vec<u8>,
    common_attributes: Option<Vec<u8>>,
    common_exclude: Option<Vec<u8>>,
}

#[cfg(feature = "test-support")]
#[derive(Debug, PartialEq, Eq)]
enum SourcePathState {
    Directory,
    File(Vec<u8>),
    Symlink(PathBuf),
}

#[cfg(feature = "test-support")]
fn injected_source_state(
    repository: &Path,
    admin_directory: &Path,
    worktree: &Path,
) -> InjectedSourceState {
    let common_git = repository.join(".git");
    let common_info = common_git.join("info");
    InjectedSourceState {
        refs: snapshot_path_tree(&common_git.join("refs")),
        packed_refs: optional_file_bytes(&common_git.join("packed-refs")),
        linked_index: optional_file_bytes(&admin_directory.join("index")),
        worktree: snapshot_path_tree(worktree),
        common_config: std::fs::read(common_git.join("config")).unwrap(),
        common_attributes: optional_file_bytes(&common_info.join("attributes")),
        common_exclude: optional_file_bytes(&common_info.join("exclude")),
    }
}

#[cfg(feature = "test-support")]
fn set_local_filter_process(repository: &Path, helper: &str) {
    let null_device = if cfg!(windows) { "NUL" } else { "/dev/null" };
    let output = support::command_output(
        Command::new("git")
            .arg("--no-pager")
            .arg("-C")
            .arg(repository)
            .args([
                "config",
                "--local",
                "--no-includes",
                "filter.delivery-probe.process",
            ])
            .arg(helper)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", null_device)
            .env("GIT_TERMINAL_PROMPT", "0"),
    )
    .unwrap();
    assert!(
        output.status.success(),
        "failed to inject test filter configuration: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(feature = "test-support")]
fn snapshot_path_tree(root: &Path) -> BTreeMap<PathBuf, SourcePathState> {
    let mut snapshot = BTreeMap::new();
    snapshot_path_tree_into(root, root, &mut snapshot);
    snapshot
}

#[cfg(feature = "test-support")]
fn snapshot_path_tree_into(
    root: &Path,
    directory: &Path,
    snapshot: &mut BTreeMap<PathBuf, SourcePathState>,
) {
    let mut entries = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap())
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        let relative = path.strip_prefix(root).unwrap().to_owned();
        let file_type = entry.file_type().unwrap();
        if file_type.is_dir() {
            snapshot.insert(relative, SourcePathState::Directory);
            snapshot_path_tree_into(root, &path, snapshot);
        } else if file_type.is_file() {
            snapshot.insert(
                relative,
                SourcePathState::File(std::fs::read(path).unwrap()),
            );
        } else if file_type.is_symlink() {
            snapshot.insert(
                relative,
                SourcePathState::Symlink(std::fs::read_link(path).unwrap()),
            );
        } else {
            panic!("unexpected source snapshot entry: {}", path.display());
        }
    }
}

#[cfg(feature = "test-support")]
fn optional_file_bytes(path: &Path) -> Option<Vec<u8>> {
    match std::fs::read(path) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => panic!("failed to read {}: {error}", path.display()),
    }
}
