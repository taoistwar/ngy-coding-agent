#[cfg(test)]
use std::ffi::OsStr;
use std::ffi::OsString;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::command_policy::{
    DeliveryGitEmptyConfig, ExecutionDirectory, GitCommandBinding, PinnedExecutable,
};
use crate::process_supervisor::{ChildEnvironment, ExactChildInput, PlatformEnvironment};

use super::DeliverySourceError;

mod branch_cleanup;
mod merge;
mod read;
mod source_mutation;
mod worktree_cleanup;

pub(super) use branch_cleanup::DeliveryBranchCleanupCommands;
pub(crate) use branch_cleanup::DeliveryGitBranchCleanupBinding;
pub(super) use merge::{DeliveryMergeMessage, DeliveryTargetMutationCommands};
pub(crate) use read::{DeliverySourceReadCommands, DeliveryTargetReadCommands};
pub(super) use source_mutation::{DeliverySourceMutationCommands, DeliverySourceRealIndexCommands};
pub(super) use worktree_cleanup::DeliveryCleanupCommands;

const MAX_CHECK_ATTRIBUTE_INPUT_BYTES: usize = 128 * 1024;

/// Exact bytes captured from one no-follow, identity-bound worktree file.
/// This remains delivery-private so callers cannot turn the fixed
/// `hash-object --stdin` command into a generic stdin or path-taking Git
/// command.
pub(super) struct DeliverySnapshotHashInput(ExactChildInput);

impl DeliverySnapshotHashInput {
    pub(super) fn try_new(bytes: Vec<u8>) -> Result<Self, DeliverySourceError> {
        ExactChildInput::try_new(bytes)
            .map(Self)
            .map_err(|_| DeliverySourceError::BoundsExceeded)
    }

    fn into_exact_input(self) -> ExactChildInput {
        self.0
    }
}

impl fmt::Debug for DeliverySnapshotHashInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliverySnapshotHashInput(<redacted>)")
    }
}

/// Exact NUL-delimited `git update-index --index-info` records.  The source
/// tree builder constructs these from typed snapshot entries, and this wrapper
/// ensures no other delivery code can substitute a generic stdin channel.
pub(super) struct DeliveryIndexInfoInput(ExactChildInput);

impl DeliveryIndexInfoInput {
    pub(super) const fn maximum_bytes() -> usize {
        ExactChildInput::maximum_bytes()
    }

    pub(super) fn try_new(
        bytes: Vec<u8>,
        object_id_length: usize,
    ) -> Result<Self, DeliverySourceError> {
        validate_index_info_input(&bytes, object_id_length)?;
        ExactChildInput::try_new(bytes)
            .map(Self)
            .map_err(|_| DeliverySourceError::BoundsExceeded)
    }

    fn into_exact_input(self) -> ExactChildInput {
        self.0
    }
}

impl fmt::Debug for DeliveryIndexInfoInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryIndexInfoInput(<redacted>)")
    }
}

/// Complete capability binding shared by every P4-B read-only Git command.
/// Its fields remain crate-private only so the typed command-policy adapter can
/// construct a `ValidatedCommand`; no generic argv entry point is exposed.
pub(crate) struct DeliveryGitReadOnlyBinding {
    pub(crate) git: Arc<PinnedExecutable>,
    pub(crate) repository: GitCommandBinding,
    pub(crate) common_git: Arc<ExecutionDirectory>,
    pub(crate) sandbox: Arc<ExecutionDirectory>,
    pub(crate) config: Arc<DeliveryGitEmptyConfig>,
    pub(crate) environment: ChildEnvironment,
    pub(crate) timeout: Duration,
}

impl fmt::Debug for DeliveryGitReadOnlyBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryGitReadOnlyBinding(<opaque>)")
    }
}

fn require_distinct_directories(
    directories: &[&ExecutionDirectory],
) -> Result<(), DeliverySourceError> {
    for (index, directory) in directories.iter().enumerate() {
        directory.revalidate()?;
        if directories[..index]
            .iter()
            .any(|prior| directory.has_same_identity(prior))
        {
            return Err(DeliverySourceError::AuthenticationChanged);
        }
    }
    Ok(())
}

/// A registered primary checkout has exactly one permitted directory alias:
/// its `common_git` and checkout-Git capabilities designate the same durable
/// Git directory.  All other capability relationships remain disjoint so the
/// target command environment cannot redirect Git through the checkout root
/// or its private sandbox.
fn require_target_directories(
    common_git: &ExecutionDirectory,
    checkout_git: &ExecutionDirectory,
    checkout: &ExecutionDirectory,
    sandbox: &ExecutionDirectory,
) -> Result<(), DeliverySourceError> {
    for directory in [common_git, checkout_git, checkout, sandbox] {
        directory.revalidate()?;
    }
    if !common_git.has_same_identity(checkout_git)
        || checkout_git.has_same_identity(checkout)
        || sandbox.has_same_identity(common_git)
        || sandbox.has_same_identity(checkout)
    {
        return Err(DeliverySourceError::AuthenticationChanged);
    }
    Ok(())
}

fn delivery_git_environment(
    platform: &PlatformEnvironment,
    empty_config: &DeliveryGitEmptyConfig,
    sandbox: &ExecutionDirectory,
) -> Result<ChildEnvironment, DeliverySourceError> {
    let mut entries = ChildEnvironment::for_git(platform).entries().clone();
    empty_config.apply_delivery_git_environment(&mut entries)?;
    for key in ["HOME", "XDG_CONFIG_HOME"] {
        entries.insert(OsString::from(key), sandbox.path().as_os_str().to_owned());
    }
    #[cfg(windows)]
    entries.insert(
        OsString::from("USERPROFILE"),
        sandbox.path().as_os_str().to_owned(),
    );
    Ok(ChildEnvironment::from_entries(entries))
}

/// Accepts only the lowercase, hyphenated UUID spelling retained by a
/// [`crate::worktree::WorktreeIdentity`]. Keeping this validation adjacent to the merge
/// message constructor prevents a raw task ID from becoming a Git argument.
fn is_canonical_task_id(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')
            }
        })
}

fn checked_path_input(paths: &[Vec<u8>]) -> Result<Vec<u8>, DeliverySourceError> {
    let total = paths.iter().try_fold(0usize, |total, path| {
        validate_raw_path(path)?;
        total
            .checked_add(path.len())
            .and_then(|value| value.checked_add(1))
            .ok_or(DeliverySourceError::BoundsExceeded)
    })?;
    if total > MAX_CHECK_ATTRIBUTE_INPUT_BYTES {
        return Err(DeliverySourceError::BoundsExceeded);
    }
    let mut input = Vec::with_capacity(total);
    for path in paths {
        input.extend_from_slice(path);
        input.push(0);
    }
    Ok(input)
}

fn validate_raw_path(path: &[u8]) -> Result<(), DeliverySourceError> {
    if path.is_empty()
        || path.contains(&0)
        || matches!(path.first(), Some(b'/' | b'\\'))
        || has_unsafe_component(path)
    {
        return Err(DeliverySourceError::UnsafeIndex);
    }
    Ok(())
}

fn validate_index_info_input(
    input: &[u8],
    object_id_length: usize,
) -> Result<(), DeliverySourceError> {
    if !matches!(object_id_length, 40 | 64) || input.is_empty() || input.last() != Some(&0) {
        return Err(DeliverySourceError::UnsafeIndex);
    }
    for record in input[..input.len() - 1].split(|byte| *byte == 0) {
        let tab = record
            .iter()
            .position(|byte| *byte == b'\t')
            .ok_or(DeliverySourceError::UnsafeIndex)?;
        let (metadata, path) = record.split_at(tab);
        let path = &path[1..];
        let mut fields = metadata.split(|byte| *byte == b' ');
        let mode = fields.next().ok_or(DeliverySourceError::UnsafeIndex)?;
        let object = fields.next().ok_or(DeliverySourceError::UnsafeIndex)?;
        let present = matches!(mode, b"100644" | b"100755")
            && object.len() == object_id_length
            && object
                .iter()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            && !object.iter().all(|byte| *byte == b'0');
        let deletion = mode == b"0"
            && object.len() == object_id_length
            && object.iter().all(|byte| *byte == b'0');
        if fields.next().is_some() || !(present || deletion) {
            return Err(DeliverySourceError::UnsafeIndex);
        }
        validate_raw_path(path)?;
    }
    Ok(())
}

fn has_unsafe_component(path: &[u8]) -> bool {
    path.split(|byte| matches!(byte, b'/' | b'\\'))
        .any(|component| {
            component.is_empty()
                || component == b"."
                || component == b".."
                || component.eq_ignore_ascii_case(b".git")
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_path_input_is_exact_and_nul_delimited() {
        let input = checked_path_input(&[b"one.txt".to_vec(), b"dir/two.txt".to_vec()]).unwrap();
        assert_eq!(input, b"one.txt\0dir/two.txt\0");
    }

    #[test]
    fn checked_path_input_rejects_escapes_and_protected_metadata() {
        for path in [
            &b""[..],
            &b"../escape"[..],
            &b"safe/../escape"[..],
            &b"safe/.git/config"[..],
            &b"/absolute"[..],
            &b"contains\0nul"[..],
        ] {
            assert_eq!(
                checked_path_input(&[path.to_vec()]),
                Err(DeliverySourceError::UnsafeIndex)
            );
        }
    }

    #[test]
    fn checked_path_input_rejects_payloads_above_the_stdin_bound() {
        let path = vec![b'x'; MAX_CHECK_ATTRIBUTE_INPUT_BYTES];
        assert_eq!(
            checked_path_input(&[path]),
            Err(DeliverySourceError::BoundsExceeded)
        );
    }

    #[test]
    fn index_info_accepts_only_typed_present_or_deletion_records() {
        let object = "a".repeat(40);
        assert!(
            DeliveryIndexInfoInput::try_new(
                format!("100755 {object}\tbin/tool\0").into_bytes(),
                40,
            )
            .is_ok()
        );
        assert!(
            DeliveryIndexInfoInput::try_new(
                format!("0 {}\tremoved.txt\0", "0".repeat(40)).into_bytes(),
                40,
            )
            .is_ok()
        );

        for input in [
            format!("0 {object}\tremoved.txt\0"),
            format!("100644 {}\tkept.txt\0", "0".repeat(40)),
            format!("100644 {object}\t../escape\0"),
            format!("120000 {object}\tlink\0"),
        ] {
            assert!(matches!(
                DeliveryIndexInfoInput::try_new(input.into_bytes(), 40),
                Err(DeliverySourceError::UnsafeIndex)
            ));
        }
    }

    #[test]
    fn snapshot_hash_input_accepts_exact_file_bytes_and_redacts_debug() {
        let input = DeliverySnapshotHashInput::try_new(b"snapshot-secret\0bytes".to_vec())
            .expect("exact file bytes are accepted");
        assert_eq!(
            format!("{input:?}"),
            "DeliverySnapshotHashInput(<redacted>)"
        );
        assert!(DeliverySnapshotHashInput::try_new(Vec::new()).is_ok());
    }

    #[test]
    fn target_directory_binding_allows_only_the_primary_git_alias() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let common_path = root.join("common-git");
        let checkout_path = root.join("checkout");
        let sandbox_path = root.join("sandbox");
        for directory in [&common_path, &checkout_path, &sandbox_path] {
            std::fs::create_dir(directory).unwrap();
        }
        let common = ExecutionDirectory::open(&common_path).unwrap();
        let checkout = ExecutionDirectory::open(&checkout_path).unwrap();
        let sandbox = ExecutionDirectory::open(&sandbox_path).unwrap();

        assert_eq!(
            require_target_directories(&common, &common, &checkout, &sandbox),
            Ok(())
        );
        assert_eq!(
            require_target_directories(&common, &checkout, &checkout, &sandbox),
            Err(DeliverySourceError::AuthenticationChanged)
        );
        assert_eq!(
            require_target_directories(&common, &common, &checkout, &common),
            Err(DeliverySourceError::AuthenticationChanged)
        );
    }

    #[test]
    fn clean_environment_binds_the_exact_retained_empty_config() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let directory = ExecutionDirectory::open(&root).unwrap();
        #[cfg(unix)]
        let config_path = root.join(".coding-agent-empty-gitconfig");
        #[cfg(unix)]
        let config = {
            std::fs::write(&config_path, b"").unwrap();
            let config_file = std::fs::File::open(&config_path).unwrap();
            std::fs::remove_file(&config_path).unwrap();
            Arc::new(
                crate::command_policy::DeliveryGitEmptyConfig::from_retained_sandbox_file(
                    Arc::new(ExecutionDirectory::open(&root).unwrap()),
                    config_file,
                )
                .unwrap(),
            )
        };
        #[cfg(windows)]
        let config = Arc::new(crate::command_policy::DeliveryGitEmptyConfig::windows_nul());
        #[cfg(windows)]
        let system_root = std::env::var_os("SYSTEMROOT")
            .or_else(|| std::env::var_os("WINDIR"))
            .map(std::path::PathBuf::from);
        #[cfg(unix)]
        let system_root = None;
        let platform = PlatformEnvironment::try_new(root.clone(), system_root).unwrap();
        let environment = delivery_git_environment(&platform, &config, &directory).unwrap();
        let entries = environment.entries();
        let mut expected_config = std::collections::BTreeMap::new();
        config
            .apply_delivery_git_environment(&mut expected_config)
            .unwrap();
        for key in ["GIT_DIR", "GIT_WORK_TREE", "GIT_CONFIG_COUNT"] {
            assert!(!entries.contains_key(OsStr::new(key)));
        }
        for key in ["GIT_CONFIG_GLOBAL", "GIT_CONFIG_SYSTEM"] {
            assert_eq!(
                entries.get(OsStr::new(key)).map(OsString::as_os_str),
                expected_config
                    .get(OsStr::new(key))
                    .map(OsString::as_os_str)
            );
        }
        #[cfg(unix)]
        for key in ["GIT_CONFIG_GLOBAL", "GIT_CONFIG_SYSTEM"] {
            assert_ne!(
                entries.get(OsStr::new(key)).map(OsString::as_os_str),
                Some(config_path.as_os_str())
            );
        }
        #[cfg(windows)]
        for key in ["GIT_CONFIG_GLOBAL", "GIT_CONFIG_SYSTEM"] {
            assert_eq!(
                entries.get(OsStr::new(key)).map(OsString::as_os_str),
                Some(OsStr::new("NUL"))
            );
        }
        assert_eq!(
            entries.get(OsStr::new("HOME")).map(OsString::as_os_str),
            Some(root.as_os_str())
        );
    }
}
