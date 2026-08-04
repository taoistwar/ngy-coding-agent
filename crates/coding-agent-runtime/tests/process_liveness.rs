mod support;

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;

use coding_agent_runtime::{ProcessCleanupProof, ProcessLivenessDirectory, ProcessLivenessError};

const SENTINEL_DIRECTORY: &str = "process-liveness";
const CONTENT_MAGIC: &str = "coding-agent-process-liveness-v1";

#[test]
fn empty_directory_and_scope_have_confirmed_cleanup_proof() {
    let fixture = tempfile::tempdir().expect("create liveness fixture");
    let runtime = support::private_liveness_runtime(fixture.path());
    let directory = ProcessLivenessDirectory::open(&runtime).expect("open liveness directory");
    let instance = directory
        .instance_scope(test_uuid(1))
        .expect("create instance scope");
    let task = instance
        .task_scope(test_uuid(2))
        .expect("create task scope");

    assert_eq!(
        directory.probe_stale().expect("probe an empty directory"),
        ProcessCleanupProof::Confirmed
    );
    assert_eq!(
        instance
            .cleanup_proof()
            .expect("prove empty instance scope"),
        ProcessCleanupProof::Confirmed
    );
    assert_eq!(
        task.cleanup_proof().expect("prove empty task scope"),
        ProcessCleanupProof::Confirmed
    );
    assert_eq!(instance.active_tree_count(), 0);
    assert_eq!(task.active_tree_count(), 0);
    assert_eq!(
        format!("{directory:?}"),
        "ProcessLivenessDirectory(<opaque>)"
    );
    assert_eq!(format!("{task:?}"), "ProcessLivenessScope(<opaque>)");
}

#[test]
fn invalid_instance_and_task_identities_are_rejected() {
    let fixture = tempfile::tempdir().expect("create identity fixture");
    let runtime = support::private_liveness_runtime(fixture.path());
    let directory = ProcessLivenessDirectory::open(&runtime).expect("open liveness directory");

    assert_eq!(
        directory.instance_scope([0; 16]).unwrap_err(),
        ProcessLivenessError::InvalidIdentity
    );
    let instance = directory
        .instance_scope(test_uuid(3))
        .expect("create valid instance scope");
    assert_eq!(
        instance.task_scope([0; 16]).unwrap_err(),
        ProcessLivenessError::InvalidIdentity
    );

    let mut wrong_version = test_uuid(4);
    wrong_version[6] = (wrong_version[6] & 0x0f) | 0x50;
    assert_eq!(
        directory.instance_scope(wrong_version).unwrap_err(),
        ProcessLivenessError::InvalidIdentity
    );

    let mut wrong_variant = test_uuid(5);
    wrong_variant[8] = (wrong_variant[8] & 0x3f) | 0x40;
    assert_eq!(
        directory.instance_scope(wrong_variant).unwrap_err(),
        ProcessLivenessError::InvalidIdentity
    );
    assert_eq!(
        instance.task_scope(wrong_version).unwrap_err(),
        ProcessLivenessError::InvalidIdentity
    );
    assert_eq!(
        instance.task_scope(wrong_variant).unwrap_err(),
        ProcessLivenessError::InvalidIdentity
    );
}

#[test]
fn held_sentinel_is_never_deleted_and_released_sentinel_is_removed() {
    let fixture = tempfile::tempdir().expect("create held-sentinel fixture");
    let runtime = support::private_liveness_runtime(fixture.path());
    let directory = ProcessLivenessDirectory::open(&runtime).expect("open liveness directory");
    let sentinel_directory = runtime.join(SENTINEL_DIRECTORY);
    let name = sentinel_name(test_uuid(4), Some(test_uuid(5)), [6; 16]);
    let sentinel = create_private_sentinel(&sentinel_directory, &name);
    sentinel.try_lock().expect("hold sentinel lock");

    assert_eq!(
        directory.probe_stale().expect("probe held sentinel"),
        ProcessCleanupProof::Held
    );
    assert!(
        sentinel_directory.join(&name).is_file(),
        "a held sentinel must remain in the namespace"
    );

    drop(sentinel);
    assert_eq!(
        directory.probe_stale().expect("probe released sentinel"),
        ProcessCleanupProof::Confirmed
    );
    assert!(
        !sentinel_directory.join(name).exists(),
        "only an exclusively probed stale sentinel may be deleted"
    );
}

#[test]
fn same_task_can_hold_multiple_independent_tree_nonces() {
    let fixture = tempfile::tempdir().expect("create nonce fixture");
    let runtime = support::private_liveness_runtime(fixture.path());
    let directory = ProcessLivenessDirectory::open(&runtime).expect("open liveness directory");
    let sentinel_directory = runtime.join(SENTINEL_DIRECTORY);
    let instance_id = test_uuid(7);
    let task_id = test_uuid(8);
    let first_name = sentinel_name(instance_id, Some(task_id), [9; 16]);
    let second_name = sentinel_name(instance_id, Some(task_id), [10; 16]);
    assert_ne!(first_name, second_name);

    let first = create_private_sentinel(&sentinel_directory, &first_name);
    let second = create_private_sentinel(&sentinel_directory, &second_name);
    first.try_lock().expect("hold first tree sentinel");
    second.try_lock().expect("hold second tree sentinel");

    assert_eq!(
        directory.probe_stale().expect("probe two held trees"),
        ProcessCleanupProof::Held
    );
    assert_eq!(
        fs::read_dir(&sentinel_directory)
            .expect("scan held tree sentinels")
            .count(),
        2
    );

    drop(first);
    drop(second);
    assert_eq!(
        directory.probe_stale().expect("clean two stale trees"),
        ProcessCleanupProof::Confirmed
    );
    assert_eq!(
        fs::read_dir(&sentinel_directory)
            .expect("scan cleaned sentinel directory")
            .count(),
        0
    );
}

#[cfg(unix)]
#[test]
fn interrupted_quarantine_cleanup_is_authenticated_and_recoverable() {
    let fixture = tempfile::tempdir().expect("create quarantine-recovery fixture");
    let runtime = support::private_liveness_runtime(fixture.path());
    let directory = ProcessLivenessDirectory::open(&runtime).expect("open liveness directory");
    let sentinel_directory = runtime.join(SENTINEL_DIRECTORY);
    let name = sentinel_name(test_uuid(31), Some(test_uuid(32)), [33; 16]);
    let sentinel = create_private_sentinel(&sentinel_directory, &name);
    drop(sentinel);

    let quarantine = format!(".cleanup-v1-{}-{name}", hex([34; 16]));
    fs::rename(
        sentinel_directory.join(&name),
        sentinel_directory.join(&quarantine),
    )
    .expect("simulate a crash after quarantine rename");

    assert_eq!(
        directory
            .probe_stale()
            .expect("recover an interrupted quarantine"),
        ProcessCleanupProof::Confirmed
    );
    assert!(
        !sentinel_directory.join(&quarantine).exists(),
        "a validated interrupted quarantine must be removed"
    );
}

#[test]
fn malformed_name_and_forged_content_fail_closed_without_deletion() {
    let fixture = tempfile::tempdir().expect("create forgery fixture");
    let runtime = support::private_liveness_runtime(fixture.path());
    let directory = ProcessLivenessDirectory::open(&runtime).expect("open liveness directory");
    let sentinel_directory = runtime.join(SENTINEL_DIRECTORY);
    let malformed = sentinel_directory.join("latest.sentinel");
    fs::write(&malformed, b"forged").expect("write malformed sentinel");

    assert_eq!(
        directory.probe_stale().expect("probe malformed sentinel"),
        ProcessCleanupProof::Unknown
    );
    assert!(malformed.exists());
    fs::remove_file(&malformed).expect("remove malformed fixture");

    let valid_name = sentinel_name(test_uuid(11), None, [12; 16]);
    let forged = sentinel_directory.join(&valid_name);
    fs::write(&forged, b"forged").expect("write forged sentinel contents");
    make_owner_only(&forged);

    assert_eq!(
        directory.probe_stale().expect("probe forged sentinel"),
        ProcessCleanupProof::Unknown
    );
    assert!(
        forged.exists(),
        "a syntactically valid name cannot authorize deletion of forged contents"
    );
}

#[cfg(windows)]
#[test]
fn sentinel_directory_rejects_any_non_owner_allow_ace() {
    let fixture = tempfile::tempdir().expect("create ACL rejection fixture");
    let runtime = support::private_liveness_runtime(fixture.path());
    let directory =
        ProcessLivenessDirectory::open(&runtime).expect("create private sentinel directory");
    drop(directory);
    support::add_non_owner_allow_ace(&runtime.join(SENTINEL_DIRECTORY))
        .expect("add a non-owner allow ACE");

    assert_eq!(
        ProcessLivenessDirectory::open(&runtime).unwrap_err(),
        ProcessLivenessError::Unavailable,
        "an allow ACE for LocalSystem must fail the owner-only DACL contract"
    );
}

#[test]
fn hard_link_alias_fails_closed_and_preserves_both_names() {
    let fixture = tempfile::tempdir().expect("create hard-link fixture");
    let runtime = support::private_liveness_runtime(fixture.path());
    let directory = ProcessLivenessDirectory::open(&runtime).expect("open liveness directory");
    let sentinel_directory = runtime.join(SENTINEL_DIRECTORY);
    let name = sentinel_name(test_uuid(13), None, [14; 16]);
    let original = fixture.path().join("outside-sentinel");
    let mut file = create_private_file(&original);
    file.write_all(sentinel_contents(&name).as_bytes())
        .expect("write aliased sentinel contents");
    file.sync_all().expect("sync aliased sentinel contents");
    drop(file);
    let alias = sentinel_directory.join(&name);
    fs::hard_link(&original, &alias).expect("create sentinel hard link");

    assert_eq!(
        directory.probe_stale().expect("probe hard-link alias"),
        ProcessCleanupProof::Unknown
    );
    assert!(original.exists());
    assert!(alias.exists());
}

#[cfg(unix)]
#[test]
fn symlink_substitute_fails_closed_without_touching_its_target() {
    use std::os::unix::fs::symlink;

    let fixture = tempfile::tempdir().expect("create symlink fixture");
    let runtime = support::private_liveness_runtime(fixture.path());
    let directory = ProcessLivenessDirectory::open(&runtime).expect("open liveness directory");
    let sentinel_directory = runtime.join(SENTINEL_DIRECTORY);
    let name = sentinel_name(test_uuid(15), None, [16; 16]);
    let target = fixture.path().join("target");
    fs::write(&target, b"target bytes").expect("write symlink target");
    let substitute = sentinel_directory.join(name);
    symlink(&target, &substitute).expect("install sentinel symlink");

    assert_eq!(
        directory.probe_stale().expect("probe symlink substitute"),
        ProcessCleanupProof::Unknown
    );
    assert_eq!(
        fs::read(&target).expect("read untouched symlink target"),
        b"target bytes"
    );
    assert!(
        fs::symlink_metadata(substitute)
            .expect("inspect retained symlink")
            .file_type()
            .is_symlink()
    );
}

#[cfg(windows)]
#[test]
fn reparse_substitute_fails_closed_without_touching_its_target() {
    use std::os::windows::fs::symlink_file;

    let fixture = tempfile::tempdir().expect("create reparse fixture");
    let runtime = support::private_liveness_runtime(fixture.path());
    let directory = ProcessLivenessDirectory::open(&runtime).expect("open liveness directory");
    let sentinel_directory = runtime.join(SENTINEL_DIRECTORY);
    let name = sentinel_name(test_uuid(17), None, [18; 16]);
    let target = fixture.path().join("target");
    fs::write(&target, b"target bytes").expect("write reparse target");
    let substitute = sentinel_directory.join(name);
    if let Err(error) = symlink_file(&target, &substitute) {
        if error.kind() == std::io::ErrorKind::PermissionDenied {
            return;
        }
        panic!("install sentinel reparse substitute: {error}");
    }

    assert_eq!(
        directory.probe_stale().expect("probe reparse substitute"),
        ProcessCleanupProof::Unknown
    );
    assert_eq!(
        fs::read(&target).expect("read untouched reparse target"),
        b"target bytes"
    );
    assert!(
        fs::symlink_metadata(substitute)
            .expect("inspect retained reparse substitute")
            .file_type()
            .is_symlink()
    );
}

fn sentinel_name(instance_id: [u8; 16], task_id: Option<[u8; 16]>, nonce: [u8; 16]) -> String {
    let task = task_id.map_or_else(|| "none".to_owned(), hex);
    format!(
        "v1-i-{}-t-{task}-n-{}.sentinel",
        hex(instance_id),
        hex(nonce)
    )
}

fn sentinel_contents(name: &str) -> String {
    format!("{CONTENT_MAGIC}\n{name}\n")
}

fn create_private_sentinel(directory: &Path, name: &str) -> File {
    let path = directory.join(name);
    let mut file = create_private_file(&path);
    file.write_all(sentinel_contents(name).as_bytes())
        .expect("write sentinel contents");
    file.sync_all().expect("sync sentinel contents");
    file
}

fn create_private_file(path: &Path) -> File {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).expect("create private sentinel fixture")
}

#[cfg(unix)]
fn make_owner_only(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("make sentinel owner-only");
}

#[cfg(windows)]
fn make_owner_only(_: &Path) {}

fn test_uuid(seed: u8) -> [u8; 16] {
    let mut identity = [seed; 16];
    identity[6] = (identity[6] & 0x0f) | 0x40;
    identity[8] = (identity[8] & 0x3f) | 0x80;
    identity
}

fn hex(bytes: [u8; 16]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(32);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}
