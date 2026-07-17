use coding_agent_runtime::{
    AtomicFileReplacer, AtomicReplaceError, AtomicReplaceLimits, RelativePath, ReplaceDisposition,
    RootCapability,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

#[test]
fn null_digest_creates_only_when_the_target_is_absent() {
    let fixture = fixture(1_024);
    let path = RelativePath::parse("src/new.rs").unwrap();
    std::fs::create_dir(fixture.root_path.join("src")).unwrap();

    let result = fixture
        .replacer
        .replace_file(&path, None, b"fn new() {}\n", &CancellationToken::new())
        .unwrap();

    assert_eq!(result.disposition, ReplaceDisposition::Created);
    assert_eq!(result.bytes_written, 12);
    assert_eq!(result.sha256, digest(b"fn new() {}\n"));
    assert_eq!(
        std::fs::read(fixture.root_path.join("src/new.rs")).unwrap(),
        b"fn new() {}\n"
    );
    assert_no_temporary_files(&fixture.root_path.join("src"));

    assert!(matches!(
        fixture
            .replacer
            .replace_file(&path, None, b"other", &CancellationToken::new()),
        Err(AtomicReplaceError::TargetAlreadyExists)
    ));
    assert_eq!(
        std::fs::read(fixture.root_path.join("src/new.rs")).unwrap(),
        b"fn new() {}\n"
    );
}

#[test]
fn a_single_code_unit_target_name_can_be_created_and_replaced() {
    let fixture = fixture(1_024);
    let path = RelativePath::parse("x").unwrap();

    fixture
        .replacer
        .replace_file(&path, None, b"old", &CancellationToken::new())
        .unwrap();
    fixture
        .replacer
        .replace_file(
            &path,
            Some(&digest(b"old")),
            b"new",
            &CancellationToken::new(),
        )
        .unwrap();

    assert_eq!(std::fs::read(fixture.root_path.join("x")).unwrap(), b"new");
}

#[test]
fn matching_digest_replaces_and_stale_or_missing_digests_fail_closed() {
    let fixture = fixture(1_024);
    let path = RelativePath::parse("value.txt").unwrap();
    std::fs::write(fixture.root_path.join("value.txt"), b"old").unwrap();

    assert!(matches!(
        fixture.replacer.replace_file(
            &path,
            Some(&digest(b"stale")),
            b"wrong",
            &CancellationToken::new(),
        ),
        Err(AtomicReplaceError::FileChangedSinceRead)
    ));
    assert_eq!(
        std::fs::read(fixture.root_path.join("value.txt")).unwrap(),
        b"old"
    );

    let result = fixture
        .replacer
        .replace_file(
            &path,
            Some(&digest(b"old")),
            b"new",
            &CancellationToken::new(),
        )
        .unwrap();
    assert_eq!(result.disposition, ReplaceDisposition::Replaced);
    assert_eq!(
        std::fs::read(fixture.root_path.join("value.txt")).unwrap(),
        b"new"
    );

    let missing = RelativePath::parse("missing.txt").unwrap();
    assert!(matches!(
        fixture.replacer.replace_file(
            &missing,
            Some(&digest(b"anything")),
            b"new",
            &CancellationToken::new(),
        ),
        Err(AtomicReplaceError::TargetNotFound)
    ));
}

#[test]
fn invalid_inputs_and_pre_cancelled_operations_have_no_side_effects() {
    let fixture = fixture(4);
    let path = RelativePath::parse("value.txt").unwrap();

    assert!(matches!(
        fixture.replacer.replace_file(
            &RelativePath::parse("").unwrap(),
            None,
            b"x",
            &CancellationToken::new(),
        ),
        Err(AtomicReplaceError::TargetNotRegular)
    ));
    assert!(matches!(
        fixture
            .replacer
            .replace_file(&path, Some("ABC"), b"x", &CancellationToken::new(),),
        Err(AtomicReplaceError::InvalidExpectedDigest)
    ));
    assert!(matches!(
        fixture
            .replacer
            .replace_file(&path, None, b"12345", &CancellationToken::new(),),
        Err(AtomicReplaceError::ContentTooLarge)
    ));

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        fixture
            .replacer
            .replace_file(&path, None, b"x", &cancellation),
        Err(AtomicReplaceError::Cancelled)
    ));
    assert!(!fixture.root_path.join("value.txt").exists());
    assert_no_temporary_files(&fixture.root_path);
}

#[test]
fn replacement_never_follows_a_final_link_or_overwrites_a_directory() {
    let fixture = fixture(1_024);
    let outside = tempfile::tempdir().unwrap();
    let outside_file = outside.path().join("outside.txt");
    std::fs::write(&outside_file, b"outside").unwrap();
    create_file_link(&outside_file, &fixture.root_path.join("link.txt")).unwrap();
    std::fs::create_dir(fixture.root_path.join("directory")).unwrap();

    assert!(matches!(
        fixture.replacer.replace_file(
            &RelativePath::parse("link.txt").unwrap(),
            None,
            b"inside",
            &CancellationToken::new(),
        ),
        Err(AtomicReplaceError::TargetAlreadyExists)
    ));
    assert!(matches!(
        fixture.replacer.replace_file(
            &RelativePath::parse("link.txt").unwrap(),
            Some(&digest(b"outside")),
            b"inside",
            &CancellationToken::new(),
        ),
        Err(AtomicReplaceError::TargetNotRegular)
    ));
    assert!(matches!(
        fixture.replacer.replace_file(
            &RelativePath::parse("directory").unwrap(),
            None,
            b"inside",
            &CancellationToken::new(),
        ),
        Err(AtomicReplaceError::TargetAlreadyExists)
    ));
    assert_eq!(std::fs::read(outside_file).unwrap(), b"outside");
    assert_no_temporary_files(&fixture.root_path);
}

#[cfg(unix)]
#[test]
fn replacement_preserves_the_existing_posix_mode() {
    use std::os::unix::fs::PermissionsExt;

    let fixture = fixture(1_024);
    let target = fixture.root_path.join("script.sh");
    std::fs::write(&target, b"old").unwrap();
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o751)).unwrap();

    fixture
        .replacer
        .replace_file(
            &RelativePath::parse("script.sh").unwrap(),
            Some(&digest(b"old")),
            b"new",
            &CancellationToken::new(),
        )
        .unwrap();

    assert_eq!(
        std::fs::metadata(target).unwrap().permissions().mode() & 0o7777,
        0o751
    );
}

#[cfg(windows)]
#[test]
fn replacement_preserves_the_existing_windows_readonly_bit() {
    let fixture = fixture(1_024);
    let target = fixture.root_path.join("readonly.txt");
    std::fs::write(&target, b"old").unwrap();
    let mut permissions = std::fs::metadata(&target).unwrap().permissions();
    permissions.set_readonly(true);
    std::fs::set_permissions(&target, permissions).unwrap();

    fixture
        .replacer
        .replace_file(
            &RelativePath::parse("readonly.txt").unwrap(),
            Some(&digest(b"old")),
            b"new",
            &CancellationToken::new(),
        )
        .unwrap();

    assert_eq!(std::fs::read(&target).unwrap(), b"new");
    assert!(std::fs::metadata(target).unwrap().permissions().readonly());
}

#[cfg(windows)]
#[test]
fn an_occupied_windows_target_is_preserved_when_atomic_replace_fails() {
    use std::io::Read;
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_READ, FILE_SHARE_WRITE};

    let fixture = fixture(1_024);
    let target = fixture.root_path.join("occupied.txt");
    std::fs::write(&target, b"old").unwrap();
    let mut occupied = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .open(&target)
        .unwrap();

    let result = fixture.replacer.replace_file(
        &RelativePath::parse("occupied.txt").unwrap(),
        Some(&digest(b"old")),
        b"new",
        &CancellationToken::new(),
    );

    assert!(matches!(
        result,
        Err(AtomicReplaceError::AtomicReplaceFailed(_))
    ));
    assert_eq!(std::fs::read(&target).unwrap(), b"old");
    let mut held_content = Vec::new();
    occupied.read_to_end(&mut held_content).unwrap();
    assert_eq!(held_content, b"old");
    assert_no_temporary_files(&fixture.root_path);
}

#[cfg(windows)]
#[test]
fn repeated_occupied_failures_do_not_leak_windows_handles() {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_READ, FILE_SHARE_WRITE};

    let fixture = fixture(1_024);
    let target = fixture.root_path.join("occupied.txt");
    std::fs::write(&target, b"old").unwrap();
    let _occupied = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .open(&target)
        .unwrap();
    let before = process_handle_count();

    for _ in 0..500 {
        assert!(matches!(
            fixture.replacer.replace_file(
                &RelativePath::parse("occupied.txt").unwrap(),
                Some(&digest(b"old")),
                b"new",
                &CancellationToken::new(),
            ),
            Err(AtomicReplaceError::AtomicReplaceFailed(_))
        ));
    }

    let after = process_handle_count();
    assert!(
        after <= before + 16,
        "occupied replacements leaked handles: before={before}, after={after}"
    );
    assert_eq!(std::fs::read(target).unwrap(), b"old");
    assert_no_temporary_files(&fixture.root_path);
}

struct Fixture {
    _temp: tempfile::TempDir,
    root_path: std::path::PathBuf,
    replacer: AtomicFileReplacer,
}

fn fixture(max_content_bytes: usize) -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let root_path = temp.path().to_path_buf();
    let root = RootCapability::open(&root_path).unwrap();
    let limits = AtomicReplaceLimits::try_new(max_content_bytes).unwrap();
    Fixture {
        _temp: temp,
        root_path,
        replacer: AtomicFileReplacer::new(root, limits),
    }
}

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn assert_no_temporary_files(directory: &std::path::Path) {
    let names = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(
        names
            .iter()
            .all(|name| !name.starts_with(".coding-agent-replace-")),
        "temporary files remained: {names:?}"
    );
}

#[cfg(unix)]
fn create_file_link(target: &std::path::Path, link: &std::path::Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_file_link(target: &std::path::Path, link: &std::path::Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_file(target, link)
}

#[cfg(windows)]
fn process_handle_count() -> u32 {
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessHandleCount};

    let mut count = 0;
    assert_ne!(
        unsafe { GetProcessHandleCount(GetCurrentProcess(), &mut count) },
        0
    );
    count
}
