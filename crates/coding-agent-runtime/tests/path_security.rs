use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::path::Path;
#[cfg(windows)]
use std::path::PathBuf;

use coding_agent_runtime::{
    DirectoryIdentityError, RelativePath, RelativePathError, RootCapability,
};

#[test]
fn relative_paths_are_utf8_slash_paths_with_an_explicit_root() {
    let root = RelativePath::parse("").unwrap();
    let file = RelativePath::parse("src/nested/lib.rs").unwrap();

    assert!(root.is_root());
    assert_eq!(root.components().collect::<Vec<_>>(), Vec::<&str>::new());
    assert_eq!(file.as_slash_str(), "src/nested/lib.rs");
    assert_eq!(
        file.components().collect::<Vec<_>>(),
        vec!["src", "nested", "lib.rs"]
    );
}

#[test]
fn relative_paths_reject_namespace_and_metadata_escapes() {
    let cases = [
        ("/etc/passwd", RelativePathError::Absolute),
        ("C:/Windows/win.ini", RelativePathError::Absolute),
        (r"C:\Windows\win.ini", RelativePathError::Absolute),
        (r"\\server\share\file", RelativePathError::Backslash),
        (r"\\?\C:\file", RelativePathError::Backslash),
        ("src//lib.rs", RelativePathError::EmptyComponent),
        ("src/./lib.rs", RelativePathError::CurrentDirectory),
        ("src/../secret", RelativePathError::ParentDirectory),
        ("src\\lib.rs", RelativePathError::Backslash),
        ("src/name:stream", RelativePathError::AlternateDataStream),
        ("src/trailing. ", RelativePathError::TrailingDotOrSpace),
        ("src/CON.txt", RelativePathError::ReservedDeviceName),
        ("src/.git/config", RelativePathError::ProtectedMetadata),
        ("src/.GIT /config", RelativePathError::ProtectedMetadata),
    ];

    for (input, expected) in cases {
        assert_eq!(
            RelativePath::parse(input).unwrap_err(),
            expected,
            "unexpected result for {input:?}"
        );
    }
    assert_eq!(
        RelativePath::parse("src/evil\0name").unwrap_err(),
        RelativePathError::Nul
    );
}

#[cfg(unix)]
#[test]
fn relative_paths_reject_non_utf8_os_paths() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let path = OsString::from_vec(vec![b's', b'r', b'c', b'/', 0xff]);
    assert_eq!(
        RelativePath::try_from_os_path(Path::new(&path)).unwrap_err(),
        RelativePathError::NonUtf8
    );
}

#[test]
fn root_capability_reads_regular_files_one_component_at_a_time() {
    let temp = tempfile::tempdir().unwrap();
    let nested = temp.path().join("src").join("nested");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(nested.join("lib.rs"), b"pub fn answer() -> u8 { 42 }").unwrap();

    let root = RootCapability::open(temp.path().canonicalize().unwrap()).unwrap();
    let relative = RelativePath::parse("src/nested/lib.rs").unwrap();
    let mut file = root.open_file_for_read(&relative).unwrap();
    let mut content = String::new();
    file.read_to_string(&mut content).unwrap();

    assert_eq!(content, "pub fn answer() -> u8 { 42 }");
}

#[test]
fn directory_identity_markers_compare_and_hash_only_by_authenticated_object() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let first_path = first.path().canonicalize().unwrap();
    let first_capability = RootCapability::open(&first_path).unwrap();
    let same_capability = RootCapability::open(&first_path).unwrap();
    let second_capability = RootCapability::open(second.path().canonicalize().unwrap()).unwrap();

    let first_marker = first_capability.identity_marker().unwrap();
    let same_marker = same_capability.identity_marker().unwrap();
    let second_marker = second_capability.identity_marker().unwrap();
    let markers = HashSet::from([first_marker, same_marker, second_marker]);

    assert_eq!(first_marker, same_marker);
    assert_ne!(first_marker, second_marker);
    assert_eq!(markers.len(), 2);
    let mut recording_hasher = RecordingHasher::default();
    first_marker.hash(&mut recording_hasher);
    assert_eq!(
        recording_hasher.bytes.len(),
        std::mem::size_of::<u64>(),
        "Hash must expose only one keyed opaque token, never raw platform identity fields"
    );
    assert_eq!(
        format!("{first_marker:?}"),
        "DirectoryIdentityMarker(<opaque>)"
    );
    assert!(!format!("{first_marker:?}").contains(&first_path.display().to_string()));
    assert!(first_capability.require_identity(first_marker).is_ok());
    assert_eq!(
        first_capability.require_identity(second_marker),
        Err(DirectoryIdentityError::Mismatch)
    );
    assert_eq!(
        DirectoryIdentityError::Unavailable.to_string(),
        "authenticated directory identity is unavailable"
    );
    assert_eq!(
        DirectoryIdentityError::Mismatch.to_string(),
        "authenticated directory identity does not match"
    );
}

#[cfg(windows)]
#[test]
fn windows_case_and_subst_aliases_share_directory_identity() {
    use std::process::Command;

    let temporary = tempfile::tempdir().unwrap();
    let directory = temporary.path().join("MarkerCaseRoot");
    std::fs::create_dir(&directory).unwrap();
    let canonical = directory.canonicalize().unwrap();
    let expected = RootCapability::open(&canonical)
        .unwrap()
        .identity_marker()
        .unwrap();

    let case_alias = canonical.to_string_lossy().to_ascii_uppercase();
    assert_eq!(
        RootCapability::open(case_alias)
            .unwrap()
            .identity_marker()
            .unwrap(),
        expected
    );

    let Some(drive) = (b'D'..=b'Z')
        .rev()
        .map(|letter| format!("{}:", char::from(letter)))
        .find(|drive| !Path::new(&format!("{drive}\\")).exists())
    else {
        return;
    };
    let status = Command::new("subst.exe")
        .arg(&drive)
        .arg(temporary.path().canonicalize().unwrap())
        .status();
    let Ok(status) = status else {
        return;
    };
    if !status.success() {
        return;
    }
    let _subst = SubstDrive(drive.clone());
    let subst_root = PathBuf::from(format!("{drive}\\"));
    let subst_alias = subst_root.join("MarkerCaseRoot");
    assert_eq!(
        RootCapability::open(subst_alias)
            .unwrap()
            .identity_marker()
            .unwrap(),
        expected
    );
}

#[test]
fn retained_directory_identity_rejects_a_same_path_replacement() {
    let parent = tempfile::tempdir().unwrap();
    let path = parent.path().join("repository");
    let retained_path = parent.path().join("retained-repository");
    std::fs::create_dir(&path).unwrap();
    let capability = RootCapability::open(path.canonicalize().unwrap()).unwrap();
    let marker = capability.identity_marker().unwrap();

    std::fs::rename(&path, &retained_path).unwrap();
    std::fs::create_dir(&path).unwrap();
    let replacement = RootCapability::open(path.canonicalize().unwrap()).unwrap();
    let replacement_marker = replacement.identity_marker().unwrap();

    assert_eq!(capability.identity_marker().unwrap(), marker);
    assert_ne!(replacement_marker, marker);
    assert_eq!(
        replacement.require_identity(marker),
        Err(DirectoryIdentityError::Mismatch)
    );
}

#[derive(Default)]
struct RecordingHasher {
    bytes: Vec<u8>,
}

impl Hasher for RecordingHasher {
    fn finish(&self) -> u64 {
        0
    }

    fn write(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }
}

#[cfg(windows)]
struct SubstDrive(String);

#[cfg(windows)]
impl Drop for SubstDrive {
    fn drop(&mut self) {
        let _ = std::process::Command::new("subst.exe")
            .arg(&self.0)
            .arg("/D")
            .status();
    }
}

#[test]
fn root_capability_rejects_final_and_ancestor_links() {
    let temp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("secret.txt"), b"outside").unwrap();
    std::fs::create_dir(temp.path().join("safe")).unwrap();
    std::fs::write(temp.path().join("safe").join("inside.txt"), b"inside").unwrap();
    let root = RootCapability::open(temp.path().canonicalize().unwrap()).unwrap();

    create_file_link(
        &outside.path().join("secret.txt"),
        &temp.path().join("final-link"),
    )
    .expect("path-security tests require file-link creation");
    let final_link = RelativePath::parse("final-link").unwrap();
    assert!(root.open_file_for_read(&final_link).is_err());

    create_dir_link(outside.path(), &temp.path().join("ancestor-link"))
        .expect("path-security tests require directory-link creation");
    let ancestor_link = RelativePath::parse("ancestor-link/secret.txt").unwrap();
    assert!(root.open_file_for_read(&ancestor_link).is_err());
}

#[test]
fn root_capability_rejects_a_link_as_the_root() {
    let parent = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let link = parent.path().canonicalize().unwrap().join("root-link");
    create_dir_link(target.path(), &link)
        .expect("path-security tests require directory-link creation");
    assert!(RootCapability::open(&link).is_err());
}

#[test]
fn root_capability_rejects_a_link_in_the_root_ancestors() {
    let parent = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::create_dir(outside.path().join("worktree")).unwrap();
    let link = parent.path().canonicalize().unwrap().join("ancestor-link");
    create_dir_link(outside.path(), &link)
        .expect("path-security tests require directory-link creation");

    assert!(RootCapability::open(link.join("worktree")).is_err());
}

#[cfg(windows)]
#[test]
fn repeated_rejected_links_do_not_leak_process_handles() {
    let temp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("secret"), b"outside").unwrap();
    create_file_link(&outside.path().join("secret"), &temp.path().join("link")).unwrap();
    let root = RootCapability::open(temp.path()).unwrap();
    let link = RelativePath::parse("link").unwrap();
    let before = process_handle_count();

    for _ in 0..2_000 {
        assert!(root.open_file_for_read(&link).is_err());
    }

    let after = process_handle_count();
    assert!(
        after <= before + 16,
        "rejected link opens leaked handles: before={before}, after={after}"
    );
}

#[cfg(windows)]
#[test]
fn root_capability_rejects_windows_junctions() {
    let temp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("secret"), b"outside").unwrap();
    let junction = temp.path().join("junction");
    create_junction(outside.path(), &junction).unwrap();
    let root = RootCapability::open(temp.path()).unwrap();

    assert!(
        root.open_file_for_read(&RelativePath::parse("junction/secret").unwrap())
            .is_err()
    );
    assert!(RootCapability::open(junction).is_err());
}

#[cfg(unix)]
#[test]
fn opening_a_fifo_fails_without_blocking() {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::time::{Duration, Instant};

    let temp = tempfile::tempdir().unwrap();
    let fifo = temp.path().join("pipe");
    let fifo_name = CString::new(fifo.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo_name.as_ptr(), 0o600) }, 0);
    let root = RootCapability::open(temp.path().canonicalize().unwrap()).unwrap();
    let started = Instant::now();

    assert!(
        root.open_file_for_read(&RelativePath::parse("pipe").unwrap())
            .is_err()
    );
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[cfg(unix)]
fn create_file_link(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(unix)]
fn create_dir_link(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_file_link(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_file(target, link)
}

#[cfg(windows)]
fn create_dir_link(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_dir(target, link)
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

#[cfg(windows)]
fn create_junction(target: &Path, junction: &Path) -> std::io::Result<()> {
    use std::ffi::c_void;
    use std::fs::File;
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::{AsRawHandle, FromRawHandle};
    use std::ptr::{null, null_mut};

    use windows_sys::Win32::Foundation::{GENERIC_WRITE, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
    };
    use windows_sys::Win32::System::IO::DeviceIoControl;
    use windows_sys::Win32::System::Ioctl::FSCTL_SET_REPARSE_POINT;
    use windows_sys::Win32::System::SystemServices::IO_REPARSE_TAG_MOUNT_POINT;

    let target = target.canonicalize()?;
    std::fs::create_dir(junction)?;
    let mut junction_wide = junction
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let handle = unsafe {
        CreateFileW(
            junction_wide.as_mut_ptr(),
            GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            null(),
            OPEN_EXISTING,
            FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS,
            null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        let error = std::io::Error::last_os_error();
        let _ = std::fs::remove_dir(junction);
        return Err(error);
    }
    let file = unsafe { File::from_raw_handle(handle) };

    let mut substitute = r"\??\".encode_utf16().collect::<Vec<_>>();
    substitute.extend(target.as_os_str().encode_wide());
    let print = target.as_os_str().encode_wide().collect::<Vec<_>>();
    let print_offset = (substitute.len() + 1)
        .checked_mul(2)
        .and_then(|value| u16::try_from(value).ok())
        .ok_or_else(|| std::io::Error::other("junction target is too long"))?;
    let substitute_bytes = u16::try_from(substitute.len() * 2)
        .map_err(|_| std::io::Error::other("junction target is too long"))?;
    let print_bytes = u16::try_from(print.len() * 2)
        .map_err(|_| std::io::Error::other("junction target is too long"))?;
    let mut paths = substitute;
    paths.push(0);
    paths.extend(print);
    paths.push(0);
    let data_length = u16::try_from(8 + paths.len() * 2)
        .map_err(|_| std::io::Error::other("junction target is too long"))?;
    let mut buffer = vec![0u8; 8 + data_length as usize];
    buffer[0..4].copy_from_slice(&IO_REPARSE_TAG_MOUNT_POINT.to_le_bytes());
    buffer[4..6].copy_from_slice(&data_length.to_le_bytes());
    buffer[8..10].copy_from_slice(&0u16.to_le_bytes());
    buffer[10..12].copy_from_slice(&substitute_bytes.to_le_bytes());
    buffer[12..14].copy_from_slice(&print_offset.to_le_bytes());
    buffer[14..16].copy_from_slice(&print_bytes.to_le_bytes());
    for (index, value) in paths.into_iter().enumerate() {
        let offset = 16 + index * 2;
        buffer[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }

    let mut returned = 0u32;
    let succeeded = unsafe {
        DeviceIoControl(
            file.as_raw_handle(),
            FSCTL_SET_REPARSE_POINT,
            buffer.as_ptr().cast::<c_void>(),
            buffer.len() as u32,
            null_mut(),
            0,
            &mut returned,
            null_mut(),
        )
    };
    if succeeded == 0 {
        let error = std::io::Error::last_os_error();
        drop(file);
        let _ = std::fs::remove_dir(junction);
        Err(error)
    } else {
        Ok(())
    }
}
