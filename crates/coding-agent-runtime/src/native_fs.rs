#[cfg(unix)]
use std::ffi::{OsStr, OsString};
#[cfg(unix)]
use std::fs::File;
#[cfg(unix)]
use std::io;

#[cfg(unix)]
pub(crate) fn open_child_directory(parent: &File, name: &OsStr) -> io::Result<File> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;

    let name = CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path component contains NUL"))?;
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY
                | libc::O_CLOEXEC
                | libc::O_DIRECTORY
                | libc::O_NOCTTY
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK,
        )
    };
    if descriptor < 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ENOTDIR) {
            let mut metadata = std::mem::MaybeUninit::<libc::stat>::zeroed();
            let result = unsafe {
                libc::fstatat(
                    parent.as_raw_fd(),
                    name.as_ptr(),
                    metadata.as_mut_ptr(),
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            };
            if result == 0
                && unsafe { metadata.assume_init() }.st_mode & libc::S_IFMT == libc::S_IFLNK
            {
                return Err(io::Error::from_raw_os_error(libc::ELOOP));
            }
        }
        Err(error)
    } else {
        // SAFETY: openat returned a new owned descriptor on success.
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(all(test, unix))]
mod unix_tests {
    use std::ffi::{OsStr, OsString};
    use std::fs::File;
    use std::os::unix::fs::PermissionsExt as _;

    use super::{create_private_child_file_exclusive, open_child_directory, read_directory_names};

    #[test]
    fn private_child_file_excludes_group_and_other_access() {
        let fixture = tempfile::tempdir().expect("create fixture");
        let parent = File::open(fixture.path()).expect("open fixture parent");
        let file = create_private_child_file_exclusive(&parent, OsStr::new("private-file"))
            .expect("create private child");

        assert_eq!(file.metadata().unwrap().permissions().mode() & 0o077, 0);
    }

    #[test]
    fn directory_enumeration_excludes_unix_dot_entries_from_limits_and_results() {
        let fixture = tempfile::tempdir().expect("create fixture");
        let mut directory = File::open(fixture.path()).expect("open fixture parent");

        assert!(read_directory_names(&mut directory, 0).unwrap().is_empty());

        std::fs::write(fixture.path().join("child"), b"child").expect("write child");
        let mut directory = File::open(fixture.path()).expect("reopen fixture parent");
        assert_eq!(
            read_directory_names(&mut directory, 1).unwrap(),
            vec![OsString::from("child")]
        );
    }

    #[test]
    fn directory_open_distinguishes_a_link_from_a_plain_wrong_kind_entry() {
        use std::os::unix::fs::symlink;

        let fixture = tempfile::tempdir().expect("create fixture");
        let parent = File::open(fixture.path()).expect("open fixture parent");
        std::fs::write(fixture.path().join("plain-file"), b"plain")
            .expect("create plain wrong-kind entry");
        symlink("plain-file", fixture.path().join("linked-entry")).expect("create linked entry");

        let plain_error = open_child_directory(&parent, OsStr::new("plain-file"))
            .expect_err("a plain file is not a directory");
        assert_eq!(plain_error.raw_os_error(), Some(libc::ENOTDIR));

        let link_error = open_child_directory(&parent, OsStr::new("linked-entry"))
            .expect_err("a linked entry is never followed as a directory");
        assert_eq!(link_error.raw_os_error(), Some(libc::ELOOP));
    }
}

#[cfg(all(test, windows))]
mod tests {
    use std::ffi::OsStr;
    use std::fs::{File, OpenOptions};
    use std::os::windows::fs::{MetadataExt as _, OpenOptionsExt as _};
    use std::path::Path;

    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ADD_FILE, FILE_ADD_SUBDIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_DELETE_CHILD,
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_LIST_DIRECTORY,
        FILE_READ_ATTRIBUTES, FILE_TRAVERSE, SYNCHRONIZE,
    };

    use super::{
        child_directory_matches, child_file_matches, create_child_directory,
        create_child_file_exclusive, open_child_directory, open_child_file,
        quarantine_child_entry_no_replace, remove_child_directory, remove_child_file,
        reopen_directory_for_delete, reopen_file_for_delete,
    };

    fn open_parent(path: &Path) -> File {
        let mut options = OpenOptions::new();
        options
            .read(true)
            .access_mode(
                FILE_ADD_FILE
                    | FILE_ADD_SUBDIRECTORY
                    | FILE_DELETE_CHILD
                    | FILE_LIST_DIRECTORY
                    | FILE_TRAVERSE
                    | FILE_READ_ATTRIBUTES
                    | SYNCHRONIZE,
            )
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT);
        options.open(path).expect("open child-identity parent")
    }

    #[test]
    fn child_file_match_compares_the_namespace_entry_to_the_open_handle() {
        let fixture = tempfile::tempdir().expect("create child-identity fixture");
        let parent = open_parent(fixture.path());
        assert_eq!(
            parent
                .metadata()
                .expect("read parent metadata")
                .file_attributes()
                & FILE_ATTRIBUTE_REPARSE_POINT,
            0
        );

        let expected_name = OsStr::new("expected.sentinel");
        let other_name = OsStr::new("other.sentinel");
        drop(
            create_child_file_exclusive(&parent, expected_name)
                .expect("create expected namespace child"),
        );
        drop(
            create_child_file_exclusive(&parent, other_name).expect("create other namespace child"),
        );
        let expected =
            open_child_file(&parent, expected_name).expect("open expected namespace child");
        let other = open_child_file(&parent, other_name).expect("open other namespace child");

        assert!(
            child_file_matches(&parent, expected_name, &expected)
                .expect("compare matching child identity")
        );
        assert!(
            !child_file_matches(&parent, expected_name, &other)
                .expect("compare mismatched child identity"),
            "a different open object must never authorize namespace cleanup"
        );
    }

    #[test]
    fn reopen_file_for_delete_keeps_the_retained_file_when_its_name_is_replaced() {
        let fixture = tempfile::tempdir().expect("create retained-file fixture");
        let parent = open_parent(fixture.path());
        let source = OsStr::new("retained-file");
        let quarantine = OsStr::new("quarantined-retained-file");
        let source_path = fixture.path().join(source);
        let moved_path = fixture.path().join("moved-retained-file");

        drop(create_child_file_exclusive(&parent, source).expect("create retained file"));
        let retained = open_child_file(&parent, source).expect("open retained file");
        std::fs::rename(&source_path, &moved_path).expect("replace retained file namespace");
        std::fs::write(&source_path, b"foreign replacement").expect("create foreign replacement");

        let deletion = reopen_file_for_delete(&retained).expect("reopen retained file for delete");
        quarantine_child_entry_no_replace(&parent, source, quarantine, &deletion)
            .expect("quarantine retained file through its handle");

        assert_eq!(
            std::fs::read(&source_path).expect("read foreign replacement"),
            b"foreign replacement"
        );
        assert!(
            child_file_matches(&parent, quarantine, &retained)
                .expect("compare quarantined retained file")
        );

        remove_child_file(&parent, quarantine, &deletion)
            .expect("delete retained file through handle");
        drop(deletion);
        drop(retained);
        std::fs::remove_file(&source_path).expect("remove foreign replacement");
    }

    #[test]
    fn reopen_directory_for_delete_keeps_the_retained_directory_when_its_name_is_replaced() {
        let fixture = tempfile::tempdir().expect("create retained-directory fixture");
        let parent = open_parent(fixture.path());
        let source = OsStr::new("retained-directory");
        let quarantine = OsStr::new("quarantined-retained-directory");
        let source_path = fixture.path().join(source);
        let moved_path = fixture.path().join("moved-retained-directory");

        drop(create_child_directory(&parent, source).expect("create retained directory"));
        let retained = open_child_directory(&parent, source).expect("open retained directory");
        std::fs::rename(&source_path, &moved_path).expect("replace retained directory namespace");
        std::fs::create_dir(&source_path).expect("create foreign replacement");
        std::fs::write(source_path.join("foreign-marker"), b"keep")
            .expect("write foreign replacement marker");

        let deletion =
            reopen_directory_for_delete(&retained).expect("reopen retained directory for delete");
        quarantine_child_entry_no_replace(&parent, source, quarantine, &deletion)
            .expect("quarantine retained directory through its handle");

        assert_eq!(
            std::fs::read(source_path.join("foreign-marker")).expect("read foreign replacement"),
            b"keep"
        );
        assert!(
            child_directory_matches(&parent, quarantine, &retained)
                .expect("compare quarantined retained directory")
        );

        remove_child_directory(&parent, quarantine, &deletion)
            .expect("delete retained directory through handle");
        drop(deletion);
        drop(retained);
        std::fs::remove_dir_all(&source_path).expect("remove foreign replacement");
    }
}

#[cfg(unix)]
pub(crate) fn open_child_file(parent: &File, name: &OsStr) -> io::Result<File> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;

    let name = CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path component contains NUL"))?;
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOCTTY | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if descriptor < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: openat returned a new owned descriptor on success.
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(unix)]
pub(crate) fn open_child_file_for_exclusive_probe(parent: &File, name: &OsStr) -> io::Result<File> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;

    let name = CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path component contains NUL"))?;
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOCTTY | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if descriptor < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: openat returned a new owned descriptor on success.
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(unix)]
pub(crate) fn create_child_directory(parent: &File, name: &OsStr) -> io::Result<File> {
    use std::ffi::CString;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;

    let name = CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path component contains NUL"))?;
    // Artifact-control directories are private application state.  Use a
    // restrictive mode directly rather than relying on the host process umask
    // to remove group/other access from a permissive default.
    let created = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
    if created != 0 {
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::AlreadyExists {
            return Err(error);
        }
    }
    open_child_directory(parent, OsStr::from_bytes(name.as_bytes()))
}

#[cfg(unix)]
pub(crate) fn child_entry_exists(parent: &File, name: &OsStr) -> io::Result<bool> {
    use std::ffi::CString;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;

    let name = CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path component contains NUL"))?;
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::zeroed();
    let result = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result == 0 {
        Ok(true)
    } else {
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::NotFound {
            Ok(false)
        } else {
            Err(error)
        }
    }
}

#[cfg(unix)]
pub(crate) fn create_child_file_exclusive(parent: &File, name: &OsStr) -> io::Result<File> {
    create_child_file_exclusive_with_mode(parent, name, 0o666)
}

/// Creates an owner-only private file for short-lived security authorities.
///
/// Unlike generic replacement temporaries, these files must never be visible
/// to other local users during their brief namespace lifetime.
#[cfg(unix)]
pub(crate) fn create_private_child_file_exclusive(parent: &File, name: &OsStr) -> io::Result<File> {
    create_child_file_exclusive_with_mode(parent, name, 0o600)
}

#[cfg(unix)]
fn create_child_file_exclusive_with_mode(
    parent: &File,
    name: &OsStr,
    mode: libc::mode_t,
) -> io::Result<File> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;

    let name = CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path component contains NUL"))?;
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDWR | libc::O_CLOEXEC | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW,
            mode as libc::c_uint,
        )
    };
    if descriptor < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: openat returned a new owned descriptor on success.
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(unix)]
pub(crate) fn remove_child_file(parent: &File, name: &OsStr, _: &File) -> io::Result<()> {
    let options = fs_at::OpenOptions::default();
    options.unlink_at(parent, name)
}

#[cfg(unix)]
pub(crate) fn remove_child_directory(parent: &File, name: &OsStr, _: &File) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;

    let name = CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "directory name contains NUL"))?;
    let result = unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn quarantine_child_file_no_replace(
    parent: &File,
    source: &OsStr,
    quarantine: &OsStr,
) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;

    let source = CString::new(source.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "source name contains NUL"))?;
    let quarantine = CString::new(quarantine.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "target name contains NUL"))?;
    let descriptor = parent.as_raw_fd();
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            descriptor,
            source.as_ptr(),
            descriptor,
            quarantine.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "macos")]
pub(crate) fn quarantine_child_file_no_replace(
    parent: &File,
    source: &OsStr,
    quarantine: &OsStr,
) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;

    let source = CString::new(source.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "source name contains NUL"))?;
    let quarantine = CString::new(quarantine.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "target name contains NUL"))?;
    let descriptor = parent.as_raw_fd();
    let result = unsafe {
        libc::renameatx_np(
            descriptor,
            source.as_ptr(),
            descriptor,
            quarantine.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "android", target_os = "macos"))
))]
pub(crate) fn quarantine_child_file_no_replace(_: &File, _: &OsStr, _: &OsStr) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "no no-replace quarantine rename is available",
    ))
}

#[cfg(unix)]
pub(crate) fn quarantine_child_entry_no_replace(
    parent: &File,
    source: &OsStr,
    quarantine: &OsStr,
    _: &File,
) -> io::Result<()> {
    quarantine_child_file_no_replace(parent, source, quarantine)
}

#[cfg(unix)]
pub(crate) fn publish_child_file(
    _: &File,
    parent: &File,
    temporary_name: &OsStr,
    target_name: &OsStr,
    replace: bool,
) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;

    let temporary_name = CString::new(temporary_name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "temporary name contains NUL"))?;
    let target_name = CString::new(target_name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "target name contains NUL"))?;
    let parent_fd = parent.as_raw_fd();
    if replace {
        let result = unsafe {
            libc::renameat(
                parent_fd,
                temporary_name.as_ptr(),
                parent_fd,
                target_name.as_ptr(),
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    } else {
        // linkat is the portable POSIX no-replace publication primitive: the
        // destination appears atomically and EEXIST leaves it untouched. The
        // temporary link is removed only after publication has committed.
        let result = unsafe {
            libc::linkat(
                parent_fd,
                temporary_name.as_ptr(),
                parent_fd,
                target_name.as_ptr(),
                0,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        let _ = unsafe { libc::unlinkat(parent_fd, temporary_name.as_ptr(), 0) };
        Ok(())
    }
}

#[cfg(unix)]
pub(crate) fn preserve_replace_metadata(target: &File, temporary: &File) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    temporary.set_permissions(std::fs::Permissions::from_mode(
        target.metadata()?.permissions().mode(),
    ))
}

#[cfg(unix)]
#[allow(clippy::unnecessary_cast)] // dev_t/ino_t widths differ across Unix targets.
pub(crate) fn child_file_matches(parent: &File, name: &OsStr, file: &File) -> io::Result<bool> {
    use std::ffi::CString;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;

    let name = CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path component contains NUL"))?;
    let mut namespace_stat = std::mem::MaybeUninit::<libc::stat>::zeroed();
    let result = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            namespace_stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result != 0 {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::NotFound {
            Ok(false)
        } else {
            Err(error)
        };
    }
    let namespace_stat = unsafe { namespace_stat.assume_init() };
    let metadata = file.metadata()?;
    Ok(namespace_stat.st_dev as u64 == metadata.dev()
        && namespace_stat.st_ino as u64 == metadata.ino()
        && namespace_stat.st_mode & libc::S_IFMT == libc::S_IFREG)
}

#[cfg(unix)]
#[allow(clippy::unnecessary_cast)] // dev_t/ino_t widths differ across Unix targets.
pub(crate) fn child_directory_matches(
    parent: &File,
    name: &OsStr,
    directory: &File,
) -> io::Result<bool> {
    use std::ffi::CString;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;

    let name = CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path component contains NUL"))?;
    let mut namespace_stat = std::mem::MaybeUninit::<libc::stat>::zeroed();
    let result = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            namespace_stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result != 0 {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::NotFound {
            Ok(false)
        } else {
            Err(error)
        };
    }
    let namespace_stat = unsafe { namespace_stat.assume_init() };
    let metadata = directory.metadata()?;
    Ok(namespace_stat.st_dev as u64 == metadata.dev()
        && namespace_stat.st_ino as u64 == metadata.ino()
        && namespace_stat.st_mode & libc::S_IFMT == libc::S_IFDIR)
}

#[cfg(unix)]
pub(crate) fn child_matches_protected_metadata(_: &File, _: &File) -> io::Result<bool> {
    // Unix has no DOS 8.3 alias namespace. Literal and case-equivalent `.git`
    // components are rejected before a handle-relative open is attempted.
    Ok(false)
}

#[cfg(unix)]
pub(crate) fn read_directory_names(
    directory: &mut File,
    max_entries: usize,
) -> io::Result<Vec<OsString>> {
    let mut names = Vec::new();
    for entry in fs_at::read_dir(directory)? {
        let name = entry?.name().to_owned();
        if name == OsStr::new(".") || name == OsStr::new("..") {
            continue;
        }
        if names.len() == max_entries {
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "directory exceeds its entry limit",
            ));
        }
        names.push(name);
    }
    Ok(names)
}

#[cfg(unix)]
pub(crate) fn reopen_directory(directory: &File) -> io::Result<File> {
    open_child_directory(directory, OsStr::new("."))
}

#[cfg(unix)]
pub(crate) fn reopen_directory_for_write(directory: &File) -> io::Result<File> {
    reopen_directory(directory)
}

/// Reopens an already retained directory with the authority needed to remove
/// that exact object. The reopen is relative to the retained descriptor, not
/// to a mutable parent/name namespace entry.
#[cfg(unix)]
pub(crate) fn reopen_directory_for_delete(directory: &File) -> io::Result<File> {
    reopen_directory(directory)
}

#[cfg(unix)]
pub(crate) fn reopen_directory_for_child_directory(directory: &File) -> io::Result<File> {
    reopen_directory(directory)
}

#[cfg(unix)]
pub(crate) fn reopen_directory_path_lease(directory: &File) -> io::Result<File> {
    reopen_directory(directory)
}

#[cfg(windows)]
pub(crate) use windows::{
    child_directory_matches, child_entry_exists, child_file_matches,
    child_matches_protected_metadata, create_child_directory, create_child_directory_with_created,
    create_child_file_exclusive, open_child_directory, open_child_file,
    open_child_file_for_exclusive_probe, preserve_replace_metadata, publish_child_file,
    quarantine_child_entry_no_replace, read_directory_names, remove_child_directory,
    remove_child_file, reopen_directory, reopen_directory_for_child_directory,
    reopen_directory_for_delete, reopen_directory_for_write, reopen_directory_path_lease,
    reopen_file_for_delete, reopen_file_read_lease,
};

#[cfg(windows)]
#[path = "native_fs/windows.rs"]
mod windows;
