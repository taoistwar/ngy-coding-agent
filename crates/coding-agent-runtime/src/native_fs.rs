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
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: openat returned a new owned descriptor on success.
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(all(test, windows))]
mod tests {
    use std::ffi::OsStr;
    use std::fs::OpenOptions;
    use std::os::windows::fs::{MetadataExt as _, OpenOptionsExt as _};

    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ADD_FILE, FILE_ATTRIBUTE_REPARSE_POINT, FILE_DELETE_CHILD, FILE_FLAG_BACKUP_SEMANTICS,
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_LIST_DIRECTORY, FILE_READ_ATTRIBUTES, FILE_TRAVERSE,
        SYNCHRONIZE,
    };

    use super::{child_file_matches, create_child_file_exclusive, open_child_file};

    #[test]
    fn child_file_match_compares_the_namespace_entry_to_the_open_handle() {
        let fixture = tempfile::tempdir().expect("create child-identity fixture");
        let mut options = OpenOptions::new();
        options
            .read(true)
            .access_mode(
                FILE_ADD_FILE
                    | FILE_DELETE_CHILD
                    | FILE_LIST_DIRECTORY
                    | FILE_TRAVERSE
                    | FILE_READ_ATTRIBUTES
                    | SYNCHRONIZE,
            )
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT);
        let parent = options
            .open(fixture.path())
            .expect("open child-identity parent");
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
            0o666,
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
        if names.len() == max_entries {
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "directory exceeds its entry limit",
            ));
        }
        names.push(entry?.name().to_owned());
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
    child_entry_exists, child_file_matches, child_matches_protected_metadata,
    create_child_directory, create_child_directory_with_created, create_child_file_exclusive,
    open_child_directory, open_child_file, open_child_file_for_exclusive_probe,
    preserve_replace_metadata, publish_child_file, read_directory_names, remove_child_file,
    reopen_directory, reopen_directory_for_child_directory, reopen_directory_for_write,
    reopen_directory_path_lease, reopen_file_read_lease,
};

#[cfg(windows)]
mod windows {
    use std::ffi::{OsStr, OsString, c_void};
    use std::fs::File;
    use std::io;
    use std::mem::{MaybeUninit, size_of};
    use std::os::windows::ffi::{OsStrExt, OsStringExt};
    use std::os::windows::io::{AsRawHandle, FromRawHandle};
    use std::ptr::{null, null_mut};
    use std::slice;

    use windows_sys::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows_sys::Wdk::Storage::FileSystem::{
        FILE_CREATE, FILE_DIRECTORY_FILE, FILE_DISPOSITION_DELETE,
        FILE_DISPOSITION_IGNORE_READONLY_ATTRIBUTE, FILE_DISPOSITION_INFORMATION,
        FILE_DISPOSITION_INFORMATION_EX, FILE_DISPOSITION_POSIX_SEMANTICS, FILE_NON_DIRECTORY_FILE,
        FILE_OPEN, FILE_OPEN_REPARSE_POINT, FILE_RENAME_IGNORE_READONLY_ATTRIBUTE,
        FILE_RENAME_INFORMATION, FILE_RENAME_INFORMATION_0, FILE_RENAME_REPLACE_IF_EXISTS,
        FILE_SYNCHRONOUS_IO_NONALERT, FileDispositionInformation, FileDispositionInformationEx,
        FileRenameInformationEx, NtCreateFile, NtSetInformationFile,
    };
    use windows_sys::Win32::Foundation::{
        ERROR_NO_MORE_FILES, HANDLE, OBJ_CASE_INSENSITIVE, RtlNtStatusToDosError, UNICODE_STRING,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, DELETE, FILE_ADD_FILE, FILE_ADD_SUBDIRECTORY,
        FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_READONLY, FILE_BASIC_INFO, FILE_DELETE_CHILD,
        FILE_ID_BOTH_DIR_INFO, FILE_LIST_DIRECTORY, FILE_READ_ATTRIBUTES, FILE_READ_DATA,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_TRAVERSE, FILE_WRITE_ATTRIBUTES,
        FILE_WRITE_DATA, FileBasicInfo, FileIdBothDirectoryInfo, FileIdBothDirectoryRestartInfo,
        GetFileInformationByHandle, GetFileInformationByHandleEx, READ_CONTROL, SYNCHRONIZE,
        SetFileInformationByHandle, WRITE_DAC, WRITE_OWNER,
    };
    use windows_sys::Win32::System::IO::IO_STATUS_BLOCK;

    pub(crate) fn open_child_directory(parent: &File, name: &OsStr) -> io::Result<File> {
        open_child(
            parent,
            name,
            FILE_LIST_DIRECTORY | FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            FILE_DIRECTORY_FILE,
            false,
            FILE_OPEN,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        )
    }

    pub(crate) fn open_child_file(parent: &File, name: &OsStr) -> io::Result<File> {
        open_child(
            parent,
            name,
            FILE_READ_DATA | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            FILE_NON_DIRECTORY_FILE,
            false,
            FILE_OPEN,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        )
    }

    pub(crate) fn open_child_file_for_exclusive_probe(
        parent: &File,
        name: &OsStr,
    ) -> io::Result<File> {
        open_child(
            parent,
            name,
            DELETE | FILE_READ_DATA | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            FILE_NON_DIRECTORY_FILE,
            false,
            FILE_OPEN,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        )
    }

    pub(crate) fn create_child_directory(parent: &File, name: &OsStr) -> io::Result<File> {
        let access = FILE_ADD_SUBDIRECTORY
            | FILE_LIST_DIRECTORY
            | FILE_TRAVERSE
            | FILE_READ_ATTRIBUTES
            | READ_CONTROL
            | SYNCHRONIZE;
        match open_child(
            parent,
            name,
            access,
            FILE_DIRECTORY_FILE,
            false,
            FILE_CREATE,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        ) {
            Ok(directory) => Ok(directory),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => open_child(
                parent,
                name,
                access,
                FILE_DIRECTORY_FILE,
                false,
                FILE_OPEN,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            ),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn create_child_directory_with_created(
        parent: &File,
        name: &OsStr,
    ) -> io::Result<(File, bool)> {
        let access = FILE_ADD_SUBDIRECTORY
            | FILE_LIST_DIRECTORY
            | FILE_TRAVERSE
            | FILE_READ_ATTRIBUTES
            | READ_CONTROL
            | WRITE_DAC
            | WRITE_OWNER
            | SYNCHRONIZE;
        match open_child(
            parent,
            name,
            access,
            FILE_DIRECTORY_FILE,
            false,
            FILE_CREATE,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        ) {
            Ok(directory) => Ok((directory, true)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => open_child(
                parent,
                name,
                FILE_ADD_SUBDIRECTORY
                    | FILE_LIST_DIRECTORY
                    | FILE_TRAVERSE
                    | FILE_READ_ATTRIBUTES
                    | READ_CONTROL
                    | SYNCHRONIZE,
                FILE_DIRECTORY_FILE,
                false,
                FILE_OPEN,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            )
            .map(|directory| (directory, false)),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn child_entry_exists(parent: &File, name: &OsStr) -> io::Result<bool> {
        match open_child(
            parent,
            name,
            FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            0,
            false,
            FILE_OPEN,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        ) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Reopens an already validated executable without permitting writers or
    /// namespace replacement while the returned lease remains alive.
    pub(crate) fn reopen_file_read_lease(file: &File) -> io::Result<File> {
        open_child(
            file,
            OsStr::new(""),
            FILE_READ_DATA | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            FILE_NON_DIRECTORY_FILE,
            true,
            FILE_OPEN,
            FILE_SHARE_READ,
        )
    }

    pub(crate) fn create_child_file_exclusive(parent: &File, name: &OsStr) -> io::Result<File> {
        open_child(
            parent,
            name,
            DELETE | FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES | FILE_WRITE_DATA | SYNCHRONIZE,
            FILE_NON_DIRECTORY_FILE,
            false,
            FILE_CREATE,
            0,
        )
    }

    pub(crate) fn reopen_directory(directory: &File) -> io::Result<File> {
        open_child(
            directory,
            OsStr::new(""),
            FILE_LIST_DIRECTORY | FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            FILE_DIRECTORY_FILE,
            true,
            FILE_OPEN,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        )
    }

    pub(crate) fn reopen_directory_for_write(directory: &File) -> io::Result<File> {
        open_child(
            directory,
            OsStr::new(""),
            FILE_ADD_FILE
                | FILE_DELETE_CHILD
                | FILE_LIST_DIRECTORY
                | FILE_TRAVERSE
                | FILE_READ_ATTRIBUTES
                | SYNCHRONIZE,
            FILE_DIRECTORY_FILE,
            true,
            FILE_OPEN,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        )
    }

    pub(crate) fn reopen_directory_for_child_directory(directory: &File) -> io::Result<File> {
        open_child(
            directory,
            OsStr::new(""),
            FILE_ADD_SUBDIRECTORY
                | FILE_LIST_DIRECTORY
                | FILE_TRAVERSE
                | FILE_READ_ATTRIBUTES
                | SYNCHRONIZE,
            FILE_DIRECTORY_FILE,
            true,
            FILE_OPEN,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        )
    }

    pub(crate) fn reopen_directory_path_lease(directory: &File) -> io::Result<File> {
        open_child(
            directory,
            OsStr::new(""),
            FILE_LIST_DIRECTORY | FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            FILE_DIRECTORY_FILE,
            true,
            FILE_OPEN,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
        )
    }

    fn open_child(
        parent: &File,
        name: &OsStr,
        desired_access: u32,
        kind: u32,
        allow_empty: bool,
        disposition: u32,
        share_access: u32,
    ) -> io::Result<File> {
        let mut name = name.encode_wide().collect::<Vec<_>>();
        if (!allow_empty && name.is_empty()) || name.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "path component is empty or contains NUL",
            ));
        }
        let byte_length = name
            .len()
            .checked_mul(size_of::<u16>())
            .and_then(|length| u16::try_from(length).ok())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "path component too long")
            })?;
        let unicode = UNICODE_STRING {
            Length: byte_length,
            MaximumLength: byte_length,
            Buffer: if name.is_empty() {
                null_mut()
            } else {
                name.as_mut_ptr()
            },
        };
        let attributes = OBJECT_ATTRIBUTES {
            Length: size_of::<OBJECT_ATTRIBUTES>() as u32,
            RootDirectory: parent.as_raw_handle() as HANDLE,
            ObjectName: &unicode,
            Attributes: OBJ_CASE_INSENSITIVE,
            SecurityDescriptor: null(),
            SecurityQualityOfService: null(),
        };
        let mut handle: HANDLE = null_mut();
        let mut status = IO_STATUS_BLOCK::default();
        let ntstatus = unsafe {
            NtCreateFile(
                &mut handle,
                desired_access,
                &attributes,
                &mut status,
                null(),
                FILE_ATTRIBUTE_NORMAL,
                share_access,
                disposition,
                kind | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
                null(),
                0,
            )
        };
        if ntstatus < 0 {
            let code = unsafe { RtlNtStatusToDosError(ntstatus) };
            return Err(io::Error::from_raw_os_error(code as i32));
        }
        if handle.is_null() {
            return Err(io::Error::other(
                "NtCreateFile succeeded without returning a handle",
            ));
        }

        // SAFETY: NtCreateFile returned a new owned handle. Establish ownership
        // before any validation can fail so every later error path closes it.
        Ok(unsafe { File::from_raw_handle(handle) })
    }

    pub(crate) fn preserve_replace_metadata(target: &File, temporary: &File) -> io::Result<()> {
        let target_info = basic_info(target)?;
        let attributes = if target_info.FileAttributes & FILE_ATTRIBUTE_READONLY != 0 {
            FILE_ATTRIBUTE_READONLY
        } else {
            FILE_ATTRIBUTE_NORMAL
        };
        set_basic_attributes(temporary, attributes)
    }

    pub(crate) fn child_file_matches(parent: &File, name: &OsStr, file: &File) -> io::Result<bool> {
        let named = match open_child(
            parent,
            name,
            FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            FILE_NON_DIRECTORY_FILE,
            false,
            FILE_OPEN,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        ) {
            Ok(named) => named,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        Ok(file_identity(file)? == file_identity(&named)?)
    }

    pub(crate) fn child_matches_protected_metadata(
        parent: &File,
        child: &File,
    ) -> io::Result<bool> {
        // A DOS 8.3 name is a second namespace name for the same object. Open
        // the protected long name relative to the same authenticated parent
        // and compare stable file identities after the requested child has
        // already been opened without following reparse points.
        let protected = match open_child(
            parent,
            OsStr::new(".git"),
            FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            0,
            false,
            FILE_OPEN,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        ) {
            Ok(protected) => protected,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        Ok(file_identity(child)? == file_identity(&protected)?)
    }

    fn file_identity(file: &File) -> io::Result<(u64, u64)> {
        let mut information = MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
        if unsafe {
            GetFileInformationByHandle(file.as_raw_handle() as HANDLE, information.as_mut_ptr())
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let information = unsafe { information.assume_init() };
        Ok((
            u64::from(information.dwVolumeSerialNumber),
            (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow),
        ))
    }

    fn basic_info(file: &File) -> io::Result<FILE_BASIC_INFO> {
        let mut info = FILE_BASIC_INFO::default();
        let succeeded = unsafe {
            GetFileInformationByHandleEx(
                file.as_raw_handle() as HANDLE,
                FileBasicInfo,
                (&mut info as *mut FILE_BASIC_INFO).cast::<c_void>(),
                size_of::<FILE_BASIC_INFO>() as u32,
            )
        };
        if succeeded == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(info)
        }
    }

    fn set_basic_attributes(file: &File, attributes: u32) -> io::Result<()> {
        let info = FILE_BASIC_INFO {
            FileAttributes: attributes,
            ..FILE_BASIC_INFO::default()
        };
        let succeeded = unsafe {
            SetFileInformationByHandle(
                file.as_raw_handle() as HANDLE,
                FileBasicInfo,
                (&info as *const FILE_BASIC_INFO).cast::<c_void>(),
                size_of::<FILE_BASIC_INFO>() as u32,
            )
        };
        if succeeded == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub(crate) fn publish_child_file(
        temporary: &File,
        parent: &File,
        _: &OsStr,
        target_name: &OsStr,
        replace: bool,
    ) -> io::Result<()> {
        let mut target_name = target_name.encode_wide().collect::<Vec<_>>();
        if target_name.is_empty() || target_name.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "target name is empty or contains NUL",
            ));
        }
        let name_bytes = target_name
            .len()
            .checked_mul(size_of::<u16>())
            .and_then(|length| u32::try_from(length).ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "target name too long"))?;
        let structure_bytes = size_of::<FILE_RENAME_INFORMATION>()
            .checked_add(name_bytes as usize)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "target name too long"))?;
        let words = structure_bytes.div_ceil(size_of::<usize>());
        let mut storage = vec![0usize; words];
        let information = storage.as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();
        unsafe {
            (*information).Anonymous = FILE_RENAME_INFORMATION_0 {
                Flags: if replace {
                    FILE_RENAME_REPLACE_IF_EXISTS | FILE_RENAME_IGNORE_READONLY_ATTRIBUTE
                } else {
                    0
                },
            };
            (*information).RootDirectory = parent.as_raw_handle() as HANDLE;
            (*information).FileNameLength = name_bytes;
            std::ptr::copy_nonoverlapping(
                target_name.as_mut_ptr(),
                (*information).FileName.as_mut_ptr(),
                target_name.len(),
            );
        }
        let mut status = IO_STATUS_BLOCK::default();
        let ntstatus = unsafe {
            NtSetInformationFile(
                temporary.as_raw_handle() as HANDLE,
                &mut status,
                information.cast::<c_void>(),
                structure_bytes as u32,
                FileRenameInformationEx,
            )
        };
        ntstatus_result(ntstatus)
    }

    pub(crate) fn remove_child_file(_: &File, _: &OsStr, temporary: &File) -> io::Result<()> {
        let mut status = IO_STATUS_BLOCK::default();
        let information = FILE_DISPOSITION_INFORMATION_EX {
            Flags: FILE_DISPOSITION_DELETE
                | FILE_DISPOSITION_POSIX_SEMANTICS
                | FILE_DISPOSITION_IGNORE_READONLY_ATTRIBUTE,
        };
        let ntstatus = unsafe {
            NtSetInformationFile(
                temporary.as_raw_handle() as HANDLE,
                &mut status,
                (&information as *const FILE_DISPOSITION_INFORMATION_EX).cast::<c_void>(),
                size_of::<FILE_DISPOSITION_INFORMATION_EX>() as u32,
                FileDispositionInformationEx,
            )
        };
        if ntstatus >= 0 {
            return Ok(());
        }

        // The extended disposition class is unavailable on older systems.
        // Clear the only metadata bit we preserve, then use the legacy class.
        set_basic_attributes(temporary, FILE_ATTRIBUTE_NORMAL)?;
        let information = FILE_DISPOSITION_INFORMATION { DeleteFile: true };
        let ntstatus = unsafe {
            NtSetInformationFile(
                temporary.as_raw_handle() as HANDLE,
                &mut status,
                (&information as *const FILE_DISPOSITION_INFORMATION).cast::<c_void>(),
                size_of::<FILE_DISPOSITION_INFORMATION>() as u32,
                FileDispositionInformation,
            )
        };
        ntstatus_result(ntstatus)
    }

    fn ntstatus_result(ntstatus: i32) -> io::Result<()> {
        if ntstatus >= 0 {
            Ok(())
        } else {
            let code = unsafe { RtlNtStatusToDosError(ntstatus) };
            Err(io::Error::from_raw_os_error(code as i32))
        }
    }

    pub(crate) fn read_directory_names(
        directory: &mut File,
        max_entries: usize,
    ) -> io::Result<Vec<OsString>> {
        let mut names = Vec::new();
        let mut restart = true;
        loop {
            // u64 storage guarantees the alignment required by
            // FILE_ID_BOTH_DIR_INFO.
            let mut buffer = vec![0u64; 512];
            let class = if restart {
                FileIdBothDirectoryRestartInfo
            } else {
                FileIdBothDirectoryInfo
            };
            let succeeded = unsafe {
                GetFileInformationByHandleEx(
                    directory.as_raw_handle() as HANDLE,
                    class,
                    buffer.as_mut_ptr().cast::<c_void>(),
                    u32::try_from(buffer.len() * size_of::<u64>()).unwrap(),
                )
            };
            if succeeded == 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(ERROR_NO_MORE_FILES as i32) {
                    break;
                }
                return Err(error);
            }
            restart = false;
            parse_directory_buffer(&buffer, &mut names, max_entries)?;
        }
        Ok(names)
    }

    fn parse_directory_buffer(
        buffer: &[u64],
        names: &mut Vec<OsString>,
        max_entries: usize,
    ) -> io::Result<()> {
        let bytes = std::mem::size_of_val(buffer);
        let mut offset = 0usize;
        loop {
            if bytes.saturating_sub(offset) < size_of::<FILE_ID_BOTH_DIR_INFO>() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "directory information buffer is truncated",
                ));
            }
            // SAFETY: buffer is u64-aligned, offset is validated below, and the
            // fixed structure fits in the remaining byte range.
            let info = unsafe {
                &*buffer
                    .as_ptr()
                    .cast::<u8>()
                    .add(offset)
                    .cast::<FILE_ID_BOTH_DIR_INFO>()
            };
            let name_bytes = info.FileNameLength as usize;
            if !name_bytes.is_multiple_of(size_of::<u16>()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "directory entry name has an odd byte length",
                ));
            }
            let prefix = std::mem::offset_of!(FILE_ID_BOTH_DIR_INFO, FileName);
            if prefix
                .checked_add(name_bytes)
                .is_none_or(|end| end > bytes - offset)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "directory entry name exceeds its buffer",
                ));
            }
            // SAFETY: the byte length and enclosing buffer were validated.
            let name = unsafe {
                slice::from_raw_parts(info.FileName.as_ptr(), name_bytes / size_of::<u16>())
            };
            // NtQueryDirectoryFile includes the two kernel navigation aliases,
            // unlike Unix read_dir. They are not child namespace entries and
            // must never consume protocol limits or reach callers.
            if name != [u16::from(b'.')] && name != [u16::from(b'.'), u16::from(b'.')] {
                if names.len() == max_entries {
                    return Err(io::Error::new(
                        io::ErrorKind::FileTooLarge,
                        "directory exceeds its entry limit",
                    ));
                }
                names.push(OsString::from_wide(name));
            }

            if info.NextEntryOffset == 0 {
                break;
            }
            let next = info.NextEntryOffset as usize;
            if next < prefix || offset.checked_add(next).is_none_or(|next| next >= bytes) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "directory entry offset is invalid",
                ));
            }
            offset += next;
        }
        Ok(())
    }
}
