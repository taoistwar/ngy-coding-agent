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

pub(crate) fn open_child_file_for_exclusive_probe(parent: &File, name: &OsStr) -> io::Result<File> {
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

/// Reopens a retained file, rather than resolving its mutable namespace
/// name, with the access required to delete that exact file.
pub(crate) fn reopen_file_for_delete(file: &File) -> io::Result<File> {
    open_child(
        file,
        OsStr::new(""),
        DELETE | FILE_READ_DATA | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        FILE_NON_DIRECTORY_FILE,
        true,
        FILE_OPEN,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
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

/// Reopens a retained directory, rather than resolving its mutable
/// namespace name, with the access required to delete that exact
/// directory.
pub(crate) fn reopen_directory_for_delete(directory: &File) -> io::Result<File> {
    open_child(
        directory,
        OsStr::new(""),
        DELETE | FILE_LIST_DIRECTORY | FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
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
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path component too long"))?;
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

pub(crate) fn child_directory_matches(
    parent: &File,
    name: &OsStr,
    directory: &File,
) -> io::Result<bool> {
    let named = match open_child(
        parent,
        name,
        FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        FILE_DIRECTORY_FILE,
        false,
        FILE_OPEN,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
    ) {
        Ok(named) => named,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    Ok(file_identity(directory)? == file_identity(&named)?)
}

pub(crate) fn child_matches_protected_metadata(parent: &File, child: &File) -> io::Result<bool> {
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

pub(crate) fn quarantine_child_entry_no_replace(
    parent: &File,
    source_name: &OsStr,
    quarantine_name: &OsStr,
    entry: &File,
) -> io::Result<()> {
    publish_child_file(entry, parent, source_name, quarantine_name, false)
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

pub(crate) fn remove_child_directory(
    parent: &File,
    name: &OsStr,
    directory: &File,
) -> io::Result<()> {
    remove_child_file(parent, name, directory)
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
        let name =
            unsafe { slice::from_raw_parts(info.FileName.as_ptr(), name_bytes / size_of::<u16>()) };
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
