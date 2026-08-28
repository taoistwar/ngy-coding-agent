use std::fs::File;
use std::io;
use std::path::Path;

pub(super) fn open_private_file_exclusive(path: &Path) -> io::Result<File> {
    use std::os::windows::io::FromRawHandle as _;
    use std::ptr::null_mut;

    use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
    use windows_sys::Win32::Storage::FileSystem::{
        CREATE_NEW, CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    let encoded = encode_windows_path(path)?;
    with_private_security_attributes(false, |attributes| {
        let handle = unsafe {
            CreateFileW(
                encoded.as_ptr(),
                super::windows_private_file_access_mode(),
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                attributes,
                CREATE_NEW,
                FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT,
                null_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            Err(io::Error::last_os_error())
        } else {
            // SAFETY: CreateFileW returned a new owned handle on success.
            Ok(unsafe { File::from_raw_handle(handle) })
        }
    })
}

pub(super) fn ensure_private_directory_exists(path: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => validate_directory_path_metadata(&metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let parent = path.parent().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "private directory has no parent",
                )
            })?;
            std::fs::create_dir_all(parent)?;
            if !create_private_directory(path)? {
                let metadata = std::fs::symlink_metadata(path)?;
                validate_directory_path_metadata(&metadata)?;
            }
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn validate_directory_path_metadata(metadata: &std::fs::Metadata) -> io::Result<()> {
    if super::metadata_is_link_or_reparse_point(metadata) {
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "private directory path is a link or reparse point",
        ))
    } else if metadata.is_dir() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "private directory path is not a directory",
        ))
    }
}

fn create_private_directory(path: &Path) -> io::Result<bool> {
    use windows_sys::Win32::Storage::FileSystem::CreateDirectoryW;

    let encoded = encode_windows_path(path)?;
    with_private_security_attributes(true, |attributes| {
        if unsafe { CreateDirectoryW(encoded.as_ptr(), attributes) } != 0 {
            Ok(true)
        } else {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::AlreadyExists {
                Ok(false)
            } else {
                Err(error)
            }
        }
    })
}

pub(super) fn encode_windows_path(path: &Path) -> io::Result<Vec<u16>> {
    use std::os::windows::ffi::OsStrExt as _;

    let mut encoded = path.as_os_str().encode_wide().collect::<Vec<_>>();
    if encoded.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "private path contains an embedded NUL",
        ));
    }
    encoded.push(0);
    Ok(encoded)
}

fn with_private_security_attributes<T>(
    directory: bool,
    create: impl FnOnce(*const windows_sys::Win32::Security::SECURITY_ATTRIBUTES) -> io::Result<T>,
) -> io::Result<T> {
    use std::mem::size_of;

    use windows_sys::Win32::Security::{
        InitializeSecurityDescriptor, SE_DACL_PROTECTED, SECURITY_ATTRIBUTES, SECURITY_DESCRIPTOR,
        SetSecurityDescriptorControl, SetSecurityDescriptorDacl, SetSecurityDescriptorOwner,
    };
    use windows_sys::Win32::System::SystemServices::SECURITY_DESCRIPTOR_REVISION;

    super::with_windows_user_acl(directory, |acl, user_sid| {
        let mut descriptor = SECURITY_DESCRIPTOR::default();
        let descriptor_ptr = (&mut descriptor as *mut SECURITY_DESCRIPTOR).cast();
        if unsafe { InitializeSecurityDescriptor(descriptor_ptr, SECURITY_DESCRIPTOR_REVISION) }
            == 0
            || unsafe { SetSecurityDescriptorOwner(descriptor_ptr, user_sid, 0) } == 0
            || unsafe { SetSecurityDescriptorDacl(descriptor_ptr, 1, acl, 0) } == 0
            || unsafe {
                SetSecurityDescriptorControl(descriptor_ptr, SE_DACL_PROTECTED, SE_DACL_PROTECTED)
            } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let attributes = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: descriptor_ptr,
            bInheritHandle: 0,
        };
        create(&attributes)
    })
}
