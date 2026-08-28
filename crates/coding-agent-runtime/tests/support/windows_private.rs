use std::io;
use std::path::Path;

pub(super) fn prepare(path: &Path) -> io::Result<()> {
    use windows_sys::Win32::Foundation::ERROR_ALREADY_EXISTS;

    validate_path(path)?;
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "process-liveness test runtime has no parent",
        )
    })?;
    std::fs::create_dir_all(parent)?;
    match create_private_directory(path) {
        Ok(()) => {}
        Err(error) if error.raw_os_error() == Some(ERROR_ALREADY_EXISTS as i32) => {}
        Err(error) => return Err(error),
    }
    harden(path)
}

pub(super) fn harden(path: &Path) -> io::Result<()> {
    set_private_directory_acl(path, false)
}

pub(super) fn add_non_owner_allow_ace(path: &Path) -> io::Result<()> {
    set_private_directory_acl(path, true)
}

fn create_private_directory(path: &Path) -> io::Result<()> {
    use std::mem::{MaybeUninit, size_of};
    use windows_sys::Win32::Security::{
        InitializeSecurityDescriptor, SE_DACL_PROTECTED, SECURITY_ATTRIBUTES, SECURITY_DESCRIPTOR,
        SetSecurityDescriptorControl, SetSecurityDescriptorDacl, SetSecurityDescriptorOwner,
    };
    use windows_sys::Win32::Storage::FileSystem::CreateDirectoryW;
    use windows_sys::Win32::System::SystemServices::SECURITY_DESCRIPTOR_REVISION;

    let path = windows_path_with_terminator(path)?;
    with_user_acl(false, |user_sid, acl| {
        let mut descriptor = MaybeUninit::<SECURITY_DESCRIPTOR>::zeroed();
        let descriptor_pointer = descriptor.as_mut_ptr().cast();
        if unsafe { InitializeSecurityDescriptor(descriptor_pointer, SECURITY_DESCRIPTOR_REVISION) }
            == 0
            || unsafe { SetSecurityDescriptorOwner(descriptor_pointer, user_sid, 0) } == 0
            || unsafe { SetSecurityDescriptorDacl(descriptor_pointer, 1, acl, 0) } == 0
            || unsafe {
                SetSecurityDescriptorControl(
                    descriptor_pointer,
                    SE_DACL_PROTECTED,
                    SE_DACL_PROTECTED,
                )
            } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let attributes = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: descriptor_pointer,
            bInheritHandle: 0,
        };
        if unsafe { CreateDirectoryW(path.as_ptr(), &attributes) } == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    })
}

fn set_private_directory_acl(path: &Path, include_local_system: bool) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle as _;
    use std::ptr::{null, null_mut};

    use windows_sys::Win32::Foundation::{ERROR_SUCCESS, HANDLE};
    use windows_sys::Win32::Security::Authorization::{SE_FILE_OBJECT, SetSecurityInfo};
    use windows_sys::Win32::Security::{
        DACL_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION,
    };

    let directory = open_plain_directory(path)?;
    with_user_acl(include_local_system, |user_sid, acl| {
        validate_current_user_owner(&directory, user_sid)?;
        let status = unsafe {
            SetSecurityInfo(
                directory.as_raw_handle() as HANDLE,
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                acl,
                null(),
            )
        };
        if status != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(status as i32));
        }
        validate_current_user_owner(&directory, user_sid)?;
        if include_local_system {
            Ok(())
        } else {
            validate_owner_only_acl(&directory, user_sid)
        }
    })
}

fn open_plain_directory(path: &Path) -> io::Result<std::fs::File> {
    use std::fs::OpenOptions;
    use std::os::windows::fs::{MetadataExt as _, OpenOptionsExt as _};
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        READ_CONTROL, WRITE_DAC,
    };

    validate_path(path)?;
    let mut options = OpenOptions::new();
    options
        .read(true)
        .access_mode(READ_CONTROL | WRITE_DAC)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT);
    let directory = options.open(path)?;
    let metadata = directory.metadata()?;
    if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "process-liveness test runtime is not a plain directory",
        ));
    }
    Ok(directory)
}

fn with_user_acl<T>(
    include_local_system: bool,
    apply: impl FnOnce(
        windows_sys::Win32::Security::PSID,
        *mut windows_sys::Win32::Security::ACL,
    ) -> io::Result<T>,
) -> io::Result<T> {
    use std::ffi::c_void;
    use std::ptr::{null, null_mut};

    use windows_sys::Win32::Foundation::{ERROR_SUCCESS, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        BuildTrusteeWithSidW, EXPLICIT_ACCESS_W, SET_ACCESS, SetEntriesInAclW, TRUSTEE_W,
    };
    use windows_sys::Win32::Security::{
        ACL, CreateWellKnownSid, SECURITY_MAX_SID_SIZE, SUB_CONTAINERS_AND_OBJECTS_INHERIT,
        TOKEN_USER, WinLocalSystemSid,
    };
    use windows_sys::Win32::Storage::FileSystem::FILE_ALL_ACCESS;

    struct LocalAcl(*mut ACL);

    impl Drop for LocalAcl {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe {
                    LocalFree(self.0.cast());
                }
            }
        }
    }

    let user_buffer = current_user_token_buffer()?;
    let user = unsafe { &*user_buffer.as_ptr().cast::<TOKEN_USER>() };
    let mut trustee = TRUSTEE_W::default();
    unsafe { BuildTrusteeWithSidW(&mut trustee, user.User.Sid) };
    let mut accesses = vec![EXPLICIT_ACCESS_W {
        grfAccessPermissions: FILE_ALL_ACCESS,
        grfAccessMode: SET_ACCESS,
        grfInheritance: SUB_CONTAINERS_AND_OBJECTS_INHERIT,
        Trustee: trustee,
    }];
    let mut local_system_sid = vec![0u8; SECURITY_MAX_SID_SIZE as usize];
    if include_local_system {
        let mut local_system_sid_size = SECURITY_MAX_SID_SIZE;
        if unsafe {
            CreateWellKnownSid(
                WinLocalSystemSid,
                null_mut(),
                local_system_sid.as_mut_ptr().cast::<c_void>(),
                &mut local_system_sid_size,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let mut local_system_trustee = TRUSTEE_W::default();
        unsafe {
            BuildTrusteeWithSidW(
                &mut local_system_trustee,
                local_system_sid.as_mut_ptr().cast::<c_void>(),
            );
        }
        accesses.push(EXPLICIT_ACCESS_W {
            grfAccessPermissions: FILE_ALL_ACCESS,
            grfAccessMode: SET_ACCESS,
            grfInheritance: SUB_CONTAINERS_AND_OBJECTS_INHERIT,
            Trustee: local_system_trustee,
        });
    }
    let mut acl = null_mut();
    let status =
        unsafe { SetEntriesInAclW(accesses.len() as u32, accesses.as_ptr(), null(), &mut acl) };
    if status != ERROR_SUCCESS || acl.is_null() {
        return Err(if status == ERROR_SUCCESS {
            invalid_private_acl()
        } else {
            io::Error::from_raw_os_error(status as i32)
        });
    }
    let acl = LocalAcl(acl);
    apply(user.User.Sid, acl.0)
}

fn current_user_token_buffer() -> io::Result<Vec<usize>> {
    use std::ffi::c_void;
    use std::mem::size_of;
    use std::ptr::null_mut;

    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::Security::{GetTokenInformation, TOKEN_QUERY, TokenUser};
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    let mut token: HANDLE = null_mut();
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let result = (|| {
        let mut required = 0u32;
        unsafe {
            GetTokenInformation(token, TokenUser, null_mut(), 0, &mut required);
        }
        if required == 0 {
            return Err(io::Error::last_os_error());
        }
        let words = (required as usize).div_ceil(size_of::<usize>());
        let mut buffer = vec![0usize; words];
        if unsafe {
            GetTokenInformation(
                token,
                TokenUser,
                buffer.as_mut_ptr().cast::<c_void>(),
                required,
                &mut required,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(buffer)
    })();
    unsafe {
        CloseHandle(token);
    }
    result
}

fn validate_current_user_owner(
    directory: &std::fs::File,
    user_sid: windows_sys::Win32::Security::PSID,
) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle as _;
    use std::ptr::null_mut;

    use windows_sys::Win32::Foundation::{ERROR_SUCCESS, HANDLE};
    use windows_sys::Win32::Security::Authorization::{GetSecurityInfo, SE_FILE_OBJECT};
    use windows_sys::Win32::Security::{EqualSid, IsValidSid, OWNER_SECURITY_INFORMATION, PSID};

    let mut owner: PSID = null_mut();
    let mut descriptor = null_mut();
    let status = unsafe {
        GetSecurityInfo(
            directory.as_raw_handle() as HANDLE,
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION,
            &mut owner,
            null_mut(),
            null_mut(),
            null_mut(),
            &mut descriptor,
        )
    };
    if status != ERROR_SUCCESS {
        return Err(io::Error::from_raw_os_error(status as i32));
    }
    let _descriptor = LocalSecurityDescriptor(descriptor.cast());
    if owner.is_null()
        || unsafe { IsValidSid(owner) } == 0
        || unsafe { EqualSid(owner, user_sid) } == 0
    {
        Err(invalid_private_acl())
    } else {
        Ok(())
    }
}

fn validate_owner_only_acl(
    directory: &std::fs::File,
    user_sid: windows_sys::Win32::Security::PSID,
) -> io::Result<()> {
    use std::ffi::c_void;
    use std::mem::{MaybeUninit, size_of};
    use std::os::windows::io::AsRawHandle as _;
    use std::ptr::null_mut;

    use windows_sys::Win32::Foundation::{ERROR_SUCCESS, HANDLE};
    use windows_sys::Win32::Security::Authorization::{GetSecurityInfo, SE_FILE_OBJECT};
    use windows_sys::Win32::Security::{
        ACCESS_ALLOWED_ACE, ACE_HEADER, ACL, ACL_SIZE_INFORMATION, AclSizeInformation,
        DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetAclInformation,
        GetSecurityDescriptorControl, INHERIT_ONLY_ACE, INHERITED_ACE, IsValidSid,
        OWNER_SECURITY_INFORMATION, PSID, SE_DACL_PROTECTED, SUB_CONTAINERS_AND_OBJECTS_INHERIT,
    };
    use windows_sys::Win32::Storage::FileSystem::FILE_ALL_ACCESS;
    use windows_sys::Win32::System::SystemServices::ACCESS_ALLOWED_ACE_TYPE;

    let mut owner: PSID = null_mut();
    let mut dacl: *mut ACL = null_mut();
    let mut descriptor = null_mut();
    let status = unsafe {
        GetSecurityInfo(
            directory.as_raw_handle() as HANDLE,
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            &mut owner,
            null_mut(),
            &mut dacl,
            null_mut(),
            &mut descriptor,
        )
    };
    if status != ERROR_SUCCESS {
        return Err(io::Error::from_raw_os_error(status as i32));
    }
    let _descriptor = LocalSecurityDescriptor(descriptor.cast());
    if owner.is_null()
        || dacl.is_null()
        || unsafe { IsValidSid(owner) } == 0
        || unsafe { EqualSid(owner, user_sid) } == 0
    {
        return Err(invalid_private_acl());
    }
    let mut control = 0u16;
    let mut revision = 0u32;
    if unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) } == 0
        || control & SE_DACL_PROTECTED == 0
    {
        return Err(invalid_private_acl());
    }
    let mut information = MaybeUninit::<ACL_SIZE_INFORMATION>::zeroed();
    if unsafe {
        GetAclInformation(
            dacl,
            information.as_mut_ptr().cast::<c_void>(),
            size_of::<ACL_SIZE_INFORMATION>() as u32,
            AclSizeInformation,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    if unsafe { information.assume_init() }.AceCount != 1 {
        return Err(invalid_private_acl());
    }
    let mut raw_ace = null_mut::<c_void>();
    if unsafe { GetAce(dacl, 0, &mut raw_ace) } == 0 || raw_ace.is_null() {
        return Err(io::Error::last_os_error());
    }
    let header = unsafe { &*raw_ace.cast::<ACE_HEADER>() };
    let flags = u32::from(header.AceFlags);
    if u32::from(header.AceType) != ACCESS_ALLOWED_ACE_TYPE
        || flags & (INHERITED_ACE | INHERIT_ONLY_ACE) != 0
        || flags & SUB_CONTAINERS_AND_OBJECTS_INHERIT != SUB_CONTAINERS_AND_OBJECTS_INHERIT
        || usize::from(header.AceSize) < size_of::<ACCESS_ALLOWED_ACE>()
    {
        return Err(invalid_private_acl());
    }
    let allowed = unsafe { &*raw_ace.cast::<ACCESS_ALLOWED_ACE>() };
    let sid = (&raw const allowed.SidStart).cast_mut().cast::<c_void>();
    if allowed.Mask != FILE_ALL_ACCESS
        || unsafe { IsValidSid(sid) } == 0
        || unsafe { EqualSid(sid, user_sid) } == 0
    {
        return Err(invalid_private_acl());
    }
    Ok(())
}

struct LocalSecurityDescriptor(*mut std::ffi::c_void);

impl Drop for LocalSecurityDescriptor {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                windows_sys::Win32::Foundation::LocalFree(self.0);
            }
        }
    }
}

fn validate_path(path: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt as _;

    if path.as_os_str().encode_wide().any(|unit| unit == 0) {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "process-liveness test runtime path contains NUL",
        ))
    } else {
        Ok(())
    }
}

fn windows_path_with_terminator(path: &Path) -> io::Result<Vec<u16>> {
    use std::os::windows::ffi::OsStrExt as _;

    validate_path(path)?;
    Ok(path.as_os_str().encode_wide().chain(Some(0)).collect())
}

fn invalid_private_acl() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "process-liveness test runtime ACL is not restricted to the current owner",
    )
}
