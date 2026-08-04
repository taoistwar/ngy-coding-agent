#![allow(dead_code)]

use std::io;
use std::path::Path;
use std::process::{Command, ExitStatus, Output, Stdio};

use coding_agent_runtime::{ProcessLivenessDirectory, ProcessLivenessScope};

pub fn instance_process_scope(runtime_directory: &Path) -> ProcessLivenessScope {
    let liveness_runtime = private_liveness_runtime(runtime_directory);
    let mut instance_id = [0x15; 16];
    instance_id[6] = 0x45;
    instance_id[8] = 0x95;
    ProcessLivenessDirectory::open(&liveness_runtime)
        .expect("open process-liveness test directory")
        .instance_scope(instance_id)
        .expect("create process-liveness test instance scope")
}

pub fn private_liveness_runtime(runtime_directory: &Path) -> std::path::PathBuf {
    let liveness_runtime = runtime_directory.join(".process-liveness-test-runtime");
    std::fs::create_dir_all(&liveness_runtime)
        .expect("create private process-liveness test runtime");
    harden_private_directory(&liveness_runtime)
        .expect("harden private process-liveness test runtime");
    liveness_runtime
}

pub fn task_process_scope(runtime_directory: &Path) -> ProcessLivenessScope {
    let mut task_id = [0x25; 16];
    task_id[6] = 0x45;
    task_id[8] = 0xa5;
    instance_process_scope(runtime_directory)
        .task_scope(task_id)
        .expect("create process-liveness test task scope")
}

#[cfg(unix)]
fn harden_private_directory(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
}

#[cfg(windows)]
fn harden_private_directory(path: &Path) -> io::Result<()> {
    set_private_directory_acl(path, false)
}

#[cfg(windows)]
pub fn add_non_owner_allow_ace(path: &Path) -> io::Result<()> {
    set_private_directory_acl(path, true)
}

#[cfg(windows)]
fn set_private_directory_acl(path: &Path, include_local_system: bool) -> io::Result<()> {
    use std::ffi::c_void;
    use std::fs::OpenOptions;
    use std::mem::size_of;
    use std::os::windows::fs::{MetadataExt as _, OpenOptionsExt as _};
    use std::os::windows::io::AsRawHandle as _;
    use std::ptr::{null, null_mut};

    use windows_sys::Win32::Foundation::{CloseHandle, ERROR_SUCCESS, HANDLE, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        BuildTrusteeWithSidW, EXPLICIT_ACCESS_W, SE_FILE_OBJECT, SET_ACCESS, SetEntriesInAclW,
        SetSecurityInfo, TRUSTEE_W,
    };
    use windows_sys::Win32::Security::{
        CreateWellKnownSid, DACL_SECURITY_INFORMATION, GetTokenInformation,
        PROTECTED_DACL_SECURITY_INFORMATION, SECURITY_MAX_SID_SIZE,
        SUB_CONTAINERS_AND_OBJECTS_INHERIT, TOKEN_QUERY, TOKEN_USER, TokenUser, WinLocalSystemSid,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ALL_ACCESS, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS,
        FILE_FLAG_OPEN_REPARSE_POINT, READ_CONTROL, WRITE_DAC,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

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
        let user = unsafe { &*buffer.as_ptr().cast::<TOKEN_USER>() };
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
        let acl_status =
            unsafe { SetEntriesInAclW(accesses.len() as u32, accesses.as_ptr(), null(), &mut acl) };
        if acl_status != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(acl_status as i32));
        }
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
        unsafe {
            LocalFree(acl.cast());
        }
        if status == ERROR_SUCCESS {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(status as i32))
        }
    })();
    unsafe {
        CloseHandle(token);
    }
    result
}

pub fn command_output(command: &mut Command) -> io::Result<Output> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = {
        let _spawn_guard = coding_agent_runtime::acquire_process_spawn_lock();
        command.spawn()?
    };
    child.wait_with_output()
}

pub fn command_status(command: &mut Command) -> io::Result<ExitStatus> {
    let mut child = {
        let _spawn_guard = coding_agent_runtime::acquire_process_spawn_lock();
        command.spawn()?
    };
    child.wait()
}
