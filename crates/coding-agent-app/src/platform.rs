use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

#[cfg(windows)]
#[path = "platform/windows_private_creation.rs"]
mod windows_private_creation;
#[cfg(all(test, windows))]
use windows_private_creation::encode_windows_path;
#[cfg(windows)]
use windows_private_creation::{ensure_private_directory_exists, open_private_file_exclusive};

pub trait WallClock: Send + Sync + 'static {
    fn now_utc(&self) -> time::OffsetDateTime;
}

#[derive(Debug, Default)]
pub struct SystemWallClock;

impl WallClock for SystemWallClock {
    fn now_utc(&self) -> time::OffsetDateTime {
        time::OffsetDateTime::now_utc()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformPaths {
    pub data_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub runtime_config: PathBuf,
    pub database_path: PathBuf,
    pub instance_lock: PathBuf,
    pub instance_descriptor: PathBuf,
    pub unclean_shutdown: PathBuf,
}

impl PlatformPaths {
    pub fn discover() -> io::Result<Self> {
        let project =
            directories::ProjectDirs::from("com", "ngy", "coding-agent").ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "project directories unavailable")
            })?;
        let data_dir = project.data_local_dir().to_path_buf();
        let runtime_dir = project
            .runtime_dir()
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| data_dir.join("run"));
        Ok(Self::new(data_dir, runtime_dir))
    }

    pub fn new(data_dir: impl Into<PathBuf>, runtime_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        let runtime_dir = runtime_dir.into();
        Self {
            runtime_config: data_dir.join("runtime.json"),
            database_path: data_dir.join("coding-agent.sqlite3"),
            instance_lock: runtime_dir.join("instance.lock"),
            instance_descriptor: runtime_dir.join("instance.json"),
            unclean_shutdown: data_dir.join("unclean-shutdown.json"),
            data_dir,
            runtime_dir,
        }
    }

    pub fn prepare(&self) -> io::Result<()> {
        self.prepare_data_directory()?;
        self.prepare_runtime_directory()
    }

    pub(crate) fn prepare_data_directory(&self) -> io::Result<()> {
        create_private_directory(&self.data_dir)
    }

    pub(crate) fn prepare_runtime_directory(&self) -> io::Result<()> {
        create_private_directory(&self.runtime_dir)
    }

    /// Reauthenticates the runtime directory and returns the exact descriptor
    /// that was hardened. Callers which need a capability must hand this
    /// descriptor forward rather than opening `runtime_dir` by name again.
    pub(crate) fn retain_private_runtime_directory(&self) -> io::Result<File> {
        prepare_private_directory(&self.runtime_dir)
    }
}

#[derive(Debug)]
pub struct PrivateFile(File);

impl PrivateFile {
    pub fn create_new(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::create_new_with_after_open(path, || Ok(()))
    }

    fn create_new_with_after_open(
        path: impl AsRef<Path>,
        after_open: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<Self> {
        let path = path.as_ref();
        let file = open_private_file_exclusive(path)?;
        if let Err(error) = after_open().and_then(|()| make_private_new_file(&file)) {
            drop(file);
            // The path may have been replaced after the exclusive open. Never
            // unlink by name here: that could delete an attacker's replacement.
            // Retaining the exact opened object is the fail-closed outcome for
            // namespace integrity; callers must not consume it after this error.
            return Err(error);
        }
        Ok(Self(file))
    }

    pub fn as_file(&self) -> &File {
        &self.0
    }

    pub fn into_file(self) -> File {
        self.0
    }
}

impl Read for PrivateFile {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.0.read(buffer)
    }
}

impl Write for PrivateFile {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl Seek for PrivateFile {
    fn seek(&mut self, position: io::SeekFrom) -> io::Result<u64> {
        self.0.seek(position)
    }
}

#[cfg(unix)]
fn open_private_file_exclusive(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt as _;

    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true).mode(0o600);
    options.open(path)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PrivateFileReadError {
    #[error("private file could not be opened")]
    Open(#[source] io::Error),
    #[error("private file metadata could not be validated")]
    Metadata(#[source] io::Error),
    #[error("private file permissions are invalid")]
    NotPrivate(#[source] io::Error),
    #[error("private file exceeds its byte limit")]
    TooLarge,
    #[error("private file could not be read")]
    Read(#[source] io::Error),
}

pub(crate) fn read_private_file_bounded(
    path: &Path,
    max_bytes: usize,
) -> Result<Vec<u8>, PrivateFileReadError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;

        options.custom_flags(private_read_custom_flags());
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;

        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let file = options.open(path).map_err(PrivateFileReadError::Open)?;
    validate_private_file(&file).map_err(PrivateFileReadError::NotPrivate)?;
    let metadata = file.metadata().map_err(PrivateFileReadError::Metadata)?;
    let max_bytes_u64 = u64::try_from(max_bytes).unwrap_or(u64::MAX);
    if metadata.len() > max_bytes_u64 {
        return Err(PrivateFileReadError::TooLarge);
    }

    let read_limit = max_bytes_u64.saturating_add(1);
    let mut encoded = Vec::with_capacity(
        usize::try_from(metadata.len())
            .unwrap_or(max_bytes)
            .min(max_bytes),
    );
    file.take(read_limit)
        .read_to_end(&mut encoded)
        .map_err(PrivateFileReadError::Read)?;
    if encoded.len() > max_bytes {
        return Err(PrivateFileReadError::TooLarge);
    }
    Ok(encoded)
}

#[cfg(unix)]
const fn private_read_custom_flags() -> i32 {
    libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK
}

pub(crate) fn validate_private_file(file: &File) -> io::Result<()> {
    validate_private_file_with(file, validate_private_file_permissions)
}

pub(crate) fn validate_private_file_snapshot(file: &File) -> io::Result<()> {
    #[cfg(unix)]
    {
        validate_private_file_with(file, validate_private_file_snapshot_permissions)
    }
    #[cfg(windows)]
    {
        validate_private_file(file)
    }
}

fn validate_private_file_with(
    file: &File,
    validate_permissions: fn(&File) -> io::Result<()>,
) -> io::Result<()> {
    validate_private_file_kind(file)?;
    validate_permissions(file)
}

fn validate_private_file_kind(file: &File) -> io::Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "private path is not a regular file",
        ));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;

        if !windows_attributes_are_non_reparse(metadata.file_attributes()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "private path is a reparse point",
            ));
        }
    }
    Ok(())
}

pub(crate) fn harden_private_file(file: &File) -> io::Result<()> {
    validate_private_file_kind(file)?;
    #[cfg(windows)]
    validate_windows_current_user_owner(file)?;
    make_private_file(file)?;
    validate_private_file(file)
}

#[derive(Clone, PartialEq, Eq, thiserror::Error)]
#[error("failed to open the browser")]
pub struct BrowserLaunchError {
    url: String,
}

impl std::fmt::Debug for BrowserLaunchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BrowserLaunchError")
            .field("url", &"<redacted>")
            .finish()
    }
}

impl BrowserLaunchError {
    pub fn for_url(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }

    pub fn url(&self) -> &str {
        &self.url
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BrowserLauncher;

impl BrowserLauncher {
    pub fn url(port: u16, token: &str) -> String {
        format!("http://127.0.0.1:{port}/#token={token}")
    }

    pub fn open(port: u16, token: &str) -> Result<(), BrowserLaunchError> {
        Self::open_with(port, token, webbrowser::open)
    }

    fn open_with<E>(
        port: u16,
        token: &str,
        opener: impl FnOnce(&str) -> Result<(), E>,
    ) -> Result<(), BrowserLaunchError> {
        let url = Self::url(port, token);
        if port == 0 || token.is_empty() {
            return Err(BrowserLaunchError { url });
        }
        let open_result = {
            let _spawn_guard = coding_agent_runtime::acquire_process_spawn_lock();
            opener(&url)
        };
        if open_result.is_err() {
            return Err(BrowserLaunchError { url });
        }
        Ok(())
    }
}

pub(crate) fn create_private_directory(path: &Path) -> io::Result<()> {
    drop(prepare_private_directory(path)?);
    Ok(())
}

fn prepare_private_directory(path: &Path) -> io::Result<File> {
    ensure_private_directory_exists(path)?;
    let directory = open_private_directory(path)?;
    harden_private_directory(&directory)?;
    Ok(directory)
}

#[cfg(unix)]
fn ensure_private_directory_exists(path: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata_is_link_or_reparse_point(&metadata) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "private directory path is a link or reparse point",
            ));
        }
        Ok(metadata) if !metadata.is_dir() => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "private directory path is not a directory",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            std::fs::create_dir_all(path)?;
        }
        Err(error) => return Err(error),
    }
    Ok(())
}

#[cfg(unix)]
fn open_private_directory(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW);
    options.open(path)
}

#[cfg(windows)]
fn open_private_directory(path: &Path) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;

    let mut options = OpenOptions::new();
    options
        .read(true)
        .access_mode(windows_private_directory_access_mode())
        .custom_flags(windows_private_directory_open_flags());
    options.open(path)
}

fn harden_private_directory(directory: &File) -> io::Result<()> {
    validate_private_directory_handle(directory)?;
    harden_private_directory_permissions(directory)?;
    validate_private_directory_permissions(directory)
}

fn validate_private_directory_handle(directory: &File) -> io::Result<()> {
    let metadata = directory.metadata()?;
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "private directory handle is not a directory",
        ));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;

        if !windows_attributes_are_non_reparse(metadata.file_attributes()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "private directory handle is a reparse point",
            ));
        }
    }
    Ok(())
}

fn metadata_is_link_or_reparse_point(metadata: &std::fs::Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;

        !windows_attributes_are_non_reparse(metadata.file_attributes())
    }
    #[cfg(not(windows))]
    {
        false
    }
}

#[cfg(windows)]
fn windows_attributes_are_non_reparse(attributes: u32) -> bool {
    use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

    attributes & FILE_ATTRIBUTE_REPARSE_POINT == 0
}

#[cfg(windows)]
pub(crate) fn windows_private_file_access_mode() -> u32 {
    use windows_sys::Win32::Foundation::{GENERIC_READ, GENERIC_WRITE};
    use windows_sys::Win32::Storage::FileSystem::WRITE_DAC;

    GENERIC_READ | GENERIC_WRITE | WRITE_DAC
}

#[cfg(windows)]
fn windows_private_directory_access_mode() -> u32 {
    use windows_sys::Win32::Storage::FileSystem::{READ_CONTROL, WRITE_DAC};

    READ_CONTROL | WRITE_DAC
}

#[cfg(windows)]
fn windows_private_directory_open_flags() -> u32 {
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
    };

    FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT
}

#[cfg(unix)]
fn harden_private_directory_permissions(directory: &File) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    validate_private_unix_directory_owner(directory)?;
    directory.set_permissions(std::fs::Permissions::from_mode(0o700))?;
    #[cfg(target_os = "macos")]
    crate::macos_acl::clear_extended_acl(directory)?;
    Ok(())
}

#[cfg(unix)]
fn validate_private_directory_permissions(directory: &File) -> io::Result<()> {
    validate_private_unix_directory_owner(directory)?;

    use std::os::unix::fs::PermissionsExt;

    if directory.metadata()?.permissions().mode() & 0o7777 != 0o700 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "private directory permissions are not owner-only",
        ));
    }
    #[cfg(target_os = "macos")]
    crate::macos_acl::validate_no_extended_acl(directory)?;
    Ok(())
}

#[cfg(unix)]
fn validate_private_unix_directory_owner(directory: &File) -> io::Result<()> {
    validate_private_unix_directory_owner_as(directory, unsafe { libc::geteuid() })
}

#[cfg(unix)]
fn validate_private_unix_directory_owner_as(
    directory: &File,
    expected_owner: u32,
) -> io::Result<()> {
    use std::os::unix::fs::MetadataExt;

    let metadata = directory.metadata()?;
    if !metadata.is_dir() || metadata.uid() != expected_owner {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "private directory is not owned by the current user",
        ));
    }
    Ok(())
}

#[cfg(all(unix, not(target_os = "macos")))]
fn make_private_file(file: &File) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    file.set_permissions(std::fs::Permissions::from_mode(0o600))
}

#[cfg(unix)]
fn make_private_new_file(file: &File) -> io::Result<()> {
    validate_private_file_kind(file)?;
    make_private_file(file)?;
    validate_private_file(file)
}

#[cfg(target_os = "macos")]
fn make_private_file(file: &File) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    crate::macos_acl::clear_extended_acl(file)
}

#[cfg(unix)]
fn validate_private_file_permissions(file: &File) -> io::Result<()> {
    validate_private_unix_file_permissions(file, PrivateFileLinkPolicy::ExactlyOne)
}

#[cfg(unix)]
fn validate_private_file_snapshot_permissions(file: &File) -> io::Result<()> {
    validate_private_unix_file_permissions(file, PrivateFileLinkPolicy::Snapshot)
}

#[cfg(unix)]
#[derive(Clone, Copy)]
enum PrivateFileLinkPolicy {
    ExactlyOne,
    Snapshot,
}

#[cfg(unix)]
fn validate_private_unix_file_permissions(
    file: &File,
    link_policy: PrivateFileLinkPolicy,
) -> io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let metadata = file.metadata()?;
    let link_count_is_valid = match link_policy {
        PrivateFileLinkPolicy::ExactlyOne => metadata.nlink() == 1,
        PrivateFileLinkPolicy::Snapshot => metadata.nlink() <= 1,
    };
    if metadata.permissions().mode() & 0o7777 != 0o600
        || !link_count_is_valid
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "private file permissions are not owner-only read/write",
        ));
    }

    #[cfg(target_os = "macos")]
    {
        crate::macos_acl::validate_no_extended_acl(file)?;
    }
    Ok(())
}

#[cfg(windows)]
fn harden_private_directory_permissions(directory: &File) -> io::Result<()> {
    validate_windows_current_user_owner(directory)?;
    make_private_windows_handle(directory, true)
}

#[cfg(windows)]
fn validate_private_directory_permissions(directory: &File) -> io::Result<()> {
    validate_private_file_permissions(directory)
}

#[cfg(windows)]
fn make_private_file(file: &File) -> io::Result<()> {
    make_private_windows_handle(file, false)
}

#[cfg(windows)]
fn make_private_new_file(file: &File) -> io::Result<()> {
    validate_private_file(file)
}

#[cfg(windows)]
fn make_private_windows_handle(file: &File, directory: bool) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use std::ptr::{null, null_mut};

    use windows_sys::Win32::Security::Authorization::{SE_FILE_OBJECT, SetSecurityInfo};
    use windows_sys::Win32::Security::{
        DACL_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION,
    };

    with_windows_user_acl(directory, |acl, _user_sid| {
        let status = unsafe {
            SetSecurityInfo(
                file.as_raw_handle(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                acl,
                null(),
            )
        };
        if status == windows_sys::Win32::Foundation::ERROR_SUCCESS {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(status as i32))
        }
    })
}

#[cfg(windows)]
fn validate_windows_current_user_owner(file: &File) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use std::ptr::null_mut;

    use windows_sys::Win32::Foundation::{ERROR_SUCCESS, LocalFree};
    use windows_sys::Win32::Security::Authorization::{GetSecurityInfo, SE_FILE_OBJECT};
    use windows_sys::Win32::Security::{EqualSid, OWNER_SECURITY_INFORMATION, TOKEN_USER};

    let mut owner = null_mut();
    let mut descriptor = null_mut();
    let status = unsafe {
        GetSecurityInfo(
            file.as_raw_handle(),
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

    let validation = (|| {
        if owner.is_null() {
            return Err(invalid_private_permissions());
        }
        let user_buffer = current_user_token_buffer()?;
        let user = unsafe { &*user_buffer.as_ptr().cast::<TOKEN_USER>() };
        if unsafe { EqualSid(owner, user.User.Sid) } == 0 {
            return Err(invalid_private_permissions());
        }
        Ok(())
    })();

    unsafe { LocalFree(descriptor) };
    validation
}

#[cfg(windows)]
fn validate_private_file_permissions(file: &File) -> io::Result<()> {
    use std::mem::size_of;
    use std::os::windows::io::AsRawHandle;
    use std::ptr::null_mut;

    use windows_sys::Win32::Foundation::{ERROR_SUCCESS, LocalFree};
    use windows_sys::Win32::Security::Authorization::{GetSecurityInfo, SE_FILE_OBJECT};
    use windows_sys::Win32::Security::{
        ACCESS_ALLOWED_ACE, ACL, ACL_SIZE_INFORMATION, AclSizeInformation,
        DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetAclInformation,
        GetSecurityDescriptorControl, OWNER_SECURITY_INFORMATION, SE_DACL_PROTECTED,
    };

    let mut owner = null_mut();
    let mut dacl: *mut ACL = null_mut();
    let mut descriptor = null_mut();
    let status = unsafe {
        GetSecurityInfo(
            file.as_raw_handle(),
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

    let validation = (|| {
        if owner.is_null() || dacl.is_null() {
            return Err(invalid_private_permissions());
        }

        let mut control = 0u16;
        let mut revision = 0u32;
        if unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) } == 0
            || control & SE_DACL_PROTECTED == 0
        {
            return Err(invalid_private_permissions());
        }

        let user_buffer = current_user_token_buffer()?;
        let user = unsafe {
            &*user_buffer
                .as_ptr()
                .cast::<windows_sys::Win32::Security::TOKEN_USER>()
        };
        if unsafe { EqualSid(owner, user.User.Sid) } == 0 {
            return Err(invalid_private_permissions());
        }

        let mut acl_info = ACL_SIZE_INFORMATION::default();
        if unsafe {
            GetAclInformation(
                dacl,
                (&mut acl_info as *mut ACL_SIZE_INFORMATION).cast(),
                size_of::<ACL_SIZE_INFORMATION>() as u32,
                AclSizeInformation,
            )
        } == 0
            || acl_info.AceCount != 1
        {
            return Err(invalid_private_permissions());
        }

        let mut raw_ace = null_mut();
        if unsafe { GetAce(dacl, 0, &mut raw_ace) } == 0 || raw_ace.is_null() {
            return Err(invalid_private_permissions());
        }
        let ace = unsafe { &*raw_ace.cast::<ACCESS_ALLOWED_ACE>() };
        if ace.Header.AceType as u32
            != windows_sys::Win32::System::SystemServices::ACCESS_ALLOWED_ACE_TYPE
            || ace.Header.AceFlags as u32 & windows_sys::Win32::Security::INHERITED_ACE != 0
        {
            return Err(invalid_private_permissions());
        }
        let ace_sid = std::ptr::addr_of!(ace.SidStart).cast_mut().cast();
        if unsafe { EqualSid(ace_sid, user.User.Sid) } == 0 {
            return Err(invalid_private_permissions());
        }
        Ok(())
    })();

    unsafe { LocalFree(descriptor) };
    validation
}

#[cfg(windows)]
fn invalid_private_permissions() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "private file ACL is not restricted to the current owner",
    )
}

#[cfg(windows)]
fn with_windows_user_acl<T>(
    directory: bool,
    apply: impl FnOnce(
        *mut windows_sys::Win32::Security::ACL,
        windows_sys::Win32::Security::PSID,
    ) -> io::Result<T>,
) -> io::Result<T> {
    use std::ptr::{null, null_mut};

    use windows_sys::Win32::Foundation::{ERROR_SUCCESS, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        BuildTrusteeWithSidW, EXPLICIT_ACCESS_W, SET_ACCESS, SetEntriesInAclW, TRUSTEE_W,
    };
    use windows_sys::Win32::Security::{
        ACL, NO_INHERITANCE, SUB_CONTAINERS_AND_OBJECTS_INHERIT, TOKEN_USER,
    };
    use windows_sys::Win32::Storage::FileSystem::FILE_ALL_ACCESS;

    let buffer = current_user_token_buffer()?;

    let user = unsafe { &*buffer.as_ptr().cast::<TOKEN_USER>() };
    let mut trustee = TRUSTEE_W::default();
    unsafe { BuildTrusteeWithSidW(&mut trustee, user.User.Sid) };
    let access = EXPLICIT_ACCESS_W {
        grfAccessPermissions: FILE_ALL_ACCESS,
        grfAccessMode: SET_ACCESS,
        grfInheritance: if directory {
            SUB_CONTAINERS_AND_OBJECTS_INHERIT
        } else {
            NO_INHERITANCE
        },
        Trustee: trustee,
    };
    let mut acl: *mut ACL = null_mut();
    let acl_status = unsafe { SetEntriesInAclW(1, &access, null(), &mut acl) };
    if acl_status != ERROR_SUCCESS {
        return Err(io::Error::from_raw_os_error(acl_status as i32));
    }

    let result = apply(acl, user.User.Sid);
    unsafe { LocalFree(acl.cast()) };
    result
}

#[cfg(windows)]
fn current_user_token_buffer() -> io::Result<Vec<usize>> {
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
                buffer.as_mut_ptr().cast(),
                required,
                &mut required,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(buffer)
    })();
    unsafe { CloseHandle(token) };
    result
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[cfg(unix)]
    #[test]
    fn private_read_flags_are_nonblocking_cloexec_and_nofollow() {
        let flags = private_read_custom_flags();

        assert_ne!(flags & libc::O_NONBLOCK, 0);
        assert_ne!(flags & libc::O_CLOEXEC, 0);
        assert_ne!(flags & libc::O_NOFOLLOW, 0);
    }

    #[test]
    fn browser_backend_failure_returns_the_exact_url_that_was_delegated() {
        let mut delegated = None;
        let error = BrowserLauncher::open_with(42_123, "launch-token", |url| {
            delegated = Some(url.to_owned());
            Err(())
        })
        .expect_err("injected browser failure is observable");

        assert_eq!(
            delegated.as_deref(),
            Some("http://127.0.0.1:42123/#token=launch-token")
        );
        assert_eq!(error.url(), delegated.unwrap());
        assert!(!format!("{error:?}").contains("launch-token"));
        assert!(!error.to_string().contains("launch-token"));
    }

    #[test]
    fn private_file_permissions_stay_bound_to_the_opened_file_during_a_path_swap() {
        let temp = tempfile::tempdir().expect("create handle-bound permission fixture");
        let path = temp.path().join("instance.json");
        let opened_path = temp.path().join("opened-instance.json");
        let victim = temp.path().join("victim.json");
        std::fs::write(&victim, b"victim contents").expect("create permission victim");
        make_test_file_broad(&victim);
        let victim_permissions = permission_fingerprint(&victim);

        let mut private = PrivateFile::create_new_with_after_open(&path, || {
            std::fs::rename(&path, &opened_path).map_err(|error| {
                io::Error::new(error.kind(), format!("rename opened file: {error}"))
            })?;
            create_test_file_symlink(&victim, &path).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("install replacement symlink: {error}"),
                )
            })
        })
        .expect("create private file while its name is replaced");

        assert_eq!(
            permission_fingerprint(&victim),
            victim_permissions,
            "permission hardening must not follow the replacement path"
        );
        assert_ne!(
            permission_fingerprint(&opened_path),
            victim_permissions,
            "the opened file itself must be hardened"
        );
        private
            .write_all(b"opened file contents")
            .expect("write through the original opened handle");
        validate_private_file(private.as_file())
            .expect("the opened private file has an owner-only ACL");
        drop(private);
        assert_eq!(
            std::fs::read(&opened_path).unwrap(),
            b"opened file contents"
        );
        assert_eq!(std::fs::read(&victim).unwrap(), b"victim contents");
    }

    #[cfg(unix)]
    #[test]
    fn private_directory_permissions_stay_bound_to_the_opened_directory_during_a_path_swap() {
        let temp = tempfile::tempdir().expect("create directory permission fixture");
        let path = temp.path().join("runtime");
        let opened_path = temp.path().join("opened-runtime");
        let victim = temp.path().join("victim");
        std::fs::create_dir(&path).expect("create private directory");
        std::fs::create_dir(&victim).expect("create directory permission victim");
        make_test_directory_broad(&victim);
        let victim_permissions = permission_fingerprint(&victim);

        let directory = open_private_directory(&path).expect("open original private directory");
        std::fs::rename(&path, &opened_path).expect("rename opened directory");
        std::os::unix::fs::symlink(&victim, &path).expect("install replacement directory link");

        harden_private_directory(&directory)
            .expect("harden through the original opened directory handle");

        assert_eq!(
            permission_fingerprint(&victim),
            victim_permissions,
            "directory hardening must not follow the replacement path"
        );
        assert_eq!(
            permission_fingerprint(&opened_path),
            0o700,
            "the opened directory itself must be hardened"
        );
    }

    #[cfg(unix)]
    #[test]
    fn existing_current_user_directory_is_hardened_before_use() {
        let temp = tempfile::tempdir().expect("create existing directory fixture");
        let path = temp.path().join("runtime");
        std::fs::create_dir(&path).expect("create existing runtime directory");
        make_test_directory_broad(&path);

        create_private_directory(&path).expect("harden current-user runtime directory");

        assert_eq!(permission_fingerprint(&path), 0o700);
    }

    #[test]
    fn retained_runtime_authority_requires_the_expected_namespace_identity() {
        let temporary = tempfile::tempdir().expect("create retained runtime fixture");
        let root = temporary
            .path()
            .canonicalize()
            .expect("canonicalize retained runtime fixture");
        let paths = PlatformPaths::new(root.join("data"), root.join("runtime"));
        let retained = paths
            .retain_private_runtime_directory()
            .expect("prepare and retain private runtime directory");
        let replacement = root.join("replacement-runtime");
        std::fs::create_dir(&replacement).expect("create replacement runtime directory");

        assert!(matches!(
            coding_agent_runtime::ExecutionDirectory::from_retained_directory(
                &replacement,
                retained,
            ),
            Err(coding_agent_runtime::CommandPolicyError::IdentityChanged)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn retained_runtime_authority_rejects_a_namespace_replacement() {
        let temporary = tempfile::tempdir().expect("create retained runtime fixture");
        let root = temporary
            .path()
            .canonicalize()
            .expect("canonicalize retained runtime fixture");
        let paths = PlatformPaths::new(root.join("data"), root.join("runtime"));
        let retained = paths
            .retain_private_runtime_directory()
            .expect("prepare and retain private runtime directory");
        let held = paths.runtime_dir.with_extension("held");
        let replacement = paths.runtime_dir.with_extension("replacement");
        std::fs::create_dir(&replacement).expect("create current-user replacement directory");
        std::fs::rename(&paths.runtime_dir, &held).expect("move retained runtime directory");
        std::fs::rename(&replacement, &paths.runtime_dir)
            .expect("install replacement runtime directory");

        assert!(matches!(
            coding_agent_runtime::ExecutionDirectory::from_retained_directory(
                &paths.runtime_dir,
                retained,
            ),
            Err(coding_agent_runtime::CommandPolicyError::IdentityChanged)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn foreign_directory_owner_is_rejected_before_permissions_change() {
        use std::os::unix::fs::MetadataExt;

        let temp = tempfile::tempdir().expect("create foreign-owner directory fixture");
        let path = temp.path().join("runtime");
        std::fs::create_dir(&path).expect("create candidate runtime directory");
        make_test_directory_broad(&path);
        let original_permissions = permission_fingerprint(&path);
        let directory = open_private_directory(&path).expect("open candidate runtime directory");
        let actual_owner = directory.metadata().expect("read owner metadata").uid();
        let foreign_owner = if actual_owner == 0 { 1 } else { 0 };

        let error = validate_private_unix_directory_owner_as(&directory, foreign_owner)
            .expect_err("foreign directory owner must be rejected");

        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(
            permission_fingerprint(&path),
            original_permissions,
            "owner validation must occur before chmod"
        );
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_validation_accepts_an_open_inode_unlinked_by_atomic_replacement() {
        use std::os::unix::fs::MetadataExt as _;

        let temp = tempfile::tempdir().expect("create descriptor replacement fixture");
        let path = temp.path().join("instance.json");
        let replacement_path = temp.path().join("instance.json.next");
        let old_contents = b"complete old descriptor";
        let new_contents = b"complete new descriptor";

        let mut opened = PrivateFile::create_new(&path).expect("create old descriptor");
        opened
            .write_all(old_contents)
            .expect("write old descriptor");
        opened.flush().expect("flush old descriptor");

        let mut replacement =
            PrivateFile::create_new(&replacement_path).expect("create replacement descriptor");
        replacement
            .write_all(new_contents)
            .expect("write replacement descriptor");
        replacement.flush().expect("flush replacement descriptor");
        drop(replacement);

        std::fs::rename(&replacement_path, &path).expect("atomically replace descriptor");
        assert_eq!(
            opened.as_file().metadata().unwrap().nlink(),
            0,
            "the still-open old descriptor no longer has a directory entry"
        );
        assert!(
            validate_private_file(opened.as_file()).is_err(),
            "permanent private files must remain linked exactly once"
        );
        validate_private_file_snapshot(opened.as_file())
            .expect("an already-open unlinked descriptor snapshot remains private");

        opened
            .seek(io::SeekFrom::Start(0))
            .expect("rewind old descriptor");
        let mut observed = Vec::new();
        opened
            .read_to_end(&mut observed)
            .expect("read old descriptor snapshot");
        assert_eq!(observed, old_contents);
        assert_eq!(std::fs::read(&path).unwrap(), new_contents);
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_validation_rejects_a_file_with_multiple_hard_links() {
        use std::os::unix::fs::MetadataExt as _;

        let temp = tempfile::tempdir().expect("create hard-link fixture");
        let path = temp.path().join("instance.json");
        let alias = temp.path().join("instance-alias.json");
        let opened = PrivateFile::create_new(&path).expect("create private descriptor");
        std::fs::hard_link(&path, &alias).expect("create descriptor hard link");

        assert_eq!(opened.as_file().metadata().unwrap().nlink(), 2);
        assert!(validate_private_file(opened.as_file()).is_err());
        assert!(validate_private_file_snapshot(opened.as_file()).is_err());
    }

    #[cfg(windows)]
    #[test]
    fn windows_private_paths_reject_every_reparse_point_attribute() {
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_REPARSE_POINT,
        };

        assert!(windows_attributes_are_non_reparse(FILE_ATTRIBUTE_NORMAL));
        assert!(windows_attributes_are_non_reparse(FILE_ATTRIBUTE_DIRECTORY));
        assert!(!windows_attributes_are_non_reparse(
            FILE_ATTRIBUTE_NORMAL | FILE_ATTRIBUTE_REPARSE_POINT
        ));
        assert!(!windows_attributes_are_non_reparse(
            FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT
        ));
    }

    #[cfg(windows)]
    #[test]
    fn windows_private_path_encoding_rejects_embedded_nul() {
        use std::ffi::OsString;
        use std::os::windows::ffi::OsStringExt as _;

        let path = PathBuf::from(OsString::from_wide(&[
            b'p' as u16,
            b'r' as u16,
            b'e' as u16,
            0,
            b'f' as u16,
            b'i' as u16,
            b'x' as u16,
        ]));

        assert_eq!(
            encode_windows_path(&path).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_private_paths_never_request_owner_mutation_after_open() {
        use windows_sys::Win32::Storage::FileSystem::{WRITE_DAC, WRITE_OWNER};

        assert_eq!(
            windows_private_file_access_mode() & WRITE_DAC,
            WRITE_DAC,
            "private file hardening must retain DACL authority"
        );
        assert_eq!(
            windows_private_file_access_mode() & WRITE_OWNER,
            0,
            "existing private files must reject foreign ownership rather than replace it"
        );
        assert_eq!(
            windows_private_directory_access_mode() & WRITE_OWNER,
            0,
            "private directory hardening must reject foreign ownership rather than replace it"
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_existing_private_file_never_takes_over_a_foreign_default_owner() {
        use std::os::windows::fs::OpenOptionsExt as _;

        let temp = tempfile::tempdir().expect("create existing-owner fixture");
        let path = temp.path().join("existing-private-file");
        std::fs::write(&path, b"existing").expect("create file with the token's default owner");
        let observed = File::open(&path).expect("open existing owner for observation");
        let owner_is_current = validate_windows_current_user_owner(&observed).is_ok();

        let mut options = OpenOptions::new();
        options
            .read(true)
            .write(true)
            .access_mode(windows_private_file_access_mode());
        match options.open(&path) {
            Ok(file) if owner_is_current => {
                harden_private_file(&file).expect("harden a current-user-owned existing file");
            }
            Ok(file) => {
                assert_eq!(
                    harden_private_file(&file).unwrap_err().kind(),
                    io::ErrorKind::PermissionDenied
                );
                assert!(
                    validate_windows_current_user_owner(&observed).is_err(),
                    "hardening must not replace a foreign default owner"
                );
            }
            Err(error) if !owner_is_current => {
                assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
                assert!(validate_windows_current_user_owner(&observed).is_err());
            }
            Err(error) => panic!("open current-user-owned existing file: {error}"),
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_private_directory_open_is_handle_bound_and_reparse_aware() {
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        };

        let flags = windows_private_directory_open_flags();
        assert_eq!(
            flags & FILE_FLAG_BACKUP_SEMANTICS,
            FILE_FLAG_BACKUP_SEMANTICS
        );
        assert_eq!(
            flags & FILE_FLAG_OPEN_REPARSE_POINT,
            FILE_FLAG_OPEN_REPARSE_POINT
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_hardening_removes_an_inherited_extended_acl() {
        use std::process::{Command, Stdio};

        let temp = tempfile::tempdir().expect("create inherited ACL fixture");
        let parent = temp.path().join("parent");
        std::fs::create_dir(&parent).expect("create ACL parent");
        let mut username_command = Command::new("id");
        username_command
            .arg("-un")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let username_child = {
            let _spawn_guard = coding_agent_runtime::acquire_process_spawn_lock();
            username_command
                .spawn()
                .expect("spawn current macOS username query")
        };
        let username = username_child
            .wait_with_output()
            .expect("read current macOS username");
        assert!(username.status.success());
        let username = String::from_utf8(username.stdout)
            .expect("username is UTF-8")
            .trim()
            .to_owned();
        let inherited_ace = format!("{username} allow read,file_inherit");
        let mut chmod_command = Command::new("chmod");
        chmod_command
            .args(["+a", inherited_ace.as_str()])
            .arg(&parent);
        let mut chmod_child = {
            let _spawn_guard = coding_agent_runtime::acquire_process_spawn_lock();
            chmod_command
                .spawn()
                .expect("spawn inheritable macOS ACL installer")
        };
        let status = chmod_child.wait().expect("install inheritable macOS ACL");
        assert!(status.success());

        let path = parent.join("instance.json");
        std::fs::write(&path, b"descriptor").expect("create inherited-ACL file");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open inherited-ACL file");
        assert!(validate_private_file(&file).is_err());
        harden_private_file(&file).expect("clear inherited ACL while hardening");
        validate_private_file(&file).expect("hardened file has no extended ACL");
    }

    #[cfg(unix)]
    fn make_test_file_broad(path: &Path) {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o666))
            .expect("make Unix permission victim broad");
    }

    #[cfg(unix)]
    fn make_test_directory_broad(path: &Path) {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
            .expect("make Unix directory permission victim broad");
    }

    #[cfg(unix)]
    fn permission_fingerprint(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;

        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[cfg(unix)]
    fn create_test_file_symlink(target: &Path, link: &Path) -> io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn make_test_file_broad(path: &Path) {
        use std::os::windows::ffi::OsStrExt;
        use std::ptr::null_mut;

        use windows_sys::Win32::Foundation::ERROR_SUCCESS;
        use windows_sys::Win32::Security::Authorization::{SE_FILE_OBJECT, SetNamedSecurityInfoW};
        use windows_sys::Win32::Security::DACL_SECURITY_INFORMATION;

        let mut wide = path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let status = unsafe {
            SetNamedSecurityInfoW(
                wide.as_mut_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                null_mut(),
                null_mut(),
            )
        };
        assert_eq!(
            status, ERROR_SUCCESS,
            "give the permission victim a null DACL"
        );
    }

    #[cfg(windows)]
    fn permission_fingerprint(path: &Path) -> String {
        use std::os::windows::ffi::OsStrExt;
        use std::ptr::{null_mut, slice_from_raw_parts};

        use windows_sys::Win32::Foundation::{ERROR_SUCCESS, LocalFree};
        use windows_sys::Win32::Security::Authorization::{
            ConvertSecurityDescriptorToStringSecurityDescriptorW, GetNamedSecurityInfoW,
            SE_FILE_OBJECT,
        };
        use windows_sys::Win32::Security::{DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR};

        let wide = path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let mut descriptor: PSECURITY_DESCRIPTOR = null_mut();
        let status = unsafe {
            GetNamedSecurityInfoW(
                wide.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                null_mut(),
                null_mut(),
                &mut descriptor,
            )
        };
        assert_eq!(status, ERROR_SUCCESS, "read permission victim DACL");

        let mut text = null_mut();
        let mut length = 0u32;
        assert_ne!(
            unsafe {
                ConvertSecurityDescriptorToStringSecurityDescriptorW(
                    descriptor,
                    1,
                    DACL_SECURITY_INFORMATION,
                    &mut text,
                    &mut length,
                )
            },
            0,
            "convert DACL to a stable fingerprint"
        );
        let fingerprint =
            String::from_utf16_lossy(unsafe { &*slice_from_raw_parts(text, length as usize) });
        unsafe {
            LocalFree(text.cast());
            LocalFree(descriptor.cast());
        }
        fingerprint
    }

    #[cfg(windows)]
    fn create_test_file_symlink(target: &Path, link: &Path) -> io::Result<()> {
        std::os::windows::fs::symlink_file(target, link)
    }
}
