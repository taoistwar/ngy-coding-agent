use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

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
            database_path: data_dir.join("coding-agent.sqlite3"),
            instance_lock: runtime_dir.join("instance.lock"),
            instance_descriptor: runtime_dir.join("instance.json"),
            unclean_shutdown: data_dir.join("unclean-shutdown.json"),
            data_dir,
            runtime_dir,
        }
    }

    pub fn prepare(&self) -> io::Result<()> {
        create_private_directory(&self.data_dir)?;
        create_private_directory(&self.runtime_dir)
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
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;

            options.access_mode(windows_private_file_access_mode());
        }
        let file = options.open(path)?;
        if let Err(error) = after_open().and_then(|()| make_private_file(&file)) {
            drop(file);
            let _ = std::fs::remove_file(path);
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

pub(crate) fn validate_private_file(file: &File) -> io::Result<()> {
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
    validate_private_file_permissions(file)
}

pub(crate) fn harden_private_file(file: &File) -> io::Result<()> {
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
        if port == 0 || token.is_empty() || opener(&url).is_err() {
            return Err(BrowserLaunchError { url });
        }
        Ok(())
    }
}

pub(crate) fn create_private_directory(path: &Path) -> io::Result<()> {
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
    make_private(path, true)
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
    use windows_sys::Win32::Storage::FileSystem::{WRITE_DAC, WRITE_OWNER};

    GENERIC_READ | GENERIC_WRITE | WRITE_DAC | WRITE_OWNER
}

#[cfg(windows)]
fn windows_private_directory_access_mode() -> u32 {
    use windows_sys::Win32::Storage::FileSystem::{READ_CONTROL, WRITE_DAC, WRITE_OWNER};

    READ_CONTROL | WRITE_DAC | WRITE_OWNER
}

#[cfg(windows)]
fn windows_private_directory_open_flags() -> u32 {
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
    };

    FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT
}

#[cfg(all(unix, not(target_os = "macos")))]
fn make_private(path: &Path, directory: bool) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mode = if directory { 0o700 } else { 0o600 };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

#[cfg(target_os = "macos")]
fn make_private(path: &Path, directory: bool) -> io::Result<()> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    if !directory {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path-based private hardening is restricted to directories",
        ));
    }
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW);
    let directory = options.open(path)?;
    let metadata = directory.metadata()?;
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "private directory handle is not a directory",
        ));
    }
    directory.set_permissions(std::fs::Permissions::from_mode(0o700))?;
    crate::macos_acl::clear_extended_acl(&directory)?;
    validate_private_macos_directory(&directory)
}

#[cfg(all(unix, not(target_os = "macos")))]
fn make_private_file(file: &File) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    file.set_permissions(std::fs::Permissions::from_mode(0o600))
}

#[cfg(target_os = "macos")]
fn make_private_file(file: &File) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    crate::macos_acl::clear_extended_acl(file)
}

#[cfg(all(unix, not(target_os = "macos")))]
fn validate_private_file_permissions(file: &File) -> io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let metadata = file.metadata()?;
    if metadata.permissions().mode() & 0o7777 == 0o600
        && metadata.nlink() == 1
        && metadata.uid() == unsafe { libc::geteuid() }
    {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "private file permissions are not owner-only read/write",
        ))
    }
}

#[cfg(target_os = "macos")]
fn validate_private_file_permissions(file: &File) -> io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let metadata = file.metadata()?;
    if metadata.permissions().mode() & 0o7777 != 0o600
        || metadata.nlink() != 1
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "private file permissions are not owner-only read/write",
        ));
    }
    crate::macos_acl::validate_no_extended_acl(file)
}

#[cfg(target_os = "macos")]
fn validate_private_macos_directory(directory: &File) -> io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let metadata = directory.metadata()?;
    if !metadata.is_dir()
        || metadata.permissions().mode() & 0o7777 != 0o700
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "private directory permissions are not owner-only",
        ));
    }
    crate::macos_acl::validate_no_extended_acl(directory)
}

#[cfg(windows)]
fn make_private(path: &Path, directory: bool) -> io::Result<()> {
    use std::os::windows::fs::{MetadataExt, OpenOptionsExt};

    if !directory {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path-based private hardening is restricted to directories",
        ));
    }
    let mut options = OpenOptions::new();
    options
        .read(true)
        .access_mode(windows_private_directory_access_mode())
        .custom_flags(windows_private_directory_open_flags());
    let directory = options.open(path)?;
    let metadata = directory.metadata()?;
    if !metadata.is_dir() || !windows_attributes_are_non_reparse(metadata.file_attributes()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "private directory handle is not a plain directory",
        ));
    }
    make_private_windows_handle(&directory, true)?;
    validate_private_file_permissions(&directory)
}

#[cfg(windows)]
fn make_private_file(file: &File) -> io::Result<()> {
    make_private_windows_handle(file, false)
}

#[cfg(windows)]
fn make_private_windows_handle(file: &File, directory: bool) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use std::ptr::{null, null_mut};

    use windows_sys::Win32::Security::Authorization::{SE_FILE_OBJECT, SetSecurityInfo};
    use windows_sys::Win32::Security::{
        DACL_SECURITY_INFORMATION, OWNER_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION,
    };

    with_windows_user_owner_and_acl(directory, |owner, acl| unsafe {
        SetSecurityInfo(
            file.as_raw_handle(),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION
                | DACL_SECURITY_INFORMATION
                | PROTECTED_DACL_SECURITY_INFORMATION,
            owner,
            null_mut(),
            acl,
            null(),
        )
    })
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
fn with_windows_user_owner_and_acl(
    directory: bool,
    apply: impl FnOnce(
        windows_sys::Win32::Security::PSID,
        *mut windows_sys::Win32::Security::ACL,
    ) -> u32,
) -> io::Result<()> {
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

    let status = apply(user.User.Sid, acl);
    unsafe { LocalFree(acl.cast()) };
    if status == ERROR_SUCCESS {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(status as i32))
    }
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
        drop(private);
        assert_eq!(
            std::fs::read(&opened_path).unwrap(),
            b"opened file contents"
        );
        assert_eq!(std::fs::read(&victim).unwrap(), b"victim contents");
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
    fn windows_private_file_handle_can_set_an_explicit_user_owner() {
        use windows_sys::Win32::Storage::FileSystem::WRITE_OWNER;

        assert_eq!(
            windows_private_file_access_mode() & WRITE_OWNER,
            WRITE_OWNER
        );
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
        use std::process::Command;

        let temp = tempfile::tempdir().expect("create inherited ACL fixture");
        let parent = temp.path().join("parent");
        std::fs::create_dir(&parent).expect("create ACL parent");
        let username = Command::new("id")
            .arg("-un")
            .output()
            .expect("read current macOS username");
        assert!(username.status.success());
        let username = String::from_utf8(username.stdout)
            .expect("username is UTF-8")
            .trim()
            .to_owned();
        let inherited_ace = format!("{username} allow read,file_inherit");
        let status = Command::new("chmod")
            .args(["+a", inherited_ace.as_str()])
            .arg(&parent)
            .status()
            .expect("install inheritable macOS ACL");
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
