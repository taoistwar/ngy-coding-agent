use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

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
            use windows_sys::Win32::Foundation::{GENERIC_READ, GENERIC_WRITE};
            use windows_sys::Win32::Storage::FileSystem::WRITE_DAC;

            options.access_mode(GENERIC_READ | GENERIC_WRITE | WRITE_DAC);
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("failed to open browser for {url}")]
pub struct BrowserLaunchError {
    url: String,
}

impl BrowserLaunchError {
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

fn create_private_directory(path: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "private directory path is a symlink",
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

#[cfg(unix)]
fn make_private(path: &Path, directory: bool) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mode = if directory { 0o700 } else { 0o600 };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

#[cfg(unix)]
fn make_private_file(file: &File) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    file.set_permissions(std::fs::Permissions::from_mode(0o600))
}

#[cfg(windows)]
fn make_private(path: &Path, directory: bool) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use std::ptr::{null, null_mut};

    use windows_sys::Win32::Security::Authorization::{SE_FILE_OBJECT, SetNamedSecurityInfoW};
    use windows_sys::Win32::Security::{
        DACL_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION,
    };

    let mut wide = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    with_windows_owner_only_acl(directory, |acl| unsafe {
        SetNamedSecurityInfoW(
            wide.as_mut_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            null_mut(),
            null_mut(),
            acl,
            null(),
        )
    })
}

#[cfg(windows)]
fn make_private_file(file: &File) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use std::ptr::{null, null_mut};

    use windows_sys::Win32::Security::Authorization::{SE_FILE_OBJECT, SetSecurityInfo};
    use windows_sys::Win32::Security::{
        DACL_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION,
    };

    with_windows_owner_only_acl(false, |acl| unsafe {
        SetSecurityInfo(
            file.as_raw_handle(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            null_mut(),
            null_mut(),
            acl,
            null(),
        )
    })
}

#[cfg(windows)]
fn with_windows_owner_only_acl(
    directory: bool,
    apply: impl FnOnce(*mut windows_sys::Win32::Security::ACL) -> u32,
) -> io::Result<()> {
    use std::mem::size_of;
    use std::ptr::{null, null_mut};

    use windows_sys::Win32::Foundation::{CloseHandle, ERROR_SUCCESS, HANDLE, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        BuildTrusteeWithSidW, EXPLICIT_ACCESS_W, SET_ACCESS, SetEntriesInAclW, TRUSTEE_W,
    };
    use windows_sys::Win32::Security::{
        ACL, GetTokenInformation, NO_INHERITANCE, SUB_CONTAINERS_AND_OBJECTS_INHERIT, TOKEN_QUERY,
        TOKEN_USER, TokenUser,
    };
    use windows_sys::Win32::Storage::FileSystem::FILE_ALL_ACCESS;
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    let mut token: HANDLE = null_mut();
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }

    let mut required = 0u32;
    unsafe {
        GetTokenInformation(token, TokenUser, null_mut(), 0, &mut required);
    }
    if required == 0 {
        unsafe { CloseHandle(token) };
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
        let error = io::Error::last_os_error();
        unsafe { CloseHandle(token) };
        return Err(error);
    }
    unsafe { CloseHandle(token) };

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

    let status = apply(acl);
    unsafe { LocalFree(acl.cast()) };
    if status == ERROR_SUCCESS {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(status as i32))
    }
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
