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
        let path = path.as_ref();
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path)?;
        if let Err(error) = make_private(path, false) {
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

#[cfg(windows)]
fn make_private(path: &Path, _directory: bool) -> io::Result<()> {
    use std::mem::size_of;
    use std::os::windows::ffi::OsStrExt;
    use std::ptr::{null, null_mut};

    use windows_sys::Win32::Foundation::{CloseHandle, ERROR_SUCCESS, HANDLE, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        BuildTrusteeWithSidW, EXPLICIT_ACCESS_W, SE_FILE_OBJECT, SET_ACCESS, SetEntriesInAclW,
        SetNamedSecurityInfoW, TRUSTEE_W,
    };
    use windows_sys::Win32::Security::{
        ACL, DACL_SECURITY_INFORMATION, GetTokenInformation, NO_INHERITANCE,
        PROTECTED_DACL_SECURITY_INFORMATION, TOKEN_QUERY, TOKEN_USER, TokenUser,
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
        grfInheritance: NO_INHERITANCE,
        Trustee: trustee,
    };
    let mut acl: *mut ACL = null_mut();
    let acl_status = unsafe { SetEntriesInAclW(1, &access, null(), &mut acl) };
    if acl_status != ERROR_SUCCESS {
        return Err(io::Error::from_raw_os_error(acl_status as i32));
    }

    let mut wide = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let status = unsafe {
        SetNamedSecurityInfoW(
            wide.as_mut_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            null_mut(),
            null_mut(),
            acl,
            null(),
        )
    };
    unsafe { LocalFree(acl.cast()) };
    if status == ERROR_SUCCESS {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(status as i32))
    }
}

#[cfg(test)]
mod tests {
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
}
