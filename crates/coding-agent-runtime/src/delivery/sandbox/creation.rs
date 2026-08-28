use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::sync::Arc;

use crate::RelativePath;
use crate::command_policy::ExecutionDirectory;
#[cfg(windows)]
use crate::native_fs::create_child_directory_with_created;
#[cfg(unix)]
use crate::native_fs::create_private_child_file_exclusive;
#[cfg(unix)]
use crate::native_fs::open_child_directory;
use crate::native_fs::reopen_directory_for_child_directory;

#[cfg(unix)]
use super::EMPTY_CONFIG_NAME;
#[cfg(unix)]
use super::validation::require_empty_plain_file;
use super::validation::{is_direct_child, require_same_directory};
use super::{DeliveryCommandSandbox, DeliverySourceError, MAX_ALLOCATION_ATTEMPTS, SandboxStage};

#[cfg(unix)]
use crate::native_fs::{child_entry_exists, remove_child_file};

impl DeliveryCommandSandbox {
    pub(in crate::delivery) fn create(
        parent: Arc<ExecutionDirectory>,
    ) -> Result<Self, DeliverySourceError> {
        parent
            .revalidate()
            .map_err(|_| DeliverySourceError::SandboxUnavailable)?;
        let parent_root = parent
            .cloned_root_capability()
            .map_err(|_| DeliverySourceError::SandboxUnavailable)?;

        for _ in 0..MAX_ALLOCATION_ATTEMPTS {
            let name = random_workspace_name()?;
            let parent_handle = parent_root
                .try_clone_root()
                .and_then(|root| reopen_directory_for_child_directory(&root))
                .map_err(|_| DeliverySourceError::SandboxUnavailable)?;
            let Some(created) = create_direct_child_exclusive(&parent_handle, OsStr::new(&name))
                .map_err(|_| DeliverySourceError::SandboxUnavailable)?
            else {
                continue;
            };
            let mut sandbox =
                Self::from_created_workspace(Arc::clone(&parent), &parent_root, name, created)?;
            if let Err(error) = sandbox.initialize() {
                return match sandbox.cleanup_inner() {
                    Ok(()) => Err(error),
                    Err(_) => Err(DeliverySourceError::SandboxCleanupUnproven),
                };
            }
            return Ok(sandbox);
        }
        Err(DeliverySourceError::SandboxUnavailable)
    }

    fn from_created_workspace(
        parent: Arc<ExecutionDirectory>,
        parent_root: &crate::RootCapability,
        name: String,
        created: File,
    ) -> Result<Self, DeliverySourceError> {
        let relative = RelativePath::parse(name.clone())
            .map_err(|_| DeliverySourceError::SandboxCleanupUnproven)?;
        let guard = parent_root
            .ensure_directory_path(&relative)
            .map_err(|_| DeliverySourceError::SandboxCleanupUnproven)?;
        let path = parent.path().join(&name);
        if !is_direct_child(&path, parent.path(), &name) {
            return Err(DeliverySourceError::SandboxCleanupUnproven);
        }
        let directory = Arc::new(
            ExecutionDirectory::open(&path)
                .map_err(|_| DeliverySourceError::SandboxCleanupUnproven)?,
        );
        require_same_directory(&created, &guard, &directory)?;
        parent
            .revalidate()
            .map_err(|_| DeliverySourceError::SandboxCleanupUnproven)?;

        Ok(Self {
            parent,
            name,
            path,
            workspace_directory: Some(directory),
            workspace_guard: Some(guard),
            #[cfg(unix)]
            config_file: None,
            stage: SandboxStage::Workspace,
            cleaned: false,
        })
    }

    fn initialize(&mut self) -> Result<(), DeliverySourceError> {
        #[cfg(unix)]
        self.create_empty_config()?;
        self.stage = SandboxStage::Ready;
        self.revalidate()
    }

    #[cfg(unix)]
    fn create_empty_config(&mut self) -> Result<(), DeliverySourceError> {
        let root = self.workspace_root()?;
        let parent = root
            .try_clone_root()
            .map_err(|_| DeliverySourceError::SandboxUnavailable)?;
        let config = create_private_child_file_exclusive(&parent, OsStr::new(EMPTY_CONFIG_NAME))
            .map_err(|_| DeliverySourceError::SandboxUnavailable)?;
        config
            .sync_all()
            .map_err(|_| DeliverySourceError::SandboxUnavailable)?;
        let retained = config;
        require_empty_plain_file(&retained)?;
        remove_child_file(&parent, OsStr::new(EMPTY_CONFIG_NAME), &retained)
            .map_err(|_| DeliverySourceError::SandboxCleanupUnproven)?;
        if child_entry_exists(&parent, OsStr::new(EMPTY_CONFIG_NAME))
            .map_err(|_| DeliverySourceError::SandboxCleanupUnproven)?
        {
            return Err(DeliverySourceError::SandboxCleanupUnproven);
        }
        self.config_file = Some(retained);
        self.stage = SandboxStage::Config;
        Ok(())
    }
}

fn random_workspace_name() -> Result<String, DeliverySourceError> {
    let mut random = [0u8; 16];
    getrandom::fill(&mut random).map_err(|_| DeliverySourceError::SandboxUnavailable)?;
    let mut name = String::from(".coding-agent-delivery-sandbox-");
    for byte in random {
        use std::fmt::Write as _;
        write!(&mut name, "{byte:02x}").expect("writing hexadecimal bytes to String cannot fail");
    }
    Ok(name)
}

#[cfg(unix)]
fn create_direct_child_exclusive(parent: &File, name: &OsStr) -> io::Result<Option<File>> {
    use std::ffi::CString;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;

    let name = CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "directory name contains NUL"))?;
    let result = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
    if result == 0 {
        open_child_directory(parent, OsStr::from_bytes(name.as_bytes())).map(Some)
    } else {
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::AlreadyExists {
            Ok(None)
        } else {
            Err(error)
        }
    }
}

#[cfg(windows)]
fn create_direct_child_exclusive(parent: &File, name: &OsStr) -> io::Result<Option<File>> {
    create_child_directory_with_created(parent, name)
        .map(|(directory, created)| created.then_some(directory))
}
