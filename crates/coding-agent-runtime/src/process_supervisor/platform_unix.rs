use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use super::*;
use tokio::signal::unix::{Signal, SignalKind};

pub(super) struct DeliveryDescriptorArguments {
    arguments: Vec<OsString>,
    environment: Vec<(OsString, OsString)>,
    inherited_resources: Vec<File>,
}

impl DeliveryDescriptorArguments {
    pub(super) fn try_new(command: &ValidatedCommand) -> Result<Self, ProcessError> {
        let mut materialized = Self {
            arguments: command.arguments().to_vec(),
            environment: Vec::new(),
            inherited_resources: Vec::new(),
        };
        let bindings = command.unix_delivery_directory_bindings();
        let config = command.delivery_git_empty_config();
        if bindings.is_none() && config.is_none() {
            return Ok(materialized);
        }
        let descriptor_root = delivery_descriptor_root().map_err(ProcessError::TreeSetupFailed)?;
        if let Some(bindings) = bindings {
            for binding in bindings.bindings() {
                materialized.materialize_binding(binding, descriptor_root)?;
            }
        }
        if let Some(config) = config {
            materialized.materialize_empty_config(config, descriptor_root)?;
        }
        Ok(materialized)
    }

    fn materialize_binding(
        &mut self,
        binding: &crate::command_policy::UnixDeliveryDirectoryBinding,
        descriptor_root: &std::path::Path,
    ) -> Result<(), ProcessError> {
        match binding.role() {
            UnixDeliveryDirectoryRole::WorkTree { argument_index } => {
                replace_delivery_argument(
                    &mut self.arguments,
                    argument_index,
                    OsString::from("--work-tree=."),
                )?;
            }
            role => {
                #[cfg(target_os = "macos")]
                let directory_path = {
                    let _ = descriptor_root;
                    // Darwin's /dev/fd entries can reopen one descriptor, but
                    // an entry for a directory is not a traversable namespace:
                    // /dev/fd/<dirfd>/child cannot be resolved. Revalidate the
                    // admitted namespace immediately before spawn and use it
                    // while prepare() retains the matching dependent-directory
                    // handle through exec, mirroring the macOS executable-path
                    // compatibility boundary above.
                    binding
                        .directory()
                        .revalidate()
                        .map_err(ProcessError::CommandPolicy)?;
                    binding.directory().path().to_owned()
                };
                #[cfg(not(target_os = "macos"))]
                let directory = binding
                    .directory()
                    .cloned_directory()
                    .map_err(ProcessError::CommandPolicy)?;
                #[cfg(not(target_os = "macos"))]
                let directory =
                    normalize_inherited_file(directory).map_err(ProcessError::TreeSetupFailed)?;
                #[cfg(not(target_os = "macos"))]
                let directory_path = descriptor_root.join(directory.as_raw_fd().to_string());
                match role {
                    UnixDeliveryDirectoryRole::GitDirectory { argument_index } => {
                        replace_delivery_argument(
                            &mut self.arguments,
                            argument_index,
                            prefixed_path_argument("--git-dir=", &directory_path),
                        )?;
                    }
                    UnixDeliveryDirectoryRole::CommonGitEnvironment => {
                        self.environment.push((
                            OsString::from("GIT_COMMON_DIR"),
                            directory_path.into_os_string(),
                        ));
                    }
                    UnixDeliveryDirectoryRole::TemporaryIndexEnvironment => {
                        self.environment.push((
                            OsString::from("GIT_INDEX_FILE"),
                            directory_path.join("index").into_os_string(),
                        ));
                    }
                    UnixDeliveryDirectoryRole::WorkTree { .. } => unreachable!(
                        "the working tree is materialized from the retained cwd capability"
                    ),
                }
                #[cfg(not(target_os = "macos"))]
                self.inherited_resources.push(directory);
            }
        }
        Ok(())
    }

    fn materialize_empty_config(
        &mut self,
        config: &crate::command_policy::DeliveryGitEmptyConfig,
        descriptor_root: &std::path::Path,
    ) -> Result<(), ProcessError> {
        config.revalidate().map_err(ProcessError::CommandPolicy)?;
        let file = config.cloned_file().map_err(ProcessError::CommandPolicy)?;
        let file = normalize_inherited_file(file).map_err(ProcessError::TreeSetupFailed)?;
        let descriptor_path = descriptor_root.join(file.as_raw_fd().to_string());
        for key in ["GIT_CONFIG_GLOBAL", "GIT_CONFIG_SYSTEM"] {
            self.environment.push((
                OsString::from(key),
                descriptor_path.clone().into_os_string(),
            ));
        }
        self.inherited_resources.push(file);
        Ok(())
    }

    pub(super) fn arguments(&self) -> &[OsString] {
        &self.arguments
    }

    pub(super) fn environment(&self) -> &[(OsString, OsString)] {
        &self.environment
    }

    #[cfg(test)]
    pub(super) fn inherited_resource_count(&self) -> usize {
        self.inherited_resources.len()
    }

    pub(super) fn into_inherited_resources(self) -> Vec<File> {
        self.inherited_resources
    }
}

fn replace_delivery_argument(
    arguments: &mut [OsString],
    index: usize,
    replacement: OsString,
) -> Result<(), ProcessError> {
    let argument = arguments.get_mut(index).ok_or(ProcessError::CommandPolicy(
        CommandPolicyError::InvalidGitBinding,
    ))?;
    *argument = replacement;
    Ok(())
}

fn prefixed_path_argument(prefix: &str, path: &std::path::Path) -> OsString {
    let mut argument = OsString::from(prefix);
    argument.push(path);
    argument
}

fn delivery_descriptor_root() -> io::Result<&'static std::path::Path> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let root = std::path::Path::new("/proc/self/fd");
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let root = std::path::Path::new("/dev/fd");
    if root.is_dir() {
        Ok(root)
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "descriptor-backed delivery directory namespace is unavailable",
        ))
    }
}

pub(super) struct Executable {
    file: File,
    program: PathBuf,
}

impl Executable {
    pub(super) fn new(path: &std::path::Path, file: File) -> io::Result<Self> {
        let file = normalize_inherited_file(file)?;
        #[cfg(target_os = "macos")]
        let program = path.to_owned();
        #[cfg(not(target_os = "macos"))]
        let program = {
            let _ = path;
            #[cfg(any(target_os = "linux", target_os = "android"))]
            let descriptor_root = std::path::Path::new("/proc/self/fd");
            #[cfg(not(any(target_os = "linux", target_os = "android")))]
            let descriptor_root = std::path::Path::new("/dev/fd");
            if !descriptor_root.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "descriptor-backed executable namespace is unavailable",
                ));
            }
            descriptor_root.join(file.as_raw_fd().to_string())
        };
        Ok(Self { file, program })
    }

    pub(super) fn program(&self) -> &std::path::Path {
        &self.program
    }
}

pub(super) struct Prepared {
    sigchld: Signal,
    liveness_read: OwnedFd,
    liveness_write: OwnedFd,
}

pub(super) struct LeaderExit {
    process_id: libc::id_t,
    sigchld: Signal,
    liveness_read: OwnedFd,
}

pub(super) struct ProcessTree {
    process_group: i32,
}

pub(super) fn prepare(
    command: &mut Command,
    executable: Executable,
    working_directory: File,
    dependent_directories: Vec<File>,
    delivery_descriptor_resources: Vec<File>,
    process_liveness_descriptor: std::os::fd::RawFd,
) -> io::Result<Prepared> {
    let sigchld = tokio::signal::unix::signal(SignalKind::child())?;
    let (liveness_read, liveness_write) = create_liveness_pipe()?;
    let inherited_write = liveness_write.as_raw_fd();
    #[cfg(not(target_os = "macos"))]
    let executable_descriptor = executable.file.as_raw_fd();
    let working_directory = normalize_inherited_file(working_directory)?;
    let working_directory_descriptor = working_directory.as_raw_fd();
    unsafe {
        command.pre_exec(move || {
            // Keep both owned descriptors captured until this closure has
            // run in the child. The executable descriptor must survive the
            // following exec long enough for descriptor-backed Unix paths
            // to resolve it. macOS retains the same revalidated handle until
            // path-based exec and leaves it CLOEXEC so it is not exposed to
            // the launched tool. The cwd is selected from its retained
            // capability.
            let _executable = &executable.file;
            let _working_directory = &working_directory;
            let _dependent_directories = &dependent_directories;
            let _delivery_descriptor_resources = &delivery_descriptor_resources;
            if libc::fchdir(working_directory_descriptor) != 0 {
                return Err(io::Error::last_os_error());
            }
            for resource in &delivery_descriptor_resources {
                clear_close_on_exec(resource.as_raw_fd())?;
            }
            #[cfg(not(target_os = "macos"))]
            clear_close_on_exec(executable_descriptor)?;
            clear_close_on_exec(inherited_write)?;
            clear_close_on_exec(process_liveness_descriptor)
        });
    }
    command.process_group(0);
    Ok(Prepared {
        sigchld,
        liveness_read,
        liveness_write,
    })
}

pub(super) fn leader_anchor_lost(error: &io::Error) -> bool {
    error.raw_os_error() == Some(libc::ECHILD)
}

impl Prepared {
    pub(super) fn attach_and_resume(
        self,
        child: &Child,
    ) -> io::Result<(TreeKillHandle, LeaderExit)> {
        let Self {
            sigchld,
            liveness_read,
            liveness_write,
        } = self;
        let process_group = child
            .id()
            .and_then(|id| i32::try_from(id).ok())
            .ok_or_else(|| io::Error::other("spawned process has no valid process group"))?;
        let attached = (
            TreeKillHandle::new(ProcessTree { process_group }),
            LeaderExit {
                process_id: process_group as libc::id_t,
                sigchld,
                liveness_read,
            },
        );
        drop(liveness_write);
        Ok(attached)
    }
}

impl LeaderExit {
    pub(super) async fn wait(&mut self, _child: &mut Child) -> io::Result<Option<ExitStatus>> {
        loop {
            if exit_is_waitable(self.process_id)? {
                return Ok(None);
            }
            self.sigchld
                .recv()
                .await
                .ok_or_else(|| io::Error::other("SIGCHLD listener closed"))?;
        }
    }

    pub(super) async fn wait_tree_before_reap(&mut self) -> io::Result<()> {
        loop {
            if liveness_pipe_has_no_writers(&self.liveness_read)? {
                return Ok(());
            }
            time::sleep(Duration::from_millis(2)).await;
        }
    }

    #[cfg(target_os = "macos")]
    pub(super) fn liveness_pipe_has_no_writers_now(&self) -> io::Result<bool> {
        liveness_pipe_has_no_writers(&self.liveness_read)
    }

    pub(super) async fn wait_tree_after_reap(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn create_liveness_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut descriptors = [-1; 2];
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let result =
        unsafe { libc::pipe2(descriptors.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let result = unsafe { libc::pipe(descriptors.as_mut_ptr()) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    let read = match normalize_sentinel_descriptor(descriptors[0]) {
        Ok(read) => read,
        Err(error) => {
            let _ = unsafe { libc::close(descriptors[1]) };
            return Err(error);
        }
    };
    let write = normalize_sentinel_descriptor(descriptors[1])?;
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        set_close_on_exec(read.as_raw_fd())?;
        set_close_on_exec(write.as_raw_fd())?;
        set_nonblocking(read.as_raw_fd())?;
    }
    Ok((read, write))
}

fn normalize_sentinel_descriptor(descriptor: i32) -> io::Result<OwnedFd> {
    let original = unsafe { OwnedFd::from_raw_fd(descriptor) };
    if descriptor > libc::STDERR_FILENO {
        return Ok(original);
    }
    let duplicate =
        unsafe { libc::fcntl(descriptor, libc::F_DUPFD_CLOEXEC, libc::STDERR_FILENO + 1) };
    if duplicate == -1 {
        return Err(io::Error::last_os_error());
    }
    drop(original);
    Ok(unsafe { OwnedFd::from_raw_fd(duplicate) })
}

fn normalize_inherited_file(file: File) -> io::Result<File> {
    let descriptor = file.as_raw_fd();
    if descriptor > libc::STDERR_FILENO {
        return Ok(file);
    }
    let duplicate =
        unsafe { libc::fcntl(descriptor, libc::F_DUPFD_CLOEXEC, libc::STDERR_FILENO + 1) };
    if duplicate == -1 {
        return Err(io::Error::last_os_error());
    }
    drop(file);
    Ok(unsafe { File::from_raw_fd(duplicate) })
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn set_close_on_exec(descriptor: i32) -> io::Result<()> {
    update_descriptor_flags(descriptor, |flags| flags | libc::FD_CLOEXEC)
}

fn clear_close_on_exec(descriptor: i32) -> io::Result<()> {
    update_descriptor_flags(descriptor, |flags| flags & !libc::FD_CLOEXEC)
}

fn update_descriptor_flags(descriptor: i32, update: impl FnOnce(i32) -> i32) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
    if flags == -1 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(descriptor, libc::F_SETFD, update(flags)) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn set_nonblocking(descriptor: i32) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
    if flags == -1 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub(super) fn liveness_pipe_has_no_writers(read: &OwnedFd) -> io::Result<bool> {
    let mut byte = 0u8;
    loop {
        let read_count = unsafe {
            libc::read(
                read.as_raw_fd(),
                (&mut byte as *mut u8).cast::<libc::c_void>(),
                1,
            )
        };
        if read_count == 0 {
            return Ok(true);
        }
        if read_count > 0 {
            continue;
        }
        let error = io::Error::last_os_error();
        return match error.kind() {
            io::ErrorKind::Interrupted => continue,
            io::ErrorKind::WouldBlock => Ok(false),
            _ => Err(error),
        };
    }
}

fn exit_is_waitable(process_id: libc::id_t) -> io::Result<bool> {
    loop {
        let mut information = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                process_id,
                information.as_mut_ptr(),
                libc::WEXITED | libc::WNOWAIT | libc::WNOHANG,
            )
        };
        if result == 0 {
            let information = unsafe { information.assume_init() };
            return Ok(unsafe { information.si_pid() } != 0);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

impl ProcessTree {
    pub(super) fn kill(&self) -> io::Result<()> {
        let result = unsafe { libc::kill(-self.process_group, libc::SIGKILL) };
        if result == 0 {
            Ok(())
        } else {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ESRCH) {
                Ok(())
            } else {
                Err(error)
            }
        }
    }
}
