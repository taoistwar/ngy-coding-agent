use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::OsStr;
use std::fmt;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};

#[cfg(windows)]
use crate::native_fs::create_child_directory_with_created;
use crate::native_fs::{
    child_entry_exists, child_file_matches, create_child_file_exclusive,
    open_child_file_for_exclusive_probe, read_directory_names, remove_child_file,
    reopen_directory_for_child_directory,
};
#[cfg(unix)]
use crate::native_fs::{create_child_directory, quarantine_child_file_no_replace};
use crate::root_capability::{
    DirectoryIdentityMarker, RootCapability, directory_identity_marker, ensure_plain_directory,
    ensure_plain_file,
};

const SENTINEL_DIRECTORY: &str = "process-liveness";
const CONTENT_MAGIC: &str = "coding-agent-process-liveness-v1";
const MAX_SENTINELS: usize = 4_096;
const MAX_SENTINEL_BYTES: u64 = 256;
const NONCE_ATTEMPTS: usize = 32;

/// Cross-crash proof about the process trees represented by liveness sentinels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessCleanupProof {
    /// No matching process tree retains its sentinel and every stale sentinel
    /// was exclusively opened, validated, and removed.
    Confirmed,
    /// At least one matching process tree still retains its sentinel.
    Held,
    /// The sentinel namespace could not prove either safe cleanup or liveness.
    Unknown,
}

/// Fixed, path-free failures for process-liveness setup and operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProcessLivenessError {
    #[error("process-liveness identity is invalid")]
    InvalidIdentity,
    #[error("process-liveness state is unavailable")]
    Unavailable,
    #[error("the task process-liveness scope is sealed for cleanup")]
    ScopeSealed,
}

/// Handle-backed access to the private process-liveness namespace.
#[derive(Clone)]
pub struct ProcessLivenessDirectory {
    directory: Arc<File>,
    identity: DirectoryIdentityMarker,
}

impl fmt::Debug for ProcessLivenessDirectory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProcessLivenessDirectory(<opaque>)")
    }
}

impl ProcessLivenessDirectory {
    /// Opens (or creates) the fixed private sentinel directory beneath an
    /// already private runtime directory.
    pub fn open(runtime_directory: impl AsRef<Path>) -> Result<Self, ProcessLivenessError> {
        let root = RootCapability::open(runtime_directory)
            .map_err(|_| ProcessLivenessError::Unavailable)?;
        let parent = root
            .try_clone_root()
            .and_then(|root| reopen_directory_for_child_directory(&root))
            .map_err(|_| ProcessLivenessError::Unavailable)?;
        let directory = create_process_liveness_directory(&parent)
            .and_then(|directory| {
                ensure_plain_directory(&directory)?;
                validate_private_directory(&directory)?;
                Ok(directory)
            })
            .map_err(|_| ProcessLivenessError::Unavailable)?;
        let identity =
            directory_identity_marker(&directory).map_err(|_| ProcessLivenessError::Unavailable)?;
        Ok(Self {
            directory: Arc::new(directory),
            identity,
        })
    }

    pub fn instance_scope(
        &self,
        instance_id: [u8; 16],
    ) -> Result<ProcessLivenessScope, ProcessLivenessError> {
        validate_identity(instance_id)?;
        let key = (self.identity, instance_id);
        let mut registry = lock_instance_registry();
        registry.retain(|_, instance| instance.strong_count() > 0);
        if let Some(instance) = registry.get(&key).and_then(Weak::upgrade) {
            return Ok(ProcessLivenessScope {
                instance,
                task_id: None,
            });
        }
        let instance = Arc::new(InstanceState {
            directory: self.directory.clone(),
            instance_id,
            registrations: Mutex::new(BTreeMap::new()),
            seals: Mutex::new(ScopeSeals::default()),
            #[cfg(test)]
            begin_tree_after_check: Mutex::new(None),
        });
        registry.insert(key, Arc::downgrade(&instance));
        Ok(ProcessLivenessScope {
            instance,
            task_id: None,
        })
    }

    /// Probes all pre-existing sentinels. A namespace entry is removed only
    /// after a no-follow open, an exclusive lock probe, strict metadata and
    /// content validation, and a final handle/namespace identity check.
    pub fn probe_stale(&self) -> Result<ProcessCleanupProof, ProcessLivenessError> {
        let mut scan = self
            .directory
            .try_clone()
            .map_err(|_| ProcessLivenessError::Unavailable)?;
        let names = read_directory_names(&mut scan, MAX_SENTINELS)
            .map_err(|_| ProcessLivenessError::Unavailable)?;
        let mut aggregate = ProcessCleanupProof::Confirmed;
        for name in names {
            if name == OsStr::new(".") || name == OsStr::new("..") {
                continue;
            }
            let Some(name) = name.to_str() else {
                aggregate = combine_proof(aggregate, ProcessCleanupProof::Unknown);
                continue;
            };
            let proof = if parse_sentinel_name(name).is_some() {
                probe_named(&self.directory, name)?
            } else {
                #[cfg(unix)]
                {
                    if let Some(original_name) = parse_cleanup_name(name) {
                        probe_quarantined(&self.directory, name, original_name)?
                    } else {
                        ProcessCleanupProof::Unknown
                    }
                }
                #[cfg(windows)]
                {
                    ProcessCleanupProof::Unknown
                }
            };
            aggregate = combine_proof(aggregate, proof);
        }
        Ok(aggregate)
    }
}

#[cfg(unix)]
fn create_process_liveness_directory(parent: &File) -> io::Result<File> {
    create_child_directory(parent, OsStr::new(SENTINEL_DIRECTORY))
}

#[cfg(windows)]
fn create_process_liveness_directory(parent: &File) -> io::Result<File> {
    let (directory, created) =
        create_child_directory_with_created(parent, OsStr::new(SENTINEL_DIRECTORY))?;
    if created {
        make_private_windows_acl(&directory)?;
    }
    Ok(directory)
}

/// An opaque instance- or task-scoped owner for process-tree sentinels.
#[derive(Clone)]
pub struct ProcessLivenessScope {
    instance: Arc<InstanceState>,
    task_id: Option<[u8; 16]>,
}

impl fmt::Debug for ProcessLivenessScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProcessLivenessScope(<opaque>)")
    }
}

impl ProcessLivenessScope {
    /// Returns true only for the same liveness instance and the same exact
    /// task/instance selector. Lifecycle code uses this to keep a sealed
    /// worker scope distinct from the supervisor scope that runs cleanup
    /// children.
    pub(crate) fn is_same_scope(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.instance, &other.instance) && self.task_id == other.task_id
    }

    pub(crate) fn is_same_instance(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.instance, &other.instance)
    }

    pub(crate) const fn is_task_scope(&self) -> bool {
        self.task_id.is_some()
    }

    pub fn task_scope(
        &self,
        task_id: [u8; 16],
    ) -> Result<ProcessLivenessScope, ProcessLivenessError> {
        validate_identity(task_id)?;
        if self.task_id.is_some() {
            return Err(ProcessLivenessError::InvalidIdentity);
        }
        Ok(Self {
            instance: self.instance.clone(),
            task_id: Some(task_id),
        })
    }

    /// Creates a distinct task-scoped sibling in the same authenticated
    /// liveness instance. Cleanup children use this to remain visible to the
    /// same startup/recovery namespace without reusing the sealed worker
    /// scope that they are proving inactive.
    pub fn sibling_task_scope(
        &self,
        task_id: [u8; 16],
    ) -> Result<ProcessLivenessScope, ProcessLivenessError> {
        validate_identity(task_id)?;
        let current_task = self.task_id.ok_or(ProcessLivenessError::InvalidIdentity)?;
        if current_task == task_id {
            return Err(ProcessLivenessError::InvalidIdentity);
        }
        Ok(Self {
            instance: Arc::clone(&self.instance),
            task_id: Some(task_id),
        })
    }

    pub fn active_tree_count(&self) -> usize {
        let registrations = lock_registrations(&self.instance);
        registrations
            .values()
            .filter(|registration| self.matches(registration.task_id))
            .count()
    }

    pub fn cleanup_proof(&self) -> Result<ProcessCleanupProof, ProcessLivenessError> {
        let names = {
            let registrations = lock_registrations(&self.instance);
            registrations
                .iter()
                .filter(|(_, registration)| self.matches(registration.task_id))
                .map(|(name, _)| name.clone())
                .collect::<Vec<_>>()
        };
        if names.is_empty() {
            return Ok(ProcessCleanupProof::Confirmed);
        }
        let mut aggregate = ProcessCleanupProof::Confirmed;
        for name in names {
            aggregate = combine_proof(
                aggregate,
                probe_registered(&self.instance.directory, &name)?,
            );
        }
        Ok(aggregate)
    }

    /// Observes cleanup for the exact task scope supplied by the scheduler.
    ///
    /// This does not itself authorize any lifecycle or permit transition. It
    /// only prevents an instance scope or a different task scope from being
    /// substituted when the task-manager actor later mints its private
    /// runner-returned cleanup confirmation.
    pub fn cleanup_proof_for_task(
        &self,
        task_id: [u8; 16],
    ) -> Result<ProcessCleanupProof, ProcessLivenessError> {
        validate_identity(task_id)?;
        if self.task_id != Some(task_id) {
            return Err(ProcessLivenessError::InvalidIdentity);
        }
        self.cleanup_proof()
    }

    /// Seals this exact task scope against all future sentinel creation in the
    /// same instance before cleanup is probed.
    pub fn seal_task_scope(
        &self,
        task_id: [u8; 16],
    ) -> Result<SealedProcessLivenessScope, ProcessLivenessError> {
        validate_identity(task_id)?;
        if self.task_id != Some(task_id) {
            return Err(ProcessLivenessError::InvalidIdentity);
        }
        lock_scope_seals(&self.instance).tasks.insert(task_id);
        Ok(SealedProcessLivenessScope {
            scope: self.clone(),
            selector: SealedScopeSelector::Task(task_id),
        })
    }

    /// Seals the complete instance against every future process-tree
    /// registration before an aggregate cleanup proof is observed.
    pub fn seal_instance_scope(&self) -> Result<SealedProcessLivenessScope, ProcessLivenessError> {
        if self.task_id.is_some() {
            return Err(ProcessLivenessError::InvalidIdentity);
        }
        lock_scope_seals(&self.instance).instance = true;
        Ok(SealedProcessLivenessScope {
            scope: self.clone(),
            selector: SealedScopeSelector::Instance,
        })
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn hold_tree_for_test(
        &self,
    ) -> Result<HeldProcessLivenessTreeForTest, ProcessLivenessError> {
        self.begin_tree()
            .map(|sentinel| HeldProcessLivenessTreeForTest {
                _sentinel: sentinel,
            })
    }

    pub(crate) fn begin_tree(&self) -> Result<ProcessLivenessSentinel, ProcessLivenessError> {
        let seals = lock_scope_seals(&self.instance);
        if seals.instance
            || self
                .task_id
                .is_some_and(|task_id| seals.tasks.contains(&task_id))
        {
            return Err(ProcessLivenessError::ScopeSealed);
        }
        #[cfg(test)]
        if let Some(hook) = self
            .instance
            .begin_tree_after_check
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            hook.entered.wait();
            hook.release.wait();
        }
        for _ in 0..NONCE_ATTEMPTS {
            let mut nonce = [0u8; 16];
            getrandom::fill(&mut nonce).map_err(|_| ProcessLivenessError::Unavailable)?;
            let name = sentinel_name(self.instance.instance_id, self.task_id, nonce);
            match create_sentinel(&self.instance.directory, &name) {
                Ok(file) => {
                    lock_registrations(&self.instance).insert(
                        name.clone(),
                        Registration {
                            task_id: self.task_id,
                        },
                    );
                    return Ok(ProcessLivenessSentinel {
                        instance: self.instance.clone(),
                        name,
                        parent_file: Some(file),
                        spawn_committed: false,
                        completed: false,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(_) => return Err(ProcessLivenessError::Unavailable),
            }
        }
        Err(ProcessLivenessError::Unavailable)
    }

    fn matches(&self, task_id: Option<[u8; 16]>) -> bool {
        self.task_id.is_none() || self.task_id == task_id
    }
}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub struct HeldProcessLivenessTreeForTest {
    _sentinel: ProcessLivenessSentinel,
}

#[cfg(feature = "test-support")]
impl fmt::Debug for HeldProcessLivenessTreeForTest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HeldProcessLivenessTreeForTest(<opaque>)")
    }
}

/// Opaque proof source obtained only after an exact task scope is sealed.
pub struct SealedProcessLivenessScope {
    scope: ProcessLivenessScope,
    selector: SealedScopeSelector,
}

enum SealedScopeSelector {
    Instance,
    Task([u8; 16]),
}

impl SealedProcessLivenessScope {
    pub fn cleanup_proof(&self) -> Result<ProcessCleanupProof, ProcessLivenessError> {
        match self.selector {
            SealedScopeSelector::Instance => self.scope.cleanup_proof(),
            SealedScopeSelector::Task(task_id) => self.scope.cleanup_proof_for_task(task_id),
        }
    }

    /// Proves that this sealed cleanup authority was minted from the exact
    /// process-liveness scope retained by a mutation runtime. A confirmed
    /// proof from another task or another liveness instance must never
    /// authorize cleanup of this task's worktree.
    pub(crate) fn is_bound_to(&self, expected: &ProcessLivenessScope) -> bool {
        self.scope.is_same_scope(expected)
            && match self.selector {
                SealedScopeSelector::Instance => expected.task_id.is_none(),
                SealedScopeSelector::Task(task_id) => expected.task_id == Some(task_id),
            }
    }
}

impl fmt::Debug for SealedProcessLivenessScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SealedProcessLivenessScope(<opaque>)")
    }
}

#[cfg(test)]
pub(crate) fn test_process_scope() -> ProcessLivenessScope {
    static SCOPE: OnceLock<(tempfile::TempDir, ProcessLivenessScope)> = OnceLock::new();

    SCOPE
        .get_or_init(|| {
            let temporary =
                tempfile::tempdir().expect("create private process-liveness unit-test directory");
            let directory =
                ProcessLivenessDirectory::open(temporary.path().canonicalize().unwrap())
                    .expect("open process-liveness unit-test directory");
            let mut instance_id = [0x15; 16];
            instance_id[6] = 0x45;
            instance_id[8] = 0x95;
            let scope = directory
                .instance_scope(instance_id)
                .expect("create process-liveness unit-test scope");
            (temporary, scope)
        })
        .1
        .clone()
}

struct InstanceState {
    directory: Arc<File>,
    instance_id: [u8; 16],
    registrations: Mutex<BTreeMap<String, Registration>>,
    seals: Mutex<ScopeSeals>,
    #[cfg(test)]
    begin_tree_after_check: Mutex<Option<BeginTreeAfterCheckHook>>,
}

#[derive(Default)]
struct ScopeSeals {
    instance: bool,
    tasks: BTreeSet<[u8; 16]>,
}

type InstanceRegistry = HashMap<(DirectoryIdentityMarker, [u8; 16]), Weak<InstanceState>>;

fn lock_instance_registry() -> MutexGuard<'static, InstanceRegistry> {
    static REGISTRY: OnceLock<Mutex<InstanceRegistry>> = OnceLock::new();
    REGISTRY
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
#[derive(Clone)]
struct BeginTreeAfterCheckHook {
    entered: Arc<std::sync::Barrier>,
    release: Arc<std::sync::Barrier>,
}

#[derive(Clone, Copy)]
struct Registration {
    task_id: Option<[u8; 16]>,
}

/// An armed sentinel owned by exactly one supervised process tree.
///
/// The parent handle is retained through spawn setup, inherited by the child
/// tree, and then dropped in the parent. A committed sentinel is deliberately
/// not removed by `Drop`: cleanup requires a positive OS tree-exit proof.
pub(crate) struct ProcessLivenessSentinel {
    instance: Arc<InstanceState>,
    name: String,
    parent_file: Option<File>,
    spawn_committed: bool,
    completed: bool,
}

impl fmt::Debug for ProcessLivenessSentinel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProcessLivenessSentinel(<opaque>)")
    }
}

impl ProcessLivenessSentinel {
    #[cfg(unix)]
    pub(crate) fn raw_descriptor(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;

        self.parent_file
            .as_ref()
            .expect("an uncommitted sentinel retains its parent descriptor")
            .as_raw_fd()
    }

    #[cfg(windows)]
    pub(crate) fn make_parent_handle_inheritable(&self) -> Result<(), ProcessLivenessError> {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Foundation::{HANDLE, HANDLE_FLAG_INHERIT, SetHandleInformation};

        let handle = self
            .parent_file
            .as_ref()
            .expect("an uncommitted sentinel retains its parent handle")
            .as_raw_handle() as HANDLE;
        let succeeded =
            unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT) };
        if succeeded == 0 {
            Err(ProcessLivenessError::Unavailable)
        } else {
            Ok(())
        }
    }

    pub(crate) fn mark_spawned(&mut self) {
        self.spawn_committed = true;
        // Closing the parent copy while the global spawn lock is still held
        // prevents this inheritable handle from leaking into another spawn.
        // The child copy deliberately retains HANDLE_FLAG_INHERIT on Windows
        // so protocol descendants inherit the same unforgeable sentinel.
        self.parent_file.take();
    }

    pub(crate) fn try_complete_after_tree_exit(
        &mut self,
    ) -> Result<ProcessCleanupProof, ProcessLivenessError> {
        if self.completed {
            return Ok(ProcessCleanupProof::Confirmed);
        }
        #[cfg(windows)]
        if self.parent_file.is_some() {
            self.clear_parent_handle_inheritance()?;
        }
        self.parent_file.take();
        let proof = probe_named(&self.instance.directory, &self.name)?;
        if proof == ProcessCleanupProof::Confirmed {
            lock_registrations(&self.instance).remove(&self.name);
            self.completed = true;
        }
        self.spawn_committed = true;
        Ok(proof)
    }

    #[cfg(windows)]
    fn clear_parent_handle_inheritance(&self) -> Result<(), ProcessLivenessError> {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Foundation::{HANDLE, HANDLE_FLAG_INHERIT, SetHandleInformation};

        let handle = self
            .parent_file
            .as_ref()
            .expect("the parent sentinel handle is available")
            .as_raw_handle() as HANDLE;
        let succeeded = unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) };
        if succeeded == 0 {
            Err(ProcessLivenessError::Unavailable)
        } else {
            Ok(())
        }
    }
}

impl Drop for ProcessLivenessSentinel {
    fn drop(&mut self) {
        if self.spawn_committed {
            return;
        }
        #[cfg(windows)]
        let _ = self.clear_parent_handle_inheritance();
        if let Some(file) = self.parent_file.take() {
            let _ = remove_validated_file(&self.instance.directory, &self.name, file);
        }
        lock_registrations(&self.instance).remove(&self.name);
    }
}

fn create_sentinel(directory: &File, name: &str) -> io::Result<File> {
    let mut file = create_child_file_exclusive(directory, OsStr::new(name))?;
    let result = (|| {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            match file.try_lock() {
                Ok(()) => {}
                Err(std::fs::TryLockError::WouldBlock) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "process-liveness sentinel lock is held",
                    ));
                }
                Err(std::fs::TryLockError::Error(error)) => return Err(error),
            }
        }
        file.write_all(sentinel_contents(name).as_bytes())?;
        file.sync_all()
    })();
    if let Err(error) = result {
        let _ = remove_validated_file(directory, name, file);
        return Err(error);
    }
    Ok(file)
}

fn probe_named(directory: &File, name: &str) -> Result<ProcessCleanupProof, ProcessLivenessError> {
    let mut file = match open_child_file_for_exclusive_probe(directory, OsStr::new(name)) {
        Ok(file) => file,
        Err(error) if exclusive_open_is_held(&error) => return Ok(ProcessCleanupProof::Held),
        Err(_) => return Ok(ProcessCleanupProof::Unknown),
    };
    match file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => return Ok(ProcessCleanupProof::Held),
        Err(std::fs::TryLockError::Error(_)) => return Ok(ProcessCleanupProof::Unknown),
    }
    if !validate_sentinel_file(&mut file, name) {
        return Ok(ProcessCleanupProof::Unknown);
    }
    match remove_validated_file(directory, name, file) {
        Ok(true) => Ok(ProcessCleanupProof::Confirmed),
        Ok(false) | Err(_) => Ok(ProcessCleanupProof::Unknown),
    }
}

#[cfg(unix)]
fn probe_quarantined(
    directory: &File,
    quarantine_name: &str,
    original_name: &str,
) -> Result<ProcessCleanupProof, ProcessLivenessError> {
    let mut file = match open_child_file_for_exclusive_probe(directory, OsStr::new(quarantine_name))
    {
        Ok(file) => file,
        Err(error) if exclusive_open_is_held(&error) => return Ok(ProcessCleanupProof::Held),
        Err(_) => return Ok(ProcessCleanupProof::Unknown),
    };
    match file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => return Ok(ProcessCleanupProof::Held),
        Err(std::fs::TryLockError::Error(_)) => return Ok(ProcessCleanupProof::Unknown),
    }
    if !validate_sentinel_file(&mut file, original_name) {
        return Ok(ProcessCleanupProof::Unknown);
    }
    let removed = (|| -> io::Result<bool> {
        if !child_file_matches(directory, OsStr::new(quarantine_name), &file)? {
            return Ok(false);
        }
        remove_child_file(directory, OsStr::new(quarantine_name), &file)?;
        Ok(!child_entry_exists(directory, OsStr::new(original_name))?
            && !child_entry_exists(directory, OsStr::new(quarantine_name))?)
    })();
    match removed {
        Ok(true) => Ok(ProcessCleanupProof::Confirmed),
        Ok(false) | Err(_) => Ok(ProcessCleanupProof::Unknown),
    }
}

fn probe_registered(
    directory: &File,
    name: &str,
) -> Result<ProcessCleanupProof, ProcessLivenessError> {
    let file = match open_child_file_for_exclusive_probe(directory, OsStr::new(name)) {
        Ok(file) => file,
        Err(error) if exclusive_open_is_held(&error) => return Ok(ProcessCleanupProof::Held),
        Err(_) => return Ok(ProcessCleanupProof::Unknown),
    };
    match file.try_lock() {
        Err(std::fs::TryLockError::WouldBlock) => Ok(ProcessCleanupProof::Held),
        Err(std::fs::TryLockError::Error(_)) => Ok(ProcessCleanupProof::Unknown),
        Ok(()) => {
            // A local registration is retired only by the supervisor after its
            // OS-specific process-tree proof succeeds. Even an exclusive
            // sentinel probe cannot substitute for that proof.
            Ok(ProcessCleanupProof::Unknown)
        }
    }
}

fn validate_sentinel_file(file: &mut File, name: &str) -> bool {
    if ensure_plain_file(file).is_err() || !validate_private_file(file) {
        return false;
    }
    let Ok(metadata) = file.metadata() else {
        return false;
    };
    if metadata.len() > MAX_SENTINEL_BYTES {
        return false;
    }
    if file.seek(SeekFrom::Start(0)).is_err() {
        return false;
    }
    let mut contents = Vec::new();
    if file
        .take(MAX_SENTINEL_BYTES + 1)
        .read_to_end(&mut contents)
        .is_err()
    {
        return false;
    }
    contents == sentinel_contents(name).as_bytes()
}

fn remove_validated_file(directory: &File, name: &str, file: File) -> io::Result<bool> {
    if !child_file_matches(directory, OsStr::new(name), &file)? {
        return Ok(false);
    }
    #[cfg(unix)]
    return quarantine_and_remove_validated_file(directory, name, &file);
    #[cfg(windows)]
    {
        remove_child_file(directory, OsStr::new(name), &file)?;
        drop(file);
        Ok(!child_entry_exists(directory, OsStr::new(name))?)
    }
}

#[cfg(unix)]
fn quarantine_and_remove_validated_file(
    directory: &File,
    name: &str,
    file: &File,
) -> io::Result<bool> {
    for _ in 0..NONCE_ATTEMPTS {
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce)
            .map_err(|_| io::Error::other("process-liveness quarantine randomness failed"))?;
        let quarantine = format!(".cleanup-v1-{}-{name}", encode_hex(nonce));
        match quarantine_child_file_no_replace(directory, OsStr::new(name), OsStr::new(&quarantine))
        {
            Ok(()) => {
                // The no-replace rename first removes the predictable protocol
                // name from the namespace. Revalidating the unguessable
                // quarantine name against the already locked descriptor closes
                // the name-to-unlink substitution window. A mismatch is
                // preserved under quarantine and therefore fails closed.
                if !child_file_matches(directory, OsStr::new(&quarantine), file)? {
                    return Ok(false);
                }
                remove_child_file(directory, OsStr::new(&quarantine), file)?;
                return Ok(!child_entry_exists(directory, OsStr::new(name))?
                    && !child_entry_exists(directory, OsStr::new(&quarantine))?);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "process-liveness quarantine namespace is exhausted",
    ))
}

#[cfg(windows)]
fn make_private_windows_acl(file: &File) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use std::ptr::{null, null_mut};

    use windows_sys::Win32::Foundation::{ERROR_SUCCESS, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        BuildTrusteeWithSidW, EXPLICIT_ACCESS_W, SE_FILE_OBJECT, SET_ACCESS, SetEntriesInAclW,
        SetSecurityInfo, TRUSTEE_W,
    };
    use windows_sys::Win32::Security::{
        ACL, DACL_SECURITY_INFORMATION, OWNER_SECURITY_INFORMATION,
        PROTECTED_DACL_SECURITY_INFORMATION, SUB_CONTAINERS_AND_OBJECTS_INHERIT, TOKEN_USER,
    };
    use windows_sys::Win32::Storage::FileSystem::FILE_ALL_ACCESS;

    let user_buffer = current_user_token_buffer()?;
    let user = unsafe { &*user_buffer.as_ptr().cast::<TOKEN_USER>() };
    let mut trustee = TRUSTEE_W::default();
    unsafe { BuildTrusteeWithSidW(&mut trustee, user.User.Sid) };
    let access = EXPLICIT_ACCESS_W {
        grfAccessPermissions: FILE_ALL_ACCESS,
        grfAccessMode: SET_ACCESS,
        grfInheritance: SUB_CONTAINERS_AND_OBJECTS_INHERIT,
        Trustee: trustee,
    };
    let mut acl: *mut ACL = null_mut();
    let acl_status = unsafe { SetEntriesInAclW(1, &access, null(), &mut acl) };
    if acl_status != ERROR_SUCCESS {
        return Err(io::Error::from_raw_os_error(acl_status as i32));
    }
    let status = unsafe {
        SetSecurityInfo(
            file.as_raw_handle(),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION
                | DACL_SECURITY_INFORMATION
                | PROTECTED_DACL_SECURITY_INFORMATION,
            user.User.Sid,
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
}

#[cfg(windows)]
fn validate_private_windows_acl(file: &File) -> io::Result<()> {
    use std::ffi::c_void;
    use std::mem::{MaybeUninit, size_of};
    use std::os::windows::io::AsRawHandle;
    use std::ptr::null_mut;

    use windows_sys::Win32::Foundation::{ERROR_SUCCESS, HANDLE, HLOCAL, LocalFree};
    use windows_sys::Win32::Security::Authorization::{GetSecurityInfo, SE_FILE_OBJECT};
    use windows_sys::Win32::Security::{
        ACCESS_ALLOWED_ACE, ACE_HEADER, ACL, ACL_SIZE_INFORMATION, AclSizeInformation,
        DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetAclInformation,
        GetSecurityDescriptorControl, INHERIT_ONLY_ACE, INHERITED_ACE, IsValidSid,
        OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SE_DACL_PROTECTED, TOKEN_USER,
    };
    use windows_sys::Win32::System::SystemServices::ACCESS_ALLOWED_ACE_TYPE;

    struct SecurityDescriptor(PSECURITY_DESCRIPTOR);

    impl Drop for SecurityDescriptor {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe {
                    LocalFree(self.0 as HLOCAL);
                }
            }
        }
    }

    let mut owner: PSID = null_mut();
    let mut dacl: *mut ACL = null_mut();
    let mut descriptor: PSECURITY_DESCRIPTOR = null_mut();
    let status = unsafe {
        GetSecurityInfo(
            file.as_raw_handle() as HANDLE,
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
    let _descriptor = SecurityDescriptor(descriptor);
    if owner.is_null()
        || dacl.is_null()
        || descriptor.is_null()
        || unsafe { IsValidSid(owner) } == 0
    {
        return Err(invalid_private_windows_acl());
    }

    let mut control = 0u16;
    let mut revision = 0u32;
    if unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) } == 0
        || control & SE_DACL_PROTECTED == 0
    {
        return Err(invalid_private_windows_acl());
    }

    let user_buffer = current_user_token_buffer()?;
    let user = unsafe { &*user_buffer.as_ptr().cast::<TOKEN_USER>() };
    if unsafe { EqualSid(owner, user.User.Sid) } == 0 {
        return Err(invalid_private_windows_acl());
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
    let information = unsafe { information.assume_init() };
    if information.AceCount != 1 {
        return Err(invalid_private_windows_acl());
    }
    let mut raw_ace = null_mut::<c_void>();
    if unsafe { GetAce(dacl, 0, &mut raw_ace) } == 0 || raw_ace.is_null() {
        return Err(io::Error::last_os_error());
    }
    let header = unsafe { &*raw_ace.cast::<ACE_HEADER>() };
    if u32::from(header.AceType) != ACCESS_ALLOWED_ACE_TYPE
        || u32::from(header.AceFlags) & (INHERITED_ACE | INHERIT_ONLY_ACE) != 0
        || usize::from(header.AceSize) < size_of::<ACCESS_ALLOWED_ACE>()
    {
        return Err(invalid_private_windows_acl());
    }
    let allowed = unsafe { &*raw_ace.cast::<ACCESS_ALLOWED_ACE>() };
    let sid = (&raw const allowed.SidStart).cast_mut().cast::<c_void>();
    if unsafe { IsValidSid(sid) } == 0 || unsafe { EqualSid(sid, user.User.Sid) } == 0 {
        return Err(invalid_private_windows_acl());
    }
    Ok(())
}

#[cfg(windows)]
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

#[cfg(windows)]
fn invalid_private_windows_acl() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "process-liveness object ACL is not restricted to the current owner",
    )
}

#[cfg(windows)]
fn validate_private_directory(directory: &File) -> io::Result<()> {
    validate_private_windows_acl(directory)
}

#[cfg(unix)]
fn validate_private_directory(directory: &File) -> io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let metadata = directory.metadata()?;
    if metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "process-liveness directory is not private",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn validate_private_file(file: &File) -> bool {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let Ok(metadata) = file.metadata() else {
        return false;
    };
    metadata.uid() == unsafe { libc::geteuid() }
        && metadata.permissions().mode() & 0o777 == 0o600
        && metadata.nlink() == 1
}

#[cfg(windows)]
fn validate_private_file(file: &File) -> bool {
    use std::mem::MaybeUninit;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, FILE_ATTRIBUTE_REPARSE_POINT, GetFileInformationByHandle,
    };

    let mut information = MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::zeroed();
    let succeeded = unsafe {
        GetFileInformationByHandle(file.as_raw_handle() as HANDLE, information.as_mut_ptr())
    };
    if succeeded == 0 {
        return false;
    }
    let information = unsafe { information.assume_init() };
    information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT == 0
        && information.nNumberOfLinks == 1
}

#[cfg(unix)]
fn exclusive_open_is_held(_: &io::Error) -> bool {
    false
}

#[cfg(windows)]
fn exclusive_open_is_held(error: &io::Error) -> bool {
    const ERROR_SHARING_VIOLATION: i32 = 32;

    error.raw_os_error() == Some(ERROR_SHARING_VIOLATION)
}

fn validate_identity(identity: [u8; 16]) -> Result<(), ProcessLivenessError> {
    if identity[6] >> 4 == 4 && identity[8] & 0xc0 == 0x80 {
        Ok(())
    } else {
        Err(ProcessLivenessError::InvalidIdentity)
    }
}

fn sentinel_name(instance_id: [u8; 16], task_id: Option<[u8; 16]>, nonce: [u8; 16]) -> String {
    let task_id = task_id.map_or_else(|| "none".to_owned(), encode_hex);
    format!(
        "v1-i-{}-t-{task_id}-n-{}.sentinel",
        encode_hex(instance_id),
        encode_hex(nonce)
    )
}

fn sentinel_contents(name: &str) -> String {
    format!("{CONTENT_MAGIC}\n{name}\n")
}

fn parse_sentinel_name(name: &str) -> Option<()> {
    let body = name.strip_prefix("v1-i-")?.strip_suffix(".sentinel")?;
    let (instance, task_and_nonce) = body.split_once("-t-")?;
    let (task, nonce) = task_and_nonce.split_once("-n-")?;
    if instance.len() != 32
        || (task != "none" && task.len() != 32)
        || nonce.len() != 32
        || !is_lower_hex(instance)
        || (task != "none" && !is_lower_hex(task))
        || !is_lower_hex(nonce)
    {
        return None;
    }
    validate_identity(decode_hex(instance)?).ok()?;
    if task != "none" {
        validate_identity(decode_hex(task)?).ok()?;
    }
    decode_hex(nonce)?;
    Some(())
}

#[cfg(unix)]
fn parse_cleanup_name(name: &str) -> Option<&str> {
    let body = name.strip_prefix(".cleanup-v1-")?;
    let (nonce, original_name) = body.split_once('-')?;
    if nonce.len() != 32 || !is_lower_hex(nonce) {
        return None;
    }
    decode_hex(nonce)?;
    parse_sentinel_name(original_name)?;
    Some(original_name)
}

fn encode_hex(bytes: [u8; 16]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(32);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn decode_hex(value: &str) -> Option<[u8; 16]> {
    if value.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (decode_nibble(chunk[0])? << 4) | decode_nibble(chunk[1])?;
    }
    Some(bytes)
}

fn decode_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn is_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn combine_proof(left: ProcessCleanupProof, right: ProcessCleanupProof) -> ProcessCleanupProof {
    match (left, right) {
        (ProcessCleanupProof::Unknown, _) | (_, ProcessCleanupProof::Unknown) => {
            ProcessCleanupProof::Unknown
        }
        (ProcessCleanupProof::Held, _) | (_, ProcessCleanupProof::Held) => {
            ProcessCleanupProof::Held
        }
        _ => ProcessCleanupProof::Confirmed,
    }
}

fn lock_registrations(instance: &InstanceState) -> MutexGuard<'_, BTreeMap<String, Registration>> {
    instance
        .registrations
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn lock_scope_seals(instance: &InstanceState) -> MutexGuard<'_, ScopeSeals> {
    instance
        .seals
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier, mpsc};
    use std::thread;
    use std::time::Duration;

    use super::{
        BeginTreeAfterCheckHook, ProcessCleanupProof, ProcessLivenessDirectory,
        ProcessLivenessError,
    };

    #[test]
    fn sealed_cleanup_scope_binds_only_the_exact_worker_scope() {
        let runtime = tempfile::tempdir().expect("create process-liveness runtime");
        let directory = ProcessLivenessDirectory::open(runtime.path().canonicalize().unwrap())
            .expect("open process-liveness runtime");
        let mut instance_id = [0x21; 16];
        instance_id[6] = 0x41;
        instance_id[8] = 0x81;
        let instance = directory
            .instance_scope(instance_id)
            .expect("derive process-liveness instance");
        let mut worker_id = [0x32; 16];
        worker_id[6] = 0x42;
        worker_id[8] = 0x82;
        let mut cleanup_id = [0x43; 16];
        cleanup_id[6] = 0x43;
        cleanup_id[8] = 0x83;
        let worker = instance.task_scope(worker_id).expect("derive worker scope");
        let same_worker = instance
            .task_scope(worker_id)
            .expect("rederive worker scope");
        let cleanup = instance
            .task_scope(cleanup_id)
            .expect("derive cleanup command scope");
        let cleanup_from_worker = worker
            .sibling_task_scope(cleanup_id)
            .expect("derive cleanup sibling from worker scope");
        let sealed = worker
            .seal_task_scope(worker_id)
            .expect("seal exact worker scope");

        assert!(worker.is_same_scope(&same_worker));
        assert!(sealed.is_bound_to(&same_worker));
        assert!(!worker.is_same_scope(&cleanup));
        assert!(worker.is_same_instance(&cleanup_from_worker));
        assert!(cleanup.is_same_scope(&cleanup_from_worker));
        assert!(worker.sibling_task_scope(worker_id).is_err());
        assert!(!sealed.is_bound_to(&cleanup));
        assert!(!sealed.is_bound_to(&instance));
    }

    #[test]
    fn task_scope_seal_linearizes_after_in_flight_registration_and_rejects_late_begins() {
        let runtime = tempfile::tempdir().expect("create process-liveness runtime");
        let directory = ProcessLivenessDirectory::open(runtime.path().canonicalize().unwrap())
            .expect("open process-liveness runtime");
        let mut instance_id = [0x31; 16];
        instance_id[6] = 0x41;
        instance_id[8] = 0x81;
        let instance = directory
            .instance_scope(instance_id)
            .expect("derive process-liveness instance");
        let mut task_id = [0x42; 16];
        task_id[6] = 0x42;
        task_id[8] = 0x82;
        let scope = instance
            .task_scope(task_id)
            .expect("derive task process-liveness scope");
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        *scope
            .instance
            .begin_tree_after_check
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(BeginTreeAfterCheckHook {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        });

        let begin_scope = scope.clone();
        let (sentinel_sender, sentinel_receiver) = mpsc::channel();
        let begin_thread = thread::spawn(move || {
            sentinel_sender
                .send(begin_scope.begin_tree())
                .expect("publish begin_tree result");
        });
        entered.wait();

        let seal_scope = scope.clone();
        let (seal_started_sender, seal_started_receiver) = mpsc::channel();
        let (seal_sender, seal_receiver) = mpsc::channel();
        let seal_thread = thread::spawn(move || {
            seal_started_sender
                .send(())
                .expect("publish seal attempt start");
            let sealed = seal_scope
                .seal_task_scope(task_id)
                .expect("seal exact task scope");
            let proof = sealed.cleanup_proof();
            seal_sender
                .send((sealed, proof))
                .expect("publish sealed cleanup proof");
        });
        seal_started_receiver
            .recv()
            .expect("observe seal attempt start");
        assert!(
            matches!(
                seal_receiver.recv_timeout(Duration::from_millis(100)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "seal must not pass an in-flight begin_tree check/create/registration gate"
        );

        release.wait();
        let sentinel = sentinel_receiver
            .recv()
            .expect("receive begin_tree result")
            .expect("in-flight begin_tree completes registration");
        let (sealed, first_proof) = seal_receiver
            .recv()
            .expect("seal completes after registration");
        assert_eq!(
            first_proof.expect("probe registered sentinel"),
            ProcessCleanupProof::Held
        );

        drop(sentinel);
        assert_eq!(
            sealed.cleanup_proof().expect("probe after sentinel drop"),
            ProcessCleanupProof::Confirmed
        );
        assert!(matches!(
            scope.begin_tree(),
            Err(ProcessLivenessError::ScopeSealed)
        ));

        begin_thread.join().expect("join begin_tree thread");
        seal_thread.join().expect("join seal thread");
    }

    #[test]
    fn instance_scope_seal_linearizes_after_in_flight_registration_and_rejects_all_late_begins() {
        let runtime = tempfile::tempdir().expect("create process-liveness runtime");
        let runtime_path = runtime.path().canonicalize().unwrap();
        let directory =
            ProcessLivenessDirectory::open(&runtime_path).expect("open process-liveness runtime");
        let reopened =
            ProcessLivenessDirectory::open(runtime_path).expect("reopen process-liveness runtime");
        let mut instance_id = [0x71; 16];
        instance_id[6] = 0x41;
        instance_id[8] = 0x81;
        let instance = directory
            .instance_scope(instance_id)
            .expect("derive process-liveness instance");
        let mut task_id = [0x82; 16];
        task_id[6] = 0x42;
        task_id[8] = 0x82;
        let task = instance
            .task_scope(task_id)
            .expect("derive task process-liveness scope");
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        *instance
            .instance
            .begin_tree_after_check
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(BeginTreeAfterCheckHook {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        });

        let begin_scope = task.clone();
        let (sentinel_sender, sentinel_receiver) = mpsc::channel();
        let begin_thread = thread::spawn(move || {
            sentinel_sender
                .send(begin_scope.begin_tree())
                .expect("publish begin_tree result");
        });
        entered.wait();

        let seal_scope = instance.clone();
        let (seal_started_sender, seal_started_receiver) = mpsc::channel();
        let (seal_sender, seal_receiver) = mpsc::channel();
        let seal_thread = thread::spawn(move || {
            seal_started_sender
                .send(())
                .expect("publish instance seal attempt start");
            let sealed = seal_scope
                .seal_instance_scope()
                .expect("seal exact instance scope");
            let proof = sealed.cleanup_proof();
            seal_sender
                .send((sealed, proof))
                .expect("publish sealed instance cleanup proof");
        });
        seal_started_receiver
            .recv()
            .expect("observe instance seal attempt start");
        assert!(
            matches!(
                seal_receiver.recv_timeout(Duration::from_millis(100)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "instance seal must not pass an in-flight begin_tree registration gate"
        );

        release.wait();
        let sentinel = sentinel_receiver
            .recv()
            .expect("receive begin_tree result")
            .expect("in-flight task begin_tree completes registration");
        let (sealed, first_proof) = seal_receiver
            .recv()
            .expect("instance seal completes after registration");
        assert_eq!(
            first_proof.expect("probe registered task sentinel"),
            ProcessCleanupProof::Held
        );

        assert!(matches!(
            instance.begin_tree(),
            Err(ProcessLivenessError::ScopeSealed)
        ));
        assert!(matches!(
            task.begin_tree(),
            Err(ProcessLivenessError::ScopeSealed)
        ));
        let mut other_task_id = [0x93; 16];
        other_task_id[6] = 0x43;
        other_task_id[8] = 0x83;
        let reopened_task = reopened
            .instance_scope(instance_id)
            .expect("derive reopened instance scope")
            .task_scope(other_task_id)
            .expect("derive reopened task scope");
        assert!(matches!(
            reopened_task.begin_tree(),
            Err(ProcessLivenessError::ScopeSealed)
        ));

        drop(sentinel);
        assert_eq!(
            sealed.cleanup_proof().expect("probe after sentinel drop"),
            ProcessCleanupProof::Confirmed
        );

        begin_thread.join().expect("join begin_tree thread");
        seal_thread.join().expect("join instance seal thread");
    }

    #[test]
    fn repeated_instance_scope_for_the_same_directory_shares_registration_and_seal_state() {
        let runtime = tempfile::tempdir().expect("create shared-instance runtime");
        let runtime_path = runtime.path().canonicalize().unwrap();
        let directory =
            ProcessLivenessDirectory::open(&runtime_path).expect("open process-liveness runtime");
        let reopened = ProcessLivenessDirectory::open(runtime_path)
            .expect("reopen the same process-liveness runtime");
        let mut instance_id = [0x51; 16];
        instance_id[6] = 0x41;
        instance_id[8] = 0x81;
        let mut task_id = [0x62; 16];
        task_id[6] = 0x42;
        task_id[8] = 0x82;
        let mut other_task_id = [0x73; 16];
        other_task_id[6] = 0x43;
        other_task_id[8] = 0x83;
        let scope_a = directory
            .instance_scope(instance_id)
            .expect("derive scope A")
            .task_scope(task_id)
            .expect("derive task scope A");
        let scope_b = directory
            .clone()
            .instance_scope(instance_id)
            .expect("derive scope B from a cloned directory")
            .task_scope(task_id)
            .expect("derive task scope B");
        let scope_c = reopened
            .instance_scope(instance_id)
            .expect("derive scope C from a reopened directory")
            .task_scope(task_id)
            .expect("derive task scope C");
        assert!(matches!(
            scope_a.task_scope(other_task_id),
            Err(ProcessLivenessError::InvalidIdentity)
        ));

        let held = scope_b.begin_tree().expect("scope B holds a process tree");
        assert_eq!(
            scope_a
                .cleanup_proof()
                .expect("scope A probes scope B tree"),
            ProcessCleanupProof::Held
        );
        let sealed = scope_a
            .seal_task_scope(task_id)
            .expect("scope A seals the shared task scope");
        assert!(matches!(
            scope_b.begin_tree(),
            Err(ProcessLivenessError::ScopeSealed)
        ));
        assert!(matches!(
            scope_c.begin_tree(),
            Err(ProcessLivenessError::ScopeSealed)
        ));

        drop(held);
        assert_eq!(
            sealed.cleanup_proof().expect("probe after held tree drops"),
            ProcessCleanupProof::Confirmed
        );
    }

    #[test]
    fn sealed_instance_scope_aggregates_instance_and_multiple_task_trees() {
        let runtime = tempfile::tempdir().expect("create aggregate instance fixture");
        let directory = ProcessLivenessDirectory::open(runtime.path().canonicalize().unwrap())
            .expect("open process-liveness runtime");
        let instance = directory
            .instance_scope({
                let mut id = [0xa1; 16];
                id[6] = 0x41;
                id[8] = 0x81;
                id
            })
            .expect("derive aggregate instance scope");
        let task_one = instance
            .task_scope({
                let mut id = [0xb2; 16];
                id[6] = 0x42;
                id[8] = 0x82;
                id
            })
            .expect("derive first task scope");
        let task_two = instance
            .task_scope({
                let mut id = [0xc3; 16];
                id[6] = 0x43;
                id[8] = 0x83;
                id
            })
            .expect("derive second task scope");

        let instance_tree = instance.begin_tree().expect("hold instance tree");
        let first_task_tree = task_one.begin_tree().expect("hold first task tree");
        let second_task_tree = task_two.begin_tree().expect("hold second task tree");
        let sealed = instance
            .seal_instance_scope()
            .expect("seal aggregate instance scope");

        assert_eq!(
            sealed.cleanup_proof().expect("probe all held trees"),
            ProcessCleanupProof::Held
        );
        drop(instance_tree);
        assert_eq!(
            sealed.cleanup_proof().expect("probe two task trees"),
            ProcessCleanupProof::Held
        );
        drop(first_task_tree);
        assert_eq!(
            sealed.cleanup_proof().expect("probe final task tree"),
            ProcessCleanupProof::Held
        );
        drop(second_task_tree);
        assert_eq!(
            sealed
                .cleanup_proof()
                .expect("probe fully released instance"),
            ProcessCleanupProof::Confirmed
        );
    }
}
