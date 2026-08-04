use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coding_agent_domain::RepositoryId;
use coding_agent_runtime::{
    RootCapability, VolumeIdentity, VolumeSample, VolumeSampleError, VolumeSampler,
};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::scheduler::{
    RepositoryCoordinationKey, SchedulerRepositoryStorageState, SchedulerStorageNotification,
    SchedulerStorageNotificationSink,
};
use crate::storage_policy::{
    StorageObservation, StoragePolicy, StoragePolicyError, StorageScope, StorageScopeBinding,
    StorageScopeHysteresis, StorageState, aggregate_storage_state,
};

pub const STORAGE_SAMPLE_FRESHNESS: Duration = Duration::from_secs(5);
pub const STORAGE_SAMPLE_INTERVAL: Duration = Duration::from_secs(5);
pub const STORAGE_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

pub trait StorageMonitorClock: Send + Sync {
    fn now(&self) -> Duration;
}

#[derive(Debug, Clone)]
pub struct TokioStorageMonitorClock {
    origin: Instant,
}

impl TokioStorageMonitorClock {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for TokioStorageMonitorClock {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageMonitorClock for TokioStorageMonitorClock {
    fn now(&self) -> Duration {
        Instant::now().duration_since(self.origin)
    }
}

#[derive(Clone)]
pub struct StorageProbeTarget {
    expected_volume: VolumeIdentity,
    root: Arc<RootCapability>,
}

impl StorageProbeTarget {
    pub fn new(expected_volume: VolumeIdentity, root: Arc<RootCapability>) -> Self {
        Self {
            expected_volume,
            root,
        }
    }

    pub const fn expected_volume(&self) -> VolumeIdentity {
        self.expected_volume
    }

    pub fn root(&self) -> &RootCapability {
        &self.root
    }
}

impl PartialEq for StorageProbeTarget {
    fn eq(&self, other: &Self) -> bool {
        self.expected_volume == other.expected_volume
    }
}

impl Eq for StorageProbeTarget {}

impl std::hash::Hash for StorageProbeTarget {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.expected_volume, state);
    }
}

impl fmt::Debug for StorageProbeTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageProbeTarget")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MonitoredStorageScope {
    Data,
    Runtime,
    RepositoryGit(RepositoryId),
}

impl MonitoredStorageScope {
    const fn policy_scope(self) -> StorageScope {
        match self {
            Self::Data => StorageScope::Data,
            Self::Runtime => StorageScope::Runtime,
            Self::RepositoryGit(_) => StorageScope::RepositoryGit,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct MonitoredStorageScopeBinding {
    scope: MonitoredStorageScope,
    target: StorageProbeTarget,
}

impl MonitoredStorageScopeBinding {
    pub fn data(target: StorageProbeTarget) -> Self {
        Self {
            scope: MonitoredStorageScope::Data,
            target,
        }
    }

    pub fn runtime(target: StorageProbeTarget) -> Self {
        Self {
            scope: MonitoredStorageScope::Runtime,
            target,
        }
    }

    pub fn repository_git(repository_id: RepositoryId, target: StorageProbeTarget) -> Self {
        Self {
            scope: MonitoredStorageScope::RepositoryGit(repository_id),
            target,
        }
    }

    pub const fn scope(&self) -> MonitoredStorageScope {
        self.scope
    }

    pub fn target(&self) -> &StorageProbeTarget {
        &self.target
    }

    pub fn policy_binding(&self) -> StorageScopeBinding {
        StorageScopeBinding::new(self.scope.policy_scope(), self.target.expected_volume)
    }
}

impl fmt::Debug for MonitoredStorageScopeBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MonitoredStorageScopeBinding")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct StorageCriticalNotification {
    scopes: Vec<MonitoredStorageScope>,
}

impl StorageCriticalNotification {
    pub fn new(mut scopes: Vec<MonitoredStorageScope>) -> Self {
        scopes.sort_unstable_by_key(scope_sort_key);
        scopes.dedup();
        Self { scopes }
    }

    pub fn scopes(&self) -> &[MonitoredStorageScope] {
        &self.scopes
    }
}

impl fmt::Debug for StorageCriticalNotification {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageCriticalNotification")
            .field("scopes", &self.scopes)
            .finish()
    }
}

fn scope_sort_key(scope: &MonitoredStorageScope) -> (u8, Option<uuid::Uuid>) {
    match scope {
        MonitoredStorageScope::Data => (0, None),
        MonitoredStorageScope::Runtime => (1, None),
        MonitoredStorageScope::RepositoryGit(repository_id) => (2, Some(repository_id.as_uuid())),
    }
}

fn authenticated_target_key(
    target: &StorageProbeTarget,
) -> Result<RepositoryCoordinationKey, StorageMonitorError> {
    target
        .root
        .identity_marker()
        .map(RepositoryCoordinationKey::from_authenticated_marker)
        .map_err(|_| StorageMonitorError::Unavailable)
}

fn same_authenticated_target(
    left_key: RepositoryCoordinationKey,
    left: &StorageProbeTarget,
    right_key: RepositoryCoordinationKey,
    right: &StorageProbeTarget,
) -> Result<bool, StorageMonitorError> {
    let left_identity = left
        .root
        .identity_marker()
        .map_err(|_| StorageMonitorError::Unavailable)?;
    let right_identity = right
        .root
        .identity_marker()
        .map_err(|_| StorageMonitorError::Unavailable)?;
    Ok(left_key == right_key
        && left.expected_volume == right.expected_volume
        && left_identity == right_identity)
}

pub trait StorageCriticalNotificationSink: Send + Sync {
    fn notify_storage_critical(&self, notification: StorageCriticalNotification);
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageActivity {
    queued_tasks: u32,
    active_tasks: u32,
}

impl StorageActivity {
    pub const fn new(queued_tasks: u32, active_tasks: u32) -> Self {
        Self {
            queued_tasks,
            active_tasks,
        }
    }

    pub const fn queued_tasks(self) -> u32 {
        self.queued_tasks
    }

    pub const fn active_tasks(self) -> u32 {
        self.active_tasks
    }

    pub const fn requires_periodic_sampling(self) -> bool {
        self.queued_tasks > 0 || self.active_tasks > 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonitoredRepositoryStorageState {
    repository_id: RepositoryId,
    state: Option<StorageState>,
}

impl MonitoredRepositoryStorageState {
    const fn uninitialized(repository_id: RepositoryId) -> Self {
        Self {
            repository_id,
            state: None,
        }
    }

    pub const fn repository_id(&self) -> RepositoryId {
        self.repository_id
    }

    pub const fn state(&self) -> Option<StorageState> {
        self.state
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMonitorSnapshot {
    state: Option<StorageState>,
    data_state: Option<StorageState>,
    runtime_state: Option<StorageState>,
    repositories: Vec<MonitoredRepositoryStorageState>,
}

impl StorageMonitorSnapshot {
    fn initial(bindings: &[MonitoredStorageScopeBinding]) -> Self {
        let mut repositories = bindings
            .iter()
            .filter_map(|binding| match binding.scope {
                MonitoredStorageScope::RepositoryGit(repository_id) => Some(
                    MonitoredRepositoryStorageState::uninitialized(repository_id),
                ),
                MonitoredStorageScope::Data | MonitoredStorageScope::Runtime => None,
            })
            .collect::<Vec<_>>();
        repositories.sort_unstable_by_key(|repository| repository.repository_id.as_uuid());
        Self {
            state: None,
            data_state: None,
            runtime_state: None,
            repositories,
        }
    }

    pub const fn state(&self) -> Option<StorageState> {
        self.state
    }

    pub const fn data_state(&self) -> Option<StorageState> {
        self.data_state
    }

    pub const fn runtime_state(&self) -> Option<StorageState> {
        self.runtime_state
    }

    pub fn repositories(&self) -> &[MonitoredRepositoryStorageState] {
        &self.repositories
    }

    pub fn repository_state(&self, repository_id: RepositoryId) -> Option<StorageState> {
        self.repositories()
            .iter()
            .find(|repository| repository.repository_id == repository_id)
            .and_then(MonitoredRepositoryStorageState::state)
    }

    /// Fail-closed readiness for the complete, globally aggregated snapshot.
    ///
    /// Candidate scheduling must use [`Self::blocks_admission_for`] so an
    /// unrelated repository does not block another coordination key.
    pub fn blocks_admission(&self) -> bool {
        match (self.is_complete(), self.state) {
            (false, _) => true,
            (true, Some(state)) => state.blocks_admission(),
            (true, None) => true,
        }
    }

    pub fn blocks_admission_for(&self, repository_id: RepositoryId) -> bool {
        self.data_state.is_none_or(StorageState::blocks_admission)
            || self
                .runtime_state
                .is_none_or(StorageState::blocks_admission)
            || self
                .repository_state(repository_id)
                .is_none_or(StorageState::blocks_admission)
    }

    pub fn is_complete(&self) -> bool {
        self.state.is_some()
            && self.data_state.is_some()
            && self.runtime_state.is_some()
            && self
                .repositories
                .iter()
                .all(|repository| repository.state.is_some())
    }

    fn scheduler_notification(&self) -> Option<SchedulerStorageNotification> {
        let state = self.state?;
        let data_state = self.data_state?;
        let runtime_state = self.runtime_state?;
        let repositories = self
            .repositories
            .iter()
            .map(|repository| {
                Some(SchedulerRepositoryStorageState::new(
                    repository.repository_id,
                    repository.state?,
                ))
            })
            .collect::<Option<Vec<_>>>()?;
        Some(SchedulerStorageNotification::new(
            state,
            data_state,
            runtime_state,
            repositories,
        ))
    }
}

pub struct StorageMonitorConfig {
    policy: StoragePolicy,
    sampler: Arc<dyn VolumeSampler>,
    clock: Arc<dyn StorageMonitorClock>,
    scheduler_notifications: Arc<dyn SchedulerStorageNotificationSink>,
    critical_notifications: Arc<dyn StorageCriticalNotificationSink>,
}

impl StorageMonitorConfig {
    pub fn new(
        policy: StoragePolicy,
        sampler: Arc<dyn VolumeSampler>,
        clock: Arc<dyn StorageMonitorClock>,
        scheduler_notifications: Arc<dyn SchedulerStorageNotificationSink>,
        critical_notifications: Arc<dyn StorageCriticalNotificationSink>,
    ) -> Self {
        Self {
            policy,
            sampler,
            clock,
            scheduler_notifications,
            critical_notifications,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StorageMonitorError {
    #[error(transparent)]
    Policy(#[from] StoragePolicyError),
    #[error("storage monitor is unavailable")]
    Unavailable,
    #[error("storage monitor requires exactly one data scope")]
    InvalidDataScope,
    #[error("storage monitor requires exactly one runtime scope")]
    InvalidRuntimeScope,
    #[error("storage monitor contains a duplicate repository scope")]
    DuplicateRepositoryScope,
    #[error("storage monitor does not contain the requested repository scope")]
    UnknownRepositoryScope,
    #[error("storage monitor probe sequence is exhausted")]
    ProbeSequenceExhausted,
}

#[derive(Clone)]
pub struct StorageMonitorHandle {
    command_sender: mpsc::Sender<StorageMonitorCommand>,
    snapshot: Arc<Mutex<StorageMonitorSnapshot>>,
    #[cfg(all(test, feature = "test-support"))]
    registration_ack_pause: Arc<Mutex<Option<Arc<tokio::sync::Notify>>>>,
}

#[cfg(all(test, feature = "test-support"))]
pub(crate) struct StorageRegistrationAckPause {
    release: Arc<tokio::sync::Notify>,
}

#[cfg(all(test, feature = "test-support"))]
impl StorageRegistrationAckPause {
    pub(crate) fn release(&self) {
        self.release.notify_one();
    }
}

impl StorageMonitorHandle {
    pub fn spawn(
        config: StorageMonitorConfig,
        bindings: Vec<MonitoredStorageScopeBinding>,
    ) -> Result<Self, StorageMonitorError> {
        let snapshot = Arc::new(Mutex::new(StorageMonitorSnapshot::initial(&bindings)));
        #[cfg(all(test, feature = "test-support"))]
        let registration_ack_pause = Arc::new(Mutex::new(None));
        let (command_sender, command_receiver) = mpsc::channel(128);
        let actor = StorageMonitorActor::try_new(
            config,
            bindings,
            snapshot.clone(),
            command_sender.downgrade(),
            #[cfg(all(test, feature = "test-support"))]
            registration_ack_pause.clone(),
        )?;
        tokio::spawn(actor.run(command_receiver));
        Ok(Self {
            command_sender,
            snapshot,
            #[cfg(all(test, feature = "test-support"))]
            registration_ack_pause,
        })
    }

    #[cfg(all(test, feature = "test-support"))]
    pub(crate) fn pause_next_repository_registration_ack_for_test(
        &self,
    ) -> StorageRegistrationAckPause {
        let release = Arc::new(tokio::sync::Notify::new());
        let mut pause = self
            .registration_ack_pause
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            pause.is_none(),
            "only one registration ack pause may be armed"
        );
        *pause = Some(release.clone());
        StorageRegistrationAckPause { release }
    }

    pub async fn set_activity(&self, activity: StorageActivity) -> Result<(), StorageMonitorError> {
        let (response_sender, response_receiver) = oneshot::channel();
        self.command_sender
            .send(StorageMonitorCommand::SetActivity {
                activity,
                response: response_sender,
            })
            .await
            .map_err(|_| StorageMonitorError::Unavailable)?;
        response_receiver
            .await
            .map_err(|_| StorageMonitorError::Unavailable)?
    }

    /// Monotonically attaches one durable repository to its authenticated
    /// common-Git volume. The actor applies the binding before acknowledging
    /// the command, making a lost reply safe to retry.
    pub async fn register_repository_scope(
        &self,
        repository_id: RepositoryId,
        coordination_key: RepositoryCoordinationKey,
        target: StorageProbeTarget,
    ) -> Result<(), StorageMonitorError> {
        let (response_sender, response_receiver) = oneshot::channel();
        self.command_sender
            .send(StorageMonitorCommand::RegisterRepositoryScope {
                repository_id,
                coordination_key,
                target,
                response: response_sender,
            })
            .await
            .map_err(|_| StorageMonitorError::Unavailable)?;
        response_receiver
            .await
            .map_err(|_| StorageMonitorError::Unavailable)?
    }

    /// Refreshes every registered logical scope for bootstrap/global projection.
    pub async fn refresh_for_admission(
        &self,
        active_task_count: u32,
    ) -> Result<StorageMonitorSnapshot, StorageMonitorError> {
        let (response_sender, response_receiver) = oneshot::channel();
        self.command_sender
            .send(StorageMonitorCommand::RefreshForAdmission {
                active_task_count,
                repository_id: None,
                response: response_sender,
            })
            .await
            .map_err(|_| StorageMonitorError::Unavailable)?;
        response_receiver
            .await
            .map_err(|_| StorageMonitorError::Unavailable)?
    }

    /// Refreshes only data, runtime, and the opaque candidate repository scope.
    pub async fn refresh_for_repository_admission(
        &self,
        active_task_count: u32,
        repository_id: RepositoryId,
    ) -> Result<StorageMonitorSnapshot, StorageMonitorError> {
        let (response_sender, response_receiver) = oneshot::channel();
        self.command_sender
            .send(StorageMonitorCommand::RefreshForAdmission {
                active_task_count,
                repository_id: Some(repository_id),
                response: response_sender,
            })
            .await
            .map_err(|_| StorageMonitorError::Unavailable)?;
        response_receiver
            .await
            .map_err(|_| StorageMonitorError::Unavailable)?
    }

    pub fn current_snapshot(&self) -> StorageMonitorSnapshot {
        self.snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl fmt::Debug for StorageMonitorHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageMonitorHandle")
            .finish_non_exhaustive()
    }
}

enum StorageMonitorCommand {
    RegisterRepositoryScope {
        repository_id: RepositoryId,
        coordination_key: RepositoryCoordinationKey,
        target: StorageProbeTarget,
        response: oneshot::Sender<Result<(), StorageMonitorError>>,
    },
    SetActivity {
        activity: StorageActivity,
        response: oneshot::Sender<Result<(), StorageMonitorError>>,
    },
    RefreshForAdmission {
        active_task_count: u32,
        repository_id: Option<RepositoryId>,
        response: oneshot::Sender<Result<StorageMonitorSnapshot, StorageMonitorError>>,
    },
    ProbeCompleted {
        volume: VolumeIdentity,
        probe_id: u64,
        result: Result<VolumeSample, VolumeSampleError>,
    },
    ProbeTimedOut {
        volume: VolumeIdentity,
        probe_id: u64,
    },
    ProbeExited {
        volume: VolumeIdentity,
        probe_id: u64,
    },
}

struct StorageMonitorActor {
    policy: StoragePolicy,
    sampler: Arc<dyn VolumeSampler>,
    clock: Arc<dyn StorageMonitorClock>,
    scheduler_notifications: Arc<dyn SchedulerStorageNotificationSink>,
    critical_notifications: Arc<dyn StorageCriticalNotificationSink>,
    command_sender: mpsc::WeakSender<StorageMonitorCommand>,
    snapshot: Arc<Mutex<StorageMonitorSnapshot>>,
    logical_scopes: Vec<LogicalScopeRuntime>,
    volumes: HashMap<VolumeIdentity, VolumeRuntime>,
    volume_order: Vec<VolumeIdentity>,
    activity: StorageActivity,
    active_task_count: u32,
    next_periodic_deadline: Option<Instant>,
    next_probe_id: u64,
    refresh_waiters: Vec<RefreshWaiter>,
    last_published: Option<SchedulerStorageNotification>,
    #[cfg(all(test, feature = "test-support"))]
    registration_ack_pause: Arc<Mutex<Option<Arc<tokio::sync::Notify>>>>,
}

struct LogicalScopeRuntime {
    scope: MonitoredStorageScope,
    coordination_key: Option<RepositoryCoordinationKey>,
    target: StorageProbeTarget,
    hysteresis: StorageScopeHysteresis,
}

struct VolumeRuntime {
    target: StorageProbeTarget,
    latest: Option<VolumeObservationRecord>,
    in_flight: Option<ProbeFlight>,
}

impl VolumeRuntime {
    fn new(target: StorageProbeTarget) -> Self {
        Self {
            target,
            latest: None,
            in_flight: None,
        }
    }
}

#[derive(Clone, Copy)]
struct VolumeObservationRecord {
    observation: StorageObservation,
    observed_at: Duration,
}

#[derive(Clone, Copy)]
struct ProbeFlight {
    id: u64,
    timed_out: bool,
}

struct RefreshWaiter {
    required_volumes: HashSet<VolumeIdentity>,
    pending_volumes: HashSet<VolumeIdentity>,
    response: oneshot::Sender<Result<StorageMonitorSnapshot, StorageMonitorError>>,
}

impl StorageMonitorActor {
    fn try_new(
        config: StorageMonitorConfig,
        bindings: Vec<MonitoredStorageScopeBinding>,
        snapshot: Arc<Mutex<StorageMonitorSnapshot>>,
        command_sender: mpsc::WeakSender<StorageMonitorCommand>,
        #[cfg(all(test, feature = "test-support"))] registration_ack_pause: Arc<
            Mutex<Option<Arc<tokio::sync::Notify>>>,
        >,
    ) -> Result<Self, StorageMonitorError> {
        let StorageMonitorConfig {
            policy,
            sampler,
            clock,
            scheduler_notifications,
            critical_notifications,
        } = config;
        let mut saw_data = false;
        let mut saw_runtime = false;
        let mut repositories = HashSet::new();
        let mut logical_scopes = Vec::with_capacity(bindings.len());
        let mut volumes = HashMap::new();
        let mut volume_order = Vec::new();

        for binding in bindings {
            match binding.scope {
                MonitoredStorageScope::Data if saw_data => {
                    return Err(StorageMonitorError::InvalidDataScope);
                }
                MonitoredStorageScope::Data => saw_data = true,
                MonitoredStorageScope::Runtime if saw_runtime => {
                    return Err(StorageMonitorError::InvalidRuntimeScope);
                }
                MonitoredStorageScope::Runtime => saw_runtime = true,
                MonitoredStorageScope::RepositoryGit(repository_id)
                    if !repositories.insert(repository_id) =>
                {
                    return Err(StorageMonitorError::DuplicateRepositoryScope);
                }
                MonitoredStorageScope::RepositoryGit(_) => {}
            }

            let volume = binding.target.expected_volume;
            if let std::collections::hash_map::Entry::Vacant(entry) = volumes.entry(volume) {
                volume_order.push(volume);
                entry.insert(VolumeRuntime::new(binding.target.clone()));
            }
            let policy_binding = binding.policy_binding();
            logical_scopes.push(LogicalScopeRuntime {
                scope: binding.scope,
                coordination_key: match binding.scope {
                    MonitoredStorageScope::Data | MonitoredStorageScope::Runtime => None,
                    MonitoredStorageScope::RepositoryGit(_) => {
                        Some(authenticated_target_key(&binding.target)?)
                    }
                },
                target: binding.target,
                hysteresis: StorageScopeHysteresis::new(policy_binding),
            });
        }
        if !saw_data {
            return Err(StorageMonitorError::InvalidDataScope);
        }
        if !saw_runtime {
            return Err(StorageMonitorError::InvalidRuntimeScope);
        }
        policy.next_candidate_task_count(0)?;

        Ok(Self {
            policy,
            sampler,
            clock,
            scheduler_notifications,
            critical_notifications,
            command_sender,
            snapshot,
            logical_scopes,
            volumes,
            volume_order,
            activity: StorageActivity::default(),
            active_task_count: 0,
            next_periodic_deadline: None,
            next_probe_id: 1,
            refresh_waiters: Vec::new(),
            last_published: None,
            #[cfg(all(test, feature = "test-support"))]
            registration_ack_pause,
        })
    }

    async fn run(mut self, mut receiver: mpsc::Receiver<StorageMonitorCommand>) {
        loop {
            let command = if let Some(deadline) = self.next_periodic_deadline {
                tokio::select! {
                    command = receiver.recv() => command,
                    _ = tokio::time::sleep_until(deadline) => {
                        self.handle_periodic_tick();
                        continue;
                    }
                }
            } else {
                receiver.recv().await
            };
            let Some(command) = command else {
                break;
            };
            self.handle_command(command);
        }
        for waiter in self.refresh_waiters.drain(..) {
            let _ = waiter.response.send(Err(StorageMonitorError::Unavailable));
        }
    }

    fn handle_command(&mut self, command: StorageMonitorCommand) {
        match command {
            StorageMonitorCommand::RegisterRepositoryScope {
                repository_id,
                coordination_key,
                target,
                response,
            } => {
                let result =
                    self.handle_register_repository_scope(repository_id, coordination_key, target);
                #[cfg(all(test, feature = "test-support"))]
                if let Some(release) = self
                    .registration_ack_pause
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .take()
                {
                    tokio::spawn(async move {
                        release.notified().await;
                        let _ = response.send(result);
                    });
                    return;
                }
                let _ = response.send(result);
            }
            StorageMonitorCommand::SetActivity { activity, response } => {
                let result = self.handle_set_activity(activity);
                let _ = response.send(result);
            }
            StorageMonitorCommand::RefreshForAdmission {
                active_task_count,
                repository_id,
                response,
            } => {
                self.handle_refresh_for_admission(active_task_count, repository_id, response);
            }
            StorageMonitorCommand::ProbeCompleted {
                volume,
                probe_id,
                result,
            } => self.handle_probe_completed(volume, probe_id, result),
            StorageMonitorCommand::ProbeTimedOut { volume, probe_id } => {
                self.handle_probe_timed_out(volume, probe_id);
            }
            StorageMonitorCommand::ProbeExited { volume, probe_id } => {
                self.handle_probe_exited(volume, probe_id);
            }
        }
    }

    fn handle_register_repository_scope(
        &mut self,
        repository_id: RepositoryId,
        coordination_key: RepositoryCoordinationKey,
        target: StorageProbeTarget,
    ) -> Result<(), StorageMonitorError> {
        if authenticated_target_key(&target)? != coordination_key {
            return Err(StorageMonitorError::Unavailable);
        }
        if let Some(existing) = self.logical_scopes.iter().find(|logical_scope| {
            logical_scope.scope == MonitoredStorageScope::RepositoryGit(repository_id)
        }) {
            return if same_authenticated_target(
                existing
                    .coordination_key
                    .expect("repository storage scopes always have a coordination key"),
                &existing.target,
                coordination_key,
                &target,
            )? {
                Ok(())
            } else {
                Err(StorageMonitorError::DuplicateRepositoryScope)
            };
        }

        let volume = target.expected_volume;
        if !self.volumes.contains_key(&volume) {
            self.volume_order.push(volume);
            self.volumes
                .insert(volume, VolumeRuntime::new(target.clone()));
        }
        self.logical_scopes.push(LogicalScopeRuntime {
            scope: MonitoredStorageScope::RepositoryGit(repository_id),
            coordination_key: Some(coordination_key),
            hysteresis: StorageScopeHysteresis::new(StorageScopeBinding::new(
                StorageScope::RepositoryGit,
                volume,
            )),
            target,
        });
        self.update_outputs(Vec::new());
        Ok(())
    }

    fn handle_set_activity(
        &mut self,
        activity: StorageActivity,
    ) -> Result<(), StorageMonitorError> {
        self.policy
            .next_candidate_task_count(activity.active_tasks)?;
        if self.active_task_count != activity.active_tasks {
            self.active_task_count = activity.active_tasks;
            self.reclassify_current_observations()?;
        }

        let was_active = self.activity.requires_periodic_sampling();
        let is_active = activity.requires_periodic_sampling();
        self.activity = activity;
        match (was_active, is_active) {
            (false, true) => {
                self.next_periodic_deadline = Some(Instant::now() + STORAGE_SAMPLE_INTERVAL);
            }
            (true, false) => self.next_periodic_deadline = None,
            (false, false) | (true, true) => {}
        }
        Ok(())
    }

    fn handle_refresh_for_admission(
        &mut self,
        active_task_count: u32,
        repository_id: Option<RepositoryId>,
        response: oneshot::Sender<Result<StorageMonitorSnapshot, StorageMonitorError>>,
    ) {
        if let Err(error) = self.policy.next_candidate_task_count(active_task_count) {
            let _ = response.send(Err(error.into()));
            return;
        }
        if self.active_task_count != active_task_count {
            self.active_task_count = active_task_count;
            if let Err(error) = self.reclassify_current_observations() {
                let _ = response.send(Err(error));
                return;
            }
        }

        let required_volumes = match self.required_volumes(repository_id) {
            Ok(required_volumes) => required_volumes,
            Err(error) => {
                let _ = response.send(Err(error));
                return;
            }
        };
        let waiter = RefreshWaiter {
            required_volumes,
            pending_volumes: HashSet::new(),
            response,
        };
        if let Some(waiter) = self.recheck_refresh_waiter(waiter) {
            self.refresh_waiters.push(waiter);
        }
    }

    fn required_volumes(
        &self,
        repository_id: Option<RepositoryId>,
    ) -> Result<HashSet<VolumeIdentity>, StorageMonitorError> {
        let Some(repository_id) = repository_id else {
            return Ok(self.volume_order.iter().copied().collect());
        };
        let mut required = HashSet::new();
        let mut found_repository = false;
        for logical_scope in &self.logical_scopes {
            let required_for_candidate = match logical_scope.scope {
                MonitoredStorageScope::Data | MonitoredStorageScope::Runtime => true,
                MonitoredStorageScope::RepositoryGit(current) if current == repository_id => {
                    found_repository = true;
                    true
                }
                MonitoredStorageScope::RepositoryGit(_) => false,
            };
            if required_for_candidate {
                required.insert(logical_scope.hysteresis.binding().volume());
            }
        }
        if !found_repository {
            return Err(StorageMonitorError::UnknownRepositoryScope);
        }
        Ok(required)
    }

    fn handle_periodic_tick(&mut self) {
        for volume in self.volume_order.clone() {
            if self
                .volumes
                .get(&volume)
                .is_some_and(|runtime| runtime.in_flight.is_none())
            {
                let _ = self.start_probe(volume);
            }
        }
        self.next_periodic_deadline = self
            .activity
            .requires_periodic_sampling()
            .then(|| Instant::now() + STORAGE_SAMPLE_INTERVAL);
    }

    fn start_probe(&mut self, volume: VolumeIdentity) -> Result<(), StorageMonitorError> {
        let probe_id = self.next_probe_id;
        self.next_probe_id = self
            .next_probe_id
            .checked_add(1)
            .ok_or(StorageMonitorError::ProbeSequenceExhausted)?;
        let runtime = self
            .volumes
            .get_mut(&volume)
            .ok_or(StorageMonitorError::Unavailable)?;
        if runtime.in_flight.is_some() {
            return Ok(());
        }
        runtime.in_flight = Some(ProbeFlight {
            id: probe_id,
            timed_out: false,
        });
        spawn_volume_probe(
            self.command_sender.clone(),
            self.sampler.clone(),
            runtime.target.clone(),
            volume,
            probe_id,
        );
        Ok(())
    }

    fn handle_probe_completed(
        &mut self,
        volume: VolumeIdentity,
        probe_id: u64,
        result: Result<VolumeSample, VolumeSampleError>,
    ) {
        let Some(runtime) = self.volumes.get_mut(&volume) else {
            return;
        };
        if !runtime
            .in_flight
            .is_some_and(|flight| flight.id == probe_id && !flight.timed_out)
        {
            return;
        }
        runtime.in_flight = None;

        let observation = match result {
            Ok(sample) if sample.identity() == volume => StorageObservation::available(sample),
            Ok(_) | Err(_) => StorageObservation::unavailable(volume),
        };
        let observed_at = self.clock.now();
        let result = self.apply_observations(&[(volume, observation, observed_at)], true);
        self.complete_refresh_waiters(volume, result);
    }

    fn handle_probe_timed_out(&mut self, volume: VolumeIdentity, probe_id: u64) {
        let Some(runtime) = self.volumes.get_mut(&volume) else {
            return;
        };
        let Some(flight) = runtime.in_flight.as_mut() else {
            return;
        };
        if flight.id != probe_id || flight.timed_out {
            return;
        }
        flight.timed_out = true;

        let observed_at = self.clock.now();
        let result = self.apply_observations(
            &[(volume, StorageObservation::unavailable(volume), observed_at)],
            true,
        );
        self.complete_refresh_waiters(volume, result);
    }

    fn handle_probe_exited(&mut self, volume: VolumeIdentity, probe_id: u64) {
        let Some(runtime) = self.volumes.get_mut(&volume) else {
            return;
        };
        if runtime
            .in_flight
            .is_some_and(|flight| flight.id == probe_id && flight.timed_out)
        {
            runtime.in_flight = None;
        }
    }

    fn volume_is_fresh(
        &self,
        volume: VolumeIdentity,
        observed_now: Duration,
    ) -> Result<bool, StorageMonitorError> {
        let Some(latest) = self.volumes.get(&volume).and_then(|runtime| runtime.latest) else {
            return Ok(false);
        };
        let age = observed_now
            .checked_sub(latest.observed_at)
            .ok_or(StoragePolicyError::NonMonotonicObservationTime)?;
        Ok(age <= STORAGE_SAMPLE_FRESHNESS)
    }

    fn reclassify_current_observations(&mut self) -> Result<(), StorageMonitorError> {
        let observations = self
            .volume_order
            .iter()
            .filter_map(|volume| {
                self.volumes
                    .get(volume)
                    .and_then(|runtime| runtime.latest)
                    .map(|latest| (*volume, latest.observation, latest.observed_at))
            })
            .collect::<Vec<_>>();
        if observations.is_empty() {
            return Ok(());
        }
        self.apply_observations(&observations, false)
    }

    fn apply_observations(
        &mut self,
        observations: &[(VolumeIdentity, StorageObservation, Duration)],
        update_latest: bool,
    ) -> Result<(), StorageMonitorError> {
        let mut classifications = Vec::new();
        for (volume, observation, observed_at) in observations {
            for (index, logical_scope) in self.logical_scopes.iter().enumerate() {
                if logical_scope.hysteresis.binding().volume() != *volume {
                    continue;
                }
                let classification = self.policy.classify_scope(
                    logical_scope.hysteresis.binding(),
                    *observation,
                    self.active_task_count,
                )?;
                classifications.push((index, classification, *observed_at));
            }
        }

        let mut critical_scopes = Vec::new();
        for (index, classification, observed_at) in classifications {
            let logical_scope = &mut self.logical_scopes[index];
            let previous = logical_scope.hysteresis.state();
            let current = logical_scope
                .hysteresis
                .observe(classification, observed_at)?;
            if previous != Some(StorageState::Critical) && current == StorageState::Critical {
                critical_scopes.push(logical_scope.scope);
            }
        }
        if update_latest {
            for (volume, observation, observed_at) in observations {
                if let Some(runtime) = self.volumes.get_mut(volume) {
                    runtime.latest = Some(VolumeObservationRecord {
                        observation: *observation,
                        observed_at: *observed_at,
                    });
                }
            }
        }
        self.update_outputs(critical_scopes);
        Ok(())
    }

    fn update_outputs(&mut self, critical_scopes: Vec<MonitoredStorageScope>) {
        let snapshot = self.build_snapshot();
        *self
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = snapshot.clone();

        if !critical_scopes.is_empty() {
            self.critical_notifications
                .notify_storage_critical(StorageCriticalNotification::new(critical_scopes));
        }
        let Some(notification) = snapshot.scheduler_notification() else {
            return;
        };
        if self.last_published.as_ref() == Some(&notification) {
            return;
        }
        self.last_published = Some(notification.clone());
        self.scheduler_notifications
            .notify_storage_classification(notification);
    }

    fn build_snapshot(&self) -> StorageMonitorSnapshot {
        let mut data_state = None;
        let mut runtime_state = None;
        let mut repositories = Vec::new();
        for logical_scope in &self.logical_scopes {
            let state = logical_scope.hysteresis.state();
            match logical_scope.scope {
                MonitoredStorageScope::Data => data_state = state,
                MonitoredStorageScope::Runtime => runtime_state = state,
                MonitoredStorageScope::RepositoryGit(repository_id) => {
                    repositories.push(MonitoredRepositoryStorageState {
                        repository_id,
                        state,
                    });
                }
            }
        }
        repositories.sort_unstable_by_key(|repository| repository.repository_id.as_uuid());
        let complete = data_state.is_some()
            && runtime_state.is_some()
            && repositories
                .iter()
                .all(|repository| repository.state.is_some());
        let state = complete.then(|| {
            aggregate_storage_state(
                data_state.into_iter().chain(runtime_state).chain(
                    repositories
                        .iter()
                        .filter_map(|repository| repository.state),
                ),
            )
        });
        StorageMonitorSnapshot {
            state,
            data_state,
            runtime_state,
            repositories,
        }
    }

    fn complete_refresh_waiters(
        &mut self,
        volume: VolumeIdentity,
        result: Result<(), StorageMonitorError>,
    ) {
        let refresh_waiters = std::mem::take(&mut self.refresh_waiters);
        let mut waiting = Vec::new();
        for mut waiter in refresh_waiters {
            if !waiter.pending_volumes.remove(&volume) {
                waiting.push(waiter);
                continue;
            }
            match result {
                Err(error) => {
                    let _ = waiter.response.send(Err(error));
                }
                Ok(()) if waiter.pending_volumes.is_empty() => {
                    if let Some(waiter) = self.recheck_refresh_waiter(waiter) {
                        waiting.push(waiter);
                    }
                }
                Ok(()) => waiting.push(waiter),
            }
        }
        self.refresh_waiters = waiting;
    }

    fn recheck_refresh_waiter(&mut self, mut waiter: RefreshWaiter) -> Option<RefreshWaiter> {
        waiter.pending_volumes.clear();
        let observed_now = self.clock.now();
        for volume in waiter.required_volumes.iter().copied().collect::<Vec<_>>() {
            match self.volume_is_fresh(volume, observed_now) {
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => {
                    let _ = waiter.response.send(Err(error));
                    return None;
                }
            }

            let in_flight = self
                .volumes
                .get(&volume)
                .and_then(|runtime| runtime.in_flight);
            match in_flight {
                Some(flight) if !flight.timed_out => {
                    waiter.pending_volumes.insert(volume);
                }
                Some(_) => {
                    let _ = waiter.response.send(Err(StorageMonitorError::Unavailable));
                    return None;
                }
                None => match self.start_probe(volume) {
                    Ok(()) => {
                        waiter.pending_volumes.insert(volume);
                    }
                    Err(error) => {
                        let _ = waiter.response.send(Err(error));
                        return None;
                    }
                },
            }
        }

        if waiter.pending_volumes.is_empty() {
            let _ = waiter.response.send(Ok(self.snapshot()));
            None
        } else {
            Some(waiter)
        }
    }

    fn snapshot(&self) -> StorageMonitorSnapshot {
        self.snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

fn spawn_volume_probe(
    command_sender: mpsc::WeakSender<StorageMonitorCommand>,
    sampler: Arc<dyn VolumeSampler>,
    target: StorageProbeTarget,
    volume: VolumeIdentity,
    probe_id: u64,
) {
    let mut probe = tokio::task::spawn_blocking(move || sampler.sample(target.root()));
    tokio::spawn(async move {
        let timeout = tokio::time::sleep(STORAGE_PROBE_TIMEOUT);
        tokio::pin!(timeout);
        tokio::select! {
            result = &mut probe => {
                let result = result.unwrap_or(Err(VolumeSampleError::Unavailable));
                if let Some(sender) = command_sender.upgrade() {
                    let _ = sender
                        .send(StorageMonitorCommand::ProbeCompleted {
                            volume,
                            probe_id,
                            result,
                        })
                        .await;
                }
            }
            _ = &mut timeout => {
                if let Some(sender) = command_sender.upgrade() {
                    let _ = sender
                        .send(StorageMonitorCommand::ProbeTimedOut { volume, probe_id })
                        .await;
                }
                let _ = probe.await;
                if let Some(sender) = command_sender.upgrade() {
                    let _ = sender
                        .send(StorageMonitorCommand::ProbeExited { volume, probe_id })
                        .await;
                }
            }
        }
    });
}
