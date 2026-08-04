use std::collections::{HashMap, HashSet};
use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

use coding_agent_domain::TaskId;
use coding_agent_runtime::{VolumeIdentity, VolumeSample};

use crate::RuntimeConfig;

const MEBIBYTE: u64 = 1024 * 1024;

pub const GIT_RUNTIME_ADMISSION_BYTES: u64 = 256 * MEBIBYTE;
pub const DATA_CRITICAL_BYTES: u64 = 512 * MEBIBYTE;
pub const GIT_RUNTIME_CRITICAL_BYTES: u64 = 64 * MEBIBYTE;
pub const DATA_RECOVERY_MARGIN_BYTES: u64 = 512 * MEBIBYTE;
pub const GIT_RUNTIME_RECOVERY_MARGIN_BYTES: u64 = 64 * MEBIBYTE;
pub const STORAGE_RECOVERY_SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageState {
    Normal,
    Pressure,
    Critical,
    Unavailable,
}

impl StorageState {
    const fn severity(self) -> u8 {
        match self {
            Self::Normal => 0,
            Self::Pressure => 1,
            Self::Unavailable => 2,
            Self::Critical => 3,
        }
    }

    pub const fn blocks_admission(self) -> bool {
        !matches!(self, Self::Normal)
    }

    pub const fn requires_critical_stop(self) -> bool {
        matches!(self, Self::Critical)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageScope {
    Data,
    RepositoryGit,
    Runtime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StorageScopeBinding {
    scope: StorageScope,
    volume: VolumeIdentity,
}

impl StorageScopeBinding {
    pub const fn new(scope: StorageScope, volume: VolumeIdentity) -> Self {
        Self { scope, volume }
    }

    pub const fn scope(self) -> StorageScope {
        self.scope
    }

    pub const fn volume(self) -> VolumeIdentity {
        self.volume
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageObservation {
    Available(VolumeSample),
    Unavailable(VolumeIdentity),
}

impl StorageObservation {
    pub const fn available(sample: VolumeSample) -> Self {
        Self::Available(sample)
    }

    pub const fn unavailable(identity: VolumeIdentity) -> Self {
        Self::Unavailable(identity)
    }

    pub const fn identity(self) -> VolumeIdentity {
        match self {
            Self::Available(sample) => sample.identity(),
            Self::Unavailable(identity) => identity,
        }
    }

    const fn available_bytes(self) -> Option<u64> {
        match self {
            Self::Available(sample) => Some(sample.available_bytes()),
            Self::Unavailable(_) => None,
        }
    }
}

impl From<VolumeSample> for StorageObservation {
    fn from(sample: VolumeSample) -> Self {
        Self::Available(sample)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StoragePolicyError {
    #[error("active task count exceeds the configured concurrency limit")]
    ActiveTaskCountExceedsLimit,
    #[error("storage policy byte arithmetic overflowed")]
    ArithmeticOverflow,
    #[error("volume observation does not match its logical scope binding")]
    VolumeIdentityMismatch,
    #[error("storage classification does not match its hysteresis binding")]
    ScopeBindingMismatch,
    #[error("storage observation time moved backwards")]
    NonMonotonicObservationTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageThresholds {
    pressure_below_bytes: u64,
    critical_below_bytes: u64,
    recovery_at_least_bytes: u64,
}

impl StorageThresholds {
    pub const fn pressure_below_bytes(self) -> u64 {
        self.pressure_below_bytes
    }

    pub const fn critical_below_bytes(self) -> u64 {
        self.critical_below_bytes
    }

    pub const fn recovery_at_least_bytes(self) -> u64 {
        self.recovery_at_least_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeStorageClassification {
    binding: StorageScopeBinding,
    state: StorageState,
    recovery_margin_satisfied: bool,
}

impl ScopeStorageClassification {
    pub const fn binding(self) -> StorageScopeBinding {
        self.binding
    }

    pub const fn state(self) -> StorageState {
        self.state
    }

    pub const fn recovery_margin_satisfied(self) -> bool {
        self.recovery_margin_satisfied
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoragePolicy {
    max_concurrent_tasks: NonZeroU32,
    data_control_reserve_bytes: NonZeroU64,
    data_task_reservation_bytes: NonZeroU64,
}

impl StoragePolicy {
    pub fn try_from_runtime_config(config: &RuntimeConfig) -> Result<Self, StoragePolicyError> {
        Self::try_new(
            config.max_concurrent_tasks(),
            config.storage().data_control_reserve_bytes(),
            config.storage().data_task_reservation_bytes(),
        )
    }

    pub fn try_new(
        max_concurrent_tasks: NonZeroU32,
        data_control_reserve_bytes: NonZeroU64,
        data_task_reservation_bytes: NonZeroU64,
    ) -> Result<Self, StoragePolicyError> {
        let policy = Self {
            max_concurrent_tasks,
            data_control_reserve_bytes,
            data_task_reservation_bytes,
        };
        policy.thresholds(StorageScope::Data, max_concurrent_tasks.get())?;
        Ok(policy)
    }

    pub const fn max_concurrent_tasks(self) -> NonZeroU32 {
        self.max_concurrent_tasks
    }

    pub fn next_candidate_task_count(
        self,
        active_task_count: u32,
    ) -> Result<u32, StoragePolicyError> {
        let maximum = self.max_concurrent_tasks.get();
        if active_task_count > maximum {
            return Err(StoragePolicyError::ActiveTaskCountExceedsLimit);
        }
        active_task_count
            .checked_add(1)
            .map(|with_candidate| with_candidate.min(maximum))
            .ok_or(StoragePolicyError::ArithmeticOverflow)
    }

    pub fn data_next_candidate_threshold(
        self,
        active_task_count: u32,
    ) -> Result<u64, StoragePolicyError> {
        let task_count = self.next_candidate_task_count(active_task_count)?;
        self.data_task_reservation_bytes
            .get()
            .checked_mul(u64::from(task_count))
            .and_then(|reserved| self.data_control_reserve_bytes.get().checked_add(reserved))
            .ok_or(StoragePolicyError::ArithmeticOverflow)
    }

    pub fn thresholds(
        self,
        scope: StorageScope,
        active_task_count: u32,
    ) -> Result<StorageThresholds, StoragePolicyError> {
        match scope {
            StorageScope::Data => {
                let pressure_below_bytes = self.data_next_candidate_threshold(active_task_count)?;
                let recovery_at_least_bytes = pressure_below_bytes
                    .checked_add(DATA_RECOVERY_MARGIN_BYTES)
                    .ok_or(StoragePolicyError::ArithmeticOverflow)?;
                Ok(StorageThresholds {
                    pressure_below_bytes,
                    critical_below_bytes: DATA_CRITICAL_BYTES,
                    recovery_at_least_bytes,
                })
            }
            StorageScope::RepositoryGit | StorageScope::Runtime => {
                self.next_candidate_task_count(active_task_count)?;
                Ok(StorageThresholds {
                    pressure_below_bytes: GIT_RUNTIME_ADMISSION_BYTES,
                    critical_below_bytes: GIT_RUNTIME_CRITICAL_BYTES,
                    recovery_at_least_bytes: GIT_RUNTIME_ADMISSION_BYTES
                        + GIT_RUNTIME_RECOVERY_MARGIN_BYTES,
                })
            }
        }
    }

    pub fn classify_scope(
        self,
        binding: StorageScopeBinding,
        observation: StorageObservation,
        active_task_count: u32,
    ) -> Result<ScopeStorageClassification, StoragePolicyError> {
        if binding.volume != observation.identity() {
            return Err(StoragePolicyError::VolumeIdentityMismatch);
        }
        let thresholds = self.thresholds(binding.scope, active_task_count)?;
        let Some(available_bytes) = observation.available_bytes() else {
            return Ok(ScopeStorageClassification {
                binding,
                state: StorageState::Unavailable,
                recovery_margin_satisfied: false,
            });
        };
        let state = if available_bytes < thresholds.critical_below_bytes {
            StorageState::Critical
        } else if available_bytes < thresholds.pressure_below_bytes {
            StorageState::Pressure
        } else {
            StorageState::Normal
        };
        Ok(ScopeStorageClassification {
            binding,
            state,
            recovery_margin_satisfied: available_bytes >= thresholds.recovery_at_least_bytes,
        })
    }

    pub fn volume_admission_requirements(
        self,
        active_task_count: u32,
        bindings: impl IntoIterator<Item = StorageScopeBinding>,
    ) -> Result<VolumeAdmissionRequirements, StoragePolicyError> {
        self.next_candidate_task_count(active_task_count)?;
        let mut required_bytes = HashMap::new();
        for binding in bindings {
            let requirement = self
                .thresholds(binding.scope, active_task_count)?
                .pressure_below_bytes;
            required_bytes
                .entry(binding.volume)
                .and_modify(|current: &mut u64| *current = (*current).max(requirement))
                .or_insert(requirement);
        }
        Ok(VolumeAdmissionRequirements { required_bytes })
    }
}

#[derive(Clone, Default)]
pub struct VolumeAdmissionRequirements {
    required_bytes: HashMap<VolumeIdentity, u64>,
}

impl VolumeAdmissionRequirements {
    pub fn required_bytes(&self, volume: VolumeIdentity) -> Option<u64> {
        self.required_bytes.get(&volume).copied()
    }

    pub fn len(&self) -> usize {
        self.required_bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.required_bytes.is_empty()
    }

    pub fn admits(&self, sample: VolumeSample) -> Option<bool> {
        self.required_bytes(sample.identity())
            .map(|required| sample.available_bytes() >= required)
    }
}

impl fmt::Debug for VolumeAdmissionRequirements {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VolumeAdmissionRequirements")
            .field("volume_count", &self.required_bytes.len())
            .finish_non_exhaustive()
    }
}

pub fn aggregate_storage_state(states: impl IntoIterator<Item = StorageState>) -> StorageState {
    states
        .into_iter()
        .max_by_key(|state| state.severity())
        .unwrap_or(StorageState::Normal)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageScopeState {
    binding: StorageScopeBinding,
    state: StorageState,
}

impl StorageScopeState {
    pub const fn new(binding: StorageScopeBinding, state: StorageState) -> Self {
        Self { binding, state }
    }

    pub const fn binding(self) -> StorageScopeBinding {
        self.binding
    }

    pub const fn state(self) -> StorageState {
        self.state
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageScopeHysteresis {
    binding: StorageScopeBinding,
    state: Option<StorageState>,
    first_recovery_sample_at: Option<Duration>,
    last_observed_at: Option<Duration>,
}

impl StorageScopeHysteresis {
    pub const fn new(binding: StorageScopeBinding) -> Self {
        Self {
            binding,
            state: None,
            first_recovery_sample_at: None,
            last_observed_at: None,
        }
    }

    pub const fn binding(&self) -> StorageScopeBinding {
        self.binding
    }

    pub const fn state(&self) -> Option<StorageState> {
        self.state
    }

    pub const fn blocks_admission(&self) -> bool {
        match self.state {
            Some(state) => state.blocks_admission(),
            None => true,
        }
    }

    pub const fn snapshot(&self) -> Option<StorageScopeState> {
        match self.state {
            Some(state) => Some(StorageScopeState::new(self.binding, state)),
            None => None,
        }
    }

    pub fn observe(
        &mut self,
        classification: ScopeStorageClassification,
        observed_at: Duration,
    ) -> Result<StorageState, StoragePolicyError> {
        if classification.binding != self.binding {
            return Err(StoragePolicyError::ScopeBindingMismatch);
        }
        if self
            .last_observed_at
            .is_some_and(|last_observed_at| observed_at < last_observed_at)
        {
            return Err(StoragePolicyError::NonMonotonicObservationTime);
        }
        self.last_observed_at = Some(observed_at);

        let Some(current) = self.state else {
            self.state = Some(classification.state);
            self.first_recovery_sample_at = None;
            return Ok(classification.state);
        };

        if current == StorageState::Normal {
            self.state = Some(classification.state);
            self.first_recovery_sample_at = None;
            return Ok(classification.state);
        }

        if classification.state.severity() > current.severity() {
            self.state = Some(classification.state);
            self.first_recovery_sample_at = None;
            return Ok(classification.state);
        }

        if !classification.recovery_margin_satisfied {
            self.first_recovery_sample_at = None;
            return Ok(current);
        }

        let Some(first_recovery_sample_at) = self.first_recovery_sample_at else {
            self.first_recovery_sample_at = Some(observed_at);
            return Ok(current);
        };
        let elapsed = observed_at
            .checked_sub(first_recovery_sample_at)
            .ok_or(StoragePolicyError::NonMonotonicObservationTime)?;
        if elapsed >= STORAGE_RECOVERY_SAMPLE_INTERVAL {
            self.state = Some(StorageState::Normal);
            self.first_recovery_sample_at = None;
            Ok(StorageState::Normal)
        } else {
            Ok(current)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveTaskStorage {
    task_id: TaskId,
    repository_git_volume: VolumeIdentity,
}

impl ActiveTaskStorage {
    pub const fn new(task_id: TaskId, repository_git_volume: VolumeIdentity) -> Self {
        Self {
            task_id,
            repository_git_volume,
        }
    }

    pub const fn task_id(self) -> TaskId {
        self.task_id
    }

    pub const fn repository_git_volume(self) -> VolumeIdentity {
        self.repository_git_volume
    }
}

pub fn critical_affected_tasks(
    scope_states: impl IntoIterator<Item = StorageScopeState>,
    active_tasks: impl IntoIterator<Item = ActiveTaskStorage>,
) -> Vec<TaskId> {
    let mut all_active = false;
    let mut critical_git_volumes = HashSet::new();
    for scope_state in scope_states {
        if !scope_state.state.requires_critical_stop() {
            continue;
        }
        match scope_state.binding.scope {
            StorageScope::Data | StorageScope::Runtime => all_active = true,
            StorageScope::RepositoryGit => {
                critical_git_volumes.insert(scope_state.binding.volume);
            }
        }
    }

    let mut affected = active_tasks
        .into_iter()
        .filter(|active| all_active || critical_git_volumes.contains(&active.repository_git_volume))
        .map(|active| active.task_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    affected.sort_unstable_by_key(|task_id| task_id.as_uuid());
    affected
}
