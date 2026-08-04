use std::collections::{HashMap, HashSet};
use std::fmt;
use std::num::NonZeroU32;

use coding_agent_domain::{RepositoryId, TaskId, UtcTimestamp};
use coding_agent_runtime::DirectoryIdentityMarker;

use super::PermitLedgerSnapshot;
use crate::storage_policy::StorageState;

const MAX_CONCURRENT_TASKS: u32 = 4;

/// Process-local repository grouping derived from an authenticated directory identity.
///
/// The key deliberately exposes no path, durable seed, or marker representation.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RepositoryCoordinationKey(DirectoryIdentityMarker);

impl RepositoryCoordinationKey {
    pub const fn from_authenticated_marker(marker: DirectoryIdentityMarker) -> Self {
        Self(marker)
    }
}

impl fmt::Debug for RepositoryCoordinationKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RepositoryCoordinationKey(<opaque>)")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SchedulerLimitError {
    #[error("global concurrency limit must be between one and four")]
    InvalidGlobal,
    #[error("repository concurrency limit must be between one and four")]
    InvalidRepository,
    #[error("repository concurrency limit cannot exceed the global limit")]
    RepositoryExceedsGlobal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerConcurrencyLimits {
    global: NonZeroU32,
    per_repository: NonZeroU32,
}

impl SchedulerConcurrencyLimits {
    pub fn try_new(global: u32, per_repository: u32) -> Result<Self, SchedulerLimitError> {
        let global = NonZeroU32::new(global)
            .filter(|value| value.get() <= MAX_CONCURRENT_TASKS)
            .ok_or(SchedulerLimitError::InvalidGlobal)?;
        let per_repository = NonZeroU32::new(per_repository)
            .filter(|value| value.get() <= MAX_CONCURRENT_TASKS)
            .ok_or(SchedulerLimitError::InvalidRepository)?;
        if per_repository > global {
            return Err(SchedulerLimitError::RepositoryExceedsGlobal);
        }
        Ok(Self {
            global,
            per_repository,
        })
    }

    pub const fn global(self) -> NonZeroU32 {
        self.global
    }

    pub const fn per_repository(self) -> NonZeroU32 {
        self.per_repository
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueueReason {
    ServicePaused,
    StoragePressure,
    GlobalCapacity,
    RepositoryCapacity,
    RepositoryControlBusy,
}

impl QueueReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ServicePaused => "service_paused",
            Self::StoragePressure => "storage_pressure",
            Self::GlobalCapacity => "global_capacity",
            Self::RepositoryCapacity => "repository_capacity",
            Self::RepositoryControlBusy => "repository_control_busy",
        }
    }

    const fn blocks_coordination_key(self) -> bool {
        matches!(
            self,
            Self::StoragePressure | Self::RepositoryCapacity | Self::RepositoryControlBusy
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerRepositoryStorageState {
    repository_id: RepositoryId,
    state: StorageState,
}

impl SchedulerRepositoryStorageState {
    pub const fn new(repository_id: RepositoryId, state: StorageState) -> Self {
        Self {
            repository_id,
            state,
        }
    }

    pub const fn repository_id(self) -> RepositoryId {
        self.repository_id
    }

    pub const fn state(self) -> StorageState {
        self.state
    }
}

/// Path-free storage classification input for Scheduler projection updates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerStorageNotification {
    state: StorageState,
    data_state: StorageState,
    runtime_state: StorageState,
    repositories: Vec<SchedulerRepositoryStorageState>,
}

impl SchedulerStorageNotification {
    pub fn new(
        state: StorageState,
        data_state: StorageState,
        runtime_state: StorageState,
        mut repositories: Vec<SchedulerRepositoryStorageState>,
    ) -> Self {
        repositories.sort_unstable_by_key(|repository| repository.repository_id.as_uuid());
        Self {
            state,
            data_state,
            runtime_state,
            repositories,
        }
    }

    pub const fn state(&self) -> StorageState {
        self.state
    }

    pub const fn data_state(&self) -> StorageState {
        self.data_state
    }

    pub const fn runtime_state(&self) -> StorageState {
        self.runtime_state
    }

    pub fn repositories(&self) -> &[SchedulerRepositoryStorageState] {
        &self.repositories
    }
}

/// Classification-only notification port consumed by Scheduler.
///
/// Implementations advance Scheduler projection generation only when this
/// categorical value changes; raw volume samples never cross this boundary.
pub trait SchedulerStorageNotificationSink: Send + Sync {
    fn notify_storage_classification(&self, notification: SchedulerStorageNotification);
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueueReasonSignals {
    pub service_paused: bool,
    pub storage_pressure: bool,
    pub global_capacity: bool,
    pub repository_capacity: bool,
    pub repository_control_busy: bool,
}

/// Selects the one public queue reason using the protocol's fixed priority.
pub const fn project_queue_reason(signals: QueueReasonSignals) -> Option<QueueReason> {
    if signals.service_paused {
        Some(QueueReason::ServicePaused)
    } else if signals.storage_pressure {
        Some(QueueReason::StoragePressure)
    } else if signals.global_capacity {
        Some(QueueReason::GlobalCapacity)
    } else if signals.repository_capacity {
        Some(QueueReason::RepositoryCapacity)
    } else if signals.repository_control_busy {
        Some(QueueReason::RepositoryControlBusy)
    } else {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueuedTaskCandidate {
    task_id: TaskId,
    repository_id: RepositoryId,
    coordination_key: RepositoryCoordinationKey,
    created_at: UtcTimestamp,
}

impl QueuedTaskCandidate {
    pub const fn new(
        task_id: TaskId,
        repository_id: RepositoryId,
        coordination_key: RepositoryCoordinationKey,
        created_at: UtcTimestamp,
    ) -> Self {
        Self {
            task_id,
            repository_id,
            coordination_key,
            created_at,
        }
    }

    pub const fn task_id(self) -> TaskId {
        self.task_id
    }

    pub const fn repository_id(self) -> RepositoryId {
        self.repository_id
    }

    pub const fn coordination_key(self) -> RepositoryCoordinationKey {
        self.coordination_key
    }

    pub const fn created_at(self) -> UtcTimestamp {
        self.created_at
    }
}

#[derive(Debug, Clone, Default)]
pub struct SchedulerAdmissionGates {
    service_paused: bool,
    storage_blocked_tasks: HashSet<TaskId>,
    repository_control_busy: HashSet<RepositoryCoordinationKey>,
}

impl SchedulerAdmissionGates {
    pub fn new(service_paused: bool) -> Self {
        Self {
            service_paused,
            ..Self::default()
        }
    }

    pub const fn service_paused(&self) -> bool {
        self.service_paused
    }

    pub fn set_service_paused(&mut self, paused: bool) {
        self.service_paused = paused;
    }

    pub fn set_storage_pressure(&mut self, task_id: TaskId, blocked: bool) {
        set_membership(&mut self.storage_blocked_tasks, task_id, blocked);
    }

    pub fn set_repository_control_busy(&mut self, key: RepositoryCoordinationKey, busy: bool) {
        set_membership(&mut self.repository_control_busy, key, busy);
    }

    pub fn storage_blocks(&self, task_id: TaskId) -> bool {
        self.storage_blocked_tasks.contains(&task_id)
    }

    pub fn repository_control_is_busy(&self, key: RepositoryCoordinationKey) -> bool {
        self.repository_control_busy.contains(&key)
    }
}

fn set_membership<T>(set: &mut HashSet<T>, value: T, present: bool)
where
    T: Eq + std::hash::Hash,
{
    if present {
        set.insert(value);
    } else {
        set.remove(&value);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CandidateEvaluation {
    pub candidate: QueuedTaskCandidate,
    pub reason: Option<QueueReason>,
    pub blocked_by_earlier_same_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerScan {
    pub next_candidate: Option<QueuedTaskCandidate>,
    pub evaluations: Vec<CandidateEvaluation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SchedulerScanError {
    #[error("queued scheduler input contains a duplicate task")]
    DuplicateTask,
}

/// Purely evaluates the durable queue against a point-in-time ledger and gate snapshot.
///
/// The function performs no claim, lease acquisition, Store mutation, or runtime I/O.
pub fn scan_queued_candidates(
    candidates: &[QueuedTaskCandidate],
    ledger: &PermitLedgerSnapshot,
    gates: &SchedulerAdmissionGates,
) -> Result<SchedulerScan, SchedulerScanError> {
    let mut ordered = candidates.to_vec();
    ordered.sort_unstable_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.task_id.as_uuid().cmp(&right.task_id.as_uuid()))
    });

    let mut task_ids = HashSet::with_capacity(ordered.len());
    if ordered
        .iter()
        .any(|candidate| !task_ids.insert(candidate.task_id))
    {
        return Err(SchedulerScanError::DuplicateTask);
    }

    let service_paused = gates.service_paused || ledger.has_abandoned();
    let global_capacity = ledger.global_owned() >= ledger.limits().global().get();
    let mut blocked_keys = HashMap::<RepositoryCoordinationKey, QueueReason>::new();
    let mut evaluations = Vec::with_capacity(ordered.len());

    for candidate in ordered {
        if let Some(reason) = blocked_keys.get(&candidate.coordination_key).copied() {
            evaluations.push(CandidateEvaluation {
                candidate,
                reason: Some(reason),
                blocked_by_earlier_same_key: true,
            });
            continue;
        }

        let reason = project_queue_reason(QueueReasonSignals {
            service_paused,
            storage_pressure: gates.storage_blocks(candidate.task_id),
            global_capacity,
            repository_capacity: ledger.repository_owned(candidate.coordination_key)
                >= ledger.limits().per_repository().get(),
            repository_control_busy: gates.repository_control_is_busy(candidate.coordination_key),
        });

        if let Some(reason) = reason
            && reason.blocks_coordination_key()
        {
            blocked_keys.insert(candidate.coordination_key, reason);
        }
        evaluations.push(CandidateEvaluation {
            candidate,
            reason,
            blocked_by_earlier_same_key: false,
        });
    }

    let next_candidate = evaluations
        .iter()
        .find(|evaluation| evaluation.reason.is_none())
        .map(|evaluation| evaluation.candidate);

    Ok(SchedulerScan {
        next_candidate,
        evaluations,
    })
}
