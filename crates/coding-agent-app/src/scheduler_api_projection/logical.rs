use std::collections::{HashMap, HashSet};

use coding_agent_domain::{RepositoryId, Task, TaskId, TaskStatus, UtcTimestamp};
use coding_agent_store::{SchedulerBootstrapSnapshot, StopIntentKind, StopIntentReceipt};
use uuid::Uuid;

use crate::repository_control::{RepositoryControlCoordinator, RepositoryControlState};
use crate::scheduler::{
    PermitLedgerSnapshot, QueueReason, QueueReasonSignals, SchedulerConcurrencyLimits,
    SchedulerStorageNotification, project_queue_reason,
};
use crate::storage_monitor::StorageActivity;
use crate::storage_policy::{StorageState, aggregate_storage_state};

const MAX_QUEUED_TASKS: u32 = 256;
const MAX_CARGO_JOBS_PER_TASK: u32 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SchedulerPublicLimits {
    concurrency: SchedulerConcurrencyLimits,
    queued: u32,
    cargo_jobs_per_task: u32,
}

impl SchedulerPublicLimits {
    pub(crate) fn try_new(
        concurrency: SchedulerConcurrencyLimits,
        queued: u32,
        cargo_jobs_per_task: u32,
    ) -> Result<Self, SchedulerProjectionBuildError> {
        if queued == 0 || queued > MAX_QUEUED_TASKS {
            return Err(SchedulerProjectionBuildError::InvalidLimits);
        }
        if cargo_jobs_per_task == 0 || cargo_jobs_per_task > MAX_CARGO_JOBS_PER_TASK {
            return Err(SchedulerProjectionBuildError::InvalidLimits);
        }
        Ok(Self {
            concurrency,
            queued,
            cargo_jobs_per_task,
        })
    }

    #[cfg(test)]
    pub(crate) fn compatibility_defaults(concurrency: SchedulerConcurrencyLimits) -> Self {
        Self {
            concurrency,
            queued: MAX_QUEUED_TASKS,
            cargo_jobs_per_task: 1,
        }
    }

    pub(crate) const fn concurrency(self) -> SchedulerConcurrencyLimits {
        self.concurrency
    }

    pub(crate) const fn queued(self) -> u32 {
        self.queued
    }

    pub(crate) const fn cargo_jobs_per_task(self) -> u32 {
        self.cargo_jobs_per_task
    }
}

pub(crate) struct SchedulerRuntimeProjection<'a> {
    pub service_paused: bool,
    pub permit_ledger: &'a PermitLedgerSnapshot,
    pub repository_control: &'a RepositoryControlCoordinator,
    pub storage: Option<&'a SchedulerStorageNotification>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SchedulerProjectionBuildError {
    #[error("scheduler public limits are invalid")]
    InvalidLimits,
    #[error("scheduler projection count exceeds the public u32 range")]
    CountOverflow,
    #[error("scheduler projection contains inconsistent durable membership")]
    InconsistentMembership,
    #[error("scheduler projection repository control state is unavailable")]
    RepositoryControlUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchedulerStoreState {
    pub(super) server_instance_id: Uuid,
    witness: SchedulerStoreWitness,
    pub(super) logical: SchedulerLogicalState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SchedulerStoreWitness {
    initialized: bool,
    repositories: Vec<RepositoryId>,
    tasks: Vec<SchedulerTaskMembership>,
    running_stop_intents: Vec<StopIntentReceipt>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SchedulerTaskMembership {
    task_id: TaskId,
    repository_id: RepositoryId,
    status: TaskStatus,
    created_at: UtcTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SchedulerLogicalState {
    pub(super) limits: SchedulerPublicLimits,
    pub(super) admission: SchedulerAdmissionState,
    pub(super) active_task_count: u32,
    pub(super) queued_tasks: Vec<SchedulerQueuedTask>,
    pub(super) stopping_tasks: Vec<SchedulerStoppingTask>,
    pub(super) storage: SchedulerLogicalStorage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SchedulerAdmissionState {
    Running,
    Paused,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SchedulerQueuedTask {
    pub(super) task_id: TaskId,
    pub(super) reason: QueueReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SchedulerStoppingTask {
    pub(super) task_id: TaskId,
    pub(super) intent: StopIntentKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SchedulerLogicalStorage {
    pub(super) state: StorageState,
    pub(super) data: StorageState,
    pub(super) runtime: StorageState,
    pub(super) repositories: Vec<SchedulerRepositoryStorage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SchedulerRepositoryStorage {
    pub(super) repository_id: RepositoryId,
    pub(super) state: StorageState,
}

impl SchedulerStoreState {
    pub(crate) fn empty(
        server_instance_id: Uuid,
        limits: SchedulerPublicLimits,
        service_paused: bool,
    ) -> Self {
        Self {
            server_instance_id,
            witness: SchedulerStoreWitness {
                initialized: false,
                repositories: Vec::new(),
                tasks: Vec::new(),
                running_stop_intents: Vec::new(),
            },
            logical: SchedulerLogicalState {
                limits,
                admission: if service_paused {
                    SchedulerAdmissionState::Paused
                } else {
                    SchedulerAdmissionState::Running
                },
                active_task_count: 0,
                queued_tasks: Vec::new(),
                stopping_tasks: Vec::new(),
                storage: SchedulerLogicalStorage {
                    state: StorageState::Unavailable,
                    data: StorageState::Unavailable,
                    runtime: StorageState::Unavailable,
                    repositories: Vec::new(),
                },
            },
        }
    }

    /// Store-only constructor used by bounded-join unit fixtures.
    ///
    /// Production publication uses [`Self::from_complete_snapshot`] so no
    /// public Scheduler field is synthesized from this conservative fixture.
    #[cfg(test)]
    pub(crate) fn from_store_snapshot(
        server_instance_id: Uuid,
        snapshot: &SchedulerBootstrapSnapshot,
    ) -> Self {
        let limits = SchedulerPublicLimits::compatibility_defaults(
            SchedulerConcurrencyLimits::try_new(4, 4)
                .expect("fixed scheduler fixture limits are valid"),
        );
        let witness = SchedulerStoreWitness::from_snapshot(snapshot);
        let queued_tasks = ordered_queued_tasks(&snapshot.tasks)
            .into_iter()
            .map(|task| SchedulerQueuedTask {
                task_id: task.id,
                reason: QueueReason::ServicePaused,
            })
            .collect();
        let stopping_tasks = ordered_stopping_tasks(snapshot)
            .expect("Store scheduler fixture has valid running stop intents");
        let active_task_count = u32::try_from(
            snapshot
                .tasks
                .iter()
                .filter(|task| task.status == TaskStatus::Running)
                .count(),
        )
        .expect("Store scheduler fixture count fits u32");
        let repositories = witness
            .repositories
            .iter()
            .copied()
            .map(|repository_id| SchedulerRepositoryStorage {
                repository_id,
                state: StorageState::Unavailable,
            })
            .collect();
        Self {
            server_instance_id,
            witness,
            logical: SchedulerLogicalState {
                limits,
                admission: SchedulerAdmissionState::Paused,
                active_task_count,
                queued_tasks,
                stopping_tasks,
                storage: SchedulerLogicalStorage {
                    state: StorageState::Unavailable,
                    data: StorageState::Unavailable,
                    runtime: StorageState::Unavailable,
                    repositories,
                },
            },
        }
    }

    pub(crate) fn from_complete_snapshot(
        server_instance_id: Uuid,
        limits: SchedulerPublicLimits,
        snapshot: &SchedulerBootstrapSnapshot,
        runtime: SchedulerRuntimeProjection<'_>,
    ) -> Result<Self, SchedulerProjectionBuildError> {
        let witness = SchedulerStoreWitness::from_snapshot(snapshot);
        let storage =
            SchedulerLogicalStorage::from_notification(&witness.repositories, runtime.storage)?;
        let active =
            ActiveProjection::build(snapshot, runtime.permit_ledger, runtime.repository_control)?;
        if active.global > limits.concurrency().global().get() {
            return Err(SchedulerProjectionBuildError::InconsistentMembership);
        }
        let service_paused = runtime.service_paused || runtime.permit_ledger.has_abandoned();
        let queued_tasks = project_queued_tasks(
            snapshot,
            limits,
            service_paused,
            &active,
            &storage,
            runtime.repository_control,
        )?;
        let stopping_tasks = ordered_stopping_tasks(snapshot)?;
        if stopping_tasks.len() > usize::try_from(active.global).unwrap_or(usize::MAX) {
            return Err(SchedulerProjectionBuildError::InconsistentMembership);
        }
        Ok(Self {
            server_instance_id,
            witness,
            logical: SchedulerLogicalState {
                limits,
                admission: if service_paused {
                    SchedulerAdmissionState::Paused
                } else {
                    SchedulerAdmissionState::Running
                },
                active_task_count: active.global,
                queued_tasks,
                stopping_tasks,
                storage,
            },
        })
    }

    pub(crate) const fn server_instance_id(&self) -> Uuid {
        self.server_instance_id
    }

    pub(crate) fn storage_activity(
        &self,
    ) -> Result<StorageActivity, SchedulerProjectionBuildError> {
        let queued_tasks = u32::try_from(self.logical.queued_tasks.len())
            .map_err(|_| SchedulerProjectionBuildError::CountOverflow)?;
        Ok(StorageActivity::new(
            queued_tasks,
            self.logical.active_task_count,
        ))
    }

    pub(crate) fn exactly_matches(&self, snapshot: &SchedulerBootstrapSnapshot) -> bool {
        self.witness == SchedulerStoreWitness::from_snapshot(snapshot)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn tasks_for_test(&self) -> Vec<(TaskId, TaskStatus)> {
        self.witness
            .tasks
            .iter()
            .map(|task| (task.task_id, task.status))
            .collect()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) const fn service_paused_for_test(&self) -> bool {
        matches!(self.logical.admission, SchedulerAdmissionState::Paused)
    }

    #[cfg(test)]
    pub(crate) const fn active_task_count_for_test(&self) -> u32 {
        self.logical.active_task_count
    }
}

impl SchedulerStoreWitness {
    fn from_snapshot(snapshot: &SchedulerBootstrapSnapshot) -> Self {
        let mut repositories = snapshot
            .repositories
            .iter()
            .map(|repository| repository.id)
            .collect::<Vec<_>>();
        repositories.sort_unstable_by_key(|repository_id| repository_id.as_uuid());
        let mut tasks = snapshot
            .tasks
            .iter()
            .map(|task| SchedulerTaskMembership {
                task_id: task.id,
                repository_id: task.repository_id,
                status: task.status,
                created_at: task.created_at,
            })
            .collect::<Vec<_>>();
        tasks.sort_unstable_by_key(|task| task.task_id.as_uuid());
        let mut running_stop_intents = snapshot.running_stop_intents.clone();
        running_stop_intents.sort_unstable_by_key(|intent| intent.task_id.as_uuid());
        Self {
            initialized: true,
            repositories,
            tasks,
            running_stop_intents,
        }
    }
}

struct ActiveProjection {
    global: u32,
    by_coordination_key: HashMap<crate::RepositoryCoordinationKey, u32>,
}

impl ActiveProjection {
    fn build(
        snapshot: &SchedulerBootstrapSnapshot,
        ledger: &PermitLedgerSnapshot,
        repository_control: &RepositoryControlCoordinator,
    ) -> Result<Self, SchedulerProjectionBuildError> {
        let mut active_tasks = HashMap::<TaskId, crate::RepositoryCoordinationKey>::new();
        for task in snapshot
            .tasks
            .iter()
            .filter(|task| task.status == TaskStatus::Running)
        {
            let key = repository_control
                .coordination_key(task.repository_id)
                .map_err(|_| SchedulerProjectionBuildError::RepositoryControlUnavailable)?;
            if active_tasks.insert(task.id, key).is_some() {
                return Err(SchedulerProjectionBuildError::InconsistentMembership);
            }
        }
        for (task_id, key) in ledger.active_tasks() {
            if let Some(durable_key) = active_tasks.insert(task_id, key)
                && durable_key != key
            {
                return Err(SchedulerProjectionBuildError::InconsistentMembership);
            }
        }
        let global = u32::try_from(active_tasks.len())
            .map_err(|_| SchedulerProjectionBuildError::CountOverflow)?;
        let mut by_coordination_key = HashMap::<crate::RepositoryCoordinationKey, u32>::new();
        for key in active_tasks.into_values() {
            let count = by_coordination_key.entry(key).or_default();
            *count = count
                .checked_add(1)
                .ok_or(SchedulerProjectionBuildError::CountOverflow)?;
        }
        Ok(Self {
            global,
            by_coordination_key,
        })
    }

    fn repository(&self, key: crate::RepositoryCoordinationKey) -> u32 {
        self.by_coordination_key.get(&key).copied().unwrap_or(0)
    }
}

impl SchedulerLogicalStorage {
    fn from_notification(
        repository_ids: &[RepositoryId],
        notification: Option<&SchedulerStorageNotification>,
    ) -> Result<Self, SchedulerProjectionBuildError> {
        let Some(notification) = notification else {
            let repositories = repository_ids
                .iter()
                .copied()
                .map(|repository_id| SchedulerRepositoryStorage {
                    repository_id,
                    state: StorageState::Unavailable,
                })
                .collect();
            return Ok(Self {
                state: StorageState::Unavailable,
                data: StorageState::Unavailable,
                runtime: StorageState::Unavailable,
                repositories,
            });
        };
        let mut observed = HashMap::new();
        for repository in notification.repositories() {
            if observed
                .insert(repository.repository_id(), repository.state())
                .is_some()
            {
                return Err(SchedulerProjectionBuildError::InconsistentMembership);
            }
        }
        let mut repositories = repository_ids
            .iter()
            .copied()
            .map(|repository_id| SchedulerRepositoryStorage {
                repository_id,
                state: observed
                    .get(&repository_id)
                    .copied()
                    .unwrap_or(StorageState::Unavailable),
            })
            .collect::<Vec<_>>();
        repositories.sort_unstable_by_key(|entry| entry.repository_id.as_uuid());
        let data = notification.data_state();
        let runtime = notification.runtime_state();
        let state = aggregate_storage_state(
            [data, runtime]
                .into_iter()
                .chain(repositories.iter().map(|entry| entry.state)),
        );
        Ok(Self {
            state,
            data,
            runtime,
            repositories,
        })
    }

    fn repository_state(&self, repository_id: RepositoryId) -> StorageState {
        self.repositories
            .binary_search_by_key(&repository_id.as_uuid(), |entry| {
                entry.repository_id.as_uuid()
            })
            .ok()
            .map(|index| self.repositories[index].state)
            .unwrap_or(StorageState::Unavailable)
    }
}

fn project_queued_tasks(
    snapshot: &SchedulerBootstrapSnapshot,
    limits: SchedulerPublicLimits,
    service_paused: bool,
    active: &ActiveProjection,
    storage: &SchedulerLogicalStorage,
    repository_control: &RepositoryControlCoordinator,
) -> Result<Vec<SchedulerQueuedTask>, SchedulerProjectionBuildError> {
    let queued = ordered_queued_tasks(&snapshot.tasks);
    let global_storage_blocked =
        storage.data.blocks_admission() || storage.runtime.blocks_admission();
    queued
        .into_iter()
        .map(|task| {
            let key = repository_control
                .coordination_key(task.repository_id)
                .map_err(|_| SchedulerProjectionBuildError::RepositoryControlUnavailable)?;
            let control_busy = repository_control
                .control_state(task.repository_id)
                .map_err(|_| SchedulerProjectionBuildError::RepositoryControlUnavailable)?
                != RepositoryControlState::Available;
            let reason = project_queue_reason(QueueReasonSignals {
                service_paused,
                storage_pressure: global_storage_blocked
                    || storage
                        .repository_state(task.repository_id)
                        .blocks_admission(),
                global_capacity: active.global >= limits.concurrency().global().get(),
                repository_capacity: active.repository(key)
                    >= limits.concurrency().per_repository().get(),
                repository_control_busy: control_busy,
            })
            // An otherwise admissible durable Queued task is between scans or
            // admission stages. The lowest-priority coordination reason keeps
            // the required projection total until its durable started event.
            .unwrap_or(QueueReason::RepositoryControlBusy);
            Ok(SchedulerQueuedTask {
                task_id: task.id,
                reason,
            })
        })
        .collect()
}

fn ordered_queued_tasks(tasks: &[Task]) -> Vec<&Task> {
    let mut queued = tasks
        .iter()
        .filter(|task| task.status == TaskStatus::Queued)
        .collect::<Vec<_>>();
    queued.sort_unstable_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.id.as_uuid().cmp(&right.id.as_uuid()))
    });
    queued
}

fn ordered_stopping_tasks(
    snapshot: &SchedulerBootstrapSnapshot,
) -> Result<Vec<SchedulerStoppingTask>, SchedulerProjectionBuildError> {
    let running = snapshot
        .tasks
        .iter()
        .filter(|task| task.status == TaskStatus::Running)
        .map(|task| (task.id, task))
        .collect::<HashMap<_, _>>();
    let mut seen = HashSet::new();
    let mut intents = snapshot.running_stop_intents.iter().collect::<Vec<_>>();
    intents.sort_unstable_by(|left, right| {
        left.requested_at
            .cmp(&right.requested_at)
            .then_with(|| left.task_id.as_uuid().cmp(&right.task_id.as_uuid()))
    });
    intents
        .into_iter()
        .map(|intent| {
            let task = running
                .get(&intent.task_id)
                .ok_or(SchedulerProjectionBuildError::InconsistentMembership)?;
            if !seen.insert(intent.task_id)
                || task.repository_id != intent.repository_id
                || task.attempt != intent.attempt
            {
                return Err(SchedulerProjectionBuildError::InconsistentMembership);
            }
            Ok(SchedulerStoppingTask {
                task_id: intent.task_id,
                intent: intent.kind,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_empty_notification_does_not_classify_a_new_repository() {
        let repository_id = RepositoryId::new();
        let notification = SchedulerStorageNotification::new(
            StorageState::Normal,
            StorageState::Normal,
            StorageState::Normal,
            Vec::new(),
        );

        let storage =
            SchedulerLogicalStorage::from_notification(&[repository_id], Some(&notification))
                .expect("project a retained storage notification");

        assert_eq!(storage.repositories.len(), 1);
        assert_eq!(storage.repositories[0].repository_id, repository_id);
        assert_eq!(storage.repositories[0].state, StorageState::Unavailable);
        assert_eq!(storage.state, StorageState::Unavailable);
    }
}
