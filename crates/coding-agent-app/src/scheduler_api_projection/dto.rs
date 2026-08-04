use std::collections::{HashMap, HashSet};

use coding_agent_api::{
    SchedulerAdmissionStateDto, SchedulerLimitsDto, SchedulerQueueReasonDto,
    SchedulerQueuedTaskDto, SchedulerRepositoryStorageDto, SchedulerStateDto,
    SchedulerStopIntentDto, SchedulerStoppingTaskDto, SchedulerStorageDto,
    SchedulerStorageScopeDto, SchedulerStorageStateDto,
};
use coding_agent_domain::{EventCursor, TaskStatus, UtcTimestamp};
use coding_agent_store::StopIntentKind;

use super::logical::{SchedulerAdmissionState, SchedulerStoreState};
use crate::ServiceState;
use crate::bootstrap_join::JoinedBootstrapSnapshot;
use crate::scheduler::{QueueReason, SchedulerProjectionSnapshot};
use crate::storage_policy::{StorageState, aggregate_storage_state};

const JSON_SAFE_INTEGER_MAX: u64 = 9_007_199_254_740_991;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SchedulerApiProjectionError {
    #[error("DATABASE_PROJECTION_LIMIT_EXCEEDED")]
    DatabaseProjectionLimitExceeded,
    #[error("joined scheduler projection is inconsistent")]
    InconsistentSnapshot,
}

pub(crate) fn project_joined_scheduler(
    joined: &JoinedBootstrapSnapshot,
    server_started_at: UtcTimestamp,
) -> Result<SchedulerStateDto, SchedulerApiProjectionError> {
    validate_joined_scheduler(joined)?;
    project_scheduler_snapshot(joined.scheduler.as_ref(), server_started_at)
}

/// Projects one already-published immutable Scheduler snapshot for the SSE
/// control plane. Bootstrap and live control deliberately share this exact
/// field mapping so generation, watermarks, ordering, and bounds cannot drift.
pub(crate) fn project_scheduler_snapshot(
    scheduler: &SchedulerProjectionSnapshot<SchedulerStoreState>,
    server_started_at: UtcTimestamp,
) -> Result<SchedulerStateDto, SchedulerApiProjectionError> {
    let state = scheduler.public_state();
    let logical = &state.logical;
    let generation = safe_u64(scheduler.generation())?;
    let as_of_event_id = safe_event_cursor(scheduler.as_of_event_id())?;
    let service_state_generation = safe_u64(scheduler.service_state_generation())?;
    let queued_task_count = u32::try_from(logical.queued_tasks.len())
        .map_err(|_| SchedulerApiProjectionError::DatabaseProjectionLimitExceeded)?;
    Ok(SchedulerStateDto {
        schema_version: 1,
        server_instance_id: state.server_instance_id,
        server_started_at: server_started_at.into(),
        generation,
        as_of_event_id,
        service_state_generation,
        admission_state: match logical.admission {
            SchedulerAdmissionState::Running => SchedulerAdmissionStateDto::Running,
            SchedulerAdmissionState::Paused => SchedulerAdmissionStateDto::Paused,
        },
        limits: SchedulerLimitsDto {
            global: logical.limits.concurrency().global().get(),
            per_repository: logical.limits.concurrency().per_repository().get(),
            queued: logical.limits.queued(),
            cargo_jobs_per_task: logical.limits.cargo_jobs_per_task(),
        },
        active_task_count: logical.active_task_count,
        queued_task_count,
        queued_tasks: logical
            .queued_tasks
            .iter()
            .map(|task| SchedulerQueuedTaskDto {
                task_id: task.task_id.as_uuid(),
                reason: queue_reason_dto(task.reason),
            })
            .collect(),
        stopping_tasks: logical
            .stopping_tasks
            .iter()
            .map(|task| SchedulerStoppingTaskDto {
                task_id: task.task_id.as_uuid(),
                intent: match task.intent {
                    StopIntentKind::UserCancelled => SchedulerStopIntentDto::UserCancelled,
                    StopIntentKind::DiskPressureCritical => {
                        SchedulerStopIntentDto::DiskPressureCritical
                    }
                },
            })
            .collect(),
        storage: SchedulerStorageDto {
            state: storage_state_dto(logical.storage.state),
            data: SchedulerStorageScopeDto {
                state: storage_state_dto(logical.storage.data),
            },
            runtime: SchedulerStorageScopeDto {
                state: storage_state_dto(logical.storage.runtime),
            },
            repositories: logical
                .storage
                .repositories
                .iter()
                .map(|repository| SchedulerRepositoryStorageDto {
                    repository_id: repository.repository_id.as_uuid(),
                    state: storage_state_dto(repository.state),
                })
                .collect(),
        },
    })
}

fn validate_joined_scheduler(
    joined: &JoinedBootstrapSnapshot,
) -> Result<(), SchedulerApiProjectionError> {
    let scheduler = joined.scheduler.as_ref();
    let state = scheduler.public_state();
    if joined.server_instance_id != state.server_instance_id
        || joined.server_instance_id.get_version() != Some(uuid::Version::Random)
        || scheduler.as_of_event_id() != joined.store.membership_event_id
        || scheduler.service_state_generation() != joined.service_state.generation
        || joined.store.membership_event_id > joined.store.latest_event_id
        || !state.exactly_matches(&joined.store)
    {
        return Err(SchedulerApiProjectionError::InconsistentSnapshot);
    }
    safe_event_cursor(joined.store.latest_event_id)?;
    let logical = &state.logical;
    if logical.active_task_count > logical.limits.concurrency().global().get()
        || logical.stopping_tasks.len()
            > usize::try_from(logical.active_task_count).unwrap_or(usize::MAX)
        || logical.queued_tasks.len()
            != joined
                .store
                .tasks
                .iter()
                .filter(|task| task.status == TaskStatus::Queued)
                .count()
        || logical.storage.repositories.len() != joined.store.repositories.len()
    {
        return Err(SchedulerApiProjectionError::InconsistentSnapshot);
    }
    validate_logical_membership(joined, state)?;
    Ok(())
}

fn validate_logical_membership(
    joined: &JoinedBootstrapSnapshot,
    state: &SchedulerStoreState,
) -> Result<(), SchedulerApiProjectionError> {
    let logical = &state.logical;
    if logical.admission == SchedulerAdmissionState::Running
        && joined.service_state.state != ServiceState::Ready
    {
        return Err(SchedulerApiProjectionError::InconsistentSnapshot);
    }

    let mut queued = joined
        .store
        .tasks
        .iter()
        .filter(|task| task.status == TaskStatus::Queued)
        .collect::<Vec<_>>();
    queued.sort_unstable_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.id.as_uuid().cmp(&right.id.as_uuid()))
    });
    let unique_queued = queued.iter().map(|task| task.id).collect::<HashSet<_>>();
    if unique_queued.len() != queued.len()
        || !logical
            .queued_tasks
            .iter()
            .map(|task| task.task_id)
            .eq(queued.iter().map(|task| task.id))
    {
        return Err(SchedulerApiProjectionError::InconsistentSnapshot);
    }

    let running = joined
        .store
        .tasks
        .iter()
        .filter(|task| task.status == TaskStatus::Running)
        .map(|task| (task.id, (task.repository_id, task.attempt)))
        .collect::<HashMap<_, _>>();
    let durable_running_count = u32::try_from(running.len())
        .map_err(|_| SchedulerApiProjectionError::DatabaseProjectionLimitExceeded)?;
    if durable_running_count > logical.active_task_count {
        return Err(SchedulerApiProjectionError::InconsistentSnapshot);
    }
    let mut stopping = joined.store.running_stop_intents.iter().collect::<Vec<_>>();
    stopping.sort_unstable_by(|left, right| {
        left.requested_at
            .cmp(&right.requested_at)
            .then_with(|| left.task_id.as_uuid().cmp(&right.task_id.as_uuid()))
    });
    let unique_stopping = stopping
        .iter()
        .map(|intent| intent.task_id)
        .collect::<HashSet<_>>();
    if unique_stopping.len() != stopping.len()
        || stopping.iter().any(|intent| {
            running.get(&intent.task_id).copied() != Some((intent.repository_id, intent.attempt))
        })
        || !logical
            .stopping_tasks
            .iter()
            .map(|task| (task.task_id, task.intent))
            .eq(stopping.iter().map(|intent| (intent.task_id, intent.kind)))
    {
        return Err(SchedulerApiProjectionError::InconsistentSnapshot);
    }

    let mut repository_ids = joined
        .store
        .repositories
        .iter()
        .map(|repository| repository.id)
        .collect::<Vec<_>>();
    repository_ids.sort_unstable_by_key(|repository_id| repository_id.as_uuid());
    let unique_repositories = repository_ids.iter().copied().collect::<HashSet<_>>();
    if unique_repositories.len() != repository_ids.len()
        || !logical
            .storage
            .repositories
            .iter()
            .map(|repository| repository.repository_id)
            .eq(repository_ids)
        || logical.storage.state
            != aggregate_storage_state(
                [logical.storage.data, logical.storage.runtime]
                    .into_iter()
                    .chain(
                        logical
                            .storage
                            .repositories
                            .iter()
                            .map(|repository| repository.state),
                    ),
            )
    {
        return Err(SchedulerApiProjectionError::InconsistentSnapshot);
    }
    Ok(())
}

fn safe_event_cursor(cursor: EventCursor) -> Result<u64, SchedulerApiProjectionError> {
    let value = u64::try_from(cursor.get())
        .map_err(|_| SchedulerApiProjectionError::InconsistentSnapshot)?;
    safe_u64(value)
}

fn safe_u64(value: u64) -> Result<u64, SchedulerApiProjectionError> {
    if value > JSON_SAFE_INTEGER_MAX {
        Err(SchedulerApiProjectionError::DatabaseProjectionLimitExceeded)
    } else {
        Ok(value)
    }
}

const fn queue_reason_dto(reason: QueueReason) -> SchedulerQueueReasonDto {
    match reason {
        QueueReason::ServicePaused => SchedulerQueueReasonDto::ServicePaused,
        QueueReason::StoragePressure => SchedulerQueueReasonDto::StoragePressure,
        QueueReason::GlobalCapacity => SchedulerQueueReasonDto::GlobalCapacity,
        QueueReason::RepositoryCapacity => SchedulerQueueReasonDto::RepositoryCapacity,
        QueueReason::RepositoryControlBusy => SchedulerQueueReasonDto::RepositoryControlBusy,
    }
}

const fn storage_state_dto(state: StorageState) -> SchedulerStorageStateDto {
    match state {
        StorageState::Normal => SchedulerStorageStateDto::Normal,
        StorageState::Pressure => SchedulerStorageStateDto::Pressure,
        StorageState::Critical => SchedulerStorageStateDto::Critical,
        StorageState::Unavailable => SchedulerStorageStateDto::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use coding_agent_domain::{
        CanonicalPath, ClientRequestId, DeliveryReadiness, EventId, Repository, RepositoryId, Task,
        TaskId,
    };
    use coding_agent_store::SchedulerBootstrapSnapshot;
    use time::OffsetDateTime;
    use uuid::Uuid;

    use super::*;
    use crate::ServiceStateSnapshot;
    use crate::scheduler::{SchedulerProjectionCandidate, SchedulerStatePublisher};

    #[test]
    fn projection_rejects_running_admission_for_non_ready_service() {
        let store = empty_snapshot();
        let instance_id = Uuid::new_v4();
        let mut state = SchedulerStoreState::from_store_snapshot(instance_id, &store);
        state.logical.admission = SchedulerAdmissionState::Running;
        let joined = joined(
            store,
            state,
            ServiceStateSnapshot {
                state: ServiceState::StoreDegraded,
                generation: 0,
            },
            instance_id,
        );

        assert_eq!(
            project_joined_scheduler(&joined, timestamp(0)),
            Err(SchedulerApiProjectionError::InconsistentSnapshot)
        );
    }

    #[test]
    fn projection_rejects_same_length_queue_with_wrong_authoritative_order() {
        let store = queued_snapshot();
        let instance_id = Uuid::new_v4();
        let mut state = SchedulerStoreState::from_store_snapshot(instance_id, &store);
        state.logical.queued_tasks.swap(0, 1);
        let joined = joined(
            store,
            state,
            ServiceStateSnapshot {
                state: ServiceState::Ready,
                generation: 0,
            },
            instance_id,
        );

        assert_eq!(
            project_joined_scheduler(&joined, timestamp(0)),
            Err(SchedulerApiProjectionError::InconsistentSnapshot)
        );
    }

    #[test]
    fn projection_rejects_same_length_storage_with_wrong_repository() {
        let store = queued_snapshot();
        let instance_id = Uuid::new_v4();
        let mut state = SchedulerStoreState::from_store_snapshot(instance_id, &store);
        state.logical.storage.repositories[0].repository_id = RepositoryId::new();
        let joined = joined(
            store,
            state,
            ServiceStateSnapshot {
                state: ServiceState::Ready,
                generation: 0,
            },
            instance_id,
        );

        assert_eq!(
            project_joined_scheduler(&joined, timestamp(0)),
            Err(SchedulerApiProjectionError::InconsistentSnapshot)
        );
    }

    #[test]
    fn live_scheduler_snapshot_projection_matches_the_exact_bootstrap_projection() {
        let store = queued_snapshot();
        let instance_id = Uuid::new_v4();
        let state = SchedulerStoreState::from_store_snapshot(instance_id, &store);
        let joined = joined(
            store,
            state,
            ServiceStateSnapshot {
                state: ServiceState::Ready,
                generation: 7,
            },
            instance_id,
        );

        assert_eq!(
            project_scheduler_snapshot(joined.scheduler.as_ref(), timestamp(0)),
            project_joined_scheduler(&joined, timestamp(0)),
            "bootstrap and future live control must share one DTO projection",
        );
    }

    fn joined(
        store: SchedulerBootstrapSnapshot,
        state: SchedulerStoreState,
        service_state: ServiceStateSnapshot,
        server_instance_id: Uuid,
    ) -> JoinedBootstrapSnapshot {
        let scheduler = SchedulerStatePublisher::new(SchedulerProjectionCandidate::new(
            state,
            store.membership_event_id,
            service_state.generation,
        ))
        .current();
        JoinedBootstrapSnapshot {
            store,
            scheduler,
            service_state,
            server_instance_id,
        }
    }

    fn empty_snapshot() -> SchedulerBootstrapSnapshot {
        SchedulerBootstrapSnapshot {
            repositories: Vec::new(),
            tasks: Vec::new(),
            running_stop_intents: Vec::new(),
            latest_event_id: EventCursor::ZERO,
            membership_event_id: EventCursor::ZERO,
        }
    }

    fn queued_snapshot() -> SchedulerBootstrapSnapshot {
        let repository = repository();
        SchedulerBootstrapSnapshot {
            repositories: vec![repository.clone()],
            tasks: vec![
                task(&repository, timestamp(1)),
                task(&repository, timestamp(0)),
            ],
            running_stop_intents: Vec::new(),
            latest_event_id: EventCursor::new(2).expect("valid latest cursor"),
            membership_event_id: EventCursor::new(2).expect("valid membership cursor"),
        }
    }

    fn repository() -> Repository {
        let root = std::env::current_dir()
            .expect("read current directory")
            .canonicalize()
            .expect("canonicalize current directory");
        let root = CanonicalPath::try_from_canonical(root).expect("canonical test path");
        Repository {
            id: RepositoryId::new(),
            selected_path: root.clone(),
            display_name: "scheduler dto".to_owned(),
            git_root: root.clone(),
            cargo_workspace_root: root,
            created_at: timestamp(0),
            last_opened_at: timestamp(0),
        }
    }

    fn task(repository: &Repository, created_at: UtcTimestamp) -> Task {
        Task {
            id: TaskId::new(),
            client_request_id: ClientRequestId::new(),
            repository_id: repository.id,
            prompt: "scheduler dto".to_owned(),
            status: TaskStatus::Queued,
            delivery_readiness: DeliveryReadiness::Unreviewed,
            attempt: 1,
            retry_of: None,
            created_at,
            started_at: None,
            finished_at: None,
            last_event_id: EventId::new(1).expect("valid task event ID"),
            failure: None,
        }
    }

    fn timestamp(seconds: i64) -> UtcTimestamp {
        UtcTimestamp::new(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(seconds))
            .expect("valid test timestamp")
    }
}
