use std::collections::BTreeSet;

use crate::contract::{SchedulerStateDto, SchedulerStorageStateDto};

use super::{JSON_SAFE_INTEGER_MAX, SCHEDULER_SCHEMA_VERSION, SchedulerWireError};

#[derive(Debug, Clone, Copy)]
pub(super) struct SnapshotCounts {
    pub(super) stopping: u32,
    pub(super) repositories: u32,
    pub(super) items: u32,
}

pub(super) fn validate_snapshot(
    snapshot: &SchedulerStateDto,
) -> Result<SnapshotCounts, SchedulerWireError> {
    validate_header(snapshot)?;
    validate_limits_and_counts(snapshot)?;
    validate_identity_sets(snapshot)?;
    validate_storage(snapshot)?;

    let stopping = u32::try_from(snapshot.stopping_tasks.len())
        .map_err(|_| SchedulerWireError::ItemCountOverflow)?;
    let repositories = u32::try_from(snapshot.storage.repositories.len())
        .map_err(|_| SchedulerWireError::ItemCountOverflow)?;
    let items = snapshot
        .queued_task_count
        .checked_add(stopping)
        .and_then(|count| count.checked_add(repositories))
        .ok_or(SchedulerWireError::ItemCountOverflow)?;

    Ok(SnapshotCounts {
        stopping,
        repositories,
        items,
    })
}

fn validate_header(snapshot: &SchedulerStateDto) -> Result<(), SchedulerWireError> {
    if snapshot.schema_version != SCHEDULER_SCHEMA_VERSION {
        return Err(SchedulerWireError::InvalidSchemaVersion {
            actual: snapshot.schema_version,
        });
    }
    if snapshot.server_instance_id.get_version_num() != 4
        || snapshot.server_instance_id.get_variant() != uuid::Variant::RFC4122
    {
        return Err(SchedulerWireError::InvalidServerInstanceId);
    }
    for (field, value) in [
        ("generation", snapshot.generation),
        ("as_of_event_id", snapshot.as_of_event_id),
        (
            "service_state_generation",
            snapshot.service_state_generation,
        ),
    ] {
        if value > JSON_SAFE_INTEGER_MAX {
            return Err(SchedulerWireError::UnsafeInteger { field, value });
        }
    }
    Ok(())
}

fn validate_limits_and_counts(snapshot: &SchedulerStateDto) -> Result<(), SchedulerWireError> {
    validate_limit("global", snapshot.limits.global, 1, 4)?;
    validate_limit("per_repository", snapshot.limits.per_repository, 1, 4)?;
    validate_limit("queued", snapshot.limits.queued, 1, 256)?;
    validate_limit(
        "cargo_jobs_per_task",
        snapshot.limits.cargo_jobs_per_task,
        1,
        8,
    )?;
    if snapshot.limits.per_repository > snapshot.limits.global {
        return Err(SchedulerWireError::RepositoryLimitExceedsGlobal {
            per_repository: snapshot.limits.per_repository,
            global: snapshot.limits.global,
        });
    }
    if snapshot.active_task_count > snapshot.limits.global {
        return Err(SchedulerWireError::ActiveTaskCountExceedsGlobal {
            active: snapshot.active_task_count,
            global: snapshot.limits.global,
        });
    }

    let queued = u32::try_from(snapshot.queued_tasks.len())
        .map_err(|_| SchedulerWireError::ItemCountOverflow)?;
    if snapshot.queued_task_count != queued {
        return Err(SchedulerWireError::QueuedTaskCountMismatch {
            declared: snapshot.queued_task_count,
            actual: queued,
        });
    }
    let stopping = u32::try_from(snapshot.stopping_tasks.len())
        .map_err(|_| SchedulerWireError::ItemCountOverflow)?;
    if stopping > snapshot.active_task_count {
        return Err(SchedulerWireError::StoppingTaskCountExceedsActive {
            stopping,
            active: snapshot.active_task_count,
        });
    }
    Ok(())
}

fn validate_limit(
    field: &'static str,
    value: u32,
    minimum: u32,
    maximum: u32,
) -> Result<(), SchedulerWireError> {
    if (minimum..=maximum).contains(&value) {
        Ok(())
    } else {
        Err(SchedulerWireError::InvalidLimit { field, value })
    }
}

fn validate_identity_sets(snapshot: &SchedulerStateDto) -> Result<(), SchedulerWireError> {
    let mut task_ids = BTreeSet::new();
    for task in &snapshot.queued_tasks {
        if !task_ids.insert(task.task_id) {
            return Err(SchedulerWireError::DuplicateTaskId);
        }
    }
    for task in &snapshot.stopping_tasks {
        if !task_ids.insert(task.task_id) {
            return Err(SchedulerWireError::DuplicateTaskId);
        }
    }

    let mut repository_ids = BTreeSet::new();
    let mut previous = None;
    for repository in &snapshot.storage.repositories {
        if !repository_ids.insert(repository.repository_id) {
            return Err(SchedulerWireError::DuplicateRepositoryId);
        }
        if previous.is_some_and(|previous| previous > repository.repository_id) {
            return Err(SchedulerWireError::RepositoryStorageNotCanonical);
        }
        previous = Some(repository.repository_id);
    }
    Ok(())
}

fn validate_storage(snapshot: &SchedulerStateDto) -> Result<(), SchedulerWireError> {
    let aggregate = std::iter::once(snapshot.storage.data.state)
        .chain(std::iter::once(snapshot.storage.runtime.state))
        .chain(
            snapshot
                .storage
                .repositories
                .iter()
                .map(|repository| repository.state),
        )
        .max_by_key(|state| storage_priority(*state))
        .unwrap_or(SchedulerStorageStateDto::Normal);
    if aggregate != snapshot.storage.state {
        return Err(SchedulerWireError::StorageAggregateMismatch);
    }
    Ok(())
}

const fn storage_priority(state: SchedulerStorageStateDto) -> u8 {
    match state {
        SchedulerStorageStateDto::Normal => 0,
        SchedulerStorageStateDto::Pressure => 1,
        SchedulerStorageStateDto::Unavailable => 2,
        SchedulerStorageStateDto::Critical => 3,
    }
}
