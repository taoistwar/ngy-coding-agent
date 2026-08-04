use std::fmt::Write as _;

use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::contract::{
    SchedulerLimitsDto, SchedulerQueuedTaskDto, SchedulerRepositoryStorageDto, SchedulerStateDto,
    SchedulerStoppingTaskDto, SchedulerStorageDto, SchedulerStorageScopeDto,
};

use super::SchedulerWireError;

pub(super) fn digest_validated_snapshot(
    snapshot: &SchedulerStateDto,
) -> Result<String, SchedulerWireError> {
    let canonical = scheduler_state_bytes_validated(snapshot)?;
    let hash = Sha256::digest(canonical);
    let mut digest = String::with_capacity(64);
    for byte in hash {
        write!(&mut digest, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(digest)
}

pub(super) fn scheduler_state_bytes_validated(
    snapshot: &SchedulerStateDto,
) -> Result<Vec<u8>, SchedulerWireError> {
    let mut output = Vec::new();
    write_scheduler_state(&mut output, snapshot)?;
    Ok(output)
}

fn write_scheduler_state(
    output: &mut Vec<u8>,
    snapshot: &SchedulerStateDto,
) -> Result<(), SchedulerWireError> {
    let SchedulerStateDto {
        schema_version: _,
        server_instance_id: _,
        server_started_at: _,
        generation: _,
        as_of_event_id: _,
        service_state_generation: _,
        admission_state: _,
        limits: _,
        active_task_count: _,
        queued_task_count: _,
        queued_tasks: _,
        stopping_tasks: _,
        storage: _,
    } = snapshot;
    output.push(b'{');
    let mut first = true;
    write_property(output, &mut first, "active_task_count", |output| {
        write_scalar(output, &snapshot.active_task_count)
    })?;
    write_property(output, &mut first, "admission_state", |output| {
        write_scalar(output, &snapshot.admission_state)
    })?;
    write_property(output, &mut first, "as_of_event_id", |output| {
        write_scalar(output, &snapshot.as_of_event_id)
    })?;
    write_property(output, &mut first, "generation", |output| {
        write_scalar(output, &snapshot.generation)
    })?;
    write_property(output, &mut first, "limits", |output| {
        write_limits(output, &snapshot.limits)
    })?;
    write_property(output, &mut first, "queued_task_count", |output| {
        write_scalar(output, &snapshot.queued_task_count)
    })?;
    write_property(output, &mut first, "queued_tasks", |output| {
        write_array(output, &snapshot.queued_tasks, write_queued_task)
    })?;
    write_property(output, &mut first, "schema_version", |output| {
        write_scalar(output, &snapshot.schema_version)
    })?;
    write_property(output, &mut first, "server_instance_id", |output| {
        write_scalar(output, &snapshot.server_instance_id)
    })?;
    write_property(output, &mut first, "server_started_at", |output| {
        write_scalar(output, &snapshot.server_started_at)
    })?;
    write_property(output, &mut first, "service_state_generation", |output| {
        write_scalar(output, &snapshot.service_state_generation)
    })?;
    write_property(output, &mut first, "stopping_tasks", |output| {
        write_array(output, &snapshot.stopping_tasks, write_stopping_task)
    })?;
    write_property(output, &mut first, "storage", |output| {
        write_storage(output, snapshot)
    })?;
    output.push(b'}');
    Ok(())
}

fn write_limits(
    output: &mut Vec<u8>,
    limits: &SchedulerLimitsDto,
) -> Result<(), SchedulerWireError> {
    let SchedulerLimitsDto {
        global: _,
        per_repository: _,
        queued: _,
        cargo_jobs_per_task: _,
    } = limits;
    output.push(b'{');
    let mut first = true;
    write_property(output, &mut first, "cargo_jobs_per_task", |output| {
        write_scalar(output, &limits.cargo_jobs_per_task)
    })?;
    write_property(output, &mut first, "global", |output| {
        write_scalar(output, &limits.global)
    })?;
    write_property(output, &mut first, "per_repository", |output| {
        write_scalar(output, &limits.per_repository)
    })?;
    write_property(output, &mut first, "queued", |output| {
        write_scalar(output, &limits.queued)
    })?;
    output.push(b'}');
    Ok(())
}

fn write_queued_task(
    output: &mut Vec<u8>,
    task: &SchedulerQueuedTaskDto,
) -> Result<(), SchedulerWireError> {
    let SchedulerQueuedTaskDto {
        task_id: _,
        reason: _,
    } = task;
    output.push(b'{');
    let mut first = true;
    write_property(output, &mut first, "reason", |output| {
        write_scalar(output, &task.reason)
    })?;
    write_property(output, &mut first, "task_id", |output| {
        write_scalar(output, &task.task_id)
    })?;
    output.push(b'}');
    Ok(())
}

fn write_stopping_task(
    output: &mut Vec<u8>,
    task: &SchedulerStoppingTaskDto,
) -> Result<(), SchedulerWireError> {
    let SchedulerStoppingTaskDto {
        task_id: _,
        intent: _,
    } = task;
    output.push(b'{');
    let mut first = true;
    write_property(output, &mut first, "intent", |output| {
        write_scalar(output, &task.intent)
    })?;
    write_property(output, &mut first, "task_id", |output| {
        write_scalar(output, &task.task_id)
    })?;
    output.push(b'}');
    Ok(())
}

fn write_storage(
    output: &mut Vec<u8>,
    snapshot: &SchedulerStateDto,
) -> Result<(), SchedulerWireError> {
    let SchedulerStorageDto {
        state: _,
        data: _,
        runtime: _,
        repositories: _,
    } = &snapshot.storage;
    output.push(b'{');
    let mut first = true;
    write_property(output, &mut first, "data", |output| {
        write_storage_scope(output, &snapshot.storage.data)
    })?;
    write_property(output, &mut first, "repositories", |output| {
        write_array(
            output,
            &snapshot.storage.repositories,
            write_repository_storage,
        )
    })?;
    write_property(output, &mut first, "runtime", |output| {
        write_storage_scope(output, &snapshot.storage.runtime)
    })?;
    write_property(output, &mut first, "state", |output| {
        write_scalar(output, &snapshot.storage.state)
    })?;
    output.push(b'}');
    Ok(())
}

fn write_storage_scope(
    output: &mut Vec<u8>,
    scope: &SchedulerStorageScopeDto,
) -> Result<(), SchedulerWireError> {
    let SchedulerStorageScopeDto { state: _ } = scope;
    output.push(b'{');
    let mut first = true;
    write_property(output, &mut first, "state", |output| {
        write_scalar(output, &scope.state)
    })?;
    output.push(b'}');
    Ok(())
}

fn write_repository_storage(
    output: &mut Vec<u8>,
    repository: &SchedulerRepositoryStorageDto,
) -> Result<(), SchedulerWireError> {
    let SchedulerRepositoryStorageDto {
        repository_id: _,
        state: _,
    } = repository;
    output.push(b'{');
    let mut first = true;
    write_property(output, &mut first, "repository_id", |output| {
        write_scalar(output, &repository.repository_id)
    })?;
    write_property(output, &mut first, "state", |output| {
        write_scalar(output, &repository.state)
    })?;
    output.push(b'}');
    Ok(())
}

fn write_array<T>(
    output: &mut Vec<u8>,
    items: &[T],
    write_item: fn(&mut Vec<u8>, &T) -> Result<(), SchedulerWireError>,
) -> Result<(), SchedulerWireError> {
    output.push(b'[');
    for (index, item) in items.iter().enumerate() {
        if index != 0 {
            output.push(b',');
        }
        write_item(output, item)?;
    }
    output.push(b']');
    Ok(())
}

fn write_property(
    output: &mut Vec<u8>,
    first: &mut bool,
    key: &'static str,
    write_value: impl FnOnce(&mut Vec<u8>) -> Result<(), SchedulerWireError>,
) -> Result<(), SchedulerWireError> {
    if !*first {
        output.push(b',');
    }
    *first = false;
    write_scalar(output, key)?;
    output.push(b':');
    write_value(output)
}

fn write_scalar<T: Serialize + ?Sized>(
    output: &mut Vec<u8>,
    value: &T,
) -> Result<(), SchedulerWireError> {
    serde_json::to_writer(output, value).map_err(|_| SchedulerWireError::JsonSerialization)
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::write_scalar;

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Fixture {
        snapshot: serde::de::IgnoredAny,
        canonical_json: String,
        sha256: String,
        unicode_string: UnicodeStringFixture,
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct UnicodeStringFixture {
        source: String,
        canonical_json: String,
    }

    #[test]
    fn scheduler_wire_rfc8785_string_encoding_matches_the_shared_unicode_probe() {
        let fixture: Fixture = serde_json::from_str(include_str!(
            "../../../../testdata/scheduler-state-rfc8785.json"
        ))
        .unwrap();
        let mut canonical = Vec::new();
        write_scalar(&mut canonical, &fixture.unicode_string.source).unwrap();

        assert_eq!(
            String::from_utf8(canonical).unwrap(),
            fixture.unicode_string.canonical_json
        );
        assert!(!fixture.canonical_json.is_empty());
        assert_eq!(fixture.sha256.len(), 64);
        let _ = fixture.snapshot;
    }
}
