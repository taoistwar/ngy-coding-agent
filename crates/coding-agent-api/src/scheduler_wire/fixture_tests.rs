use coding_agent_domain::UtcTimestamp;
use serde::Deserialize;

use crate::contract::{
    SchedulerAdmissionStateDto, SchedulerLimitsDto, SchedulerQueueReasonDto,
    SchedulerQueuedTaskDto, SchedulerRepositoryStorageDto, SchedulerStateDto,
    SchedulerStopIntentDto, SchedulerStoppingTaskDto, SchedulerStorageDto,
    SchedulerStorageScopeDto, SchedulerStorageStateDto,
};

use super::{canonical_scheduler_state_bytes, scheduler_snapshot_digest};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchedulerCanonicalFixture {
    snapshot: FixtureSchedulerState,
    canonical_json: String,
    sha256: String,
    unicode_string: FixtureUnicodeString,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureUnicodeString {
    source: String,
    canonical_json: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureSchedulerState {
    schema_version: u16,
    server_instance_id: uuid::Uuid,
    server_started_at: String,
    generation: u64,
    as_of_event_id: u64,
    service_state_generation: u64,
    admission_state: FixtureAdmissionState,
    limits: FixtureLimits,
    active_task_count: u32,
    queued_task_count: u32,
    queued_tasks: Vec<FixtureQueuedTask>,
    stopping_tasks: Vec<FixtureStoppingTask>,
    storage: FixtureStorage,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum FixtureAdmissionState {
    Running,
    Paused,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureLimits {
    global: u32,
    per_repository: u32,
    queued: u32,
    cargo_jobs_per_task: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureQueuedTask {
    task_id: uuid::Uuid,
    reason: FixtureQueueReason,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum FixtureQueueReason {
    ServicePaused,
    StoragePressure,
    GlobalCapacity,
    RepositoryCapacity,
    RepositoryControlBusy,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureStoppingTask {
    task_id: uuid::Uuid,
    intent: FixtureStopIntent,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum FixtureStopIntent {
    UserCancelled,
    DiskPressureCritical,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureStorage {
    state: FixtureStorageState,
    data: FixtureStorageScope,
    runtime: FixtureStorageScope,
    repositories: Vec<FixtureRepositoryStorage>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureStorageScope {
    state: FixtureStorageState,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureRepositoryStorage {
    repository_id: uuid::Uuid,
    state: FixtureStorageState,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FixtureStorageState {
    Normal,
    Pressure,
    Critical,
    Unavailable,
}

#[test]
fn scheduler_wire_shared_fixture_parses_as_an_exact_scheduler_state() {
    let fixture: SchedulerCanonicalFixture = serde_json::from_str(include_str!(
        "../../../../testdata/scheduler-state-rfc8785.json"
    ))
    .unwrap();
    let snapshot = fixture.snapshot.into_dto();

    assert_eq!(
        String::from_utf8(canonical_scheduler_state_bytes(&snapshot).unwrap()).unwrap(),
        fixture.canonical_json
    );
    assert_eq!(
        scheduler_snapshot_digest(&snapshot).unwrap(),
        fixture.sha256
    );
    assert!(!fixture.unicode_string.source.is_empty());
    assert!(!fixture.unicode_string.canonical_json.is_empty());
}

impl FixtureSchedulerState {
    fn into_dto(self) -> SchedulerStateDto {
        SchedulerStateDto {
            schema_version: self.schema_version,
            server_instance_id: self.server_instance_id,
            server_started_at: UtcTimestamp::parse_rfc3339(&self.server_started_at)
                .unwrap()
                .into(),
            generation: self.generation,
            as_of_event_id: self.as_of_event_id,
            service_state_generation: self.service_state_generation,
            admission_state: self.admission_state.into(),
            limits: self.limits.into(),
            active_task_count: self.active_task_count,
            queued_task_count: self.queued_task_count,
            queued_tasks: self
                .queued_tasks
                .into_iter()
                .map(FixtureQueuedTask::into_dto)
                .collect(),
            stopping_tasks: self
                .stopping_tasks
                .into_iter()
                .map(FixtureStoppingTask::into_dto)
                .collect(),
            storage: self.storage.into_dto(),
        }
    }
}

impl From<FixtureAdmissionState> for SchedulerAdmissionStateDto {
    fn from(value: FixtureAdmissionState) -> Self {
        match value {
            FixtureAdmissionState::Running => Self::Running,
            FixtureAdmissionState::Paused => Self::Paused,
        }
    }
}

impl From<FixtureLimits> for SchedulerLimitsDto {
    fn from(value: FixtureLimits) -> Self {
        Self {
            global: value.global,
            per_repository: value.per_repository,
            queued: value.queued,
            cargo_jobs_per_task: value.cargo_jobs_per_task,
        }
    }
}

impl FixtureQueuedTask {
    fn into_dto(self) -> SchedulerQueuedTaskDto {
        SchedulerQueuedTaskDto {
            task_id: self.task_id,
            reason: self.reason.into(),
        }
    }
}

impl From<FixtureQueueReason> for SchedulerQueueReasonDto {
    fn from(value: FixtureQueueReason) -> Self {
        match value {
            FixtureQueueReason::ServicePaused => Self::ServicePaused,
            FixtureQueueReason::StoragePressure => Self::StoragePressure,
            FixtureQueueReason::GlobalCapacity => Self::GlobalCapacity,
            FixtureQueueReason::RepositoryCapacity => Self::RepositoryCapacity,
            FixtureQueueReason::RepositoryControlBusy => Self::RepositoryControlBusy,
        }
    }
}

impl FixtureStoppingTask {
    fn into_dto(self) -> SchedulerStoppingTaskDto {
        SchedulerStoppingTaskDto {
            task_id: self.task_id,
            intent: self.intent.into(),
        }
    }
}

impl From<FixtureStopIntent> for SchedulerStopIntentDto {
    fn from(value: FixtureStopIntent) -> Self {
        match value {
            FixtureStopIntent::UserCancelled => Self::UserCancelled,
            FixtureStopIntent::DiskPressureCritical => Self::DiskPressureCritical,
        }
    }
}

impl FixtureStorage {
    fn into_dto(self) -> SchedulerStorageDto {
        SchedulerStorageDto {
            state: self.state.into(),
            data: SchedulerStorageScopeDto {
                state: self.data.state.into(),
            },
            runtime: SchedulerStorageScopeDto {
                state: self.runtime.state.into(),
            },
            repositories: self
                .repositories
                .into_iter()
                .map(|repository| SchedulerRepositoryStorageDto {
                    repository_id: repository.repository_id,
                    state: repository.state.into(),
                })
                .collect(),
        }
    }
}

impl From<FixtureStorageState> for SchedulerStorageStateDto {
    fn from(value: FixtureStorageState) -> Self {
        match value {
            FixtureStorageState::Normal => Self::Normal,
            FixtureStorageState::Pressure => Self::Pressure,
            FixtureStorageState::Critical => Self::Critical,
            FixtureStorageState::Unavailable => Self::Unavailable,
        }
    }
}
