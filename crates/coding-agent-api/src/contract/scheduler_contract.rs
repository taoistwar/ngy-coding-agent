use serde::Serialize;
use utoipa::ToSchema;

use super::UtcTimestampDto;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchedulerStateDto {
    #[schema(minimum = 1, maximum = 1)]
    pub schema_version: u16,
    #[schema(
        value_type = String,
        format = Uuid,
        pattern = "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    )]
    pub server_instance_id: uuid::Uuid,
    pub server_started_at: UtcTimestampDto,
    #[schema(minimum = 0, maximum = 9_007_199_254_740_991_u64)]
    pub generation: u64,
    #[schema(minimum = 0, maximum = 9_007_199_254_740_991_u64)]
    pub as_of_event_id: u64,
    #[schema(minimum = 0, maximum = 9_007_199_254_740_991_u64)]
    pub service_state_generation: u64,
    pub admission_state: SchedulerAdmissionStateDto,
    pub limits: SchedulerLimitsDto,
    #[schema(minimum = 0, maximum = 4)]
    pub active_task_count: u32,
    #[schema(minimum = 0, maximum = 4_294_967_295_u64)]
    pub queued_task_count: u32,
    pub queued_tasks: Vec<SchedulerQueuedTaskDto>,
    #[schema(max_items = 4)]
    pub stopping_tasks: Vec<SchedulerStoppingTaskDto>,
    pub storage: SchedulerStorageDto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerAdmissionStateDto {
    Running,
    Paused,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchedulerLimitsDto {
    #[schema(minimum = 1, maximum = 4)]
    pub global: u32,
    #[schema(minimum = 1, maximum = 4)]
    pub per_repository: u32,
    #[schema(minimum = 1, maximum = 256)]
    pub queued: u32,
    #[schema(minimum = 1, maximum = 8)]
    pub cargo_jobs_per_task: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchedulerQueuedTaskDto {
    #[schema(
        value_type = String,
        format = Uuid,
        pattern = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )]
    pub task_id: uuid::Uuid,
    pub reason: SchedulerQueueReasonDto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerQueueReasonDto {
    ServicePaused,
    StoragePressure,
    GlobalCapacity,
    RepositoryCapacity,
    RepositoryControlBusy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchedulerStoppingTaskDto {
    #[schema(
        value_type = String,
        format = Uuid,
        pattern = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )]
    pub task_id: uuid::Uuid,
    pub intent: SchedulerStopIntentDto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerStopIntentDto {
    UserCancelled,
    DiskPressureCritical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchedulerStorageDto {
    pub state: SchedulerStorageStateDto,
    pub data: SchedulerStorageScopeDto,
    pub runtime: SchedulerStorageScopeDto,
    pub repositories: Vec<SchedulerRepositoryStorageDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchedulerStorageScopeDto {
    pub state: SchedulerStorageStateDto,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchedulerRepositoryStorageDto {
    #[schema(
        value_type = String,
        format = Uuid,
        pattern = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )]
    pub repository_id: uuid::Uuid,
    pub state: SchedulerStorageStateDto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerStorageStateDto {
    Normal,
    Pressure,
    Critical,
    Unavailable,
}
