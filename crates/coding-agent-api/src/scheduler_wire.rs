use serde::Serialize;
use utoipa::openapi::Ref;
use utoipa::openapi::schema::{Discriminator, OneOfBuilder, Schema};
use utoipa::{PartialSchema, ToSchema};

use crate::contract::{
    SchedulerAdmissionStateDto, SchedulerLimitsDto, SchedulerQueueReasonDto, SchedulerStateDto,
    SchedulerStopIntentDto, SchedulerStorageScopeDto, SchedulerStorageStateDto, UtcTimestampDto,
};

mod canonical;
#[cfg(test)]
mod fixture_tests;
mod validation;

use canonical::{digest_validated_snapshot, scheduler_state_bytes_validated};
use validation::validate_snapshot;

pub const MAX_SCHEDULER_ITEMS_PER_CHUNK: usize = 128;
pub const MAX_SCHEDULER_FRAME_BYTES: usize = 64 * 1024;

pub(super) const SCHEDULER_SCHEMA_VERSION: u16 = 1;
pub(super) const JSON_SAFE_INTEGER_MAX: u64 = 9_007_199_254_740_991;
const SCHEDULER_STATE_EVENT_NAME: &str = "scheduler.state";
const SCHEDULER_STATE_CHUNK_EVENT_NAME: &str = "scheduler.state.chunk";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
pub enum SchedulerStateKind {
    #[serde(rename = "scheduler.state")]
    SchedulerState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchedulerControlStorageDto {
    pub state: SchedulerStorageStateDto,
    pub data: SchedulerStorageScopeDto,
    pub runtime: SchedulerStorageScopeDto,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchedulerStateControl {
    #[schema(minimum = 1, maximum = 1)]
    pub schema_version: u16,
    #[schema(value_type = SchedulerStateKind)]
    pub kind: SchedulerStateKind,
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
    #[schema(minimum = 0, maximum = 4)]
    pub stopping_task_count: u32,
    #[schema(minimum = 0, maximum = 4_294_967_295_u64)]
    pub repository_storage_count: u32,
    pub storage: SchedulerControlStorageDto,
    #[schema(minimum = 0, maximum = 4_294_967_295_u64)]
    pub item_count: u32,
    #[schema(minimum = 0, maximum = 4_294_967_295_u64)]
    pub chunk_count: u32,
    #[schema(pattern = "^[0-9a-f]{64}$")]
    pub snapshot_digest: String,
}

impl SchedulerStateControl {
    pub const fn event_name(&self) -> &'static str {
        SCHEDULER_STATE_EVENT_NAME
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
pub enum SchedulerStateChunkKind {
    #[serde(rename = "scheduler.state.chunk")]
    SchedulerStateChunk,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
pub enum SchedulerQueuedTaskItemKind {
    #[serde(rename = "queued_task")]
    QueuedTask,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchedulerQueuedTaskItemDto {
    pub kind: SchedulerQueuedTaskItemKind,
    #[schema(
        value_type = String,
        format = Uuid,
        pattern = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )]
    pub task_id: uuid::Uuid,
    pub reason: SchedulerQueueReasonDto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
pub enum SchedulerStoppingTaskItemKind {
    #[serde(rename = "stopping_task")]
    StoppingTask,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchedulerStoppingTaskItemDto {
    pub kind: SchedulerStoppingTaskItemKind,
    #[schema(
        value_type = String,
        format = Uuid,
        pattern = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )]
    pub task_id: uuid::Uuid,
    pub intent: SchedulerStopIntentDto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
pub enum SchedulerRepositoryStorageItemKind {
    #[serde(rename = "repository_storage")]
    RepositoryStorage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchedulerRepositoryStorageItemDto {
    pub kind: SchedulerRepositoryStorageItemKind,
    #[schema(
        value_type = String,
        format = Uuid,
        pattern = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )]
    pub repository_id: uuid::Uuid,
    pub state: SchedulerStorageStateDto,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum SchedulerStateItemDto {
    QueuedTask(SchedulerQueuedTaskItemDto),
    StoppingTask(SchedulerStoppingTaskItemDto),
    RepositoryStorage(SchedulerRepositoryStorageItemDto),
}

impl PartialSchema for SchedulerStateItemDto {
    fn schema() -> utoipa::openapi::RefOr<Schema> {
        OneOfBuilder::new()
            .item(Ref::from_schema_name("SchedulerQueuedTaskItemDto"))
            .item(Ref::from_schema_name("SchedulerStoppingTaskItemDto"))
            .item(Ref::from_schema_name("SchedulerRepositoryStorageItemDto"))
            .discriminator(Some(Discriminator::with_mapping(
                "kind",
                [
                    (
                        "queued_task",
                        "#/components/schemas/SchedulerQueuedTaskItemDto",
                    ),
                    (
                        "stopping_task",
                        "#/components/schemas/SchedulerStoppingTaskItemDto",
                    ),
                    (
                        "repository_storage",
                        "#/components/schemas/SchedulerRepositoryStorageItemDto",
                    ),
                ],
            )))
            .into()
    }
}

impl ToSchema for SchedulerStateItemDto {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchedulerStateChunkControl {
    #[schema(minimum = 1, maximum = 1)]
    pub schema_version: u16,
    #[schema(value_type = SchedulerStateChunkKind)]
    pub kind: SchedulerStateChunkKind,
    #[schema(
        value_type = String,
        format = Uuid,
        pattern = "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    )]
    pub server_instance_id: uuid::Uuid,
    #[schema(minimum = 0, maximum = 9_007_199_254_740_991_u64)]
    pub generation: u64,
    #[schema(pattern = "^[0-9a-f]{64}$")]
    pub snapshot_digest: String,
    #[schema(minimum = 0, maximum = 4_294_967_295_u64)]
    pub chunk_index: u32,
    #[schema(minimum = 1, maximum = 4_294_967_295_u64)]
    pub chunk_count: u32,
    #[schema(min_items = 1, max_items = 128)]
    pub items: Vec<SchedulerStateItemDto>,
}

impl SchedulerStateChunkControl {
    pub const fn event_name(&self) -> &'static str {
        SCHEDULER_STATE_CHUNK_EVENT_NAME
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerStateFrames {
    manifest: SchedulerStateControl,
    chunks: Vec<SchedulerStateChunkControl>,
}

impl SchedulerStateFrames {
    pub fn try_from_snapshot(snapshot: &SchedulerStateDto) -> Result<Self, SchedulerWireError> {
        let counts = validate_snapshot(snapshot)?;
        let snapshot_digest = digest_validated_snapshot(snapshot)?;
        let items = canonical_items(snapshot);
        let chunk_count = u32::try_from(items.len().div_ceil(MAX_SCHEDULER_ITEMS_PER_CHUNK))
            .map_err(|_| SchedulerWireError::ItemCountOverflow)?;

        let manifest = SchedulerStateControl {
            schema_version: SCHEDULER_SCHEMA_VERSION,
            kind: SchedulerStateKind::SchedulerState,
            server_instance_id: snapshot.server_instance_id,
            server_started_at: snapshot.server_started_at.clone(),
            generation: snapshot.generation,
            as_of_event_id: snapshot.as_of_event_id,
            service_state_generation: snapshot.service_state_generation,
            admission_state: snapshot.admission_state,
            limits: snapshot.limits.clone(),
            active_task_count: snapshot.active_task_count,
            queued_task_count: snapshot.queued_task_count,
            stopping_task_count: counts.stopping,
            repository_storage_count: counts.repositories,
            storage: SchedulerControlStorageDto {
                state: snapshot.storage.state,
                data: snapshot.storage.data.clone(),
                runtime: snapshot.storage.runtime.clone(),
            },
            item_count: counts.items,
            chunk_count,
            snapshot_digest: snapshot_digest.clone(),
        };
        ensure_scheduler_state_frame_size(&manifest)?;

        let chunks = items
            .chunks(MAX_SCHEDULER_ITEMS_PER_CHUNK)
            .enumerate()
            .map(|(index, items)| {
                let chunk = SchedulerStateChunkControl {
                    schema_version: SCHEDULER_SCHEMA_VERSION,
                    kind: SchedulerStateChunkKind::SchedulerStateChunk,
                    server_instance_id: snapshot.server_instance_id,
                    generation: snapshot.generation,
                    snapshot_digest: snapshot_digest.clone(),
                    chunk_index: u32::try_from(index)
                        .map_err(|_| SchedulerWireError::ItemCountOverflow)?,
                    chunk_count,
                    items: items.to_vec(),
                };
                ensure_scheduler_state_chunk_frame_size(&chunk)?;
                Ok(chunk)
            })
            .collect::<Result<Vec<_>, SchedulerWireError>>()?;

        Ok(Self { manifest, chunks })
    }

    pub const fn manifest(&self) -> &SchedulerStateControl {
        &self.manifest
    }

    pub fn chunks(&self) -> &[SchedulerStateChunkControl] {
        &self.chunks
    }

    pub fn into_parts(self) -> (SchedulerStateControl, Vec<SchedulerStateChunkControl>) {
        (self.manifest, self.chunks)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SchedulerWireError {
    #[error("scheduler snapshot schema_version must be 1")]
    InvalidSchemaVersion { actual: u16 },
    #[error("scheduler server_instance_id must be an RFC 4122 UUID v4")]
    InvalidServerInstanceId,
    #[error("scheduler integer exceeds the JSON safe integer range: {field}")]
    UnsafeInteger { field: &'static str, value: u64 },
    #[error("scheduler limit is outside its approved range: {field}")]
    InvalidLimit { field: &'static str, value: u32 },
    #[error("scheduler per-repository limit exceeds the global limit")]
    RepositoryLimitExceedsGlobal { per_repository: u32, global: u32 },
    #[error("scheduler active task count exceeds the global limit")]
    ActiveTaskCountExceedsGlobal { active: u32, global: u32 },
    #[error("scheduler queued task count does not match the item array")]
    QueuedTaskCountMismatch { declared: u32, actual: u32 },
    #[error("scheduler stopping task count exceeds the active task count")]
    StoppingTaskCountExceedsActive { stopping: u32, active: u32 },
    #[error("scheduler snapshot contains duplicate or overlapping task ids")]
    DuplicateTaskId,
    #[error("scheduler snapshot contains duplicate repository ids")]
    DuplicateRepositoryId,
    #[error("scheduler repository storage is not in canonical repository-id order")]
    RepositoryStorageNotCanonical,
    #[error("scheduler aggregate storage state does not match its scopes")]
    StorageAggregateMismatch,
    #[error("scheduler item count exceeds the wire count range")]
    ItemCountOverflow,
    #[error("scheduler wire JSON serialization failed")]
    JsonSerialization,
    #[error("scheduler SSE frame exceeds the 64 KiB wire limit: {kind}")]
    FrameTooLarge {
        kind: &'static str,
        encoded_bytes: usize,
    },
}

pub fn canonical_scheduler_state_bytes(
    snapshot: &SchedulerStateDto,
) -> Result<Vec<u8>, SchedulerWireError> {
    validate_snapshot(snapshot)?;
    scheduler_state_bytes_validated(snapshot)
}

pub fn scheduler_snapshot_digest(
    snapshot: &SchedulerStateDto,
) -> Result<String, SchedulerWireError> {
    validate_snapshot(snapshot)?;
    digest_validated_snapshot(snapshot)
}

pub fn scheduler_state_frame_len(
    control: &SchedulerStateControl,
) -> Result<usize, SchedulerWireError> {
    serialized_sse_frame_len(SCHEDULER_STATE_EVENT_NAME, control)
}

pub fn scheduler_state_chunk_frame_len(
    control: &SchedulerStateChunkControl,
) -> Result<usize, SchedulerWireError> {
    serialized_sse_frame_len(SCHEDULER_STATE_CHUNK_EVENT_NAME, control)
}

pub fn ensure_scheduler_state_frame_size(
    control: &SchedulerStateControl,
) -> Result<(), SchedulerWireError> {
    ensure_frame_size(
        SCHEDULER_STATE_EVENT_NAME,
        scheduler_state_frame_len(control)?,
    )
}

pub fn ensure_scheduler_state_chunk_frame_size(
    control: &SchedulerStateChunkControl,
) -> Result<(), SchedulerWireError> {
    ensure_frame_size(
        SCHEDULER_STATE_CHUNK_EVENT_NAME,
        scheduler_state_chunk_frame_len(control)?,
    )
}

fn canonical_items(snapshot: &SchedulerStateDto) -> Vec<SchedulerStateItemDto> {
    snapshot
        .queued_tasks
        .iter()
        .map(|task| {
            SchedulerStateItemDto::QueuedTask(SchedulerQueuedTaskItemDto {
                kind: SchedulerQueuedTaskItemKind::QueuedTask,
                task_id: task.task_id,
                reason: task.reason,
            })
        })
        .chain(snapshot.stopping_tasks.iter().map(|task| {
            SchedulerStateItemDto::StoppingTask(SchedulerStoppingTaskItemDto {
                kind: SchedulerStoppingTaskItemKind::StoppingTask,
                task_id: task.task_id,
                intent: task.intent,
            })
        }))
        .chain(snapshot.storage.repositories.iter().map(|repository| {
            SchedulerStateItemDto::RepositoryStorage(SchedulerRepositoryStorageItemDto {
                kind: SchedulerRepositoryStorageItemKind::RepositoryStorage,
                repository_id: repository.repository_id,
                state: repository.state,
            })
        }))
        .collect()
}

fn serialized_sse_frame_len(
    event_name: &'static str,
    value: &impl Serialize,
) -> Result<usize, SchedulerWireError> {
    let json = serde_json::to_vec(value).map_err(|_| SchedulerWireError::JsonSerialization)?;
    // axum's Event::event(...).data(...) renders exactly:
    // `event: <name>\ndata: <single-line-json>\n\n`.
    Ok(b"event: ".len() + event_name.len() + b"\ndata: ".len() + json.len() + b"\n\n".len())
}

fn ensure_frame_size(kind: &'static str, encoded_bytes: usize) -> Result<(), SchedulerWireError> {
    if encoded_bytes <= MAX_SCHEDULER_FRAME_BYTES {
        Ok(())
    } else {
        Err(SchedulerWireError::FrameTooLarge {
            kind,
            encoded_bytes,
        })
    }
}
