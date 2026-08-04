use std::fmt;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

use crate::platform::{PlatformPaths, PrivateFileReadError, read_private_file_bounded};

pub const RUNTIME_CONFIG_INVALID: &str = "RUNTIME_CONFIG_INVALID";
pub const MAX_RUNTIME_CONFIG_BYTES: usize = 16 * 1024;

const RUNTIME_CONFIG_SCHEMA_VERSION: u32 = 1;
const MAX_CONCURRENT_TASKS: u32 = 4;
const MAX_QUEUED_TASKS: u32 = 256;
const MAX_CARGO_JOBS_PER_TASK: usize = 8;
const DEFAULT_MAX_CONCURRENT_TASKS: u32 = 2;
const DEFAULT_MAX_CONCURRENT_TASKS_PER_REPOSITORY: u32 = 2;
const DEFAULT_MAX_QUEUED_TASKS: u32 = 32;
const DEFAULT_STORAGE_RESERVE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeConfig {
    max_concurrent_tasks: NonZeroU32,
    max_concurrent_tasks_per_repository: NonZeroU32,
    max_queued_tasks: NonZeroU32,
    storage: RuntimeStorageConfig,
    cargo_jobs_per_task: NonZeroU32,
}

impl RuntimeConfig {
    pub const fn max_concurrent_tasks(&self) -> NonZeroU32 {
        self.max_concurrent_tasks
    }

    pub const fn max_concurrent_tasks_per_repository(&self) -> NonZeroU32 {
        self.max_concurrent_tasks_per_repository
    }

    pub const fn max_queued_tasks(&self) -> NonZeroU32 {
        self.max_queued_tasks
    }

    pub const fn storage(&self) -> &RuntimeStorageConfig {
        &self.storage
    }

    pub const fn cargo_jobs_per_task(&self) -> NonZeroU32 {
        self.cargo_jobs_per_task
    }

    fn defaults(available_parallelism: Option<NonZeroUsize>) -> Self {
        Self::try_from_raw(
            RawRuntimeConfig {
                schema_version: RUNTIME_CONFIG_SCHEMA_VERSION,
                max_concurrent_tasks: DEFAULT_MAX_CONCURRENT_TASKS,
                max_concurrent_tasks_per_repository: DEFAULT_MAX_CONCURRENT_TASKS_PER_REPOSITORY,
                max_queued_tasks: DEFAULT_MAX_QUEUED_TASKS,
                storage: RawRuntimeStorageConfig {
                    data_control_reserve_bytes: DEFAULT_STORAGE_RESERVE_BYTES,
                    data_task_reservation_bytes: DEFAULT_STORAGE_RESERVE_BYTES,
                },
            },
            available_parallelism,
        )
        .expect("recorded runtime defaults satisfy their own validation")
    }

    fn from_json(
        encoded: &[u8],
        available_parallelism: Option<NonZeroUsize>,
    ) -> Result<Self, RuntimeConfigLoadError> {
        let mut deserializer = serde_json::Deserializer::from_slice(encoded);
        let raw = RawRuntimeConfig::deserialize(&mut deserializer)
            .map_err(|_| RuntimeConfigLoadError::new(RuntimeConfigLoadErrorKind::Invalid))?;
        deserializer
            .end()
            .map_err(|_| RuntimeConfigLoadError::new(RuntimeConfigLoadErrorKind::Invalid))?;
        Self::try_from_raw(raw, available_parallelism)
    }

    fn try_from_raw(
        raw: RawRuntimeConfig,
        available_parallelism: Option<NonZeroUsize>,
    ) -> Result<Self, RuntimeConfigLoadError> {
        if raw.schema_version != RUNTIME_CONFIG_SCHEMA_VERSION {
            return Err(RuntimeConfigLoadError::invalid());
        }

        let max_concurrent_tasks =
            bounded_nonzero_u32(raw.max_concurrent_tasks, MAX_CONCURRENT_TASKS)?;
        let max_concurrent_tasks_per_repository = bounded_nonzero_u32(
            raw.max_concurrent_tasks_per_repository,
            MAX_CONCURRENT_TASKS,
        )?;
        if max_concurrent_tasks_per_repository > max_concurrent_tasks {
            return Err(RuntimeConfigLoadError::invalid());
        }
        let max_queued_tasks = bounded_nonzero_u32(raw.max_queued_tasks, MAX_QUEUED_TASKS)?;
        let data_control_reserve_bytes = NonZeroU64::new(raw.storage.data_control_reserve_bytes)
            .ok_or_else(RuntimeConfigLoadError::invalid)?;
        let data_task_reservation_bytes = NonZeroU64::new(raw.storage.data_task_reservation_bytes)
            .ok_or_else(RuntimeConfigLoadError::invalid)?;

        data_task_reservation_bytes
            .get()
            .checked_mul(u64::from(max_concurrent_tasks.get()))
            .and_then(|reservation| data_control_reserve_bytes.get().checked_add(reservation))
            .ok_or_else(RuntimeConfigLoadError::invalid)?;

        Ok(Self {
            max_concurrent_tasks,
            max_concurrent_tasks_per_repository,
            max_queued_tasks,
            storage: RuntimeStorageConfig {
                data_control_reserve_bytes,
                data_task_reservation_bytes,
            },
            cargo_jobs_per_task: derive_cargo_jobs_per_task(
                available_parallelism,
                max_concurrent_tasks,
            ),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeStorageConfig {
    data_control_reserve_bytes: NonZeroU64,
    data_task_reservation_bytes: NonZeroU64,
}

impl RuntimeStorageConfig {
    pub const fn data_control_reserve_bytes(&self) -> NonZeroU64 {
        self.data_control_reserve_bytes
    }

    pub const fn data_task_reservation_bytes(&self) -> NonZeroU64 {
        self.data_task_reservation_bytes
    }
}

struct RawRuntimeConfig {
    schema_version: u32,
    max_concurrent_tasks: u32,
    max_concurrent_tasks_per_repository: u32,
    max_queued_tasks: u32,
    storage: RawRuntimeStorageConfig,
}

struct RawRuntimeStorageConfig {
    data_control_reserve_bytes: u64,
    data_task_reservation_bytes: u64,
}

const RUNTIME_CONFIG_FIELDS: &[&str] = &[
    "schema_version",
    "max_concurrent_tasks",
    "max_concurrent_tasks_per_repository",
    "max_queued_tasks",
    "storage",
];

enum RuntimeConfigField {
    SchemaVersion,
    MaxConcurrentTasks,
    MaxConcurrentTasksPerRepository,
    MaxQueuedTasks,
    Storage,
}

impl<'de> Deserialize<'de> for RuntimeConfigField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RuntimeConfigFieldVisitor;

        impl Visitor<'_> for RuntimeConfigFieldVisitor {
            type Value = RuntimeConfigField;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a runtime configuration field")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match value {
                    "schema_version" => Ok(RuntimeConfigField::SchemaVersion),
                    "max_concurrent_tasks" => Ok(RuntimeConfigField::MaxConcurrentTasks),
                    "max_concurrent_tasks_per_repository" => {
                        Ok(RuntimeConfigField::MaxConcurrentTasksPerRepository)
                    }
                    "max_queued_tasks" => Ok(RuntimeConfigField::MaxQueuedTasks),
                    "storage" => Ok(RuntimeConfigField::Storage),
                    _ => Err(de::Error::unknown_field(value, RUNTIME_CONFIG_FIELDS)),
                }
            }
        }

        deserializer.deserialize_identifier(RuntimeConfigFieldVisitor)
    }
}

impl<'de> Deserialize<'de> for RawRuntimeConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RawRuntimeConfigVisitor;

        impl<'de> Visitor<'de> for RawRuntimeConfigVisitor {
            type Value = RawRuntimeConfig;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an exact runtime configuration object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut schema_version = None;
                let mut max_concurrent_tasks = None;
                let mut max_concurrent_tasks_per_repository = None;
                let mut max_queued_tasks = None;
                let mut storage = None;

                while let Some(field) = map.next_key()? {
                    match field {
                        RuntimeConfigField::SchemaVersion => {
                            if schema_version.is_some() {
                                return Err(de::Error::duplicate_field("schema_version"));
                            }
                            schema_version = Some(map.next_value()?);
                        }
                        RuntimeConfigField::MaxConcurrentTasks => {
                            if max_concurrent_tasks.is_some() {
                                return Err(de::Error::duplicate_field("max_concurrent_tasks"));
                            }
                            max_concurrent_tasks = Some(map.next_value()?);
                        }
                        RuntimeConfigField::MaxConcurrentTasksPerRepository => {
                            if max_concurrent_tasks_per_repository.is_some() {
                                return Err(de::Error::duplicate_field(
                                    "max_concurrent_tasks_per_repository",
                                ));
                            }
                            max_concurrent_tasks_per_repository = Some(map.next_value()?);
                        }
                        RuntimeConfigField::MaxQueuedTasks => {
                            if max_queued_tasks.is_some() {
                                return Err(de::Error::duplicate_field("max_queued_tasks"));
                            }
                            max_queued_tasks = Some(map.next_value()?);
                        }
                        RuntimeConfigField::Storage => {
                            if storage.is_some() {
                                return Err(de::Error::duplicate_field("storage"));
                            }
                            storage = Some(map.next_value()?);
                        }
                    }
                }

                Ok(RawRuntimeConfig {
                    schema_version: schema_version
                        .ok_or_else(|| de::Error::missing_field("schema_version"))?,
                    max_concurrent_tasks: max_concurrent_tasks
                        .ok_or_else(|| de::Error::missing_field("max_concurrent_tasks"))?,
                    max_concurrent_tasks_per_repository: max_concurrent_tasks_per_repository
                        .ok_or_else(|| {
                            de::Error::missing_field("max_concurrent_tasks_per_repository")
                        })?,
                    max_queued_tasks: max_queued_tasks
                        .ok_or_else(|| de::Error::missing_field("max_queued_tasks"))?,
                    storage: storage.ok_or_else(|| de::Error::missing_field("storage"))?,
                })
            }
        }

        deserializer.deserialize_map(RawRuntimeConfigVisitor)
    }
}

const RUNTIME_STORAGE_FIELDS: &[&str] =
    &["data_control_reserve_bytes", "data_task_reservation_bytes"];

enum RuntimeStorageField {
    DataControlReserveBytes,
    DataTaskReservationBytes,
}

impl<'de> Deserialize<'de> for RuntimeStorageField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RuntimeStorageFieldVisitor;

        impl Visitor<'_> for RuntimeStorageFieldVisitor {
            type Value = RuntimeStorageField;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a runtime storage configuration field")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match value {
                    "data_control_reserve_bytes" => {
                        Ok(RuntimeStorageField::DataControlReserveBytes)
                    }
                    "data_task_reservation_bytes" => {
                        Ok(RuntimeStorageField::DataTaskReservationBytes)
                    }
                    _ => Err(de::Error::unknown_field(value, RUNTIME_STORAGE_FIELDS)),
                }
            }
        }

        deserializer.deserialize_identifier(RuntimeStorageFieldVisitor)
    }
}

impl<'de> Deserialize<'de> for RawRuntimeStorageConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RawRuntimeStorageConfigVisitor;

        impl<'de> Visitor<'de> for RawRuntimeStorageConfigVisitor {
            type Value = RawRuntimeStorageConfig;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an exact runtime storage configuration object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut data_control_reserve_bytes = None;
                let mut data_task_reservation_bytes = None;

                while let Some(field) = map.next_key()? {
                    match field {
                        RuntimeStorageField::DataControlReserveBytes => {
                            if data_control_reserve_bytes.is_some() {
                                return Err(de::Error::duplicate_field(
                                    "data_control_reserve_bytes",
                                ));
                            }
                            data_control_reserve_bytes = Some(map.next_value()?);
                        }
                        RuntimeStorageField::DataTaskReservationBytes => {
                            if data_task_reservation_bytes.is_some() {
                                return Err(de::Error::duplicate_field(
                                    "data_task_reservation_bytes",
                                ));
                            }
                            data_task_reservation_bytes = Some(map.next_value()?);
                        }
                    }
                }

                Ok(RawRuntimeStorageConfig {
                    data_control_reserve_bytes: data_control_reserve_bytes
                        .ok_or_else(|| de::Error::missing_field("data_control_reserve_bytes"))?,
                    data_task_reservation_bytes: data_task_reservation_bytes
                        .ok_or_else(|| de::Error::missing_field("data_task_reservation_bytes"))?,
                })
            }
        }

        deserializer.deserialize_map(RawRuntimeStorageConfigVisitor)
    }
}

fn bounded_nonzero_u32(value: u32, maximum: u32) -> Result<NonZeroU32, RuntimeConfigLoadError> {
    let value = NonZeroU32::new(value).ok_or_else(RuntimeConfigLoadError::invalid)?;
    if value.get() > maximum {
        return Err(RuntimeConfigLoadError::invalid());
    }
    Ok(value)
}

fn derive_cargo_jobs_per_task(
    available_parallelism: Option<NonZeroUsize>,
    max_concurrent_tasks: NonZeroU32,
) -> NonZeroU32 {
    let available_parallelism = available_parallelism.map_or(1, NonZeroUsize::get);
    let configured_tasks = usize::try_from(max_concurrent_tasks.get())
        .expect("u32 task limit fits usize on supported platforms");
    let jobs = (available_parallelism / configured_tasks).clamp(1, MAX_CARGO_JOBS_PER_TASK);
    NonZeroU32::new(u32::try_from(jobs).expect("cargo jobs maximum fits u32"))
        .expect("cargo jobs are clamped to a nonzero value")
}

#[cfg(feature = "test-support")]
pub fn derive_cargo_jobs_per_task_for_test(
    available_parallelism: Option<NonZeroUsize>,
    max_concurrent_tasks: NonZeroU32,
) -> NonZeroU32 {
    derive_cargo_jobs_per_task(available_parallelism, max_concurrent_tasks)
}

pub fn load_runtime_config(paths: &PlatformPaths) -> Result<RuntimeConfig, RuntimeConfigLoadError> {
    load_runtime_config_with_parallelism(paths, std::thread::available_parallelism().ok())
}

#[cfg(feature = "test-support")]
pub fn load_runtime_config_for_test(
    paths: &PlatformPaths,
    available_parallelism: Option<NonZeroUsize>,
) -> Result<RuntimeConfig, RuntimeConfigLoadError> {
    load_runtime_config_with_parallelism(paths, available_parallelism)
}

pub(crate) fn load_runtime_config_with_parallelism(
    paths: &PlatformPaths,
    available_parallelism: Option<NonZeroUsize>,
) -> Result<RuntimeConfig, RuntimeConfigLoadError> {
    let encoded = match read_private_file_bounded(&paths.runtime_config, MAX_RUNTIME_CONFIG_BYTES) {
        Ok(encoded) => encoded,
        Err(PrivateFileReadError::Open(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RuntimeConfig::defaults(available_parallelism));
        }
        Err(error) => return Err(RuntimeConfigLoadError::from_private_file(error)),
    };
    RuntimeConfig::from_json(&encoded, available_parallelism)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeConfigLoadErrorKind {
    NotPrivate,
    TooLarge,
    Io,
    Invalid,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct RuntimeConfigLoadError {
    kind: RuntimeConfigLoadErrorKind,
}

impl RuntimeConfigLoadError {
    const fn new(kind: RuntimeConfigLoadErrorKind) -> Self {
        Self { kind }
    }

    const fn invalid() -> Self {
        Self::new(RuntimeConfigLoadErrorKind::Invalid)
    }

    fn from_private_file(error: PrivateFileReadError) -> Self {
        let kind = match error {
            PrivateFileReadError::Open(error)
                if error.kind() == std::io::ErrorKind::PermissionDenied =>
            {
                RuntimeConfigLoadErrorKind::NotPrivate
            }
            PrivateFileReadError::NotPrivate(_) => RuntimeConfigLoadErrorKind::NotPrivate,
            PrivateFileReadError::TooLarge => RuntimeConfigLoadErrorKind::TooLarge,
            PrivateFileReadError::Open(_)
            | PrivateFileReadError::Metadata(_)
            | PrivateFileReadError::Read(_) => RuntimeConfigLoadErrorKind::Io,
        };
        Self::new(kind)
    }

    pub const fn kind(&self) -> RuntimeConfigLoadErrorKind {
        self.kind
    }

    pub const fn code(&self) -> &'static str {
        RUNTIME_CONFIG_INVALID
    }

    pub const fn retryable(&self) -> bool {
        false
    }
}

impl fmt::Debug for RuntimeConfigLoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeConfigLoadError")
            .field("code", &RUNTIME_CONFIG_INVALID)
            .field("kind", &self.kind)
            .finish()
    }
}

impl fmt::Display for RuntimeConfigLoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the runtime configuration is invalid")
    }
}

impl std::error::Error for RuntimeConfigLoadError {}
