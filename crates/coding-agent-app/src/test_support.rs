//! Process-scoped fault configuration for real-process tests.
//!
//! This entire module is compiled only with the `test-support` feature. The
//! production binary therefore has neither environment overrides nor a JSON
//! scenario parser.

use std::collections::{HashMap, HashSet};
use std::env;
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use sqlx::Connection as _;
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use tokio::sync::{OnceCell, watch};

use crate::PrivateFile;
use crate::platform::{create_private_directory, harden_private_file};
use crate::{
    BrowserLaunchError, BrowserOpener, FakeRunnerConfig, FakeScenario, FixedStartupRunnerFactory,
    NativeMessageSink, PlatformPaths, ScriptedFakeRunner, StartupDependencies, StartupPaths,
    StoreFactory, StoreWriterFaultPoint, StoreWriterFaultSpec, StoreWriterTestController,
};
use coding_agent_domain::{CanonicalPath, NewRepository};
use coding_agent_store::{Store, StoreError};

mod delivery;
mod process_storage;

use delivery::ProcessOfflineDeliveryRuntime;
pub use delivery::{
    ProcessDeliveryProcessFault, ProcessDeliveryProviderScenario, ProcessRunnerMode,
};
pub use process_storage::ProcessStorageSample;
use process_storage::ProcessVolumeSampler;

pub const TEST_APP_DATA_ENV: &str = "CODING_AGENT_TEST_APP_DATA_DIR";
pub const TEST_RUNTIME_ENV: &str = "CODING_AGENT_TEST_RUNTIME_DIR";
pub const TEST_SCENARIO_ENV: &str = "CODING_AGENT_TEST_SCENARIO";

const MAX_SCENARIO_BYTES: u64 = 1024 * 1024;
const MAX_SIGNAL_NAME_BYTES: usize = 64;
const SIGNAL_DIRECTORY_NAME: &str = "signals";
const PROCESS_TEST_WATCHER_SHUTDOWN_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(5);
pub const TEST_PICKER_PROBE_FILE: &str = "native-picker-invoked.probe";
pub const TEST_BROWSER_PROBE_FILE: &str = "browser-invoked.probe";
pub const TEST_STARTUP_RECOVERY_PROBE_FILE: &str = "startup-recovery.json";

/// Every actor boundary which a real-process scenario may name.
///
/// The enum is intentionally closed. A future pause point must be added here
/// and implemented by the owning actor before a scenario can request it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorPausePoint {
    CancelEnqueued,
    ClaimPermitAcquired,
    ClaimHandleRegistered,
    ClaimRunningCommitted,
    AfterFinalGateBeforeSpawn,
    TerminalAfterDispatchBeforeSchedulerPublish,
    CreateBeforeWrite,
    RetryBeforeWrite,
    ResultBeforeWrite,
    QuiesceBeforeRecovery,
    RecoveryBeforeDescriptor,
    DescriptorBeforeBrowser,
    TaskDetailAfterSnapshot,
    BootstrapBeforeSse,
    BootstrapCursorAhead,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualReleaseSignal {
    pub name: String,
    pub path: PathBuf,
    pub target: VirtualReleaseTarget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VirtualReleaseTarget {
    RunnerNext,
    StorageNext,
    StoreWriterBeforeExecute,
    StoreWriterAfterCommitBeforeWake,
    ActorCancelEnqueued,
    ActorClaimPermitAcquired,
    ActorClaimHandleRegistered,
    ActorClaimRunningCommitted,
    ActorAfterFinalGateBeforeSpawn,
    ActorTerminalAfterDispatchBeforeSchedulerPublish,
    ActorCreateBeforeWrite,
    ActorRetryBeforeWrite,
    ActorResultBeforeWrite,
    ActorQuiesceBeforeRecovery,
    ActorRecoveryBeforeDescriptor,
    ActorDescriptorBeforeBrowser,
    ActorTaskDetailAfterSnapshot,
    ActorBootstrapBeforeSse,
    ActorBootstrapCursorAhead,
}

impl VirtualReleaseTarget {
    const fn actor_pause(self) -> Option<ActorPausePoint> {
        match self {
            Self::RunnerNext
            | Self::StorageNext
            | Self::StoreWriterBeforeExecute
            | Self::StoreWriterAfterCommitBeforeWake => None,
            Self::ActorCancelEnqueued => Some(ActorPausePoint::CancelEnqueued),
            Self::ActorClaimPermitAcquired => Some(ActorPausePoint::ClaimPermitAcquired),
            Self::ActorClaimHandleRegistered => Some(ActorPausePoint::ClaimHandleRegistered),
            Self::ActorClaimRunningCommitted => Some(ActorPausePoint::ClaimRunningCommitted),
            Self::ActorAfterFinalGateBeforeSpawn => {
                Some(ActorPausePoint::AfterFinalGateBeforeSpawn)
            }
            Self::ActorTerminalAfterDispatchBeforeSchedulerPublish => {
                Some(ActorPausePoint::TerminalAfterDispatchBeforeSchedulerPublish)
            }
            Self::ActorCreateBeforeWrite => Some(ActorPausePoint::CreateBeforeWrite),
            Self::ActorRetryBeforeWrite => Some(ActorPausePoint::RetryBeforeWrite),
            Self::ActorResultBeforeWrite => Some(ActorPausePoint::ResultBeforeWrite),
            Self::ActorQuiesceBeforeRecovery => Some(ActorPausePoint::QuiesceBeforeRecovery),
            Self::ActorRecoveryBeforeDescriptor => Some(ActorPausePoint::RecoveryBeforeDescriptor),
            Self::ActorDescriptorBeforeBrowser => Some(ActorPausePoint::DescriptorBeforeBrowser),
            Self::ActorTaskDetailAfterSnapshot => Some(ActorPausePoint::TaskDetailAfterSnapshot),
            Self::ActorBootstrapBeforeSse => Some(ActorPausePoint::BootstrapBeforeSse),
            Self::ActorBootstrapCursorAhead => Some(ActorPausePoint::BootstrapCursorAhead),
        }
    }

    const fn store_pause(self) -> Option<StoreWriterFaultPoint> {
        match self {
            Self::StoreWriterBeforeExecute => Some(StoreWriterFaultPoint::PauseBeforeExecute),
            Self::StoreWriterAfterCommitBeforeWake => {
                Some(StoreWriterFaultPoint::PauseAfterCommitBeforeWake)
            }
            _ => None,
        }
    }

    const fn publishes_reached(self) -> bool {
        self.actor_pause().is_some() || self.store_pause().is_some()
    }
}

/// The complete, closed JSON contract consumed by a test-enabled process.
///
/// All fields are required deliberately. Callers must state empty arrays and
/// `false` explicitly so an old harness cannot silently ignore a new control.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LegacyV2Seed {
    None,
    CompletedTask {
        repository_path: PathBuf,
        task_prompt: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessRuntimeConfig {
    pub schema_version: u32,
    pub max_concurrent_tasks: u32,
    pub max_concurrent_tasks_per_repository: u32,
    pub max_queued_tasks: u32,
    pub storage: ProcessRuntimeStorageConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessRuntimeStorageConfig {
    pub data_control_reserve_bytes: u64,
    pub data_task_reservation_bytes: u64,
}

fn deserialize_required_runtime_config<'de, D>(
    deserializer: D,
) -> Result<Option<ProcessRuntimeConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<ProcessRuntimeConfig>::deserialize(deserializer)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessTestConfig {
    pub runner_mode: ProcessRunnerMode,
    #[serde(deserialize_with = "deserialize_required_runtime_config")]
    pub runtime_config: Option<ProcessRuntimeConfig>,
    pub fake_scenarios: Vec<FakeScenario>,
    pub storage_samples: Vec<ProcessStorageSample>,
    pub store_writer_faults: Vec<StoreWriterFaultSpec>,
    pub actor_pauses: Vec<ActorPausePoint>,
    pub virtual_release_signals: Vec<VirtualReleaseSignal>,
    pub legacy_v2_seed: LegacyV2Seed,
    pub marker_write_failure: bool,
}

impl ProcessTestConfig {
    /// Loads, validates, claims, zeroes, and removes one scenario source.
    ///
    /// Invalid input remains at the source path for diagnosis. Once validation
    /// succeeds, an atomic rename claims the source so concurrent loaders
    /// cannot both construct a configuration from the same bytes.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ProcessTestConfigError> {
        Self::load_with_validation(path.as_ref(), |_| Ok(()))
    }

    fn load_with_validation(
        path: &Path,
        additional_validation: impl FnOnce(&Self) -> Result<(), ProcessTestConfigError>,
    ) -> Result<Self, ProcessTestConfigError> {
        validate_scenario_source_path(path)?;

        let mut source = open_scenario(path)?;
        let identity = validate_scenario_handle(&source, path)?;
        harden_private_file(&source).map_err(|source| ProcessTestConfigError::Io {
            action: "harden scenario",
            path: path.to_path_buf(),
            source,
        })?;
        if validate_scenario_handle(&source, path)? != identity {
            return Err(ProcessTestConfigError::ScenarioChanged);
        }
        let mut bytes = read_scenario_bounded(&mut source, path)?;
        let parsed =
            serde_json::from_slice::<Self>(&bytes).map_err(ProcessTestConfigError::InvalidJson)?;
        parsed.validate()?;
        additional_validation(&parsed)?;

        let claimed_path = claimed_scenario_path(path);
        fs::rename(path, &claimed_path).map_err(|source| ProcessTestConfigError::Io {
            action: "claim scenario",
            path: path.to_path_buf(),
            source,
        })?;
        verify_claimed_scenario_identity(&claimed_path, identity)?;

        let consume_result = consume_claimed_scenario(&mut source, &claimed_path, &bytes, identity);
        bytes.fill(0);
        consume_result?;
        Ok(parsed)
    }

    fn validate(&self) -> Result<(), ProcessTestConfigError> {
        self.validate_runner_mode()?;
        if self.storage_samples.is_empty() {
            return Err(ProcessTestConfigError::EmptyStorageSamples);
        }
        let expected_storage_releases = self.storage_samples.len() - 1;
        let actual_storage_releases = self
            .virtual_release_signals
            .iter()
            .filter(|signal| signal.target == VirtualReleaseTarget::StorageNext)
            .count();
        if actual_storage_releases != expected_storage_releases {
            return Err(ProcessTestConfigError::StorageReleaseCount {
                expected: expected_storage_releases,
                actual: actual_storage_releases,
            });
        }

        if let LegacyV2Seed::CompletedTask {
            repository_path,
            task_prompt,
        } = &self.legacy_v2_seed
        {
            validate_absolute("legacy v2 repository", repository_path)?;
            let metadata = fs::symlink_metadata(repository_path).map_err(|source| {
                ProcessTestConfigError::Io {
                    action: "inspect legacy v2 repository",
                    path: repository_path.clone(),
                    source,
                }
            })?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(ProcessTestConfigError::InvalidLegacyV2Repository(
                    repository_path.clone(),
                ));
            }
            let prompt = task_prompt.trim();
            if prompt.is_empty() || prompt.chars().count() > 50_000 {
                return Err(ProcessTestConfigError::InvalidLegacyV2Prompt);
            }
        }

        let mut configured_store_pauses = HashSet::new();
        for fault in &self.store_writer_faults {
            if fault.count == 0 {
                return Err(ProcessTestConfigError::InvalidFaultCount);
            }
            if matches!(
                fault.point,
                StoreWriterFaultPoint::PauseBeforeExecute
                    | StoreWriterFaultPoint::PauseAfterCommitBeforeWake
            ) {
                if fault.count != 1 {
                    return Err(ProcessTestConfigError::InvalidStorePauseCount {
                        point: fault.point,
                        count: fault.count,
                    });
                }
                if !configured_store_pauses.insert(fault.point) {
                    return Err(ProcessTestConfigError::DuplicateStorePause(fault.point));
                }
            }
        }

        let mut pauses = HashSet::new();
        for pause in &self.actor_pauses {
            if !pauses.insert(*pause) {
                return Err(ProcessTestConfigError::DuplicateActorPause(*pause));
            }
        }

        let configured_pauses = pauses;
        let mut names = HashSet::new();
        let mut release_paths = HashSet::new();
        for signal in &self.virtual_release_signals {
            validate_signal_name(&signal.name)?;
            validate_release_path(&signal.path)?;
            if is_reserved_probe_path(&signal.path) {
                return Err(ProcessTestConfigError::ReservedReleasePath(
                    signal.path.clone(),
                ));
            }
            if !names.insert(signal.name.clone()) {
                return Err(ProcessTestConfigError::DuplicateReleaseName(
                    signal.name.clone(),
                ));
            }
            if !release_paths.insert(release_path_identity(&signal.path)?) {
                return Err(ProcessTestConfigError::DuplicateReleasePath(
                    signal.path.clone(),
                ));
            }
        }

        let mut released_pauses = HashSet::new();
        let mut released_store_pauses = HashSet::new();
        let mut reached_paths = HashSet::new();
        for signal in &self.virtual_release_signals {
            if signal.target.publishes_reached() {
                let pause = signal.target.actor_pause();
                if let Some(pause) = pause {
                    if !configured_pauses.contains(&pause) {
                        return Err(ProcessTestConfigError::UnexpectedActorRelease(pause));
                    }
                    if !released_pauses.insert(pause) {
                        return Err(ProcessTestConfigError::DuplicateActorRelease(pause));
                    }
                }
                if let Some(point) = signal.target.store_pause() {
                    if !configured_store_pauses.contains(&point) {
                        return Err(ProcessTestConfigError::UnexpectedStoreRelease(point));
                    }
                    if !released_store_pauses.insert(point) {
                        return Err(ProcessTestConfigError::DuplicateStoreRelease(point));
                    }
                }
                let reached_path = actor_reached_path(&signal.path)?;
                validate_release_path(&reached_path).map_err(|_| {
                    ProcessTestConfigError::InvalidActorReachedPath(reached_path.clone())
                })?;
                let identity = release_path_identity(&reached_path)?;
                if release_paths.contains(&identity) {
                    return Err(ProcessTestConfigError::ActorReachedPathConflict(
                        reached_path,
                    ));
                }
                if !reached_paths.insert(identity) {
                    return Err(ProcessTestConfigError::DuplicateActorReachedPath(
                        reached_path,
                    ));
                }
            }
        }
        for pause in configured_pauses {
            if !released_pauses.contains(&pause) {
                return Err(ProcessTestConfigError::MissingActorRelease(pause));
            }
        }
        for pause in configured_store_pauses {
            if !released_store_pauses.contains(&pause) {
                return Err(ProcessTestConfigError::MissingStoreRelease(pause));
            }
        }
        Ok(())
    }

    fn validate_runner_mode(&self) -> Result<(), ProcessTestConfigError> {
        let ProcessRunnerMode::ProductionOfflineDelivery {
            repository_path,
            provider_scenario,
            process_fault,
        } = &self.runner_mode
        else {
            return Ok(());
        };
        if *process_fault != ProcessDeliveryProcessFault::None
            && *provider_scenario != ProcessDeliveryProviderScenario::Approve
        {
            return Err(ProcessTestConfigError::ProductionProcessFaultRequiresApprove);
        }
        if !self.fake_scenarios.is_empty() {
            return Err(ProcessTestConfigError::ProductionRunnerHasFakeScenarios);
        }
        if !matches!(self.legacy_v2_seed, LegacyV2Seed::None) {
            return Err(ProcessTestConfigError::ProductionRunnerHasLegacySeed);
        }
        if self
            .virtual_release_signals
            .iter()
            .any(|signal| signal.target == VirtualReleaseTarget::RunnerNext)
        {
            return Err(ProcessTestConfigError::ProductionRunnerHasFakeRelease);
        }
        validate_delivery_repository(repository_path)
    }
}

#[derive(Debug)]
pub struct ProcessTestEnvironment {
    paths: PlatformPaths,
    config: ProcessTestConfig,
    signals: Arc<ProcessSignalDirectory>,
}

impl ProcessTestEnvironment {
    pub fn from_environment() -> Result<Self, ProcessTestConfigError> {
        let data_dir = required_environment_path(TEST_APP_DATA_ENV)?;
        let runtime_dir = required_environment_path(TEST_RUNTIME_ENV)?;
        let scenario = required_environment_path(TEST_SCENARIO_ENV)?;
        Self::load(data_dir, runtime_dir, scenario)
    }

    pub fn load(
        data_dir: impl AsRef<Path>,
        runtime_dir: impl AsRef<Path>,
        scenario: impl AsRef<Path>,
    ) -> Result<Self, ProcessTestConfigError> {
        let data_dir = validate_root("application data", data_dir.as_ref())?;
        let runtime_dir = validate_root("runtime", runtime_dir.as_ref())?;
        let paths = PlatformPaths::new(&data_dir, &runtime_dir);
        paths
            .prepare()
            .map_err(|source| ProcessTestConfigError::Io {
                action: "prepare isolated process roots",
                path: runtime_dir.clone(),
                source,
            })?;
        let signals = Arc::new(ProcessSignalDirectory::prepare(
            &runtime_dir.join(SIGNAL_DIRECTORY_NAME),
        )?);

        let scenario = scenario.as_ref();
        validate_scenario_source_path(scenario)?;
        let scenario = fs::canonicalize(scenario).map_err(|source| ProcessTestConfigError::Io {
            action: "canonicalize scenario",
            path: scenario.to_path_buf(),
            source,
        })?;
        if !path_is_within(&scenario, &data_dir) && !path_is_within(&scenario, &runtime_dir) {
            return Err(ProcessTestConfigError::ScenarioOutsideIsolatedRoots(
                scenario,
            ));
        }
        let config = ProcessTestConfig::load_with_validation(&scenario, |config| {
            for signal in &config.virtual_release_signals {
                signals.validate_child(&signal.path)?;
                if signal.target.publishes_reached() {
                    signals.validate_child(&actor_reached_path(&signal.path)?)?;
                }
            }
            Ok(())
        })?;
        write_runtime_config_if_requested(&paths, config.runtime_config.as_ref())?;

        Ok(Self {
            paths,
            config,
            signals,
        })
    }

    pub fn paths(&self) -> &PlatformPaths {
        &self.paths
    }

    pub fn config(&self) -> &ProcessTestConfig {
        &self.config
    }

    pub fn apply(
        self,
        mut dependencies: StartupDependencies,
    ) -> Result<StartupDependencies, ProcessTestConfigError> {
        let production_repository = self
            .config
            .runner_mode
            .production_repository()
            .map(Path::to_path_buf);
        dependencies.stores = Arc::new(ProcessTestStoreFactory {
            inner: dependencies.stores.clone(),
            database_path: self.paths.database_path.clone(),
            seed: self.config.legacy_v2_seed.clone(),
            production_repository,
            seeded: OnceCell::new(),
        });
        let writer_controller = Arc::new(
            StoreWriterTestController::try_new(self.config.store_writer_faults.clone())
                .map_err(|error| ProcessTestConfigError::InvalidWriterFault(error.to_string()))?,
        );
        let runner = Arc::new(ScriptedFakeRunner::new(
            FakeRunnerConfig::default(),
            self.config.fake_scenarios.iter().copied(),
        ));
        let actor_pause_gates = self
            .config
            .virtual_release_signals
            .iter()
            .filter_map(|signal| {
                signal.target.actor_pause().map(|point| {
                    actor_reached_path(&signal.path).map(|reached_path| (point, reached_path))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let actor_pauses = Arc::new(ActorPauseController::new(
            self.signals.clone(),
            actor_pause_gates,
        ));
        let storage_sampler = Arc::new(ProcessVolumeSampler::new(
            self.config.storage_samples.clone(),
        ));
        let picker_probe = self.paths.runtime_dir.join(TEST_PICKER_PROBE_FILE);
        let offline_runtime_path = self.paths.runtime_dir.clone();
        dependencies.paths = Arc::new(ProcessStartupPaths(self.paths));
        dependencies.browser = Arc::new(ProcessBrowserOpener {
            signals: self.signals.clone(),
        });
        dependencies.messages = Arc::new(ProcessNativeMessageSink);
        dependencies.dialog = Some(crate::NativeDialogService::process_test_probe(picker_probe));
        let offline_delivery = self
            .config
            .runner_mode
            .offline_delivery()
            .map(|(repository_path, scenario, process_fault)| {
                ProcessOfflineDeliveryRuntime::start(
                    repository_path,
                    offline_runtime_path.as_path(),
                    scenario,
                    process_fault,
                )
            })
            .transpose()
            .map_err(ProcessTestConfigError::OfflineDeliveryHarness)?;
        dependencies.runner_factory = offline_delivery.as_ref().map_or_else(
            || {
                Arc::new(FixedStartupRunnerFactory::new_for_process_test(
                    runner.clone(),
                    storage_sampler.clone(),
                )) as Arc<dyn crate::StartupRunnerFactory>
            },
            ProcessOfflineDeliveryRuntime::factory,
        );
        dependencies.process_test_support = Some(Arc::new(ProcessTestRuntime {
            config: self.config,
            writer_controller,
            runner,
            storage_sampler,
            actor_pauses,
            signals: self.signals,
            _offline_delivery: offline_delivery,
        }));
        Ok(dependencies)
    }
}

fn write_runtime_config_if_requested(
    paths: &PlatformPaths,
    config: Option<&ProcessRuntimeConfig>,
) -> Result<(), ProcessTestConfigError> {
    let Some(config) = config else {
        return Ok(());
    };
    let path = &paths.runtime_config;
    let mut file = PrivateFile::create_new(path).map_err(|source| ProcessTestConfigError::Io {
        action: "create private runtime configuration",
        path: path.clone(),
        source,
    })?;
    let write_result = (|| -> io::Result<()> {
        serde_json::to_writer(&mut file, config).map_err(io::Error::other)?;
        file.write_all(b"\n")?;
        file.as_file().sync_all()
    })();
    drop(file);
    if write_result.is_err() {
        let _ = fs::remove_file(path);
    }
    write_result.map_err(|source| ProcessTestConfigError::Io {
        action: "write private runtime configuration",
        path: path.clone(),
        source,
    })
}

struct ProcessTestStoreFactory {
    inner: Arc<dyn StoreFactory>,
    database_path: PathBuf,
    seed: LegacyV2Seed,
    production_repository: Option<PathBuf>,
    seeded: OnceCell<()>,
}

#[async_trait::async_trait]
impl StoreFactory for ProcessTestStoreFactory {
    async fn open(&self, path: &Path) -> Result<Store, StoreError> {
        if !matches!(self.seed, LegacyV2Seed::None) || self.production_repository.is_some() {
            self.seeded
                .get_or_try_init(|| async {
                    if path != self.database_path {
                        return Err(StoreError::InvariantViolation(
                            "process seed database path mismatch",
                        ));
                    }
                    if let Some(repository_path) = &self.production_repository {
                        seed_production_delivery_repository(path, repository_path).await
                    } else {
                        seed_legacy_v2_database(path, &self.seed).await
                    }
                })
                .await?;
        }
        self.inner.open(path).await
    }
}

async fn seed_production_delivery_repository(
    path: &Path,
    repository_path: &Path,
) -> Result<(), StoreError> {
    let repository_path = fs::canonicalize(repository_path)
        .map_err(|source| StoreError::Database(sqlx::Error::Io(source)))?;
    let canonical = CanonicalPath::try_from_canonical(repository_path.clone())
        .map_err(|_| StoreError::InvariantViolation("delivery repository path is not canonical"))?;
    let display_name = repository_path
        .file_name()
        .and_then(OsStr::to_str)
        .filter(|name| !name.is_empty())
        .unwrap_or("offline-delivery-repository")
        .to_owned();
    let store = Store::open(path).await?;
    store.migrate().await?;
    store
        .register_repository(NewRepository {
            selected_path: canonical.clone(),
            display_name,
            git_root: canonical.clone(),
            cargo_workspace_root: canonical,
        })
        .await?;
    store.close().await;
    Ok(())
}

async fn seed_legacy_v2_database(path: &Path, seed: &LegacyV2Seed) -> Result<(), StoreError> {
    let LegacyV2Seed::CompletedTask {
        repository_path,
        task_prompt,
    } = seed
    else {
        return Ok(());
    };
    let repository_path = fs::canonicalize(repository_path)
        .map_err(|source| StoreError::Database(sqlx::Error::Io(source)))?;
    let repository_path = repository_path
        .to_str()
        .ok_or(StoreError::InvariantViolation(
            "legacy v2 process seed repository path is not Unicode",
        ))?;
    #[cfg(windows)]
    let repository_identity_key = repository_path.replace('/', "\\").to_lowercase();
    #[cfg(not(windows))]
    let repository_identity_key = repository_path.to_owned();
    let mut connection = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true),
    )
    .await?;

    sqlx::raw_sql(include_str!(
        "../../coding-agent-store/migrations/0001_initial.sql"
    ))
    .execute(&mut connection)
    .await?;
    sqlx::query("INSERT INTO schema_migrations (version, applied_at) VALUES (1, ?)")
        .bind("2026-07-23T00:00:00.000000000Z")
        .execute(&mut connection)
        .await?;
    sqlx::raw_sql(include_str!(
        "../../coding-agent-store/migrations/0002_task_attempt_artifacts.sql"
    ))
    .execute(&mut connection)
    .await?;
    sqlx::query("INSERT INTO schema_migrations (version, applied_at) VALUES (2, ?)")
        .bind("2026-07-23T00:00:01.000000000Z")
        .execute(&mut connection)
        .await?;

    sqlx::query(
        "INSERT INTO repositories (
             id, selected_path, display_name, git_root, cargo_workspace_root,
             git_identity_key, cargo_identity_key, created_at, last_opened_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind("11111111-1111-4111-8111-111111111111")
    .bind(repository_path)
    .bind("Legacy v2 repository")
    .bind(repository_path)
    .bind(repository_path)
    .bind(&repository_identity_key)
    .bind(&repository_identity_key)
    .bind("2026-07-23T00:00:00.000000000Z")
    .bind("2026-07-23T00:00:00.000000000Z")
    .execute(&mut connection)
    .await?;
    sqlx::query(
        "INSERT INTO tasks (
             id, client_request_id, repository_id, prompt, status, attempt,
             retry_of, created_at, started_at, finished_at, last_event_id,
             failure_json
         ) VALUES (?, ?, ?, ?, 'completed', 1, NULL, ?, ?, ?, 41, NULL)",
    )
    .bind("22222222-2222-4222-8222-222222222222")
    .bind("33333333-3333-4333-8333-333333333333")
    .bind("11111111-1111-4111-8111-111111111111")
    .bind(task_prompt.trim())
    .bind("2026-07-23T00:00:00.000000000Z")
    .bind("2026-07-23T00:00:01.000000000Z")
    .bind("2026-07-23T00:00:02.000000000Z")
    .execute(&mut connection)
    .await?;
    sqlx::query(
        "INSERT INTO task_events (
             id, schema_version, task_id, kind, payload_json, created_at
         ) VALUES (40, 1, ?, 'plan.updated', ?, ?)",
    )
    .bind("22222222-2222-4222-8222-222222222222")
    .bind(
        r#"{"plan":{"revision":7,"items":[{"id":"legacy-step","title":"Legacy execution","status":"completed"}]}}"#,
    )
    .bind("2026-07-23T00:00:01.000000000Z")
    .execute(&mut connection)
    .await?;
    let completed_payload = serde_json::to_string(&serde_json::json!({
        "task": {
            "id": "22222222-2222-4222-8222-222222222222",
            "client_request_id": "33333333-3333-4333-8333-333333333333",
            "repository_id": "11111111-1111-4111-8111-111111111111",
            "prompt": task_prompt.trim(),
            "status": "completed",
            "attempt": 1,
            "retry_of": null,
            "created_at": "2026-07-23T00:00:00.000000000Z",
            "started_at": "2026-07-23T00:00:01.000000000Z",
            "finished_at": "2026-07-23T00:00:02.000000000Z",
            "last_event_id": 41,
            "failure": null
        }
    }))?;
    sqlx::query(
        "INSERT INTO task_events (
             id, schema_version, task_id, kind, payload_json, created_at
         ) VALUES (41, 1, ?, 'task.completed', ?, ?)",
    )
    .bind("22222222-2222-4222-8222-222222222222")
    .bind(completed_payload)
    .bind("2026-07-23T00:00:02.000000000Z")
    .execute(&mut connection)
    .await?;
    sqlx::query(
        "INSERT INTO task_attempt_artifacts (
             task_id, repository_id, attempt, base_commit, branch_name,
             worktree_path, state, failure_code, created_at, updated_at
         ) VALUES (?, ?, 1, ?, ?, ?, 'ready', NULL, ?, ?)",
    )
    .bind("22222222-2222-4222-8222-222222222222")
    .bind("11111111-1111-4111-8111-111111111111")
    .bind("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    .bind("codex/legacy-v2-completed")
    .bind(repository_path)
    .bind("2026-07-23T00:00:00.000000000Z")
    .bind("2026-07-23T00:00:00.000000000Z")
    .execute(&mut connection)
    .await?;
    connection.close().await?;
    Ok(())
}

pub(crate) struct ProcessTestRuntime {
    pub config: ProcessTestConfig,
    pub writer_controller: Arc<StoreWriterTestController>,
    pub actor_pauses: Arc<ActorPauseController>,
    runner: Arc<ScriptedFakeRunner>,
    storage_sampler: Arc<ProcessVolumeSampler>,
    signals: Arc<ProcessSignalDirectory>,
    _offline_delivery: Option<ProcessOfflineDeliveryRuntime>,
}

impl ProcessTestRuntime {
    pub(crate) fn close_signal_capability(&self) -> Result<(), ProcessSignalCapabilityCloseError> {
        self.signals.close_capability()
    }

    pub(crate) fn publish_startup_recovery_probe(
        &self,
        interrupted_count: usize,
    ) -> io::Result<()> {
        if interrupted_count == 0 {
            return Ok(());
        }
        let payload = serde_json::to_vec(&serde_json::json!({
            "interrupted_count": interrupted_count,
        }))
        .map_err(io::Error::other)?;
        self.signals
            .publish_probe_bytes(OsStr::new(TEST_STARTUP_RECOVERY_PROBE_FILE), &payload)
    }

    pub fn spawn_virtual_release_watchers(&self) -> ProcessTestWatchers {
        let watchers = self
            .config
            .virtual_release_signals
            .iter()
            .cloned()
            .map(|signal| {
                let runner = self.runner.clone();
                let storage_sampler = self.storage_sampler.clone();
                let writer = self.writer_controller.clone();
                let actor_pauses = self.actor_pauses.clone();
                let signals = self.signals.clone();
                tokio::spawn(async move {
                    if let Some(point) = signal.target.store_pause() {
                        writer.wait_until_reached(point, 1).await;
                        let reached_path = match actor_reached_path(&signal.path) {
                            Ok(path) => path,
                            Err(_) => return,
                        };
                        let reached_name = match signals.child_name(&reached_path) {
                            Ok(name) => name,
                            Err(_) => return,
                        };
                        if signals.publish_reached(&reached_name).is_err() {
                            tracing::warn!(
                                error_code = "TEST_REACHED_SIGNAL_FAILED",
                                signal = %signal.name,
                                "process-test reached signal could not be published"
                            );
                            return;
                        }
                    }
                    if wait_for_virtual_signal(&signals, &signal.path)
                        .await
                        .is_err()
                    {
                        tracing::warn!(
                            error_code = "TEST_RELEASE_SIGNAL_FAILED",
                            signal = %signal.name,
                            "process-test release signal could not be consumed"
                        );
                        return;
                    }
                    if let Some(pause) = signal.target.actor_pause() {
                        actor_pauses.release(pause);
                        return;
                    }
                    match signal.target {
                        VirtualReleaseTarget::RunnerNext => {
                            runner.wait_and_release_next().await;
                        }
                        VirtualReleaseTarget::StorageNext => {
                            if !storage_sampler.advance() {
                                tracing::warn!(
                                    error_code = "TEST_STORAGE_SCRIPT_EXHAUSTED",
                                    signal = %signal.name,
                                    "process-test storage script could not advance"
                                );
                            }
                        }
                        VirtualReleaseTarget::StoreWriterBeforeExecute => {
                            writer.release(StoreWriterFaultPoint::PauseBeforeExecute);
                        }
                        VirtualReleaseTarget::StoreWriterAfterCommitBeforeWake => {
                            writer.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake);
                        }
                        VirtualReleaseTarget::ActorCancelEnqueued
                        | VirtualReleaseTarget::ActorClaimPermitAcquired
                        | VirtualReleaseTarget::ActorClaimHandleRegistered
                        | VirtualReleaseTarget::ActorClaimRunningCommitted
                        | VirtualReleaseTarget::ActorAfterFinalGateBeforeSpawn
                        | VirtualReleaseTarget::ActorTerminalAfterDispatchBeforeSchedulerPublish
                        | VirtualReleaseTarget::ActorCreateBeforeWrite
                        | VirtualReleaseTarget::ActorRetryBeforeWrite
                        | VirtualReleaseTarget::ActorResultBeforeWrite
                        | VirtualReleaseTarget::ActorQuiesceBeforeRecovery
                        | VirtualReleaseTarget::ActorRecoveryBeforeDescriptor
                        | VirtualReleaseTarget::ActorDescriptorBeforeBrowser
                        | VirtualReleaseTarget::ActorTaskDetailAfterSnapshot
                        | VirtualReleaseTarget::ActorBootstrapBeforeSse
                        | VirtualReleaseTarget::ActorBootstrapCursorAhead => {
                            unreachable!("actor release targets are handled above")
                        }
                    }
                })
            })
            .collect();
        ProcessTestWatchers::new(watchers)
    }
}

pub(crate) struct ProcessTestWatchers {
    watchers: Mutex<Option<Vec<tokio::task::JoinHandle<()>>>>,
    completion: watch::Sender<Option<Result<(), ProcessTestWatcherShutdownError>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessTestWatcherShutdownError {
    DeadlineExceeded,
    WatcherPanicked,
    CoordinatorUnavailable,
}

impl ProcessTestWatchers {
    fn new(watchers: Vec<tokio::task::JoinHandle<()>>) -> Self {
        let (completion, _) = watch::channel(None);
        Self {
            watchers: Mutex::new(Some(watchers)),
            completion,
        }
    }

    pub(crate) async fn shutdown_and_join(&self) -> Result<(), ProcessTestWatcherShutdownError> {
        if let Some(outcome) = *self.completion.borrow() {
            return outcome;
        }
        let deadline = tokio::time::Instant::now() + PROCESS_TEST_WATCHER_SHUTDOWN_TIMEOUT;
        let watchers = self
            .watchers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(watchers) = watchers {
            let completion = self.completion.clone();
            tokio::spawn(async move {
                let outcome = join_process_test_watchers(watchers, deadline).await;
                let _ = completion.send_replace(Some(outcome));
            });
        }
        let mut completion = self.completion.subscribe();
        loop {
            if let Some(outcome) = *completion.borrow_and_update() {
                return outcome;
            }
            match tokio::time::timeout_at(deadline, completion.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    return Err(ProcessTestWatcherShutdownError::CoordinatorUnavailable);
                }
                Err(_) => return Err(ProcessTestWatcherShutdownError::DeadlineExceeded),
            }
        }
    }
}

impl Default for ProcessTestWatchers {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl Drop for ProcessTestWatchers {
    fn drop(&mut self) {
        if let Some(watchers) = self
            .watchers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            for watcher in watchers {
                watcher.abort();
            }
        }
    }
}

async fn join_process_test_watchers(
    watchers: Vec<tokio::task::JoinHandle<()>>,
    deadline: tokio::time::Instant,
) -> Result<(), ProcessTestWatcherShutdownError> {
    for watcher in &watchers {
        watcher.abort();
    }
    let mut outcome = Ok(());
    for watcher in watchers {
        match tokio::time::timeout_at(deadline, watcher).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) if error.is_cancelled() => {}
            Ok(Err(_)) => outcome = Err(ProcessTestWatcherShutdownError::WatcherPanicked),
            Err(_) => outcome = Err(ProcessTestWatcherShutdownError::DeadlineExceeded),
        }
    }
    outcome
}

pub(crate) struct ActorPauseController {
    gates: HashMap<ActorPausePoint, Arc<ActorPauseGate>>,
    signals: Arc<ProcessSignalDirectory>,
}

struct ActorPauseGate {
    state: AtomicU8,
    reached_name: OsString,
    release: tokio::sync::Notify,
}

impl ActorPauseController {
    fn new(
        signals: Arc<ProcessSignalDirectory>,
        pauses: impl IntoIterator<Item = (ActorPausePoint, PathBuf)>,
    ) -> Self {
        Self {
            gates: pauses
                .into_iter()
                .map(|(pause, reached_path)| {
                    (
                        pause,
                        Arc::new(ActorPauseGate {
                            state: AtomicU8::new(ACTOR_PAUSE_PENDING),
                            reached_name: signals
                                .child_name(&reached_path)
                                .expect("validated actor reached path has one child name"),
                            release: tokio::sync::Notify::new(),
                        }),
                    )
                })
                .collect(),
            signals,
        }
    }

    /// Pauses the first visit to a configured point and returns whether it consumed that pause.
    pub(crate) async fn pause(&self, point: ActorPausePoint) -> bool {
        let Some(gate) = self.gates.get(&point) else {
            return false;
        };
        match gate.state.compare_exchange(
            ACTOR_PAUSE_PENDING,
            ACTOR_PAUSE_ENTERED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {}
            Err(ACTOR_PAUSE_REACHED_FAILED) => {
                return std::future::pending::<bool>().await;
            }
            Err(_) => return false,
        }
        if let Err(error) = self.signals.publish_reached(&gate.reached_name) {
            gate.state
                .store(ACTOR_PAUSE_REACHED_FAILED, Ordering::Release);
            tracing::error!(
                error = %error,
                error_code = "TEST_ACTOR_REACHED_WRITE_FAILED",
                pause = ?point,
                "process-test actor reached marker could not be published"
            );
            return std::future::pending::<bool>().await;
        }
        gate.release.notified().await;
        gate.state.store(ACTOR_PAUSE_RELEASED, Ordering::Release);
        true
    }

    fn release(&self, point: ActorPausePoint) -> bool {
        let Some(gate) = self.gates.get(&point) else {
            return false;
        };
        gate.release.notify_one();
        true
    }
}

const ACTOR_PAUSE_PENDING: u8 = 0;
const ACTOR_PAUSE_ENTERED: u8 = 1;
const ACTOR_PAUSE_RELEASED: u8 = 2;
const ACTOR_PAUSE_REACHED_FAILED: u8 = 3;

struct ProcessStartupPaths(PlatformPaths);

impl StartupPaths for ProcessStartupPaths {
    fn discover(&self) -> std::io::Result<PlatformPaths> {
        Ok(self.0.clone())
    }

    fn prepare_lock_parent(&self, paths: &PlatformPaths) -> std::io::Result<()> {
        paths.prepare_runtime_directory()
    }

    fn prepare(&self, paths: &PlatformPaths) -> std::io::Result<()> {
        paths.prepare()
    }
}

struct ProcessBrowserOpener {
    signals: Arc<ProcessSignalDirectory>,
}

impl BrowserOpener for ProcessBrowserOpener {
    fn open(&self, _port: u16, _token: &str) -> Result<(), BrowserLaunchError> {
        if self
            .signals
            .publish_probe_bytes(OsStr::new(TEST_BROWSER_PROBE_FILE), &[])
            .is_err()
        {
            tracing::warn!(
                error_code = "TEST_BROWSER_PROBE_FAILED",
                "process-test browser probe could not be published"
            );
        }
        Ok(())
    }
}

struct ProcessNativeMessageSink;

impl NativeMessageSink for ProcessNativeMessageSink {
    fn show_error(&self, _title: &'static str, _body: String) {}
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessTestConfigError {
    #[error("required test-support environment variable {name} is missing")]
    MissingEnvironment { name: &'static str },
    #[error("test-support environment variable {name} is not valid Unicode")]
    NonUnicodeEnvironment { name: &'static str },
    #[error("{field} path must be absolute: {path}")]
    RelativePath { field: &'static str, path: PathBuf },
    #[error("{field} root is invalid: {path}")]
    InvalidRoot { field: &'static str, path: PathBuf },
    #[error("scenario path is outside the isolated application data/runtime roots: {0}")]
    ScenarioOutsideIsolatedRoots(PathBuf),
    #[error("scenario source is not a private, singly-linked regular file: {0}")]
    InvalidScenarioFile(PathBuf),
    #[error("process signal directory is invalid: {0}")]
    InvalidSignalDirectory(PathBuf),
    #[error(
        "release signal must be a direct child of the dedicated runtime signals directory: {0}"
    )]
    ReleaseOutsideSignalDirectory(PathBuf),
    #[error("release signal path is outside the isolated runtime root: {0}")]
    ReleaseOutsideRuntime(PathBuf),
    #[error("scenario size {actual} is outside 1..={maximum} bytes")]
    InvalidScenarioSize { actual: u64, maximum: u64 },
    #[error("scenario JSON is invalid: {0}")]
    InvalidJson(#[source] serde_json::Error),
    #[error("legacy v2 repository path is not a real directory: {0}")]
    InvalidLegacyV2Repository(PathBuf),
    #[error("legacy v2 completed task prompt is invalid")]
    InvalidLegacyV2Prompt,
    #[error("production offline delivery runner requires fake_scenarios to be empty")]
    ProductionRunnerHasFakeScenarios,
    #[error("production offline delivery runner cannot be combined with legacy_v2_seed")]
    ProductionRunnerHasLegacySeed,
    #[error("production offline delivery runner cannot use runner_next release targets")]
    ProductionRunnerHasFakeRelease,
    #[error("production offline delivery process faults require the approve provider scenario")]
    ProductionProcessFaultRequiresApprove,
    #[error("production offline delivery repository is invalid: {0}")]
    InvalidDeliveryRepository(PathBuf),
    #[error("production offline delivery harness could not start: {0}")]
    OfflineDeliveryHarness(#[source] std::io::Error),
    #[error("process storage sample script must contain at least one sample")]
    EmptyStorageSamples,
    #[error(
        "process storage sample script requires {expected} storage release targets, got {actual}"
    )]
    StorageReleaseCount { expected: usize, actual: usize },
    #[error("StoreWriter fault count must be positive")]
    InvalidFaultCount,
    #[error("StoreWriter process pause {point:?} must have count 1, got {count}")]
    InvalidStorePauseCount {
        point: StoreWriterFaultPoint,
        count: u32,
    },
    #[error("StoreWriter process pause is configured more than once: {0:?}")]
    DuplicateStorePause(StoreWriterFaultPoint),
    #[error("StoreWriter process pause {0:?} has no virtual release target")]
    MissingStoreRelease(StoreWriterFaultPoint),
    #[error("StoreWriter release target {0:?} has no matching pause fault")]
    UnexpectedStoreRelease(StoreWriterFaultPoint),
    #[error("StoreWriter process pause {0:?} has more than one virtual release target")]
    DuplicateStoreRelease(StoreWriterFaultPoint),
    #[error("StoreWriter fault script is invalid: {0}")]
    InvalidWriterFault(String),
    #[error("actor pause is configured more than once: {0:?}")]
    DuplicateActorPause(ActorPausePoint),
    #[error("actor pause {0:?} has no virtual release target")]
    MissingActorRelease(ActorPausePoint),
    #[error("actor release target {0:?} is not configured in actor_pauses")]
    UnexpectedActorRelease(ActorPausePoint),
    #[error("actor pause {0:?} has more than one virtual release target")]
    DuplicateActorRelease(ActorPausePoint),
    #[error("actor reached marker path is invalid or already exists: {0}")]
    InvalidActorReachedPath(PathBuf),
    #[error("actor reached marker path conflicts with a virtual release path: {0}")]
    ActorReachedPathConflict(PathBuf),
    #[error("actor reached marker path is duplicated: {0}")]
    DuplicateActorReachedPath(PathBuf),
    #[error("virtual release signal name is invalid: {0}")]
    InvalidReleaseName(String),
    #[error("virtual release signal name is duplicated: {0}")]
    DuplicateReleaseName(String),
    #[error("virtual release signal path is duplicated: {0}")]
    DuplicateReleasePath(PathBuf),
    #[error("virtual release signal path already exists or is not a regular path: {0}")]
    InvalidReleasePath(PathBuf),
    #[error("virtual release signal path is reserved for an internal process probe: {0}")]
    ReservedReleasePath(PathBuf),
    #[error("scenario source changed while it was being claimed")]
    ScenarioChanged,
    #[error("could not {action} at {path}: {source}")]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

fn required_environment_path(name: &'static str) -> Result<PathBuf, ProcessTestConfigError> {
    match env::var_os(name) {
        None => Err(ProcessTestConfigError::MissingEnvironment { name }),
        Some(value) => os_string_into_path(name, value),
    }
}

fn os_string_into_path(
    name: &'static str,
    value: OsString,
) -> Result<PathBuf, ProcessTestConfigError> {
    if value.to_str().is_none() {
        return Err(ProcessTestConfigError::NonUnicodeEnvironment { name });
    }
    Ok(PathBuf::from(value))
}

fn validate_root(field: &'static str, path: &Path) -> Result<PathBuf, ProcessTestConfigError> {
    validate_absolute(field, path)?;
    let metadata = fs::symlink_metadata(path).map_err(|source| ProcessTestConfigError::Io {
        action: "inspect isolated root",
        path: path.to_path_buf(),
        source,
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() || path.parent().is_none() {
        return Err(ProcessTestConfigError::InvalidRoot {
            field,
            path: path.to_path_buf(),
        });
    }
    fs::canonicalize(path).map_err(|source| ProcessTestConfigError::Io {
        action: "canonicalize isolated root",
        path: path.to_path_buf(),
        source,
    })
}

fn validate_scenario_source_path(path: &Path) -> Result<(), ProcessTestConfigError> {
    validate_absolute("scenario", path)?;
    let metadata = fs::symlink_metadata(path).map_err(|source| ProcessTestConfigError::Io {
        action: "inspect scenario",
        path: path.to_path_buf(),
        source,
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ProcessTestConfigError::InvalidScenarioFile(
            path.to_path_buf(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScenarioFileIdentity {
    volume: u64,
    file: u64,
}

fn open_scenario(path: &Path) -> Result<File, ProcessTestConfigError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        };
        options
            .access_mode(crate::platform::windows_private_file_access_mode())
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    options
        .open(path)
        .map_err(|source| ProcessTestConfigError::Io {
            action: "open scenario without following links",
            path: path.to_path_buf(),
            source,
        })
}

fn validate_scenario_handle(
    file: &File,
    path: &Path,
) -> Result<ScenarioFileIdentity, ProcessTestConfigError> {
    scenario_file_identity(file).map_err(|source| match source.kind() {
        io::ErrorKind::InvalidData | io::ErrorKind::PermissionDenied => {
            ProcessTestConfigError::InvalidScenarioFile(path.to_path_buf())
        }
        _ => ProcessTestConfigError::Io {
            action: "validate scenario file identity",
            path: path.to_path_buf(),
            source,
        },
    })
}

#[cfg(unix)]
fn scenario_file_identity(file: &File) -> io::Result<ScenarioFileIdentity> {
    use std::os::unix::fs::MetadataExt as _;

    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "scenario is not a singly-linked regular file",
        ));
    }
    Ok(ScenarioFileIdentity {
        volume: metadata.dev(),
        file: metadata.ino(),
    })
}

#[cfg(windows)]
fn scenario_file_identity(file: &File) -> io::Result<ScenarioFileIdentity> {
    use std::os::windows::fs::MetadataExt as _;
    use std::os::windows::io::AsRawHandle as _;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, FILE_ATTRIBUTE_REPARSE_POINT, GetFileInformationByHandle,
    };

    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "scenario is not a plain regular file",
        ));
    }
    let mut information = unsafe { std::mem::zeroed::<BY_HANDLE_FILE_INFORMATION>() };
    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut information) } == 0 {
        return Err(io::Error::last_os_error());
    }
    if information.nNumberOfLinks != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "scenario has more than one hard link",
        ));
    }
    Ok(ScenarioFileIdentity {
        volume: u64::from(information.dwVolumeSerialNumber),
        file: u64::from(information.nFileIndexHigh) << 32 | u64::from(information.nFileIndexLow),
    })
}

fn read_scenario_bounded(file: &mut File, path: &Path) -> Result<Vec<u8>, ProcessTestConfigError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|source| ProcessTestConfigError::Io {
            action: "rewind scenario",
            path: path.to_path_buf(),
            source,
        })?;
    let mut bytes = Vec::new();
    file.take(MAX_SCENARIO_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| ProcessTestConfigError::Io {
            action: "read bounded scenario",
            path: path.to_path_buf(),
            source,
        })?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_SCENARIO_BYTES {
        return Err(ProcessTestConfigError::InvalidScenarioSize {
            actual: bytes.len() as u64,
            maximum: MAX_SCENARIO_BYTES,
        });
    }
    Ok(bytes)
}

fn verify_claimed_scenario_identity(
    path: &Path,
    expected: ScenarioFileIdentity,
) -> Result<(), ProcessTestConfigError> {
    let claimed = open_scenario(path)?;
    if validate_scenario_handle(&claimed, path)? != expected {
        return Err(ProcessTestConfigError::ScenarioChanged);
    }
    Ok(())
}

fn validate_absolute(field: &'static str, path: &Path) -> Result<(), ProcessTestConfigError> {
    if !path.is_absolute() {
        return Err(ProcessTestConfigError::RelativePath {
            field,
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

fn validate_delivery_repository(path: &Path) -> Result<(), ProcessTestConfigError> {
    validate_absolute("production offline delivery repository", path)?;
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| ProcessTestConfigError::InvalidDeliveryRepository(path.to_path_buf()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(ProcessTestConfigError::InvalidDeliveryRepository(
            path.to_path_buf(),
        ));
    }
    fs::canonicalize(path)
        .map_err(|_| ProcessTestConfigError::InvalidDeliveryRepository(path.to_path_buf()))?;
    let required_directories = [path.join(".git"), path.join("src")];
    if required_directories.iter().any(|required| {
        fs::symlink_metadata(required).map_or(true, |metadata| {
            metadata.file_type().is_symlink() || !metadata.is_dir()
        })
    }) {
        return Err(ProcessTestConfigError::InvalidDeliveryRepository(
            path.to_path_buf(),
        ));
    }
    let required_files = [path.join("Cargo.toml"), path.join("src").join("lib.rs")];
    if required_files.iter().any(|required| {
        fs::symlink_metadata(required).map_or(true, |metadata| {
            metadata.file_type().is_symlink() || !metadata.is_file()
        })
    }) {
        return Err(ProcessTestConfigError::InvalidDeliveryRepository(
            path.to_path_buf(),
        ));
    }
    Ok(())
}

fn validate_signal_name(name: &str) -> Result<(), ProcessTestConfigError> {
    let valid = !name.is_empty()
        && name.len() <= MAX_SIGNAL_NAME_BYTES
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if valid {
        Ok(())
    } else {
        Err(ProcessTestConfigError::InvalidReleaseName(name.to_owned()))
    }
}

fn validate_release_path(path: &Path) -> Result<(), ProcessTestConfigError> {
    validate_absolute("virtual release signal", path)?;
    if path.components().any(|component| {
        matches!(
            component,
            std::path::Component::CurDir | std::path::Component::ParentDir
        )
    }) {
        return Err(ProcessTestConfigError::InvalidReleasePath(
            path.to_path_buf(),
        ));
    }
    let parent = path
        .parent()
        .ok_or_else(|| ProcessTestConfigError::InvalidReleasePath(path.to_path_buf()))?;
    let parent_metadata = fs::symlink_metadata(parent)
        .map_err(|_| ProcessTestConfigError::InvalidReleasePath(path.to_path_buf()))?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(ProcessTestConfigError::InvalidReleasePath(
            path.to_path_buf(),
        ));
    }
    match fs::symlink_metadata(path) {
        Ok(_) => Err(ProcessTestConfigError::InvalidReleasePath(
            path.to_path_buf(),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(ProcessTestConfigError::InvalidReleasePath(
            path.to_path_buf(),
        )),
    }
}

fn actor_reached_path(release_path: &Path) -> Result<PathBuf, ProcessTestConfigError> {
    let file_name = release_path.file_name().ok_or_else(|| {
        ProcessTestConfigError::InvalidActorReachedPath(release_path.to_path_buf())
    })?;
    let mut reached_name = file_name.to_os_string();
    reached_name.push(".reached");
    Ok(release_path.with_file_name(reached_name))
}

fn is_reserved_probe_path(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(OsStr::to_str) else {
        return false;
    };
    #[cfg(windows)]
    {
        name.eq_ignore_ascii_case(TEST_BROWSER_PROBE_FILE)
            || name.eq_ignore_ascii_case(TEST_STARTUP_RECOVERY_PROBE_FILE)
    }
    #[cfg(not(windows))]
    {
        name == TEST_BROWSER_PROBE_FILE || name == TEST_STARTUP_RECOVERY_PROBE_FILE
    }
}

#[derive(Debug)]
struct ProcessSignalDirectory {
    path: PathBuf,
    capability: Mutex<Option<File>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessSignalCapabilityCloseError {
    LockPoisoned,
}

impl ProcessSignalDirectory {
    fn prepare(path: &Path) -> Result<Self, ProcessTestConfigError> {
        create_private_directory(path).map_err(|source| ProcessTestConfigError::Io {
            action: "prepare private process signal directory",
            path: path.to_path_buf(),
            source,
        })?;
        let canonical = fs::canonicalize(path).map_err(|source| ProcessTestConfigError::Io {
            action: "canonicalize process signal directory",
            path: path.to_path_buf(),
            source,
        })?;
        let handle =
            open_signal_directory(&canonical).map_err(|source| ProcessTestConfigError::Io {
                action: "open process signal directory capability",
                path: canonical.clone(),
                source,
            })?;
        Ok(Self {
            path: canonical,
            capability: Mutex::new(Some(handle)),
        })
    }

    fn close_capability(&self) -> Result<(), ProcessSignalCapabilityCloseError> {
        let mut capability = self
            .capability
            .lock()
            .map_err(|_| ProcessSignalCapabilityCloseError::LockPoisoned)?;
        drop(capability.take());
        Ok(())
    }

    fn with_capability<T>(&self, operation: impl FnOnce(&File) -> io::Result<T>) -> io::Result<T> {
        let capability = self.capability.lock().map_err(|_| {
            io::Error::other("process signal directory capability lock is poisoned")
        })?;
        let capability = capability.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "process signal directory capability is closed",
            )
        })?;
        operation(capability)
    }

    fn validate_child(&self, path: &Path) -> Result<(), ProcessTestConfigError> {
        self.with_capability(|_| Ok(()))
            .map_err(|source| ProcessTestConfigError::Io {
                action: "validate open process signal capability",
                path: self.path.clone(),
                source,
            })?;
        validate_release_path(path)?;
        let parent = path
            .parent()
            .ok_or_else(|| ProcessTestConfigError::InvalidReleasePath(path.to_path_buf()))?;
        let canonical_parent =
            fs::canonicalize(parent).map_err(|source| ProcessTestConfigError::Io {
                action: "canonicalize release signal parent",
                path: parent.to_path_buf(),
                source,
            })?;
        if canonical_parent != self.path {
            return Err(ProcessTestConfigError::ReleaseOutsideSignalDirectory(
                path.to_path_buf(),
            ));
        }
        self.child_name(path)?;
        Ok(())
    }

    fn child_name(&self, path: &Path) -> Result<OsString, ProcessTestConfigError> {
        path.file_name()
            .filter(|name| !name.is_empty())
            .map(OsStr::to_os_string)
            .ok_or_else(|| ProcessTestConfigError::InvalidReleasePath(path.to_path_buf()))
    }

    fn release_ready(&self, name: &OsStr) -> io::Result<bool> {
        self.with_capability(|directory| {
            let Some(file) = self.open_child_no_follow(directory, name)? else {
                return Ok(false);
            };
            let metadata = file.metadata()?;
            if !release_metadata_is_regular(&metadata) || metadata.len() != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "release signal is not an empty regular file",
                ));
            }
            Ok(true)
        })
    }

    fn publish_probe_bytes(&self, name: &OsStr, payload: &[u8]) -> io::Result<()> {
        self.with_capability(|directory| {
            self.remove_child_if_present(directory, name)?;
            self.publish_bytes(directory, name, payload)
        })
    }

    #[cfg(unix)]
    fn remove_child_if_present(&self, directory: &File, name: &OsStr) -> io::Result<()> {
        use std::os::fd::AsRawFd as _;
        use std::os::unix::ffi::OsStrExt as _;

        let name = std::ffi::CString::new(name.as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "signal name contains NUL"))?;
        let result = unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), 0) };
        if result == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::NotFound {
            Ok(())
        } else {
            Err(error)
        }
    }

    #[cfg(windows)]
    fn remove_child_if_present(&self, _directory: &File, name: &OsStr) -> io::Result<()> {
        match fs::remove_file(self.path.join(name)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    #[cfg(unix)]
    fn open_child_no_follow(&self, directory: &File, name: &OsStr) -> io::Result<Option<File>> {
        use std::os::fd::{AsRawFd as _, FromRawFd as _};
        use std::os::unix::ffi::OsStrExt as _;

        let name = std::ffi::CString::new(name.as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "signal name contains NUL"))?;
        let descriptor = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            )
        };
        if descriptor < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(error);
        }
        Ok(Some(unsafe { File::from_raw_fd(descriptor) }))
    }

    #[cfg(windows)]
    fn open_child_no_follow(&self, _directory: &File, name: &OsStr) -> io::Result<Option<File>> {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;

        let mut options = OpenOptions::new();
        options
            .read(true)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
        match options.open(self.path.join(name)) {
            Ok(file) => Ok(Some(file)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    #[cfg(unix)]
    fn publish_reached(&self, name: &OsStr) -> io::Result<()> {
        self.with_capability(|directory| self.publish_bytes(directory, name, &[]))
    }

    #[cfg(unix)]
    fn publish_bytes(&self, directory: &File, name: &OsStr, payload: &[u8]) -> io::Result<()> {
        use std::os::fd::{AsRawFd as _, FromRawFd as _};
        use std::os::unix::ffi::OsStrExt as _;

        let final_name = std::ffi::CString::new(name.as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "signal name contains NUL"))?;
        let temporary_name = std::ffi::CString::new(format!(
            ".process-probe-{}-{}.tmp",
            std::process::id(),
            uuid::Uuid::new_v4()
        ))
        .expect("generated reached marker name has no NUL");
        let directory = directory.as_raw_fd();
        let descriptor = unsafe {
            libc::openat(
                directory,
                temporary_name.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o600,
            )
        };
        if descriptor < 0 {
            return Err(io::Error::last_os_error());
        }
        let mut temporary = unsafe { File::from_raw_fd(descriptor) };
        if let Err(error) = temporary.write_all(payload) {
            drop(temporary);
            unsafe {
                libc::unlinkat(directory, temporary_name.as_ptr(), 0);
            }
            return Err(error);
        }
        if let Err(error) = temporary.sync_all() {
            drop(temporary);
            unsafe {
                libc::unlinkat(directory, temporary_name.as_ptr(), 0);
            }
            return Err(error);
        }
        let linked = unsafe {
            libc::linkat(
                directory,
                temporary_name.as_ptr(),
                directory,
                final_name.as_ptr(),
                0,
            )
        };
        let link_error = (linked != 0).then(io::Error::last_os_error);
        drop(temporary);
        unsafe {
            libc::unlinkat(directory, temporary_name.as_ptr(), 0);
        }
        match link_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    #[cfg(windows)]
    fn publish_reached(&self, name: &OsStr) -> io::Result<()> {
        self.with_capability(|directory| self.publish_bytes(directory, name, &[]))
    }

    #[cfg(windows)]
    fn publish_bytes(&self, _directory: &File, name: &OsStr, payload: &[u8]) -> io::Result<()> {
        let temporary_path = self.path.join(format!(
            ".process-probe-{}-{}.tmp",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let final_path = self.path.join(name);
        let mut temporary = PrivateFile::create_new(&temporary_path)?;
        if let Err(error) = temporary.write_all(payload) {
            drop(temporary);
            let _ = fs::remove_file(&temporary_path);
            return Err(error);
        }
        if let Err(error) = temporary.as_file().sync_all() {
            drop(temporary);
            let _ = fs::remove_file(&temporary_path);
            return Err(error);
        }
        drop(temporary);
        let publication = fs::hard_link(&temporary_path, &final_path);
        let _ = fs::remove_file(&temporary_path);
        publication
    }
}

#[cfg(unix)]
fn open_signal_directory(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt as _;

    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW);
    let directory = options.open(path)?;
    if !directory.metadata()?.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "process signal capability is not a directory",
        ));
    }
    Ok(directory)
}

#[cfg(windows)]
fn open_signal_directory(path: &Path) -> io::Result<File> {
    use std::os::windows::fs::{MetadataExt as _, OpenOptionsExt as _};
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    let mut options = OpenOptions::new();
    options
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT);
    let directory = options.open(path)?;
    let metadata = directory.metadata()?;
    if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "process signal capability is not a plain directory",
        ));
    }
    Ok(directory)
}

fn release_path_identity(path: &Path) -> Result<String, ProcessTestConfigError> {
    let parent = path
        .parent()
        .ok_or_else(|| ProcessTestConfigError::InvalidReleasePath(path.to_path_buf()))?;
    let file_name = path
        .file_name()
        .ok_or_else(|| ProcessTestConfigError::InvalidReleasePath(path.to_path_buf()))?;
    let canonical_parent = fs::canonicalize(parent)
        .map_err(|_| ProcessTestConfigError::InvalidReleasePath(path.to_path_buf()))?;
    let identity = canonical_parent
        .join(file_name)
        .to_string_lossy()
        .into_owned();
    #[cfg(windows)]
    let identity = identity.to_lowercase();
    Ok(identity)
}

async fn wait_for_virtual_signal(
    signals: &ProcessSignalDirectory,
    path: &Path,
) -> std::io::Result<()> {
    let name = signals.child_name(path).map_err(io::Error::other)?;
    loop {
        if signals.release_ready(&name)? {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[cfg(not(windows))]
fn release_metadata_is_regular(metadata: &fs::Metadata) -> bool {
    metadata.is_file() && !metadata.file_type().is_symlink()
}

#[cfg(windows)]
fn release_metadata_is_regular(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
    metadata.is_file() && metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0
}

fn path_is_within(path: &Path, root: &Path) -> bool {
    path.starts_with(root)
}

fn claimed_scenario_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("scenario");
    path.with_file_name(format!(
        ".{file_name}.consuming-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ))
}

fn consume_claimed_scenario(
    file: &mut File,
    path: &Path,
    expected: &[u8],
    identity: ScenarioFileIdentity,
) -> Result<(), ProcessTestConfigError> {
    if validate_scenario_handle(file, path)? != identity {
        return Err(ProcessTestConfigError::ScenarioChanged);
    }
    let mut claimed = read_scenario_bounded(file, path)?;
    let unchanged = claimed == expected;
    claimed.fill(0);
    if validate_scenario_handle(file, path)? != identity {
        return Err(ProcessTestConfigError::ScenarioChanged);
    }
    zero_scenario_handle(file, path, expected.len() as u64)?;
    verify_claimed_scenario_identity(path, identity)?;
    fs::remove_file(path).map_err(|source| ProcessTestConfigError::Io {
        action: "remove consumed scenario",
        path: path.to_path_buf(),
        source,
    })?;
    if unchanged {
        Ok(())
    } else {
        Err(ProcessTestConfigError::ScenarioChanged)
    }
}

fn zero_scenario_handle(
    file: &mut File,
    path: &Path,
    len: u64,
) -> Result<(), ProcessTestConfigError> {
    file.set_len(len)
        .map_err(|source| ProcessTestConfigError::Io {
            action: "bound claimed scenario before zeroing",
            path: path.to_path_buf(),
            source,
        })?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| ProcessTestConfigError::Io {
            action: "rewind claimed scenario",
            path: path.to_path_buf(),
            source,
        })?;
    let zeroes = [0_u8; 8 * 1024];
    let mut remaining = len;
    while remaining > 0 {
        let count = usize::try_from(remaining.min(zeroes.len() as u64))
            .expect("zeroing chunk length fits usize");
        file.write_all(&zeroes[..count])
            .map_err(|source| ProcessTestConfigError::Io {
                action: "zero claimed scenario",
                path: path.to_path_buf(),
                source,
            })?;
        remaining -= count as u64;
    }
    file.flush().map_err(|source| ProcessTestConfigError::Io {
        action: "flush zeroed scenario",
        path: path.to_path_buf(),
        source,
    })?;
    file.sync_all()
        .map_err(|source| ProcessTestConfigError::Io {
            action: "sync zeroed scenario",
            path: path.to_path_buf(),
            source,
        })?;
    file.set_len(0)
        .map_err(|source| ProcessTestConfigError::Io {
            action: "truncate zeroed scenario",
            path: path.to_path_buf(),
            source,
        })?;
    file.sync_all()
        .map_err(|source| ProcessTestConfigError::Io {
            action: "sync truncated scenario",
            path: path.to_path_buf(),
            source,
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::sync::Arc;

    use super::{
        ActorPauseController, ActorPausePoint, LegacyV2Seed, ProcessRunnerMode,
        ProcessSignalDirectory, ProcessStorageSample, ProcessTestConfig, ProcessTestConfigError,
        ProcessTestWatchers, StoreWriterFaultPoint, StoreWriterFaultSpec, VirtualReleaseSignal,
        VirtualReleaseTarget, wait_for_virtual_signal,
    };
    use crate::StoreWriterOperationKind;

    fn signal_fixture() -> (tempfile::TempDir, Arc<ProcessSignalDirectory>) {
        let fixture = tempfile::tempdir().expect("create process-signal fixture");
        let signals = Arc::new(
            ProcessSignalDirectory::prepare(&fixture.path().join("signals"))
                .expect("prepare process-signal capability"),
        );
        (fixture, signals)
    }

    #[test]
    fn process_signal_capability_close_is_idempotent_and_rejects_later_io() {
        let (fixture, signals) = signal_fixture();
        let signal_name = OsStr::new("closed.signal");
        assert!(!signals.release_ready(signal_name).unwrap());

        assert_eq!(signals.close_capability(), Ok(()));
        assert_eq!(signals.close_capability(), Ok(()));
        for error in [
            signals.release_ready(signal_name).unwrap_err(),
            signals.publish_reached(signal_name).unwrap_err(),
            signals
                .publish_probe_bytes(signal_name, b"closed")
                .unwrap_err(),
        ] {
            assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
        }

        drop(signals);
        let root = fixture.path().to_path_buf();
        fixture
            .close()
            .expect("closed signal capability releases its fixture directory");
        assert!(!root.exists());
    }

    #[tokio::test]
    async fn process_test_watcher_shutdown_joins_once_and_replays_to_concurrent_callers() {
        let capability = Arc::new(());
        let released = Arc::downgrade(&capability);
        let watcher = tokio::spawn(async move {
            let _capability = capability;
            std::future::pending::<()>().await;
        });
        let watchers = ProcessTestWatchers::new(vec![watcher]);

        let (first, concurrent) =
            tokio::join!(watchers.shutdown_and_join(), watchers.shutdown_and_join());

        assert_eq!(first, Ok(()));
        assert_eq!(concurrent, Ok(()));
        assert_eq!(watchers.shutdown_and_join().await, Ok(()));
        assert!(
            released.upgrade().is_none(),
            "successful watcher shutdown releases task-owned capabilities"
        );
    }

    #[tokio::test]
    async fn virtual_release_file_is_observed_without_destructive_consumption() {
        let (_fixture, signals) = signal_fixture();
        let path = signals.path.join("release.signal");
        let waiter = tokio::spawn({
            let path = path.clone();
            let signals = signals.clone();
            async move { wait_for_virtual_signal(&signals, &path).await }
        });

        tokio::fs::write(&path, [])
            .await
            .expect("publish virtual release signal");
        waiter
            .await
            .expect("join virtual signal waiter")
            .expect("consume virtual release signal");

        assert!(
            path.is_file(),
            "release observation must not delete the file"
        );
    }

    #[tokio::test]
    async fn actor_release_is_race_safe_and_each_pause_is_consumed_once() {
        let (_fixture, signals) = signal_fixture();
        let reached = signals.path.join("claim.release.reached");
        let controller = Arc::new(ActorPauseController::new(
            signals,
            [(ActorPausePoint::ClaimPermitAcquired, reached.clone())],
        ));

        assert!(controller.release(ActorPausePoint::ClaimPermitAcquired));
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                controller.pause(ActorPausePoint::ClaimPermitAcquired),
            )
            .await
            .expect("a release published before the hook remains available")
        );
        assert!(
            reached.is_file(),
            "release-before-pause must still publish the reached marker"
        );
        assert!(
            !controller.pause(ActorPausePoint::ClaimPermitAcquired).await,
            "the same configured pause is consumed only once"
        );
        assert!(
            !controller.release(ActorPausePoint::RetryBeforeWrite),
            "an unconfigured release target fails closed"
        );
    }

    #[tokio::test]
    async fn actor_pause_publishes_reached_before_waiting_for_release() {
        let (_fixture, signals) = signal_fixture();
        let reached = signals.path.join("retry.release.reached");
        let controller = Arc::new(ActorPauseController::new(
            signals,
            [(ActorPausePoint::RetryBeforeWrite, reached.clone())],
        ));
        let paused = tokio::spawn({
            let controller = controller.clone();
            async move { controller.pause(ActorPausePoint::RetryBeforeWrite).await }
        });

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !reached.is_file() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actor reached marker is published");
        assert!(
            !paused.is_finished(),
            "the hook remains paused after publishing"
        );

        assert!(controller.release(ActorPausePoint::RetryBeforeWrite));
        assert!(paused.await.expect("join actor pause"));
    }

    #[test]
    fn failed_atomic_reached_publication_cleans_its_private_temporary_file() {
        let (_fixture, signals) = signal_fixture();
        let name = OsStr::new("occupied.reached");
        std::fs::write(signals.path.join(name), b"occupied").expect("occupy reached path");

        signals
            .publish_reached(name)
            .expect_err("atomic publication never replaces an existing path");

        let temporary_count = std::fs::read_dir(&signals.path)
            .expect("scan signal directory")
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".process-probe-")
            })
            .count();
        assert_eq!(temporary_count, 0);
    }

    #[test]
    fn probe_publication_replaces_the_previous_generation() {
        let (_fixture, signals) = signal_fixture();
        let name = OsStr::new("generation.probe");

        signals
            .publish_probe_bytes(name, b"first")
            .expect("publish first probe generation");
        signals
            .publish_probe_bytes(name, b"second")
            .expect("replace probe generation");

        assert_eq!(
            std::fs::read(signals.path.join(name)).expect("read current probe generation"),
            b"second"
        );
    }

    #[test]
    fn process_store_pauses_require_one_matching_release() {
        let (_fixture, signals) = signal_fixture();
        let release_path = signals.path.join("store.release");
        let pause = StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::InterruptRemainingAfterStops),
            count: 1,
        };
        let base = ProcessTestConfig {
            runner_mode: ProcessRunnerMode::ScriptedFake {},
            runtime_config: None,
            fake_scenarios: Vec::new(),
            storage_samples: vec![ProcessStorageSample::Native],
            store_writer_faults: vec![pause.clone()],
            actor_pauses: Vec::new(),
            virtual_release_signals: Vec::new(),
            legacy_v2_seed: LegacyV2Seed::None,
            marker_write_failure: false,
        };

        assert!(matches!(
            base.validate(),
            Err(ProcessTestConfigError::MissingStoreRelease(
                StoreWriterFaultPoint::PauseBeforeExecute
            ))
        ));

        let mut invalid_count = base.clone();
        invalid_count.store_writer_faults[0].count = 2;
        assert!(matches!(
            invalid_count.validate(),
            Err(ProcessTestConfigError::InvalidStorePauseCount {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                count: 2,
            })
        ));

        let release = VirtualReleaseSignal {
            name: "store".to_owned(),
            path: release_path,
            target: VirtualReleaseTarget::StoreWriterBeforeExecute,
        };
        let mut valid = base;
        valid.virtual_release_signals.push(release.clone());
        valid.validate().expect("one process pause has one release");

        let mut unexpected = valid;
        unexpected.store_writer_faults.clear();
        assert!(matches!(
            unexpected.validate(),
            Err(ProcessTestConfigError::UnexpectedStoreRelease(
                StoreWriterFaultPoint::PauseBeforeExecute
            ))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn unix_signal_capability_ignores_a_replaced_parent_without_touching_its_target() {
        use std::os::unix::fs::symlink;

        let (fixture, signals) = signal_fixture();
        let moved = fixture.path().join("signals-moved");
        let outside = fixture.path().join("outside");
        std::fs::create_dir(&outside).expect("create outside directory");
        std::fs::rename(&signals.path, &moved).expect("move named signal directory");
        symlink(&outside, &signals.path).expect("redirect original signal path");
        let outside_release = outside.join("release.signal");
        std::fs::write(&outside_release, []).expect("publish outside decoy release");

        assert!(
            !signals
                .release_ready(OsStr::new("release.signal"))
                .expect("inspect capability-bound release"),
            "an outside decoy must not release the actor"
        );
        signals
            .publish_reached(OsStr::new("actor.reached"))
            .expect("publish through held directory capability");

        assert!(outside_release.is_file(), "outside decoy is never deleted");
        assert!(!outside.join("actor.reached").exists());
        assert!(moved.join("actor.reached").is_file());
    }

    #[cfg(windows)]
    #[test]
    fn windows_signal_capability_prevents_parent_replacement() {
        let (fixture, signals) = signal_fixture();
        let moved = fixture.path().join("signals-moved");

        std::fs::rename(&signals.path, &moved)
            .expect_err("the non-delete-sharing directory capability blocks replacement");
        assert!(signals.path.is_dir());
        assert!(!moved.exists());
    }
}
