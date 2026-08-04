use std::any::Any;
use std::num::NonZeroU32;
use std::sync::Arc;

use coding_agent_domain::UtcTimestamp;
use coding_agent_runtime::ProcessLivenessScope;
use coding_agent_store::{RecoveryReceipt, Store};
use uuid::Uuid;

use crate::repository_service::{RepositoryDiscovery, RepositoryRuntimeRegistrar};
use crate::runner_factory::{StartupRunnerSelection, ValidatedStartupInputs};
use crate::runtime_config::load_runtime_config_with_parallelism;
#[cfg(feature = "test-support")]
use crate::test_support::{ActorPausePoint, ProcessTestWatchers};
use crate::{
    EventDispatcherHandle, MutationGate, PreActorStartupRunnerContext, ServiceStateController,
    StoreWriterHandle, TaskManagerHandle,
};

use super::{
    InstanceLock, LockLease, PlatformPaths, PrimaryRuntime, PrimaryRuntimeCleanup,
    StartupDependencies, StartupError, StartupShutdownGuard,
};

mod actors;
mod http;

pub(super) async fn start_primary(
    paths: PlatformPaths,
    lock: InstanceLock,
    instance_id: Uuid,
    dependencies: StartupDependencies,
) -> Result<PrimaryRuntime, StartupError> {
    let configured = ConfiguredPrimary::load(paths, lock, instance_id, dependencies).await?;
    let sentinel = configured.claim_sentinel().await?;
    let recovered = sentinel.recover_store().await?;
    let actors = recovered.start_actors().await?;
    http::finish(actors).await
}

struct StartupContext {
    paths: PlatformPaths,
    instance_id: Uuid,
    dependencies: StartupDependencies,
}

struct ConfiguredPrimary {
    context: StartupContext,
    lock: InstanceLock,
    max_queued_tasks: NonZeroU32,
    validated_inputs: ValidatedStartupInputs,
}

impl ConfiguredPrimary {
    async fn load(
        paths: PlatformPaths,
        lock: InstanceLock,
        instance_id: Uuid,
        dependencies: StartupDependencies,
    ) -> Result<Self, StartupError> {
        let available_parallelism = dependencies.available_parallelism.available_parallelism();
        let runtime_config = load_runtime_config_with_parallelism(&paths, available_parallelism)?;
        let max_queued_tasks = runtime_config.max_queued_tasks();
        let runner_inputs = dependencies
            .runner_factory
            .validate_pre_database(&paths)
            .await?;
        let validated_inputs = ValidatedStartupInputs::new(runtime_config, runner_inputs);

        Ok(Self {
            context: StartupContext {
                paths,
                instance_id,
                dependencies,
            },
            lock,
            max_queued_tasks,
            validated_inputs,
        })
    }

    async fn claim_sentinel(self) -> Result<SentinelReady, StartupError> {
        let process_liveness = super::await_process_liveness_directory(&self.context.paths).await;
        super::await_previous_process_cleanup(&process_liveness).await;
        let instance_process_scope =
            process_liveness.instance_scope(*self.context.instance_id.as_bytes())?;
        let cleanup = Arc::new(PrimaryRuntimeCleanup::new(
            self.lock,
            self.context.paths.instance_descriptor.clone(),
            instance_process_scope.clone(),
        ));
        super::remove_stale_descriptors(&self.context.paths)?;

        #[cfg(feature = "test-support")]
        let test_signal_watchers = self
            .context
            .dependencies
            .process_test_support
            .as_ref()
            .map_or_else(ProcessTestWatchers::default, |support| {
                support.spawn_virtual_release_watchers()
            });

        Ok(SentinelReady {
            context: self.context,
            max_queued_tasks: self.max_queued_tasks,
            validated_inputs: self.validated_inputs,
            cleanup,
            instance_process_scope,
            #[cfg(feature = "test-support")]
            test_signal_watchers,
        })
    }
}

struct SentinelReady {
    context: StartupContext,
    max_queued_tasks: NonZeroU32,
    validated_inputs: ValidatedStartupInputs,
    cleanup: Arc<PrimaryRuntimeCleanup>,
    instance_process_scope: ProcessLivenessScope,
    #[cfg(feature = "test-support")]
    test_signal_watchers: ProcessTestWatchers,
}

impl SentinelReady {
    async fn recover_store(self) -> Result<RecoveredStore, StartupError> {
        let store = self
            .context
            .dependencies
            .stores
            .open(&self.context.paths.database_path)
            .await?;
        store.migrate().await?;
        let started_at = UtcTimestamp::new(self.context.dependencies.wall_clock.now_utc())
            .map_err(|_| StartupError::Timestamp)?;
        let pre_actor_context = PreActorStartupRunnerContext::new(
            self.context.paths.clone(),
            store.clone(),
            self.context.dependencies.wall_clock.clone(),
            self.validated_inputs,
            self.context.instance_id,
            self.instance_process_scope.clone(),
        );
        let prepared_runner_inputs = self
            .context
            .dependencies
            .runner_factory
            .prepare_before_actors(&pre_actor_context)
            .await?;
        let recovery = store.recover_after_restart().await?;
        #[cfg(feature = "test-support")]
        publish_test_recovery_probe(&self.context.dependencies, &recovery).await;
        super::remove_recovered_shutdown_marker(&self.context.paths);

        Ok(RecoveredStore {
            context: self.context,
            max_queued_tasks: self.max_queued_tasks,
            cleanup: self.cleanup,
            instance_process_scope: self.instance_process_scope,
            store,
            started_at,
            pre_actor_context,
            prepared_runner_inputs,
            recovery,
            #[cfg(feature = "test-support")]
            test_signal_watchers: self.test_signal_watchers,
        })
    }
}

#[cfg(feature = "test-support")]
async fn publish_test_recovery_probe(
    dependencies: &StartupDependencies,
    recovery: &RecoveryReceipt,
) {
    if let Some(support) = &dependencies.process_test_support {
        if support
            .publish_startup_recovery_probe(recovery.interrupted_count)
            .is_err()
        {
            tracing::warn!(
                error_code = "TEST_RECOVERY_PROBE_FAILED",
                "process-test recovery probe could not be published"
            );
        }
        support
            .actor_pauses
            .pause(ActorPausePoint::RecoveryBeforeDescriptor)
            .await;
    }
}

struct RecoveredStore {
    context: StartupContext,
    max_queued_tasks: NonZeroU32,
    cleanup: Arc<PrimaryRuntimeCleanup>,
    instance_process_scope: ProcessLivenessScope,
    store: Store,
    started_at: UtcTimestamp,
    pre_actor_context: PreActorStartupRunnerContext,
    prepared_runner_inputs: Arc<dyn Any + Send + Sync>,
    recovery: RecoveryReceipt,
    #[cfg(feature = "test-support")]
    test_signal_watchers: ProcessTestWatchers,
}

impl RecoveredStore {
    async fn start_actors(self) -> Result<ActorsReady, StartupError> {
        actors::start(self).await
    }
}

struct ActorsReady {
    context: StartupContext,
    max_queued_tasks: NonZeroU32,
    cleanup: Arc<PrimaryRuntimeCleanup>,
    instance_process_scope: ProcessLivenessScope,
    store: Store,
    started_at: UtcTimestamp,
    writer: StoreWriterHandle,
    dispatcher: EventDispatcherHandle,
    task_manager: TaskManagerHandle,
    runner_selection: StartupRunnerSelection,
    repository_registrar: Option<RepositoryRuntimeRegistrar>,
    repository_discovery: Option<RepositoryDiscovery>,
    service_state: ServiceStateController,
    mutation_gate: MutationGate,
    lock_keepalive: Option<Arc<LockLease>>,
    shutdown_guard: StartupShutdownGuard,
    #[cfg(feature = "test-support")]
    test_signal_watchers: ProcessTestWatchers,
}
