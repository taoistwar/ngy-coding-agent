use std::sync::Arc;

use coding_agent_runtime::ProcessLivenessScope;
use coding_agent_store::Store;

use crate::bootstrap_join::{BootstrapJoin, BootstrapJoinError};
use crate::{
    EventDispatcherHandle, MutationGate, ServiceState, ServiceStateController, ShutdownCoordinator,
    StoreWriterHandle, TaskManagerHandle, TaskManagerLaunchResources,
};

use super::{ActorsReady, RecoveredStore, StartupContext};
use crate::single_instance::{
    ACTOR_QUEUE_CAPACITY, EVENT_BROADCAST_CAPACITY, PrimaryRuntimeCleanup, StartupDependencies,
    StartupError, StartupShutdownGuard,
};

pub(super) async fn start(recovered: RecoveredStore) -> Result<ActorsReady, StartupError> {
    let dispatcher = EventDispatcherHandle::spawn_at(
        recovered.store.clone(),
        EVENT_BROADCAST_CAPACITY,
        recovered.recovery.high_watermark,
    )
    .await?;
    recovered.cleanup.mark_runtime_actors_installed();
    let writer = spawn_store_writer(
        &recovered.context.dependencies,
        &recovered.store,
        &dispatcher,
    );
    let runner_selection = recovered
        .context
        .dependencies
        .runner_factory
        .create(
            recovered
                .pre_actor_context
                .into_live(writer.clone(), recovered.prepared_runner_inputs),
        )
        .await?;
    let runner = runner_selection.runner();
    let launch_resources = runner_selection.launch_resources();
    let repository_registrar = runner_selection
        .repository_registrar()
        .ok_or_else(runner_startup_failed)?;
    let repository_discovery = runner_selection
        .repository_discovery()
        .ok_or_else(runner_startup_failed)?;
    let service_state = ServiceStateController::new(ServiceState::Ready);
    let mutation_gate = MutationGate::new(service_state.clone());
    let lock_keepalive = recovered.cleanup.lock_keepalive();
    let task_manager = spawn_task_manager_with_resources(
        &recovered.context.dependencies,
        &recovered.store,
        &writer,
        &dispatcher,
        &service_state,
        runner,
        launch_resources,
    );
    let shutdown_guard = build_shutdown_guard(
        &recovered.context,
        &recovered.cleanup,
        &recovered.instance_process_scope,
        &recovered.store,
        &dispatcher,
        &task_manager,
        &mutation_gate,
    );
    verify_initial_scheduler_snapshot(&recovered.store, &service_state, &task_manager).await?;

    Ok(ActorsReady {
        context: recovered.context,
        max_queued_tasks: recovered.max_queued_tasks,
        cleanup: recovered.cleanup,
        instance_process_scope: recovered.instance_process_scope,
        store: recovered.store,
        started_at: recovered.started_at,
        writer,
        dispatcher,
        task_manager,
        runner_selection,
        repository_registrar: Some(repository_registrar),
        repository_discovery: Some(repository_discovery),
        service_state,
        mutation_gate,
        lock_keepalive: Some(lock_keepalive),
        shutdown_guard,
        #[cfg(feature = "test-support")]
        test_signal_watchers: recovered.test_signal_watchers,
    })
}

fn spawn_store_writer(
    dependencies: &StartupDependencies,
    store: &Store,
    dispatcher: &EventDispatcherHandle,
) -> StoreWriterHandle {
    #[cfg(not(feature = "test-support"))]
    {
        let _ = dependencies;
        StoreWriterHandle::spawn(
            store.clone(),
            Arc::new(dispatcher.clone()),
            ACTOR_QUEUE_CAPACITY,
        )
    }
    #[cfg(feature = "test-support")]
    {
        match &dependencies.process_test_support {
            Some(support) => StoreWriterHandle::spawn_with_test_controller(
                store.clone(),
                Arc::new(dispatcher.clone()),
                ACTOR_QUEUE_CAPACITY,
                support.writer_controller.clone(),
            ),
            None => StoreWriterHandle::spawn(
                store.clone(),
                Arc::new(dispatcher.clone()),
                ACTOR_QUEUE_CAPACITY,
            ),
        }
    }
}

fn runner_startup_failed() -> StartupError {
    StartupError::Runner(crate::StartupRunnerFactoryError::new(
        "RUNNER_STARTUP_FAILED",
    ))
}

#[allow(clippy::too_many_arguments)]
fn spawn_task_manager_with_resources(
    dependencies: &StartupDependencies,
    store: &Store,
    writer: &StoreWriterHandle,
    dispatcher: &EventDispatcherHandle,
    service_state: &ServiceStateController,
    runner: Arc<dyn crate::TaskRunner>,
    launch_resources: TaskManagerLaunchResources,
) -> TaskManagerHandle {
    #[cfg(not(feature = "test-support"))]
    {
        let _ = dependencies;
        TaskManagerHandle::spawn(
            store.clone(),
            writer.clone(),
            dispatcher.clone(),
            service_state.clone(),
            runner.clone(),
            launch_resources.clone(),
            ACTOR_QUEUE_CAPACITY,
        )
    }
    #[cfg(feature = "test-support")]
    {
        match &dependencies.process_test_support {
            Some(support) => TaskManagerHandle::spawn_with_process_test_pauses(
                store.clone(),
                writer.clone(),
                dispatcher.clone(),
                service_state.clone(),
                runner.clone(),
                launch_resources.clone(),
                ACTOR_QUEUE_CAPACITY,
                support.actor_pauses.clone(),
            ),
            None => TaskManagerHandle::spawn(
                store.clone(),
                writer.clone(),
                dispatcher.clone(),
                service_state.clone(),
                runner.clone(),
                launch_resources,
                ACTOR_QUEUE_CAPACITY,
            ),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn build_shutdown_guard(
    context: &StartupContext,
    cleanup: &Arc<PrimaryRuntimeCleanup>,
    instance_process_scope: &ProcessLivenessScope,
    store: &Store,
    dispatcher: &EventDispatcherHandle,
    task_manager: &TaskManagerHandle,
    mutation_gate: &MutationGate,
) -> StartupShutdownGuard {
    #[cfg(not(feature = "test-support"))]
    let shutdown = ShutdownCoordinator::new(
        mutation_gate.clone(),
        instance_process_scope.clone(),
        task_manager.clone(),
        dispatcher.clone(),
        store.clone(),
        cleanup.clone(),
        &context.paths,
        context.instance_id,
        context.dependencies.wall_clock.clone(),
        context.dependencies.messages.clone(),
    );
    #[cfg(feature = "test-support")]
    let shutdown = ShutdownCoordinator::new_for_process_test(
        mutation_gate.clone(),
        instance_process_scope.clone(),
        task_manager.clone(),
        dispatcher.clone(),
        store.clone(),
        cleanup.clone(),
        &context.paths,
        context.instance_id,
        context.dependencies.wall_clock.clone(),
        context.dependencies.messages.clone(),
        context
            .dependencies
            .process_test_support
            .as_ref()
            .is_some_and(|support| support.config.marker_write_failure),
    );
    StartupShutdownGuard::new(shutdown)
}

async fn verify_initial_scheduler_snapshot(
    store: &Store,
    service_state: &ServiceStateController,
    task_manager: &TaskManagerHandle,
) -> Result<(), StartupError> {
    task_manager
        .notify_admission_changed()
        .await
        .map_err(|_| runner_startup_failed())?;
    let bootstrap_join = BootstrapJoin::new(
        store.clone(),
        service_state.clone(),
        task_manager.scheduler_state_reader(),
    );
    match bootstrap_join.snapshot().await {
        Ok(_) => Ok(()),
        Err(BootstrapJoinError::Store(error)) => Err(StartupError::Store(error)),
        Err(BootstrapJoinError::SnapshotUnavailable) => Err(StartupError::Runner(
            crate::StartupRunnerFactoryError::new("BOOTSTRAP_SNAPSHOT_UNAVAILABLE"),
        )),
    }
}
