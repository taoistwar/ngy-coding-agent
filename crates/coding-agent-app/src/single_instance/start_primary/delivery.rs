use coding_agent_runtime::ProcessLivenessScope;
use coding_agent_store::Store;

use crate::delivery_manager::{
    DeliveryManagerLiveDependencies, DeliveryTaskOwnershipBinding,
    DeliveryTaskOwnershipInstallError,
};
use crate::delivery_reconciliation::DeliveryStartupRecoveryCoordinator;
use crate::{
    DeliveryManagerHandle, ServiceStateController, StartupRunnerFactoryError, StoreWriterHandle,
    TaskManagerHandle, TaskManagerLaunchResources,
};

use super::super::ACTOR_QUEUE_CAPACITY;
use super::{StartupError, StartupRunnerSelection};

pub(super) struct StartedDelivery {
    pub(super) manager: DeliveryManagerHandle,
    task_ownership: DeliveryTaskOwnershipBinding,
}

impl StartedDelivery {
    pub(super) fn install_task_manager(
        &self,
        task_manager: TaskManagerHandle,
    ) -> Result<(), DeliveryTaskOwnershipInstallError> {
        self.task_ownership.install_live(task_manager)
    }
}

pub(super) async fn start(
    store: &Store,
    writer: &StoreWriterHandle,
    runner: &StartupRunnerSelection,
    launch_resources: &TaskManagerLaunchResources,
    service_state: &ServiceStateController,
    instance_process_scope: &ProcessLivenessScope,
) -> Result<StartedDelivery, StartupError> {
    let repository_control = launch_resources.repository_control();
    let task_ownership = DeliveryTaskOwnershipBinding::startup_proven_inactive();
    let Some(startup) = runner.delivery_startup() else {
        return Ok(StartedDelivery {
            manager: DeliveryManagerHandle::spawn_unavailable(
                repository_control,
                service_state.clone(),
                ACTOR_QUEUE_CAPACITY,
            ),
            task_ownership,
        });
    };
    let Some(runtime) = startup.runtime() else {
        if startup.ownership().is_empty() {
            return Ok(StartedDelivery {
                manager: DeliveryManagerHandle::spawn_unavailable(
                    repository_control,
                    service_state.clone(),
                    ACTOR_QUEUE_CAPACITY,
                ),
                task_ownership,
            });
        }
        return Err(delivery_startup_failed());
    };

    let dependencies = DeliveryManagerLiveDependencies::new_for_startup(
        store.clone(),
        writer.clone(),
        task_ownership.clone(),
        repository_control.clone(),
        runtime.runtime(),
        startup.process_proofs(instance_process_scope.clone()),
    )
    .with_live_runtime_registry(runtime.live_runtime())
    .with_cleanup_runtime_registry(runtime.cleanup_runtime());
    let manager = DeliveryManagerHandle::spawn_live(
        dependencies,
        service_state.clone(),
        ACTOR_QUEUE_CAPACITY,
    );
    DeliveryStartupRecoveryCoordinator::new(store, &manager, repository_control.as_ref())
        .recover(startup.ownership())
        .await
        .map_err(|_| delivery_startup_failed())?;

    Ok(StartedDelivery {
        manager,
        task_ownership,
    })
}

fn delivery_startup_failed() -> StartupError {
    StartupError::Runner(StartupRunnerFactoryError::new(
        "DELIVERY_STARTUP_RECOVERY_FAILED",
    ))
}
