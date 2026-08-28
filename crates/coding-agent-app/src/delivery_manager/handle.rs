use std::sync::Arc;

use coding_agent_domain::TaskId;
use coding_agent_store::DeliveryOperationId;
use tokio::sync::{Semaphore, mpsc, oneshot};

use crate::{
    DeliveryCleanupAcceptanceOutcome, DeliveryMergeAcceptanceOutcome,
    DeliveryOperationQueryOutcome, DeliveryPreflightOutcome, DeliveryTaskQueryOutcome,
    RepositoryControlCoordinator, ServiceState, ServiceStateController,
};

use super::command::DeliveryManagerCommand;
use super::shutdown::DeliveryShutdownLatch;
use super::{
    DeliveryAcceptRequest, DeliveryDeleteBranchRequest, DeliveryIntakeGate, DeliveryManager,
    DeliveryManagerBackend, DeliveryManagerError, DeliveryManagerLiveDependencies,
    DeliveryManagerQuiesceSnapshot, DeliveryOperationRecoveryOutcome, DeliveryPreflightRequest,
    DeliveryRemoveWorktreeRequest,
};

const DELIVERY_GIT_OPERATION_LIMIT: usize = 2;

#[derive(Clone)]
pub struct DeliveryManagerHandle {
    pub(super) sender: mpsc::Sender<DeliveryManagerCommand>,
    pub(super) intake_gate: Arc<DeliveryIntakeGate>,
    pub(super) shutdown_latch: Arc<DeliveryShutdownLatch>,
    pub(super) global_git_operations: Arc<Semaphore>,
}

impl DeliveryManagerHandle {
    /// Starts the bounded actor without durable/runtime dependencies. This is
    /// retained for composition sites which Task 23 has not connected yet.
    pub fn spawn_unavailable(
        repository_control: Arc<RepositoryControlCoordinator>,
        service_state: ServiceStateController,
        capacity: usize,
    ) -> Self {
        Self::spawn_inner(
            repository_control,
            service_state,
            DeliveryManagerBackend::Unavailable,
            capacity,
        )
    }

    #[allow(dead_code)]
    pub(crate) fn spawn_live(
        dependencies: DeliveryManagerLiveDependencies,
        service_state: ServiceStateController,
        capacity: usize,
    ) -> Self {
        let repository_control = Arc::clone(&dependencies.repository_control);
        Self::spawn_inner(
            repository_control,
            service_state,
            DeliveryManagerBackend::Live(Arc::new(dependencies)),
            capacity,
        )
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn spawn_live_for_test(
        dependencies: DeliveryManagerLiveDependencies,
        service_state: ServiceStateController,
        capacity: usize,
    ) -> Self {
        Self::spawn_live(dependencies, service_state, capacity)
    }

    fn spawn_inner(
        repository_control: Arc<RepositoryControlCoordinator>,
        service_state: ServiceStateController,
        backend: DeliveryManagerBackend,
        capacity: usize,
    ) -> Self {
        assert!(capacity > 0, "delivery-manager capacity must be positive");
        let initial_service = service_state.current();
        let intake_gate = Arc::new(DeliveryIntakeGate::new(
            initial_service.state == ServiceState::Quiescing,
        ));
        let shutdown_latch = Arc::new(DeliveryShutdownLatch::default());
        let global_git_operations = Arc::new(Semaphore::new(DELIVERY_GIT_OPERATION_LIMIT));
        let (sender, receiver) = mpsc::channel(capacity);
        let actor = DeliveryManager::new(
            receiver,
            backend,
            repository_control,
            Arc::clone(&global_git_operations),
            initial_service,
            service_state.clone(),
            Arc::clone(&intake_gate),
            capacity,
        );
        tokio::spawn(actor.run());
        spawn_service_state_bridge(service_state, Arc::clone(&intake_gate), sender.downgrade());
        Self {
            sender,
            intake_gate,
            shutdown_latch,
            global_git_operations,
        }
    }

    pub(super) fn ensure_not_shutdown(&self) -> Result<(), DeliveryManagerError> {
        (!self.shutdown_latch.is_closed())
            .then_some(())
            .ok_or(DeliveryManagerError::Closed)
    }

    pub async fn query(
        &self,
        task_id: TaskId,
    ) -> Result<DeliveryTaskQueryOutcome, DeliveryManagerError> {
        self.ensure_not_shutdown()?;
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(DeliveryManagerCommand::Query {
                task_id,
                completion_sender: self.sender.clone(),
                response,
            })
            .await
            .map_err(|_| DeliveryManagerError::Closed)?;
        receiver.await.map_err(|_| DeliveryManagerError::Closed)
    }

    pub async fn query_operation(
        &self,
        operation_id: DeliveryOperationId,
    ) -> Result<DeliveryOperationQueryOutcome, DeliveryManagerError> {
        self.ensure_not_shutdown()?;
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(DeliveryManagerCommand::OperationQuery {
                operation_id,
                completion_sender: self.sender.clone(),
                response,
            })
            .await
            .map_err(|_| DeliveryManagerError::Closed)?;
        receiver.await.map_err(|_| DeliveryManagerError::Closed)
    }

    pub async fn preflight(
        &self,
        request: DeliveryPreflightRequest,
    ) -> Result<DeliveryPreflightOutcome, DeliveryManagerError> {
        self.ensure_not_shutdown()?;
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(DeliveryManagerCommand::Preflight {
                request,
                completion_sender: self.sender.clone(),
                response,
            })
            .await
            .map_err(|_| DeliveryManagerError::Closed)?;
        receiver.await.map_err(|_| DeliveryManagerError::Closed)
    }

    pub async fn accept_merge(
        &self,
        request: DeliveryAcceptRequest,
    ) -> Result<DeliveryMergeAcceptanceOutcome, DeliveryManagerError> {
        self.ensure_not_shutdown()?;
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(DeliveryManagerCommand::AcceptMerge {
                request,
                completion_sender: self.sender.clone(),
                response,
            })
            .await
            .map_err(|_| DeliveryManagerError::Closed)?;
        receiver.await.map_err(|_| DeliveryManagerError::Closed)
    }

    pub async fn remove_worktree(
        &self,
        request: DeliveryRemoveWorktreeRequest,
    ) -> Result<DeliveryCleanupAcceptanceOutcome, DeliveryManagerError> {
        self.ensure_not_shutdown()?;
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(DeliveryManagerCommand::RemoveWorktree {
                request,
                completion_sender: self.sender.clone(),
                response,
            })
            .await
            .map_err(|_| DeliveryManagerError::Closed)?;
        receiver.await.map_err(|_| DeliveryManagerError::Closed)
    }

    pub async fn delete_branch(
        &self,
        request: DeliveryDeleteBranchRequest,
    ) -> Result<DeliveryCleanupAcceptanceOutcome, DeliveryManagerError> {
        self.ensure_not_shutdown()?;
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(DeliveryManagerCommand::DeleteBranch {
                request,
                completion_sender: self.sender.clone(),
                response,
            })
            .await
            .map_err(|_| DeliveryManagerError::Closed)?;
        receiver.await.map_err(|_| DeliveryManagerError::Closed)
    }

    pub(crate) async fn recover_operation(
        &self,
        operation_id: DeliveryOperationId,
    ) -> Result<DeliveryOperationRecoveryOutcome, DeliveryManagerError> {
        self.ensure_not_shutdown()?;
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(DeliveryManagerCommand::RecoverOperation {
                operation_id,
                completion_sender: self.sender.clone(),
                response,
            })
            .await
            .map_err(|_| DeliveryManagerError::Closed)?;
        receiver.await.map_err(|_| DeliveryManagerError::Closed)
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub async fn recover_operation_for_test(
        &self,
        operation_id: DeliveryOperationId,
    ) -> Result<DeliveryOperationRecoveryOutcome, DeliveryManagerError> {
        self.recover_operation(operation_id).await
    }

    pub async fn quiesce(&self) -> Result<DeliveryManagerQuiesceSnapshot, DeliveryManagerError> {
        self.ensure_not_shutdown()?;
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(DeliveryManagerCommand::Quiesce { response })
            .await
            .map_err(|_| DeliveryManagerError::Closed)?;
        receiver.await.map_err(|_| DeliveryManagerError::Closed)
    }
}

fn spawn_service_state_bridge(
    service_state: ServiceStateController,
    intake_gate: Arc<DeliveryIntakeGate>,
    sender: mpsc::WeakSender<DeliveryManagerCommand>,
) {
    let mut changes = service_state.subscribe();
    tokio::spawn(async move {
        while changes.changed().await.is_ok() {
            let snapshot = *changes.borrow();
            if snapshot.state == ServiceState::Quiescing {
                intake_gate.close();
            }
            let Some(sender) = sender.upgrade() else {
                return;
            };
            if sender
                .send(DeliveryManagerCommand::ServiceChanged(snapshot))
                .await
                .is_err()
            {
                return;
            }
        }
    });
}
