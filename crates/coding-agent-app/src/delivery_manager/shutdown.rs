use std::sync::atomic::{AtomicBool, Ordering};

use coding_agent_domain::RepositoryId;
use tokio::sync::oneshot;

use crate::{
    DeliveryCleanupAcceptanceOutcome, DeliveryMergeAcceptanceOutcome,
    DeliveryOperationQueryOutcome, DeliveryPreflightUnavailableReason,
    DeliveryQueryUnavailableReason, DeliveryTaskQueryOutcome, RepositoryControlPoisonReason,
};

use super::command::{DeliveryManagerCommand, DeliveryWorkerRetainedOwnership};
use super::{DeliveryManager, DeliveryManagerError, DeliveryManagerHandle};

#[derive(Default)]
pub(super) struct DeliveryShutdownLatch {
    closed: AtomicBool,
}

impl DeliveryShutdownLatch {
    pub(super) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    pub(super) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

/// Proof that the actor crossed its hard intake barrier after every queued,
/// running, and fail-closed retained worker ownership record was discharged.
/// The proof is intentionally constructible only by DeliveryManager.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryManagerShutdownProof {
    _confirmed_empty: (),
}

impl DeliveryManagerShutdownProof {
    const fn confirmed() -> Self {
        Self {
            _confirmed_empty: (),
        }
    }

    pub const fn in_flight_workers(self) -> usize {
        0
    }

    pub const fn queued_workers(self) -> usize {
        0
    }

    pub const fn retained_workers(self) -> usize {
        0
    }
}

impl DeliveryManagerHandle {
    /// Installs the irreversible process-local delivery shutdown barrier and
    /// waits without a deadline for exact worker ownership discharge.
    pub async fn shutdown_and_join(
        &self,
    ) -> Result<DeliveryManagerShutdownProof, DeliveryManagerError> {
        self.shutdown_latch.close();
        self.intake_gate.close();
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(DeliveryManagerCommand::ShutdownAndJoin { response })
            .await
            .map_err(|_| DeliveryManagerError::Closed)?;
        receiver.await.map_err(|_| DeliveryManagerError::Closed)
    }

    pub(crate) fn begin_shutdown(&self) {
        self.shutdown_latch.close();
        self.intake_gate.close();
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub async fn retain_fail_closed_for_shutdown_test(
        &self,
        repository_id: RepositoryId,
    ) -> Result<(), DeliveryManagerError> {
        self.ensure_not_shutdown()?;
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(DeliveryManagerCommand::RetainFailClosedForTest {
                repository_id,
                response,
            })
            .await
            .map_err(|_| DeliveryManagerError::Closed)?;
        receiver
            .await
            .map_err(|_| DeliveryManagerError::Closed)?
            .then_some(())
            .ok_or(DeliveryManagerError::Closed)
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub async fn available_git_permits_for_test(&self) -> usize {
        self.global_git_operations.available_permits()
    }
}

impl DeliveryManager {
    pub(super) fn begin_shutdown_join(
        &mut self,
        response: oneshot::Sender<DeliveryManagerShutdownProof>,
    ) {
        self.intake_gate.close();
        self.hard_shutdown = true;
        self.shutdown_waiters.push(response);
        self.start_queued_workers();
        self.complete_shutdown_join_if_ready();
    }

    pub(super) fn complete_shutdown_join_if_ready(&mut self) {
        if !self.hard_shutdown
            || !self.query_workers.is_empty()
            || !self.mutation_workers.is_empty()
            || !self.retained_fail_closed.is_empty()
            || !self.pending_queries.is_empty()
            || !self.pending_mutations.is_empty()
        {
            return;
        }
        for waiter in self.shutdown_waiters.drain(..) {
            let _ = waiter.send(DeliveryManagerShutdownProof::confirmed());
        }
    }

    #[cfg(feature = "test-support")]
    pub(super) fn retain_fail_closed_for_test(&mut self, repository_id: RepositoryId) -> bool {
        let Ok(key) = self
            .repository_control
            .delivery_coordination_key(repository_id)
        else {
            return false;
        };
        let Ok(global_permit) = self.global_git_operations.clone().try_acquire_owned() else {
            return false;
        };
        let Ok(mut repository_lease) = self.repository_control.try_acquire_delivery(key) else {
            return false;
        };
        if repository_lease
            .mark_poisoned(RepositoryControlPoisonReason::GitChildOutcomeUnknown)
            .is_err()
        {
            return false;
        }
        let Some(worker_id) = self.allocate_worker_id() else {
            return false;
        };
        self.mutation_workers.insert(worker_id);
        self.retained_fail_closed.insert(
            worker_id,
            DeliveryWorkerRetainedOwnership::new(global_permit, repository_lease),
        );
        true
    }
}

pub(super) fn reject_after_shutdown(command: DeliveryManagerCommand) {
    match command {
        DeliveryManagerCommand::Query {
            task_id, response, ..
        } => {
            let _ = response.send(DeliveryTaskQueryOutcome::unavailable(
                task_id,
                DeliveryQueryUnavailableReason::OrchestrationUnavailable,
            ));
        }
        DeliveryManagerCommand::OperationQuery {
            operation_id,
            response,
            ..
        } => {
            let _ = response.send(DeliveryOperationQueryOutcome::unavailable(
                operation_id,
                DeliveryQueryUnavailableReason::OrchestrationUnavailable,
            ));
        }
        DeliveryManagerCommand::Preflight { response, .. } => {
            let _ = response.send(crate::DeliveryPreflightOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::ManagerQuiescing,
            ));
        }
        DeliveryManagerCommand::AcceptMerge { response, .. } => {
            let _ = response.send(DeliveryMergeAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::ManagerQuiescing,
            ));
        }
        DeliveryManagerCommand::RemoveWorktree { response, .. }
        | DeliveryManagerCommand::DeleteBranch { response, .. } => {
            let _ = response.send(DeliveryCleanupAcceptanceOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::ManagerQuiescing,
            ));
        }
        DeliveryManagerCommand::RecoverOperation { response, .. } => {
            let _ = response.send(super::DeliveryOperationRecoveryOutcome::Unavailable);
        }
        DeliveryManagerCommand::WorkerCompleted { .. }
        | DeliveryManagerCommand::ServiceChanged(_)
        | DeliveryManagerCommand::Quiesce { .. }
        | DeliveryManagerCommand::ShutdownAndJoin { .. } => {
            unreachable!("only delivery work intake is rejected after hard shutdown")
        }
        #[cfg(feature = "test-support")]
        DeliveryManagerCommand::RetainFailClosedForTest { response, .. } => {
            let _ = response.send(false);
        }
    }
}
