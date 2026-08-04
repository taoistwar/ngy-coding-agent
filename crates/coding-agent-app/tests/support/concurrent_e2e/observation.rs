use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use coding_agent_app::{
    AttemptReservation, CodingAgentAttempt, CodingAgentAttemptFactory, CodingAttemptError,
    CodingAttemptProvisionError, TaskAgentRuntime,
};
use coding_agent_domain::Repository;
use coding_agent_runtime::{ProcessLivenessScope, WorktreeIdentity};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub(super) struct ControlOperationTracker {
    active: AtomicUsize,
    maximum_active: AtomicUsize,
}

impl ControlOperationTracker {
    fn enter(self: &Arc<Self>) -> ControlOperationGuard {
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.maximum_active.fetch_max(active, Ordering::AcqRel);
        ControlOperationGuard {
            tracker: Arc::clone(self),
        }
    }

    pub(super) fn maximum_active(&self) -> usize {
        self.maximum_active.load(Ordering::Acquire)
    }
}

struct ControlOperationGuard {
    tracker: Arc<ControlOperationTracker>,
}

impl Drop for ControlOperationGuard {
    fn drop(&mut self) {
        self.tracker.active.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(super) struct ObservedAttemptFactory {
    inner: Arc<dyn CodingAgentAttemptFactory>,
    tracker: Arc<ControlOperationTracker>,
}

impl ObservedAttemptFactory {
    pub(super) fn new(
        inner: Arc<dyn CodingAgentAttemptFactory>,
        tracker: Arc<ControlOperationTracker>,
    ) -> Self {
        Self { inner, tracker }
    }
}

#[async_trait::async_trait]
impl CodingAgentAttemptFactory for ObservedAttemptFactory {
    async fn prepare(
        &self,
        identity: WorktreeIdentity,
        repository: Repository,
        process_liveness_scope: ProcessLivenessScope,
        cancellation: CancellationToken,
    ) -> Result<Box<dyn CodingAgentAttempt>, CodingAttemptError> {
        let _operation = self.tracker.enter();
        let attempt = self
            .inner
            .prepare(identity, repository, process_liveness_scope, cancellation)
            .await?;
        Ok(Box::new(ObservedAttempt {
            reservation: attempt.reservation().clone(),
            inner: tokio::sync::Mutex::new(attempt),
            tracker: Arc::clone(&self.tracker),
        }))
    }
}

struct ObservedAttempt {
    reservation: AttemptReservation,
    inner: tokio::sync::Mutex<Box<dyn CodingAgentAttempt>>,
    tracker: Arc<ControlOperationTracker>,
}

#[async_trait::async_trait]
impl CodingAgentAttempt for ObservedAttempt {
    fn reservation(&self) -> &AttemptReservation {
        &self.reservation
    }

    async fn open_existing_ready(
        &mut self,
        cancellation: CancellationToken,
    ) -> Result<(), CodingAttemptError> {
        let _operation = self.tracker.enter();
        self.inner.get_mut().open_existing_ready(cancellation).await
    }

    async fn provision(
        &mut self,
        cancellation: CancellationToken,
    ) -> Result<(), CodingAttemptProvisionError> {
        let _operation = self.tracker.enter();
        self.inner.get_mut().provision(cancellation).await
    }

    async fn runtime(
        &self,
        cancellation: CancellationToken,
    ) -> Result<TaskAgentRuntime, CodingAttemptError> {
        self.inner.lock().await.runtime(cancellation).await
    }
}
