use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use coding_agent_app::{
    AttemptReservation, CodingAgentAttempt, CodingAgentAttemptFactory, CodingAttemptError,
    CodingAttemptProvisionError, TaskAgentRuntime,
};
use coding_agent_domain::Repository;
use coding_agent_runtime::{ProcessLivenessScope, WorktreeIdentity};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub(super) struct ProvisionPauseController {
    armed: Mutex<Option<Arc<ProvisionPauseGate>>>,
}

struct ProvisionPauseGate {
    claimed: AtomicBool,
    reached: AtomicBool,
    reached_notify: Notify,
    release: CancellationToken,
}

impl ProvisionPauseGate {
    fn new() -> Self {
        Self {
            claimed: AtomicBool::new(false),
            reached: AtomicBool::new(false),
            reached_notify: Notify::new(),
            release: CancellationToken::new(),
        }
    }
}

impl ProvisionPauseController {
    pub(super) fn arm_next(&self) {
        let mut armed = self
            .armed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(armed.is_none(), "a provision pause is already armed");
        *armed = Some(Arc::new(ProvisionPauseGate::new()));
    }

    pub(super) async fn wait_until_reached(&self) {
        let gate = self
            .armed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .expect("arm a provision pause before waiting");
        loop {
            let notified = gate.reached_notify.notified();
            if gate.reached.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    pub(super) fn release(&self) {
        if let Some(gate) = self
            .armed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            gate.release.cancel();
        }
    }

    async fn pause_if_armed(&self, cancellation: &CancellationToken) {
        let gate = self
            .armed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(gate) = gate else {
            return;
        };
        if gate
            .claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        gate.reached.store(true, Ordering::Release);
        gate.reached_notify.notify_waiters();
        tokio::select! {
            () = gate.release.cancelled() => {}
            () = cancellation.cancelled() => {}
        }
    }
}

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
    provision_pause: Arc<ProvisionPauseController>,
}

impl ObservedAttemptFactory {
    pub(super) fn new(
        inner: Arc<dyn CodingAgentAttemptFactory>,
        tracker: Arc<ControlOperationTracker>,
        provision_pause: Arc<ProvisionPauseController>,
    ) -> Self {
        Self {
            inner,
            tracker,
            provision_pause,
        }
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
            provision_pause: Arc::clone(&self.provision_pause),
        }))
    }
}

struct ObservedAttempt {
    reservation: AttemptReservation,
    inner: tokio::sync::Mutex<Box<dyn CodingAgentAttempt>>,
    tracker: Arc<ControlOperationTracker>,
    provision_pause: Arc<ProvisionPauseController>,
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
        self.provision_pause.pause_if_armed(&cancellation).await;
        self.inner.get_mut().provision(cancellation).await
    }

    async fn runtime(
        &self,
        cancellation: CancellationToken,
    ) -> Result<TaskAgentRuntime, CodingAttemptError> {
        self.inner.lock().await.runtime(cancellation).await
    }
}
