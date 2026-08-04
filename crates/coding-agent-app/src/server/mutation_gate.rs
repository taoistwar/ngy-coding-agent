use std::collections::HashSet;
use std::future::Future;
use std::sync::{Arc, Mutex};

use coding_agent_api::ApiResult;
use coding_agent_domain::{ClientRequestId, RepositoryId, TaskId};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{ServiceState, ServiceStateController};

use super::{app_shutting_down, store_degraded};

#[derive(Clone)]
pub struct MutationGate {
    inner: Arc<MutationGateInner>,
}

struct MutationGateInner {
    service_state: ServiceStateController,
    lifecycle: Mutex<MutationLifecycle>,
    idle: Notify,
    force_cancel: CancellationToken,
}

#[derive(Debug, Default)]
struct MutationLifecycle {
    closed: bool,
    active: usize,
    delegated: usize,
    forced: bool,
    // Unidentified writes and forced cancellation cannot be reconciled safely in-process.
    irreversible_unproven: bool,
    // Identified writes may be discharged only by their own query-first exact replay.
    unknown_identities: HashSet<DurableMutationIdentity>,
}

pub struct MutationGuard {
    inner: Option<Arc<MutationGateInner>>,
}

pub(super) struct MutationDelegation {
    inner: Option<Arc<MutationGateInner>>,
    identity: Option<DurableMutationIdentity>,
    known: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum DurableMutationIdentity {
    CreateTask {
        client_request_id: ClientRequestId,
        repository_id: RepositoryId,
        prompt: Arc<str>,
    },
    RetryTask(TaskId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MutationDrainOutcome {
    Drained,
    Unproven,
}

impl MutationGate {
    pub fn new(service_state: ServiceStateController) -> Self {
        Self {
            inner: Arc::new(MutationGateInner {
                service_state,
                lifecycle: Mutex::new(MutationLifecycle::default()),
                idle: Notify::new(),
                force_cancel: CancellationToken::new(),
            }),
        }
    }

    pub fn enter_data_mutation(&self) -> ApiResult<MutationGuard> {
        let mut lifecycle = self
            .inner
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if lifecycle.closed {
            return Err(app_shutting_down());
        }
        match self.inner.service_state.current().state {
            ServiceState::Ready => {}
            ServiceState::StoreDegraded => return Err(store_degraded()),
            ServiceState::Quiescing => return Err(app_shutting_down()),
        }
        lifecycle.active = lifecycle
            .active
            .checked_add(1)
            .expect("mutation gate active-count overflow");
        Ok(MutationGuard {
            inner: Some(self.inner.clone()),
        })
    }

    pub(super) async fn run_data_mutation<F, T>(&self, operation: F) -> ApiResult<T>
    where
        F: Future<Output = ApiResult<T>>,
    {
        self.enter_data_mutation()?.run(operation).await
    }

    pub fn prepare_quit(&self) -> ApiResult<()> {
        let lifecycle = self
            .inner
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if lifecycle.closed || self.inner.service_state.current().state == ServiceState::Quiescing {
            Err(app_shutting_down())
        } else {
            Ok(())
        }
    }

    pub async fn wait_for_idle(&self) {
        loop {
            let notified = self.inner.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .inner
                .lifecycle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .active
                == 0
            {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn begin_quiescing(&self) -> bool {
        let mut lifecycle = self
            .inner
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if lifecycle.closed {
            return false;
        }
        // Advance the shared gate generation while entrants are blocked on
        // this mutex. Once the mutex is released, both ServiceState and the
        // local lifecycle agree that no new mutation can enter.
        let _ = self.inner.service_state.set(ServiceState::Quiescing);
        lifecycle.closed = true;
        if lifecycle.active == 0 {
            self.inner.idle.notify_waiters();
        }
        true
    }

    pub(crate) fn force_cancel_in_flight(&self) -> bool {
        let forced = {
            let mut lifecycle = self
                .inner
                .lifecycle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let forced = lifecycle.active != 0;
            lifecycle.forced = true;
            if lifecycle.delegated != 0 {
                lifecycle.irreversible_unproven = true;
            }
            forced
        };
        self.inner.force_cancel.cancel();
        forced
    }

    pub(super) fn mark_delegated(&self) -> ApiResult<MutationDelegation> {
        self.mark_delegated_with_identity(None)
    }

    pub(super) fn mark_identified_delegated(
        &self,
        identity: DurableMutationIdentity,
    ) -> ApiResult<MutationDelegation> {
        self.mark_delegated_with_identity(Some(identity))
    }

    fn mark_delegated_with_identity(
        &self,
        identity: Option<DurableMutationIdentity>,
    ) -> ApiResult<MutationDelegation> {
        let mut lifecycle = self
            .inner
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if lifecycle.forced {
            return Err(app_shutting_down());
        }
        lifecycle.delegated = lifecycle
            .delegated
            .checked_add(1)
            .expect("mutation gate delegated-count overflow");
        Ok(MutationDelegation {
            inner: Some(self.inner.clone()),
            identity,
            known: false,
        })
    }

    pub(crate) async fn drain_until(&self, deadline: Instant) -> MutationDrainOutcome {
        loop {
            let notified = self.inner.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let outcome = {
                let lifecycle = self
                    .inner
                    .lifecycle
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (lifecycle.active == 0).then_some(
                    if lifecycle.irreversible_unproven || !lifecycle.unknown_identities.is_empty() {
                        MutationDrainOutcome::Unproven
                    } else {
                        MutationDrainOutcome::Drained
                    },
                )
            };
            if let Some(outcome) = outcome {
                return outcome;
            }
            tokio::select! {
                biased;
                () = &mut notified => {}
                () = tokio::time::sleep_until(deadline) => {
                    return MutationDrainOutcome::Unproven;
                }
            }
        }
    }
}

impl MutationGuard {
    async fn run<F, T>(self, operation: F) -> ApiResult<T>
    where
        F: Future<Output = ApiResult<T>>,
    {
        let cancellation = self
            .inner
            .as_ref()
            .expect("a live mutation guard retains its gate")
            .force_cancel
            .clone();
        tokio::pin!(operation);
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(app_shutting_down()),
            result = &mut operation => result,
        }
    }
}

impl Drop for MutationGuard {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let mut lifecycle = inner
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(lifecycle.active > 0, "mutation gate guard underflow");
        lifecycle.active -= 1;
        if lifecycle.active == 0 {
            inner.idle.notify_waiters();
        }
    }
}

impl MutationDelegation {
    pub(super) fn confirm_known(mut self) {
        self.known = true;
    }

    pub(super) fn confirm_exact_resolution(mut self) {
        let identity = self
            .identity
            .take()
            .expect("only an identified durable mutation can confirm an exact replay");
        let inner = self
            .inner
            .as_ref()
            .expect("a live mutation delegation retains its gate");
        inner
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .unknown_identities
            .remove(&identity);
        self.known = true;
    }
}

impl Drop for MutationDelegation {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let mut lifecycle = inner
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(
            lifecycle.delegated > 0,
            "mutation gate delegated-count underflow"
        );
        lifecycle.delegated -= 1;
        if !self.known {
            if let Some(identity) = self.identity.take() {
                lifecycle.unknown_identities.insert(identity);
            } else {
                lifecycle.irreversible_unproven = true;
            }
        }
        if lifecycle.active == 0 {
            inner.idle.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use coding_agent_domain::{ClientRequestId, TaskId};

    use super::*;

    fn create_identity(prompt: &str) -> DurableMutationIdentity {
        DurableMutationIdentity::CreateTask {
            client_request_id: ClientRequestId::new(),
            repository_id: RepositoryId::new(),
            prompt: Arc::from(prompt),
        }
    }

    #[tokio::test]
    async fn exact_replays_reconcile_only_their_matching_unknown_identities() {
        let gate = MutationGate::new(ServiceStateController::new(ServiceState::Ready));
        let create_identity = create_identity("matching create");
        let retry_identity = DurableMutationIdentity::RetryTask(TaskId::new());

        drop(
            gate.mark_identified_delegated(create_identity.clone())
                .expect("delegate create"),
        );
        drop(
            gate.mark_identified_delegated(retry_identity.clone())
                .expect("delegate retry"),
        );

        gate.mark_identified_delegated(create_identity)
            .expect("replay create")
            .confirm_exact_resolution();
        assert_eq!(
            gate.drain_until(Instant::now() + Duration::from_secs(1))
                .await,
            MutationDrainOutcome::Unproven,
            "the unrelated retry remains unknown"
        );

        gate.mark_identified_delegated(retry_identity)
            .expect("replay retry")
            .confirm_exact_resolution();
        assert_eq!(
            gate.drain_until(Instant::now() + Duration::from_secs(1))
                .await,
            MutationDrainOutcome::Drained
        );
    }

    #[tokio::test]
    async fn an_exact_identified_replay_cannot_clear_an_unidentified_unknown() {
        let gate = MutationGate::new(ServiceStateController::new(ServiceState::Ready));
        drop(gate.mark_delegated().expect("delegate unidentified write"));

        gate.mark_identified_delegated(create_identity("identified replay"))
            .expect("delegate exact replay")
            .confirm_exact_resolution();

        assert_eq!(
            gate.drain_until(Instant::now() + Duration::from_secs(1))
                .await,
            MutationDrainOutcome::Unproven
        );
    }

    #[tokio::test]
    async fn a_different_create_payload_cannot_reconcile_an_unknown_request() {
        let gate = MutationGate::new(ServiceStateController::new(ServiceState::Ready));
        let client_request_id = ClientRequestId::new();
        let repository_id = RepositoryId::new();
        drop(
            gate.mark_identified_delegated(DurableMutationIdentity::CreateTask {
                client_request_id,
                repository_id,
                prompt: Arc::from("original payload"),
            })
            .expect("delegate original create"),
        );

        gate.mark_identified_delegated(DurableMutationIdentity::CreateTask {
            client_request_id,
            repository_id,
            prompt: Arc::from("different payload"),
        })
        .expect("delegate different create")
        .confirm_exact_resolution();

        assert_eq!(
            gate.drain_until(Instant::now() + Duration::from_secs(1))
                .await,
            MutationDrainOutcome::Unproven
        );
    }

    #[tokio::test]
    async fn forced_shutdown_remains_unproven_even_if_an_identified_write_is_later_confirmed() {
        let gate = MutationGate::new(ServiceStateController::new(ServiceState::Ready));
        let guard = gate.enter_data_mutation().expect("enter mutation gate");
        let delegation = gate
            .mark_identified_delegated(create_identity("forced create"))
            .expect("delegate create");

        assert!(gate.begin_quiescing());
        assert!(gate.force_cancel_in_flight());
        delegation.confirm_exact_resolution();
        drop(guard);

        assert_eq!(
            gate.drain_until(Instant::now() + Duration::from_secs(1))
                .await,
            MutationDrainOutcome::Unproven
        );
    }
}
