use std::sync::Arc;
use std::time::Duration;

use coding_agent_domain::{EventCursor, TaskFailure, TaskId};
use coding_agent_store::RecoveryOutcome;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::task_manager::{TaskManagerMessage, current_timestamp};
use crate::{
    EventDispatcherHandle, RunnerEvent, RunnerOutcome, ServiceState, ServiceStateController,
    StoreWriterHandle,
};

const RECOVERY_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const RECOVERY_WRITE_BUDGET: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingDurableResult {
    RunnerEvent {
        task_id: TaskId,
        event: RunnerEvent,
    },
    RunnerTerminal {
        task_id: TaskId,
        outcome: RunnerOutcome,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DegradedRecoveryResult {
    pub recovery: RecoveryOutcome,
    pub discarded_pending_count: usize,
    pub ready_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DegradedCoordinatorError {
    #[error("degraded recovery was superseded by application shutdown")]
    Quiescing,
    #[error("task manager closed before degraded recovery was finalized")]
    ManagerClosed,
}

#[derive(Clone)]
pub struct DegradedCoordinator {
    backend: Arc<dyn RecoveryBackend>,
    service_state: ServiceStateController,
    manager: mpsc::WeakSender<TaskManagerMessage>,
}

#[async_trait::async_trait]
trait RecoveryBackend: Send + Sync + 'static {
    async fn recover(&self) -> Result<RecoveryOutcome, String>;
    async fn flush(&self, high_watermark: EventCursor) -> Result<(), String>;
}

struct RuntimeRecoveryBackend {
    writer: StoreWriterHandle,
    dispatcher: EventDispatcherHandle,
}

#[async_trait::async_trait]
impl RecoveryBackend for RuntimeRecoveryBackend {
    async fn recover(&self) -> Result<RecoveryOutcome, String> {
        let now = current_timestamp().map_err(str::to_owned)?;
        self.writer
            .recover_incomplete(
                now,
                degraded_recovery_failure(),
                Instant::now() + RECOVERY_WRITE_BUDGET,
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|error| error.to_string())
    }

    async fn flush(&self, high_watermark: EventCursor) -> Result<(), String> {
        self.dispatcher
            .flush_to(high_watermark)
            .await
            .map_err(|error| error.to_string())
    }
}

impl DegradedCoordinator {
    pub(crate) fn new(
        writer: StoreWriterHandle,
        dispatcher: EventDispatcherHandle,
        service_state: ServiceStateController,
        manager: mpsc::WeakSender<TaskManagerMessage>,
    ) -> Self {
        Self {
            backend: Arc::new(RuntimeRecoveryBackend { writer, dispatcher }),
            service_state,
            manager,
        }
    }

    #[cfg(test)]
    fn with_backend(
        backend: Arc<dyn RecoveryBackend>,
        service_state: ServiceStateController,
        manager: mpsc::WeakSender<TaskManagerMessage>,
    ) -> Self {
        Self {
            backend,
            service_state,
            manager,
        }
    }

    pub async fn run(&self) -> Result<DegradedRecoveryResult, DegradedCoordinatorError> {
        let recovery = self.recover_store().await?;
        self.flush_recovery(recovery.high_watermark).await?;
        self.finalize(recovery).await
    }

    async fn recover_store(&self) -> Result<RecoveryOutcome, DegradedCoordinatorError> {
        loop {
            self.ensure_not_quiescing()?;
            match self.backend.recover().await {
                Ok(recovery) => return Ok(recovery),
                Err(error) => {
                    tracing::warn!(error = %error, "degraded store recovery attempt failed");
                    self.wait_to_retry().await?;
                }
            }
        }
    }

    async fn flush_recovery(
        &self,
        high_watermark: coding_agent_domain::EventCursor,
    ) -> Result<(), DegradedCoordinatorError> {
        loop {
            self.ensure_not_quiescing()?;
            match self.backend.flush(high_watermark).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    tracing::warn!(error = %error, "degraded recovery event flush failed");
                    self.wait_to_retry().await?;
                }
            }
        }
    }

    async fn finalize(
        &self,
        recovery: RecoveryOutcome,
    ) -> Result<DegradedRecoveryResult, DegradedCoordinatorError> {
        self.ensure_not_quiescing()?;
        let (response, receiver) = oneshot::channel();
        self.manager
            .upgrade()
            .ok_or(DegradedCoordinatorError::ManagerClosed)?
            .send(TaskManagerMessage::FinalizeDegraded { recovery, response })
            .await
            .map_err(|_| DegradedCoordinatorError::ManagerClosed)?;
        receiver
            .await
            .map_err(|_| DegradedCoordinatorError::ManagerClosed)?
    }

    async fn wait_to_retry(&self) -> Result<(), DegradedCoordinatorError> {
        let mut state = self.service_state.subscribe();
        tokio::select! {
            () = tokio::time::sleep(RECOVERY_RETRY_INTERVAL) => Ok(()),
            result = state.changed() => {
                if result.is_err() || state.borrow().state == ServiceState::Quiescing {
                    Err(DegradedCoordinatorError::Quiescing)
                } else {
                    Ok(())
                }
            }
        }
    }

    fn ensure_not_quiescing(&self) -> Result<(), DegradedCoordinatorError> {
        if self.manager.strong_count() == 0 {
            return Err(DegradedCoordinatorError::ManagerClosed);
        }
        if self.service_state.current().state == ServiceState::Quiescing {
            Err(DegradedCoordinatorError::Quiescing)
        } else {
            Ok(())
        }
    }
}

fn degraded_recovery_failure() -> TaskFailure {
    TaskFailure {
        code: "STORE_WRITE_FAILED".to_owned(),
        message: "task was interrupted while recovering the task store".to_owned(),
        retryable: true,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use coding_agent_domain::{EventCursor, EventId};

    use super::*;

    struct ScriptedBackend {
        recover: Mutex<VecDeque<Result<RecoveryOutcome, String>>>,
        flush: Mutex<VecDeque<Result<(), String>>>,
        recover_calls: AtomicUsize,
        flush_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl RecoveryBackend for ScriptedBackend {
        async fn recover(&self) -> Result<RecoveryOutcome, String> {
            self.recover_calls.fetch_add(1, Ordering::SeqCst);
            self.recover
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .expect("scripted recover result")
        }

        async fn flush(&self, _high_watermark: EventCursor) -> Result<(), String> {
            self.flush_calls.fetch_add(1, Ordering::SeqCst);
            self.flush
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .expect("scripted flush result")
        }
    }

    impl ScriptedBackend {
        fn new(
            recover: impl IntoIterator<Item = Result<RecoveryOutcome, String>>,
            flush: impl IntoIterator<Item = Result<(), String>>,
        ) -> Self {
            Self {
                recover: Mutex::new(recover.into_iter().collect()),
                flush: Mutex::new(flush.into_iter().collect()),
                recover_calls: AtomicUsize::new(0),
                flush_calls: AtomicUsize::new(0),
            }
        }

        fn recover_calls(&self) -> usize {
            self.recover_calls.load(Ordering::SeqCst)
        }

        fn flush_calls(&self) -> usize {
            self.flush_calls.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn a_flush_failure_retries_only_flush_after_recovery_commits() {
        tokio::time::pause();
        let backend = Arc::new(ScriptedBackend::new(
            [Ok(recovery())],
            [Err("read failed".to_owned()), Ok(())],
        ));
        let state = degraded_state();
        let (manager, mut messages) = mpsc::channel(8);
        let coordinator =
            DegradedCoordinator::with_backend(backend.clone(), state.clone(), manager.downgrade());
        let run = tokio::spawn(async move { coordinator.run().await });

        wait_for_calls(&backend.flush_calls, 1).await;
        settle().await;
        assert_eq!(backend.recover_calls(), 1);
        tokio::time::advance(RECOVERY_RETRY_INTERVAL + Duration::from_millis(1)).await;
        wait_for_calls(&backend.flush_calls, 2).await;
        complete_finalization(&mut messages, &state, 2).await;

        let result = run.await.unwrap().unwrap();
        assert_eq!(backend.recover_calls(), 1);
        assert_eq!(backend.flush_calls(), 2);
        assert_eq!(result.discarded_pending_count, 2);
        assert_eq!(result.ready_generation, 2);
    }

    #[tokio::test]
    async fn recover_failure_retries_recovery_without_flushing_early() {
        tokio::time::pause();
        let backend = Arc::new(ScriptedBackend::new(
            [Err("write failed".to_owned()), Ok(recovery())],
            [Ok(())],
        ));
        let state = degraded_state();
        let (manager, mut messages) = mpsc::channel(8);
        let coordinator =
            DegradedCoordinator::with_backend(backend.clone(), state.clone(), manager.downgrade());
        let run = tokio::spawn(async move { coordinator.run().await });

        wait_for_calls(&backend.recover_calls, 1).await;
        settle().await;
        assert_eq!(backend.flush_calls(), 0);
        tokio::time::advance(RECOVERY_RETRY_INTERVAL + Duration::from_millis(1)).await;
        wait_for_calls(&backend.recover_calls, 2).await;
        wait_for_calls(&backend.flush_calls, 1).await;
        complete_finalization(&mut messages, &state, 1).await;

        run.await.unwrap().unwrap();
        assert_eq!(backend.recover_calls(), 2);
        assert_eq!(backend.flush_calls(), 1);
    }

    #[tokio::test]
    async fn quiescing_during_flush_retry_never_finalizes_ready() {
        tokio::time::pause();
        let backend = Arc::new(ScriptedBackend::new(
            [Ok(recovery())],
            [Err("read failed".to_owned())],
        ));
        let state = degraded_state();
        let (manager, mut messages) = mpsc::channel(8);
        let coordinator =
            DegradedCoordinator::with_backend(backend.clone(), state.clone(), manager.downgrade());
        let run = tokio::spawn(async move { coordinator.run().await });

        wait_for_calls(&backend.flush_calls, 1).await;
        settle().await;
        state.set(ServiceState::Quiescing).unwrap();

        assert_eq!(run.await.unwrap(), Err(DegradedCoordinatorError::Quiescing));
        assert_eq!(backend.recover_calls(), 1);
        assert_eq!(backend.flush_calls(), 1);
        assert!(matches!(
            messages.try_recv(),
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
        ));
        assert_eq!(state.current().state, ServiceState::Quiescing);
    }

    fn degraded_state() -> ServiceStateController {
        let state = ServiceStateController::new(ServiceState::Ready);
        state.set(ServiceState::StoreDegraded).unwrap();
        state
    }

    fn recovery() -> RecoveryOutcome {
        let event_id = EventId::new(1).unwrap();
        RecoveryOutcome {
            interrupted_count: 1,
            first_event_id: Some(event_id),
            last_event_id: Some(event_id),
            high_watermark: EventCursor::new(event_id.get()).unwrap(),
        }
    }

    async fn complete_finalization(
        messages: &mut mpsc::Receiver<TaskManagerMessage>,
        state: &ServiceStateController,
        discarded_pending_count: usize,
    ) {
        let TaskManagerMessage::FinalizeDegraded { recovery, response } =
            messages.recv().await.expect("finalization message")
        else {
            panic!("unexpected task-manager message");
        };
        let ready = state.set(ServiceState::Ready).unwrap();
        response
            .send(Ok(DegradedRecoveryResult {
                recovery,
                discarded_pending_count,
                ready_generation: ready.generation,
            }))
            .unwrap();
    }

    async fn wait_for_calls(calls: &AtomicUsize, expected: usize) {
        for _ in 0..100 {
            if calls.load(Ordering::SeqCst) == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("call count did not reach {expected}");
    }

    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }
}
