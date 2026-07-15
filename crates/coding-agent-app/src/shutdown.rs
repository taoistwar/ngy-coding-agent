use std::future::Future;
use std::io::{self, Write as _};
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use coding_agent_domain::{EventCursor, TaskFailure, TaskId};
use coding_agent_store::{RecoveryOutcome, Store};
use futures_util::FutureExt as _;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Instant, timeout_at};
use uuid::Uuid;

use crate::task_manager::{TaskManagerMessage, current_timestamp};
use crate::{
    EventDispatcherHandle, MutationGate, NativeMessageSink, PlatformPaths, PrivateFile,
    QuiesceResult, RunnerEvent, RunnerOutcome, RunnerShutdownHandle, ServiceState,
    ServiceStateController, StoreWriterHandle, TaskManagerHandle, WallClock,
};

const RECOVERY_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const RECOVERY_WRITE_BUDGET: Duration = Duration::from_secs(5);
const SHUTDOWN_PERSISTENCE_BUDGET: Duration = Duration::from_secs(5);
const SHUTDOWN_TOTAL_BUDGET: Duration = Duration::from_secs(10);
const SHUTDOWN_RUNNER_RESERVE: Duration = Duration::from_secs(2);
const SHUTDOWN_FINALIZE_RESERVE: Duration = Duration::from_secs(1);
const SHUTDOWN_MARKER_ERROR_CODE: &str = "SHUTDOWN_PERSISTENCE_FAILED";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownOutcome {
    Clean,
    Degraded,
}

impl ShutdownOutcome {
    pub const fn exit_code(self) -> i32 {
        match self {
            Self::Clean => 0,
            Self::Degraded => 1,
        }
    }
}

#[async_trait::async_trait]
pub(crate) trait ShutdownCleanup: Send + Sync + 'static {
    async fn stop_http(&self, deadline: Instant);
    fn remove_descriptor_and_release_lock(&self);
}

#[derive(Clone)]
pub struct ShutdownCoordinator {
    inner: Arc<ShutdownCoordinatorInner>,
}

struct ShutdownCoordinatorInner {
    started: AtomicBool,
    outcome: watch::Sender<Option<ShutdownOutcome>>,
    runtime: RuntimeShutdown,
}

struct RuntimeShutdown {
    mutation_gate: MutationGate,
    task_manager: TaskManagerHandle,
    dispatcher: EventDispatcherHandle,
    store: Store,
    cleanup: Arc<dyn ShutdownCleanup>,
    marker_path: PathBuf,
    instance_id: Uuid,
    wall_clock: Arc<dyn WallClock>,
    messages: Arc<dyn NativeMessageSink>,
    marker_writer: Arc<dyn ShutdownMarkerWriter>,
}

impl ShutdownCoordinator {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        mutation_gate: MutationGate,
        task_manager: TaskManagerHandle,
        dispatcher: EventDispatcherHandle,
        store: Store,
        cleanup: Arc<dyn ShutdownCleanup>,
        paths: &PlatformPaths,
        instance_id: Uuid,
        wall_clock: Arc<dyn WallClock>,
        messages: Arc<dyn NativeMessageSink>,
    ) -> Self {
        let (outcome, _) = watch::channel(None);
        Self {
            inner: Arc::new(ShutdownCoordinatorInner {
                started: AtomicBool::new(false),
                outcome,
                runtime: RuntimeShutdown {
                    mutation_gate,
                    task_manager,
                    dispatcher,
                    store,
                    cleanup,
                    marker_path: paths.unclean_shutdown.clone(),
                    instance_id,
                    wall_clock,
                    messages,
                    marker_writer: Arc::new(FilesystemShutdownMarkerWriter),
                },
            }),
        }
    }

    pub async fn shutdown(&self) -> ShutdownOutcome {
        let mut outcome = self.inner.outcome.subscribe();
        if self
            .inner
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let inner = self.inner.clone();
            let started = Instant::now();
            tokio::spawn(async move {
                let result = AssertUnwindSafe(async {
                    match AssertUnwindSafe(inner.runtime.shutdown(started))
                        .catch_unwind()
                        .await
                    {
                        Ok(result) => result,
                        Err(_) => {
                            match AssertUnwindSafe(inner.runtime.emergency_shutdown(started))
                                .catch_unwind()
                                .await
                            {
                                Ok(result) => result,
                                Err(_) => {
                                    inner.runtime.cleanup.remove_descriptor_and_release_lock();
                                    ShutdownOutcome::Degraded
                                }
                            }
                        }
                    }
                })
                .catch_unwind()
                .await
                .unwrap_or(ShutdownOutcome::Degraded);
                inner.outcome.send_replace(Some(result));
            });
        }

        loop {
            if let Some(outcome) = *outcome.borrow() {
                return outcome;
            }
            if outcome.changed().await.is_err() {
                return ShutdownOutcome::Degraded;
            }
        }
    }

    pub(crate) fn force_cleanup(&self) {
        if self
            .inner
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
                self.inner
                    .runtime
                    .cleanup
                    .remove_descriptor_and_release_lock();
            }));
            self.inner
                .outcome
                .send_replace(Some(ShutdownOutcome::Degraded));
        }
    }
}

impl RuntimeShutdown {
    async fn shutdown(&self, started: Instant) -> ShutdownOutcome {
        let persistence_deadline = started + SHUTDOWN_PERSISTENCE_BUDGET;
        let total_deadline = started + SHUTDOWN_TOTAL_BUDGET;
        let runner_deadline = total_deadline - SHUTDOWN_RUNNER_RESERVE;
        let finalize_deadline = total_deadline - SHUTDOWN_FINALIZE_RESERVE;
        let handoff_deadline = finalize_deadline;

        self.mutation_gate.begin_quiescing();
        let quiesce = timeout_at(persistence_deadline, async {
            self.mutation_gate.wait_for_idle().await;
            self.task_manager
                .quiesce_and_interrupt(persistence_deadline)
                .await
        })
        .await;

        let (mut outcome, active, recovery) = match quiesce {
            Ok(Ok(QuiesceResult::Durable { recovery, active })) => {
                (ShutdownOutcome::Clean, active, Some(recovery))
            }
            Ok(Ok(QuiesceResult::Frozen { active, error })) => {
                tracing::error!(error = %error, error_code = SHUTDOWN_MARKER_ERROR_CODE, "shutdown persistence failed");
                (ShutdownOutcome::Degraded, active, None)
            }
            Ok(Err(error)) => {
                tracing::error!(error = %error, error_code = SHUTDOWN_MARKER_ERROR_CODE, "task manager shutdown barrier failed");
                (ShutdownOutcome::Degraded, Vec::new(), None)
            }
            Err(_) => {
                tracing::error!(
                    error_code = SHUTDOWN_MARKER_ERROR_CODE,
                    "shutdown persistence deadline elapsed"
                );
                (ShutdownOutcome::Degraded, Vec::new(), None)
            }
        };

        if outcome == ShutdownOutcome::Degraded {
            self.task_manager.freeze_and_cancel();
        }
        cancel_all(&active);

        if outcome == ShutdownOutcome::Clean {
            wait_for_runners(active, runner_deadline).await;
            if let Some(recovery) = recovery {
                if !result_until(
                    self.dispatcher.flush_to(recovery.high_watermark),
                    finalize_deadline,
                )
                .await
                {
                    tracing::warn!(
                        error_code = "SHUTDOWN_EVENT_FLUSH_INCOMPLETE",
                        "shutdown event flush failed"
                    );
                }
            } else {
                tracing::error!(
                    error_code = SHUTDOWN_MARKER_ERROR_CODE,
                    "shutdown recovery receipt was unavailable"
                );
                outcome = ShutdownOutcome::Degraded;
            }
        }

        let marker_write = (outcome == ShutdownOutcome::Degraded).then(|| {
            tokio::spawn(write_shutdown_marker_until(
                self.marker_writer.clone(),
                self.marker_path.clone(),
                self.instance_id,
                self.wall_clock.now_utc(),
                handoff_deadline,
            ))
        });
        let message_publish = (outcome == ShutdownOutcome::Degraded).then(|| {
            tokio::spawn(publish_degraded_message_until(
                self.messages.clone(),
                handoff_deadline,
            ))
        });

        if outcome == ShutdownOutcome::Degraded {
            self.cleanup.stop_http(Instant::now()).await;
        }

        if !result_until(self.dispatcher.close(), finalize_deadline).await {
            tracing::warn!(
                error_code = "EVENT_DISPATCHER_CLOSE_FAILED",
                "event dispatcher did not close cleanly"
            );
        }

        let checkpoint_closed = outcome == ShutdownOutcome::Clean
            && result_until(self.store.checkpoint_and_close(), finalize_deadline).await;
        if outcome == ShutdownOutcome::Clean && !checkpoint_closed {
            tracing::warn!(
                error_code = "SHUTDOWN_CHECKPOINT_INCOMPLETE",
                "SQLite checkpoint or close did not complete before final cleanup"
            );
        }
        if outcome == ShutdownOutcome::Degraded || !checkpoint_closed {
            close_pool_until(&self.store, finalize_deadline).await;
        }

        self.cleanup.stop_http(finalize_deadline).await;
        if let Some(marker_write) = marker_write {
            match marker_write.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(error = %error, error_code = "SHUTDOWN_MARKER_WRITE_FAILED", "unclean shutdown marker could not be written");
                }
                Err(error) => {
                    tracing::warn!(error = %error, error_code = "SHUTDOWN_MARKER_WORKER_FAILED", "unclean shutdown marker worker failed");
                }
            }
        }
        if let Some(message_publish) = message_publish {
            match message_publish.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(error = %error, error_code = "SHUTDOWN_WARNING_HANDOFF_FAILED", "degraded shutdown warning could not be handed off");
                }
                Err(error) => {
                    tracing::warn!(error = %error, error_code = "SHUTDOWN_WARNING_WORKER_FAILED", "degraded shutdown warning worker failed");
                }
            }
        }
        self.cleanup.remove_descriptor_and_release_lock();

        if outcome == ShutdownOutcome::Degraded {
            tracing::error!(
                error_code = SHUTDOWN_MARKER_ERROR_CODE,
                "Some terminal task states could not be persisted. They will be recovered the next time Coding Agent starts."
            );
        }
        outcome
    }

    async fn emergency_shutdown(&self, started: Instant) -> ShutdownOutcome {
        let handoff_deadline = started + SHUTDOWN_TOTAL_BUDGET - SHUTDOWN_FINALIZE_RESERVE;
        tracing::error!(
            error_code = "SHUTDOWN_COORDINATOR_PANICKED",
            "shutdown coordinator panicked; forcing degraded cleanup"
        );
        self.mutation_gate.begin_quiescing();
        self.task_manager.freeze_and_cancel();
        let marker_write = tokio::spawn(write_shutdown_marker_until(
            self.marker_writer.clone(),
            self.marker_path.clone(),
            self.instance_id,
            self.wall_clock.now_utc(),
            handoff_deadline,
        ));
        let message_publish = tokio::spawn(publish_degraded_message_until(
            self.messages.clone(),
            handoff_deadline,
        ));
        self.cleanup.stop_http(Instant::now()).await;
        let _ = result_until(self.dispatcher.close(), Instant::now()).await;
        close_pool_until(&self.store, Instant::now()).await;
        if let Ok(Err(error)) = marker_write.await {
            tracing::warn!(error = %error, error_code = "SHUTDOWN_MARKER_WRITE_FAILED", "unclean shutdown marker could not be written");
        }
        if let Ok(Err(error)) = message_publish.await {
            tracing::warn!(error = %error, error_code = "SHUTDOWN_WARNING_HANDOFF_FAILED", "degraded shutdown warning could not be handed off");
        }
        self.cleanup.remove_descriptor_and_release_lock();
        ShutdownOutcome::Degraded
    }
}

async fn result_until<F, T, E>(future: F, deadline: Instant) -> bool
where
    F: Future<Output = Result<T, E>>,
{
    tokio::pin!(future);
    tokio::select! {
        biased;
        result = &mut future => result.is_ok(),
        () = tokio::time::sleep_until(deadline) => false,
    }
}

async fn close_pool_until(store: &Store, deadline: Instant) {
    let close = store.pool().close();
    tokio::pin!(close);
    tokio::select! {
        biased;
        () = &mut close => {}
        () = tokio::time::sleep_until(deadline) => {}
    }
}

fn cancel_all(active: &[RunnerShutdownHandle]) {
    for runner in active {
        runner.cancellation.cancel();
    }
}

async fn wait_for_runners(active: Vec<RunnerShutdownHandle>, deadline: Instant) {
    let _ = timeout_at(deadline, async move {
        for runner in active {
            let _ = runner.done.await;
        }
    })
    .await;
}

#[derive(Serialize)]
struct ShutdownMarker {
    timestamp: String,
    instance_id: String,
    error_code: &'static str,
}

trait ShutdownMarkerWriter: Send + Sync + 'static {
    fn prepare(
        &self,
        path: &Path,
        instance_id: Uuid,
        timestamp: time::OffsetDateTime,
    ) -> io::Result<PreparedShutdownMarker>;
}

struct FilesystemShutdownMarkerWriter;

impl ShutdownMarkerWriter for FilesystemShutdownMarkerWriter {
    fn prepare(
        &self,
        path: &Path,
        instance_id: Uuid,
        timestamp: time::OffsetDateTime,
    ) -> io::Result<PreparedShutdownMarker> {
        let staging_path = shutdown_marker_staging_path(path, instance_id)?;
        write_shutdown_marker(&staging_path, instance_id, timestamp)?;
        Ok(PreparedShutdownMarker {
            staging_path,
            canonical_path: path.to_owned(),
        })
    }
}

struct PreparedShutdownMarker {
    staging_path: PathBuf,
    canonical_path: PathBuf,
}

impl PreparedShutdownMarker {
    fn publish(self) -> io::Result<()> {
        std::fs::hard_link(&self.staging_path, &self.canonical_path)?;
        let _ = std::fs::remove_file(&self.staging_path);
        Ok(())
    }
}

impl Drop for PreparedShutdownMarker {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.staging_path);
    }
}

#[derive(Debug, thiserror::Error)]
enum ShutdownMarkerWriteError {
    #[error("the shutdown marker deadline elapsed")]
    Deadline,
    #[error("the shutdown marker worker failed: {0}")]
    Worker(String),
    #[error(transparent)]
    Io(#[from] io::Error),
}

async fn write_shutdown_marker_until(
    writer: Arc<dyn ShutdownMarkerWriter>,
    path: PathBuf,
    instance_id: Uuid,
    timestamp: time::OffsetDateTime,
    deadline: Instant,
) -> Result<(), ShutdownMarkerWriteError> {
    let prepare =
        tokio::task::spawn_blocking(move || writer.prepare(&path, instance_id, timestamp));
    match timeout_at(deadline, prepare).await {
        Ok(Ok(Ok(prepared))) if Instant::now() < deadline => {
            prepared.publish().map_err(ShutdownMarkerWriteError::from)
        }
        Ok(Ok(Ok(_prepared))) => Err(ShutdownMarkerWriteError::Deadline),
        Ok(Ok(Err(error))) => Err(ShutdownMarkerWriteError::from(error)),
        Ok(Err(error)) => Err(ShutdownMarkerWriteError::Worker(error.to_string())),
        Err(_) => Err(ShutdownMarkerWriteError::Deadline),
    }
}

fn shutdown_marker_staging_path(path: &Path, instance_id: Uuid) -> io::Result<PathBuf> {
    let file_name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "shutdown marker path has no file name",
        )
    })?;
    let mut staging_name = file_name.to_os_string();
    staging_name.push(".");
    staging_name.push(instance_id.hyphenated().to_string());
    staging_name.push(".pending");
    Ok(path.with_file_name(staging_name))
}

async fn publish_degraded_message_until(
    messages: Arc<dyn NativeMessageSink>,
    deadline: Instant,
) -> io::Result<()> {
    let publish = tokio::task::spawn_blocking(move || messages.publish_degraded_shutdown());
    match timeout_at(deadline, publish).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(io::Error::other(error.to_string())),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "degraded shutdown warning handoff deadline elapsed",
        )),
    }
}

fn write_shutdown_marker(
    path: &Path,
    instance_id: Uuid,
    timestamp: time::OffsetDateTime,
) -> io::Result<()> {
    let mut file = PrivateFile::create_new(path)?;
    let marker = ShutdownMarker {
        timestamp: timestamp
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(io::Error::other)?,
        instance_id: instance_id.hyphenated().to_string(),
        error_code: SHUTDOWN_MARKER_ERROR_CODE,
    };
    serde_json::to_writer(&mut file, &marker).map_err(io::Error::other)?;
    file.flush()?;
    file.as_file().sync_all()
}

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
        let manager = self
            .manager
            .upgrade()
            .ok_or(DegradedCoordinatorError::ManagerClosed)?;
        if manager.is_closed() {
            return Err(DegradedCoordinatorError::ManagerClosed);
        }
        tokio::select! {
            () = tokio::time::sleep(RECOVERY_RETRY_INTERVAL) => Ok(()),
            () = manager.closed() => Err(DegradedCoordinatorError::ManagerClosed),
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
        let manager = self
            .manager
            .upgrade()
            .ok_or(DegradedCoordinatorError::ManagerClosed)?;
        if manager.is_closed() {
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};

    use coding_agent_domain::{EventCursor, EventId};
    use serde_json::Value;

    use super::*;

    struct PanicOnceCleanup {
        stop_calls: AtomicUsize,
        removed: AtomicBool,
    }

    #[async_trait::async_trait]
    impl ShutdownCleanup for PanicOnceCleanup {
        async fn stop_http(&self, _deadline: Instant) {
            if self.stop_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("injected shutdown cleanup panic");
            }
        }

        fn remove_descriptor_and_release_lock(&self) {
            self.removed.store(true, Ordering::SeqCst);
        }
    }

    struct FixedWallClock;

    impl WallClock for FixedWallClock {
        fn now_utc(&self) -> time::OffsetDateTime {
            time::macros::datetime!(2026-07-15 00:00 UTC)
        }
    }

    struct SilentMessages;

    impl NativeMessageSink for SilentMessages {
        fn show_error(&self, _title: &'static str, _body: String) {}
    }

    struct BlockingMarkerWriter {
        entered: Mutex<Option<oneshot::Sender<()>>>,
        release: Arc<(Mutex<bool>, Condvar)>,
    }

    impl ShutdownMarkerWriter for BlockingMarkerWriter {
        fn prepare(
            &self,
            path: &Path,
            _instance_id: Uuid,
            _timestamp: time::OffsetDateTime,
        ) -> io::Result<PreparedShutdownMarker> {
            if let Some(entered) = self
                .entered
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
            {
                let _ = entered.send(());
            }
            let (released, wake) = &*self.release;
            let mut released = released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while !*released {
                released = wake
                    .wait(released)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            Ok(PreparedShutdownMarker {
                staging_path: path.with_extension("pending"),
                canonical_path: path.to_owned(),
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_marker_write_does_not_consume_the_cleanup_reserve() {
        let (entered, entered_rx) = oneshot::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let writer = Arc::new(BlockingMarkerWriter {
            entered: Mutex::new(Some(entered)),
            release: release.clone(),
        });
        let deadline = Instant::now() + Duration::from_secs(1);
        let write = tokio::spawn(write_shutdown_marker_until(
            writer,
            PathBuf::from("unused-marker"),
            Uuid::new_v4(),
            time::macros::datetime!(2026-07-15 00:00 UTC),
            deadline,
        ));
        entered_rx.await.expect("blocking marker writer starts");

        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(matches!(
            write.await.expect("marker deadline task"),
            Err(ShutdownMarkerWriteError::Deadline)
        ));

        let (released, wake) = &*release;
        *released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        wake.notify_one();
    }

    #[tokio::test]
    async fn a_worker_panic_is_supervised_and_still_resolves_degraded_cleanup() {
        let directory = tempfile::tempdir().expect("create panic-supervision directory");
        let paths = PlatformPaths::new(directory.path().join("data"), directory.path().join("run"));
        paths.prepare().expect("prepare panic-supervision paths");
        let store = Store::open(&paths.database_path)
            .await
            .expect("open panic-supervision store");
        store
            .migrate()
            .await
            .expect("migrate panic-supervision store");
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 8)
            .await
            .expect("spawn panic-supervision dispatcher");
        let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
        let state = ServiceStateController::new(ServiceState::Ready);
        let manager = TaskManagerHandle::spawn(
            store.clone(),
            writer,
            dispatcher.clone(),
            state.clone(),
            Arc::new(crate::FakeTaskRunner::default()),
            1,
            8,
        );
        let cleanup = Arc::new(PanicOnceCleanup {
            stop_calls: AtomicUsize::new(0),
            removed: AtomicBool::new(false),
        });
        let coordinator = ShutdownCoordinator::new(
            MutationGate::new(state),
            manager,
            dispatcher,
            store,
            cleanup.clone(),
            &paths,
            Uuid::new_v4(),
            Arc::new(FixedWallClock),
            Arc::new(SilentMessages),
        );

        let outcome = tokio::time::timeout(Duration::from_secs(2), coordinator.shutdown())
            .await
            .expect("supervisor must resolve a panicked worker");

        assert_eq!(outcome, ShutdownOutcome::Degraded);
        assert!(cleanup.removed.load(Ordering::SeqCst));
        assert_eq!(cleanup.stop_calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn shutdown_marker_contains_only_the_approved_diagnostic_fields() {
        let directory = tempfile::tempdir().expect("create shutdown marker directory");
        let path = directory.path().join("unclean-shutdown.json");
        let instance_id = Uuid::new_v4();

        write_shutdown_marker(
            &path,
            instance_id,
            time::macros::datetime!(2026-07-15 00:00 UTC),
        )
        .expect("write private shutdown marker");

        let document: Value =
            serde_json::from_slice(&std::fs::read(&path).expect("read private shutdown marker"))
                .expect("parse private shutdown marker");
        let object = document.as_object().expect("marker is an object");
        let mut fields = object.keys().map(String::as_str).collect::<Vec<_>>();
        fields.sort_unstable();
        assert_eq!(fields, ["error_code", "instance_id", "timestamp"]);
        assert_eq!(object["instance_id"], instance_id.hyphenated().to_string());
        assert_eq!(object["error_code"], SHUTDOWN_MARKER_ERROR_CODE);
        assert_eq!(object["timestamp"], "2026-07-15T00:00:00Z");
    }

    #[test]
    fn a_non_file_marker_collision_is_a_best_effort_write_failure() {
        let directory = tempfile::tempdir().expect("create shutdown marker directory");
        let path = directory.path().join("unclean-shutdown.json");
        std::fs::create_dir(&path).expect("create marker-path directory collision");

        write_shutdown_marker(
            &path,
            Uuid::new_v4(),
            time::macros::datetime!(2026-07-15 00:00 UTC),
        )
        .expect_err("a directory cannot be accepted as an existing marker");

        assert!(path.is_dir());
    }

    #[test]
    fn an_existing_regular_marker_is_not_silently_accepted_as_this_instances_write() {
        let directory = tempfile::tempdir().expect("create shutdown marker directory");
        let path = directory.path().join("unclean-shutdown.json");
        std::fs::write(&path, b"preexisting marker").expect("create regular marker collision");

        write_shutdown_marker(
            &path,
            Uuid::new_v4(),
            time::macros::datetime!(2026-07-15 00:00 UTC),
        )
        .expect_err("a preexisting marker must remain a best-effort write failure");

        assert_eq!(
            std::fs::read(&path).expect("read untouched preexisting marker"),
            b"preexisting marker"
        );
    }

    struct ScriptedBackend {
        recover: Mutex<VecDeque<Result<RecoveryOutcome, String>>>,
        flush: Mutex<VecDeque<Result<(), String>>>,
        recover_calls: AtomicUsize,
        flush_calls: AtomicUsize,
        first_recover_barrier: Option<Arc<RecoverBarrier>>,
    }

    #[derive(Default)]
    struct RecoverBarrier {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl RecoverBarrier {
        async fn wait_until_entered(&self) {
            self.entered.notified().await;
        }

        fn release(&self) {
            self.release.notify_one();
        }
    }

    #[async_trait::async_trait]
    impl RecoveryBackend for ScriptedBackend {
        async fn recover(&self) -> Result<RecoveryOutcome, String> {
            let attempt = self.recover_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt == 1
                && let Some(barrier) = &self.first_recover_barrier
            {
                barrier.entered.notify_one();
                barrier.release.notified().await;
            }
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
                first_recover_barrier: None,
            }
        }

        fn with_first_recover_barrier(mut self, barrier: Arc<RecoverBarrier>) -> Self {
            self.first_recover_barrier = Some(barrier);
            self
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

    #[tokio::test(start_paused = true)]
    async fn manager_closure_during_recovery_backoff_stops_without_retrying() {
        let recover_barrier = Arc::new(RecoverBarrier::default());
        let backend = Arc::new(
            ScriptedBackend::new([Err("write failed".to_owned())], std::iter::empty())
                .with_first_recover_barrier(recover_barrier.clone()),
        );
        let state = degraded_state();
        let (manager, messages) = mpsc::channel(8);
        let weak_manager = manager.downgrade();
        let coordinator =
            DegradedCoordinator::with_backend(backend.clone(), state, weak_manager.clone());
        let run = tokio::spawn(async move { coordinator.run().await });

        recover_barrier.wait_until_entered().await;
        assert_eq!(backend.recover_calls(), 1);
        recover_barrier.release();
        wait_for_strong_senders(&weak_manager, 2).await;

        drop(messages);

        let result = tokio::time::timeout(Duration::from_millis(1), run)
            .await
            .expect("manager closure must wake the recovery backoff")
            .expect("join degraded coordinator");

        assert_eq!(result, Err(DegradedCoordinatorError::ManagerClosed));
        assert_eq!(backend.recover_calls(), 1);
        drop(manager);
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

    async fn wait_for_strong_senders(
        sender: &mpsc::WeakSender<TaskManagerMessage>,
        expected: usize,
    ) {
        for _ in 0..100 {
            if sender.strong_count() == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("strong sender count did not reach {expected}");
    }

    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }
}
