use super::*;

#[cfg(all(test, feature = "test-support"))]
struct SchedulerRefreshPauseForTest {
    server_instance_id: uuid::Uuid,
    reached: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

#[cfg(all(test, feature = "test-support"))]
pub(super) struct SchedulerRefreshPauseControlForTest {
    server_instance_id: uuid::Uuid,
    reached: oneshot::Receiver<()>,
    release: Option<oneshot::Sender<()>>,
}

#[cfg(all(test, feature = "test-support"))]
impl SchedulerRefreshPauseControlForTest {
    pub(super) async fn wait_until_reached(&mut self) {
        tokio::time::timeout(Duration::from_secs(5), &mut self.reached)
            .await
            .expect("scheduler refresh did not reach the post-read pause")
            .expect("scheduler refresh dropped the post-read pause");
    }

    pub(super) fn resume(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

#[cfg(all(test, feature = "test-support"))]
impl Drop for SchedulerRefreshPauseControlForTest {
    fn drop(&mut self) {
        self.resume();
        let mut pause = scheduler_refresh_pause_for_test()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if pause
            .as_ref()
            .is_some_and(|pause| pause.server_instance_id == self.server_instance_id)
        {
            *pause = None;
        }
    }
}

#[cfg(all(test, feature = "test-support"))]
pub(super) fn install_scheduler_refresh_pause_for_test(
    server_instance_id: uuid::Uuid,
) -> SchedulerRefreshPauseControlForTest {
    let (reached, reached_receiver) = oneshot::channel();
    let (release, release_receiver) = oneshot::channel();
    let previous = scheduler_refresh_pause_for_test()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .replace(SchedulerRefreshPauseForTest {
            server_instance_id,
            reached,
            release: release_receiver,
        });
    assert!(
        previous.is_none(),
        "scheduler refresh pause installed twice"
    );
    SchedulerRefreshPauseControlForTest {
        server_instance_id,
        reached: reached_receiver,
        release: Some(release),
    }
}

#[cfg(all(test, feature = "test-support"))]
fn scheduler_refresh_pause_for_test() -> &'static Mutex<Option<SchedulerRefreshPauseForTest>> {
    static PAUSE: std::sync::OnceLock<Mutex<Option<SchedulerRefreshPauseForTest>>> =
        std::sync::OnceLock::new();
    PAUSE.get_or_init(|| Mutex::new(None))
}

#[cfg(all(test, feature = "test-support"))]
async fn pause_scheduler_refresh_after_snapshot_for_test(server_instance_id: uuid::Uuid) {
    let pause = {
        let mut pause = scheduler_refresh_pause_for_test()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if pause
            .as_ref()
            .is_some_and(|pause| pause.server_instance_id == server_instance_id)
        {
            pause.take()
        } else {
            None
        }
    };
    if let Some(pause) = pause {
        let _ = pause.reached.send(());
        let _ = pause.release.await;
    }
}

impl TaskManager {
    /// A returned runner without exact cleanup proof is a process-wide
    /// admission uncertainty. Deriving this from active ownership avoids a
    /// separately latched pause that could remain stale after the last retry.
    pub(super) fn process_cleanup_pauses_scheduler(&self) -> bool {
        self.active.values().any(|active| {
            active.phase == AdmissionPhase::RunnerReturned && active.cleanup_confirmation.is_none()
        })
    }

    pub(super) fn publish_scheduler_snapshot(
        &mut self,
        snapshot: &SchedulerBootstrapSnapshot,
    ) -> Result<EventCursor, SchedulerProjectionPublishError> {
        let service = self.service_state.current();
        let service_paused = self.main_closed
            || self.is_frozen()
            || service.state != ServiceState::Ready
            || self.process_cleanup_pauses_scheduler()
            || self.repository_control_recovery_pauses_admission();
        let permit_ledger = self.permit_ledger.snapshot();
        let repository_control = Arc::clone(&self.repository_control);
        let (_, storage) = self.scheduler_storage_signals.latest_scheduler_storage();
        let published = self.scheduler_projection.publish_complete(
            snapshot,
            service.generation,
            self.scheduler_public_limits,
            SchedulerRuntimeProjection {
                service_paused,
                permit_ledger: &permit_ledger,
                repository_control: repository_control.as_ref(),
                storage: storage.as_ref(),
            },
        )?;
        let activity = published.public_state().storage_activity()?;
        if let Err(error) = self.synchronize_storage_activity(activity) {
            tracing::error!(%error, "storage activity synchronization could not be scheduled");
            return Err(SchedulerProjectionPublishError::StorageActivitySync);
        }
        Ok(published.as_of_event_id())
    }

    pub(super) async fn refresh_scheduler_after_storage_change(&mut self) {
        let (generation, _) = self.scheduler_storage_signals.latest_scheduler_storage();
        if generation <= self.applied_scheduler_storage_generation {
            return;
        }
        match self.refresh_scheduler_projection().await {
            Ok(_) => {
                self.applied_scheduler_storage_generation = generation;
            }
            Err(error) => {
                tracing::error!(
                    %error,
                    "storage semantic change scheduler projection failed"
                );
                self.freeze_degraded();
            }
        }
    }

    pub(super) async fn refresh_scheduler_after_service_change(&mut self) {
        let service_generation = self.service_state.current().generation;
        if self
            .scheduler_projection
            .current()
            .service_state_generation()
            >= service_generation
        {
            return;
        }
        if let Err(error) = self.refresh_scheduler_projection().await {
            tracing::error!(
                %error,
                "service semantic change scheduler projection failed"
            );
            self.freeze_degraded();
        }
    }

    pub(super) async fn refresh_scheduler_after_process_cleanup_change(
        &mut self,
        cleanup_was_paused: bool,
        force_exact_refresh: bool,
    ) {
        let cleanup_is_paused = self.process_cleanup_pauses_scheduler();
        if cleanup_is_paused == cleanup_was_paused && !force_exact_refresh {
            return;
        }

        // Discard candidates admitted against the prior gate before exposing
        // the new exact projection. The final proof requests a fresh scan only
        // after the unpaused projection has been published.
        self.scan_requested = false;
        self.finish_scan();
        if let Err(error) = self.refresh_scheduler_projection().await {
            tracing::error!(
                %error,
                cleanup_is_paused,
                force_exact_refresh,
                "process cleanup scheduler projection failed"
            );
            self.freeze_degraded();
            return;
        }

        if !cleanup_is_paused && self.claims_allowed() {
            self.scan_requested = true;
        }
    }

    pub(super) fn scheduler_snapshot_has_exact_terminal(
        &self,
        snapshot: &SchedulerBootstrapSnapshot,
        task_id: TaskId,
        event_kind: TaskEventKind,
        projection: EventCursor,
    ) -> bool {
        let Some(active) = self.active.get(&task_id) else {
            return false;
        };
        let Some(task) = snapshot.tasks.iter().find(|task| task.id == task_id) else {
            return false;
        };
        terminal_event_kind(task.status) == Some(event_kind)
            && task.last_event_id.get() == projection.get()
            && snapshot.membership_event_id >= projection
            && !snapshot
                .running_stop_intents
                .iter()
                .any(|intent| intent.task_id == task_id)
            && active
                .terminal_task
                .as_ref()
                .is_some_and(|terminal| terminal == task)
            && terminal_receipt_is_exact(Some(active), task, event_kind, task.last_event_id)
    }

    pub(super) async fn refresh_scheduler_projection(
        &mut self,
    ) -> Result<SchedulerBootstrapSnapshot, SchedulerProjectionRefreshError> {
        self.invalidate_scan_before_scheduler_refresh();
        let store = self.store.clone();
        let read_gate = Arc::clone(&self.scheduler_snapshot_read_gate);
        let _read_guard = read_gate.lock().await;
        let snapshot = store.scheduler_bootstrap_snapshot().await?;
        #[cfg(all(test, feature = "test-support"))]
        pause_scheduler_refresh_after_snapshot_for_test(
            self.scheduler_projection
                .current()
                .public_state()
                .server_instance_id(),
        )
        .await;
        self.dispatcher
            .flush_to(snapshot.membership_event_id)
            .await?;
        self.publish_scheduler_snapshot(&snapshot)?;
        Ok(snapshot)
    }

    pub(super) fn invalidate_scan_before_scheduler_refresh(&mut self) {
        if !self.scan_in_flight {
            return;
        }
        self.finish_scan();
        self.scan_requested = self.claims_allowed();
    }
}
