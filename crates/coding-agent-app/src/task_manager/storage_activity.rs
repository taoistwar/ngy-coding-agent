use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StorageActivitySubmission {
    sequence: u64,
    activity: StorageActivity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(super) enum StorageActivitySyncError {
    #[error("storage activity synchronization sequence is exhausted")]
    SequenceExhausted,
    #[error("storage activity synchronization completion is inconsistent")]
    CompletionMismatch,
    #[error("storage activity synchronization target is unavailable")]
    TargetUnavailable,
    #[error(transparent)]
    Monitor(#[from] StorageMonitorError),
}

pub(super) struct StorageActivitySynchronizer {
    applied: StorageActivity,
    in_flight: Option<StorageActivitySubmission>,
    pending: Option<StorageActivity>,
    next_sequence: u64,
    #[cfg(test)]
    idle_completion_pause: Option<StorageActivityCompletionPauseForTest>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StorageActivitySyncSnapshotForTest {
    pub(super) applied: StorageActivity,
    pub(super) in_flight: Option<StorageActivity>,
    pub(super) pending: Option<StorageActivity>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct StorageActivityCompletionPauseForTest {
    monitor_completed: Arc<tokio::sync::Notify>,
    actor_waiting_to_exit: Arc<tokio::sync::Notify>,
    release_completion: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
impl StorageActivityCompletionPauseForTest {
    fn new() -> Self {
        Self {
            monitor_completed: Arc::new(tokio::sync::Notify::new()),
            actor_waiting_to_exit: Arc::new(tokio::sync::Notify::new()),
            release_completion: Arc::new(tokio::sync::Notify::new()),
        }
    }

    async fn wait_for_monitor_completion(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.monitor_completed.notified())
            .await
            .expect("real storage monitor applies the final idle activity");
    }

    async fn wait_for_actor_exit_barrier(&self) {
        tokio::time::timeout(
            Duration::from_secs(5),
            self.actor_waiting_to_exit.notified(),
        )
        .await
        .expect("task manager reaches the storage activity exit barrier");
    }

    pub(super) fn notify_actor_waiting_to_exit(&self) {
        self.actor_waiting_to_exit.notify_one();
    }

    fn release(&self) {
        self.release_completion.notify_one();
    }
}

impl StorageActivitySynchronizer {
    pub(super) fn new() -> Self {
        Self {
            applied: StorageActivity::default(),
            in_flight: None,
            pending: None,
            next_sequence: 1,
            #[cfg(test)]
            idle_completion_pause: None,
        }
    }

    pub(super) fn request(
        &mut self,
        activity: StorageActivity,
    ) -> Result<Option<StorageActivitySubmission>, StorageActivitySyncError> {
        if let Some(in_flight) = self.in_flight {
            self.pending = (activity != in_flight.activity).then_some(activity);
            return Ok(None);
        }
        if activity == self.applied {
            self.pending = None;
            return Ok(None);
        }
        self.start(activity).map(Some)
    }

    pub(super) fn complete(
        &mut self,
        submission: StorageActivitySubmission,
        result: Result<(), StorageMonitorError>,
    ) -> Result<Option<StorageActivitySubmission>, StorageActivitySyncError> {
        if self.in_flight != Some(submission) {
            return Err(StorageActivitySyncError::CompletionMismatch);
        }
        self.in_flight = None;
        if let Err(error) = result {
            self.pending = None;
            return Err(error.into());
        }
        self.applied = submission.activity;
        let Some(pending) = self.pending.take() else {
            return Ok(None);
        };
        self.request(pending)
    }

    pub(super) const fn has_in_flight(&self) -> bool {
        self.in_flight.is_some()
    }

    #[cfg(test)]
    pub(super) fn snapshot_for_test(&self) -> StorageActivitySyncSnapshotForTest {
        StorageActivitySyncSnapshotForTest {
            applied: self.applied,
            in_flight: self.in_flight.map(|submission| submission.activity),
            pending: self.pending,
        }
    }

    #[cfg(test)]
    pub(super) fn pause_next_idle_completion_for_test(
        &mut self,
    ) -> StorageActivityCompletionPauseForTest {
        let pause = StorageActivityCompletionPauseForTest::new();
        assert!(
            self.idle_completion_pause.replace(pause.clone()).is_none(),
            "only one storage idle completion pause may be armed"
        );
        pause
    }

    #[cfg(test)]
    fn take_idle_completion_pause_for_test(
        &mut self,
        activity: StorageActivity,
    ) -> Option<StorageActivityCompletionPauseForTest> {
        (activity == StorageActivity::default())
            .then(|| self.idle_completion_pause.take())
            .flatten()
    }

    fn start(
        &mut self,
        activity: StorageActivity,
    ) -> Result<StorageActivitySubmission, StorageActivitySyncError> {
        let sequence = self.next_sequence;
        self.next_sequence = sequence
            .checked_add(1)
            .ok_or(StorageActivitySyncError::SequenceExhausted)?;
        let submission = StorageActivitySubmission { sequence, activity };
        self.in_flight = Some(submission);
        Ok(submission)
    }
}

impl TaskManager {
    pub(super) fn synchronize_storage_activity(
        &mut self,
        activity: StorageActivity,
    ) -> Result<(), StorageActivitySyncError> {
        let Some(monitor) = self.storage_admission.activity_monitor() else {
            return Ok(());
        };
        if let Some(submission) = self.storage_activity_sync.request(activity)? {
            self.spawn_storage_activity_submission(monitor, submission);
        }
        Ok(())
    }

    pub(super) fn handle_storage_activity_synchronized(
        &mut self,
        submission: StorageActivitySubmission,
        result: Result<(), StorageMonitorError>,
    ) {
        let next = match self.storage_activity_sync.complete(submission, result) {
            Ok(next) => next,
            Err(error) => {
                tracing::error!(%error, "storage activity synchronization failed");
                self.freeze_degraded();
                return;
            }
        };
        let Some(next) = next else {
            return;
        };
        let Some(monitor) = self.storage_admission.activity_monitor() else {
            tracing::error!(
                error = %StorageActivitySyncError::TargetUnavailable,
                "storage activity synchronization failed"
            );
            self.freeze_degraded();
            return;
        };
        self.spawn_storage_activity_submission(monitor, next);
    }

    fn spawn_storage_activity_submission(
        &mut self,
        monitor: StorageMonitorHandle,
        submission: StorageActivitySubmission,
    ) {
        let completion_sender = self.completion_sender.clone();
        #[cfg(test)]
        let completion_pause = self
            .storage_activity_sync
            .take_idle_completion_pause_for_test(submission.activity);
        tokio::spawn(async move {
            let result = monitor.set_activity(submission.activity).await;
            #[cfg(test)]
            if let Some(pause) = completion_pause {
                pause.monitor_completed.notify_one();
                pause.release_completion.notified().await;
            }
            let _ = completion_sender
                .send(TaskManagerCompletion::StorageActivitySynchronized { submission, result })
                .await;
        });
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "test-support"))]
mod integration_tests {
    use std::num::{NonZeroU32, NonZeroU64};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use coding_agent_domain::{
        CanonicalPath, ClientRequestId, NewRepository, NewTask, Repository, TaskFailure, TaskStatus,
    };
    use coding_agent_runtime::{
        DirectoryIdentityMarker, RootCapability, VolumeIdentity, VolumeSample, VolumeSampleError,
        VolumeSampler,
    };
    use coding_agent_store::{RegisterRepositoryOutcome, RepositoryIdentityLookup};

    use super::*;
    use crate::SchedulerProjectionSnapshot;

    const MEBIBYTE: u64 = 1024 * 1024;

    #[derive(Default)]
    struct ActivityClock;

    impl crate::StorageMonitorClock for ActivityClock {
        fn now(&self) -> Duration {
            Duration::ZERO
        }
    }

    struct CountingNormalSampler {
        volume: VolumeIdentity,
        calls: AtomicUsize,
        changed: tokio::sync::Notify,
    }

    impl CountingNormalSampler {
        fn new(volume: VolumeIdentity) -> Self {
            Self {
                volume,
                calls: AtomicUsize::new(0),
                changed: tokio::sync::Notify::new(),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl VolumeSampler for CountingNormalSampler {
        fn sample(&self, _root: &RootCapability) -> Result<VolumeSample, VolumeSampleError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.changed.notify_waiters();
            Ok(VolumeSample::for_test(self.volume, 2 * 1024 * MEBIBYTE))
        }
    }

    struct MarkerResolver(DirectoryIdentityMarker);

    impl crate::RepositoryIdentityResolver for MarkerResolver {
        fn resolve(
            &self,
            _identity: &RepositoryIdentityLookup,
        ) -> Result<DirectoryIdentityMarker, crate::RepositoryIdentityResolutionError> {
            Ok(self.0)
        }
    }

    #[derive(Default)]
    struct BlockingFailureRunner {
        started: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl TaskRunner for BlockingFailureRunner {
        async fn run(&self, mut context: RunContext, _sink: RunnerEventSink) -> RunnerOutcome {
            context.complete_preparation_for_test().await;
            self.started.notify_one();
            self.release.notified().await;
            RunnerOutcome::Failed(TaskFailure {
                code: "STORAGE_ACTIVITY_TEST".to_owned(),
                message: "the storage activity integration runner finished".to_owned(),
                retryable: false,
            })
        }
    }

    #[tokio::test]
    async fn scheduler_publication_drives_real_monitor_polling_and_confirmed_idle_exit() {
        let temp = tempfile::tempdir().expect("create storage activity integration directory");
        let store = Store::open(temp.path().join("store.sqlite3"))
            .await
            .expect("open storage activity integration store");
        store
            .migrate()
            .await
            .expect("migrate storage activity integration store");
        let repository = register_activity_repository(&store, temp.path().to_path_buf()).await;
        let root = Arc::new(
            RootCapability::open(temp.path()).expect("open storage activity root capability"),
        );
        let marker = root
            .identity_marker()
            .expect("observe storage activity root identity");
        let volume = VolumeIdentity::for_test(0x5a17);
        let target = crate::StorageProbeTarget::new(volume, root);
        let sampler = Arc::new(CountingNormalSampler::new(volume));
        let clock = Arc::new(ActivityClock);
        let signals = TaskManagerStorageSignals::new();
        let monitor = StorageMonitorHandle::spawn(
            crate::StorageMonitorConfig::new(
                crate::StoragePolicy::try_new(
                    NonZeroU32::new(1).unwrap(),
                    NonZeroU64::new(512 * MEBIBYTE).unwrap(),
                    NonZeroU64::new(128 * MEBIBYTE).unwrap(),
                )
                .unwrap(),
                sampler.clone(),
                clock.clone(),
                Arc::new(signals.clone()),
                Arc::new(signals.clone()),
            ),
            vec![
                crate::MonitoredStorageScopeBinding::data(target.clone()),
                crate::MonitoredStorageScopeBinding::runtime(target.clone()),
                crate::MonitoredStorageScopeBinding::repository_git(repository.id, target.clone()),
            ],
        )
        .expect("spawn real storage monitor");

        let mut resources = test_task_manager_launch_resources(1, 1);
        resources
            .repository_control()
            .register_alias(
                RepositoryIdentityLookup {
                    repository_id: repository.id,
                    git_root: repository.git_root.clone(),
                    git_identity_key: format!("storage-activity-test-{}", repository.id),
                },
                &MarkerResolver(marker),
            )
            .expect("register storage activity repository control identity");
        let coordination_key = resources
            .repository_control()
            .coordination_key(repository.id)
            .expect("read storage activity coordination key");
        let held_control = resources
            .repository_control()
            .try_acquire(coordination_key)
            .expect("hold repository control while queued activity is observed");
        resources.storage_admission = TaskManagerStorageAdmission::Monitor(monitor.clone());
        resources.storage_signals = signals;

        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 32)
            .await
            .expect("spawn storage activity dispatcher");
        let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 16);
        let runner = Arc::new(BlockingFailureRunner::default());
        let manager = TaskManagerHandle::spawn(
            store.clone(),
            writer.clone(),
            dispatcher,
            ServiceStateController::new(ServiceState::Ready),
            runner.clone(),
            resources,
            16,
        );
        let scheduler = manager.scheduler_state_reader();
        let task = writer
            .create_task(
                NewTask::try_new(
                    ClientRequestId::new(),
                    repository.id,
                    "exercise storage activity publication",
                )
                .expect("construct storage activity task"),
                background_deadline(),
            )
            .await
            .expect("create storage activity task")
            .value
            .task()
            .clone();
        manager
            .notify_queued(task.id)
            .await
            .expect("notify queued storage activity task");

        wait_for_scheduler_activity(&scheduler, task.id, TaskStatus::Queued, 0).await;
        wait_for_storage_activity_sync(&manager, StorageActivity::new(1, 0)).await;
        monitor
            .refresh_for_admission(0)
            .await
            .expect("finish the queued phase's current real monitor probe");
        let queued_baseline = sampler.call_count();
        wait_for_sampler_calls(&sampler, queued_baseline + 1).await;

        held_control
            .clean_release()
            .expect("release queued repository control");
        manager
            .notify_admission_changed()
            .await
            .expect("resume admission after queued activity sample");
        runner.started.notified().await;
        wait_for_task_status(&store, task.id, TaskStatus::Running).await;
        wait_for_scheduler_activity(&scheduler, task.id, TaskStatus::Running, 1).await;
        wait_for_storage_activity_sync(&manager, StorageActivity::new(0, 1)).await;
        let active_baseline = sampler.call_count();
        wait_for_sampler_calls(&sampler, active_baseline + 1).await;

        let idle_completion_pause = manager
            .pause_next_storage_idle_completion_for_test()
            .await
            .expect("pause the final idle storage completion");
        let mut exited = manager.install_exit_probe().await;
        runner.release.notify_one();
        wait_for_task_status(&store, task.id, TaskStatus::Failed).await;
        wait_for_scheduler_idle(&scheduler).await;
        idle_completion_pause.wait_for_monitor_completion().await;
        drop(manager);
        idle_completion_pause.wait_for_actor_exit_barrier().await;
        assert!(matches!(
            exited.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        idle_completion_pause.release();
        tokio::time::timeout(Duration::from_secs(2), &mut exited)
            .await
            .expect("task manager waits for the final idle monitor acknowledgement")
            .expect("task manager exits cleanly after idle acknowledgement");

        let idle_baseline = sampler.call_count();
        let unexpected_poll = tokio::time::timeout(
            crate::STORAGE_SAMPLE_INTERVAL + Duration::from_millis(250),
            wait_until_sampler_calls(&sampler, idle_baseline + 1),
        )
        .await;
        assert!(
            unexpected_poll.is_err(),
            "the confirmed idle update disables periodic storage sampling"
        );
        assert_eq!(sampler.call_count(), idle_baseline);
    }

    async fn wait_for_storage_activity_sync(
        manager: &TaskManagerHandle,
        expected: StorageActivity,
    ) {
        let mut last = None;
        for _ in 0..10_000 {
            let snapshot = manager
                .storage_activity_sync_snapshot_for_test()
                .await
                .expect("inspect task-manager storage activity synchronization");
            if snapshot.applied == expected
                && snapshot.in_flight.is_none()
                && snapshot.pending.is_none()
            {
                return;
            }
            last = Some(snapshot);
            tokio::task::yield_now().await;
        }
        panic!("storage activity did not synchronize to {expected:?}; last snapshot: {last:?}");
    }

    async fn register_activity_repository(store: &Store, root: PathBuf) -> Repository {
        let canonical = |path| {
            CanonicalPath::try_from_canonical(path)
                .expect("construct storage activity canonical path")
        };
        let input = NewRepository {
            selected_path: canonical(root.join("selected")),
            display_name: "storage activity".to_owned(),
            git_root: canonical(root.join("git")),
            cargo_workspace_root: canonical(root.join("workspace")),
        };
        match store
            .register_repository(input)
            .await
            .expect("register storage activity repository")
        {
            RegisterRepositoryOutcome::Created(repository)
            | RegisterRepositoryOutcome::Existing(repository) => repository,
        }
    }

    async fn wait_for_sampler_calls(sampler: &CountingNormalSampler, expected: usize) {
        tokio::time::timeout(
            crate::STORAGE_SAMPLE_INTERVAL + Duration::from_secs(2),
            wait_until_sampler_calls(sampler, expected),
        )
        .await
        .unwrap_or_else(|_| panic!("storage sampler did not reach {expected} calls"));
    }

    async fn wait_until_sampler_calls(sampler: &CountingNormalSampler, expected: usize) {
        loop {
            let changed = sampler.changed.notified();
            if sampler.call_count() >= expected {
                return;
            }
            changed.await;
        }
    }

    async fn wait_for_task_status(store: &Store, task_id: TaskId, expected: TaskStatus) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let status = store
                    .task_detail(task_id)
                    .await
                    .expect("load storage activity task")
                    .expect("storage activity task exists")
                    .task
                    .status;
                if status == expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("storage activity task did not reach {expected:?}"));
    }

    async fn wait_for_scheduler_activity(
        scheduler: &SchedulerStateReader<SchedulerStoreState>,
        task_id: TaskId,
        status: TaskStatus,
        active_count: u32,
    ) {
        wait_for_scheduler_state(
            scheduler,
            || format!("publish {status:?} with active={active_count}"),
            |current| {
                current
                    .public_state()
                    .tasks_for_test()
                    .contains(&(task_id, status))
                    && current.public_state().active_task_count_for_test() == active_count
            },
        )
        .await;
    }

    async fn wait_for_scheduler_idle(scheduler: &SchedulerStateReader<SchedulerStoreState>) {
        wait_for_scheduler_state(
            scheduler,
            || "publish idle activity".to_owned(),
            |current| {
                current.public_state().active_task_count_for_test() == 0
                    && current
                        .public_state()
                        .tasks_for_test()
                        .iter()
                        .all(|(_, status)| *status != TaskStatus::Queued)
            },
        )
        .await;
    }

    async fn wait_for_scheduler_state(
        scheduler: &SchedulerStateReader<SchedulerStoreState>,
        description: impl FnOnce() -> String,
        matches: impl Fn(&SchedulerProjectionSnapshot<SchedulerStoreState>) -> bool,
    ) {
        let mut watch = scheduler.watch();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if matches(&watch.current()) {
                    return;
                }
                watch
                    .changed()
                    .await
                    .expect("scheduler state publisher remains open");
            }
        })
        .await
        .unwrap_or_else(|_| panic!("scheduler did not {}", description()));
    }
}
