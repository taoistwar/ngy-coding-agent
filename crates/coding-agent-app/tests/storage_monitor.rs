#![cfg(feature = "test-support")]

use std::collections::{HashSet, VecDeque};
use std::num::{NonZeroU32, NonZeroU64};
use std::str::FromStr;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use coding_agent_app::{
    MonitoredStorageScope, MonitoredStorageScopeBinding, RepositoryCoordinationKey,
    STORAGE_PROBE_TIMEOUT, STORAGE_SAMPLE_FRESHNESS, STORAGE_SAMPLE_INTERVAL,
    SchedulerStorageNotification, SchedulerStorageNotificationSink, StorageActivity,
    StorageCriticalNotification, StorageCriticalNotificationSink, StorageMonitorClock,
    StorageMonitorConfig, StorageMonitorError, StorageMonitorHandle, StoragePolicy,
    StorageProbeTarget, StorageState,
};
use coding_agent_domain::RepositoryId;
use coding_agent_runtime::{
    DirectoryIdentityMarker, NativeVolumeSampler, RootCapability, VolumeIdentity, VolumeSample,
    VolumeSampleError, VolumeSampler,
};
use tempfile::TempDir;

const MEBIBYTE: u64 = 1024 * 1024;
const NORMAL_BYTES: u64 = 2 * 1024 * MEBIBYTE;

#[derive(Default)]
struct FakeClock {
    now: Mutex<Duration>,
}

impl FakeClock {
    fn advance(&self, duration: Duration) {
        let mut now = self
            .now
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *now = now.checked_add(duration).unwrap();
    }
}

impl StorageMonitorClock for FakeClock {
    fn now(&self) -> Duration {
        *self
            .now
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

enum ProbeStep {
    Ready {
        expected_root: DirectoryIdentityMarker,
        result: Result<VolumeSample, VolumeSampleError>,
    },
    Blocking {
        expected_root: DirectoryIdentityMarker,
        gate: Arc<BlockingGate>,
        result: Result<VolumeSample, VolumeSampleError>,
    },
}

impl ProbeStep {
    fn expected_root(&self) -> DirectoryIdentityMarker {
        match self {
            Self::Ready { expected_root, .. } | Self::Blocking { expected_root, .. } => {
                *expected_root
            }
        }
    }

    fn sample(volume: &TestVolume, available_bytes: u64) -> Self {
        Self::Ready {
            expected_root: volume.root_identity,
            result: Ok(VolumeSample::for_test(volume.identity, available_bytes)),
        }
    }

    fn unavailable(volume: &TestVolume) -> Self {
        Self::Ready {
            expected_root: volume.root_identity,
            result: Err(VolumeSampleError::Unavailable),
        }
    }

    fn blocking_sample(volume: &TestVolume, available_bytes: u64) -> (Self, Arc<BlockingGate>) {
        let gate = Arc::new(BlockingGate::default());
        (
            Self::Blocking {
                expected_root: volume.root_identity,
                gate: gate.clone(),
                result: Ok(VolumeSample::for_test(volume.identity, available_bytes)),
            },
            gate,
        )
    }

    fn mismatched_identity(
        expected_root: DirectoryIdentityMarker,
        returned_volume: VolumeIdentity,
        available_bytes: u64,
    ) -> Self {
        Self::Ready {
            expected_root,
            result: Ok(VolumeSample::for_test(returned_volume, available_bytes)),
        }
    }
}

#[derive(Default)]
struct BlockingGate {
    released: Mutex<bool>,
    changed: Condvar,
}

impl BlockingGate {
    fn wait(&self) {
        let released = self
            .released
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        drop(
            self.changed
                .wait_timeout_while(released, Duration::from_secs(2), |released| !*released)
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
    }

    fn release(&self) {
        *self
            .released
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        self.changed.notify_all();
    }
}

struct ReleaseOnDrop(Arc<BlockingGate>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct ScriptedSampler {
    steps: Mutex<VecDeque<ProbeStep>>,
    calls: Mutex<Vec<DirectoryIdentityMarker>>,
}

impl ScriptedSampler {
    fn new(steps: impl IntoIterator<Item = ProbeStep>) -> Self {
        Self {
            steps: Mutex::new(steps.into_iter().collect()),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn call_count(&self) -> usize {
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    fn called_roots(&self) -> Vec<DirectoryIdentityMarker> {
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl VolumeSampler for ScriptedSampler {
    fn sample(&self, root: &RootCapability) -> Result<VolumeSample, VolumeSampleError> {
        let root_identity = root
            .identity_marker()
            .map_err(|_| VolumeSampleError::Unavailable)?;
        self.calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(root_identity);
        let mut steps = self
            .steps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(index) = steps
            .iter()
            .position(|step| step.expected_root() == root_identity)
        else {
            return Err(VolumeSampleError::Unavailable);
        };
        let step = steps.remove(index).unwrap();
        drop(steps);
        match step {
            ProbeStep::Ready {
                expected_root,
                result,
            } => {
                assert_eq!(root_identity, expected_root);
                result
            }
            ProbeStep::Blocking {
                expected_root,
                gate,
                result,
            } => {
                assert_eq!(root_identity, expected_root);
                gate.wait();
                result
            }
        }
    }
}

#[derive(Default)]
struct NotificationOrder {
    events: Mutex<Vec<&'static str>>,
}

impl NotificationOrder {
    fn push(&self, event: &'static str) {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(event);
    }

    fn events(&self) -> Vec<&'static str> {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

struct SchedulerRecorder {
    notifications: Mutex<Vec<SchedulerStorageNotification>>,
    order: Arc<NotificationOrder>,
}

impl SchedulerRecorder {
    fn new(order: Arc<NotificationOrder>) -> Self {
        Self {
            notifications: Mutex::new(Vec::new()),
            order,
        }
    }

    fn notifications(&self) -> Vec<SchedulerStorageNotification> {
        self.notifications
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl SchedulerStorageNotificationSink for SchedulerRecorder {
    fn notify_storage_classification(&self, notification: SchedulerStorageNotification) {
        self.order.push("scheduler");
        self.notifications
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(notification);
    }
}

struct CriticalRecorder {
    notifications: Mutex<Vec<StorageCriticalNotification>>,
    order: Arc<NotificationOrder>,
}

impl CriticalRecorder {
    fn new(order: Arc<NotificationOrder>) -> Self {
        Self {
            notifications: Mutex::new(Vec::new()),
            order,
        }
    }

    fn notifications(&self) -> Vec<StorageCriticalNotification> {
        self.notifications
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl StorageCriticalNotificationSink for CriticalRecorder {
    fn notify_storage_critical(&self, notification: StorageCriticalNotification) {
        self.order.push("critical");
        self.notifications
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(notification);
    }
}

fn policy() -> StoragePolicy {
    StoragePolicy::try_new(
        NonZeroU32::new(4).unwrap(),
        NonZeroU64::new(512 * MEBIBYTE).unwrap(),
        NonZeroU64::new(128 * MEBIBYTE).unwrap(),
    )
    .unwrap()
}

fn volume(token: u64) -> VolumeIdentity {
    VolumeIdentity::for_test(token)
}

fn repository(suffix: u32) -> RepositoryId {
    RepositoryId::from_str(&format!("30000000-0000-4000-8000-{suffix:012x}")).unwrap()
}

struct TestVolume {
    _directory: TempDir,
    identity: VolumeIdentity,
    root_identity: DirectoryIdentityMarker,
    target: StorageProbeTarget,
}

fn test_volume(token: u64) -> TestVolume {
    let directory = tempfile::tempdir().unwrap();
    let root = Arc::new(RootCapability::open(directory.path()).unwrap());
    let root_identity = root.identity_marker().unwrap();
    let identity = volume(token);
    TestVolume {
        _directory: directory,
        identity,
        root_identity,
        target: StorageProbeTarget::new(identity, root),
    }
}

fn shared_data_runtime(target: &StorageProbeTarget) -> Vec<MonitoredStorageScopeBinding> {
    vec![
        MonitoredStorageScopeBinding::data(target.clone()),
        MonitoredStorageScopeBinding::runtime(target.clone()),
    ]
}

fn spawn_monitor(
    sampler: Arc<dyn VolumeSampler>,
    clock: Arc<FakeClock>,
    bindings: Vec<MonitoredStorageScopeBinding>,
) -> (
    StorageMonitorHandle,
    Arc<SchedulerRecorder>,
    Arc<CriticalRecorder>,
    Arc<NotificationOrder>,
) {
    let order = Arc::new(NotificationOrder::default());
    let scheduler = Arc::new(SchedulerRecorder::new(order.clone()));
    let critical = Arc::new(CriticalRecorder::new(order.clone()));
    let monitor = StorageMonitorHandle::spawn(
        StorageMonitorConfig::new(
            policy(),
            sampler,
            clock,
            scheduler.clone(),
            critical.clone(),
        ),
        bindings,
    )
    .unwrap();
    (monitor, scheduler, critical, order)
}

#[tokio::test]
async fn dynamic_repository_scope_registration_is_apply_before_reply_and_never_rebinds() {
    let shared = test_volume(700);
    let repository_volume = test_volume(701);
    let replacement_volume = test_volume(701);
    let repository_id = repository(700);
    let repository_key =
        RepositoryCoordinationKey::from_authenticated_marker(repository_volume.root_identity);
    let replacement_key =
        RepositoryCoordinationKey::from_authenticated_marker(replacement_volume.root_identity);
    let (monitor, _, _, _) = spawn_monitor(
        Arc::new(ScriptedSampler::new([])),
        Arc::new(FakeClock::default()),
        shared_data_runtime(&shared.target),
    );

    monitor
        .register_repository_scope(
            repository_id,
            repository_key,
            repository_volume.target.clone(),
        )
        .await
        .expect("first dynamic registration applies");
    let applied = monitor.current_snapshot();
    assert_eq!(applied.repositories().len(), 1);
    assert_eq!(applied.repositories()[0].repository_id(), repository_id);
    assert_eq!(applied.repositories()[0].state(), None);

    monitor
        .register_repository_scope(
            repository_id,
            repository_key,
            repository_volume.target.clone(),
        )
        .await
        .expect("an exact retry is idempotent");
    assert_eq!(
        monitor
            .register_repository_scope(
                repository_id,
                replacement_key,
                replacement_volume.target.clone(),
            )
            .await,
        Err(StorageMonitorError::DuplicateRepositoryScope),
        "a duplicate ID can never overwrite its authenticated target"
    );
    assert_eq!(
        monitor.current_snapshot().repository_state(repository_id),
        None,
        "the rejected replacement leaves the original fail-closed scope installed"
    );
    monitor
        .register_repository_scope(
            repository_id,
            repository_key,
            repository_volume.target.clone(),
        )
        .await
        .expect("the original exact fingerprint remains installed");
}

async fn advance_both(clock: &FakeClock, duration: Duration) {
    clock.advance(duration);
    tokio::time::advance(duration).await;
    tokio::task::yield_now().await;
}

async fn wait_for_call_count(sampler: &ScriptedSampler, expected: usize) {
    for _ in 0..1_000 {
        if sampler.call_count() == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(sampler.call_count(), expected);
}

async fn yield_repeatedly() {
    for _ in 0..1_000 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn admission_uses_samples_at_most_five_seconds_old_and_failures_block_only_new_work() {
    let disk = test_volume(7101);
    let sampler = Arc::new(ScriptedSampler::new([
        ProbeStep::sample(&disk, NORMAL_BYTES),
        ProbeStep::unavailable(&disk),
    ]));
    let clock = Arc::new(FakeClock::default());
    let (monitor, scheduler, critical, _) = spawn_monitor(
        sampler.clone(),
        clock.clone(),
        shared_data_runtime(&disk.target),
    );

    let uninitialized = monitor.current_snapshot();
    assert_eq!(uninitialized.state(), None);
    assert!(!uninitialized.is_complete());
    assert!(uninitialized.blocks_admission());
    assert!(
        scheduler.notifications().is_empty(),
        "no sample must not be published as a fabricated unavailable state"
    );

    let fresh = monitor.refresh_for_admission(1).await.unwrap();
    assert_eq!(sampler.call_count(), 1);
    assert_eq!(fresh.state(), Some(StorageState::Normal));
    assert!(fresh.is_complete());
    assert!(!fresh.blocks_admission());

    advance_both(&clock, STORAGE_SAMPLE_FRESHNESS).await;
    let boundary = monitor.refresh_for_admission(1).await.unwrap();
    assert_eq!(sampler.call_count(), 1, "age == 5s remains fresh");
    assert_eq!(boundary.state(), Some(StorageState::Normal));

    monitor
        .set_activity(StorageActivity::new(0, 1))
        .await
        .unwrap();
    advance_both(&clock, Duration::from_nanos(1)).await;
    let unavailable = monitor.refresh_for_admission(1).await.unwrap();
    assert_eq!(sampler.call_count(), 2, "age > 5s refreshes first");
    assert_eq!(unavailable.state(), Some(StorageState::Unavailable));
    assert!(unavailable.blocks_admission());
    assert!(
        critical.notifications().is_empty(),
        "unavailable must not request a stop for the already-running task"
    );
    assert_eq!(
        scheduler
            .notifications()
            .iter()
            .map(SchedulerStorageNotification::state)
            .collect::<Vec<_>>(),
        [StorageState::Normal, StorageState::Unavailable]
    );
}

#[tokio::test(start_paused = true)]
async fn periodic_sampling_runs_for_queued_or_active_work_and_stops_while_idle() {
    let disk = test_volume(7102);
    let sampler = Arc::new(ScriptedSampler::new([
        ProbeStep::sample(&disk, NORMAL_BYTES),
        ProbeStep::sample(&disk, NORMAL_BYTES),
    ]));
    let clock = Arc::new(FakeClock::default());
    let (monitor, _, _, _) = spawn_monitor(
        sampler.clone(),
        clock.clone(),
        shared_data_runtime(&disk.target),
    );

    advance_both(&clock, STORAGE_SAMPLE_INTERVAL * 3).await;
    assert_eq!(sampler.call_count(), 0, "idle monitor must not poll");

    monitor
        .set_activity(StorageActivity::new(1, 0))
        .await
        .unwrap();
    advance_both(&clock, STORAGE_SAMPLE_INTERVAL - Duration::from_nanos(1)).await;
    assert_eq!(sampler.call_count(), 0);
    advance_both(&clock, Duration::from_nanos(1)).await;
    wait_for_call_count(&sampler, 1).await;

    monitor
        .set_activity(StorageActivity::new(0, 1))
        .await
        .unwrap();
    advance_both(&clock, STORAGE_SAMPLE_INTERVAL).await;
    wait_for_call_count(&sampler, 2).await;

    monitor
        .set_activity(StorageActivity::new(0, 0))
        .await
        .unwrap();
    advance_both(&clock, STORAGE_SAMPLE_INTERVAL * 3).await;
    assert_eq!(sampler.call_count(), 2, "returning idle cancels polling");
}

#[tokio::test(start_paused = true)]
async fn shared_volume_is_probed_once_but_all_logical_scopes_and_aliases_are_classified() {
    let disk = test_volume(7103);
    let first_repository = repository(1);
    let alias_repository = repository(2);
    let sampler = Arc::new(ScriptedSampler::new([ProbeStep::sample(
        &disk,
        32 * MEBIBYTE,
    )]));
    let clock = Arc::new(FakeClock::default());
    let bindings = vec![
        MonitoredStorageScopeBinding::data(disk.target.clone()),
        MonitoredStorageScopeBinding::runtime(disk.target.clone()),
        MonitoredStorageScopeBinding::repository_git(first_repository, disk.target.clone()),
        MonitoredStorageScopeBinding::repository_git(alias_repository, disk.target.clone()),
    ];
    let (monitor, scheduler, critical, order) = spawn_monitor(sampler.clone(), clock, bindings);

    let snapshot = monitor.refresh_for_admission(0).await.unwrap();

    assert_eq!(sampler.called_roots(), [disk.root_identity]);
    assert_eq!(snapshot.data_state(), Some(StorageState::Critical));
    assert_eq!(snapshot.runtime_state(), Some(StorageState::Critical));
    assert_eq!(
        snapshot.repository_state(first_repository),
        Some(StorageState::Critical)
    );
    assert_eq!(
        snapshot.repository_state(alias_repository),
        Some(StorageState::Critical)
    );
    assert_eq!(snapshot.state(), Some(StorageState::Critical));

    let scheduler_notifications = scheduler.notifications();
    assert_eq!(scheduler_notifications.len(), 1);
    assert_eq!(scheduler_notifications[0].repositories().len(), 2);

    let critical_notifications = critical.notifications();
    assert_eq!(critical_notifications.len(), 1);
    assert_eq!(
        critical_notifications[0].scopes(),
        [
            MonitoredStorageScope::Data,
            MonitoredStorageScope::Runtime,
            MonitoredStorageScope::RepositoryGit(first_repository),
            MonitoredStorageScope::RepositoryGit(alias_repository),
        ]
    );
    assert_eq!(
        order.events(),
        ["critical", "scheduler"],
        "critical uses the independent high-priority port first"
    );
}

#[tokio::test(start_paused = true)]
async fn returned_volume_identity_mismatch_fails_closed_as_a_real_unavailable_observation() {
    let disk = test_volume(7106);
    let sampler = Arc::new(ScriptedSampler::new([ProbeStep::mismatched_identity(
        disk.root_identity,
        volume(999_999),
        NORMAL_BYTES,
    )]));
    let clock = Arc::new(FakeClock::default());
    let (monitor, scheduler, critical, _) =
        spawn_monitor(sampler.clone(), clock, shared_data_runtime(&disk.target));

    assert_eq!(monitor.current_snapshot().state(), None);
    let snapshot = monitor.refresh_for_admission(0).await.unwrap();

    assert_eq!(sampler.called_roots(), [disk.root_identity]);
    assert_eq!(snapshot.state(), Some(StorageState::Unavailable));
    assert_eq!(snapshot.data_state(), Some(StorageState::Unavailable));
    assert_eq!(snapshot.runtime_state(), Some(StorageState::Unavailable));
    assert!(snapshot.blocks_admission());
    assert!(critical.notifications().is_empty());
    assert_eq!(
        scheduler
            .notifications()
            .iter()
            .map(SchedulerStorageNotification::state)
            .collect::<Vec<_>>(),
        [StorageState::Unavailable]
    );
}

#[tokio::test(start_paused = true)]
async fn retained_root_capability_remains_the_probe_authority_after_namespace_rename() {
    let parent = tempfile::tempdir().unwrap();
    let original_path = parent.path().join("original-root");
    let renamed_path = parent.path().join("renamed-root");
    std::fs::create_dir(&original_path).unwrap();
    let root = Arc::new(RootCapability::open(&original_path).unwrap());
    let native_sampler = NativeVolumeSampler::new();
    let initial_sample = native_sampler.sample(&root).unwrap();
    let target = StorageProbeTarget::new(initial_sample.identity(), root);
    std::fs::rename(&original_path, &renamed_path).unwrap();
    assert!(!original_path.exists());

    let clock = Arc::new(FakeClock::default());
    let (monitor, scheduler, _, _) = spawn_monitor(
        Arc::new(native_sampler),
        clock,
        shared_data_runtime(&target),
    );
    let snapshot = monitor.refresh_for_admission(0).await.unwrap();

    assert!(snapshot.is_complete());
    assert_ne!(snapshot.state(), Some(StorageState::Unavailable));
    assert_eq!(scheduler.notifications().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn admission_rechecks_every_required_volume_after_waiting_for_a_slow_probe() {
    let data = test_volume(7107);
    let runtime = test_volume(7108);
    let (initial_runtime_probe, initial_runtime_gate) =
        ProbeStep::blocking_sample(&runtime, NORMAL_BYTES);
    let (second_data_probe, second_data_gate) = ProbeStep::blocking_sample(&data, NORMAL_BYTES);
    let _initial_release_on_drop = ReleaseOnDrop(initial_runtime_gate.clone());
    let _second_release_on_drop = ReleaseOnDrop(second_data_gate.clone());
    let sampler = Arc::new(ScriptedSampler::new([
        ProbeStep::sample(&data, NORMAL_BYTES),
        initial_runtime_probe,
        second_data_probe,
        ProbeStep::sample(&runtime, NORMAL_BYTES),
    ]));
    let clock = Arc::new(FakeClock::default());
    let bindings = vec![
        MonitoredStorageScopeBinding::data(data.target.clone()),
        MonitoredStorageScopeBinding::runtime(runtime.target.clone()),
    ];
    let (monitor, _, _, _) = spawn_monitor(sampler.clone(), clock.clone(), bindings);

    let initial_refresh = tokio::spawn({
        let monitor = monitor.clone();
        async move { monitor.refresh_for_admission(0).await.unwrap() }
    });
    wait_for_call_count(&sampler, 2).await;
    yield_repeatedly().await;
    assert_eq!(
        monitor.current_snapshot().data_state(),
        Some(StorageState::Normal),
        "the fast data sample must be recorded before virtual time advances"
    );
    advance_both(&clock, STORAGE_PROBE_TIMEOUT).await;
    let initial = initial_refresh.await.unwrap();
    assert_eq!(initial.data_state(), Some(StorageState::Normal));
    assert_eq!(initial.runtime_state(), Some(StorageState::Unavailable));
    initial_runtime_gate.release();
    yield_repeatedly().await;

    advance_both(&clock, Duration::from_millis(4_900)).await;
    let refresh = tokio::spawn({
        let monitor = monitor.clone();
        async move { monitor.refresh_for_admission(0).await.unwrap() }
    });
    wait_for_call_count(&sampler, 3).await;
    advance_both(&clock, Duration::from_millis(200)).await;
    second_data_gate.release();
    yield_repeatedly().await;

    let _ = refresh.await.unwrap();
    assert_eq!(
        sampler.call_count(),
        4,
        "runtime was fresh at request start but stale after waiting, so it must be reprobed"
    );
}

#[tokio::test(start_paused = true)]
async fn repository_admission_does_not_probe_or_wait_for_an_unrelated_repository_volume() {
    let data = test_volume(7109);
    let runtime = test_volume(7110);
    let requested_repository = test_volume(7111);
    let unrelated_repository = test_volume(7112);
    let requested_repository_id = repository(11);
    let unrelated_repository_id = repository(12);
    let (unrelated_probe, unrelated_gate) =
        ProbeStep::blocking_sample(&unrelated_repository, NORMAL_BYTES);
    let _release_on_drop = ReleaseOnDrop(unrelated_gate);
    let sampler = Arc::new(ScriptedSampler::new([
        ProbeStep::sample(&data, NORMAL_BYTES),
        ProbeStep::sample(&runtime, NORMAL_BYTES),
        ProbeStep::sample(&requested_repository, NORMAL_BYTES),
        unrelated_probe,
    ]));
    let clock = Arc::new(FakeClock::default());
    let bindings = vec![
        MonitoredStorageScopeBinding::data(data.target.clone()),
        MonitoredStorageScopeBinding::runtime(runtime.target.clone()),
        MonitoredStorageScopeBinding::repository_git(
            requested_repository_id,
            requested_repository.target.clone(),
        ),
        MonitoredStorageScopeBinding::repository_git(
            unrelated_repository_id,
            unrelated_repository.target.clone(),
        ),
    ];
    let (monitor, scheduler, _, _) = spawn_monitor(sampler.clone(), clock, bindings);

    let refresh = tokio::spawn({
        let monitor = monitor.clone();
        async move {
            monitor
                .refresh_for_repository_admission(0, requested_repository_id)
                .await
                .unwrap()
        }
    });
    yield_repeatedly().await;
    assert_eq!(sampler.call_count(), 3);
    assert_eq!(
        sampler.called_roots().into_iter().collect::<HashSet<_>>(),
        HashSet::from([
            data.root_identity,
            runtime.root_identity,
            requested_repository.root_identity,
        ])
    );

    let snapshot = refresh.await.unwrap();
    assert_eq!(
        snapshot.repository_state(requested_repository_id),
        Some(StorageState::Normal)
    );
    assert_eq!(snapshot.repository_state(unrelated_repository_id), None);
    assert!(!snapshot.blocks_admission_for(requested_repository_id));
    assert!(snapshot.blocks_admission_for(unrelated_repository_id));
    assert!(!snapshot.is_complete());
    assert!(scheduler.notifications().is_empty());
}

#[tokio::test(start_paused = true)]
async fn concurrent_refreshes_coalesce_and_timeout_does_not_build_more_volume_futures() {
    let disk = test_volume(7104);
    let (blocking_step, gate) = ProbeStep::blocking_sample(&disk, NORMAL_BYTES);
    let _release_on_drop = ReleaseOnDrop(gate.clone());
    let sampler = Arc::new(ScriptedSampler::new([blocking_step]));
    let clock = Arc::new(FakeClock::default());
    let (monitor, _, critical, _) = spawn_monitor(
        sampler.clone(),
        clock.clone(),
        shared_data_runtime(&disk.target),
    );

    let first = tokio::spawn({
        let monitor = monitor.clone();
        async move { monitor.refresh_for_admission(0).await.unwrap() }
    });
    let second = tokio::spawn({
        let monitor = monitor.clone();
        async move { monitor.refresh_for_admission(0).await.unwrap() }
    });
    wait_for_call_count(&sampler, 1).await;
    assert_eq!(
        sampler.call_count(),
        1,
        "logical scopes and concurrent callers share one physical probe"
    );

    advance_both(&clock, STORAGE_PROBE_TIMEOUT).await;
    assert_eq!(
        first.await.unwrap().state(),
        Some(StorageState::Unavailable)
    );
    assert_eq!(
        second.await.unwrap().state(),
        Some(StorageState::Unavailable)
    );
    assert!(critical.notifications().is_empty());

    let mut later_callers = Vec::new();
    for _ in 0..16 {
        later_callers.push(tokio::spawn({
            let monitor = monitor.clone();
            async move { monitor.refresh_for_admission(0).await.unwrap() }
        }));
    }
    for caller in later_callers {
        assert_eq!(
            caller.await.unwrap().state(),
            Some(StorageState::Unavailable)
        );
    }
    assert_eq!(
        sampler.call_count(),
        1,
        "a timed-out blocking call remains the sole in-flight probe"
    );

    gate.release();
}

#[tokio::test(start_paused = true)]
async fn scheduler_is_notified_only_for_classification_changes_and_critical_is_separate() {
    let disk = test_volume(7105);
    let sampler = Arc::new(ScriptedSampler::new([
        ProbeStep::sample(&disk, NORMAL_BYTES),
        ProbeStep::sample(&disk, NORMAL_BYTES + 123_456_789),
        ProbeStep::sample(&disk, 600 * MEBIBYTE),
        ProbeStep::sample(&disk, 32 * MEBIBYTE),
    ]));
    let clock = Arc::new(FakeClock::default());
    let (monitor, scheduler, critical, order) =
        spawn_monitor(sampler, clock.clone(), shared_data_runtime(&disk.target));
    let stale_by = STORAGE_SAMPLE_FRESHNESS + Duration::from_nanos(1);

    assert_eq!(
        monitor.refresh_for_admission(0).await.unwrap().state(),
        Some(StorageState::Normal)
    );
    assert_eq!(scheduler.notifications().len(), 1);

    advance_both(&clock, stale_by).await;
    assert_eq!(
        monitor.refresh_for_admission(0).await.unwrap().state(),
        Some(StorageState::Normal)
    );
    assert_eq!(
        scheduler.notifications().len(),
        1,
        "raw byte changes must not advance Scheduler generation"
    );

    advance_both(&clock, stale_by).await;
    assert_eq!(
        monitor.refresh_for_admission(0).await.unwrap().state(),
        Some(StorageState::Pressure)
    );
    assert_eq!(scheduler.notifications().len(), 2);
    assert!(critical.notifications().is_empty());

    advance_both(&clock, stale_by).await;
    assert_eq!(
        monitor.refresh_for_admission(0).await.unwrap().state(),
        Some(StorageState::Critical)
    );
    assert_eq!(scheduler.notifications().len(), 3);
    assert_eq!(critical.notifications().len(), 1);
    assert_eq!(
        order.events(),
        ["scheduler", "scheduler", "critical", "scheduler"]
    );
}

#[tokio::test(start_paused = true)]
async fn snapshots_notifications_and_normal_debug_output_are_redacted_and_monitor_is_narrow() {
    let disk = test_volume(7_106_987_654_321);
    let raw_bytes = 41_234_567;
    let repository_id = repository(3);
    let sampler = Arc::new(ScriptedSampler::new([ProbeStep::sample(&disk, raw_bytes)]));
    let clock = Arc::new(FakeClock::default());
    let bindings = vec![
        MonitoredStorageScopeBinding::data(disk.target.clone()),
        MonitoredStorageScopeBinding::runtime(disk.target.clone()),
        MonitoredStorageScopeBinding::repository_git(repository_id, disk.target.clone()),
    ];
    let binding_debug = format!("{:?}", bindings[0]);
    let (monitor, scheduler, critical, _) = spawn_monitor(sampler.clone(), clock, bindings);

    let snapshot = monitor.refresh_for_admission(0).await.unwrap();
    assert_eq!(sampler.call_count(), 1);
    assert_eq!(snapshot.state(), Some(StorageState::Critical));
    let scheduler_notification = scheduler.notifications().pop().unwrap();
    let critical_notification = critical.notifications().pop().unwrap();
    let rendered = format!(
        "{snapshot:?} {scheduler_notification:?} {critical_notification:?} {monitor:?} \
         {binding_debug}"
    );
    for secret in [
        raw_bytes.to_string(),
        "7106987654321".to_owned(),
        "VolumeIdentity".to_owned(),
        r"E:\private\repository".to_owned(),
    ] {
        assert!(
            !rendered.contains(&secret),
            "ordinary debug output leaked {secret}"
        );
    }

    let implementation = include_str!("../src/storage_monitor.rs");
    for forbidden_dependency in ["StoreWriter", "RepositoryControlLease", "PermitToken"] {
        assert!(
            !implementation.contains(forbidden_dependency),
            "monitor must not own {forbidden_dependency}"
        );
    }
}
