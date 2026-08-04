use std::sync::Arc;
use std::time::Duration;

use coding_agent_store::{SchedulerBootstrapSnapshot, Store, StoreError};
use tokio::time::{Instant, timeout_at};
use uuid::Uuid;

use crate::scheduler::{
    SchedulerProjectionSnapshot, SchedulerStateReader, SchedulerStateWatch, SchedulerStoreState,
};
use crate::{ServiceStateController, ServiceStateSnapshot};

pub(crate) const BOOTSTRAP_SNAPSHOT_UNAVAILABLE: &str = "BOOTSTRAP_SNAPSHOT_UNAVAILABLE";
const DEFAULT_BOOTSTRAP_JOIN_BUDGET: Duration = Duration::from_secs(2);

#[derive(Debug, thiserror::Error)]
pub(crate) enum BootstrapJoinError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("BOOTSTRAP_SNAPSHOT_UNAVAILABLE")]
    SnapshotUnavailable,
}

#[derive(Debug)]
pub(crate) struct JoinedBootstrapSnapshot {
    pub store: SchedulerBootstrapSnapshot,
    pub scheduler: Arc<SchedulerProjectionSnapshot<SchedulerStoreState>>,
    pub service_state: ServiceStateSnapshot,
    pub server_instance_id: Uuid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BootstrapJoinPoint {
    ServiceS1,
    StoreSnapshot,
    SchedulerSnapshot,
    ServiceS2,
    BeforeSchedulerWait,
    BeforeStoreRetry,
}

trait BootstrapJoinClock: Send + Sync {
    fn now(&self) -> Instant;
}

struct TokioBootstrapJoinClock;

impl BootstrapJoinClock for TokioBootstrapJoinClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

trait BootstrapJoinHook: Send + Sync {
    fn observed(&self, _attempt: usize, _point: BootstrapJoinPoint) {}
}

struct NoopBootstrapJoinHook;

impl BootstrapJoinHook for NoopBootstrapJoinHook {}

#[async_trait::async_trait]
trait BootstrapSnapshotSource: Send + Sync {
    async fn scheduler_snapshot(&self) -> Result<SchedulerBootstrapSnapshot, StoreError>;
}

#[async_trait::async_trait]
impl BootstrapSnapshotSource for Store {
    async fn scheduler_snapshot(&self) -> Result<SchedulerBootstrapSnapshot, StoreError> {
        self.scheduler_bootstrap_snapshot().await
    }
}

#[derive(Clone)]
pub(crate) struct BootstrapJoin {
    store: Arc<dyn BootstrapSnapshotSource>,
    service_state: ServiceStateController,
    scheduler: SchedulerStateReader<SchedulerStoreState>,
    server_instance_id: Uuid,
    clock: Arc<dyn BootstrapJoinClock>,
    hook: Arc<dyn BootstrapJoinHook>,
    budget: Duration,
}

impl BootstrapJoin {
    pub(crate) fn new(
        store: Store,
        service_state: ServiceStateController,
        scheduler: SchedulerStateReader<SchedulerStoreState>,
    ) -> Self {
        let server_instance_id = scheduler.current().public_state().server_instance_id();
        assert_eq!(
            server_instance_id.get_version(),
            Some(uuid::Version::Random),
            "scheduler instance identity must be the primary UUID v4"
        );
        Self {
            store: Arc::new(store),
            service_state,
            scheduler,
            server_instance_id,
            clock: Arc::new(TokioBootstrapJoinClock),
            hook: Arc::new(NoopBootstrapJoinHook),
            budget: DEFAULT_BOOTSTRAP_JOIN_BUDGET,
        }
    }

    #[cfg(feature = "test-support")]
    pub(crate) fn set_budget_for_test(&mut self, budget: Duration) {
        self.budget = budget;
    }

    #[cfg(test)]
    fn with_test_dependencies(
        store: Arc<dyn BootstrapSnapshotSource>,
        service_state: ServiceStateController,
        scheduler: SchedulerStateReader<SchedulerStoreState>,
        server_instance_id: Uuid,
        clock: Arc<dyn BootstrapJoinClock>,
        hook: Arc<dyn BootstrapJoinHook>,
        budget: Duration,
    ) -> Self {
        Self {
            store,
            service_state,
            scheduler,
            server_instance_id,
            clock,
            hook,
            budget,
        }
    }

    pub(crate) async fn snapshot(&self) -> Result<JoinedBootstrapSnapshot, BootstrapJoinError> {
        let deadline = self
            .clock
            .now()
            .checked_add(self.budget)
            .ok_or(BootstrapJoinError::SnapshotUnavailable)?;
        let mut scheduler = self.scheduler.watch();
        let mut attempt = 0usize;

        loop {
            self.ensure_budget(deadline)?;
            attempt = attempt
                .checked_add(1)
                .ok_or(BootstrapJoinError::SnapshotUnavailable)?;

            let service_s1 = self.service_state.current();
            self.hook.observed(attempt, BootstrapJoinPoint::ServiceS1);

            let store = match timeout_at(deadline, self.store.scheduler_snapshot()).await {
                Ok(Ok(store)) => store,
                Ok(Err(error)) => return Err(BootstrapJoinError::Store(error)),
                Err(_) => return Err(BootstrapJoinError::SnapshotUnavailable),
            };
            self.hook
                .observed(attempt, BootstrapJoinPoint::StoreSnapshot);

            let scheduler_snapshot = scheduler.current();
            self.hook
                .observed(attempt, BootstrapJoinPoint::SchedulerSnapshot);

            let service_s2 = self.service_state.current();
            self.hook.observed(attempt, BootstrapJoinPoint::ServiceS2);

            if self.is_exact(&service_s1, &store, &scheduler_snapshot, &service_s2) {
                self.ensure_budget(deadline)?;
                return Ok(JoinedBootstrapSnapshot {
                    store,
                    scheduler: scheduler_snapshot,
                    service_state: service_s2,
                    server_instance_id: self.server_instance_id,
                });
            }

            if scheduler_snapshot.public_state().server_instance_id() == self.server_instance_id
                && service_s1.generation == service_s2.generation
                && scheduler_snapshot.service_state_generation() == service_s2.generation
                && scheduler_snapshot.as_of_event_id() < store.membership_event_id
                && let Some(joined) = self
                    .wait_for_scheduler(attempt, deadline, service_s1, store, &mut scheduler)
                    .await?
            {
                return Ok(joined);
            }

            self.hook
                .observed(attempt, BootstrapJoinPoint::BeforeStoreRetry);
            self.ensure_budget(deadline)?;
            tokio::task::yield_now().await;
        }
    }

    async fn wait_for_scheduler(
        &self,
        attempt: usize,
        deadline: Instant,
        service_s1: ServiceStateSnapshot,
        store: SchedulerBootstrapSnapshot,
        scheduler: &mut SchedulerStateWatch<SchedulerStoreState>,
    ) -> Result<Option<JoinedBootstrapSnapshot>, BootstrapJoinError> {
        let mut service_changes = self.service_state.subscribe();
        loop {
            self.hook
                .observed(attempt, BootstrapJoinPoint::BeforeSchedulerWait);
            self.ensure_budget(deadline)?;
            if self.service_state.current().generation != service_s1.generation {
                return Ok(None);
            }
            tokio::select! {
                result = timeout_at(deadline, scheduler.changed()) => match result {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) | Err(_) => {
                        return Err(BootstrapJoinError::SnapshotUnavailable);
                    }
                },
                result = service_changes.changed() => {
                    if result.is_err() {
                        return Err(BootstrapJoinError::SnapshotUnavailable);
                    }
                    return Ok(None);
                }
            }

            let scheduler_snapshot = scheduler.current();
            self.hook
                .observed(attempt, BootstrapJoinPoint::SchedulerSnapshot);
            let service_s2 = self.service_state.current();
            self.hook.observed(attempt, BootstrapJoinPoint::ServiceS2);

            if self.is_exact(&service_s1, &store, &scheduler_snapshot, &service_s2) {
                self.ensure_budget(deadline)?;
                return Ok(Some(JoinedBootstrapSnapshot {
                    store,
                    scheduler: scheduler_snapshot,
                    service_state: service_s2,
                    server_instance_id: self.server_instance_id,
                }));
            }

            let still_behind = scheduler_snapshot.public_state().server_instance_id()
                == self.server_instance_id
                && service_s1.generation == service_s2.generation
                && scheduler_snapshot.service_state_generation() == service_s2.generation
                && scheduler_snapshot.as_of_event_id() < store.membership_event_id;
            if !still_behind {
                return Ok(None);
            }
        }
    }

    fn is_exact(
        &self,
        service_s1: &ServiceStateSnapshot,
        store: &SchedulerBootstrapSnapshot,
        scheduler: &SchedulerProjectionSnapshot<SchedulerStoreState>,
        service_s2: &ServiceStateSnapshot,
    ) -> bool {
        service_s1.generation == service_s2.generation
            && store.membership_event_id <= store.latest_event_id
            && scheduler.public_state().server_instance_id() == self.server_instance_id
            && scheduler.service_state_generation() == service_s2.generation
            && scheduler.as_of_event_id() == store.membership_event_id
            && scheduler.public_state().exactly_matches(store)
    }

    fn ensure_budget(&self, deadline: Instant) -> Result<(), BootstrapJoinError> {
        if self.budget.is_zero() || self.clock.now() >= deadline {
            Err(BootstrapJoinError::SnapshotUnavailable)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use coding_agent_domain::{
        CanonicalPath, ClientRequestId, DeliveryReadiness, EventCursor, EventId, Repository,
        RepositoryId, Task, TaskId, TaskStatus, UtcTimestamp,
    };
    use coding_agent_store::{SchedulerBootstrapSnapshot, StopIntentKind, StopIntentReceipt};
    use time::OffsetDateTime;

    use super::*;
    use crate::ServiceState;
    use crate::scheduler::{SchedulerProjectionCandidate, SchedulerStatePublisher};

    struct ScriptedStore {
        snapshots: Mutex<VecDeque<SchedulerBootstrapSnapshot>>,
        reads: AtomicUsize,
    }

    impl ScriptedStore {
        fn new(snapshots: impl IntoIterator<Item = SchedulerBootstrapSnapshot>) -> Self {
            Self {
                snapshots: Mutex::new(snapshots.into_iter().collect()),
                reads: AtomicUsize::new(0),
            }
        }

        fn reads(&self) -> usize {
            self.reads.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl BootstrapSnapshotSource for ScriptedStore {
        async fn scheduler_snapshot(&self) -> Result<SchedulerBootstrapSnapshot, StoreError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let mut snapshots = self
                .snapshots
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let snapshot = if snapshots.len() > 1 {
                snapshots.pop_front()
            } else {
                snapshots.front().cloned()
            };
            Ok(snapshot.expect("scripted Store snapshot"))
        }
    }

    struct NeverCompletesStore {
        reads: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl BootstrapSnapshotSource for NeverCompletesStore {
        async fn scheduler_snapshot(&self) -> Result<SchedulerBootstrapSnapshot, StoreError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            std::future::pending().await
        }
    }

    struct ManualClock {
        base: Instant,
        elapsed_millis: AtomicU64,
    }

    impl ManualClock {
        fn new() -> Self {
            Self {
                base: Instant::now(),
                elapsed_millis: AtomicU64::new(0),
            }
        }

        fn advance(&self, duration: Duration) {
            let millis =
                u64::try_from(duration.as_millis()).expect("test duration fits in u64 millis");
            self.elapsed_millis.fetch_add(millis, Ordering::SeqCst);
        }
    }

    impl BootstrapJoinClock for ManualClock {
        fn now(&self) -> Instant {
            self.base + Duration::from_millis(self.elapsed_millis.load(Ordering::SeqCst))
        }
    }

    struct RecordingHook {
        observations: Mutex<Vec<(usize, BootstrapJoinPoint)>>,
        callback: Box<dyn Fn(usize, BootstrapJoinPoint) + Send + Sync>,
    }

    impl RecordingHook {
        fn new(callback: impl Fn(usize, BootstrapJoinPoint) + Send + Sync + 'static) -> Self {
            Self {
                observations: Mutex::new(Vec::new()),
                callback: Box::new(callback),
            }
        }

        fn observations(&self) -> Vec<(usize, BootstrapJoinPoint)> {
            self.observations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    impl BootstrapJoinHook for RecordingHook {
        fn observed(&self, attempt: usize, point: BootstrapJoinPoint) {
            self.observations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((attempt, point));
            (self.callback)(attempt, point);
        }
    }

    fn cursor(value: i64) -> EventCursor {
        EventCursor::new(value).expect("valid test cursor")
    }

    fn snapshot(membership: i64) -> SchedulerBootstrapSnapshot {
        SchedulerBootstrapSnapshot {
            repositories: Vec::new(),
            tasks: Vec::new(),
            running_stop_intents: Vec::new(),
            latest_event_id: cursor(membership),
            membership_event_id: cursor(membership),
        }
    }

    fn snapshot_with_repository(membership: i64) -> SchedulerBootstrapSnapshot {
        SchedulerBootstrapSnapshot {
            repositories: vec![repository("repository")],
            ..snapshot(membership)
        }
    }

    fn timestamp(seconds: i64) -> UtcTimestamp {
        UtcTimestamp::new(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(seconds))
            .expect("valid test timestamp")
    }

    fn repository(display_name: &str) -> Repository {
        let path = std::env::current_dir()
            .expect("read current directory")
            .canonicalize()
            .expect("canonicalize current directory");
        let path = CanonicalPath::try_from_canonical(path).expect("canonical test path");
        Repository {
            id: RepositoryId::new(),
            selected_path: path.clone(),
            display_name: display_name.to_owned(),
            git_root: path.clone(),
            cargo_workspace_root: path,
            created_at: timestamp(0),
            last_opened_at: timestamp(0),
        }
    }

    fn task(repository_id: RepositoryId, status: TaskStatus, created_at: UtcTimestamp) -> Task {
        let started_at = (status != TaskStatus::Queued).then_some(created_at);
        let finished_at = matches!(
            status,
            TaskStatus::Completed
                | TaskStatus::Failed
                | TaskStatus::Cancelled
                | TaskStatus::Interrupted
        )
        .then_some(created_at);
        Task {
            id: TaskId::new(),
            client_request_id: ClientRequestId::new(),
            repository_id,
            prompt: "scheduler membership".to_owned(),
            status,
            delivery_readiness: DeliveryReadiness::Unreviewed,
            attempt: 1,
            retry_of: None,
            created_at,
            started_at,
            finished_at,
            last_event_id: EventId::new(1).expect("valid test event ID"),
            failure: None,
        }
    }

    fn membership_snapshot(status: TaskStatus) -> SchedulerBootstrapSnapshot {
        let repository = repository("membership");
        SchedulerBootstrapSnapshot {
            repositories: vec![repository.clone()],
            tasks: vec![task(repository.id, status, timestamp(0))],
            running_stop_intents: Vec::new(),
            latest_event_id: cursor(1),
            membership_event_id: cursor(1),
        }
    }

    fn assert_membership_change_publishes(
        instance_id: Uuid,
        before: &SchedulerBootstrapSnapshot,
        after: &SchedulerBootstrapSnapshot,
    ) {
        let before_state = SchedulerStoreState::from_store_snapshot(instance_id, before);
        assert!(
            !before_state.exactly_matches(after),
            "the old Scheduler witness must reject changed durable membership"
        );
        let mut publisher = SchedulerStatePublisher::new(SchedulerProjectionCandidate::new(
            before_state,
            before.membership_event_id,
            0,
        ));
        publisher
            .stage(SchedulerProjectionCandidate::new(
                SchedulerStoreState::from_store_snapshot(instance_id, after),
                after.membership_event_id,
                0,
            ))
            .expect("stage changed membership");
        let published = publisher.flush().expect("publish changed membership");
        assert!(published.changed());
        assert_eq!(published.snapshot().generation(), 1);
    }

    fn publisher(
        instance_id: Uuid,
        store: &SchedulerBootstrapSnapshot,
        service_generation: u64,
    ) -> SchedulerStatePublisher<SchedulerStoreState> {
        SchedulerStatePublisher::new(SchedulerProjectionCandidate::new(
            SchedulerStoreState::from_store_snapshot(instance_id, store),
            store.membership_event_id,
            service_generation,
        ))
    }

    fn join(
        store: Arc<ScriptedStore>,
        service_state: ServiceStateController,
        reader: SchedulerStateReader<SchedulerStoreState>,
        instance_id: Uuid,
        clock: Arc<ManualClock>,
        hook: Arc<RecordingHook>,
        budget: Duration,
    ) -> BootstrapJoin {
        BootstrapJoin::with_test_dependencies(
            store,
            service_state,
            reader,
            instance_id,
            clock,
            hook,
            budget,
        )
    }

    #[tokio::test]
    async fn bootstrap_join_observes_s1_store_q_s2_and_returns_only_an_exact_snapshot() {
        let instance_id = Uuid::new_v4();
        let durable = snapshot(7);
        let store = Arc::new(ScriptedStore::new([durable.clone()]));
        let service_state = ServiceStateController::new(ServiceState::Ready);
        let publisher = publisher(instance_id, &durable, 0);
        let hook = Arc::new(RecordingHook::new(|_, _| {}));
        let joined = join(
            Arc::clone(&store),
            service_state,
            publisher.reader(),
            instance_id,
            Arc::new(ManualClock::new()),
            Arc::clone(&hook),
            Duration::from_secs(1),
        )
        .snapshot()
        .await
        .expect("join exact Bootstrap snapshot");

        assert_eq!(joined.server_instance_id, instance_id);
        assert_eq!(joined.store, durable);
        assert_eq!(joined.scheduler.as_of_event_id(), cursor(7));
        assert_eq!(joined.service_state.generation, 0);
        assert_eq!(store.reads(), 1);
        assert_eq!(
            hook.observations(),
            [
                (1, BootstrapJoinPoint::ServiceS1),
                (1, BootstrapJoinPoint::StoreSnapshot),
                (1, BootstrapJoinPoint::SchedulerSnapshot),
                (1, BootstrapJoinPoint::ServiceS2),
            ]
        );
    }

    #[tokio::test]
    async fn bootstrap_join_waits_on_scheduler_watch_when_q_is_behind_without_rereading_store() {
        let instance_id = Uuid::new_v4();
        let durable = snapshot(3);
        let store = Arc::new(ScriptedStore::new([durable.clone()]));
        let service_state = ServiceStateController::new(ServiceState::Ready);
        let publisher = Arc::new(Mutex::new(publisher(instance_id, &snapshot(0), 0)));
        let reader = publisher
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .reader();
        let publish_on_wait = Arc::clone(&publisher);
        let durable_for_hook = durable.clone();
        let hook = Arc::new(RecordingHook::new(move |attempt, point| {
            if attempt == 1 && point == BootstrapJoinPoint::BeforeSchedulerWait {
                let mut publisher = publish_on_wait
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                publisher
                    .stage(SchedulerProjectionCandidate::new(
                        SchedulerStoreState::from_store_snapshot(instance_id, &durable_for_hook),
                        durable_for_hook.membership_event_id,
                        0,
                    ))
                    .expect("stage caught-up Scheduler snapshot");
                publisher
                    .flush()
                    .expect("publish caught-up Scheduler snapshot");
            }
        }));

        let joined = join(
            Arc::clone(&store),
            service_state,
            reader,
            instance_id,
            Arc::new(ManualClock::new()),
            hook,
            Duration::from_secs(1),
        )
        .snapshot()
        .await
        .expect("wait for exact Scheduler projection");

        assert_eq!(joined.store, durable);
        assert_eq!(store.reads(), 1);
    }

    #[tokio::test]
    async fn bootstrap_join_rereads_store_when_q_is_ahead() {
        let instance_id = Uuid::new_v4();
        let first = snapshot(1);
        let second = snapshot(2);
        let store = Arc::new(ScriptedStore::new([first, second.clone()]));
        let service_state = ServiceStateController::new(ServiceState::Ready);
        let publisher = publisher(instance_id, &second, 0);

        let joined = join(
            Arc::clone(&store),
            service_state,
            publisher.reader(),
            instance_id,
            Arc::new(ManualClock::new()),
            Arc::new(RecordingHook::new(|_, _| {})),
            Duration::from_secs(1),
        )
        .snapshot()
        .await
        .expect("reread Store up to Scheduler watermark");

        assert_eq!(joined.store, second);
        assert_eq!(store.reads(), 2);
    }

    #[tokio::test]
    async fn bootstrap_join_rereads_store_after_service_generation_changes() {
        let instance_id = Uuid::new_v4();
        let durable = snapshot(1);
        let store = Arc::new(ScriptedStore::new([durable.clone(), durable.clone()]));
        let service_state = ServiceStateController::new(ServiceState::Ready);
        let publisher = publisher(instance_id, &durable, 1);
        let service_for_hook = service_state.clone();
        let hook = Arc::new(RecordingHook::new(move |attempt, point| {
            if attempt == 1 && point == BootstrapJoinPoint::StoreSnapshot {
                service_for_hook
                    .set(ServiceState::StoreDegraded)
                    .expect("advance service generation");
            }
        }));

        let joined = join(
            Arc::clone(&store),
            service_state,
            publisher.reader(),
            instance_id,
            Arc::new(ManualClock::new()),
            hook,
            Duration::from_secs(1),
        )
        .snapshot()
        .await
        .expect("reread after service transition");

        assert_eq!(joined.service_state.generation, 1);
        assert_eq!(store.reads(), 2);
    }

    #[tokio::test]
    async fn bootstrap_join_rereads_store_when_exact_collections_drift() {
        let instance_id = Uuid::new_v4();
        let first = snapshot(1);
        let second = snapshot_with_repository(1);
        let store = Arc::new(ScriptedStore::new([first, second.clone()]));
        let service_state = ServiceStateController::new(ServiceState::Ready);
        let publisher = publisher(instance_id, &second, 0);

        let joined = join(
            Arc::clone(&store),
            service_state,
            publisher.reader(),
            instance_id,
            Arc::new(ManualClock::new()),
            Arc::new(RecordingHook::new(|_, _| {})),
            Duration::from_secs(1),
        )
        .snapshot()
        .await
        .expect("reread exact collection set");

        assert_eq!(joined.store, second);
        assert_eq!(store.reads(), 2);
    }

    #[tokio::test]
    async fn bootstrap_join_budget_exhaustion_is_typed_and_never_returns_an_approximation() {
        let instance_id = Uuid::new_v4();
        let durable = snapshot(1);
        let ahead = snapshot(2);
        let store = Arc::new(ScriptedStore::new([durable]));
        let service_state = ServiceStateController::new(ServiceState::Ready);
        let publisher = publisher(instance_id, &ahead, 0);
        let clock = Arc::new(ManualClock::new());
        let clock_for_hook = Arc::clone(&clock);
        let hook = Arc::new(RecordingHook::new(move |_, point| {
            if point == BootstrapJoinPoint::StoreSnapshot {
                clock_for_hook.advance(Duration::from_millis(2));
            }
        }));

        let error = join(
            store,
            service_state,
            publisher.reader(),
            instance_id,
            clock,
            hook,
            Duration::from_millis(3),
        )
        .snapshot()
        .await
        .expect_err("budget exhaustion must fail closed");

        assert!(matches!(error, BootstrapJoinError::SnapshotUnavailable));
        assert_eq!(error.to_string(), BOOTSTRAP_SNAPSHOT_UNAVAILABLE);
    }

    #[tokio::test(start_paused = true)]
    async fn bootstrap_join_absolute_budget_bounds_a_store_read_that_never_completes() {
        let instance_id = Uuid::new_v4();
        let durable = snapshot(0);
        let store = Arc::new(NeverCompletesStore {
            reads: AtomicUsize::new(0),
        });
        let publisher = publisher(instance_id, &durable, 0);
        let started = Instant::now();

        let error = BootstrapJoin::with_test_dependencies(
            store.clone(),
            ServiceStateController::new(ServiceState::Ready),
            publisher.reader(),
            instance_id,
            Arc::new(TokioBootstrapJoinClock),
            Arc::new(RecordingHook::new(|_, _| {})),
            Duration::from_millis(25),
        )
        .snapshot()
        .await
        .expect_err("a stalled Store read must consume the bounded join budget");

        assert!(matches!(error, BootstrapJoinError::SnapshotUnavailable));
        assert_eq!(store.reads.load(Ordering::SeqCst), 1);
        assert_eq!(
            Instant::now().duration_since(started),
            Duration::from_millis(25)
        );
    }

    #[test]
    fn scheduler_membership_witness_ignores_panel_and_repository_metadata_changes() {
        let instance_id = Uuid::new_v4();
        let before = membership_snapshot(TaskStatus::Running);
        let mut after = before.clone();
        after.repositories[0].display_name = "renamed only".to_owned();
        after.repositories[0].last_opened_at = timestamp(1);
        after.tasks[0].prompt = "panel-independent prompt metadata".to_owned();
        after.tasks[0].last_event_id = EventId::new(2).expect("valid panel event ID");
        after.latest_event_id = cursor(2);

        let before_state = SchedulerStoreState::from_store_snapshot(instance_id, &before);
        assert!(before_state.exactly_matches(&after));
        let mut publisher = SchedulerStatePublisher::new(SchedulerProjectionCandidate::new(
            before_state,
            before.membership_event_id,
            0,
        ));
        publisher
            .stage(SchedulerProjectionCandidate::new(
                SchedulerStoreState::from_store_snapshot(instance_id, &after),
                after.membership_event_id,
                0,
            ))
            .expect("stage metadata-only observation");
        let unchanged = publisher.flush().expect("flush metadata-only observation");
        assert!(!unchanged.changed());
        assert_eq!(unchanged.snapshot().generation(), 0);
    }

    #[test]
    fn scheduler_membership_witness_advances_only_for_causal_set_or_lifecycle_changes() {
        let instance_id = Uuid::new_v4();
        let queued = membership_snapshot(TaskStatus::Queued);

        let mut queued_set_changed = queued.clone();
        queued_set_changed.tasks.push(task(
            queued.repositories[0].id,
            TaskStatus::Queued,
            timestamp(1),
        ));
        queued_set_changed.latest_event_id = cursor(2);
        queued_set_changed.membership_event_id = cursor(2);
        assert_membership_change_publishes(instance_id, &queued, &queued_set_changed);

        let mut started = queued.clone();
        started.tasks[0].status = TaskStatus::Running;
        started.tasks[0].started_at = Some(timestamp(0));
        started.tasks[0].last_event_id = EventId::new(2).expect("valid started event ID");
        started.latest_event_id = cursor(2);
        started.membership_event_id = cursor(2);
        assert_membership_change_publishes(instance_id, &queued, &started);

        let mut terminal = started.clone();
        terminal.tasks[0].status = TaskStatus::Completed;
        terminal.tasks[0].finished_at = Some(timestamp(1));
        terminal.tasks[0].last_event_id = EventId::new(3).expect("valid terminal event ID");
        terminal.latest_event_id = cursor(3);
        terminal.membership_event_id = cursor(3);
        assert_membership_change_publishes(instance_id, &started, &terminal);

        let mut intent = started.clone();
        intent.running_stop_intents.push(StopIntentReceipt {
            task_id: intent.tasks[0].id,
            repository_id: intent.tasks[0].repository_id,
            attempt: intent.tasks[0].attempt,
            kind: StopIntentKind::UserCancelled,
            requested_at: timestamp(1),
        });
        assert_membership_change_publishes(instance_id, &started, &intent);

        let mut repository_set_changed = queued.clone();
        repository_set_changed
            .repositories
            .push(repository("second repository"));
        assert_membership_change_publishes(instance_id, &queued, &repository_set_changed);
    }
}
