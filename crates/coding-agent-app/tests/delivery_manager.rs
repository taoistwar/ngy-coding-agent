#![cfg(feature = "test-support")]

mod support;

use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use coding_agent_app::{
    DeliveryAllowedAction, DeliveryEligibility, DeliveryManagerHandle,
    DeliveryManagerLiveDependencies, DeliveryOperationProjection, DeliveryOperationQuery,
    DeliveryOperationQueryOutcome, DeliveryOperationQueryTestSeam, DeliveryPreflightBusyReason,
    DeliveryPreflightDurability, DeliveryPreflightOperation, DeliveryPreflightOutcome,
    DeliveryPreflightRequest, DeliveryPreflightState, DeliveryPreflightUnavailableReason,
    DeliveryPreparedPreflight, DeliveryProcessProof, DeliveryProcessProofError,
    DeliveryProcessProofProvider, DeliveryProcessProofProviderTestSeam,
    DeliveryQueryUnavailableReason, DeliveryRuntimeAuthentication,
    DeliveryRuntimeAuthenticationOutcome, DeliveryRuntimeFailure, DeliveryRuntimeObservation,
    DeliveryRuntimeObservationUnavailableReason, DeliveryRuntimeRegistry,
    DeliveryRuntimeRegistryTestSeam, DeliveryRuntimeSession, DeliveryRuntimeSessionTestSeam,
    DeliveryTargetObservation, DeliveryTargetUnavailableReason, DeliveryTaskProjection,
    DeliveryTaskQueryOutcome, EventDispatcherHandle, RepositoryControlCoordinator,
    RepositoryControlState, RepositoryCoordinationKey, SchedulerConcurrencyLimits, ServiceState,
    ServiceStateController, StoreWriterFaultPoint, StoreWriterFaultSpec, StoreWriterHandle,
    StoreWriterOperationKind, StoreWriterTestController, TaskManagerHandle,
    TaskManagerLaunchResources,
};
use coding_agent_domain::{
    CanonicalPath, ClientRequestId, NewRepository, Repository, Task, TaskEventPayload, TaskId,
    TaskStatus,
};
use coding_agent_store::{
    AttemptArtifactIdentity, DeliveryAcceptedOperationState, DeliveryCommand, DeliveryCommandKind,
    DeliveryCommandLookup, DeliveryIdentity, DeliveryOperationId, DeliveryResponseDiscriminator,
    DeliveryVersion, FailUnboundMergePreflightRequest, FinalizeReviewedTaskOutcome, GitBranchRef,
    GitCommitOid, GitObjectAlgorithm, GitTreeOid, MergeConflictPaths, MergeOperationState,
    MergePreflightResult, MergeReconciliationReason, PreflightCommandRequest,
    PreflightRejectedReason, PreflightStaleReason, RecordMergePreflightResultRequest,
    RegisterRepositoryOutcome, ReserveAttemptArtifact, Sha256Digest, TaskTransition,
    TransitionOutcome, UnboundMergePreflightFailure,
};
use tokio::sync::{Notify, Semaphore};
use tokio::time::{Duration, sleep, timeout};

const BASE_COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
const TARGET_HEAD: &str = "123456789abcdef0123456789abcdef012345678";
const CANDIDATE_TREE: &str = "23456789abcdef0123456789abcdef0123456789";
const PREFLIGHT_SOURCE: &str = "3456789abcdef0123456789abcdef0123456789a";
const MERGE_BASE: &str = "456789abcdef0123456789abcdef0123456789ab";
const MERGE_TREE: &str = "56789abcdef0123456789abcdef0123456789abc";
const COMMON_IDENTITY: &str = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";
const ADMIN_IDENTITY: &str = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2";
const SOURCE_CONFIG_DIGEST: &str =
    "c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3";
const TARGET_CONFIG_DIGEST: &str =
    "d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4";
const TARGET_SECURITY_DIGEST: &str =
    "e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5";

#[derive(Default)]
struct CleanProcessProofs {
    observations: AtomicUsize,
}

impl DeliveryProcessProofProviderTestSeam for CleanProcessProofs {}

#[derive(Default)]
struct ControlledOperationQuery {
    outcome: Mutex<Option<DeliveryOperationQueryOutcome>>,
    lookups: AtomicUsize,
    panic_next: AtomicBool,
}

impl ControlledOperationQuery {
    fn return_once(&self, outcome: DeliveryOperationQueryOutcome) {
        *self.outcome.lock().expect("lock operation-query outcome") = Some(outcome);
    }

    fn panic_once(&self) {
        self.panic_next.store(true, Ordering::SeqCst);
    }
}

impl DeliveryOperationQueryTestSeam for ControlledOperationQuery {}

#[async_trait::async_trait]
impl DeliveryOperationQuery for ControlledOperationQuery {
    async fn lookup(&self, operation_id: DeliveryOperationId) -> DeliveryOperationQueryOutcome {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.panic_next.swap(false, Ordering::SeqCst),
            "controlled operation query panic"
        );
        self.outcome
            .lock()
            .expect("lock operation-query outcome")
            .take()
            .unwrap_or_else(|| DeliveryOperationQueryOutcome::not_found(operation_id))
    }
}

#[async_trait::async_trait]
impl DeliveryProcessProofProvider for CleanProcessProofs {
    async fn observe(
        &self,
        _task_id: TaskId,
    ) -> Result<DeliveryProcessProof, DeliveryProcessProofError> {
        self.observations.fetch_add(1, Ordering::SeqCst);
        Ok(DeliveryProcessProof::Clean)
    }
}

struct RuntimeControl {
    session: Arc<ControlledRuntimeSession>,
    opens: AtomicUsize,
    open_delay: Mutex<Option<Duration>>,
    coordinator: Mutex<Option<Arc<RepositoryControlCoordinator>>>,
    authority_key_override: Mutex<Option<RepositoryCoordinationKey>>,
}

impl RuntimeControl {
    fn ready() -> Arc<Self> {
        Arc::new(Self {
            session: Arc::new(ControlledRuntimeSession::default()),
            opens: AtomicUsize::new(0),
            open_delay: Mutex::new(None),
            coordinator: Mutex::new(None),
            authority_key_override: Mutex::new(None),
        })
    }

    fn bind_coordinator(&self, coordinator: Arc<RepositoryControlCoordinator>) {
        *self.coordinator.lock().expect("lock runtime coordinator") = Some(coordinator);
    }

    fn delay_next_open(&self, delay: Duration) {
        *self
            .open_delay
            .lock()
            .expect("lock controlled runtime-open delay") = Some(delay);
    }

    fn override_authority_key(&self, key: RepositoryCoordinationKey) {
        *self
            .authority_key_override
            .lock()
            .expect("lock runtime authority override") = Some(key);
    }
}

impl DeliveryRuntimeRegistryTestSeam for RuntimeControl {}

struct RuntimeTimeResumeGuard;

impl Drop for RuntimeTimeResumeGuard {
    fn drop(&mut self) {
        tokio::time::resume();
    }
}

async fn elapse_isolated_runtime_stage(delay: Duration) {
    // Keep virtual time scoped to the controlled runtime stage. Pausing the
    // whole preflight also advances Store read/write deadlines while SQLite
    // work is waiting on real I/O, which makes this test race parallel cases.
    tokio::time::pause();
    let _resume = RuntimeTimeResumeGuard;
    sleep(delay).await;
}

#[async_trait::async_trait]
impl DeliveryRuntimeRegistry for RuntimeControl {
    async fn open_session(
        &self,
        snapshot: &coding_agent_store::DeliveryEligibilitySnapshot,
    ) -> Result<Arc<dyn DeliveryRuntimeSession>, DeliveryRuntimeFailure> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        let delay = self
            .open_delay
            .lock()
            .expect("lock controlled runtime-open delay")
            .take();
        if let Some(delay) = delay {
            elapse_isolated_runtime_stage(delay).await;
        }
        let coordinator = self
            .coordinator
            .lock()
            .expect("lock runtime coordinator")
            .clone()
            .ok_or(DeliveryRuntimeFailure::Unavailable)?;
        let coordination_key = self
            .authority_key_override
            .lock()
            .expect("lock runtime authority override")
            .unwrap_or(
                coordinator
                    .coordination_key(snapshot.task.repository_id)
                    .map_err(|_| DeliveryRuntimeFailure::Unavailable)?,
            );
        let evidence = snapshot
            .evidence_identity
            .as_ref()
            .ok_or(DeliveryRuntimeFailure::Unavailable)?;
        let artifact = snapshot
            .ownership
            .artifact
            .as_ref()
            .ok_or(DeliveryRuntimeFailure::Unavailable)?;
        let source_base_commit = GitCommitOid::from_str(&artifact.base_commit)
            .map_err(|_| DeliveryRuntimeFailure::Unavailable)?;
        let source_branch = GitBranchRef::from_str(&format!("refs/heads/{}", artifact.branch_name))
            .map_err(|_| DeliveryRuntimeFailure::Unavailable)?;
        Ok(Arc::new(ContextualRuntimeSession {
            control: self.session.clone(),
            coordination_key,
            source_identity: evidence.identity(),
            source_base_commit,
            source_branch,
            approved_workspace_fingerprint: evidence.workspace_fingerprint().clone(),
        }))
    }
}

struct ContextualRuntimeSession {
    control: Arc<ControlledRuntimeSession>,
    coordination_key: RepositoryCoordinationKey,
    source_identity: DeliveryIdentity,
    source_base_commit: GitCommitOid,
    source_branch: GitBranchRef,
    approved_workspace_fingerprint: Sha256Digest,
}

impl DeliveryRuntimeSessionTestSeam for ContextualRuntimeSession {}

#[derive(Default)]
struct ControlledRuntimeSession {
    observations: AtomicUsize,
    observation_results:
        Mutex<VecDeque<Result<DeliveryRuntimeObservation, DeliveryRuntimeFailure>>>,
    authentications: AtomicUsize,
    preparations: AtomicUsize,
    preflights: AtomicUsize,
    authentication_failures: Mutex<VecDeque<DeliveryRuntimeFailure>>,
    authentication_gate: Mutex<Option<Arc<ObservationGate>>>,
    authentication_delay: Mutex<Option<Duration>>,
    prepare_failures: Mutex<VecDeque<DeliveryRuntimeFailure>>,
    prepare_gate: Mutex<Option<Arc<ObservationGate>>>,
    prepare_delay: Mutex<Option<Duration>>,
    preflight_results: Mutex<VecDeque<Result<MergePreflightResult, DeliveryRuntimeFailure>>>,
    run_gate: Mutex<Option<Arc<RunGate>>>,
    run_delay: Mutex<Option<Duration>>,
    parallel_run_gate: Mutex<Option<Arc<ParallelRunGate>>>,
}

impl ControlledRuntimeSession {
    fn push_observation(
        &self,
        observation: Result<DeliveryRuntimeObservation, DeliveryRuntimeFailure>,
    ) {
        self.observation_results
            .lock()
            .expect("lock controlled runtime observations")
            .push_back(observation);
    }

    fn push_preflight(&self, result: Result<MergePreflightResult, DeliveryRuntimeFailure>) {
        self.preflight_results
            .lock()
            .expect("lock controlled preflight results")
            .push_back(result);
    }

    fn push_authentication_failure(&self, failure: DeliveryRuntimeFailure) {
        self.authentication_failures
            .lock()
            .expect("lock controlled authentication failures")
            .push_back(failure);
    }

    fn delay_next_prepare(&self, delay: Duration) {
        *self
            .prepare_delay
            .lock()
            .expect("lock controlled preparation delay") = Some(delay);
    }

    fn delay_next_authentication(&self, delay: Duration) {
        *self
            .authentication_delay
            .lock()
            .expect("lock controlled authentication delay") = Some(delay);
    }

    fn install_prepare_gate(&self) -> Arc<ObservationGate> {
        let gate = Arc::new(ObservationGate::default());
        *self.prepare_gate.lock().expect("lock preparation gate") = Some(gate.clone());
        gate
    }

    fn delay_next_run(&self, delay: Duration) {
        *self.run_delay.lock().expect("lock controlled run delay") = Some(delay);
    }

    fn install_authentication_gate(&self) -> Arc<ObservationGate> {
        let gate = Arc::new(ObservationGate::default());
        *self
            .authentication_gate
            .lock()
            .expect("lock controlled authentication gate") = Some(gate.clone());
        gate
    }

    fn install_run_gate(
        &self,
        outcome: Result<MergePreflightResult, DeliveryRuntimeFailure>,
    ) -> Arc<RunGate> {
        let gate = Arc::new(RunGate {
            reached: Semaphore::new(0),
            release: Notify::new(),
            outcome: Mutex::new(Some(outcome)),
        });
        *self.run_gate.lock().expect("lock controlled run gate") = Some(gate.clone());
        gate
    }

    fn install_parallel_run_gate(&self) -> Arc<ParallelRunGate> {
        let gate = Arc::new(ParallelRunGate::default());
        *self
            .parallel_run_gate
            .lock()
            .expect("lock parallel controlled run gate") = Some(gate.clone());
        gate
    }
}

#[async_trait::async_trait]
impl DeliveryRuntimeSession for ContextualRuntimeSession {
    async fn observe(&self) -> Result<DeliveryRuntimeObservation, DeliveryRuntimeFailure> {
        self.control.observations.fetch_add(1, Ordering::SeqCst);
        self.control
            .observation_results
            .lock()
            .expect("lock controlled runtime observations")
            .pop_front()
            .unwrap_or_else(|| {
                Ok(DeliveryRuntimeObservation::available_for_test(
                    GitBranchRef::from_str("refs/heads/main")
                        .expect("valid observed target branch"),
                    GitCommitOid::from_str(TARGET_HEAD).expect("valid observed target head"),
                ))
            })
    }

    async fn authenticate_preflight(
        &self,
        command: &PreflightCommandRequest,
    ) -> Result<DeliveryRuntimeAuthenticationOutcome, DeliveryRuntimeFailure> {
        self.control.authentications.fetch_add(1, Ordering::SeqCst);
        let delay = self
            .control
            .authentication_delay
            .lock()
            .expect("lock controlled authentication delay")
            .take();
        if let Some(delay) = delay {
            elapse_isolated_runtime_stage(delay).await;
        }
        let authentication_gate = self
            .control
            .authentication_gate
            .lock()
            .expect("lock controlled authentication gate")
            .clone();
        if let Some(gate) = authentication_gate {
            gate.reached.add_permits(1);
            gate.release.notified().await;
        }
        let authentication = DeliveryRuntimeAuthentication::new_for_test(
            self.coordination_key,
            self.source_identity,
            self.source_base_commit.clone(),
            self.source_branch.clone(),
            self.approved_workspace_fingerprint.clone(),
            GitObjectAlgorithm::Sha1,
            coding_agent_store::DirectoryIdentity::try_new(
                "directory_identity_v1",
                COMMON_IDENTITY,
            )
            .expect("valid common identity"),
            coding_agent_store::DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY)
                .expect("valid admin identity"),
            Sha256Digest::from_str(SOURCE_CONFIG_DIGEST).expect("valid source config digest"),
            command.target_branch().clone(),
            command.expected_target_head().clone(),
            Sha256Digest::from_str(TARGET_CONFIG_DIGEST).expect("valid target config digest"),
            Sha256Digest::from_str(TARGET_SECURITY_DIGEST).expect("valid target security digest"),
        )?;
        match self
            .control
            .authentication_failures
            .lock()
            .expect("lock controlled authentication failures")
            .pop_front()
        {
            Some(failure) => Ok(DeliveryRuntimeAuthenticationOutcome::KnownFailure {
                authentication,
                failure,
            }),
            None => Ok(DeliveryRuntimeAuthenticationOutcome::Ready(authentication)),
        }
    }

    async fn prepare_preflight(&self) -> Result<DeliveryPreparedPreflight, DeliveryRuntimeFailure> {
        self.control.preparations.fetch_add(1, Ordering::SeqCst);
        let delay = self
            .control
            .prepare_delay
            .lock()
            .expect("lock controlled preparation delay")
            .take();
        if let Some(delay) = delay {
            elapse_isolated_runtime_stage(delay).await;
        }
        let prepare_gate = self
            .control
            .prepare_gate
            .lock()
            .expect("lock preparation gate")
            .clone();
        if let Some(gate) = prepare_gate {
            gate.reached.add_permits(1);
            gate.release.notified().await;
        }
        if let Some(failure) = self
            .control
            .prepare_failures
            .lock()
            .expect("lock controlled preparation failures")
            .pop_front()
        {
            return Err(failure);
        }
        Ok(DeliveryPreparedPreflight::new_for_test(
            GitTreeOid::from_str(CANDIDATE_TREE).expect("valid candidate tree"),
            GitCommitOid::from_str(PREFLIGHT_SOURCE).expect("valid preflight source"),
            (),
        ))
    }

    async fn run_preflight(
        &self,
        _prepared: &DeliveryPreparedPreflight,
    ) -> Result<MergePreflightResult, DeliveryRuntimeFailure> {
        self.control.preflights.fetch_add(1, Ordering::SeqCst);
        let delay = self
            .control
            .run_delay
            .lock()
            .expect("lock controlled run delay")
            .take();
        if let Some(delay) = delay {
            elapse_isolated_runtime_stage(delay).await;
        }
        let gate = self
            .control
            .run_gate
            .lock()
            .expect("lock controlled run gate")
            .clone();
        if let Some(gate) = gate {
            gate.reached.add_permits(1);
            gate.release.notified().await;
            return gate
                .outcome
                .lock()
                .expect("lock controlled run outcome")
                .take()
                .expect("controlled run outcome is consumed once");
        }
        let parallel_gate = self
            .control
            .parallel_run_gate
            .lock()
            .expect("lock parallel controlled run gate")
            .clone();
        if let Some(gate) = parallel_gate {
            gate.enter().await;
            return Ok(ready_result());
        }
        self.control
            .preflight_results
            .lock()
            .expect("lock controlled preflight results")
            .pop_front()
            .unwrap_or_else(|| Ok(ready_result()))
    }
}

struct ParallelRunGate {
    reached: Semaphore,
    release: Semaphore,
    active: AtomicUsize,
    peak: AtomicUsize,
}

impl Default for ParallelRunGate {
    fn default() -> Self {
        Self {
            reached: Semaphore::new(0),
            release: Semaphore::new(0),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }
}

impl ParallelRunGate {
    async fn enter(&self) {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        self.reached.add_permits(1);
        self.release
            .acquire()
            .await
            .expect("parallel run gate remains open")
            .forget();
        self.active.fetch_sub(1, Ordering::SeqCst);
    }

    async fn wait_until_reached(&self, count: u32) {
        timeout(Duration::from_secs(5), self.reached.acquire_many(count))
            .await
            .expect("runtime reaches parallel preflight gate")
            .expect("parallel preflight gate remains open")
            .forget();
    }

    fn release(&self, count: usize) {
        self.release.add_permits(count);
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}

struct RunGate {
    reached: Semaphore,
    release: Notify,
    outcome: Mutex<Option<Result<MergePreflightResult, DeliveryRuntimeFailure>>>,
}

struct ObservationGate {
    reached: Semaphore,
    release: Notify,
}

impl Default for ObservationGate {
    fn default() -> Self {
        Self {
            reached: Semaphore::new(0),
            release: Notify::new(),
        }
    }
}

impl ObservationGate {
    async fn wait_until_reached(&self) {
        timeout(Duration::from_secs(5), self.reached.acquire())
            .await
            .expect("runtime reaches observation gate")
            .expect("observation gate remains open")
            .forget();
    }

    fn release(&self) {
        self.release.notify_one();
    }
}

impl RunGate {
    async fn wait_until_reached(&self) {
        timeout(Duration::from_secs(5), self.reached.acquire())
            .await
            .expect("runtime reaches controlled preflight gate")
            .expect("controlled preflight gate remains open")
            .forget();
    }

    fn release(&self) {
        self.release.notify_one();
    }
}

struct LiveFixture {
    manager: DeliveryManagerHandle,
    service_state: ServiceStateController,
    coordinator: Arc<RepositoryControlCoordinator>,
    operation_query: Arc<ControlledOperationQuery>,
    repositories: Vec<Repository>,
    base: support::StoreFixture,
}

async fn live_fixture(
    runtime: Arc<RuntimeControl>,
    writer_controller: Option<Arc<StoreWriterTestController>>,
) -> LiveFixture {
    live_fixture_with_repository_count_and_capacity(runtime, writer_controller, 1, 16).await
}

async fn live_fixture_with_repository_count(
    runtime: Arc<RuntimeControl>,
    writer_controller: Option<Arc<StoreWriterTestController>>,
    repository_count: usize,
) -> LiveFixture {
    live_fixture_with_repository_count_and_capacity(
        runtime,
        writer_controller,
        repository_count,
        16,
    )
    .await
}

async fn live_fixture_with_repository_count_and_capacity(
    runtime: Arc<RuntimeControl>,
    writer_controller: Option<Arc<StoreWriterTestController>>,
    repository_count: usize,
    manager_capacity: usize,
) -> LiveFixture {
    live_fixture_with_operation_query_mode(
        runtime,
        writer_controller,
        repository_count,
        manager_capacity,
        false,
    )
    .await
}

async fn live_fixture_with_store_operation_query(runtime: Arc<RuntimeControl>) -> LiveFixture {
    live_fixture_with_operation_query_mode(runtime, None, 1, 16, true).await
}

async fn live_fixture_with_operation_query_mode(
    runtime: Arc<RuntimeControl>,
    writer_controller: Option<Arc<StoreWriterTestController>>,
    repository_count: usize,
    manager_capacity: usize,
    use_store_operation_query: bool,
) -> LiveFixture {
    assert!(
        repository_count > 0,
        "fixture needs at least one repository"
    );
    let base = support::store_fixture().await;
    let fixture_root = base
        .repository
        .git_root
        .as_path()
        .parent()
        .expect("seed repository has a fixture parent")
        .to_path_buf();
    let mut repositories = vec![base.repository.clone()];
    for index in 1..repository_count {
        let repository = match base
            .store
            .register_repository(NewRepository {
                selected_path: CanonicalPath::try_from_canonical(
                    fixture_root.join(format!("delivery-{index}-selected")),
                )
                .expect("valid selected path"),
                display_name: format!("delivery-{index}"),
                git_root: CanonicalPath::try_from_canonical(
                    fixture_root.join(format!("delivery-{index}-git")),
                )
                .expect("valid Git root"),
                cargo_workspace_root: CanonicalPath::try_from_canonical(
                    fixture_root.join(format!("delivery-{index}-workspace")),
                )
                .expect("valid workspace root"),
            })
            .await
            .expect("register delivery-manager fixture repository")
        {
            RegisterRepositoryOutcome::Created(repository)
            | RegisterRepositoryOutcome::Existing(repository) => repository,
        };
        repositories.push(repository);
    }
    let (coordinator, _) = support::repository_control_fixture(&base.store).await;
    runtime.bind_coordinator(coordinator.clone());
    let dispatcher = EventDispatcherHandle::spawn(base.store.clone(), 128)
        .await
        .expect("spawn delivery-manager dispatcher");
    let writer = match writer_controller {
        Some(controller) => StoreWriterHandle::spawn_with_test_controller(
            base.store.clone(),
            Arc::new(dispatcher.clone()),
            32,
            controller,
        ),
        None => StoreWriterHandle::spawn(base.store.clone(), Arc::new(dispatcher.clone()), 32),
    };
    // Keep the real TaskManager read port without letting its scheduler race
    // this fixture's direct construction of reviewed tasks/artifacts. The
    // DeliveryManager has an independent Ready service controller below.
    let task_manager_state = ServiceStateController::new(ServiceState::StoreDegraded);
    let launch_resources = TaskManagerLaunchResources::new_for_test(
        SchedulerConcurrencyLimits::try_new(4, 4).expect("valid fixture limits"),
        coordinator.clone(),
        base.instance_process_scope(),
    );
    let task_manager = TaskManagerHandle::spawn(
        base.store.clone(),
        writer.clone(),
        dispatcher,
        task_manager_state,
        Arc::new(support::ControlledRunner::default()),
        launch_resources,
        32,
    );
    let processes = Arc::new(CleanProcessProofs::default());
    let operation_query = Arc::new(ControlledOperationQuery::default());
    let dependencies = if use_store_operation_query {
        DeliveryManagerLiveDependencies::new_with_store_operation_query_for_test(
            base.store.clone(),
            writer.clone(),
            task_manager.clone(),
            coordinator.clone(),
            runtime.clone(),
            processes.clone(),
        )
    } else {
        DeliveryManagerLiveDependencies::new_for_test(
            base.store.clone(),
            writer.clone(),
            task_manager.clone(),
            coordinator.clone(),
            runtime.clone(),
            processes.clone(),
            operation_query.clone(),
        )
    };
    let state = ServiceStateController::new(ServiceState::Ready);
    let manager =
        DeliveryManagerHandle::spawn_live_for_test(dependencies, state.clone(), manager_capacity);
    LiveFixture {
        manager,
        service_state: state,
        coordinator,
        operation_query,
        repositories,
        base,
    }
}

fn preflight_request(task_id: TaskId) -> DeliveryPreflightRequest {
    DeliveryPreflightRequest::new(
        PreflightCommandRequest::try_new(
            ClientRequestId::new(),
            task_id,
            GitBranchRef::from_str("refs/heads/main").expect("valid target branch"),
            GitCommitOid::from_str(TARGET_HEAD).expect("valid target head"),
        )
        .expect("valid preflight request"),
    )
}

async fn approved_task(fixture: &support::StoreFixture) -> Task {
    approved_task_for_repository(fixture, &fixture.repository).await
}

async fn approved_task_for_repository(
    fixture: &support::StoreFixture,
    repository: &Repository,
) -> Task {
    let queued = fixture
        .store
        .create_task(support::new_task(
            repository.id,
            "delivery manager approved task",
        ))
        .await
        .expect("create fixture task")
        .task()
        .clone();
    let running = match fixture
        .store
        .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .expect("start fixture task")
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("fixture task must start"),
    };
    fixture
        .store
        .append_running_event(
            running.id,
            TaskEventPayload::PlanUpdated {
                plan: support::fixture_review_plan(),
            },
        )
        .await
        .expect("persist fixture plan");
    let running = fixture
        .store
        .task_detail(running.id)
        .await
        .expect("read fixture task")
        .expect("fixture task exists")
        .task;
    let identity = AttemptArtifactIdentity {
        task_id: running.id,
        repository_id: running.repository_id,
        attempt: running.attempt,
    };
    // Artifact branch names are Store-global identities, not merely
    // repository-local names. Give every fixture task its own namespace so
    // multi-repository concurrency tests exercise DeliveryManager rather than
    // colliding during setup.
    let artifact_namespace = format!("delivery-manager-{}-{}", repository.id, running.id);
    fixture
        .store
        .reserve_attempt_artifact(ReserveAttemptArtifact {
            identity,
            base_commit: BASE_COMMIT.to_owned(),
            branch_name: format!("codex/{artifact_namespace}"),
            worktree_path: CanonicalPath::try_from_canonical(
                repository
                    .git_root
                    .as_path()
                    .join("artifacts")
                    .join(artifact_namespace),
            )
            .expect("valid artifact path"),
        })
        .await
        .expect("reserve fixture artifact");
    fixture
        .store
        .mark_attempt_artifact_ready(identity)
        .await
        .expect("mark fixture artifact ready");
    match fixture
        .store
        .finalize_reviewed_task(
            running.id,
            running.repository_id,
            running.attempt,
            support::approved_review(),
        )
        .await
        .expect("finalize approved fixture task")
    {
        FinalizeReviewedTaskOutcome::Applied { task, .. }
        | FinalizeReviewedTaskOutcome::Existing { task, .. } => task,
    }
}

fn ready_result() -> MergePreflightResult {
    MergePreflightResult::ready(
        GitCommitOid::from_str(MERGE_BASE).expect("valid merge base"),
        GitTreeOid::from_str(MERGE_TREE).expect("valid merge tree"),
    )
    .expect("valid ready preflight")
}

fn conflict_result() -> MergePreflightResult {
    MergePreflightResult::conflict(
        GitCommitOid::from_str(MERGE_BASE).expect("valid merge base"),
        GitTreeOid::from_str(MERGE_TREE).expect("valid merge tree"),
        MergeConflictPaths::try_from_raw(vec![b"src/conflict.rs".to_vec()])
            .expect("valid conflict paths"),
    )
    .expect("valid conflict preflight")
}

fn durable_state(
    outcome: DeliveryPreflightOutcome,
) -> (DeliveryPreflightDurability, DeliveryPreflightState) {
    match outcome {
        DeliveryPreflightOutcome::Durable(operation) => (operation.durability(), operation.state()),
        other => panic!("expected durable preflight, got {other:?}"),
    }
}

fn created_preflight_operation(
    outcome: DeliveryPreflightOutcome,
    expected_state: DeliveryPreflightState,
) -> DeliveryPreflightOperation {
    match outcome {
        DeliveryPreflightOutcome::Durable(operation) => {
            assert_eq!(operation.durability(), DeliveryPreflightDurability::Created);
            assert_eq!(operation.state(), expected_state);
            operation
        }
        other => panic!("expected durable {expected_state:?} preflight, got {other:?}"),
    }
}

async fn assert_exact_preflight_receipt(
    fixture: &LiveFixture,
    task: &Task,
    command: &PreflightCommandRequest,
    operation_id: DeliveryOperationId,
) {
    let receipt = match fixture
        .base
        .store
        .lookup_delivery_command(&DeliveryCommand::Preflight(command.clone()))
        .await
        .expect("lookup exact preflight receipt")
    {
        DeliveryCommandLookup::Existing(receipt) => receipt,
        DeliveryCommandLookup::Missing => panic!("exact preflight receipt must exist"),
    };
    assert_eq!(receipt.client_request_id, command.client_request_id());
    assert_eq!(receipt.command_kind, DeliveryCommandKind::Preflight);
    assert_eq!(receipt.identity.task_id(), task.id);
    assert_eq!(receipt.identity.repository_id(), task.repository_id);
    assert_eq!(receipt.identity.attempt(), task.attempt);
    assert_eq!(
        receipt.canonical_request_hash,
        command.canonical_request_hash()
    );
    assert_eq!(receipt.operation_id, operation_id);
    assert_eq!(
        receipt.accepted_operation_version,
        DeliveryVersion::initial()
    );
    assert_eq!(
        receipt.accepted_operation_state,
        DeliveryAcceptedOperationState::PreflightPending
    );
    assert_eq!(
        receipt.response_discriminator,
        DeliveryResponseDiscriminator::PreflightCreated
    );
}

async fn assert_exact_preflight_operation(
    fixture: &LiveFixture,
    task: &Task,
    command: &PreflightCommandRequest,
    operation_id: DeliveryOperationId,
    expected_state: MergeOperationState,
) {
    let snapshot = fixture
        .base
        .store
        .delivery_eligibility_snapshot(task.id)
        .await
        .expect("read exact preflight snapshot")
        .expect("preflight task exists");
    assert_eq!(snapshot.ownership.merge_operations.len(), 1);
    assert_eq!(
        snapshot.ownership.merge_operations[0].operation_id,
        operation_id
    );
    assert_eq!(
        snapshot.ownership.merge_operations[0].preflight_receipt_id,
        command.client_request_id()
    );
    assert_eq!(snapshot.ownership.merge_operations[0].state, expected_state);
    assert!(snapshot.ownership.disposition.is_none());
    assert!(snapshot.ownership.cleanup_operations.is_empty());
}

fn found_projection(outcome: DeliveryTaskQueryOutcome) -> DeliveryTaskProjection {
    match outcome {
        DeliveryTaskQueryOutcome::Found { projection } => projection,
        other => panic!("expected found delivery projection, got {other:?}"),
    }
}

#[tokio::test]
async fn task_and_operation_queries_are_discriminated_and_operation_ids_are_exact() {
    let runtime = RuntimeControl::ready();
    let fixture = live_fixture(runtime, None).await;
    let missing_task_id = TaskId::new();
    assert!(matches!(
        fixture
            .manager
            .query(missing_task_id)
            .await
            .expect("query missing delivery task"),
        DeliveryTaskQueryOutcome::NotFound { task_id } if task_id == missing_task_id
    ));

    let operation_id = DeliveryOperationId::new();
    let operation_task_id = TaskId::new();
    let operation = DeliveryOperationProjection::merge(
        operation_id,
        operation_task_id,
        DeliveryVersion::try_new(3).expect("valid projected operation version"),
        DeliveryPreflightState::Conflict,
    );
    fixture
        .operation_query
        .return_once(DeliveryOperationQueryOutcome::found(operation.clone()));
    assert_eq!(
        fixture
            .manager
            .query_operation(operation_id)
            .await
            .expect("query found operation"),
        DeliveryOperationQueryOutcome::found(operation)
    );
    let missing_operation_id = DeliveryOperationId::new();
    assert_eq!(
        fixture
            .manager
            .query_operation(missing_operation_id)
            .await
            .expect("query missing operation"),
        DeliveryOperationQueryOutcome::not_found(missing_operation_id)
    );
    assert_eq!(fixture.operation_query.lookups.load(Ordering::SeqCst), 2);

    let requested_operation_id = DeliveryOperationId::new();
    fixture
        .operation_query
        .return_once(DeliveryOperationQueryOutcome::found(
            DeliveryOperationProjection::merge(
                DeliveryOperationId::new(),
                operation_task_id,
                DeliveryVersion::initial(),
                DeliveryPreflightState::PreflightPending,
            ),
        ));
    assert_eq!(
        fixture
            .manager
            .query_operation(requested_operation_id)
            .await
            .expect("reject mismatched operation projection"),
        DeliveryOperationQueryOutcome::unavailable(
            requested_operation_id,
            DeliveryQueryUnavailableReason::OrchestrationUnavailable,
        )
    );

    let panicking_operation_id = DeliveryOperationId::new();
    fixture.operation_query.panic_once();
    assert_eq!(
        fixture
            .manager
            .query_operation(panicking_operation_id)
            .await
            .expect("panic is converted into a typed operation-query outcome"),
        DeliveryOperationQueryOutcome::unavailable(
            panicking_operation_id,
            DeliveryQueryUnavailableReason::OrchestrationUnavailable,
        )
    );
    let after_panic_operation_id = DeliveryOperationId::new();
    assert_eq!(
        fixture
            .manager
            .query_operation(after_panic_operation_id)
            .await
            .expect("query lane remains available after a worker panic"),
        DeliveryOperationQueryOutcome::not_found(after_panic_operation_id)
    );
}

#[tokio::test]
async fn store_backed_operation_query_loads_one_audited_exact_operation() {
    let runtime = RuntimeControl::ready();
    runtime.session.push_preflight(Ok(conflict_result()));
    let fixture = live_fixture_with_store_operation_query(runtime).await;
    let task = approved_task(&fixture.base).await;
    let operation_id = match fixture
        .manager
        .preflight(preflight_request(task.id))
        .await
        .expect("persist operation for exact Store query")
    {
        DeliveryPreflightOutcome::Durable(operation) => operation.operation_id(),
        other => panic!("expected durable operation, got {other:?}"),
    };
    let before_query = fixture
        .base
        .store
        .delivery_eligibility_snapshot(task.id)
        .await
        .expect("read operation-query baseline")
        .expect("delivery task exists");

    let projection = match fixture
        .manager
        .query_operation(operation_id)
        .await
        .expect("query exact Store-backed operation")
    {
        DeliveryOperationQueryOutcome::Found { operation } => operation,
        other => panic!("expected found Store-backed operation, got {other:?}"),
    };
    assert_eq!(projection.operation_id(), operation_id);
    assert_eq!(projection.task_id(), task.id);
    assert!(matches!(
        projection,
        DeliveryOperationProjection::Merge {
            state: DeliveryPreflightState::Conflict,
            ..
        }
    ));
    assert_eq!(
        projection.allowed_actions(),
        &[DeliveryAllowedAction::RunPreflight]
    );
    assert_eq!(
        fixture
            .base
            .store
            .delivery_eligibility_snapshot(task.id)
            .await
            .expect("read operation-query post-state")
            .expect("delivery task exists"),
        before_query,
        "exact operation GET must not mutate durable delivery state"
    );

    let missing_operation_id = DeliveryOperationId::new();
    assert_eq!(
        fixture
            .manager
            .query_operation(missing_operation_id)
            .await
            .expect("query missing Store-backed operation"),
        DeliveryOperationQueryOutcome::not_found(missing_operation_id)
    );
}

#[tokio::test]
async fn runtime_open_auth_prepare_and_merge_stages_may_exceed_orchestration_budget() {
    let runtime = RuntimeControl::ready();
    let fixture = live_fixture(runtime.clone(), None).await;
    let task = approved_task(&fixture.base).await;
    runtime.delay_next_open(Duration::from_secs(31));
    runtime
        .session
        .delay_next_authentication(Duration::from_secs(31));
    runtime.session.delay_next_prepare(Duration::from_secs(31));
    runtime.session.delay_next_run(Duration::from_secs(31));

    let (durability, state) = durable_state(
        fixture
            .manager
            .preflight(preflight_request(task.id))
            .await
            .expect("complete a valid slow runtime preflight stage"),
    );
    assert!(matches!(
        durability,
        DeliveryPreflightDurability::Created | DeliveryPreflightDurability::Existing
    ));
    assert_eq!(state, DeliveryPreflightState::PreflightReady);
}

#[tokio::test]
async fn outer_runtime_timeout_during_childless_open_releases_worker_ownership() {
    let runtime = RuntimeControl::ready();
    runtime.delay_next_open(Duration::from_secs(11 * 60 + 1));
    let fixture = live_fixture(runtime, None).await;
    let task = approved_task(&fixture.base).await;

    assert_eq!(
        fixture
            .manager
            .preflight(preflight_request(task.id))
            .await
            .expect("childless runtime-open timeout remains typed"),
        DeliveryPreflightOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::RuntimeUnavailable
        )
    );
    assert_preflight_control_and_workers(
        &fixture,
        task.repository_id,
        RepositoryControlState::Poisoned,
        0,
        "childless runtime-open timeout",
    )
    .await;
}

#[tokio::test]
async fn outer_runtime_timeout_during_preflight_run_retains_repository_ownership() {
    let runtime = RuntimeControl::ready();
    let gate = runtime.session.install_run_gate(Ok(ready_result()));
    let fixture = live_fixture(runtime, None).await;
    let task = approved_task(&fixture.base).await;
    let caller = tokio::spawn({
        let manager = fixture.manager.clone();
        async move { manager.preflight(preflight_request(task.id)).await }
    });

    gate.wait_until_reached().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(11 * 60 + 1)).await;
    tokio::time::resume();

    let outcome = caller
        .await
        .expect("join outer-timeout preflight caller")
        .expect("preflight manager remains open");
    let (durability, state) = durable_state(outcome);
    assert!(matches!(
        durability,
        DeliveryPreflightDurability::Created | DeliveryPreflightDurability::Existing
    ));
    assert_eq!(state, DeliveryPreflightState::ReconciliationRequired);
    assert_eq!(
        fixture
            .manager
            .preflight(preflight_request(task.id))
            .await
            .expect("same-repository request is rejected while cleanup is unproven"),
        DeliveryPreflightOutcome::Busy(DeliveryPreflightBusyReason::RepositoryBusy)
    );
    assert_preflight_control_and_workers(
        &fixture,
        task.repository_id,
        RepositoryControlState::Busy,
        1,
        "outer preflight runtime timeout",
    )
    .await;
}

#[tokio::test]
async fn prepared_runtime_timeout_keeps_retention_after_persist_invariant_conflict() {
    let runtime = RuntimeControl::ready();
    let gate = runtime.session.install_run_gate(Ok(ready_result()));
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::RecordMergePreflightResult),
            count: 1,
        }])
        .expect("valid prepared-timeout persistence pause"),
    );
    let fixture = live_fixture(runtime, Some(controller.clone())).await;
    let task = approved_task(&fixture.base).await;
    let task_id = task.id;
    let repository_id = task.repository_id;
    let caller = tokio::spawn({
        let manager = fixture.manager.clone();
        async move { manager.preflight(preflight_request(task_id)).await }
    });

    gate.wait_until_reached().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(11 * 60 + 1)).await;
    tokio::time::resume();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;

    let snapshot = fixture
        .base
        .store
        .delivery_eligibility_snapshot(task_id)
        .await
        .expect("load prepared timeout snapshot")
        .expect("prepared timeout task exists");
    let operation = snapshot
        .ownership
        .merge_operations
        .first()
        .expect("prepared timeout operation exists");
    assert_eq!(operation.state, MergeOperationState::PreflightPending);
    assert_eq!(operation.version.get(), 2);
    assert!(matches!(
        fixture
            .base
            .store
            .record_merge_preflight_result(
                RecordMergePreflightResultRequest::try_new(
                    task_id,
                    operation.operation_id,
                    operation.version,
                    ready_result(),
                )
                .expect("valid competing prepared result"),
            )
            .await
            .expect("write competing prepared result"),
        coding_agent_store::MergeTransitionOutcome::Applied(_)
    ));
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    assert_eq!(
        caller
            .await
            .expect("join prepared-timeout caller")
            .expect("prepared-timeout manager remains open"),
        DeliveryPreflightOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::RepositoryControlUnavailable
        )
    );
    assert_preflight_control_and_workers(
        &fixture,
        repository_id,
        RepositoryControlState::Busy,
        1,
        "prepared timeout persistence invariant conflict",
    )
    .await;
}

#[tokio::test]
async fn unbound_runtime_timeout_keeps_retention_after_persist_invariant_conflict() {
    let runtime = RuntimeControl::ready();
    let gate = runtime.session.install_prepare_gate();
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::FailUnboundMergePreflight),
            count: 1,
        }])
        .expect("valid unbound-timeout persistence pause"),
    );
    let fixture = live_fixture(runtime, Some(controller.clone())).await;
    let task = approved_task(&fixture.base).await;
    let task_id = task.id;
    let repository_id = task.repository_id;
    let caller = tokio::spawn({
        let manager = fixture.manager.clone();
        async move { manager.preflight(preflight_request(task_id)).await }
    });

    gate.wait_until_reached().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(11 * 60 + 1)).await;
    tokio::time::resume();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;

    let snapshot = fixture
        .base
        .store
        .delivery_eligibility_snapshot(task_id)
        .await
        .expect("load unbound timeout snapshot")
        .expect("unbound timeout task exists");
    let operation = snapshot
        .ownership
        .merge_operations
        .first()
        .expect("unbound timeout operation exists");
    assert_eq!(operation.state, MergeOperationState::PreflightPending);
    assert_eq!(operation.version, DeliveryVersion::initial());
    assert!(matches!(
        fixture
            .base
            .store
            .fail_unbound_merge_preflight(
                FailUnboundMergePreflightRequest::try_new(
                    task_id,
                    operation.operation_id,
                    operation.version,
                    UnboundMergePreflightFailure::Rejected(
                        PreflightRejectedReason::TargetWorktreeDirty,
                    ),
                )
                .expect("valid competing unbound failure"),
            )
            .await
            .expect("write competing unbound failure"),
        coding_agent_store::MergeTransitionOutcome::Applied(_)
    ));
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    assert_eq!(
        caller
            .await
            .expect("join unbound-timeout caller")
            .expect("unbound-timeout manager remains open"),
        DeliveryPreflightOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::RepositoryControlUnavailable
        )
    );
    assert_preflight_control_and_workers(
        &fixture,
        repository_id,
        RepositoryControlState::Busy,
        1,
        "unbound timeout persistence invariant conflict",
    )
    .await;
}

#[tokio::test]
async fn query_and_live_preflight_reach_ready_then_receipt_replays_while_quiesced() {
    let runtime = RuntimeControl::ready();
    let fixture = live_fixture(runtime.clone(), None).await;
    let task = approved_task(&fixture.base).await;

    let before_query = fixture
        .base
        .store
        .delivery_eligibility_snapshot(task.id)
        .await
        .expect("read pre-query snapshot")
        .expect("delivery task exists");
    let projection = found_projection(
        fixture
            .manager
            .query(task.id)
            .await
            .expect("query delivery task"),
    );
    assert_eq!(projection.eligibility(), DeliveryEligibility::Eligible);
    assert_eq!(
        projection.allowed_actions(),
        &[DeliveryAllowedAction::RunPreflight]
    );
    assert!(matches!(
        projection.target(),
        DeliveryTargetObservation::Available { branch, head }
            if branch.as_str() == "refs/heads/main" && head.as_str() == TARGET_HEAD
    ));
    assert_eq!(
        fixture
            .base
            .store
            .delivery_eligibility_snapshot(task.id)
            .await
            .expect("read post-query snapshot")
            .expect("delivery task exists"),
        before_query,
        "task GET must not mutate durable delivery state"
    );
    assert_eq!(runtime.session.observations.load(Ordering::SeqCst), 1);

    let request = preflight_request(task.id);
    assert_eq!(
        durable_state(
            fixture
                .manager
                .preflight(request.clone())
                .await
                .expect("run live preflight"),
        ),
        (
            DeliveryPreflightDurability::Created,
            DeliveryPreflightState::PreflightReady,
        )
    );
    let snapshot = fixture
        .base
        .store
        .delivery_eligibility_snapshot(task.id)
        .await
        .expect("read delivery snapshot")
        .expect("delivery task exists");
    assert_eq!(snapshot.ownership.merge_operations.len(), 1);
    assert_eq!(
        snapshot.ownership.merge_operations[0].state,
        MergeOperationState::PreflightReady
    );
    assert!(
        snapshot.ownership.merge_operations[0]
            .preflight_inputs
            .is_some()
    );
    let ready_projection = found_projection(
        fixture
            .manager
            .query(task.id)
            .await
            .expect("query ready delivery task"),
    );
    assert!(matches!(
        ready_projection.latest_operation(),
        Some(DeliveryOperationProjection::Merge {
            state: DeliveryPreflightState::PreflightReady,
            ..
        })
    ));
    assert_eq!(
        ready_projection.allowed_actions(),
        &[DeliveryAllowedAction::AcceptMerge]
    );
    assert_eq!(
        fixture
            .base
            .store
            .delivery_eligibility_snapshot(task.id)
            .await
            .expect("read post-ready-query snapshot")
            .expect("delivery task exists"),
        snapshot,
        "ready task GET must be read-only"
    );

    fixture.manager.quiesce().await.expect("quiesce manager");
    assert_eq!(
        durable_state(
            fixture
                .manager
                .preflight(request)
                .await
                .expect("replay accepted preflight while quiesced"),
        ),
        (
            DeliveryPreflightDurability::Existing,
            DeliveryPreflightState::PreflightReady,
        )
    );
    assert_eq!(runtime.session.authentications.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.session.preparations.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.session.preflights.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn task_query_preserves_typed_target_unavailability_without_writing() {
    let runtime = RuntimeControl::ready();
    runtime
        .session
        .push_observation(Ok(DeliveryRuntimeObservation::unavailable_for_test(
            DeliveryRuntimeObservationUnavailableReason::TargetWorktreeDirty,
        )));
    let fixture = live_fixture(runtime, None).await;
    let task = approved_task(&fixture.base).await;
    let before = fixture
        .base
        .store
        .delivery_eligibility_snapshot(task.id)
        .await
        .expect("read target-unavailable baseline")
        .expect("delivery task exists");

    let projection = found_projection(
        fixture
            .manager
            .query(task.id)
            .await
            .expect("query target-unavailable task"),
    );
    assert_eq!(projection.eligibility(), DeliveryEligibility::Ineligible);
    assert_eq!(
        projection.reasons(),
        &[coding_agent_app::DeliveryEligibilityReason::TargetWorktreeDirty]
    );
    assert_eq!(
        projection.target(),
        &DeliveryTargetObservation::Unavailable {
            reason: DeliveryTargetUnavailableReason::TargetWorktreeDirty,
        }
    );
    assert!(projection.allowed_actions().is_empty());
    assert_eq!(
        fixture
            .base
            .store
            .delivery_eligibility_snapshot(task.id)
            .await
            .expect("read target-unavailable post-query snapshot")
            .expect("delivery task exists"),
        before,
        "an unavailable target observation must remain read-only"
    );
}

#[tokio::test]
async fn fresh_quiesce_generation_closes_intake_before_create() {
    let runtime = RuntimeControl::ready();
    let authentication_gate = runtime.session.install_authentication_gate();
    let fixture = live_fixture(runtime.clone(), None).await;
    let task = approved_task(&fixture.base).await;
    let request = preflight_request(task.id);
    let worker = tokio::spawn({
        let manager = fixture.manager.clone();
        let request = request.clone();
        async move { manager.preflight(request).await }
    });
    authentication_gate.wait_until_reached().await;

    fixture
        .service_state
        .set(ServiceState::Quiescing)
        .expect("quiesce service during pre-create observation");
    authentication_gate.release();

    assert_eq!(
        worker
            .await
            .expect("preflight worker does not panic")
            .expect("delivery manager remains open"),
        DeliveryPreflightOutcome::Unavailable(DeliveryPreflightUnavailableReason::ManagerQuiescing)
    );
    assert!(matches!(
        fixture
            .base
            .store
            .lookup_delivery_command(&DeliveryCommand::Preflight(request.into_command()))
            .await
            .expect("lookup quiesce-raced command"),
        DeliveryCommandLookup::Missing
    ));
    assert_eq!(runtime.session.preparations.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.session.preflights.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture
            .coordinator
            .control_state(task.repository_id)
            .expect("inspect repository after quiesce race"),
        RepositoryControlState::Available
    );
}

#[tokio::test]
async fn repository_busy_has_zero_receipt_and_zero_runtime_side_effects() {
    let runtime = RuntimeControl::ready();
    let fixture = live_fixture(runtime.clone(), None).await;
    let task = approved_task(&fixture.base).await;
    let request = preflight_request(task.id);
    let key = fixture
        .coordinator
        .coordination_key(task.repository_id)
        .expect("registered repository key");
    let lease = fixture
        .coordinator
        .try_acquire(key)
        .expect("hold repository lease");

    assert_eq!(
        fixture
            .manager
            .preflight(request.clone())
            .await
            .expect("busy result"),
        DeliveryPreflightOutcome::Busy(
            coding_agent_app::DeliveryPreflightBusyReason::RepositoryBusy
        )
    );
    assert!(matches!(
        fixture
            .base
            .store
            .lookup_delivery_command(&DeliveryCommand::Preflight(request.command().clone()))
            .await
            .expect("lookup missing command"),
        DeliveryCommandLookup::Missing
    ));
    assert_eq!(runtime.opens.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.session.authentications.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.session.preparations.load(Ordering::SeqCst), 0);
    lease
        .clean_release()
        .expect("release held repository lease");
    assert_eq!(
        fixture
            .coordinator
            .control_state(task.repository_id)
            .expect("inspect repository control"),
        RepositoryControlState::Available
    );
}

#[tokio::test]
async fn foreign_runtime_repository_authority_writes_nothing_and_poisons_the_owned_repository() {
    let runtime = RuntimeControl::ready();
    let fixture = live_fixture_with_repository_count(runtime.clone(), None, 2).await;
    let task = approved_task_for_repository(&fixture.base, &fixture.repositories[0]).await;
    let foreign_key = fixture
        .coordinator
        .coordination_key(fixture.repositories[1].id)
        .expect("foreign repository key");
    runtime.override_authority_key(foreign_key);
    let request = preflight_request(task.id);

    assert_eq!(
        fixture
            .manager
            .preflight(request.clone())
            .await
            .expect("foreign authority completion"),
        DeliveryPreflightOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::RepositoryControlUnavailable
        )
    );
    assert!(matches!(
        fixture
            .base
            .store
            .lookup_delivery_command(&DeliveryCommand::Preflight(request.command().clone()))
            .await
            .expect("lookup foreign-authority command"),
        DeliveryCommandLookup::Missing
    ));
    assert_eq!(runtime.session.preparations.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture
            .coordinator
            .control_state(task.repository_id)
            .expect("inspect poisoned repository control"),
        RepositoryControlState::Poisoned
    );
}

#[tokio::test]
async fn reconciliation_terminal_releases_the_owner_but_keeps_sticky_poison() {
    let runtime = RuntimeControl::ready();
    runtime
        .session
        .push_preflight(Err(DeliveryRuntimeFailure::ReconciliationRequired(
            MergeReconciliationReason::DeliveryStateInconsistent,
        )));
    let fixture = live_fixture(runtime, None).await;
    let task = approved_task(&fixture.base).await;

    assert_eq!(
        durable_state(
            fixture
                .manager
                .preflight(preflight_request(task.id))
                .await
                .expect("persist reconciliation terminal"),
        )
        .1,
        DeliveryPreflightState::ReconciliationRequired
    );
    assert_eq!(
        fixture
            .coordinator
            .control_state(task.repository_id)
            .expect("inspect sticky reconciliation poison"),
        RepositoryControlState::Poisoned
    );
}

#[tokio::test]
async fn poisoned_repository_does_not_block_unrelated_delivery_preflight() {
    let runtime = RuntimeControl::ready();
    runtime
        .session
        .push_preflight(Err(DeliveryRuntimeFailure::ReconciliationRequired(
            MergeReconciliationReason::DeliveryStateInconsistent,
        )));
    let fixture = live_fixture_with_repository_count(runtime.clone(), None, 2).await;
    let poisoned_task = approved_task_for_repository(&fixture.base, &fixture.repositories[0]).await;
    let available_task =
        approved_task_for_repository(&fixture.base, &fixture.repositories[1]).await;
    let poisoned_request = preflight_request(poisoned_task.id);
    let poisoned_command = poisoned_request.command().clone();

    let poisoned_operation = created_preflight_operation(
        fixture
            .manager
            .preflight(poisoned_request)
            .await
            .expect("persist repository-local reconciliation terminal"),
        DeliveryPreflightState::ReconciliationRequired,
    );
    assert_eq!(runtime.opens.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.session.authentications.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.session.preparations.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.session.preflights.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .coordinator
            .control_state(poisoned_task.repository_id)
            .expect("inspect poisoned repository control"),
        RepositoryControlState::Poisoned
    );
    assert_eq!(
        fixture
            .coordinator
            .control_state(available_task.repository_id)
            .expect("inspect unrelated repository control"),
        RepositoryControlState::Available
    );
    assert!(!fixture.coordinator.delivery_mutations_frozen());
    assert_exact_preflight_receipt(
        &fixture,
        &poisoned_task,
        &poisoned_command,
        poisoned_operation.operation_id(),
    )
    .await;
    assert_exact_preflight_operation(
        &fixture,
        &poisoned_task,
        &poisoned_command,
        poisoned_operation.operation_id(),
        MergeOperationState::ReconciliationRequired,
    )
    .await;

    let available_request = preflight_request(available_task.id);
    let available_command = available_request.command().clone();
    let available_operation = created_preflight_operation(
        fixture
            .manager
            .preflight(available_request)
            .await
            .expect("run unrelated repository preflight"),
        DeliveryPreflightState::PreflightReady,
    );
    assert_eq!(runtime.opens.load(Ordering::SeqCst), 2);
    assert_eq!(runtime.session.authentications.load(Ordering::SeqCst), 2);
    assert_eq!(runtime.session.preparations.load(Ordering::SeqCst), 2);
    assert_eq!(runtime.session.preflights.load(Ordering::SeqCst), 2);
    assert_eq!(
        fixture
            .coordinator
            .control_state(poisoned_task.repository_id)
            .expect("reinspect poisoned repository control"),
        RepositoryControlState::Poisoned
    );
    assert_eq!(
        fixture
            .coordinator
            .control_state(available_task.repository_id)
            .expect("inspect released unrelated repository control"),
        RepositoryControlState::Available
    );
    assert_exact_preflight_receipt(
        &fixture,
        &available_task,
        &available_command,
        available_operation.operation_id(),
    )
    .await;
    assert_exact_preflight_operation(
        &fixture,
        &available_task,
        &available_command,
        available_operation.operation_id(),
        MergeOperationState::PreflightReady,
    )
    .await;
}

#[tokio::test]
async fn authenticated_unbound_reconciliation_terminal_also_keeps_sticky_poison() {
    let runtime = RuntimeControl::ready();
    runtime
        .session
        .push_authentication_failure(DeliveryRuntimeFailure::ReconciliationRequired(
            MergeReconciliationReason::DeliveryStateInconsistent,
        ));
    let fixture = live_fixture(runtime.clone(), None).await;
    let task = approved_task(&fixture.base).await;

    assert_eq!(
        durable_state(
            fixture
                .manager
                .preflight(preflight_request(task.id))
                .await
                .expect("persist unbound reconciliation terminal"),
        )
        .1,
        DeliveryPreflightState::ReconciliationRequired
    );
    assert_eq!(runtime.session.preparations.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture
            .coordinator
            .control_state(task.repository_id)
            .expect("inspect unbound sticky reconciliation poison"),
        RepositoryControlState::Poisoned
    );
}

#[tokio::test]
async fn process_cleanup_unproven_terminal_retains_repository_ownership() {
    let runtime = RuntimeControl::ready();
    runtime
        .session
        .push_preflight(Err(DeliveryRuntimeFailure::ProcessCleanupUnproven));
    let fixture = live_fixture(runtime, None).await;
    let task = approved_task(&fixture.base).await;

    assert_eq!(
        durable_state(
            fixture
                .manager
                .preflight(preflight_request(task.id))
                .await
                .expect("persist cleanup-unproven terminal"),
        )
        .1,
        DeliveryPreflightState::ReconciliationRequired
    );
    assert_eq!(
        fixture
            .coordinator
            .control_state(task.repository_id)
            .expect("inspect retained repository owner"),
        RepositoryControlState::Busy
    );
}

#[tokio::test]
async fn known_not_applied_prepared_reconciliation_poisons_and_releases_worker() {
    let runtime = RuntimeControl::ready();
    for _ in 0..3 {
        runtime
            .session
            .push_preflight(Err(DeliveryRuntimeFailure::ReconciliationRequired(
                MergeReconciliationReason::DeliveryStateInconsistent,
            )));
    }
    let controller = busy_controller(StoreWriterOperationKind::RecordMergePreflightResult, 18);
    let fixture = live_fixture(runtime, Some(controller.clone())).await;
    let task = approved_task(&fixture.base).await;

    let outcome = fixture
        .manager
        .preflight(preflight_request(task.id))
        .await
        .expect("return typed retry after a proven-not-applied reconciliation write");

    assert_eq!(
        outcome,
        DeliveryPreflightOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::RepositoryControlUnavailable
        )
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::BusyBeforeExecute,
            StoreWriterOperationKind::RecordMergePreflightResult,
        ),
        6
    );
    assert_preflight_control_and_workers(
        &fixture,
        task.repository_id,
        RepositoryControlState::Poisoned,
        0,
        "prepared reconciliation",
    )
    .await;
}

#[tokio::test]
async fn known_not_applied_unbound_reconciliation_poisons_and_releases_worker() {
    let runtime = RuntimeControl::ready();
    for _ in 0..3 {
        runtime.session.push_authentication_failure(
            DeliveryRuntimeFailure::ReconciliationRequired(
                MergeReconciliationReason::DeliveryStateInconsistent,
            ),
        );
    }
    let controller = busy_controller(StoreWriterOperationKind::FailUnboundMergePreflight, 18);
    let fixture = live_fixture(runtime, Some(controller.clone())).await;
    let task = approved_task(&fixture.base).await;

    let outcome = fixture
        .manager
        .preflight(preflight_request(task.id))
        .await
        .expect("return typed retry after a proven-not-applied unbound reconciliation write");

    assert_eq!(
        outcome,
        DeliveryPreflightOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::RepositoryControlUnavailable
        )
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::BusyBeforeExecute,
            StoreWriterOperationKind::FailUnboundMergePreflight,
        ),
        6
    );
    assert_preflight_control_and_workers(
        &fixture,
        task.repository_id,
        RepositoryControlState::Poisoned,
        0,
        "unbound reconciliation",
    )
    .await;
}

#[tokio::test]
async fn known_not_applied_unbound_unavailable_poisons_and_releases_worker() {
    let runtime = RuntimeControl::ready();
    for _ in 0..3 {
        runtime
            .session
            .push_authentication_failure(DeliveryRuntimeFailure::Unavailable);
    }
    let controller = busy_controller(StoreWriterOperationKind::FailUnboundMergePreflight, 18);
    let fixture = live_fixture(runtime, Some(controller)).await;
    let task = approved_task(&fixture.base).await;

    assert_eq!(
        fixture
            .manager
            .preflight(preflight_request(task.id))
            .await
            .expect("return fail-closed unavailable outcome"),
        DeliveryPreflightOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::RepositoryControlUnavailable
        )
    );
    assert_preflight_control_and_workers(
        &fixture,
        task.repository_id,
        RepositoryControlState::Poisoned,
        0,
        "unbound unavailable",
    )
    .await;
}

#[tokio::test]
async fn known_not_applied_prepared_cleanup_unproven_retains_worker() {
    let runtime = RuntimeControl::ready();
    for _ in 0..3 {
        runtime
            .session
            .push_preflight(Err(DeliveryRuntimeFailure::ProcessCleanupUnproven));
    }
    let controller = busy_controller(StoreWriterOperationKind::RecordMergePreflightResult, 18);
    let fixture = live_fixture(runtime, Some(controller)).await;
    let task = approved_task(&fixture.base).await;

    assert!(matches!(
        fixture
            .manager
            .preflight(preflight_request(task.id))
            .await
            .expect("return typed retry after a cleanup-unproven write"),
        DeliveryPreflightOutcome::KnownNotAppliedPersisted(_)
    ));
    assert_preflight_control_and_workers(
        &fixture,
        task.repository_id,
        RepositoryControlState::Busy,
        1,
        "prepared cleanup-unproven",
    )
    .await;
}

#[tokio::test]
async fn known_not_applied_unbound_cleanup_unproven_retains_worker() {
    let runtime = RuntimeControl::ready();
    for _ in 0..3 {
        runtime
            .session
            .push_authentication_failure(DeliveryRuntimeFailure::ProcessCleanupUnproven);
    }
    let controller = busy_controller(StoreWriterOperationKind::FailUnboundMergePreflight, 18);
    let fixture = live_fixture(runtime, Some(controller)).await;
    let task = approved_task(&fixture.base).await;

    assert!(matches!(
        fixture
            .manager
            .preflight(preflight_request(task.id))
            .await
            .expect("return typed retry after an unbound cleanup-unproven write"),
        DeliveryPreflightOutcome::KnownNotAppliedPersisted(_)
    ));
    assert_preflight_control_and_workers(
        &fixture,
        task.repository_id,
        RepositoryControlState::Busy,
        1,
        "unbound cleanup-unproven",
    )
    .await;
}

#[tokio::test]
async fn saturated_preflight_capacity_does_not_starve_the_query_lane() {
    let runtime = RuntimeControl::ready();
    let gate = runtime.session.install_run_gate(Ok(ready_result()));
    let fixture = live_fixture_with_repository_count_and_capacity(runtime, None, 1, 1).await;
    let task = approved_task(&fixture.base).await;
    let task_id = task.id;
    let worker = tokio::spawn({
        let manager = fixture.manager.clone();
        async move { manager.preflight(preflight_request(task_id)).await }
    });
    gate.wait_until_reached().await;

    assert!(matches!(
        timeout(Duration::from_secs(5), fixture.manager.query(task_id))
            .await
            .expect("query is not starved by the saturated preflight lane")
            .expect("delivery manager remains open"),
        DeliveryTaskQueryOutcome::Found { .. }
    ));

    gate.release();
    assert_eq!(
        durable_state(
            worker
                .await
                .expect("preflight worker does not panic")
                .expect("delivery manager remains open")
        )
        .1,
        DeliveryPreflightState::PreflightReady
    );
}

#[tokio::test]
async fn global_cap_allows_two_repositories_and_keeps_query_mailbox_responsive() {
    let runtime = RuntimeControl::ready();
    let gate = runtime.session.install_parallel_run_gate();
    let fixture = live_fixture_with_repository_count(runtime.clone(), None, 3).await;
    let mut tasks = Vec::new();
    for repository in &fixture.repositories {
        tasks.push(approved_task_for_repository(&fixture.base, repository).await);
    }

    let workers = tasks
        .iter()
        .map(|task| {
            let manager = fixture.manager.clone();
            let request = preflight_request(task.id);
            tokio::spawn(async move { manager.preflight(request).await })
        })
        .collect::<Vec<_>>();

    gate.wait_until_reached(2).await;
    assert_eq!(runtime.session.preflights.load(Ordering::SeqCst), 2);
    let _projection = timeout(Duration::from_secs(5), fixture.manager.query(tasks[0].id))
        .await
        .expect("query responds while two Git workers are blocked")
        .expect("delivery manager remains open");

    gate.release(1);
    gate.wait_until_reached(1).await;
    assert_eq!(runtime.session.preflights.load(Ordering::SeqCst), 3);
    assert_eq!(gate.peak(), 2);
    gate.release(2);

    for worker in workers {
        let outcome = worker
            .await
            .expect("preflight worker does not panic")
            .expect("delivery manager remains open");
        assert_eq!(
            durable_state(outcome).1,
            DeliveryPreflightState::PreflightReady
        );
    }
}

#[tokio::test]
async fn conflict_is_persisted_through_pending_bind_and_terminal_result() {
    let runtime = RuntimeControl::ready();
    runtime.session.push_preflight(Ok(conflict_result()));
    let fixture = live_fixture(runtime, None).await;
    let task = approved_task(&fixture.base).await;

    assert_eq!(
        durable_state(
            fixture
                .manager
                .preflight(preflight_request(task.id))
                .await
                .expect("run conflicting preflight"),
        )
        .1,
        DeliveryPreflightState::Conflict
    );
    let snapshot = fixture
        .base
        .store
        .delivery_eligibility_snapshot(task.id)
        .await
        .expect("read conflict snapshot")
        .expect("task exists");
    let operation = &snapshot.ownership.merge_operations[0];
    assert_eq!(operation.state, MergeOperationState::Conflict);
    assert!(operation.preflight_inputs.is_some());
    assert_eq!(operation.conflicts.len(), 1);
    let projection = found_projection(
        fixture
            .manager
            .query(task.id)
            .await
            .expect("query conflict delivery task"),
    );
    assert!(matches!(
        projection.latest_operation(),
        Some(DeliveryOperationProjection::Merge {
            state: DeliveryPreflightState::Conflict,
            ..
        })
    ));
    assert_eq!(
        projection.allowed_actions(),
        &[DeliveryAllowedAction::RunPreflight]
    );
    assert_eq!(
        fixture
            .base
            .store
            .delivery_eligibility_snapshot(task.id)
            .await
            .expect("read post-conflict-query snapshot")
            .expect("task exists"),
        snapshot,
        "conflict task GET must be read-only"
    );
}

#[tokio::test]
async fn existing_receipt_whose_projected_operation_is_missing_fails_closed_without_writing() {
    let runtime = RuntimeControl::ready();
    runtime.session.push_preflight(Ok(conflict_result()));
    runtime.session.push_preflight(Ok(conflict_result()));
    let fixture = live_fixture(runtime.clone(), None).await;
    let task = approved_task(&fixture.base).await;
    let first_request = preflight_request(task.id);
    let first_operation_id = match fixture
        .manager
        .preflight(first_request.clone())
        .await
        .expect("persist first terminal preflight")
    {
        DeliveryPreflightOutcome::Durable(operation) => operation.operation_id(),
        other => panic!("expected first durable terminal, got {other:?}"),
    };
    let second_request = preflight_request(task.id);
    assert_eq!(
        durable_state(
            fixture
                .manager
                .preflight(second_request)
                .await
                .expect("persist later terminal preflight")
        )
        .1,
        DeliveryPreflightState::Conflict
    );
    let before_replay = fixture
        .base
        .store
        .delivery_eligibility_snapshot(task.id)
        .await
        .expect("read latest terminal snapshot")
        .expect("delivery task exists");
    assert!(
        before_replay
            .ownership
            .merge_operations
            .iter()
            .all(|operation| operation.operation_id != first_operation_id),
        "the task projection intentionally omits the older terminal operation"
    );
    let authentications_before = runtime.session.authentications.load(Ordering::SeqCst);

    assert_eq!(
        fixture
            .manager
            .preflight(first_request)
            .await
            .expect("fail closed while replaying an unprojected receipt"),
        DeliveryPreflightOutcome::Unavailable(
            DeliveryPreflightUnavailableReason::RepositoryControlUnavailable
        )
    );
    assert_eq!(
        fixture
            .base
            .store
            .delivery_eligibility_snapshot(task.id)
            .await
            .expect("read post-replay snapshot")
            .expect("delivery task exists"),
        before_replay,
        "a missing receipt operation must not be guessed or rewritten"
    );
    assert_eq!(
        runtime.session.authentications.load(Ordering::SeqCst),
        authentications_before,
        "receipt inconsistency is rejected before runtime authentication"
    );
    assert_eq!(
        fixture
            .coordinator
            .control_state(task.repository_id)
            .expect("inspect fail-closed repository"),
        RepositoryControlState::Poisoned
    );
}

#[tokio::test]
async fn create_reply_loss_reconciles_exact_receipt_and_finishes_ready_once() {
    let runtime = RuntimeControl::ready();
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            operation: Some(StoreWriterOperationKind::CreateMergePreflight),
            count: 2,
        }])
        .expect("valid StoreWriter fault plan"),
    );
    let fixture = live_fixture(runtime, Some(controller)).await;
    let task = approved_task(&fixture.base).await;

    let (durability, state) = durable_state(
        fixture
            .manager
            .preflight(preflight_request(task.id))
            .await
            .expect("reconcile lost create reply"),
    );
    assert_eq!(durability, DeliveryPreflightDurability::Existing);
    assert_eq!(state, DeliveryPreflightState::PreflightReady);
    let snapshot = fixture
        .base
        .store
        .delivery_eligibility_snapshot(task.id)
        .await
        .expect("read reconciled snapshot")
        .expect("task exists");
    assert_eq!(snapshot.ownership.merge_operations.len(), 1);
    assert_eq!(snapshot.ownership.merge_operations[0].version.get(), 3);
}

#[tokio::test]
async fn known_not_applied_bind_is_resumed_with_fresh_authority_and_finishes_ready() {
    let runtime = RuntimeControl::ready();
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::BusyBeforeExecute,
            operation: Some(StoreWriterOperationKind::BindMergePreflightInputs),
            // Exhaust StoreWriter's initial execution plus five bounded
            // internal busy retries so the caller observes KnownNotApplied.
            count: 6,
        }])
        .expect("valid StoreWriter fault plan"),
    );
    let fixture = live_fixture(runtime.clone(), Some(controller.clone())).await;
    let task = approved_task(&fixture.base).await;

    let outcome = fixture
        .manager
        .preflight(preflight_request(task.id))
        .await
        .expect("bounded pending resume completion");
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::BusyBeforeExecute,
            StoreWriterOperationKind::BindMergePreflightInputs,
        ),
        6,
        "the first bind must be proven not applied before manager-level resume"
    );
    assert_eq!(
        durable_state(outcome),
        (
            DeliveryPreflightDurability::Existing,
            DeliveryPreflightState::PreflightReady,
        )
    );

    let snapshot = fixture
        .base
        .store
        .delivery_eligibility_snapshot(task.id)
        .await
        .expect("read known-not-applied pending snapshot")
        .expect("task exists");
    let operation = &snapshot.ownership.merge_operations[0];
    assert_eq!(operation.state, MergeOperationState::PreflightReady);
    assert_eq!(operation.version.get(), 3);
    assert!(operation.preflight_inputs.is_some());
    assert_eq!(runtime.session.authentications.load(Ordering::SeqCst), 2);
    assert_eq!(runtime.session.preparations.load(Ordering::SeqCst), 2);
    assert_eq!(runtime.session.preflights.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .coordinator
            .control_state(task.repository_id)
            .expect("inspect released repository control"),
        RepositoryControlState::Available
    );
}

#[tokio::test]
async fn durable_pending_recovery_bypasses_closed_user_intake_and_finishes() {
    let runtime = RuntimeControl::ready();
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::BusyBeforeExecute,
            operation: Some(StoreWriterOperationKind::BindMergePreflightInputs),
            // Three manager attempts, each beyond StoreWriter's six-call busy
            // envelope, leave one durable v1 pending intent.
            count: 18,
        }])
        .expect("valid StoreWriter fault plan"),
    );
    let fixture = live_fixture(runtime, Some(controller.clone())).await;
    let task = approved_task(&fixture.base).await;
    let request = preflight_request(task.id);

    let first = fixture
        .manager
        .preflight(request.clone())
        .await
        .expect("return bounded retry advice");
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::BusyBeforeExecute,
            StoreWriterOperationKind::BindMergePreflightInputs,
        ),
        18,
        "all three manager attempts must remain known-not-applied"
    );
    assert!(matches!(
        first,
        DeliveryPreflightOutcome::KnownNotAppliedPersisted(retry)
            if retry.operation().state() == DeliveryPreflightState::PreflightPending
    ));
    assert_eq!(
        fixture
            .coordinator
            .control_state(task.repository_id)
            .expect("inspect early known-not-applied release"),
        RepositoryControlState::Available,
        "a pre-side-effect bind KNA must release repository ownership"
    );
    fixture.manager.quiesce().await.expect("close user intake");
    assert_eq!(
        durable_state(
            fixture
                .manager
                .preflight(request)
                .await
                .expect("resume durable pending work while quiesced")
        ),
        (
            DeliveryPreflightDurability::Existing,
            DeliveryPreflightState::PreflightReady,
        )
    );
}

fn busy_controller(
    operation: StoreWriterOperationKind,
    count: u32,
) -> Arc<StoreWriterTestController> {
    Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::BusyBeforeExecute,
            operation: Some(operation),
            count,
        }])
        .expect("valid preflight known-not-applied StoreWriter script"),
    )
}

async fn assert_preflight_control_and_workers(
    fixture: &LiveFixture,
    repository_id: coding_agent_domain::RepositoryId,
    expected_control: RepositoryControlState,
    expected_workers: usize,
    stage: &str,
) {
    assert_eq!(
        fixture
            .coordinator
            .control_state(repository_id)
            .unwrap_or_else(|_| panic!("inspect {stage} repository control")),
        expected_control,
        "unexpected repository control state after {stage}"
    );
    assert_eq!(
        fixture
            .manager
            .quiesce()
            .await
            .unwrap_or_else(|_| panic!("quiesce {stage} manager"))
            .in_flight_workers(),
        expected_workers,
        "unexpected retained worker count after {stage}"
    );
}

#[tokio::test]
async fn prepared_pending_resume_reprepares_exact_ids_before_rerunning() {
    let runtime = RuntimeControl::ready();
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::BusyBeforeExecute,
            operation: Some(StoreWriterOperationKind::RecordMergePreflightResult),
            count: 6,
        }])
        .expect("valid StoreWriter fault plan"),
    );
    let fixture = live_fixture(runtime.clone(), Some(controller.clone())).await;
    let task = approved_task(&fixture.base).await;

    assert_eq!(
        durable_state(
            fixture
                .manager
                .preflight(preflight_request(task.id))
                .await
                .expect("resume prepared pending"),
        ),
        (
            DeliveryPreflightDurability::Existing,
            DeliveryPreflightState::PreflightReady,
        )
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::BusyBeforeExecute,
            StoreWriterOperationKind::RecordMergePreflightResult,
        ),
        6,
        "the first v2 result write must be known not applied before resume"
    );
    assert_eq!(runtime.session.authentications.load(Ordering::SeqCst), 2);
    assert_eq!(runtime.session.preparations.load(Ordering::SeqCst), 2);
    assert_eq!(runtime.session.preflights.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn two_unknown_writes_retain_both_global_slots_and_block_a_third_repository() {
    let runtime = RuntimeControl::ready();
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            operation: Some(StoreWriterOperationKind::CreateMergePreflight),
            // One manager exact-write budget is four submissions. Each typed
            // StoreWriter submission performs the initial write plus one
            // query-first reconciliation after reply loss, so retaining two
            // unknown workers requires 2 * 4 * 2 injected losses.
            count: 16,
        }])
        .expect("valid StoreWriter fault plan"),
    );
    let fixture =
        live_fixture_with_repository_count(runtime.clone(), Some(controller.clone()), 3).await;
    let mut tasks = Vec::new();
    for repository in &fixture.repositories {
        tasks.push(approved_task_for_repository(&fixture.base, repository).await);
    }

    for (index, task) in tasks[..2].iter().enumerate() {
        assert_eq!(
            fixture
                .manager
                .preflight(preflight_request(task.id))
                .await
                .expect("unknown write completion"),
            DeliveryPreflightOutcome::Unavailable(
                DeliveryPreflightUnavailableReason::OutcomeUnknown
            )
        );
        assert_eq!(
            controller.hit_count(
                StoreWriterFaultPoint::FailAfterCommitBeforeReply,
                StoreWriterOperationKind::CreateMergePreflight,
            ),
            u32::try_from((index + 1) * 8).expect("two retained workers fit u32"),
            "each retained unknown worker must exhaust its exact reconciliation budget"
        );
        assert_eq!(
            fixture
                .coordinator
                .control_state(task.repository_id)
                .expect("inspect retained unknown repository"),
            RepositoryControlState::Busy
        );
    }
    assert_eq!(runtime.opens.load(Ordering::SeqCst), 2);

    let third_request = preflight_request(tasks[2].id);
    let third = tokio::spawn({
        let manager = fixture.manager.clone();
        let request = third_request.clone();
        async move { manager.preflight(request).await }
    });
    assert!(
        timeout(Duration::from_millis(250), third).await.is_err(),
        "the third repository must not replace either retained global slot"
    );
    assert_eq!(runtime.opens.load(Ordering::SeqCst), 2);
    assert!(matches!(
        fixture
            .base
            .store
            .lookup_delivery_command(&DeliveryCommand::Preflight(third_request.into_command(),))
            .await
            .expect("lookup blocked third command"),
        DeliveryCommandLookup::Missing
    ));
}

#[tokio::test]
async fn caller_disconnect_does_not_cancel_bound_drift_terminalization() {
    let runtime = RuntimeControl::ready();
    let gate = runtime
        .session
        .install_run_gate(Err(DeliveryRuntimeFailure::Stale(
            PreflightStaleReason::TargetHeadChanged,
        )));
    let fixture = live_fixture(runtime, None).await;
    let task = approved_task(&fixture.base).await;
    let request = preflight_request(task.id);
    let caller = tokio::spawn({
        let manager = fixture.manager.clone();
        async move { manager.preflight(request).await }
    });
    gate.wait_until_reached().await;

    let pending = fixture
        .base
        .store
        .delivery_eligibility_snapshot(task.id)
        .await
        .expect("read bound pending snapshot")
        .expect("task exists");
    assert_eq!(
        pending.ownership.merge_operations[0].state,
        MergeOperationState::PreflightPending
    );
    assert!(
        pending.ownership.merge_operations[0]
            .preflight_inputs
            .is_some()
    );
    let pending_projection = found_projection(
        fixture
            .manager
            .query(task.id)
            .await
            .expect("query bound pending operation"),
    );
    assert!(matches!(
        pending_projection.latest_operation(),
        Some(DeliveryOperationProjection::Merge {
            state: DeliveryPreflightState::PreflightPending,
            ..
        })
    ));
    assert!(pending_projection.allowed_actions().is_empty());
    assert_eq!(
        fixture
            .base
            .store
            .delivery_eligibility_snapshot(task.id)
            .await
            .expect("read post-pending-query snapshot")
            .expect("task exists"),
        pending,
        "pending task GET must be read-only"
    );
    caller.abort();
    gate.release();

    timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = fixture
                .base
                .store
                .delivery_eligibility_snapshot(task.id)
                .await
                .expect("poll drift terminal")
                .expect("task exists");
            if snapshot.ownership.merge_operations[0].state == MergeOperationState::Stale {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("disconnected worker persists stale terminal");
    assert_eq!(
        fixture
            .coordinator
            .control_state(task.repository_id)
            .expect("inspect clean repository release"),
        RepositoryControlState::Available
    );
}
