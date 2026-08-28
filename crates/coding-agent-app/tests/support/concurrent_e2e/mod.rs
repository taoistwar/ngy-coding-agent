use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::Duration;

use coding_agent_app::{
    CodingAgentAttemptFactory, CodingAgentPreparationControl, CodingAgentRunner,
    CodingAgentRunnerConfig, EventDispatcherHandle, EventWake, PlatformPaths,
    Project2RuntimeSessionFactory, ProvisionedAgentRuntimeFactory, QuiesceResult,
    RepositoryControlCoordinator, RepositoryControlState, SchedulerConcurrencyLimits, ServiceState,
    ServiceStateController, StoreWriterHandle, SystemWallClock, TaskManagerHandle,
    TaskManagerLaunchResources, WorktreeCodingAgentAttemptFactory,
};
use coding_agent_domain::{NewRepository, Repository, Task, TaskId, TaskStatus};
use coding_agent_runtime::{ProcessLivenessScope, ToolchainPaths};
use coding_agent_store::{
    AcceptMergeCommandRequest, CleanupOperationRecord, DeliveryOperationId, MergeOperationRecord,
    RegisterRepositoryOutcome, RemoveWorktreeCommandRequest, Store, TaskAttemptArtifact,
    TaskDetail,
};
use tempfile::TempDir;
use tokio::time::Instant;

use delivery::ConcurrentDelivery;
pub use delivery::DeliverySideEffectSnapshot;
use observation::{ControlOperationTracker, ObservedAttemptFactory, ProvisionPauseController};
use provider::{RoleLoopBarrier, ScriptedProviderFactory};
use repository::{
    canonical, discover_e2e_toolchain, git_line, provisioner_factory, seed_repository,
};

mod delivery;
mod observation;
mod provider;
mod repository;

const PACKAGE: &str = "offline_fixture";
const INTEGRATION_TEST: &str = "answer";
const INITIAL_SOURCE: &str = "pub fn answer() -> u32 { 41 }\n";
const CHANGED_SOURCE: &str = "pub fn answer() -> u32 { 42 }\n// concurrent offline E2E task\n";
const COMMITTED_STAGED: &str = "committed staged bytes\n";
const COMMITTED_UNSTAGED: &str = "committed unstaged bytes\n";
const DIRTY_STAGED: &str = "dirty staged bytes\n";
const DIRTY_UNSTAGED: &str = "dirty unstaged bytes\n";
const DIRTY_UNTRACKED: &str = "dirty untracked bytes\n";
const PROCESS_COMMAND_TIMEOUT: Duration = Duration::from_secs(2 * 60);
const PROCESS_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const WORKTREE_ORCHESTRATION_TIMEOUT: Duration = Duration::from_secs(30);
// A lack of observable progress may legitimately span one maximum command,
// its process-tree cleanup, and one worktree orchestration window.
const E2E_NO_PROGRESS_TIMEOUT: Duration = Duration::from_secs(2 * 60 + 5 + 30);
// One scripted task has exactly one Planner batch, at most six Executor
// batches (including the required validation), and seven Reviewer batches
// (three reads, one reserved batch, manifest, chunks, and submission). Giving
// each stage one complete no-progress budget is the fixed scenario hard cap.
const SCRIPTED_PLANNER_STAGES: u64 = 1;
const SCRIPTED_EXECUTOR_STAGES: u64 = 6;
const SCRIPTED_REVIEWER_STAGES: u64 = 7;
const MAX_SCRIPTED_TASK_STAGES: u64 =
    SCRIPTED_PLANNER_STAGES + SCRIPTED_EXECUTOR_STAGES + SCRIPTED_REVIEWER_STAGES;
const E2E_SCENARIO_HARD_TIMEOUT: Duration =
    Duration::from_secs(MAX_SCRIPTED_TASK_STAGES * E2E_NO_PROGRESS_TIMEOUT.as_secs());
const STORE_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const FIXTURE_CLOSE_STEP_TIMEOUT: Duration = Duration::from_secs(30);
const E2E_POLL_INTERVAL: Duration = Duration::from_millis(10);
const PRODUCTION_DELIVERY_STAGE_TIMEOUT: Duration = Duration::from_secs(11 * 60);

struct TrackedDispatcherWake {
    dispatcher: EventDispatcherHandle,
    _actor_lifetime: Arc<()>,
}

impl EventWake for TrackedDispatcherWake {
    fn wake(&self) {
        self.dispatcher.wake();
    }
}

pub struct ConcurrentE2eFixture {
    repository_paths: Vec<PathBuf>,
    runtime_directory: PathBuf,
    artifact_root: PathBuf,
    toolchain: ToolchainPaths,
    base_commits: Vec<String>,
    store: Store,
    repositories: Vec<Repository>,
    writer: StoreWriterHandle,
    manager: TaskManagerHandle,
    delivery: Option<ConcurrentDelivery>,
    repository_control: Arc<RepositoryControlCoordinator>,
    instance_process_scope: ProcessLivenessScope,
    role_barrier: Arc<RoleLoopBarrier>,
    control_tracker: Arc<ControlOperationTracker>,
    provision_pause: Arc<ProvisionPauseController>,
    manager_actor_lifetime: Weak<ScriptedProviderFactory>,
    writer_actor_lifetime: Weak<()>,
    dispatcher: EventDispatcherHandle,
    _service_state: ServiceStateController,
    temp: TempDir,
}

impl ConcurrentE2eFixture {
    pub async fn new(
        global_concurrency: u32,
        per_repository_concurrency: u32,
        repository_count: usize,
        blocked_role_loops: usize,
    ) -> Self {
        assert!(repository_count > 0);
        assert!(blocked_role_loops > 0);
        let temporary = tempfile::Builder::new()
            .prefix("concurrent-offline-e2e-")
            .tempdir()
            .expect("create concurrent offline E2E fixture");
        let root = temporary
            .path()
            .canonicalize()
            .expect("canonical concurrent E2E root");
        let runtime_directory = root.join("runtime");
        let artifact_root = root.join("artifacts");
        PlatformPaths::new(root.join("data"), &runtime_directory)
            .prepare()
            .expect("create private E2E runtime directory");
        std::fs::create_dir_all(&artifact_root).expect("create E2E artifact directory");

        let mut repository_paths = Vec::with_capacity(repository_count);
        let mut base_commits = Vec::with_capacity(repository_count);
        for index in 0..repository_count {
            let path = root.join(format!("repository-{index}"));
            std::fs::create_dir_all(path.join("src")).expect("create repository source directory");
            std::fs::create_dir_all(path.join("tests")).expect("create repository test directory");
            seed_repository(&path);
            base_commits.push(git_line(&path, &["rev-parse", "HEAD"]));
            repository_paths.push(path);
        }

        let instance_process_scope = super::instance_process_scope(&runtime_directory);
        let toolchain =
            discover_e2e_toolchain(&runtime_directory, instance_process_scope.clone()).await;
        let store = Store::open(root.join("store.sqlite3"))
            .await
            .expect("open concurrent E2E store");
        store.migrate().await.expect("migrate concurrent E2E store");
        let mut repositories = Vec::with_capacity(repository_count);
        for (index, repository_path) in repository_paths.iter().enumerate() {
            let repository = match store
                .register_repository(NewRepository {
                    selected_path: canonical(repository_path),
                    display_name: format!("concurrent-offline-e2e-{index}"),
                    git_root: canonical(repository_path),
                    cargo_workspace_root: canonical(repository_path),
                })
                .await
                .expect("register concurrent E2E repository")
            {
                RegisterRepositoryOutcome::Created(repository)
                | RegisterRepositoryOutcome::Existing(repository) => repository,
            };
            repositories.push(repository);
        }

        let role_barrier = Arc::new(RoleLoopBarrier::new(blocked_role_loops));
        let providers = Arc::new(ScriptedProviderFactory::new(Arc::clone(&role_barrier)));
        let manager_actor_lifetime = Arc::downgrade(&providers);
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 1_024)
            .await
            .expect("spawn concurrent E2E dispatcher");
        let writer_actor_token = Arc::new(());
        let writer_actor_lifetime = Arc::downgrade(&writer_actor_token);
        let writer = StoreWriterHandle::spawn(
            store.clone(),
            Arc::new(TrackedDispatcherWake {
                dispatcher: dispatcher.clone(),
                _actor_lifetime: writer_actor_token,
            }),
            128,
        );
        let provisioners = provisioner_factory(
            toolchain.clone(),
            artifact_root.clone(),
            runtime_directory.clone(),
        );
        let runtimes: Arc<dyn ProvisionedAgentRuntimeFactory> =
            Arc::new(Project2RuntimeSessionFactory::project_2_defaults(
                toolchain.clone(),
                runtime_directory.clone(),
                NonZeroU32::MIN,
            ));
        let real_attempts: Arc<dyn CodingAgentAttemptFactory> = Arc::new(
            WorktreeCodingAgentAttemptFactory::new(provisioners, runtimes),
        );
        let control_tracker = Arc::new(ControlOperationTracker::default());
        let provision_pause = Arc::new(ProvisionPauseController::default());
        let attempts: Arc<dyn CodingAgentAttemptFactory> = Arc::new(ObservedAttemptFactory::new(
            real_attempts,
            Arc::clone(&control_tracker),
            Arc::clone(&provision_pause),
        ));
        let (repository_control, repository_identity_resolver) =
            super::repository_control_fixture(&store).await;
        let launch_resources = TaskManagerLaunchResources::new_for_test(
            SchedulerConcurrencyLimits::try_new(global_concurrency, per_repository_concurrency)
                .expect("valid concurrent E2E scheduler limits"),
            Arc::clone(&repository_control),
            instance_process_scope.clone(),
        );
        let runner = Arc::new(CodingAgentRunner::new(
            CodingAgentPreparationControl::new(
                store.clone(),
                writer.clone(),
                Arc::clone(&repository_control),
                repository_identity_resolver,
            ),
            providers,
            attempts,
            Arc::new(SystemWallClock),
            CodingAgentRunnerConfig::try_new(Duration::from_secs(10), Duration::from_millis(10))
                .expect("valid concurrent E2E runner config"),
        ));
        let service_state = ServiceStateController::new(ServiceState::Ready);
        let manager = TaskManagerHandle::spawn(
            store.clone(),
            writer.clone(),
            dispatcher.clone(),
            service_state.clone(),
            runner,
            launch_resources,
            128,
        );

        Self {
            repository_paths,
            runtime_directory,
            artifact_root,
            toolchain,
            base_commits,
            store,
            repositories,
            writer,
            manager,
            delivery: None,
            repository_control,
            instance_process_scope,
            role_barrier,
            control_tracker,
            provision_pause,
            manager_actor_lifetime,
            writer_actor_lifetime,
            dispatcher,
            _service_state: service_state,
            temp: temporary,
        }
    }

    pub async fn start_delivery_manager(&mut self) {
        assert!(self.delivery.is_none(), "delivery manager already started");
        self.delivery = Some(
            ConcurrentDelivery::start(
                self.store.clone(),
                self.writer.clone(),
                self.manager.clone(),
                Arc::clone(&self.repository_control),
                self.instance_process_scope.clone(),
                self.toolchain.clone(),
                self.artifact_root.clone(),
                self.runtime_directory.clone(),
                self._service_state.clone(),
            )
            .await,
        );
    }

    pub fn dirty_repository(&self, repository_index: usize) {
        let repository = self.repository_path(repository_index);
        std::fs::write(repository.join("staged.txt"), DIRTY_STAGED)
            .expect("write staged dirty sentinel");
        repository::git_ok(repository, &["add", "--", "staged.txt"]);
        std::fs::write(repository.join("unstaged.txt"), DIRTY_UNSTAGED)
            .expect("write unstaged dirty sentinel");
        std::fs::write(repository.join("untracked.txt"), DIRTY_UNTRACKED)
            .expect("write untracked dirty sentinel");
    }

    pub async fn enqueue_for_repository(
        &self,
        repository_index: usize,
        prompts: &[&str],
    ) -> Vec<Task> {
        let repository = self.repository(repository_index);
        let mut tasks = Vec::with_capacity(prompts.len());
        for prompt in prompts {
            let task = self
                .writer
                .create_task(super::new_task(repository.id, prompt), super::deadline())
                .await
                .expect("persist concurrent E2E task")
                .value
                .task()
                .clone();
            self.manager
                .notify_queued(task.id)
                .await
                .expect("notify concurrent E2E task");
            tasks.push(task);
        }
        tasks
    }

    pub async fn wait_for_blocked_role_loops(&self, expected: usize) {
        self.role_barrier.wait_for_entries(expected).await;
    }

    pub fn release_role_loops(&self) {
        self.role_barrier.release();
    }

    pub fn arm_next_provision_pause(&self) {
        self.provision_pause.arm_next();
    }

    pub async fn wait_for_provision_pause(&self) {
        tokio::time::timeout(
            E2E_NO_PROGRESS_TIMEOUT,
            self.provision_pause.wait_until_reached(),
        )
        .await
        .expect("reserved attempt did not reach the deterministic provision pause");
    }

    pub fn release_provision_pause(&self) {
        self.provision_pause.release();
    }

    pub async fn prepare_delivery_accept(
        &self,
        task_id: TaskId,
    ) -> (DeliveryOperationId, AcceptMergeCommandRequest) {
        self.delivery()
            .prepare_accept(&self.store, self.repository_path(0), task_id)
            .await
    }

    pub async fn accept_delivery_merge(
        &self,
        command: AcceptMergeCommandRequest,
    ) -> coding_agent_app::DeliveryMergeAcceptanceOutcome {
        self.delivery().accept_merge(command).await
    }

    pub async fn wait_for_delivery_merge(
        &self,
        operation_id: DeliveryOperationId,
    ) -> MergeOperationRecord {
        delivery::wait_for_merge(&self.store, operation_id).await
    }

    pub async fn delivery_remove_request(&self, task_id: TaskId) -> RemoveWorktreeCommandRequest {
        delivery::remove_request(&self.store, task_id).await
    }

    pub async fn remove_delivery_worktree(
        &self,
        request: RemoveWorktreeCommandRequest,
    ) -> coding_agent_app::DeliveryCleanupAcceptanceOutcome {
        self.delivery().remove_worktree(request).await
    }

    pub async fn wait_for_delivery_cleanup(
        &self,
        operation_id: DeliveryOperationId,
    ) -> CleanupOperationRecord {
        delivery::wait_for_cleanup(&self.store, operation_id).await
    }

    pub async fn delivery_side_effect_snapshot(
        &self,
        task_id: TaskId,
    ) -> DeliverySideEffectSnapshot {
        let artifact = self.artifact(task_id).await;
        delivery::snapshot(
            &self.store,
            self.repository_path(0),
            artifact.worktree_path.as_path(),
            &format!("refs/heads/{}", artifact.branch_name),
            task_id,
        )
        .await
    }

    pub async fn clean_delivery_runtime_outputs(&self, task_id: TaskId) {
        let artifact = self.artifact(task_id).await;
        repository::git_ok(
            artifact.worktree_path.as_path(),
            &[
                "-c",
                "core.longPaths=true",
                "clean",
                "-f",
                "-d",
                "-X",
                "--",
                "target",
            ],
        );
        assert!(
            repository::git_bytes(
                artifact.worktree_path.as_path(),
                &[
                    "--no-optional-locks",
                    "status",
                    "--porcelain=v2",
                    "--ignored=matching",
                    "--untracked-files=all",
                    "-z",
                ],
            )
            .is_empty(),
            "delivery source must be exactly clean before cleanup admission"
        );
    }

    pub async fn assert_exact_no_ff_delivery_merge(
        &self,
        task_id: TaskId,
        operation: &MergeOperationRecord,
    ) {
        let expected_merge = operation
            .expected_merge_commit
            .as_ref()
            .expect("merged operation has an expected merge commit");
        assert_eq!(
            repository::git_line(
                self.repository_path(0),
                &["rev-parse", operation.target_branch.as_str()],
            ),
            expected_merge.as_str(),
            "target ref must point at the exact expected merge commit"
        );
        let parents = repository::git_line(
            self.repository_path(0),
            &["rev-list", "--parents", "-n", "1", expected_merge.as_str()],
        );
        let parents = parents.split_whitespace().collect::<Vec<_>>();
        assert_eq!(
            parents.len(),
            3,
            "delivery merge must be an exact two-parent no-ff commit"
        );
        assert_eq!(parents[1], operation.expected_target_head.as_str());
        let source = self
            .store
            .delivery_ownership_snapshot(task_id)
            .await
            .expect("load merged delivery ownership")
            .expect("merged delivery ownership exists")
            .source
            .expect("merged delivery source exists");
        let source_commit = source
            .expected_source_commit
            .expect("merged delivery source commit exists");
        assert_eq!(parents[2], source_commit.as_str());
        assert_eq!(
            repository::git_line(
                self.repository_path(0),
                &[
                    "rev-parse",
                    &format!("refs/heads/{}", self.artifact(task_id).await.branch_name)
                ],
            ),
            source_commit.as_str(),
            "source ref must point at the exact committed source"
        );
        assert_eq!(
            repository::git_line(
                self.repository_path(0),
                &[
                    "rev-parse",
                    &format!("{}^{{tree}}", expected_merge.as_str())
                ],
            ),
            operation
                .candidate_merge_tree
                .as_ref()
                .expect("merged operation has candidate merge tree")
                .as_str(),
            "exact expected merge tree must be installed"
        );
    }

    pub async fn assert_delivery_cleanup_completed(&self, task_id: TaskId) {
        let ownership = self
            .store
            .delivery_ownership_snapshot(task_id)
            .await
            .expect("load completed delivery cleanup ownership")
            .expect("completed delivery cleanup ownership exists");
        assert_eq!(
            ownership
                .disposition
                .expect("completed delivery disposition exists")
                .worktree_state,
            coding_agent_store::WorktreeDisposition::Removed
        );
        assert_eq!(delivery::receipt_counts(&self.store, task_id).await, (1, 1));
    }

    pub fn maximum_overlapping_role_loops(&self) -> usize {
        self.role_barrier.maximum_active()
    }

    pub fn maximum_overlapping_control_operations(&self) -> usize {
        self.control_tracker.maximum_active()
    }

    pub async fn cancel(&self, task_id: TaskId) {
        self.manager
            .cancel(task_id)
            .await
            .expect("cancel concurrent E2E task");
    }

    pub async fn task(&self, task_id: TaskId) -> Task {
        self.task_detail(task_id).await.task
    }

    pub async fn task_detail(&self, task_id: TaskId) -> TaskDetail {
        tokio::time::timeout(STORE_OPERATION_TIMEOUT, self.store.task_detail(task_id))
            .await
            .unwrap_or_else(|_| panic!("timed out loading concurrent E2E task {task_id}"))
            .expect("load concurrent E2E task")
            .expect("concurrent E2E task exists")
    }

    pub async fn artifact(&self, task_id: TaskId) -> TaskAttemptArtifact {
        self.store
            .load_attempt_artifact(task_id)
            .await
            .expect("load concurrent E2E artifact")
            .expect("concurrent E2E artifact exists")
    }

    pub async fn wait_for_terminal(&self, task_id: TaskId) -> Task {
        // Task is the task-local durable progress token. TaskDetail.event_cursor
        // is deliberately excluded because it is the database-wide high
        // watermark and another task must not keep this deadline alive.
        let mut last_task = None;
        let mut no_progress_deadline = Instant::now() + E2E_NO_PROGRESS_TIMEOUT;
        let hard_deadline = Instant::now() + E2E_SCENARIO_HARD_TIMEOUT;
        loop {
            let detail = self.task_detail(task_id).await;
            if is_terminal(detail.task.status) {
                return detail.task;
            }
            if last_task.as_ref() != Some(&detail.task) && Instant::now() < hard_deadline {
                no_progress_deadline = Instant::now() + E2E_NO_PROGRESS_TIMEOUT;
                last_task = Some(detail.task);
            }
            let next_deadline = no_progress_deadline.min(hard_deadline);
            if tokio::time::timeout_at(next_deadline, tokio::time::sleep(E2E_POLL_INTERVAL))
                .await
                .is_err()
            {
                let detail = self.task_detail(task_id).await;
                if is_terminal(detail.task.status) {
                    return detail.task;
                }
                if Instant::now() >= hard_deadline {
                    panic!(
                        "task {task_id} exceeded its {:?} scripted-scenario hard deadline: status={:?}, last_event_id={}",
                        E2E_SCENARIO_HARD_TIMEOUT, detail.task.status, detail.task.last_event_id
                    );
                }
                if last_task.as_ref() != Some(&detail.task) {
                    no_progress_deadline = Instant::now() + E2E_NO_PROGRESS_TIMEOUT;
                    last_task = Some(detail.task);
                    continue;
                }
                panic!(
                    "task {task_id} made no observable progress for {:?}: status={:?}, last_event_id={}",
                    E2E_NO_PROGRESS_TIMEOUT, detail.task.status, detail.task.last_event_id
                );
            }
        }
    }

    pub async fn finish(self) {
        let Self {
            store,
            writer,
            manager,
            delivery,
            repository_control: _,
            instance_process_scope,
            role_barrier,
            control_tracker: _,
            provision_pause,
            manager_actor_lifetime,
            writer_actor_lifetime,
            dispatcher,
            _service_state: _,
            temp,
            repository_paths: _,
            runtime_directory: _,
            artifact_root: _,
            toolchain: _,
            base_commits: _,
            repositories: _,
        } = self;
        let mut failures = Vec::new();
        // Release is idempotent and prevents a failed assertion from leaving a
        // provider request parked while shutdown is trying to join runners.
        role_barrier.release();
        provision_pause.release();
        let shutdown_deadline = Instant::now() + E2E_NO_PROGRESS_TIMEOUT;
        if let Some(delivery) = delivery {
            match tokio::time::timeout_at(shutdown_deadline, delivery.shutdown_and_join()).await {
                Ok(Ok(proof)) => {
                    if proof.in_flight_workers() != 0
                        || proof.queued_workers() != 0
                        || proof.retained_workers() != 0
                    {
                        failures.push(format!(
                            "delivery manager shutdown proof was not empty: {proof:?}"
                        ));
                    }
                }
                Ok(Err(error)) => failures.push(format!(
                    "concurrent E2E delivery manager shutdown failed: {error}"
                )),
                Err(_) => failures.push(
                    "concurrent E2E delivery manager shutdown exceeded the fixture deadline"
                        .to_owned(),
                ),
            }
            drop(delivery);
        }
        let quiesce = tokio::time::timeout_at(
            shutdown_deadline,
            manager.quiesce_and_interrupt(shutdown_deadline),
        )
        .await;
        let (active, quiesce_failure) = match quiesce {
            Ok(Ok(QuiesceResult::Durable { active, .. })) => (active, None),
            Ok(Ok(QuiesceResult::Frozen { active, error })) => (
                active,
                Some(format!("task manager shutdown froze: {error}")),
            ),
            Ok(Err(error)) => (
                Vec::new(),
                Some(format!("task manager could not quiesce: {error}")),
            ),
            Err(_) => (
                Vec::new(),
                Some("task manager quiesce exceeded the fixture shutdown deadline".to_owned()),
            ),
        };
        for handle in active {
            let task_id = handle.task_id;
            handle.cancellation.cancel();
            match tokio::time::timeout_at(shutdown_deadline, handle.done).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => failures.push(format!(
                    "runner {task_id} dropped its fixture shutdown signal"
                )),
                Err(_) => failures.push(format!(
                    "runner {task_id} did not stop before the shared fixture shutdown deadline"
                )),
            }
        }
        let active_role_loops = role_barrier.active();
        if active_role_loops != 0 {
            failures.push(format!(
                "{active_role_loops} concurrent E2E provider role loops remained active after shutdown"
            ));
        }
        let active_process_trees = instance_process_scope.active_tree_count();
        if active_process_trees != 0 {
            failures.push(format!(
                "{active_process_trees} concurrent E2E process trees remained registered after shutdown"
            ));
        }
        if let Some(failure) = quiesce_failure {
            failures.push(failure);
        }

        // Quiescing joins every runner. Dropping the last actor ingress handles
        // then lets the manager and writer actors exit before their shared
        // SQLite pool and temporary directory are closed.
        drop(manager);
        drop(writer);
        if !wait_for_actor_exit("task manager", &manager_actor_lifetime).await {
            failures.push("concurrent E2E task manager actor did not exit".to_owned());
        }
        if !wait_for_actor_exit("store writer", &writer_actor_lifetime).await {
            failures.push("concurrent E2E store writer actor did not exit".to_owned());
        }
        match tokio::time::timeout(FIXTURE_CLOSE_STEP_TIMEOUT, dispatcher.close()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => failures.push(format!(
                "concurrent E2E event dispatcher close failed: {error}"
            )),
            Err(_) => failures.push("concurrent E2E event dispatcher close timed out".to_owned()),
        }
        if tokio::time::timeout(FIXTURE_CLOSE_STEP_TIMEOUT, store.close())
            .await
            .is_err()
        {
            failures.push("concurrent E2E store close timed out".to_owned());
        }
        drop(dispatcher);
        drop(store);

        let temporary_root = temp.path().to_path_buf();
        if let Err(error) = temp.close() {
            failures.push(format!("close concurrent E2E temporary directory: {error}"));
        }
        if temporary_root.exists() {
            failures.push(format!(
                "concurrent E2E temporary directory leaked: {}",
                temporary_root.display()
            ));
        }
        if !failures.is_empty() {
            panic!(
                "concurrent E2E fixture shutdown failed after attempting every cleanup step: {}",
                failures.join("; ")
            );
        }
    }

    pub async fn assert_distinct_isolated_artifacts(&self, tasks: &[Task]) {
        assert_eq!(tasks.len(), 2, "the pair assertion requires two tasks");
        let first = self.artifact(tasks[0].id).await;
        let second = self.artifact(tasks[1].id).await;
        assert_ne!(first.branch_name, second.branch_name);
        assert_ne!(first.worktree_path, second.worktree_path);
        for artifact in [&first, &second] {
            assert!(
                artifact
                    .worktree_path
                    .as_path()
                    .starts_with(&self.artifact_root)
            );
            assert_eq!(
                artifact.base_commit, self.base_commits[0],
                "both isolated attempts must start from committed HEAD"
            );
            assert_eq!(
                git_line(
                    self.repository_path(0),
                    &["rev-parse", &format!("refs/heads/{}", artifact.branch_name)],
                ),
                self.base_commits[0],
                "uncommitted task edits must not move the reserved branch"
            );
        }
    }

    pub fn assert_repository_control_available(&self, repository_index: usize) {
        assert_eq!(
            self.repository_control
                .control_state(self.repository(repository_index).id)
                .expect("read repository control state"),
            RepositoryControlState::Available,
            "normal role execution must not retain the repository control lease"
        );
    }

    pub async fn wait_for_repository_control_available(&self, repository_index: usize) {
        let repository_id = self.repository(repository_index).id;
        match wait_for_repository_control_settlement(STORE_OPERATION_TIMEOUT, || {
            self.repository_control
                .control_state(repository_id)
                .expect("read repository control state")
        })
        .await
        {
            Some(RepositoryControlState::Available) => {}
            Some(RepositoryControlState::Poisoned) => {
                panic!("completed delivery operation poisoned the repository control lease");
            }
            Some(RepositoryControlState::Busy) => {
                unreachable!("repository control settlement cannot return a busy state")
            }
            None => {
                panic!(
                    "completed delivery operation did not release the repository control lease within {STORE_OPERATION_TIMEOUT:?}"
                )
            }
        }
    }

    pub fn assert_repository_control_busy(&self, repository_index: usize) {
        assert_eq!(
            self.repository_control
                .control_state(self.repository(repository_index).id)
                .expect("read repository control state"),
            RepositoryControlState::Busy,
            "durable task reservation must retain the shared repository lease"
        );
    }

    pub async fn assert_original_dirty_state_isolated(
        &self,
        repository_index: usize,
        tasks: &[Task],
    ) {
        let original = self.repository_path(repository_index);
        assert_eq!(
            std::fs::read_to_string(original.join("staged.txt"))
                .expect("read original staged sentinel"),
            DIRTY_STAGED
        );
        assert_eq!(
            std::fs::read_to_string(original.join("unstaged.txt"))
                .expect("read original unstaged sentinel"),
            DIRTY_UNSTAGED
        );
        assert_eq!(
            std::fs::read_to_string(original.join("untracked.txt"))
                .expect("read original untracked sentinel"),
            DIRTY_UNTRACKED
        );
        for task in tasks {
            let artifact = self.artifact(task.id).await;
            let worktree = artifact.worktree_path.as_path();
            assert_eq!(
                std::fs::read_to_string(worktree.join("staged.txt"))
                    .expect("read isolated staged sentinel"),
                COMMITTED_STAGED
            );
            assert_eq!(
                std::fs::read_to_string(worktree.join("unstaged.txt"))
                    .expect("read isolated unstaged sentinel"),
                COMMITTED_UNSTAGED
            );
            assert!(
                !worktree.join("untracked.txt").exists(),
                "original checkout untracked content entered an isolated task"
            );
        }
    }

    pub fn assert_no_live_process_trees(&self) {
        assert_eq!(
            self.instance_process_scope.active_tree_count(),
            0,
            "the concurrent E2E must not leave a registered process tree"
        );
    }

    fn repository(&self, index: usize) -> &Repository {
        self.repositories
            .get(index)
            .unwrap_or_else(|| panic!("repository index {index} is out of bounds"))
    }

    fn repository_path(&self, index: usize) -> &Path {
        self.repository_paths
            .get(index)
            .unwrap_or_else(|| panic!("repository index {index} is out of bounds"))
    }

    fn delivery(&self) -> &ConcurrentDelivery {
        self.delivery
            .as_ref()
            .expect("start the concurrent production delivery manager first")
    }
}

fn is_terminal(status: TaskStatus) -> bool {
    matches!(
        status,
        TaskStatus::Completed
            | TaskStatus::Failed
            | TaskStatus::Cancelled
            | TaskStatus::Interrupted
    )
}

pub(crate) async fn wait_for_repository_control_settlement(
    deadline: Duration,
    mut observe: impl FnMut() -> RepositoryControlState,
) -> Option<RepositoryControlState> {
    tokio::time::timeout(deadline, async {
        loop {
            match observe() {
                RepositoryControlState::Busy => {
                    tokio::time::sleep(E2E_POLL_INTERVAL).await;
                }
                settled => return settled,
            }
        }
    })
    .await
    .ok()
}

async fn wait_for_actor_exit<T>(_actor: &str, lifetime: &Weak<T>) -> bool {
    tokio::time::timeout(E2E_NO_PROGRESS_TIMEOUT, async {
        while lifetime.upgrade().is_some() {
            tokio::time::sleep(E2E_POLL_INTERVAL).await;
        }
    })
    .await
    .is_ok()
}
