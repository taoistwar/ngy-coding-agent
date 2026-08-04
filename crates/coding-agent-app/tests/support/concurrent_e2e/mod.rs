use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use coding_agent_app::{
    CodingAgentAttemptFactory, CodingAgentPreparationControl, CodingAgentRunner,
    CodingAgentRunnerConfig, EventDispatcherHandle, Project2RuntimeSessionFactory,
    ProvisionedAgentRuntimeFactory, RepositoryControlCoordinator, RepositoryControlState,
    SchedulerConcurrencyLimits, ServiceState, ServiceStateController, StoreWriterHandle,
    SystemWallClock, TaskManagerHandle, TaskManagerLaunchResources,
    WorktreeCodingAgentAttemptFactory,
};
use coding_agent_domain::{NewRepository, Repository, Task, TaskId, TaskStatus};
use coding_agent_runtime::ProcessLivenessScope;
use coding_agent_store::{RegisterRepositoryOutcome, Store, TaskAttemptArtifact, TaskDetail};
use tempfile::TempDir;

use observation::{ControlOperationTracker, ObservedAttemptFactory};
use provider::{RoleLoopBarrier, ScriptedProviderFactory};
use repository::{
    canonical, discover_e2e_toolchain, git_line, provisioner_factory, seed_repository,
};

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

pub struct ConcurrentE2eFixture {
    repository_paths: Vec<PathBuf>,
    artifact_root: PathBuf,
    base_commits: Vec<String>,
    store: Store,
    repositories: Vec<Repository>,
    writer: StoreWriterHandle,
    manager: TaskManagerHandle,
    repository_control: Arc<RepositoryControlCoordinator>,
    instance_process_scope: ProcessLivenessScope,
    role_barrier: Arc<RoleLoopBarrier>,
    control_tracker: Arc<ControlOperationTracker>,
    _dispatcher: EventDispatcherHandle,
    _service_state: ServiceStateController,
    _temp: TempDir,
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
        std::fs::create_dir_all(&runtime_directory).expect("create E2E runtime directory");
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
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 1_024)
            .await
            .expect("spawn concurrent E2E dispatcher");
        let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 128);
        let provisioners = provisioner_factory(
            toolchain.clone(),
            artifact_root.clone(),
            runtime_directory.clone(),
        );
        let runtimes: Arc<dyn ProvisionedAgentRuntimeFactory> =
            Arc::new(Project2RuntimeSessionFactory::project_2_defaults(
                toolchain,
                runtime_directory,
                NonZeroU32::MIN,
            ));
        let real_attempts: Arc<dyn CodingAgentAttemptFactory> = Arc::new(
            WorktreeCodingAgentAttemptFactory::new(provisioners, runtimes),
        );
        let control_tracker = Arc::new(ControlOperationTracker::default());
        let attempts: Arc<dyn CodingAgentAttemptFactory> = Arc::new(ObservedAttemptFactory::new(
            real_attempts,
            Arc::clone(&control_tracker),
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
            artifact_root,
            base_commits,
            store,
            repositories,
            writer,
            manager,
            repository_control,
            instance_process_scope,
            role_barrier,
            control_tracker,
            _dispatcher: dispatcher,
            _service_state: service_state,
            _temp: temporary,
        }
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
        self.store
            .task_detail(task_id)
            .await
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
        tokio::time::timeout(Duration::from_secs(300), async {
            loop {
                let task = self.task(task_id).await;
                if matches!(
                    task.status,
                    TaskStatus::Completed
                        | TaskStatus::Failed
                        | TaskStatus::Cancelled
                        | TaskStatus::Interrupted
                ) {
                    return task;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("task {task_id} did not become terminal"))
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
}
