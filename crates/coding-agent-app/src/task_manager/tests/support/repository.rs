async fn register_repository(store: &Store, root: PathBuf) -> Repository {
    let root = root.canonicalize().unwrap();
    let input = NewRepository {
        selected_path: canonical(root.join("selected")),
        display_name: "claim pause".to_owned(),
        git_root: canonical(root.join("git")),
        cargo_workspace_root: canonical(root.join("workspace")),
    };
    match store
        .register_repository(input)
        .await
        .expect("register claim-pause repository")
    {
        RegisterRepositoryOutcome::Created(repository)
        | RegisterRepositoryOutcome::Existing(repository) => repository,
    }
}

fn test_task_manager_launch_resources_for_repository(
    global: u32,
    per_repository: u32,
    repository: &Repository,
    root: &Path,
) -> TaskManagerLaunchResources {
    let resources = test_task_manager_launch_resources(global, per_repository);
    register_repository_control_for_test(&resources, repository, root);
    resources
}

fn register_repository_control_for_test(
    resources: &TaskManagerLaunchResources,
    repository: &Repository,
    root: &Path,
) {
    let marker = RootCapability::open(root.canonicalize().unwrap())
        .expect("open task-manager test repository capability")
        .identity_marker()
        .expect("read task-manager test repository identity");
    resources
        .repository_control()
        .register_alias(
            RepositoryIdentityLookup {
                repository_id: repository.id,
                git_root: repository.git_root.clone(),
                git_identity_key: format!("task-manager-test-{}", repository.id),
            },
            &FixedMarkerResolver(marker),
        )
        .expect("register task-manager test repository control identity");
}

fn canonical(path: PathBuf) -> CanonicalPath {
    CanonicalPath::try_from_canonical(path).expect("construct claim-pause canonical path")
}

async fn wait_for_status(store: &Store, task_id: TaskId, expected: TaskStatus) {
    let reached = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let task = store
                .task_detail(task_id)
                .await
                .expect("load claim-pause task")
                .expect("claim-pause task exists")
                .task;
            if task.status == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    if reached.is_err() {
        let current = store
            .task_detail(task_id)
            .await
            .expect("load timed-out claim-pause task")
            .expect("timed-out claim-pause task exists")
            .task;
        panic!(
            "claim-pause task did not reach {expected:?}; current status is {:?}",
            current.status
        );
    }
}

async fn wait_for_claim_resources_released(hooks: &ClaimTestHooks) {
    let released = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if hooks.active_count() == 0 && hooks.available_permits() == 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        released.is_ok(),
        "claim-pause active handle and permit were not released before timeout; active_count={}, available_permits={}",
        hooks.active_count(),
        hooks.available_permits()
    );
}

fn detached_task_manager_handle(sender: mpsc::Sender<TaskManagerMessage>) -> TaskManagerHandle {
    let (degraded_recoveries, _) = broadcast::channel(1);
    let scheduler_projection = SchedulerProjectionBridge::new(uuid::Uuid::new_v4(), 0);
    TaskManagerHandle {
        sender,
        degraded_recoveries,
        shutdown: Arc::new(TaskManagerShutdownControl::new(Arc::new(Mutex::new(())))),
        scheduler_state_reader: scheduler_projection.reader(),
        #[cfg(feature = "test-support")]
        storage_signals: TaskManagerStorageSignals::new(),
        #[cfg(feature = "test-support")]
        actor_pauses: None,
    }
}
