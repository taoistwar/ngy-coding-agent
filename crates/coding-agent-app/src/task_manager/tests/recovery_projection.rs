use super::*;

#[derive(Default)]
struct RetainedCleanupProjectionRunner {
    starts: AtomicUsize,
    retained: Notify,
    return_release: Notify,
    replacement_started: Notify,
}

#[async_trait::async_trait]
impl TaskRunner for RetainedCleanupProjectionRunner {
    async fn run(&self, mut context: RunContext, _sink: RunnerEventSink) -> RunnerOutcome {
        if self.starts.fetch_add(1, Ordering::SeqCst) == 0 {
            context
                .take_control_lease()
                .expect("the retained-cleanup runner owns repository control")
                .retain_fail_closed(crate::RepositoryControlPoisonReason::GitChildOutcomeUnknown)
                .expect("retain the exact repository owner until process proof");
            self.retained.notify_one();
            self.return_release.notified().await;
            RunnerOutcome::ProcessCleanupUnproven
        } else {
            context.complete_preparation_for_test().await;
            self.replacement_started.notify_one();
            RunnerOutcome::Cancelled
        }
    }
}

#[tokio::test]
async fn immediate_proof_refreshes_an_early_retained_pause_before_terminal_persistence() {
    let temp_dir = tempfile::tempdir().expect("create retained projection fixture");
    let first_root = temp_dir.path().join("first");
    let second_root = temp_dir.path().join("second");
    std::fs::create_dir_all(&first_root).expect("create first retained projection root");
    std::fs::create_dir_all(&second_root).expect("create second retained projection root");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open retained projection store");
    store
        .migrate()
        .await
        .expect("migrate retained projection store");
    let first_repository = register_repository(&store, first_root.clone()).await;
    let second_repository = register_repository(&store, second_root.clone()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn retained projection dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::FinishTask),
            count: 1,
        }])
        .expect("construct retained projection writer gate"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
        controller.clone(),
    );
    let runner = Arc::new(RetainedCleanupProjectionRunner::default());
    let resources = test_task_manager_launch_resources(2, 2);
    register_repository_control_for_test(&resources, &first_repository, &first_root);
    register_repository_control_for_test(&resources, &second_repository, &second_root);
    let hooks = Arc::new(ClaimTestHooks::new(ClaimPhase::RunningCommitted));
    let manager = TaskManagerHandle::spawn_with_claim_hooks(
        (
            store.clone(),
            writer.clone(),
            dispatcher,
            ServiceStateController::new(ServiceState::Ready),
        ),
        runner.clone(),
        resources,
        8,
        hooks.clone(),
    );
    let first_task = create_projection_task(&writer, first_repository.id, "retained owner").await;
    let replacement =
        create_projection_task(&writer, second_repository.id, "replacement after proof").await;

    manager
        .notify_queued(first_task.id)
        .await
        .expect("notify retained projection task");
    hooks.wait_until_reached().await;
    hooks.resume();
    tokio::time::timeout(Duration::from_secs(2), runner.retained.notified())
        .await
        .expect("runner retains repository control before returning");

    manager.notify_scheduler_storage_for_test(SchedulerStorageNotification::new(
        StorageState::Normal,
        StorageState::Normal,
        StorageState::Normal,
        vec![
            crate::scheduler::SchedulerRepositoryStorageState::new(
                first_repository.id,
                StorageState::Normal,
            ),
            crate::scheduler::SchedulerRepositoryStorageState::new(
                second_repository.id,
                StorageState::Normal,
            ),
        ],
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        while !manager.scheduler_projection_for_test().service_paused {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a concurrent refresh publishes the early recovery pause");
    runner.return_release.notify_one();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while manager.scheduler_projection_for_test().service_paused {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("exact proof must clear the early pause before terminal persistence completes");

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    wait_for_status(&store, first_task.id, TaskStatus::Failed).await;
    tokio::time::timeout(
        Duration::from_secs(2),
        runner.replacement_started.notified(),
    )
    .await
    .expect("the unpaused projection permits a fresh replacement scan");
    wait_for_status(&store, replacement.id, TaskStatus::Cancelled).await;
}

async fn create_projection_task(
    writer: &StoreWriterHandle,
    repository_id: RepositoryId,
    prompt: &str,
) -> Task {
    writer
        .create_task(
            NewTask::try_new(ClientRequestId::new(), repository_id, prompt)
                .expect("construct retained projection task"),
            background_deadline(),
        )
        .await
        .expect("create retained projection task")
        .value
        .task()
        .clone()
}
