#[cfg(feature = "test-support")]
async fn running_hard_freeze_fixture(prompt: &str) -> RunningHardFreezeFixture {
    let temp_dir = tempfile::tempdir().expect("create hard-freeze fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open hard-freeze fixture store");
    store
        .migrate()
        .await
        .expect("migrate hard-freeze fixture store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn hard-freeze fixture dispatcher");
    let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
    let runner = Arc::new(FailingReleaseRunner::default());
    let manager = TaskManagerHandle::spawn(
        store.clone(),
        writer.clone(),
        dispatcher,
        ServiceStateController::new(ServiceState::Ready),
        runner.clone(),
        test_task_manager_launch_resources_for_repository(1, 1, &repository, temp_dir.path()),
        8,
    );
    let task = writer
        .create_task(
            NewTask::try_new(ClientRequestId::new(), repository.id, prompt)
                .expect("construct hard-freeze fixture task"),
            background_deadline(),
        )
        .await
        .expect("create hard-freeze fixture task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify hard-freeze fixture actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("hard-freeze fixture runner starts");
    RunningHardFreezeFixture {
        _temp_dir: temp_dir,
        store,
        repository,
        manager,
        runner,
        task,
    }
}

#[cfg(feature = "test-support")]
async fn two_task_hard_freeze_fixture() -> TwoTaskHardFreezeFixture {
    let temp_dir = tempfile::tempdir().expect("create two-task hard-freeze fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open two-task hard-freeze fixture store");
    store
        .migrate()
        .await
        .expect("migrate two-task hard-freeze fixture store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn two-task hard-freeze fixture dispatcher");
    let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
    let runner = Arc::new(FailingReleaseRunner::default());
    let manager = TaskManagerHandle::spawn(
        store.clone(),
        writer.clone(),
        dispatcher,
        ServiceStateController::new(ServiceState::Ready),
        runner.clone(),
        test_task_manager_launch_resources_for_repository(2, 2, &repository, temp_dir.path()),
        8,
    );
    let mut tasks = Vec::with_capacity(2);
    for prompt in [
        "first staged exact survives hard freeze",
        "second staged exact survives hard freeze",
    ] {
        tasks.push(
            writer
                .create_task(
                    NewTask::try_new(ClientRequestId::new(), repository.id, prompt)
                        .expect("construct two-task hard-freeze fixture task"),
                    background_deadline(),
                )
                .await
                .expect("create two-task hard-freeze fixture task")
                .value
                .task()
                .clone(),
        );
    }
    for task in &tasks {
        manager
            .notify_queued(task.id)
            .await
            .expect("notify two-task hard-freeze fixture actor");
    }
    for task in &tasks {
        wait_for_status(&store, task.id, TaskStatus::Running).await;
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut all_prepared = true;
            for task in &tasks {
                all_prepared &= manager
                    .active_stop_snapshot_for_test(task.id)
                    .await
                    .is_ok_and(|snapshot| {
                        snapshot.is_some_and(|snapshot| snapshot.phase == AdmissionPhase::Running)
                    });
            }
            if all_prepared {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both hard-freeze fixture runners finish preparation");
    let tasks = tasks
        .try_into()
        .expect("two-task hard-freeze fixture creates exactly two tasks");
    TwoTaskHardFreezeFixture {
        _temp_dir: temp_dir,
        store,
        repository,
        manager,
        runner,
        tasks,
    }
}

#[cfg(feature = "test-support")]
async fn paused_terminal_projection_fixture(prompt: &str) -> PausedTerminalProjectionFixture {
    let temp_dir = tempfile::tempdir().expect("create paused projection fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open paused projection fixture store");
    store
        .migrate()
        .await
        .expect("migrate paused projection fixture store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn paused projection fixture dispatcher");
    let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
    let runner = Arc::new(FailingReleaseRunner::default());
    let hooks = Arc::new(ClaimTestHooks::new(ClaimPhase::TerminalDispatched));
    let manager = TaskManagerHandle::spawn_with_claim_hooks(
        (
            store.clone(),
            writer.clone(),
            dispatcher,
            ServiceStateController::new(ServiceState::Ready),
        ),
        runner.clone(),
        test_task_manager_launch_resources_for_repository(1, 1, &repository, temp_dir.path()),
        8,
        hooks.clone(),
    );
    let task = writer
        .create_task(
            NewTask::try_new(ClientRequestId::new(), repository.id, prompt)
                .expect("construct paused projection fixture task"),
            background_deadline(),
        )
        .await
        .expect("create paused projection fixture task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify paused projection fixture actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("paused projection fixture runner starts");
    runner.release.notify_one();
    hooks.wait_until_reached().await;
    let snapshot = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect paused projection fixture")
        .expect("paused projection fixture retains active ownership");
    assert_eq!(snapshot.phase, AdmissionPhase::ProjectionPending);
    assert!(snapshot.terminal_projection_attempt.is_some());
    PausedTerminalProjectionFixture {
        _temp_dir: temp_dir,
        store,
        repository,
        manager,
        hooks,
        task,
    }
}

#[cfg(feature = "test-support")]
async fn paused_final_stop_fixture() -> PausedFinalStopFixture {
    let temp_dir = tempfile::tempdir().expect("create paused final-stop fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open paused final-stop fixture store");
    store
        .migrate()
        .await
        .expect("migrate paused final-stop fixture store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn paused final-stop fixture dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::FinalizeStoppedTask),
            count: 1,
        }])
        .expect("construct paused final-stop controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
        controller.clone(),
    );
    let runner = Arc::new(DelayedCancellationRunner::default());
    let manager = TaskManagerHandle::spawn(
        store.clone(),
        writer.clone(),
        dispatcher,
        ServiceStateController::new(ServiceState::Ready),
        runner.clone(),
        test_task_manager_launch_resources_for_repository(1, 1, &repository, temp_dir.path()),
        8,
    );
    let task = writer
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "paused final stop survives hard freeze",
            )
            .expect("construct paused final-stop task"),
            background_deadline(),
        )
        .await
        .expect("create paused final-stop task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify paused final-stop actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("paused final-stop runner starts");
    let cancel = tokio::spawn({
        let manager = manager.clone();
        async move { manager.cancel(task.id).await }
    });
    tokio::time::timeout(Duration::from_secs(5), runner.cancelled.notified())
        .await
        .expect("paused final-stop runner observes its durable stop");
    runner.release.notify_one();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let pending = manager
        .active_pending_stop_write_for_test(task.id)
        .await
        .expect("inspect paused final-stop write")
        .expect("paused final-stop identity is actor-owned");
    PausedFinalStopFixture {
        _temp_dir: temp_dir,
        store,
        manager,
        controller,
        task,
        pending,
        cancel,
    }
}

#[cfg(feature = "test-support")]
async fn paused_queued_cancel_fixture() -> PausedQueuedCancelFixture {
    let temp_dir = tempfile::tempdir().expect("create paused queued-cancel fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open paused queued-cancel fixture store");
    store
        .migrate()
        .await
        .expect("migrate paused queued-cancel fixture store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn paused queued-cancel fixture dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
            operation: Some(StoreWriterOperationKind::CancelTask),
            count: 1,
        }])
        .expect("construct paused queued-cancel controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
        controller.clone(),
    );
    let resources =
        test_task_manager_launch_resources_for_repository(1, 1, &repository, temp_dir.path());
    let repository_key = resources
        .repository_control()
        .coordination_key(repository.id)
        .expect("resolve paused queued-cancel repository control identity");
    let admission_block = resources
        .repository_control()
        .try_acquire(repository_key)
        .expect("hold paused queued-cancel repository control lease");
    let manager = TaskManagerHandle::spawn(
        store.clone(),
        writer.clone(),
        dispatcher,
        ServiceStateController::new(ServiceState::Ready),
        Arc::new(CancellingRunner::default()),
        resources,
        8,
    );
    let task = writer
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "queued cancel exact completion survives hard freeze",
            )
            .expect("construct paused queued-cancel task"),
            background_deadline(),
        )
        .await
        .expect("create paused queued-cancel task")
        .value
        .task()
        .clone();
    let cancel = tokio::spawn({
        let manager = manager.clone();
        async move { manager.cancel(task.id).await }
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        controller.wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1),
    )
    .await
    .expect("queued cancel reaches its post-commit pause before admission is released");
    admission_block
        .clean_release()
        .expect("release paused queued-cancel repository control lease cleanly");
    PausedQueuedCancelFixture {
        _temp_dir: temp_dir,
        store,
        manager,
        controller,
        task,
        cancel,
    }
}
