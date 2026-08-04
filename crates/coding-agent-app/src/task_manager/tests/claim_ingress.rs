use super::*;

#[tokio::test]
async fn dropping_the_last_handle_during_registered_claim_cleans_provisional_state() {
    let temp_dir = tempfile::tempdir().expect("create dropped-claim fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open dropped-claim store");
    store.migrate().await.expect("migrate dropped-claim store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn dropped-claim dispatcher");
    let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
    let runner = Arc::new(CancellingRunner::default());
    let hooks = Arc::new(ClaimTestHooks::new(ClaimPhase::HandleRegistered));
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
    let mut exited = manager.install_exit_probe().await;
    let task = writer
        .create_task(
            NewTask::try_new(ClientRequestId::new(), repository.id, "drop during claim")
                .expect("construct dropped-claim task"),
            background_deadline(),
        )
        .await
        .expect("create dropped-claim task")
        .value
        .task()
        .clone();

    manager.notify_queued(task.id).await.expect("notify actor");
    hooks.wait_until_reached().await;
    assert_eq!(hooks.active_count(), 1);
    assert_eq!(hooks.available_permits(), 0);
    let weak_manager = manager.sender.downgrade();
    assert_eq!(weak_manager.strong_count(), 1);

    drop(manager);
    assert_eq!(weak_manager.strong_count(), 0);
    assert!(weak_manager.upgrade().is_none());
    hooks.resume();

    match tokio::time::timeout(Duration::from_secs(2), &mut exited).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("manager dropped its exit probe before claim cleanup"),
        Err(_) => panic!("manager did not exit after failed claim sender upgrade"),
    }
    let persisted = store
        .task_detail(task.id)
        .await
        .expect("read dropped-claim task")
        .expect("dropped-claim task exists")
        .task;
    assert_eq!(persisted.status, TaskStatus::Queued);
    assert_eq!(runner.starts.load(Ordering::SeqCst), 0);
    assert_eq!(hooks.active_count(), 0);
    assert_eq!(hooks.available_permits(), 1);
}

#[tokio::test]
async fn dropping_the_last_handle_after_sender_check_keeps_claim_alive_until_terminal_cleanup() {
    let temp_dir = tempfile::tempdir().expect("create checked-sender claim fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open checked-sender claim store");
    store
        .migrate()
        .await
        .expect("migrate checked-sender claim store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn checked-sender claim dispatcher");
    let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
    let runner = Arc::new(ReleaseRunner::default());
    let hooks = Arc::new(ClaimTestHooks::new(ClaimPhase::ActorLivenessAcquired));
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
    let mut exited = manager.install_exit_probe().await;
    let task = writer
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "drop after sender check",
            )
            .expect("construct checked-sender claim task"),
            background_deadline(),
        )
        .await
        .expect("create checked-sender claim task")
        .value
        .task()
        .clone();

    manager.notify_queued(task.id).await.expect("notify actor");
    hooks.wait_until_reached().await;
    assert_eq!(hooks.active_count(), 1);
    assert_eq!(hooks.available_permits(), 0);
    let weak_manager = manager.sender.downgrade();
    assert_eq!(
        weak_manager.strong_count(),
        2,
        "the external handle and the checked local sender are both live"
    );

    drop(manager);
    assert_eq!(
        weak_manager.strong_count(),
        1,
        "the checked local sender keeps the actor alive across installation"
    );
    hooks.resume();
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("claim launches after the last external handle is dropped");
    wait_for_status(&store, task.id, TaskStatus::Running).await;
    runner.release.notify_one();
    wait_for_status(&store, task.id, TaskStatus::Completed).await;

    match tokio::time::timeout(Duration::from_secs(5), &mut exited).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("manager dropped its exit probe before terminal cleanup"),
        Err(_) => panic!("manager did not exit after terminal cleanup released liveness"),
    }
    assert_eq!(weak_manager.strong_count(), 0);
    assert_eq!(hooks.active_count(), 0);
    assert_eq!(hooks.available_permits(), 1);
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn full_claim_ingress_reconciles_same_identity_before_rescan_and_later_starts() {
    let temp_dir = tempfile::tempdir().expect("create full-claim fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open full-claim store");
    store.migrate().await.expect("migrate full-claim store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let task = store
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "claim after full ingress",
            )
            .expect("construct full-claim task"),
        )
        .await
        .expect("seed full-claim task")
        .task()
        .clone();
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn full-claim dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::CreateTask),
            count: 1,
        }])
        .expect("construct full-claim writer controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        1,
        controller.clone(),
    );
    let blocked = writer
        .submit_queue_limited_create(
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "blocked writer command",
            )
            .expect("construct blocked writer command"),
            NonZeroU32::new(8).unwrap(),
            background_deadline(),
        )
        .expect("submit blocked writer command");
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let buffered = writer
        .submit_queue_limited_create(
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "buffered writer command",
            )
            .expect("construct buffered writer command"),
            NonZeroU32::new(8).unwrap(),
            background_deadline(),
        )
        .expect("fill the normal writer ingress");
    let runner = Arc::new(CancellingRunner::default());
    let hooks = Arc::new(ClaimTestHooks::new(
        ClaimPhase::ClaimRetainedForReconciliation,
    ));
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

    manager.notify_queued(task.id).await.expect("notify actor");
    hooks.wait_until_reached().await;
    assert_eq!(hooks.active_count(), 1);
    assert_eq!(hooks.available_permits(), 0);
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load retained full-claim task")
            .expect("retained full-claim task exists")
            .task
            .status,
        TaskStatus::Queued
    );
    assert_eq!(runner.starts.load(Ordering::SeqCst), 0);

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    let _ = blocked.completion().await;
    let _ = buffered.completion().await;
    hooks.resume();
    wait_for_status(&store, task.id, TaskStatus::Running).await;
    runner.wait_for_starts(1).await;
    assert_eq!(runner.starts.load(Ordering::SeqCst), 1);

    assert!(matches!(
        manager.cancel(task.id).await.expect("cancel running task"),
        CancelOutcome::Accepted { .. }
    ));
    wait_for_status(&store, task.id, TaskStatus::Cancelled).await;
    wait_for_claim_resources_released(&hooks).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn closed_claim_ingress_freezes_without_retry_loop_release_or_rescan() {
    let temp_dir = tempfile::tempdir().expect("create closed-claim fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open closed-claim store");
    store.migrate().await.expect("migrate closed-claim store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let task = store
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "claim against closed ingress",
            )
            .expect("construct closed-claim task"),
        )
        .await
        .expect("seed closed-claim task")
        .task()
        .clone();
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn closed-claim dispatcher");
    let writer = StoreWriterHandle::closed_for_test();
    let runner = Arc::new(CancellingRunner::default());
    let hooks = Arc::new(ClaimTestHooks::new(ClaimPhase::HandleRegistered));
    let manager = TaskManagerHandle::spawn_with_claim_hooks(
        (
            store.clone(),
            writer,
            dispatcher,
            ServiceStateController::new(ServiceState::Ready),
        ),
        runner.clone(),
        test_task_manager_launch_resources_for_repository(1, 1, &repository, temp_dir.path()),
        8,
        hooks.clone(),
    );
    let mut exited = manager.install_exit_probe().await;

    manager.notify_queued(task.id).await.expect("notify actor");
    hooks.wait_until_reached().await;
    assert_eq!(hooks.active_count(), 1);
    assert_eq!(hooks.available_permits(), 0);
    hooks.resume();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(
                manager.notify_queued(task.id).await,
                Err(TaskManagerError::Frozen)
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("closed claim ingress freezes without scheduling reconciliation forever");
    wait_for_claim_resources_released(&hooks).await;
    assert_eq!(runner.starts.load(Ordering::SeqCst), 0);
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load closed-claim task")
            .expect("closed-claim task exists")
            .task
            .status,
        TaskStatus::Queued
    );

    let weak_manager = manager.sender.downgrade();
    drop(manager);
    match tokio::time::timeout(Duration::from_secs(2), &mut exited).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("manager dropped its exit probe before closed-claim cleanup"),
        Err(_) => panic!("closed-claim manager retained an active retry loop"),
    }
    assert_eq!(weak_manager.strong_count(), 0);
}
