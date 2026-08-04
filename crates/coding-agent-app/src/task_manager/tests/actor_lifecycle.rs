use super::*;

#[tokio::test]
async fn dropping_the_last_idle_handle_releases_the_manager_actor() {
    let temp_dir = tempfile::tempdir().expect("create manager-exit fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open manager-exit store");
    store.migrate().await.expect("migrate manager-exit store");
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn manager-exit dispatcher");
    let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
    let manager = TaskManagerHandle::spawn(
        store,
        writer,
        dispatcher,
        ServiceStateController::new(ServiceState::Ready),
        Arc::new(CancellingRunner::default()),
        test_task_manager_launch_resources(1, 1),
        8,
    );
    let mut exited = manager.install_exit_probe().await;

    drop(manager);
    match tokio::time::timeout(Duration::from_secs(2), &mut exited).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("manager dropped its exit probe without a clean actor exit"),
        Err(_) => {
            panic!("idle task-manager actor stayed alive after its final handle was dropped")
        }
    }
}

#[tokio::test]
async fn pre_preparation_cancellation_releases_abnormal_owner_after_process_proof() {
    let temp_dir = tempfile::tempdir().expect("create early-cancel fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open early-cancel store");
    store.migrate().await.expect("migrate early-cancel store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn early-cancel dispatcher");
    let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
    let resources =
        test_task_manager_launch_resources_for_repository(1, 1, &repository, temp_dir.path());
    let repository_control = resources.repository_control();
    let manager = TaskManagerHandle::spawn(
        store.clone(),
        writer.clone(),
        dispatcher,
        ServiceStateController::new(ServiceState::Ready),
        Arc::new(EarlyCancelledRunner),
        resources,
        8,
    );
    let task = writer
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "cancel before preparation ownership is released",
            )
            .expect("construct early-cancel task"),
            background_deadline(),
        )
        .await
        .expect("create early-cancel task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify early-cancel task");

    wait_for_status(&store, task.id, TaskStatus::Cancelled).await;
    let key = repository_control
        .coordination_key(repository.id)
        .expect("load early-cancel coordination key");
    let reconciliation = repository_control
        .try_acquire_reconciliation(key)
        .expect("process proof releases the abnormal early-cancel owner");
    reconciliation
        .poison(crate::RepositoryControlPoisonReason::AbnormalLeaseDrop)
        .expect("release early-cancel reconciliation owner with sticky poison");
    let safety = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let safety = manager
                .safety_snapshot_for_test()
                .await
                .expect("inspect early-cancel ownership release");
            if safety.active_count == 0 && safety.available_permits == 1 {
                break safety;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal projection releases the early-cancel actor owner and permit");
    assert_eq!(safety.active_count, 0);
    assert_eq!(safety.available_permits, 1);
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn late_cancel_reloads_terminal_after_a_stale_running_lookup_loses_active_ownership() {
    let temp_dir = tempfile::tempdir().expect("create late-cancel race fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open late-cancel race store");
    store
        .migrate()
        .await
        .expect("migrate late-cancel race store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn late-cancel race dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::PersistStopIntentBatch),
            count: 1,
        }])
        .expect("construct late-cancel stop-intent probe"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
        controller.clone(),
    );
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
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "late cancel must re-read a stale running lookup",
            )
            .expect("construct late-cancel race task"),
            background_deadline(),
        )
        .await
        .expect("create late-cancel race task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify late-cancel race task");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("late-cancel race runner starts");
    wait_for_status(&store, task.id, TaskStatus::Running).await;
    let stale_running = store
        .task_detail(task.id)
        .await
        .expect("load stale running cancel snapshot")
        .expect("late-cancel race task exists")
        .task;
    assert_eq!(stale_running.status, TaskStatus::Running);

    runner.release.notify_one();
    wait_for_status(&store, task.id, TaskStatus::Failed).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if manager
                .active_stop_snapshot_for_test(task.id)
                .await
                .is_ok_and(|snapshot| snapshot.is_none())
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal projection releases active ownership before stale cancel delivery");
    let current = store
        .task_detail(task.id)
        .await
        .expect("load current late-cancel terminal")
        .expect("late-cancel terminal task exists")
        .task;

    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(5),
            manager.inject_stale_cancel_task_loaded_for_test(stale_running),
        )
        .await
        .expect("stale loaded cancel returns"),
        Ok(CancelOutcome::Finished { task: finished }) if finished == current
    ));
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::PersistStopIntentBatch,
        ),
        0,
        "a stale Running lookup cannot manufacture a stop intent after terminal release"
    );
    let barriers = manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect settled late-cancel lookup ownership");
    assert_eq!(barriers.detached_cancel_completions, 0);
    assert!(!barriers.hard_frozen);
    assert!(
        store
            .scheduler_bootstrap_snapshot()
            .await
            .expect("load late-cancel durable stop-intent projection")
            .running_stop_intents
            .is_empty()
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn dropping_the_last_handle_waits_for_a_queued_cancel_completion() {
    let temp_dir = tempfile::tempdir().expect("create queued-cancel exit fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open queued-cancel exit store");
    store
        .migrate()
        .await
        .expect("migrate queued-cancel exit store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn queued-cancel exit dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
            operation: Some(StoreWriterOperationKind::CancelTask),
            count: 1,
        }])
        .expect("construct queued-cancel exit writer controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
        controller.clone(),
    );
    let task = writer
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "queued cancel owns actor shutdown",
            )
            .expect("construct queued-cancel exit task"),
            background_deadline(),
        )
        .await
        .expect("create queued-cancel exit task")
        .value
        .task()
        .clone();
    let manager = TaskManagerHandle::spawn(
        store,
        writer,
        dispatcher,
        ServiceStateController::new(ServiceState::Ready),
        Arc::new(CancellingRunner::default()),
        test_task_manager_launch_resources_for_repository(1, 1, &repository, temp_dir.path()),
        8,
    );
    let mut exited = manager.install_exit_probe().await;
    let (response, cancel) = oneshot::channel();
    manager
        .send(TaskManagerMessage::Cancel {
            task_id: task.id,
            response,
        })
        .await
        .expect("enqueue queued cancellation");
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1)
        .await;

    drop(manager);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut exited)
            .await
            .is_err(),
        "the actor must retain a detached queued-cancel completion"
    );
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
        1
    );
    assert!(matches!(
        cancel.await.expect("queued-cancel response remains owned"),
        Ok(CancelOutcome::Cancelled { task: cancelled }) if cancelled.id == task.id
    ));
    tokio::time::timeout(Duration::from_secs(2), &mut exited)
        .await
        .expect("actor exits after queued-cancel completion")
        .expect("actor reports a clean exit");
}

#[tokio::test]
async fn last_handle_close_drains_a_buffered_non_target_cancel_before_exit() {
    let temp_dir = tempfile::tempdir().expect("create buffered-cancel exit fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open buffered-cancel exit store");
    store
        .migrate()
        .await
        .expect("migrate buffered-cancel exit store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn buffered-cancel exit dispatcher");
    let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
    let hooks = Arc::new(ClaimTestHooks::new(ClaimPhase::PermitAcquired));
    let manager = TaskManagerHandle::spawn_with_claim_hooks(
        (
            store.clone(),
            writer.clone(),
            dispatcher,
            ServiceStateController::new(ServiceState::Ready),
        ),
        Arc::new(CancellingRunner::default()),
        test_task_manager_launch_resources_for_repository(1, 1, &repository, temp_dir.path()),
        8,
        hooks.clone(),
    );
    let mut tasks = Vec::new();
    for prompt in ["claim target", "buffered non-target"] {
        tasks.push(
            writer
                .create_task(
                    NewTask::try_new(ClientRequestId::new(), repository.id, prompt)
                        .expect("construct buffered-cancel exit task"),
                    background_deadline(),
                )
                .await
                .expect("create buffered-cancel exit task")
                .value
                .task()
                .clone(),
        );
    }
    let mut exited = manager.install_exit_probe().await;
    manager
        .notify_queued(tasks[0].id)
        .await
        .expect("start paused claim");
    hooks.wait_until_reached().await;
    let (other_response, other_cancel) = oneshot::channel();
    manager
        .send(TaskManagerMessage::Cancel {
            task_id: tasks[1].id,
            response: other_response,
        })
        .await
        .expect("enqueue non-target cancel first");
    let (target_response, target_cancel) = oneshot::channel();
    manager
        .send(TaskManagerMessage::Cancel {
            task_id: tasks[0].id,
            response: target_response,
        })
        .await
        .expect("enqueue claim-target cancel second");

    hooks.resume();
    drop(manager);
    for (task, response) in [(&tasks[0], target_cancel), (&tasks[1], other_cancel)] {
        assert!(matches!(
            response.await.expect("buffered cancel remains actor-owned"),
            Ok(CancelOutcome::Cancelled { task: cancelled }) if cancelled.id == task.id
        ));
    }
    tokio::time::timeout(Duration::from_secs(2), &mut exited)
        .await
        .expect("actor drains deferred messages before exit")
        .expect("actor reports a clean buffered-message exit");
    assert_eq!(hooks.active_count(), 0);
    assert_eq!(hooks.available_permits(), 1);
}

#[tokio::test]
async fn active_runner_sender_keeps_actor_alive_until_terminal_cleanup() {
    let temp_dir = tempfile::tempdir().expect("create active-exit fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open active-exit store");
    store.migrate().await.expect("migrate active-exit store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn active-exit dispatcher");
    let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
    let runner = Arc::new(ReleaseRunner::default());
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
            NewTask::try_new(ClientRequestId::new(), repository.id, "active exit")
                .expect("construct active-exit task"),
            background_deadline(),
        )
        .await
        .expect("create active-exit task")
        .value
        .task()
        .clone();
    manager.notify_queued(task.id).await.expect("notify actor");
    runner.started.notified().await;
    wait_for_status(&store, task.id, TaskStatus::Running).await;
    let mut exited = manager.install_exit_probe().await;
    let manager_sender = manager.sender.downgrade();

    drop(manager);
    assert!(matches!(
        exited.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    assert!(manager_sender.strong_count() > 0);

    runner.release.notify_one();
    wait_for_status(&store, task.id, TaskStatus::Completed).await;
    match tokio::time::timeout(Duration::from_secs(2), &mut exited).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("active manager dropped its exit probe without cleanup"),
        Err(_) => panic!(
            "manager actor stayed alive after active terminal cleanup; strong_senders={}",
            manager_sender.strong_count()
        ),
    }
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn held_process_tree_globally_pauses_scheduler_until_cleanup_is_confirmed() {
    let temp_dir = tempfile::tempdir().expect("create held-cleanup fixture directory");
    let first_root = temp_dir.path().join("first");
    let second_root = temp_dir.path().join("second");
    std::fs::create_dir_all(&first_root).expect("create first held-cleanup repository root");
    std::fs::create_dir_all(&second_root).expect("create second held-cleanup repository root");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open held-cleanup store");
    store.migrate().await.expect("migrate held-cleanup store");
    let first_repository = register_repository(&store, first_root.clone()).await;
    let second_repository = register_repository(&store, second_root.clone()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn held-cleanup dispatcher");
    let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
    let runner = Arc::new(HeldCleanupRunner::default());
    let resources = test_task_manager_launch_resources(2, 2);
    register_repository_control_for_test(&resources, &first_repository, &first_root);
    register_repository_control_for_test(&resources, &second_repository, &second_root);
    let service_state = ServiceStateController::new(ServiceState::Ready);
    let manager = TaskManagerHandle::spawn(
        store.clone(),
        writer.clone(),
        dispatcher,
        service_state.clone(),
        runner.clone(),
        resources,
        8,
    );
    let first_task = writer
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                first_repository.id,
                "held cleanup blocks terminal",
            )
            .expect("construct held-cleanup task"),
            background_deadline(),
        )
        .await
        .expect("create held-cleanup task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(first_task.id)
        .await
        .expect("notify held-cleanup task");
    tokio::time::timeout(Duration::from_secs(2), runner.returned.notified())
        .await
        .expect("runner returns while its process tree remains held");

    tokio::time::timeout(Duration::from_secs(2), async {
        while !manager.scheduler_projection_for_test().service_paused {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("unconfirmed cleanup pauses the exact Scheduler projection");

    let second_task = writer
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                second_repository.id,
                "held cleanup blocks replacement",
            )
            .expect("construct replacement task"),
            background_deadline(),
        )
        .await
        .expect("create replacement task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(second_task.id)
        .await
        .expect("notify replacement task while cleanup is held");
    tokio::time::sleep(PROCESS_CLEANUP_RETRY_INTERVAL * 2).await;

    assert_eq!(
        store
            .task_detail(first_task.id)
            .await
            .expect("read held-cleanup task")
            .expect("held-cleanup task exists")
            .task
            .status,
        TaskStatus::Running,
        "no terminal transaction may cross an unconfirmed cleanup proof"
    );
    assert_eq!(
        store
            .task_detail(second_task.id)
            .await
            .expect("read replacement task")
            .expect("replacement task exists")
            .task
            .status,
        TaskStatus::Queued,
        "unconfirmed cleanup must globally block a replacement claim"
    );
    assert_eq!(
        runner.start_count(),
        1,
        "the replacement runner must not start while cleanup is unconfirmed"
    );
    assert!(manager.scheduler_projection_for_test().service_paused);
    assert_eq!(
        service_state.current().state,
        ServiceState::Ready,
        "cleanup admission pause must not borrow StoreDegraded or another ServiceState"
    );
    let safety = manager
        .safety_snapshot_for_test()
        .await
        .expect("inspect held-cleanup ownership");
    assert_eq!(safety.active_count, 1);
    assert_eq!(safety.available_permits, 1);

    runner.release_cleanup();
    wait_for_status(&store, first_task.id, TaskStatus::Cancelled).await;
    wait_for_status(&store, second_task.id, TaskStatus::Cancelled).await;
    let safety = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let safety = manager
                .safety_snapshot_for_test()
                .await
                .expect("inspect released cleanup ownership");
            if safety.active_count == 0
                && safety.available_permits == 2
                && !manager.scheduler_projection_for_test().service_paused
            {
                return safety;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cleanup retry eventually releases exact ownership");
    assert_eq!(runner.start_count(), 2);
    assert_eq!(safety.recovery_release_ready_count, 0);
}

#[tokio::test]
async fn dropping_last_handle_after_running_commit_keeps_ownership_until_terminal_cleanup() {
    let temp_dir = tempfile::tempdir().expect("create committed-exit fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open committed-exit store");
    store.migrate().await.expect("migrate committed-exit store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn committed-exit dispatcher");
    let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
    let runner = Arc::new(ReleaseRunner::default());
    let hooks = Arc::new(ClaimTestHooks::new(ClaimPhase::RunningCommitted));
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
                "drop after running commit",
            )
            .expect("construct committed-exit task"),
            background_deadline(),
        )
        .await
        .expect("create committed-exit task")
        .value
        .task()
        .clone();
    manager.notify_queued(task.id).await.expect("notify actor");
    hooks.wait_until_reached().await;
    wait_for_status(&store, task.id, TaskStatus::Running).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), runner.started.notified())
            .await
            .is_err(),
        "runner must remain gated at the Running commit boundary"
    );
    let manager_sender = manager.sender.downgrade();

    drop(manager);
    assert!(manager_sender.strong_count() > 0);
    assert!(matches!(
        exited.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));

    hooks.resume();
    tokio::time::timeout(Duration::from_secs(2), runner.started.notified())
        .await
        .expect("durably claimed runner starts after the test gate");
    runner.release.notify_one();
    wait_for_status(&store, task.id, TaskStatus::Completed).await;
    match tokio::time::timeout(Duration::from_secs(2), &mut exited).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("committed manager dropped its exit probe without cleanup"),
        Err(_) => panic!("manager stayed alive after committed terminal cleanup"),
    }
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn hard_freeze_still_projects_a_precommitted_terminal_and_releases_ownership() {
    let temp_dir = tempfile::tempdir().expect("create precommitted terminal fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open precommitted terminal store");
    store
        .migrate()
        .await
        .expect("migrate precommitted terminal store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn precommitted terminal dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
            operation: Some(StoreWriterOperationKind::FinishTask),
            count: 1,
        }])
        .expect("construct precommitted terminal controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
        controller.clone(),
    );
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
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "hard freeze accepts a precommitted terminal",
            )
            .expect("construct precommitted terminal task"),
            background_deadline(),
        )
        .await
        .expect("create precommitted terminal task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify precommitted terminal actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("precommitted terminal runner starts");

    runner.release.notify_one();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1)
        .await;
    let pending = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect precommitted terminal write")
        .expect("precommitted terminal retains active ownership");
    assert_eq!(pending.phase, AdmissionPhase::TerminalWritePending);
    assert!(pending.pending_terminal_identity.is_some());
    manager
        .freeze_degraded_for_test()
        .await
        .expect("hard-freeze after terminal commit");
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
        1
    );

    wait_for_status(&store, task.id, TaskStatus::Failed).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if manager
                .active_stop_snapshot_for_test(task.id)
                .await
                .is_ok_and(|snapshot| snapshot.is_none())
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the exact terminal completion projects and releases ownership after hard freeze");
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
            StoreWriterOperationKind::FinishTask,
        ),
        1
    );
}

#[tokio::test]
async fn terminal_dispatch_does_not_release_before_scheduler_membership_publish() {
    let temp_dir = tempfile::tempdir().expect("create publish-gate fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open publish-gate store");
    store.migrate().await.expect("migrate publish-gate store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn publish-gate dispatcher");
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
    let mut tasks = Vec::new();
    for prompt in ["publish-gate first", "publish-gate second"] {
        tasks.push(
            writer
                .create_task(
                    NewTask::try_new(ClientRequestId::new(), repository.id, prompt)
                        .expect("construct publish-gate task"),
                    background_deadline(),
                )
                .await
                .expect("create publish-gate task")
                .value
                .task()
                .clone(),
        );
    }
    manager
        .notify_queued(tasks[0].id)
        .await
        .expect("notify publish-gate actor");
    tokio::time::timeout(Duration::from_secs(2), runner.started.notified())
        .await
        .expect("first publish-gate runner starts");
    runner.release.notify_one();
    hooks.wait_until_reached().await;
    let projection_pending = manager
        .active_stop_snapshot_for_test(tasks[0].id)
        .await
        .expect("inspect projection-pending terminal")
        .expect("projection-pending terminal retains ownership");
    assert_eq!(projection_pending.phase, AdmissionPhase::ProjectionPending);
    assert_eq!(projection_pending.pending_terminal_attempt_id, None);
    assert_eq!(projection_pending.pending_terminal_identity, None);

    assert_eq!(
        store
            .task_detail(tasks[0].id)
            .await
            .expect("load first publish-gate task")
            .expect("first publish-gate task exists")
            .task
            .status,
        TaskStatus::Failed
    );
    assert_eq!(hooks.active_count(), 1);
    assert_eq!(hooks.available_permits(), 0);
    assert_eq!(
        store
            .task_detail(tasks[1].id)
            .await
            .expect("load second publish-gate task")
            .expect("second publish-gate task exists")
            .task
            .status,
        TaskStatus::Queued
    );

    hooks.resume();
    tokio::time::timeout(Duration::from_secs(2), runner.started.notified())
        .await
        .expect("second runner starts after scheduler membership publication");
    runner.release.notify_one();
    wait_for_status(&store, tasks[1].id, TaskStatus::Failed).await;
}

#[tokio::test]
async fn out_of_band_freeze_cancels_active_runners_without_a_store_round_trip() {
    let temp_dir = tempfile::tempdir().expect("create forced-freeze fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open forced-freeze store");
    store.migrate().await.expect("migrate forced-freeze store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn forced-freeze dispatcher");
    let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
    let runner = Arc::new(CancellingRunner::default());
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
            NewTask::try_new(ClientRequestId::new(), repository.id, "forced freeze")
                .expect("construct forced-freeze task"),
            background_deadline(),
        )
        .await
        .expect("create forced-freeze task")
        .value
        .task()
        .clone();
    manager.notify_queued(task.id).await.expect("notify actor");
    wait_for_status(&store, task.id, TaskStatus::Running).await;
    runner.wait_for_starts(1).await;

    manager.freeze_and_cancel();
    manager.freeze_and_cancel();

    tokio::time::timeout(Duration::from_secs(2), runner.cancelled.notified())
        .await
        .expect("forced freeze cancels the active runner");
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("read forced-freeze task")
            .expect("forced-freeze task exists")
            .task
            .status,
        TaskStatus::Running,
        "the late cancellation result must not persist after the in-memory freeze"
    );
    assert!(matches!(
        manager.notify_queued(task.id).await,
        Err(TaskManagerError::Frozen)
    ));
}
