use super::*;

#[tokio::test]
async fn busy_control_lease_for_fifo_head_does_not_block_another_repository_key() {
    let temp_dir = tempfile::tempdir().expect("create control-busy fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open control-busy store");
    store.migrate().await.expect("migrate control-busy store");
    let first_root = temp_dir.path().join("first-repository");
    let second_root = temp_dir.path().join("second-repository");
    std::fs::create_dir_all(&first_root).expect("create first repository root");
    std::fs::create_dir_all(&second_root).expect("create second repository root");
    let first_repository = register_repository(&store, first_root.clone()).await;
    let second_repository = register_repository(&store, second_root.clone()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 64)
        .await
        .expect("spawn control-busy dispatcher");
    let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
    let first_task = writer
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                first_repository.id,
                "busy FIFO head",
            )
            .expect("construct busy-head task"),
            background_deadline(),
        )
        .await
        .expect("create busy-head task")
        .value
        .task()
        .clone();
    let second_task = writer
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                second_repository.id,
                "independent repository",
            )
            .expect("construct independent task"),
            background_deadline(),
        )
        .await
        .expect("create independent task")
        .value
        .task()
        .clone();
    let resources =
        test_task_manager_launch_resources_for_repository(2, 1, &first_repository, &first_root);
    let coordinator = resources.repository_control();
    let second_marker = RootCapability::open(second_root.canonicalize().unwrap())
        .expect("open second repository capability")
        .identity_marker()
        .expect("read second repository identity");
    coordinator
        .register_alias(
            RepositoryIdentityLookup {
                repository_id: second_repository.id,
                git_root: second_repository.git_root.clone(),
                git_identity_key: format!("task-manager-test-{}", second_repository.id),
            },
            &FixedMarkerResolver(second_marker),
        )
        .expect("register second repository control identity");
    let first_key = coordinator
        .coordination_key(first_repository.id)
        .expect("resolve first repository coordination key");
    let external_lease = coordinator
        .try_acquire(first_key)
        .expect("hold the FIFO-head repository control lease");
    let runner = Arc::new(CancellingRunner::default());
    let manager = TaskManagerHandle::spawn(
        store.clone(),
        writer,
        dispatcher,
        ServiceStateController::new(ServiceState::Ready),
        runner.clone(),
        resources,
        8,
    );

    manager
        .notify_queued(first_task.id)
        .await
        .expect("request control-busy scan");
    wait_for_status(&store, second_task.id, TaskStatus::Running).await;
    assert_eq!(
        store
            .task_detail(first_task.id)
            .await
            .expect("read busy-head task")
            .expect("busy-head task exists")
            .task
            .status,
        TaskStatus::Queued
    );
    runner.wait_for_starts(1).await;
    assert_eq!(runner.starts.load(Ordering::SeqCst), 1);

    assert!(matches!(
        manager
            .cancel(second_task.id)
            .await
            .expect("cancel independent task"),
        CancelOutcome::Accepted { .. }
    ));
    wait_for_status(&store, second_task.id, TaskStatus::Cancelled).await;
    assert_eq!(runner.starts.load(Ordering::SeqCst), 1);
    external_lease
        .clean_release()
        .expect("release external control lease");
    manager
        .notify_queued(first_task.id)
        .await
        .expect("rescan released FIFO head");
    wait_for_status(&store, first_task.id, TaskStatus::Running).await;
    runner.wait_for_starts(2).await;
    assert_eq!(runner.starts.load(Ordering::SeqCst), 2);
    assert!(matches!(
        manager
            .cancel(first_task.id)
            .await
            .expect("cancel formerly blocked task"),
        CancelOutcome::Accepted { .. }
    ));
    wait_for_status(&store, first_task.id, TaskStatus::Cancelled).await;
}

#[tokio::test]
async fn missing_fifo_head_storage_scope_does_not_starve_a_ready_repository() {
    let temp_dir = tempfile::tempdir().expect("create storage-fairness fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open storage-fairness store");
    store
        .migrate()
        .await
        .expect("migrate storage-fairness store");
    let first_root = temp_dir.path().join("first-repository");
    let second_root = temp_dir.path().join("second-repository");
    std::fs::create_dir_all(&first_root).expect("create first repository root");
    std::fs::create_dir_all(&second_root).expect("create second repository root");
    let first_repository = register_repository(&store, first_root.clone()).await;
    let second_repository = register_repository(&store, second_root.clone()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 64)
        .await
        .expect("spawn storage-fairness dispatcher");
    let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
    let first_task = writer
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                first_repository.id,
                "missing storage scope at FIFO head",
            )
            .expect("construct missing-scope task"),
            background_deadline(),
        )
        .await
        .expect("create missing-scope task")
        .value
        .task()
        .clone();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let second_task = writer
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                second_repository.id,
                "ready independent storage scope",
            )
            .expect("construct ready-scope task"),
            background_deadline(),
        )
        .await
        .expect("create ready-scope task")
        .value
        .task()
        .clone();
    assert!(
        first_task.created_at < second_task.created_at,
        "the missing-scope candidate is the deterministic FIFO head"
    );

    let mut resources =
        test_task_manager_launch_resources_for_repository(2, 1, &first_repository, &first_root);
    let coordinator = resources.repository_control();
    let second_marker = RootCapability::open(second_root.canonicalize().unwrap())
        .expect("open second repository capability")
        .identity_marker()
        .expect("read second repository identity");
    coordinator
        .register_alias(
            RepositoryIdentityLookup {
                repository_id: second_repository.id,
                git_root: second_repository.git_root.clone(),
                git_identity_key: format!("task-manager-test-{}", second_repository.id),
            },
            &FixedMarkerResolver(second_marker),
        )
        .expect("register second repository control identity");
    let registered_scopes = Arc::new(Mutex::new(HashMap::from([(
        second_repository.id,
        StorageState::Normal,
    )])));
    resources.storage_admission =
        TaskManagerStorageAdmission::RepositoryScopesForTest(registered_scopes.clone());
    let runner = Arc::new(CancellingRunner::default());
    let manager = TaskManagerHandle::spawn(
        store.clone(),
        writer,
        dispatcher,
        ServiceStateController::new(ServiceState::Ready),
        runner.clone(),
        resources,
        8,
    );

    manager
        .notify_queued(first_task.id)
        .await
        .expect("request storage-fairness scan");
    wait_for_status(&store, second_task.id, TaskStatus::Running).await;
    assert_eq!(
        store
            .task_detail(first_task.id)
            .await
            .expect("read missing-scope task")
            .expect("missing-scope task exists")
            .task
            .status,
        TaskStatus::Queued
    );
    runner.wait_for_starts(1).await;
    assert_eq!(runner.starts.load(Ordering::SeqCst), 1);

    registered_scopes
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(first_repository.id, StorageState::Normal);
    manager
        .notify_admission_changed()
        .await
        .expect("notify newly registered storage scope");
    wait_for_status(&store, first_task.id, TaskStatus::Running).await;
    runner.wait_for_starts(2).await;
    assert_eq!(runner.starts.load(Ordering::SeqCst), 2);

    assert!(matches!(
        manager
            .cancel(first_task.id)
            .await
            .expect("cancel formerly missing-scope task"),
        CancelOutcome::Accepted { .. }
    ));
    assert!(matches!(
        manager
            .cancel(second_task.id)
            .await
            .expect("cancel independent ready-scope task"),
        CancelOutcome::Accepted { .. }
    ));
    wait_for_status(&store, first_task.id, TaskStatus::Cancelled).await;
    wait_for_status(&store, second_task.id, TaskStatus::Cancelled).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn stale_slow_preclaim_storage_refresh_keeps_mailbox_responsive_and_does_not_claim() {
    let temp_dir = tempfile::tempdir().expect("create slow-storage fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open slow-storage store");
    store.migrate().await.expect("migrate slow-storage store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 64)
        .await
        .expect("spawn slow-storage dispatcher");
    let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
    let runner = Arc::new(CancellingRunner::default());
    let mut resources =
        test_task_manager_launch_resources_for_repository(1, 1, &repository, temp_dir.path());
    let refresh = Arc::new(PausedStorageRefresh::default());
    resources.storage_admission =
        TaskManagerStorageAdmission::PausedRefreshForTest(refresh.clone());
    let manager = TaskManagerHandle::spawn(
        store.clone(),
        writer.clone(),
        dispatcher,
        ServiceStateController::new(ServiceState::Ready),
        runner.clone(),
        resources,
        8,
    );
    let task = writer
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "slow stale storage refresh",
            )
            .expect("construct slow-storage task"),
            background_deadline(),
        )
        .await
        .expect("create slow-storage task")
        .value
        .task()
        .clone();

    manager
        .notify_queued(task.id)
        .await
        .expect("request slow-storage scan");
    refresh.wait_until_reached().await;
    let snapshot = tokio::time::timeout(Duration::from_secs(1), manager.safety_snapshot_for_test())
        .await
        .expect("slow refresh does not block the actor mailbox")
        .expect("read safety snapshot during slow refresh");
    assert_eq!(snapshot.active_count, 0);
    assert_eq!(snapshot.available_permits, 1);
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("read task during slow refresh")
            .expect("slow-refresh task exists")
            .task
            .status,
        TaskStatus::Queued
    );
    assert_eq!(runner.starts.load(Ordering::SeqCst), 0);
    tokio::time::timeout(Duration::from_secs(1), manager.notify_queued(TaskId::new()))
        .await
        .expect("queue notification remains responsive during slow refresh")
        .expect("accept queue notification during slow refresh");

    refresh.resume();
    wait_for_status(&store, task.id, TaskStatus::Running).await;
    runner.wait_for_starts(1).await;
    assert_eq!(runner.starts.load(Ordering::SeqCst), 1);
    assert!(matches!(
        manager
            .cancel(task.id)
            .await
            .expect("cancel slow-refresh task"),
        CancelOutcome::Accepted { .. }
    ));
    wait_for_status(&store, task.id, TaskStatus::Cancelled).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn critical_publish_race_is_latched_before_final_gate_and_never_starts_runner() {
    let temp_dir = tempfile::tempdir().expect("create critical-publish fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open critical-publish store");
    store
        .migrate()
        .await
        .expect("migrate critical-publish store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 64)
        .await
        .expect("spawn critical-publish dispatcher");
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
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "critical publish race",
            )
            .expect("construct critical-publish task"),
            background_deadline(),
        )
        .await
        .expect("create critical-publish task")
        .value
        .task()
        .clone();
    manager.arm_storage_critical_on_next_publish_for_test(vec![
        MonitoredStorageScope::RepositoryGit(repository.id),
    ]);

    manager
        .notify_queued(task.id)
        .await
        .expect("request critical-publish scan");
    wait_for_status(&store, task.id, TaskStatus::Failed).await;
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("read critical-publish terminal")
            .expect("critical-publish task exists")
            .task
            .failure,
        Some(TaskFailure {
            code: "DISK_PRESSURE_CRITICAL".to_owned(),
            message: "critical disk pressure stopped the task".to_owned(),
            retryable: true,
        })
    );
    assert_eq!(
        runner.starts.load(Ordering::SeqCst),
        0,
        "the atomic publish latch must be observed before the final spawn gate"
    );
    let safety = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = manager
                .safety_snapshot_for_test()
                .await
                .expect("read post-recovery safety snapshot");
            if snapshot.active_count == 0 && snapshot.available_permits == 1 {
                return snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("critical-publish recovery releases exact ownership");
    assert_eq!(safety.recovery_release_ready_count, 0);
}

#[tokio::test]
async fn pressure_and_unavailable_after_claim_commit_do_not_suppress_or_fail_the_runner() {
    for launch_state in [StorageState::Pressure, StorageState::Unavailable] {
        let temp_dir = tempfile::tempdir().expect("create post-claim storage fixture directory");
        let store = Store::open(temp_dir.path().join("store.sqlite3"))
            .await
            .expect("open post-claim storage store");
        store
            .migrate()
            .await
            .expect("migrate post-claim storage store");
        let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 64)
            .await
            .expect("spawn post-claim storage dispatcher");
        let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
        let runner = Arc::new(CancellingRunner::default());
        let hooks = Arc::new(ClaimTestHooks::new(ClaimPhase::RunningCommitted));
        let mut launch_resources =
            test_task_manager_launch_resources_for_repository(1, 1, &repository, temp_dir.path());
        let controlled_storage = Arc::new(Mutex::new(Some(StorageState::Normal)));
        launch_resources.storage_admission =
            TaskManagerStorageAdmission::ControlledForTest(controlled_storage.clone());
        let manager = TaskManagerHandle::spawn_with_claim_hooks(
            (
                store.clone(),
                writer.clone(),
                dispatcher,
                ServiceStateController::new(ServiceState::Ready),
            ),
            runner.clone(),
            launch_resources,
            8,
            hooks.clone(),
        );
        let task = writer
            .create_task(
                NewTask::try_new(
                    ClientRequestId::new(),
                    repository.id,
                    "post-claim noncritical storage",
                )
                .expect("construct post-claim storage task"),
                background_deadline(),
            )
            .await
            .expect("create post-claim storage task")
            .value
            .task()
            .clone();

        manager.notify_queued(task.id).await.expect("notify actor");
        hooks.wait_until_reached().await;
        *controlled_storage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(launch_state);
        hooks.resume();
        tokio::time::timeout(Duration::from_secs(5), async {
            while runner.starts.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!("runner did not start after noncritical {launch_state:?} sample")
        });

        let running = store
            .task_detail(task.id)
            .await
            .expect("load post-claim storage task")
            .expect("post-claim storage task exists")
            .task;
        assert_eq!(running.status, TaskStatus::Running);
        assert_eq!(running.failure, None);
        assert!(
            store
                .scheduler_bootstrap_snapshot()
                .await
                .expect("load post-claim storage bootstrap")
                .running_stop_intents
                .is_empty()
        );
        assert_eq!(
            store
                .task_events_after(task.id, EventCursor::ZERO, usize::MAX)
                .await
                .expect("load post-claim storage events")
                .events
                .into_iter()
                .map(|event| event.payload.kind())
                .collect::<Vec<_>>(),
            vec![TaskEventKind::TaskQueued, TaskEventKind::TaskStarted]
        );

        assert!(matches!(
            manager.cancel(task.id).await.expect("cancel active runner"),
            CancelOutcome::Accepted { .. }
        ));
        wait_for_status(&store, task.id, TaskStatus::Cancelled).await;
        wait_for_claim_resources_released(&hooks).await;
    }
}

#[tokio::test]
async fn out_of_band_freeze_at_every_claim_phase_never_starts_a_new_runner() {
    for phase in [
        ClaimPhase::PermitAcquired,
        ClaimPhase::HandleRegistered,
        ClaimPhase::RunningCommitted,
    ] {
        let temp_dir = tempfile::tempdir().expect("create frozen-claim fixture directory");
        let store = Store::open(temp_dir.path().join("store.sqlite3"))
            .await
            .expect("open frozen-claim store");
        store.migrate().await.expect("migrate frozen-claim store");
        let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 64)
            .await
            .expect("spawn frozen-claim dispatcher");
        let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
        let runner = Arc::new(CancellingRunner::default());
        let hooks = Arc::new(ClaimTestHooks::new(phase));
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
                NewTask::try_new(ClientRequestId::new(), repository.id, "frozen claim")
                    .expect("construct frozen-claim task"),
                background_deadline(),
            )
            .await
            .expect("create frozen-claim task")
            .value
            .task()
            .clone();

        manager.notify_queued(task.id).await.expect("notify actor");
        hooks.wait_until_reached().await;
        manager.freeze_and_cancel();
        hooks.resume();
        assert!(matches!(
            manager.notify_queued(task.id).await,
            Err(TaskManagerError::Frozen)
        ));

        let persisted = store
            .task_detail(task.id)
            .await
            .expect("read frozen-claim task")
            .expect("frozen-claim task exists")
            .task;
        let expected = match phase {
            ClaimPhase::PermitAcquired | ClaimPhase::HandleRegistered => TaskStatus::Queued,
            ClaimPhase::RunningCommitted => TaskStatus::Running,
            ClaimPhase::ActorLivenessAcquired
            | ClaimPhase::ClaimRetainedForReconciliation
            | ClaimPhase::TerminalDispatched
            | ClaimPhase::PendingReplayBeforeActorDelivery => {
                unreachable!("the claim-freeze matrix excludes terminal dispatch")
            }
        };
        assert_eq!(persisted.status, expected, "phase {phase:?}");
        assert_eq!(runner.starts.load(Ordering::SeqCst), 0, "phase {phase:?}");
        match phase {
            ClaimPhase::PermitAcquired | ClaimPhase::HandleRegistered => {
                assert_eq!(hooks.active_count(), 0, "phase {phase:?}");
                assert_eq!(hooks.available_permits(), 1, "phase {phase:?}");
            }
            ClaimPhase::RunningCommitted => {
                assert_eq!(
                    hooks.active_count(),
                    1,
                    "durable Running membership remains actor-owned while frozen"
                );
                assert_eq!(
                    hooks.available_permits(),
                    0,
                    "the durable Running membership retains its permit while frozen"
                );
            }
            ClaimPhase::ActorLivenessAcquired
            | ClaimPhase::ClaimRetainedForReconciliation
            | ClaimPhase::TerminalDispatched
            | ClaimPhase::PendingReplayBeforeActorDelivery => {
                unreachable!("the claim-freeze matrix excludes terminal dispatch")
            }
        }
    }
}
