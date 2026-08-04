use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn generic_finalize_is_superseded_by_a_new_typed_barrier_and_stale_finalize_is_a_no_op() {
    let temp_dir =
        tempfile::tempdir().expect("create generic finalize supersede fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open generic finalize supersede store");
    store
        .migrate()
        .await
        .expect("migrate generic finalize supersede store");
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn generic finalize supersede dispatcher");
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
    let attempt_id = 41;
    let barrier_epoch = 7;
    manager
        .install_generic_recovery_lease_for_test(attempt_id, barrier_epoch)
        .await
        .expect("install one checked generic recovery lease");
    let task_id = TaskId::new();
    let pending = PendingDurableResult::RecordReview {
        identity: TaskMutationIdentity {
            task_id,
            sequence: MutationSequence::new(
                NonZeroU64::new(1).expect("one is a nonzero mutation sequence"),
            ),
            kind: DurableOperationKind::RecordReview,
        },
        request: RecordReviewRequest {
            task_id,
            expected_repository_id: RepositoryId::new(),
            expected_attempt: 1,
            evidence: staged_review_evidence(),
        },
    };
    manager
        .install_canonical_pending_for_test(pending.clone())
        .await
        .expect("install a newer canonical typed barrier");
    let blocked = manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect typed barrier behind the generic lease");
    assert_eq!(blocked.generic_recovery_attempt_id, Some(attempt_id));
    assert_eq!(blocked.generic_recovery_barrier_epoch, Some(barrier_epoch));
    assert!(blocked.barrier_epoch > barrier_epoch);
    assert_eq!(blocked.pending_durable_result_count, 1);
    assert!(!blocked.pending_replay_in_flight);
    assert!(!blocked.hard_frozen);
    let recovery = coding_agent_store::RecoveryOutcome {
        interrupted_count: 0,
        first_event_id: None,
        last_event_id: None,
        high_watermark: EventCursor::ZERO,
    };

    assert_eq!(
        manager
            .inject_finalize_degraded_for_test(attempt_id, barrier_epoch, recovery.clone())
            .await
            .expect("inject exact generic finalize behind a newer typed barrier"),
        Err(DegradedCoordinatorError::Superseded),
        "a lease whose epoch acquired a newer exact barrier is benignly superseded"
    );
    let after_superseded = manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect benignly superseded generic finalize");
    assert_eq!(
        after_superseded.generic_recovery_attempt_id,
        Some(attempt_id)
    );
    assert_eq!(after_superseded.pending_durable_result_count, 1);
    assert!(!after_superseded.hard_frozen);

    assert_eq!(
        manager
            .inject_finalize_degraded_for_test(
                attempt_id
                    .checked_add(1)
                    .expect("the current attempt has a stale successor ID"),
                barrier_epoch,
                recovery,
            )
            .await
            .expect("inject a stale generic finalize"),
        Err(DegradedCoordinatorError::Superseded),
        "a stale finalize cannot freeze, publish, release, or replace the current lease"
    );
    let after_stale = manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect stale generic finalize no-op");
    assert_eq!(after_stale.generic_recovery_attempt_id, Some(attempt_id));
    assert_eq!(after_stale.pending_durable_result_count, 1);
    assert!(!after_stale.hard_frozen);
    assert_eq!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect retained canonical typed barrier"),
        vec![pending]
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn generic_finalize_is_superseded_by_an_out_of_actor_critical_latch() {
    let temp_dir = tempfile::tempdir().expect("create generic critical-race fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open generic critical-race store");
    store
        .migrate()
        .await
        .expect("migrate generic critical-race store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn generic critical-race dispatcher");
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
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "generic finalize cannot erase a critical latch",
            )
            .expect("construct generic critical-race task"),
            background_deadline(),
        )
        .await
        .expect("create generic critical-race task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify generic critical-race actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("generic critical-race runner starts");

    let attempt_id = 41;
    let barrier_epoch = 7;
    manager
        .install_generic_recovery_lease_for_test(attempt_id, barrier_epoch)
        .await
        .expect("install generic critical-race lease");
    runner.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = manager
                .safety_snapshot_for_test()
                .await
                .expect("inspect generic critical-race cleanup");
            if snapshot.active_count == 1 && snapshot.recovery_release_ready_count == 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("generic critical-race active ownership becomes release-ready");
    let generic = writer
        .interrupt_remaining_after_stops(shutdown_failure(), background_deadline())
        .await
        .expect("durably recover generic critical-race task")
        .value;
    let recovery = coding_agent_store::RecoveryOutcome {
        interrupted_count: generic.interrupted_count,
        first_event_id: generic.first_event_id,
        last_event_id: generic.last_event_id,
        high_watermark: generic.high_watermark,
    };
    let (finalization_reached, finalization_release) = manager
        .pause_next_degraded_finalization_for_test()
        .await
        .expect("arm generic critical-race finalization pause");
    let finalize = tokio::spawn({
        let manager = manager.clone();
        let recovery = recovery.clone();
        async move {
            manager
                .inject_finalize_degraded_for_test(attempt_id, barrier_epoch, recovery)
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), finalization_reached)
        .await
        .expect("generic finalization reaches the actor after durable recovery")
        .expect("generic finalization pause remains connected");

    manager.notify_storage_critical_at_for_test(
        vec![MonitoredStorageScope::RepositoryGit(repository.id)],
        Instant::now(),
    );
    assert_eq!(
        manager.safety_registry_snapshot_for_test(),
        SafetyRegistrySnapshotForTest {
            entry_count: 1,
            pending_critical_count: 1,
            safety_latched_count: 1,
        },
        "critical acceptance is synchronously visible while the actor is paused"
    );
    finalization_release
        .send(())
        .expect("release generic critical-race finalization");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), finalize)
            .await
            .expect("generic critical-race finalization returns")
            .expect("join generic critical-race finalization")
            .expect("generic critical-race manager remains connected"),
        Err(DegradedCoordinatorError::Superseded),
        "a critical latch accepted outside the actor supersedes the generic finalization"
    );
    assert_eq!(
        manager.safety_registry_snapshot_for_test(),
        SafetyRegistrySnapshotForTest {
            entry_count: 1,
            pending_critical_count: 0,
            safety_latched_count: 1,
        },
        "the accepted critical fact moves into the actor-owned typed stop without releasing ownership"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = manager
                .safety_snapshot_for_test()
                .await
                .expect("inspect generic critical-race typed stop");
            if snapshot.active_count == 0 && snapshot.available_permits == 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("critical typed stop and projection release exact ownership");
    assert_eq!(
        manager.safety_registry_snapshot_for_test(),
        SafetyRegistrySnapshotForTest {
            entry_count: 0,
            pending_critical_count: 0,
            safety_latched_count: 0,
        }
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn quiesce_recovery_retries_after_an_out_of_actor_critical_latch() {
    let temp_dir = tempfile::tempdir().expect("create quiesce critical-race fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open quiesce critical-race store");
    store
        .migrate()
        .await
        .expect("migrate quiesce critical-race store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn quiesce critical-race dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::InterruptRemainingAfterStops),
            count: 2,
        }])
        .expect("construct quiesce critical-race writer controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
        controller.clone(),
    );
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
                "quiesce recovery cannot erase a critical latch",
            )
            .expect("construct quiesce critical-race task"),
            background_deadline(),
        )
        .await
        .expect("create quiesce critical-race task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify quiesce critical-race actor");
    tokio::time::timeout(Duration::from_secs(5), async {
        while runner.starts.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("quiesce critical-race runner starts");
    let (finalization_reached, finalization_release) = manager
        .pause_next_quiesce_finalization_for_test()
        .await
        .expect("arm quiesce critical-race finalization pause");
    let quiesce = tokio::spawn({
        let manager = manager.clone();
        async move {
            manager
                .quiesce_and_interrupt(Instant::now() + Duration::from_secs(10))
                .await
        }
    });
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    tokio::time::timeout(Duration::from_secs(5), finalization_reached)
        .await
        .expect("quiesce finalization reaches the actor after durable recovery")
        .expect("quiesce finalization pause remains connected");

    manager.notify_storage_critical_at_for_test(
        vec![MonitoredStorageScope::RepositoryGit(repository.id)],
        Instant::now(),
    );
    assert_eq!(
        manager.safety_registry_snapshot_for_test(),
        SafetyRegistrySnapshotForTest {
            entry_count: 1,
            pending_critical_count: 1,
            safety_latched_count: 1,
        },
        "critical acceptance is visible before quiesce final release"
    );
    finalization_release
        .send(())
        .expect("release quiesce critical-race finalization");
    tokio::time::timeout(
        Duration::from_secs(3),
        controller.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 2),
    )
    .await
    .expect("the superseded quiesce recovery retries after critical typed proof");
    assert!(
        !quiesce.is_finished(),
        "quiesce cannot return durable from the superseded first recovery"
    );
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    let result = tokio::time::timeout(Duration::from_secs(5), quiesce)
        .await
        .expect("quiesce critical-race recovery completes")
        .expect("join quiesce critical-race recovery")
        .expect("quiesce critical-race manager remains connected");
    assert!(matches!(result, QuiesceResult::Durable { .. }));
    assert_eq!(
        manager.safety_registry_snapshot_for_test(),
        SafetyRegistrySnapshotForTest {
            entry_count: 0,
            pending_critical_count: 0,
            safety_latched_count: 0,
        },
        "critical typed stop and projection consume the fact before durable quiesce"
    );
}
