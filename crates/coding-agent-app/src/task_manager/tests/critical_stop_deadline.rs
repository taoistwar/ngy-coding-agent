use super::*;

#[cfg(feature = "test-support")]
#[test]
fn critical_stop_budget_override_is_instance_local_and_keeps_the_default() {
    let default = test_task_manager_launch_resources(1, 1);
    let extended = default
        .clone()
        .with_critical_stop_persistence_budget_for_test(Duration::from_secs(30));

    assert_eq!(
        default.critical_stop_persistence_budget,
        CRITICAL_STOP_PERSISTENCE_BUDGET
    );
    assert_eq!(
        extended.critical_stop_persistence_budget,
        Duration::from_secs(30)
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn critical_stop_deadline_freezes_while_writer_remains_in_flight() {
    let temp_dir = tempfile::tempdir().expect("create hung critical writer fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open hung critical writer store");
    store
        .migrate()
        .await
        .expect("migrate hung critical writer store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn hung critical writer dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::PersistStopIntentBatch),
            count: 1,
        }])
        .expect("construct hung critical writer controller"),
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
                "critical deadline does not depend on writer completion",
            )
            .expect("construct hung critical writer task"),
            background_deadline(),
        )
        .await
        .expect("create hung critical writer task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify hung critical writer actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("hung critical writer runner starts");

    manager.notify_storage_critical_for_test(vec![MonitoredStorageScope::RepositoryGit(
        repository.id,
    )]);
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let pending_before = manager
        .active_pending_stop_write_for_test(task.id)
        .await
        .expect("inspect hung critical write")
        .expect("hung critical identity is actor-owned");

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if manager.shutdown_latched_for_test() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the actor freezes at the absolute deadline while the backend remains in flight");

    assert_eq!(
        manager
            .active_pending_stop_write_for_test(task.id)
            .await
            .expect("inspect retained hung critical write"),
        Some(pending_before.clone()),
        "deadline expiry preserves the original identity, sequence, and request"
    );
    let frozen = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect hung critical ownership")
        .expect("hung critical task remains actor-owned");
    assert_eq!(frozen.stage, ActiveStopStageForTest::IntentWritePending);
    assert_eq!(frozen.active_count, 1);
    assert_eq!(frozen.available_permits, 0);
    assert!(!frozen.terminal_task_set);
    assert!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect hung critical canonical ownership")
            .is_empty(),
        "an admitted in-flight write is not manufactured into canonical unknown ownership"
    );
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load hung critical task")
            .expect("hung critical task exists")
            .task
            .status,
        TaskStatus::Running,
        "deadline expiry cannot manufacture a terminal task"
    );

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    let receipt = committed_stop_intent(&store, task.id).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if manager
                .active_stop_snapshot_for_test(task.id)
                .await
                .is_ok_and(|snapshot| {
                    snapshot.is_some_and(|snapshot| {
                        snapshot.stage == ActiveStopStageForTest::IntentDurable
                    })
                })
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the exact late backend completion advances the retained lineage");
    let (identity, exact_late_completion) = exact_late_stop_completion(&pending_before, receipt);
    manager
        .inject_stop_intent_completion_for_test(identity, exact_late_completion)
        .await
        .expect("inject duplicate exact completion after deadline freeze");

    assert!(matches!(
        manager.safety_snapshot_for_test().await,
        Err(TaskManagerError::Frozen)
    ));
    let after_late = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect late critical completion")
        .expect("late critical task remains actor-owned");
    assert_eq!(after_late.stage, ActiveStopStageForTest::IntentDurable);
    assert_eq!(after_late.active_count, 1);
    assert_eq!(after_late.available_permits, 0);
    assert!(!after_late.terminal_task_set);
    assert!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect late critical canonical ownership")
            .is_empty()
    );
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load late critical task")
            .expect("late critical task exists")
            .task
            .status,
        TaskStatus::Running
    );

    controller
        .arm_fault(StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::FinalizeStoppedTask),
            count: 1,
        })
        .expect("arm a final-stop probe after hard freeze");
    let next_sequence = after_late.next_mutation_sequence;
    runner.release.notify_one();
    let final_stop_reached = tokio::time::timeout(
        Duration::from_millis(500),
        controller.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 2),
    )
    .await;
    if final_stop_reached.is_ok() {
        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
    }
    assert!(
        final_stop_reached.is_err(),
        "a late exact stop receipt may update lineage after hard freeze but cannot start a final write"
    );
    let retained = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect hard-frozen durable stop")
        .expect("hard-frozen durable stop retains ownership");
    assert_eq!(retained.stage, ActiveStopStageForTest::IntentDurable);
    assert_eq!(retained.next_mutation_sequence, next_sequence);
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn already_expired_critical_stop_freezes_when_every_identity_is_deferred() {
    let temp_dir = tempfile::tempdir().expect("create expired deferred critical fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open expired deferred critical store");
    store
        .migrate()
        .await
        .expect("migrate expired deferred critical store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn expired deferred critical dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::RecordReview),
                count: 3,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailUnknownBeforeExecute,
                operation: Some(StoreWriterOperationKind::RecordReview),
                count: 2,
            },
        ])
        .expect("construct expired deferred critical writer controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
        controller.clone(),
    );
    let runner = Arc::new(StagedReviewStopRunner::default());
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
                "already expired critical stop behind unknown review",
            )
            .expect("construct expired deferred critical task"),
            background_deadline(),
        )
        .await
        .expect("create expired deferred critical task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify expired deferred critical actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("expired deferred critical runner starts");

    runner.review_release.notify_one();
    for expected_pause in 1..=2 {
        controller
            .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, expected_pause)
            .await;
        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
    }
    let predecessor = wait_for_single_pending_record_review(&manager).await;
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 3)
        .await;

    manager.notify_storage_critical_at_for_test(
        vec![MonitoredStorageScope::RepositoryGit(repository.id)],
        Instant::now() - Duration::from_secs(2),
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if manager.shutdown_latched_for_test() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("an already-expired critical fact freezes even when every identity is deferred");

    let deferred_before = manager
        .active_pending_stop_write_for_test(task.id)
        .await
        .expect("inspect expired deferred critical winner")
        .expect("expired deferred identity is actor-owned");
    assert_eq!(
        manager
            .active_pending_stop_write_for_test(task.id)
            .await
            .expect("inspect retained expired deferred winner"),
        Some(deferred_before),
        "expiry preserves the fixed deferred identity, sequence, and request"
    );
    let frozen = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect expired deferred ownership")
        .expect("expired deferred task remains actor-owned");
    assert_eq!(
        frozen.stage,
        ActiveStopStageForTest::IntentSubmissionDeferred
    );
    assert_eq!(frozen.active_count, 1);
    assert_eq!(frozen.available_permits, 0);
    assert!(!frozen.terminal_task_set);
    assert_eq!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect expired deferred canonical ownership"),
        vec![predecessor],
        "expiry cannot manufacture canonical ownership for an identity never accepted by ingress"
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::PersistStopIntentBatch,
        ),
        0,
        "an already-expired deferred identity never enters stop-intent ingress"
    );
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load expired deferred task")
            .expect("expired deferred task exists")
            .task
            .status,
        TaskStatus::Running
    );

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    runner.finish_release.notify_one();
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn critical_stop_deadline_expiry_is_a_no_op_after_the_identity_is_durable() {
    let temp_dir = tempfile::tempdir().expect("create stale critical expiry fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open stale critical expiry store");
    store
        .migrate()
        .await
        .expect("migrate stale critical expiry store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn stale critical expiry dispatcher");
    let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
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
                "durable critical identity ignores its stale deadline wake",
            )
            .expect("construct stale critical expiry task"),
            background_deadline(),
        )
        .await
        .expect("create stale critical expiry task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify stale critical expiry actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("stale critical expiry runner starts");

    manager.notify_storage_critical_for_test(vec![MonitoredStorageScope::RepositoryGit(
        repository.id,
    )]);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if manager
                .active_stop_snapshot_for_test(task.id)
                .await
                .is_ok_and(|snapshot| {
                    snapshot.is_some_and(|snapshot| {
                        snapshot.stage == ActiveStopStageForTest::IntentDurable
                    })
                })
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("critical identity becomes durable before its deadline");
    tokio::time::sleep(Duration::from_millis(1_100)).await;

    assert!(
        !manager.shutdown_latched_for_test(),
        "a stale keyed expiry cannot freeze an already-durable winner"
    );
    assert!(
        manager.safety_snapshot_for_test().await.is_ok(),
        "the stale deadline wake is an exact no-op after IntentDurable"
    );
    let durable = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect durable critical identity")
        .expect("durable critical task remains actor-owned");
    assert_eq!(durable.stage, ActiveStopStageForTest::IntentDurable);
    assert_eq!(durable.active_count, 1);
    assert_eq!(durable.available_permits, 0);
    assert!(!durable.terminal_task_set);
    assert!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect stale expiry canonical ownership")
            .is_empty()
    );
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load durable critical task")
            .expect("durable critical task exists")
            .task
            .status,
        TaskStatus::Running
    );

    runner.release.notify_one();
    wait_for_status(&store, task.id, TaskStatus::Failed).await;
}
