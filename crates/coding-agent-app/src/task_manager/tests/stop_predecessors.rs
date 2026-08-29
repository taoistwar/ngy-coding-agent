use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn critical_stop_waits_for_an_established_unknown_predecessor_before_first_ingress() {
    let temp_dir =
        tempfile::tempdir().expect("create deferred critical predecessor fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open deferred critical predecessor store");
    store
        .migrate()
        .await
        .expect("migrate deferred critical predecessor store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn deferred critical predecessor dispatcher");
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
        .expect("construct deferred critical predecessor writer controller"),
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
                "critical after established unknown predecessor",
            )
            .expect("construct deferred critical predecessor task"),
            background_deadline(),
        )
        .await
        .expect("create deferred critical predecessor task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify deferred critical predecessor actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("deferred critical predecessor runner starts");

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
    manager.notify_storage_critical_for_test(vec![MonitoredStorageScope::RepositoryGit(
        repository.id,
    )]);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = manager
                .active_stop_snapshot_for_test(task.id)
                .await
                .expect("inspect deferred critical stop")
                .expect("deferred critical task remains active");
            if snapshot.stage != ActiveStopStageForTest::NoWinner {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("critical winner becomes actor-owned while predecessor is unresolved");
    assert_eq!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect deferred critical canonical ownership"),
        vec![predecessor],
        "an identity not yet accepted by ingress must never become canonical pending"
    );

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    tokio::time::timeout(Duration::from_secs(5), runner.review_applied.notified())
        .await
        .expect("predecessor replay resolves the original review caller");
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
    .expect("deferred critical identity enters ingress after its predecessor resolves");

    runner.finish_release.notify_one();
    wait_for_status(&store, task.id, TaskStatus::Failed).await;
    let detail = store
        .task_detail(task.id)
        .await
        .expect("load deferred critical terminal")
        .expect("deferred critical task exists");
    assert_eq!(
        detail.task.failure,
        Some(TaskFailure {
            code: "DISK_PRESSURE_CRITICAL".to_owned(),
            message: "critical disk pressure stopped the task".to_owned(),
            retryable: true,
        })
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn user_stop_accepted_after_lookup_waits_for_an_established_unknown_predecessor() {
    let temp_dir = tempfile::tempdir().expect("create deferred user predecessor fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open deferred user predecessor store");
    store
        .migrate()
        .await
        .expect("migrate deferred user predecessor store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn deferred user predecessor dispatcher");
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
        .expect("construct deferred user predecessor writer controller"),
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
                "user stop after established unknown predecessor",
            )
            .expect("construct deferred user predecessor task"),
            background_deadline(),
        )
        .await
        .expect("create deferred user predecessor task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify deferred user predecessor actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("deferred user predecessor runner starts");

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
    let cancel = tokio::spawn({
        let manager = manager.clone();
        async move {
            manager
                .inject_running_user_cancel_after_lookup_for_test(task.id)
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if manager
                .active_stop_snapshot_for_test(task.id)
                .await
                .is_ok_and(|snapshot| {
                    snapshot.is_some_and(|snapshot| {
                        snapshot.stage == ActiveStopStageForTest::IntentSubmissionDeferred
                    })
                })
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("late lookup fixes the user winner identity without entering ingress");
    assert_eq!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect deferred user canonical ownership"),
        vec![predecessor],
        "the fixed but unregistered user identity is not canonical pending"
    );

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    tokio::time::timeout(Duration::from_secs(5), runner.review_applied.notified())
        .await
        .expect("user predecessor replay resolves the review caller");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), cancel)
            .await
            .expect("deferred user cancel becomes durable")
            .expect("join deferred user cancel")
            .expect("deferred user cancel succeeds"),
        CancelOutcome::Accepted { task: accepted } if accepted.id == task.id
    ));

    runner.finish_release.notify_one();
    wait_for_status(&store, task.id, TaskStatus::Cancelled).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn mixed_critical_batch_submits_the_unblocked_peer_without_manufacturing_gap_pending() {
    let temp_dir = tempfile::tempdir().expect("create mixed critical fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open mixed critical store");
    store.migrate().await.expect("migrate mixed critical store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 32)
        .await
        .expect("spawn mixed critical dispatcher");
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
        .expect("construct mixed critical writer controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        16,
        controller.clone(),
    );
    let runner = Arc::new(StagedReviewStopRunner::default());
    let service_state = ServiceStateController::new(ServiceState::Ready);
    let manager = TaskManagerHandle::spawn(
        store.clone(),
        writer.clone(),
        dispatcher,
        service_state.clone(),
        runner.clone(),
        test_task_manager_launch_resources_for_repository(2, 2, &repository, temp_dir.path())
            .with_critical_stop_persistence_budget_for_test(Duration::from_secs(30)),
        16,
    );
    let mut tasks = Vec::new();
    for prompt in ["mixed critical first", "mixed critical second"] {
        tasks.push(
            writer
                .create_task(
                    NewTask::try_new(ClientRequestId::new(), repository.id, prompt)
                        .expect("construct mixed critical task"),
                    background_deadline(),
                )
                .await
                .expect("create mixed critical task")
                .value
                .task()
                .clone(),
        );
    }
    manager
        .notify_queued(tasks[0].id)
        .await
        .expect("notify mixed critical actor");
    for task in &tasks {
        wait_for_status(&store, task.id, TaskStatus::Running).await;
    }

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
    let blocked_task_id = match &predecessor {
        PendingDurableResult::RecordReview { identity, .. } => identity.task_id,
        _ => unreachable!("the helper returns only RecordReview"),
    };
    let peer_task_id = tasks
        .iter()
        .find(|task| task.id != blocked_task_id)
        .expect("mixed critical fixture has one unblocked peer")
        .id;
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 3)
        .await;
    manager.notify_storage_critical_for_test(vec![MonitoredStorageScope::RepositoryGit(
        repository.id,
    )]);
    let ready = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let blocked = manager
                .active_stop_snapshot_for_test(blocked_task_id)
                .await
                .expect("inspect mixed blocked task")
                .expect("mixed blocked task remains active");
            let peer = manager
                .active_stop_snapshot_for_test(peer_task_id)
                .await
                .expect("inspect mixed peer task")
                .expect("mixed peer task remains active");
            if blocked.stage == ActiveStopStageForTest::IntentSubmissionDeferred
                && peer.stage == ActiveStopStageForTest::IntentWritePending
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    if ready.is_err() {
        let blocked = manager.active_stop_snapshot_for_test(blocked_task_id).await;
        let peer = manager.active_stop_snapshot_for_test(peer_task_id).await;
        let safety = manager.safety_snapshot_for_test().await;
        let pending = manager.pending_durable_results_for_test().await;
        panic!(
            "the unblocked critical peer was not independently accepted by urgent ingress: \
             blocked={blocked:?}, peer={peer:?}, service_state={:?}, \
             shutdown_latched={}, safety={safety:?}, pending={pending:?}, \
             record_review_pause_hits={}, stop_pause_hits={}",
            service_state.current(),
            manager.shutdown_latched_for_test(),
            controller.hit_count(
                StoreWriterFaultPoint::PauseBeforeExecute,
                StoreWriterOperationKind::RecordReview,
            ),
            controller.hit_count(
                StoreWriterFaultPoint::PauseBeforeExecute,
                StoreWriterOperationKind::PersistStopIntentBatch,
            ),
        );
    }
    assert_eq!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect mixed critical canonical ownership"),
        vec![predecessor],
        "neither fixed deferred identity nor unblocked peer becomes false pending"
    );

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    tokio::time::timeout(Duration::from_secs(5), runner.review_applied.notified())
        .await
        .expect("mixed blocked predecessor resolves");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let blocked_is_durable = manager
                .active_stop_snapshot_for_test(blocked_task_id)
                .await
                .is_ok_and(|snapshot| {
                    snapshot.is_some_and(|snapshot| {
                        snapshot.stage == ActiveStopStageForTest::IntentDurable
                    })
                });
            let peer_is_durable = manager
                .active_stop_snapshot_for_test(peer_task_id)
                .await
                .is_ok_and(|snapshot| {
                    snapshot.is_some_and(|snapshot| {
                        snapshot.stage == ActiveStopStageForTest::IntentDurable
                    })
                });
            if blocked_is_durable && peer_is_durable {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("mixed blocked critical identity enters after predecessor");

    runner.review_release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), runner.review_applied.notified())
        .await
        .expect("mixed peer exits its review gate after its stop winner");
    runner.finish_release.notify_waiters();
    for task in &tasks {
        wait_for_status(&store, task.id, TaskStatus::Failed).await;
    }
}
