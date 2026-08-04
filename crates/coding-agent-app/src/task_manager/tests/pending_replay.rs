use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn stale_pending_replay_attempt_messages_do_not_clear_freeze_or_duplicate_current_attempt() {
    let temp_dir = tempfile::tempdir().expect("create stale replay-attempt fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open stale replay-attempt store");
    store
        .migrate()
        .await
        .expect("migrate stale replay-attempt store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn stale replay-attempt dispatcher");
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
        .expect("construct stale replay-attempt writer controller"),
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
                "stale replay attempt messages are inert",
            )
            .expect("construct stale replay-attempt task"),
            background_deadline(),
        )
        .await
        .expect("create stale replay-attempt task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify stale replay-attempt actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("stale replay-attempt runner starts");

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
    let pending = wait_for_single_pending_record_review(&manager).await;
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 3)
        .await;
    let current = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect current replay attempt")
        .expect("stale replay-attempt task remains active");
    let current_attempt_id = current
        .pending_replay_attempt_id
        .expect("the paused typed replay has an actor-owned attempt ID");
    let stale_attempt_id = current_attempt_id
        .checked_sub(1)
        .expect("pending replay attempt IDs start above zero");
    let current_deadline = current
        .pending_replay_deadline
        .expect("the paused typed replay has an actor-owned deadline");

    manager
        .inject_pending_replay_retry_for_test(stale_attempt_id)
        .await
        .expect("inject stale replay retry");
    manager
        .inject_pending_replay_completion_for_test(
            stale_attempt_id,
            pending.clone(),
            Err(StoreWriterSubmitError::InvalidIdentity),
        )
        .await
        .expect("inject stale replay completion");

    let after_stale = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect state after stale replay messages")
        .expect("current replay attempt remains active");
    assert!(!after_stale.hard_frozen);
    assert!(after_stale.pending_replay_in_flight);
    assert_eq!(
        after_stale.pending_replay_attempt_id,
        Some(current_attempt_id),
        "stale messages cannot clear or replace the current attempt"
    );
    assert_eq!(
        after_stale.pending_replay_deadline,
        Some(current_deadline),
        "stale messages cannot refresh the current absolute deadline"
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::RecordReview,
        ),
        3,
        "stale messages cannot submit a duplicate replay"
    );
    assert_eq!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect canonical pending after stale replay messages"),
        vec![pending.clone()]
    );

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    tokio::time::timeout(Duration::from_secs(5), runner.review_applied.notified())
        .await
        .expect("the untouched current replay resolves normally");
    let applied = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect applied current replay")
        .expect("current replay task remains active until runner cleanup");
    assert!(!applied.hard_frozen);
    assert!(!applied.pending_replay_in_flight);
    assert_eq!(applied.pending_replay_attempt_id, None);
    assert!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect canonical ownership after current replay")
            .is_empty()
    );

    runner.finish_release.notify_one();
    wait_for_status(&store, task.id, TaskStatus::Interrupted).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn quiesce_reuses_original_absolute_deadline_across_busy_retries() {
    let temp_dir = tempfile::tempdir().expect("create quiesce replay-deadline fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open quiesce replay-deadline store");
    store
        .migrate()
        .await
        .expect("migrate quiesce replay-deadline store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn quiesce replay-deadline dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::RecordReview),
                count: 2,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailUnknownBeforeExecute,
                operation: Some(StoreWriterOperationKind::RecordReview),
                count: 2,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::BusyBeforeExecute,
                operation: Some(StoreWriterOperationKind::RecordReview),
                count: 6,
            },
        ])
        .expect("construct quiesce replay-deadline writer controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
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
        test_task_manager_launch_resources_for_repository(1, 1, &repository, temp_dir.path()),
        8,
    );
    let task = writer
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "quiesce keeps one replay deadline across busy retries",
            )
            .expect("construct quiesce replay-deadline task"),
            background_deadline(),
        )
        .await
        .expect("create quiesce replay-deadline task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify quiesce replay-deadline actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("quiesce replay-deadline runner starts");

    runner.review_release.notify_one();
    tokio::time::timeout(
        Duration::from_secs(5),
        controller.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1),
    )
    .await
    .expect("the original review reaches its pre-execute pause");
    let caller_deadline = Instant::now() + Duration::from_secs(4);
    let quiesce = tokio::spawn({
        let manager = manager.clone();
        async move { manager.quiesce_and_interrupt(caller_deadline).await }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if service_state.current().state == ServiceState::Quiescing {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("replay-deadline quiesce closes admission");
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        controller.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 2),
    )
    .await
    .expect("the original review reconciliation reaches its pre-execute pause");
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );

    let pending = wait_for_single_pending_record_review(&manager).await;
    let first_attempt = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = manager
                .active_stop_snapshot_for_test(task.id)
                .await
                .expect("inspect first busy replay attempt")
                .expect("busy replay task remains active");
            if snapshot.pending_replay_attempt_id.is_some() {
                return snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first busy replay attempt becomes actor-owned");
    let first_attempt_id = first_attempt
        .pending_replay_attempt_id
        .expect("first busy replay attempt has an ID");
    assert_eq!(
        first_attempt.pending_replay_deadline,
        Some(caller_deadline),
        "the replay born during quiesce adopts the caller's absolute deadline"
    );

    tokio::time::timeout(
        Duration::from_secs(5),
        controller.wait_until_reached(StoreWriterFaultPoint::BusyBeforeExecute, 6),
    )
    .await
    .expect("the first replay exhausts its bounded busy attempts");
    controller
        .arm_fault(StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::RecordReview),
            count: 1,
        })
        .expect("pause the replacement replay attempt");
    let replacement_pause = tokio::time::timeout(
        Duration::from_secs(5),
        controller.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 3),
    )
    .await;
    if replacement_pause.is_err() {
        let snapshot = manager
            .active_stop_snapshot_for_test(task.id)
            .await
            .expect("inspect timed-out replacement replay");
        let canonical = manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect timed-out canonical replay");
        panic!(
            "replacement replay did not pause; snapshot={snapshot:?}, canonical={canonical:?}, \
             busy_hits={}, pause_hits={}, quiesce_finished={}",
            controller.hit_count(
                StoreWriterFaultPoint::BusyBeforeExecute,
                StoreWriterOperationKind::RecordReview,
            ),
            controller.hit_count(
                StoreWriterFaultPoint::PauseBeforeExecute,
                StoreWriterOperationKind::RecordReview,
            ),
            quiesce.is_finished(),
        );
    }
    let retry_attempt = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect replacement busy replay attempt")
        .expect("replacement busy replay task remains active");
    assert_eq!(
        retry_attempt.pending_replay_attempt_id,
        first_attempt_id.checked_add(1),
        "a completed busy attempt is replaced by exactly one new replay ID"
    );
    assert_eq!(
        retry_attempt.pending_replay_deadline,
        Some(caller_deadline),
        "the replacement replay inherits, rather than refreshes, the absolute deadline"
    );
    assert!(!retry_attempt.hard_frozen);
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::BusyBeforeExecute,
            StoreWriterOperationKind::RecordReview,
        ),
        6,
        "the first replay exhausts exactly one bounded busy window"
    );

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    tokio::time::timeout(Duration::from_secs(5), runner.review_applied.notified())
        .await
        .expect("the replacement replay resolves before the original caller deadline");
    assert!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect replay-deadline canonical ownership")
            .is_empty()
    );
    let applied = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect applied replay-deadline state")
        .expect("replay-deadline task remains active until runner cleanup");
    assert!(!applied.pending_replay_in_flight);
    assert!(!applied.hard_frozen);

    runner.finish_release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(5), quiesce)
        .await
        .expect("replay-deadline quiesce completes")
        .expect("join replay-deadline quiesce")
        .expect("replay-deadline quiesce remains connected");
    assert!(matches!(result, QuiesceResult::Durable { .. }));
    wait_for_status(&store, task.id, TaskStatus::Interrupted).await;
    drop(pending);
}
