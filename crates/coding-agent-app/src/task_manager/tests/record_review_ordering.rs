use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn concurrent_record_reviews_keep_two_logical_tokens_until_each_exact_completion() {
    let temp_dir = tempfile::tempdir().expect("create concurrent review fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open concurrent review store");
    store
        .migrate()
        .await
        .expect("migrate concurrent review store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn concurrent review dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::RecordReview),
            count: 2,
        }])
        .expect("construct concurrent review controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
        controller.clone(),
    );
    let runner = Arc::new(ConcurrentReviewRunner::default());
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
                "two concurrent logical reviews",
            )
            .expect("construct concurrent review task"),
            background_deadline(),
        )
        .await
        .expect("create concurrent review task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify concurrent review actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("concurrent review runner starts");
    runner.review_release.notify_one();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let both_pending = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = manager
                .active_stop_snapshot_for_test(task.id)
                .await
                .expect("inspect concurrent review writes")
                .expect("concurrent review task remains active");
            if snapshot.pending_record_review_write_count == 2 {
                return snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both logical reviews become actor-owned");
    assert_eq!(both_pending.in_flight_mutations, 2);
    assert_eq!(both_pending.pending_record_review_identity, None);

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        controller.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 2),
    )
    .await
    .expect("the second concurrent review reaches the writer");
    let one_pending = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect second logical review")
        .expect("second logical review remains active");
    assert_eq!(one_pending.pending_record_review_write_count, 1);
    assert_eq!(one_pending.in_flight_mutations, 1);
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    tokio::time::timeout(Duration::from_secs(5), runner.reviews_applied.notified())
        .await
        .expect("both concurrent review callers resolve");
    let results = (*runner
        .review_results
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner))
    .expect("concurrent review results were captured");
    assert!(results.0.is_ok());
    assert!(results.1.is_ok());
    runner.finish_release.notify_one();
    wait_for_status(&store, task.id, TaskStatus::Cancelled).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn successor_review_allocates_only_after_unknown_predecessor_replay_converges() {
    let temp_dir = tempfile::tempdir().expect("create deferred successor review fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open deferred successor review store");
    store
        .migrate()
        .await
        .expect("migrate deferred successor review store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn deferred successor review dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::RecordReview),
                count: 4,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailUnknownBeforeExecute,
                operation: Some(StoreWriterOperationKind::RecordReview),
                count: 2,
            },
        ])
        .expect("construct deferred successor review controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
        controller.clone(),
    );
    let runner = Arc::new(ConcurrentReviewRunner::default());
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
                "successor waits for unknown predecessor replay",
            )
            .expect("construct deferred successor review task"),
            background_deadline(),
        )
        .await
        .expect("create deferred successor review task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify deferred successor review actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("deferred successor review runner starts");
    runner.review_release.notify_one();
    for expected_pause in 1..=2 {
        tokio::time::timeout(
            Duration::from_secs(5),
            controller
                .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, expected_pause),
        )
        .await
        .expect("the predecessor reaches each bounded unknown attempt");
        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
    }
    let _pending = wait_for_single_pending_record_review(&manager).await;
    tokio::time::timeout(
        Duration::from_secs(5),
        controller.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 3),
    )
    .await
    .expect("the predecessor canonical replay reaches the writer");
    let blocked = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect deferred successor behind canonical replay")
        .expect("deferred successor task remains active");
    assert_eq!(blocked.pending_record_review_replay_count, 1);
    assert_eq!(blocked.pending_record_review_write_count, 1);
    assert_eq!(blocked.pending_record_review_identity, None);
    assert_eq!(blocked.in_flight_mutations, 2);
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::RecordReview,
        ),
        3,
        "the successor has not been submitted while the predecessor replay is unresolved"
    );

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        controller.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 4),
    )
    .await
    .expect("the successor submits after predecessor replay convergence");
    let submitted = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect submitted successor review")
        .expect("submitted successor task remains active");
    assert_eq!(submitted.pending_record_review_replay_count, 0);
    assert_eq!(submitted.pending_record_review_write_count, 1);
    assert!(submitted.pending_record_review_identity.is_some());
    assert_eq!(submitted.in_flight_mutations, 1);
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    tokio::time::timeout(Duration::from_secs(5), runner.reviews_applied.notified())
        .await
        .expect("both logical reviews resolve after ordered replay");
    let results = (*runner
        .review_results
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner))
    .expect("deferred successor results were captured");
    assert!(results.0.is_ok());
    assert!(results.1.is_ok());
    runner.finish_release.notify_one();
    wait_for_status(&store, task.id, TaskStatus::Interrupted).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn runner_event_and_record_review_share_one_bidirectional_fifo() {
    for review_first in [true, false] {
        let temp_dir = tempfile::tempdir().expect("create bidirectional FIFO fixture directory");
        let store = Store::open(temp_dir.path().join("store.sqlite3"))
            .await
            .expect("open bidirectional FIFO store");
        store
            .migrate()
            .await
            .expect("migrate bidirectional FIFO store");
        let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
            .await
            .expect("spawn bidirectional FIFO dispatcher");
        let controller = Arc::new(
            StoreWriterTestController::try_new(std::iter::empty::<StoreWriterFaultSpec>())
                .expect("construct bidirectional FIFO controller"),
        );
        let writer = StoreWriterHandle::spawn_with_test_controller(
            store.clone(),
            Arc::new(dispatcher.clone()),
            8,
            controller.clone(),
        );
        let runner = Arc::new(BidirectionalFifoRunner::new(review_first));
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
                    format!("bidirectional FIFO review-first={review_first}"),
                )
                .expect("construct bidirectional FIFO task"),
                background_deadline(),
            )
            .await
            .expect("create bidirectional FIFO task")
            .value
            .task()
            .clone();
        manager
            .notify_queued(task.id)
            .await
            .expect("notify bidirectional FIFO actor");
        tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
            .await
            .expect("bidirectional FIFO runner starts");
        for operation in [
            StoreWriterOperationKind::RecordReview,
            StoreWriterOperationKind::AppendRunningEvent,
        ] {
            controller
                .arm_fault(StoreWriterFaultSpec {
                    point: StoreWriterFaultPoint::PauseBeforeExecute,
                    operation: Some(operation),
                    count: 1,
                })
                .expect("arm bidirectional FIFO pause");
        }
        runner.release.notify_one();

        tokio::time::timeout(
            Duration::from_secs(5),
            controller.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1),
        )
        .await
        .expect("the first bidirectional FIFO mutation reaches the writer");
        let queued = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = manager
                    .active_stop_snapshot_for_test(task.id)
                    .await
                    .expect("inspect bidirectional FIFO ownership")
                    .expect("bidirectional FIFO task remains active");
                if snapshot.in_flight_mutations == 2
                    && snapshot.pending_runner_event_write_count == 1
                    && snapshot.pending_record_review_write_count == 1
                {
                    return snapshot;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both bidirectional FIFO logical mutations become actor-owned");
        if review_first {
            assert!(queued.pending_record_review_identity.is_some());
            assert_eq!(queued.pending_runner_event_identity, None);
        } else {
            assert!(queued.pending_runner_event_identity.is_some());
            assert_eq!(queued.pending_record_review_identity, None);
        }
        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                controller.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 2,),
            )
            .await
            .is_err(),
            "only the logical FIFO head may be submitted"
        );
        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
        tokio::time::timeout(
            Duration::from_secs(5),
            controller.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 2),
        )
        .await
        .expect("the second mutation submits only after the first completes");
        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
        tokio::time::timeout(Duration::from_secs(5), runner.completed.notified())
            .await
            .expect("both bidirectional FIFO callers complete");
        let results = (*runner
            .results
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner))
        .expect("bidirectional FIFO results were captured");
        assert!(results.0.is_ok());
        assert!(results.1.is_ok());
        runner.finish_release.notify_one();
        wait_for_status(&store, task.id, TaskStatus::Cancelled).await;
    }
}
