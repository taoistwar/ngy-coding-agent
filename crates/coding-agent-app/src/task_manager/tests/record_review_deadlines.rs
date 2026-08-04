use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn hard_quiesce_deadline_blocks_unreviewed_terminal_retry_identity() {
    let temp_dir = tempfile::tempdir().expect("create hard terminal deadline fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open hard terminal deadline store");
    store
        .migrate()
        .await
        .expect("migrate hard terminal deadline store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn hard terminal deadline dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::FinishTask),
                count: 7,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::BusyBeforeExecute,
                operation: Some(StoreWriterOperationKind::FinishTask),
                count: 6,
            },
        ])
        .expect("construct hard terminal deadline writer controller"),
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
                "hard quiesce blocks terminal N+1",
            )
            .expect("construct hard terminal deadline task"),
            background_deadline(),
        )
        .await
        .expect("create hard terminal deadline task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify hard terminal deadline actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("hard terminal deadline runner starts");
    runner.release.notify_one();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let original = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect original hard terminal write")
        .expect("hard terminal write remains active");
    let original_identity = original
        .pending_terminal_identity
        .expect("the original terminal identity is actor-owned");
    let original_deadline = original
        .pending_terminal_deadline
        .expect("the original terminal deadline is actor-owned");

    let quiesce_deadline = Instant::now() + Duration::from_millis(100);
    let clamped_deadline = original_deadline.min(quiesce_deadline);
    let quiesce = tokio::spawn({
        let manager = manager.clone();
        async move { manager.quiesce_and_interrupt(quiesce_deadline).await }
    });
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), quiesce)
            .await
            .expect("hard terminal deadline quiesce returns")
            .expect("join hard terminal deadline quiesce")
            .expect("hard terminal deadline manager remains connected"),
        QuiesceResult::Frozen {
            error: StoreWriterError::DeadlineElapsed,
            ..
        }
    ));
    for expected_attempt in 2..=6 {
        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
        tokio::time::timeout(
            Duration::from_secs(2),
            controller
                .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, expected_attempt),
        )
        .await
        .expect("the already-admitted writer attempt may finish its bounded Busy loop");
    }
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );

    let replacement_reached = tokio::time::timeout(
        Duration::from_millis(500),
        controller.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 7),
    )
    .await;
    if replacement_reached.is_ok() {
        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
    }
    assert!(
        replacement_reached.is_err(),
        "a Busy completion after the hard deadline must not allocate or submit terminal N+1"
    );
    let retained = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect retained hard terminal write")
        .expect("hard terminal ownership is retained");
    assert_eq!(retained.pending_terminal_identity, Some(original_identity));
    assert_eq!(retained.pending_terminal_deadline, Some(clamped_deadline));
    assert!(
        retained.hard_frozen,
        "hard expiry freezes without consuming the unresolved terminal write"
    );
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load hard terminal deadline task")
            .expect("hard terminal deadline task exists")
            .task
            .status,
        TaskStatus::Running,
        "hard expiry retains active ownership without a false terminal"
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn expired_record_review_retry_does_not_allocate_a_new_attempt_or_sequence() {
    let temp_dir = tempfile::tempdir().expect("create expired review retry fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open expired review retry store");
    store
        .migrate()
        .await
        .expect("migrate expired review retry store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn expired review retry dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::RecordReview),
                count: 7,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::BusyBeforeExecute,
                operation: Some(StoreWriterOperationKind::RecordReview),
                count: 6,
            },
        ])
        .expect("construct expired review retry writer controller"),
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
                "expired review cannot consume N+1",
            )
            .expect("construct expired review retry task"),
            background_deadline(),
        )
        .await
        .expect("create expired review retry task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify expired review retry actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("expired review retry runner starts");
    runner.review_release.notify_one();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let original = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect original expired review write")
        .expect("expired review write remains active");
    let _original_identity = original
        .pending_record_review_identity
        .expect("the original review identity is actor-owned");
    let _original_deadline = original
        .pending_record_review_deadline
        .expect("the original review deadline is actor-owned");

    let quiesce_deadline = Instant::now() + Duration::from_millis(100);
    let quiesce = tokio::spawn({
        let manager = manager.clone();
        async move { manager.quiesce_and_interrupt(quiesce_deadline).await }
    });
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), quiesce)
            .await
            .expect("expired review quiesce returns")
            .expect("join expired review quiesce")
            .expect("expired review manager remains connected"),
        QuiesceResult::Frozen {
            error: StoreWriterError::DeadlineElapsed,
            ..
        }
    ));
    for expected_attempt in 2..=6 {
        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
        tokio::time::timeout(
            Duration::from_secs(2),
            controller
                .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, expected_attempt),
        )
        .await
        .expect("the admitted review write may finish its bounded Busy loop");
    }
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    let replacement_reached = tokio::time::timeout(
        Duration::from_millis(500),
        controller.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 7),
    )
    .await;
    if replacement_reached.is_ok() {
        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
    }
    assert!(
        replacement_reached.is_err(),
        "an expired review completion must not submit N+1"
    );

    let retained = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect retained expired review")
        .expect("expired review ownership is retained");
    assert_eq!(retained.pending_record_review_identity, None);
    assert_eq!(retained.pending_record_review_deadline, None);
    assert_eq!(
        retained.next_typed_write_attempt_id, original.next_typed_write_attempt_id,
        "expiry is decided before allocating a typed-write attempt"
    );
    assert_eq!(
        retained.next_mutation_sequence, original.next_mutation_sequence,
        "expiry is decided before allocating a task mutation identity"
    );
    assert!(retained.hard_frozen);
    assert_eq!(retained.pending_record_review_write_count, 0);
    assert_eq!(retained.in_flight_mutations, 0);
    assert_eq!(retained.applied_record_review_count, 0);
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load expired review task")
            .expect("expired review task exists")
            .task
            .status,
        TaskStatus::Running
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn soft_quiesce_allows_one_record_review_retry_with_the_original_token() {
    let temp_dir = tempfile::tempdir().expect("create soft review retry fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open soft review retry store");
    store
        .migrate()
        .await
        .expect("migrate soft review retry store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn soft review retry dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::RecordReview),
                count: 7,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::BusyBeforeExecute,
                operation: Some(StoreWriterOperationKind::RecordReview),
                count: 6,
            },
        ])
        .expect("construct soft review retry writer controller"),
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
                "soft quiesce preserves a review continuation",
            )
            .expect("construct soft review retry task"),
            background_deadline(),
        )
        .await
        .expect("create soft review retry task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify soft review retry actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("soft review retry runner starts");
    runner.review_release.notify_one();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let original = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect original review write")
        .expect("original review write remains active");
    let original_identity = original
        .pending_record_review_identity
        .expect("the original review identity is actor-owned");
    let original_deadline = original
        .pending_record_review_deadline
        .expect("the original review deadline is actor-owned");
    assert_eq!(original.pending_record_review_write_count, 1);
    assert_eq!(original.in_flight_mutations, 1);

    let quiesce = tokio::spawn({
        let manager = manager.clone();
        async move {
            manager
                .quiesce_and_interrupt(Instant::now() + Duration::from_secs(10))
                .await
        }
    });
    for expected_attempt in 2..=6 {
        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
        tokio::time::timeout(
            Duration::from_secs(2),
            controller
                .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, expected_attempt),
        )
        .await
        .expect("the original review attempt completes its bounded Busy loop");
    }
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    tokio::time::timeout(
        Duration::from_secs(2),
        controller.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 7),
    )
    .await
    .expect("soft quiesce admits exactly one actor-owned N+1 review continuation");
    let retry = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect N+1 review write")
        .expect("N+1 review write remains active");
    let retry_identity = retry
        .pending_record_review_identity
        .expect("the N+1 review identity is actor-owned");
    assert_eq!(
        retry_identity.sequence.get(),
        original_identity.sequence.get() + 1,
        "RetryNext owns exactly N+1"
    );
    assert_eq!(
        retry.pending_record_review_deadline,
        Some(original_deadline),
        "RetryNext inherits the first absolute deadline"
    );
    assert_eq!(retry.pending_record_review_retry_available, Some(false));
    assert_eq!(
        retry.in_flight_mutations, 1,
        "N to N+1 reuses one logical in-flight token"
    );
    assert!(
        !quiesce.is_finished(),
        "review retry remains a typed barrier during soft quiesce"
    );
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    tokio::time::timeout(Duration::from_secs(5), runner.review_applied.notified())
        .await
        .expect("the retried review resolves its original caller");
    assert!(
        runner
            .review_result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some_and(|result| result.is_ok()),
        "the original logical review response is retained across N to N+1"
    );
    runner.finish_release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), quiesce)
        .await
        .expect("soft review retry quiesce completes")
        .expect("join soft review retry quiesce")
        .expect("soft review retry manager remains connected");
}
