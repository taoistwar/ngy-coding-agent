use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn unreviewed_terminal_replay_same_keeps_identity_and_first_deadline() {
    let temp_dir = tempfile::tempdir().expect("create unreviewed replay fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open unreviewed replay store");
    store
        .migrate()
        .await
        .expect("migrate unreviewed replay store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn unreviewed replay dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::FinishTask),
                count: 3,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailUnknownBeforeExecute,
                operation: Some(StoreWriterOperationKind::FinishTask),
                count: 2,
            },
        ])
        .expect("construct unreviewed replay controller"),
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
                "unreviewed terminal replays N",
            )
            .expect("construct unreviewed replay task"),
            background_deadline(),
        )
        .await
        .expect("create unreviewed replay task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify unreviewed replay actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("unreviewed replay runner starts");
    runner.release.notify_one();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let original = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect original unreviewed terminal")
        .expect("original unreviewed terminal remains active");
    let identity = original
        .pending_terminal_identity
        .expect("original unreviewed identity is actor-owned");
    let deadline = original
        .pending_terminal_deadline
        .expect("original unreviewed deadline is actor-owned");
    let attempt_id = original
        .pending_terminal_attempt_id
        .expect("original unreviewed attempt is actor-owned");

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 2)
        .await;
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 3)
        .await;
    let replay = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect reconciled unreviewed terminal")
        .expect("reconciled unreviewed terminal remains active");
    assert_eq!(replay.pending_terminal_identity, Some(identity));
    assert_eq!(replay.pending_terminal_deadline, Some(deadline));
    assert_eq!(
        replay.pending_terminal_stage,
        Some(TerminalWriteStage::ReconcileSamePending)
    );
    assert_ne!(replay.pending_terminal_attempt_id, Some(attempt_id));
    assert_eq!(replay.pending_terminal_retry_available, Some(true));

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    wait_for_status(&store, task.id, TaskStatus::Failed).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn reviewed_terminal_replay_same_keeps_identity_and_first_deadline() {
    let temp_dir = tempfile::tempdir().expect("create reviewed replay fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open reviewed replay store");
    store
        .migrate()
        .await
        .expect("migrate reviewed replay store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn reviewed replay dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::FinalizeReviewedTask),
                count: 3,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailUnknownBeforeExecute,
                operation: Some(StoreWriterOperationKind::FinalizeReviewedTask),
                count: 2,
            },
        ])
        .expect("construct reviewed replay controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
        controller.clone(),
    );
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
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "reviewed terminal replays N",
            )
            .expect("construct reviewed replay task"),
            background_deadline(),
        )
        .await
        .expect("create reviewed replay task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify reviewed replay actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("reviewed replay runner starts");
    runner.release.notify_one();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let original = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect original reviewed terminal")
        .expect("original reviewed terminal remains active");
    let identity = original
        .pending_terminal_identity
        .expect("original reviewed identity is actor-owned");
    let deadline = original
        .pending_terminal_deadline
        .expect("original reviewed deadline is actor-owned");

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 2)
        .await;
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 3)
        .await;
    let replay = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect reconciled reviewed terminal")
        .expect("reconciled reviewed terminal remains active");
    assert_eq!(replay.pending_terminal_identity, Some(identity));
    assert_eq!(replay.pending_terminal_deadline, Some(deadline));
    assert_eq!(
        replay.pending_terminal_stage,
        Some(TerminalWriteStage::ReconcileSamePending)
    );
    assert_eq!(replay.pending_terminal_retry_available, Some(true));

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    wait_for_status(&store, task.id, TaskStatus::Completed).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn unreviewed_terminal_retry_next_owns_n_plus_one_with_the_first_deadline() {
    let temp_dir = tempfile::tempdir().expect("create unreviewed retry-next fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open unreviewed retry-next store");
    store
        .migrate()
        .await
        .expect("migrate unreviewed retry-next store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn unreviewed retry-next dispatcher");
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
        .expect("construct unreviewed retry-next controller"),
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
                "unreviewed terminal retries N plus one",
            )
            .expect("construct unreviewed retry-next task"),
            background_deadline(),
        )
        .await
        .expect("create unreviewed retry-next task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify unreviewed retry-next actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("unreviewed retry-next runner starts");
    runner.release.notify_one();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let original = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect original unreviewed retry")
        .expect("original unreviewed retry remains active");
    let original_identity = original
        .pending_terminal_identity
        .expect("original unreviewed retry identity is actor-owned");
    let original_deadline = original
        .pending_terminal_deadline
        .expect("original unreviewed retry deadline is actor-owned");
    let original_attempt_id = original
        .pending_terminal_attempt_id
        .expect("original unreviewed retry attempt is actor-owned");
    let original_stage = original
        .pending_terminal_stage
        .expect("original unreviewed retry stage is actor-owned");
    for expected_attempt in 2..=7 {
        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
        controller
            .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, expected_attempt)
            .await;
    }
    let retry = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect N+1 unreviewed retry")
        .expect("N+1 unreviewed retry remains active");
    let retry_identity = retry
        .pending_terminal_identity
        .expect("N+1 unreviewed retry identity is actor-owned");
    assert_eq!(
        retry_identity.sequence.get(),
        original_identity.sequence.get() + 1
    );
    assert_eq!(retry.pending_terminal_deadline, Some(original_deadline));
    assert_eq!(retry.pending_terminal_retry_available, Some(false));
    let retry_attempt_id = retry
        .pending_terminal_attempt_id
        .expect("N+1 unreviewed retry attempt is actor-owned");
    let retry_stage = retry
        .pending_terminal_stage
        .expect("N+1 unreviewed retry stage is actor-owned");
    manager
        .inject_terminal_write_completion_for_test(
            task.id,
            original_attempt_id,
            original_identity,
            original_stage,
            TerminalWriteCompletion::Unreviewed(DurableCompletion {
                identity: DurableOperationIdentity::TaskMutation(original_identity),
                sequence_disposition: MutationSequenceDisposition::AdvanceNext,
                disposition: DurableDisposition::KnownNotApplied {
                    reason: KnownNotAppliedReason::BusyRolledBack,
                    outcome: None,
                    error: None,
                },
            }),
        )
        .await
        .expect("inject stale N terminal completion");
    let after_stale = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect stale terminal completion")
        .expect("stale terminal completion retains N+1");
    assert_eq!(after_stale.pending_terminal_identity, Some(retry_identity));
    assert!(!after_stale.hard_frozen);

    manager
        .inject_terminal_write_completion_for_test(
            task.id,
            retry_attempt_id,
            original_identity,
            retry_stage,
            TerminalWriteCompletion::Unreviewed(DurableCompletion {
                identity: DurableOperationIdentity::TaskMutation(original_identity),
                sequence_disposition: MutationSequenceDisposition::AdvanceNext,
                disposition: DurableDisposition::KnownNotApplied {
                    reason: KnownNotAppliedReason::BusyRolledBack,
                    outcome: None,
                    error: None,
                },
            }),
        )
        .await
        .expect("inject current-attempt identity mismatch");
    let frozen = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect current terminal mismatch")
        .expect("current terminal mismatch retains ownership");
    assert!(frozen.hard_frozen);
    assert_eq!(frozen.pending_terminal_identity, Some(retry_identity));
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    wait_for_status(&store, task.id, TaskStatus::Failed).await;
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn reviewed_terminal_retry_next_owns_n_plus_one_with_the_first_deadline() {
    let temp_dir = tempfile::tempdir().expect("create reviewed retry-next fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open reviewed retry-next store");
    store
        .migrate()
        .await
        .expect("migrate reviewed retry-next store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn reviewed retry-next dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::FinalizeReviewedTask),
                count: 7,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::BusyBeforeExecute,
                operation: Some(StoreWriterOperationKind::FinalizeReviewedTask),
                count: 6,
            },
        ])
        .expect("construct reviewed retry-next controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
        controller.clone(),
    );
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
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "reviewed terminal retries N plus one",
            )
            .expect("construct reviewed retry-next task"),
            background_deadline(),
        )
        .await
        .expect("create reviewed retry-next task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify reviewed retry-next actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("reviewed retry-next runner starts");
    runner.release.notify_one();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let original = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect original reviewed retry")
        .expect("original reviewed retry remains active");
    let original_identity = original
        .pending_terminal_identity
        .expect("original reviewed retry identity is actor-owned");
    let original_deadline = original
        .pending_terminal_deadline
        .expect("original reviewed retry deadline is actor-owned");
    for expected_attempt in 2..=7 {
        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
        controller
            .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, expected_attempt)
            .await;
    }
    let retry = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect N+1 reviewed retry")
        .expect("N+1 reviewed retry remains active");
    let retry_identity = retry
        .pending_terminal_identity
        .expect("N+1 reviewed retry identity is actor-owned");
    assert_eq!(
        retry_identity.sequence.get(),
        original_identity.sequence.get() + 1
    );
    assert_eq!(retry.pending_terminal_deadline, Some(original_deadline));
    assert_eq!(retry.pending_terminal_retry_available, Some(false));
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    wait_for_status(&store, task.id, TaskStatus::Completed).await;
}
