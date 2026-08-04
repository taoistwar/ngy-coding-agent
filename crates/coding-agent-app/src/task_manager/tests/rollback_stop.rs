use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn known_rollback_stop_intent_freezes_without_allocating_or_submitting_a_retry() {
    let temp_dir = tempfile::tempdir().expect("create stop rollback fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open stop rollback store");
    store.migrate().await.expect("migrate stop rollback store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn stop rollback dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::PersistStopIntentBatch),
            count: 1,
        }])
        .expect("construct stop rollback writer controller"),
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
                "deterministic stop rollback",
            )
            .expect("construct stop rollback task"),
            background_deadline(),
        )
        .await
        .expect("create stop rollback task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify stop rollback actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("stop rollback runner starts");
    let cancel = tokio::spawn({
        let manager = manager.clone();
        async move { manager.cancel(task.id).await }
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        controller.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1),
    )
    .await
    .expect("the first concurrent review reaches the writer");
    let pending_before = manager
        .active_pending_stop_write_for_test(task.id)
        .await
        .expect("inspect stop rollback write")
        .expect("stop rollback identity is actor-owned");
    let identity = pending_before.identity();
    manager
        .inject_stop_intent_completion_for_test(
            identity.clone(),
            DurableCompletion {
                identity,
                sequence_disposition: MutationSequenceDisposition::AdvanceNext,
                disposition: DurableDisposition::KnownNotApplied {
                    reason: KnownNotAppliedReason::KnownRollback,
                    outcome: None,
                    error: Some(KnownNotAppliedError::TaskNotFound),
                },
            },
        )
        .await
        .expect("inject deterministic stop rollback");
    assert!(matches!(
        cancel.await.expect("join stop rollback cancel"),
        Err(TaskManagerError::StoreDegraded)
    ));
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        manager
            .active_pending_stop_write_for_test(task.id)
            .await
            .expect("inspect retained stop rollback write"),
        Some(pending_before),
        "KnownRollback preserves the exact actor-owned identity and request"
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::PersistStopIntentBatch,
        ),
        1,
        "KnownRollback must not allocate or submit a replacement sequence"
    );
    assert!(matches!(
        manager.safety_snapshot_for_test().await,
        Err(TaskManagerError::Frozen)
    ));
    assert!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect deterministic rollback pending ownership")
            .is_empty(),
        "a deterministic rollback is not an unknown canonical pending"
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn known_rollback_final_stop_freezes_without_allocating_or_submitting_a_retry() {
    let temp_dir = tempfile::tempdir().expect("create final rollback fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open final rollback store");
    store.migrate().await.expect("migrate final rollback store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn final rollback dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::FinalizeStoppedTask),
            count: 1,
        }])
        .expect("construct final rollback writer controller"),
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
                "deterministic final-stop rollback",
            )
            .expect("construct final rollback task"),
            background_deadline(),
        )
        .await
        .expect("create final rollback task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify final rollback actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("final rollback runner starts");
    assert!(matches!(
        manager
            .cancel(task.id)
            .await
            .expect("persist stop before final rollback"),
        CancelOutcome::Accepted { .. }
    ));
    tokio::time::timeout(Duration::from_secs(5), runner.cancelled.notified())
        .await
        .expect("final rollback runner observes stop");
    runner.release.notify_one();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let pending_before = manager
        .active_pending_stop_write_for_test(task.id)
        .await
        .expect("inspect final rollback write")
        .expect("final rollback identity is actor-owned");
    let (identity, request) = match pending_before.clone() {
        PendingDurableResult::FinalizeStoppedTask { identity, request } => (identity, request),
        _ => panic!("expected an actor-owned final-stop write"),
    };
    manager
        .inject_final_stop_completion_for_test(
            identity,
            request,
            DurableCompletion {
                identity: DurableOperationIdentity::TaskMutation(identity),
                sequence_disposition: MutationSequenceDisposition::AdvanceNext,
                disposition: DurableDisposition::KnownNotApplied {
                    reason: KnownNotAppliedReason::KnownRollback,
                    outcome: None,
                    error: Some(KnownNotAppliedError::TaskNotFound),
                },
            },
        )
        .await
        .expect("inject deterministic final rollback");
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        manager
            .active_pending_stop_write_for_test(task.id)
            .await
            .expect("inspect retained final rollback write"),
        Some(pending_before),
        "final-stop KnownRollback preserves the exact actor-owned identity and request"
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::FinalizeStoppedTask,
        ),
        1,
        "final-stop KnownRollback must not submit a replacement sequence"
    );
    assert!(matches!(
        manager.safety_snapshot_for_test().await,
        Err(TaskManagerError::Frozen)
    ));
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load retained final rollback task")
            .expect("final rollback task exists")
            .task
            .status,
        TaskStatus::Running,
        "deterministic rollback preserves active ownership without a false terminal"
    );
}
