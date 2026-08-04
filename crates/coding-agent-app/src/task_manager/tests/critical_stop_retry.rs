use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn critical_stop_retry_preserves_the_original_one_second_deadline() {
    let temp_dir = tempfile::tempdir().expect("create critical deadline fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open critical deadline store");
    store
        .migrate()
        .await
        .expect("migrate critical deadline store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn critical deadline dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::PersistStopIntentBatch),
                count: 1,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::BusyBeforeExecute,
                operation: Some(StoreWriterOperationKind::PersistStopIntentBatch),
                count: 1,
            },
        ])
        .expect("construct critical deadline writer controller"),
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
                "critical stop keeps its observed-at deadline",
            )
            .expect("construct critical deadline task"),
            background_deadline(),
        )
        .await
        .expect("create critical deadline task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify critical deadline actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("critical deadline runner starts");

    manager.notify_storage_critical_for_test(vec![MonitoredStorageScope::RepositoryGit(
        repository.id,
    )]);
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let pending_before = manager
        .active_pending_stop_write_for_test(task.id)
        .await
        .expect("inspect critical deadline write")
        .expect("critical deadline identity is actor-owned");
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(
                manager.safety_snapshot_for_test().await,
                Err(TaskManagerError::Frozen)
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("an expired critical deadline freezes before retry admission");
    assert_eq!(
        manager
            .active_pending_stop_write_for_test(task.id)
            .await
            .expect("inspect retained critical deadline write"),
        Some(pending_before),
        "the expired retry cannot allocate a replacement identity or refresh its deadline"
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::BusyBeforeExecute,
            StoreWriterOperationKind::PersistStopIntentBatch,
        ),
        1,
        "only the original critical write reaches the backend"
    );
    assert!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect critical deadline canonical ownership")
            .is_empty(),
        "a known rollback at the absolute deadline is not canonical unknown ownership"
    );
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load retained critical deadline task")
            .expect("critical deadline task exists")
            .task
            .status,
        TaskStatus::Running,
        "deadline expiry retains active ownership without inventing a terminal"
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn critical_stop_retry_rekeys_the_same_absolute_deadline() {
    let temp_dir = tempfile::tempdir().expect("create critical rekey fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open critical rekey store");
    store.migrate().await.expect("migrate critical rekey store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn critical rekey dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::PersistStopIntentBatch),
            count: 1,
        }])
        .expect("construct critical rekey writer controller"),
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
                "critical retry rekeys one absolute deadline",
            )
            .expect("construct critical rekey task"),
            background_deadline(),
        )
        .await
        .expect("create critical rekey task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify critical rekey actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("critical rekey runner starts");

    manager.notify_storage_critical_for_test(vec![MonitoredStorageScope::RepositoryGit(
        repository.id,
    )]);
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let original = manager
        .active_pending_stop_write_for_test(task.id)
        .await
        .expect("inspect original critical rekey write")
        .expect("original critical identity is actor-owned");
    let original_operation_identity = original.identity();
    manager
        .inject_stop_intent_completion_for_test(
            original_operation_identity.clone(),
            DurableCompletion {
                identity: original_operation_identity,
                sequence_disposition: MutationSequenceDisposition::AdvanceNext,
                disposition: DurableDisposition::KnownNotApplied {
                    reason: KnownNotAppliedReason::BusyRolledBack,
                    outcome: None,
                    error: None,
                },
            },
        )
        .await
        .expect("inject in-budget critical Busy rollback");
    let retry = tokio::time::timeout(Duration::from_millis(800), async {
        loop {
            let pending = manager
                .active_pending_stop_write_for_test(task.id)
                .await
                .expect("inspect rekeyed critical write")
                .expect("rekeyed critical identity is actor-owned");
            if pending != original {
                return pending;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("an in-budget Busy rollback allocates N+1 before the deadline");
    let (original_identity, original_request) = match &original {
        PendingDurableResult::PersistStopIntentBatch {
            identity: DurableOperationIdentity::StopIntentBatch { items },
            requests,
        } => (items[0], requests[0]),
        _ => panic!("original critical rekey state is a one-item stop batch"),
    };
    let (retry_identity, retry_request) = match &retry {
        PendingDurableResult::PersistStopIntentBatch {
            identity: DurableOperationIdentity::StopIntentBatch { items },
            requests,
        } => (items[0], requests[0]),
        _ => panic!("retried critical rekey state is a one-item stop batch"),
    };
    assert_eq!(retry_identity.task_id, original_identity.task_id);
    assert_eq!(retry_identity.kind, original_identity.kind);
    assert_eq!(
        retry_identity.sequence.get(),
        original_identity.sequence.get() + 1,
        "the bounded Busy retry owns exactly N+1"
    );
    assert_eq!(retry_request, original_request);
    assert!(
        !manager.shutdown_latched_for_test(),
        "the in-budget replacement is admitted before the shared deadline"
    );

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if manager.shutdown_latched_for_test() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the old or replacement wake expires the current N+1 at the shared deadline");
    assert_eq!(
        manager
            .active_pending_stop_write_for_test(task.id)
            .await
            .expect("inspect retained critical N+1"),
        Some(retry),
        "deadline expiry retains the rekeyed actor-owned identity and request"
    );
    assert!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect critical rekey canonical ownership")
            .is_empty()
    );
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load retained critical rekey task")
            .expect("critical rekey task exists")
            .task
            .status,
        TaskStatus::Running
    );

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    runner.release.notify_one();
}
