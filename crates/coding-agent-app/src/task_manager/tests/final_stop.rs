use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn late_exact_stop_intent_receipt_is_a_no_op_across_all_post_replay_phases() {
    let temp_dir = tempfile::tempdir().expect("create late-stop fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open late-stop store");
    store.migrate().await.expect("migrate late-stop store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn late-stop dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailUnknownBeforeExecute,
                operation: Some(StoreWriterOperationKind::PersistStopIntentBatch),
                count: 2,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
                operation: Some(StoreWriterOperationKind::PersistStopIntentBatch),
                count: 1,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
                operation: Some(StoreWriterOperationKind::FinalizeStoppedTask),
                count: 1,
            },
        ])
        .expect("construct late-stop writer controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
        controller.clone(),
    );
    let runner = Arc::new(DelayedCancellationRunner::default());
    let hooks = Arc::new(ClaimTestHooks::new(ClaimPhase::TerminalDispatched));
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
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "late exact stop receipt",
            )
            .expect("construct late-stop task"),
            background_deadline(),
        )
        .await
        .expect("create late-stop task")
        .value
        .task()
        .clone();
    manager.notify_queued(task.id).await.expect("notify actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("late-stop runner starts");
    wait_for_status(&store, task.id, TaskStatus::Running).await;

    let cancel = tokio::spawn({
        let manager = manager.clone();
        async move { manager.cancel(task.id).await }
    });
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1)
        .await;
    let pending = wait_for_single_pending_stop_intent(&manager).await;
    let receipt = committed_stop_intent(&store, task.id).await;
    let (identity, exact_completion) = exact_late_stop_completion(&pending, receipt);
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
        1
    );

    tokio::time::timeout(Duration::from_secs(5), runner.cancelled.notified())
        .await
        .expect("replayed stop intent reaches IntentDurable");
    manager
        .inject_stop_intent_completion_for_test(identity.clone(), exact_completion.clone())
        .await
        .expect("inject exact late receipt at IntentDurable");
    assert!(
        manager.safety_snapshot_for_test().await.is_ok(),
        "an exact late receipt cannot freeze IntentDurable"
    );

    runner.release.notify_one();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 2)
        .await;
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
            StoreWriterOperationKind::FinalizeStoppedTask,
        ),
        1,
        "the replayed intent starts exactly one final-stop transaction"
    );
    manager
        .inject_stop_intent_completion_for_test(identity.clone(), exact_completion.clone())
        .await
        .expect("inject exact late receipt at FinalStopWritePending");
    assert!(
        manager.safety_snapshot_for_test().await.is_ok(),
        "an exact late receipt cannot freeze FinalStopWritePending"
    );
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
        1
    );

    hooks.wait_until_reached().await;
    manager
        .inject_stop_intent_completion_for_test(identity.clone(), exact_completion.clone())
        .await
        .expect("inject exact late receipt at StopTerminal");
    assert!(
        manager.safety_snapshot_for_test().await.is_ok(),
        "an exact late receipt cannot freeze StopTerminal"
    );
    hooks.resume();

    wait_for_status(&store, task.id, TaskStatus::Cancelled).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let safety = manager
                .safety_snapshot_for_test()
                .await
                .expect("inspect late-stop recovery safety");
            if safety.active_count == 0 && safety.available_permits == 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("late-stop ownership releases after exact projection");
    manager
        .inject_stop_intent_completion_for_test(identity, exact_completion)
        .await
        .expect("inject exact duplicate after ownership release");
    assert!(
        manager.safety_snapshot_for_test().await.is_ok(),
        "a fully absent exact stop duplicate is a stale no-op"
    );
    assert!(matches!(
        cancel.await.expect("join late-stop cancel"),
        Err(TaskManagerError::StoreDegraded)
    ));
    let events = store
        .task_events_after(task.id, EventCursor::ZERO, usize::MAX)
        .await
        .expect("load late-stop events")
        .events;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.payload.kind() == TaskEventKind::TaskCancelled)
            .count(),
        1
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn final_stop_ingress_full_retains_the_exact_pending_until_reconciliation() {
    let temp_dir = tempfile::tempdir().expect("create full-final fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open full-final store");
    store.migrate().await.expect("migrate full-final store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn full-final dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new(std::iter::empty::<StoreWriterFaultSpec>())
            .expect("construct full-final writer controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        1,
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
                "full final-stop ingress",
            )
            .expect("construct full-final task"),
            background_deadline(),
        )
        .await
        .expect("create full-final task")
        .value
        .task()
        .clone();
    manager.notify_queued(task.id).await.expect("notify actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("full-final runner starts");
    wait_for_status(&store, task.id, TaskStatus::Running).await;

    let cancel = tokio::spawn({
        let manager = manager.clone();
        async move { manager.cancel(task.id).await }
    });
    tokio::time::timeout(Duration::from_secs(5), runner.cancelled.notified())
        .await
        .expect("durable stop intent cancels the delayed runner");
    assert!(matches!(
        cancel.await.expect("join full-final cancel"),
        Ok(CancelOutcome::Accepted { task: accepted }) if accepted.id == task.id
    ));

    controller
        .arm_fault(StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::CreateTask),
            count: 1,
        })
        .expect("arm full-final normal-lane pause");
    let blocked = writer
        .submit_queue_limited_create(
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "blocked ahead of final stop",
            )
            .expect("construct blocked full-final task"),
            NonZeroU32::new(64).expect("positive queue limit"),
            background_deadline(),
        )
        .expect("submit blocked normal write");
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let buffered = writer
        .submit_queue_limited_create(
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "buffered ahead of final stop",
            )
            .expect("construct buffered full-final task"),
            NonZeroU32::new(64).expect("positive queue limit"),
            background_deadline(),
        )
        .expect("fill the one-slot normal ingress");

    runner.release.notify_one();
    let pending = wait_for_single_pending_final_stop(&manager).await;
    let (identity, request) = match pending {
        PendingDurableResult::FinalizeStoppedTask { identity, request } => (identity, request),
        _ => unreachable!("wait helper returns only final-stop pending"),
    };
    assert_eq!(identity.task_id, task.id);
    assert_eq!(identity.kind, DurableOperationKind::FinalizeStoppedTask);
    assert_eq!(request.task_id, task.id);
    assert_eq!(request.expected_repository_id, repository.id);
    assert_eq!(request.expected_attempt, task.attempt);
    assert_eq!(request.expected_intent, StopIntentKind::UserCancelled);
    let safety = manager
        .safety_snapshot_for_test()
        .await
        .expect("full ingress degrades without freezing exact ownership");
    assert_eq!(safety.active_count, 1);
    assert_eq!(safety.available_permits, 0);
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load full-final task")
            .expect("full-final task exists")
            .task
            .status,
        TaskStatus::Running
    );

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    tokio::time::timeout(Duration::from_secs(5), blocked.completion())
        .await
        .expect("blocked normal write completes");
    tokio::time::timeout(Duration::from_secs(5), buffered.completion())
        .await
        .expect("buffered normal write completes");
    wait_for_status(&store, task.id, TaskStatus::Cancelled).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let pending = manager
                .pending_durable_results_for_test()
                .await
                .expect("inspect replayed final-stop pending");
            let safety = manager
                .safety_snapshot_for_test()
                .await
                .expect("inspect replayed full-final ownership");
            if pending.is_empty() && safety.active_count == 0 && safety.available_permits == 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("exact final-stop replay clears pending and releases ownership");
    let events = store
        .task_events_after(task.id, EventCursor::ZERO, usize::MAX)
        .await
        .expect("load full-final events")
        .events;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.payload.kind() == TaskEventKind::TaskCancelled)
            .count(),
        1
    );
}
