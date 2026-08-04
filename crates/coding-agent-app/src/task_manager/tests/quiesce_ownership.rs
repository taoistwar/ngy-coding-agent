use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn quiesce_waits_for_a_detached_queued_cancel_before_generic_interrupt() {
    let temp_dir = tempfile::tempdir().expect("create detached queued-cancel fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open detached queued-cancel store");
    store
        .migrate()
        .await
        .expect("migrate detached queued-cancel store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn detached queued-cancel dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
                operation: Some(StoreWriterOperationKind::CancelTask),
                count: 1,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::InterruptRemainingAfterStops),
                count: 1,
            },
        ])
        .expect("construct detached queued-cancel writer controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
        controller.clone(),
    );
    let service_state = ServiceStateController::new(ServiceState::Ready);
    let manager = TaskManagerHandle::spawn(
        store.clone(),
        writer.clone(),
        dispatcher,
        service_state.clone(),
        Arc::new(CancellingRunner::default()),
        test_task_manager_launch_resources_for_repository(1, 1, &repository, temp_dir.path()),
        8,
    );
    let task = writer
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                repository.id,
                "quiesce waits for detached queued cancel",
            )
            .expect("construct detached queued-cancel task"),
            background_deadline(),
        )
        .await
        .expect("create detached queued-cancel task")
        .value
        .task()
        .clone();
    let cancel = tokio::spawn({
        let manager = manager.clone();
        async move { manager.cancel(task.id).await }
    });
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1)
        .await;

    let quiesce = tokio::spawn({
        let manager = manager.clone();
        async move {
            manager
                .quiesce_and_interrupt(Instant::now() + Duration::from_secs(5))
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while service_state.current().state != ServiceState::Quiescing {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("quiesce closes admission behind the detached cancel");
    let blocked = manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect detached queued-cancel barrier");
    assert_eq!(blocked.detached_cancel_completions, 1);
    assert_eq!(blocked.staged_stop_completion_count, 0);
    assert_eq!(blocked.pending_durable_result_count, 0);
    assert!(!blocked.pending_replay_in_flight);
    assert_eq!(blocked.generic_recovery_attempt_id, None);
    assert!(
        !blocked.quiesce_recovery_running,
        "quiesce cannot submit generic interrupt while an accepted queued cancel is detached"
    );
    assert!(!blocked.hard_frozen);
    assert!(!quiesce.is_finished());

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
        1
    );
    assert!(matches!(
        cancel
            .await
            .expect("join detached queued cancel")
            .expect("detached queued cancel remains connected"),
        CancelOutcome::Cancelled { task: cancelled } if cancelled.id == task.id
    ));
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    let result = tokio::time::timeout(Duration::from_secs(5), quiesce)
        .await
        .expect("detached queued-cancel quiesce completes")
        .expect("join detached queued-cancel quiesce")
        .expect("detached queued-cancel quiesce remains connected");
    assert!(matches!(result, QuiesceResult::Durable { .. }));
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load detached queued-cancel terminal")
            .expect("detached queued-cancel task exists")
            .task
            .status,
        TaskStatus::Cancelled
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn quiesce_waits_for_an_existing_generic_recovery_lease_without_duplicate_recover() {
    let temp_dir = tempfile::tempdir().expect("create generic recovery lease fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open generic recovery lease store");
    store
        .migrate()
        .await
        .expect("migrate generic recovery lease store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn generic recovery lease dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailBeforeExecute,
                operation: Some(StoreWriterOperationKind::AppendRunningEvent),
                count: 1,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::InterruptRemainingAfterStops),
                count: 2,
            },
        ])
        .expect("construct generic recovery lease writer controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
        controller.clone(),
    );
    let runner = Arc::new(GenericRecoveryLeaseRunner::default());
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
                "quiesce waits for the existing generic recovery lease",
            )
            .expect("construct generic recovery lease task"),
            background_deadline(),
        )
        .await
        .expect("create generic recovery lease task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify generic recovery lease actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("generic recovery lease runner starts");

    runner.event_release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), runner.event_completed.notified())
        .await
        .expect("generic recovery lease event reports its deterministic failure");
    assert_eq!(
        runner
            .event_result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
        Some(Err(RunnerEventError::StoreDegraded))
    );
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let leased = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = manager
                .safety_snapshot_for_test()
                .await
                .expect("inspect existing generic recovery lease");
            if snapshot.active_count == 1
                && snapshot.recovery_release_ready_count == 1
                && snapshot.degraded_recovery_running
            {
                return snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("generic recovery starts only after active cleanup becomes release-ready");
    assert_eq!(leased.available_permits, 0);
    let generic_attempt_id = leased
        .generic_recovery_attempt_id
        .expect("the paused generic recovery owns one checked attempt ID");
    assert!(!leased.quiesce_recovery_running);
    assert_eq!(
        usize::from(leased.generic_recovery_attempt_id.is_some())
            + usize::from(leased.quiesce_recovery_running),
        1,
        "only the generic recovery is in flight before quiesce"
    );
    assert!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect generic recovery canonical pending")
            .is_empty()
    );
    assert_eq!(service_state.current().state, ServiceState::StoreDegraded);

    let quiesce = tokio::spawn({
        let manager = manager.clone();
        async move {
            manager
                .quiesce_and_interrupt(Instant::now() + Duration::from_secs(5))
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while service_state.current().state != ServiceState::Quiescing {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("quiesce closes admission while the generic recovery lease is live");
    assert!(manager.shutdown_latched_for_test());
    assert!(!quiesce.is_finished());
    let waiting = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect quiesce waiting on the generic lease")
        .expect("generic recovery retains active ownership");
    assert!(!waiting.hard_frozen);
    assert_eq!(
        waiting.generic_recovery_attempt_id,
        Some(generic_attempt_id)
    );
    assert!(!waiting.quiesce_recovery_running);
    assert_eq!(
        usize::from(waiting.generic_recovery_attempt_id.is_some())
            + usize::from(waiting.quiesce_recovery_running),
        1,
        "quiesce adopts the one existing recovery lease"
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::InterruptRemainingAfterStops,
        ),
        1,
        "quiesce cannot start a duplicate recovery while the generic lease is live"
    );
    let stale_attempt_id = generic_attempt_id
        .checked_add(1)
        .expect("the first generic recovery attempt has a stale successor ID");
    manager
        .inject_generic_recovery_completion_for_test(
            stale_attempt_id,
            Err(DegradedCoordinatorError::Quiescing),
        )
        .await
        .expect("inject stale generic recovery completion");
    let after_stale = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect stale generic recovery completion")
        .expect("stale completion cannot release active ownership");
    assert_eq!(
        after_stale.generic_recovery_attempt_id,
        Some(generic_attempt_id),
        "a stale attempt ID cannot clear the current generic lease"
    );
    assert!(!after_stale.quiesce_recovery_running);
    assert!(!after_stale.hard_frozen);
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::InterruptRemainingAfterStops,
        ),
        1,
        "a stale completion cannot submit a duplicate recovery"
    );

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    tokio::time::timeout(
        Duration::from_secs(2),
        controller.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 2),
    )
    .await
    .expect("quiesce starts its recovery after the superseded generic attempt releases");
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::InterruptRemainingAfterStops,
        ),
        2,
        "the old generic attempt and the quiesce attempt execute serially"
    );
    assert!(!quiesce.is_finished());
    let handed_off = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect generic-to-quiesce recovery handoff")
        .expect("quiesce recovery retains active ownership");
    assert_eq!(handed_off.generic_recovery_attempt_id, None);
    assert!(handed_off.quiesce_recovery_running);
    assert!(!handed_off.hard_frozen);
    assert_eq!(
        usize::from(handed_off.generic_recovery_attempt_id.is_some())
            + usize::from(handed_off.quiesce_recovery_running),
        1,
        "generic and quiesce recovery attempts are never concurrent"
    );
    manager
        .inject_generic_recovery_completion_for_test(
            generic_attempt_id,
            Err(DegradedCoordinatorError::Quiescing),
        )
        .await
        .expect("inject duplicate old generic recovery completion");
    let after_late = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect duplicate old generic completion")
        .expect("late generic completion cannot release quiesce ownership");
    assert_eq!(after_late.generic_recovery_attempt_id, None);
    assert!(after_late.quiesce_recovery_running);
    assert!(!after_late.hard_frozen);
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::InterruptRemainingAfterStops,
        ),
        2,
        "a late generic completion cannot start a third recovery"
    );
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );

    let result = tokio::time::timeout(Duration::from_secs(5), quiesce)
        .await
        .expect("generic lease quiesce completes")
        .expect("join generic lease quiesce")
        .expect("generic lease quiesce remains connected");
    let active = match result {
        QuiesceResult::Durable { active, .. } => active,
        QuiesceResult::Frozen { error, .. } => {
            panic!("generic lease quiesce unexpectedly froze: {error}")
        }
    };
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].task_id, task.id);
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load generic lease terminal")
            .expect("generic lease task exists")
            .task
            .status,
        TaskStatus::Interrupted
    );
}
