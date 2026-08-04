use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn quiesce_deadline_during_typed_replay_returns_frozen_and_retains_ownership() {
    let temp_dir =
        tempfile::tempdir().expect("create quiesce typed-replay deadline fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open quiesce typed-replay deadline store");
    store
        .migrate()
        .await
        .expect("migrate quiesce typed-replay deadline store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn quiesce typed-replay deadline dispatcher");
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
        .expect("construct quiesce typed-replay deadline writer controller"),
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
                "quiesce deadline while typed replay remains paused",
            )
            .expect("construct quiesce typed-replay deadline task"),
            background_deadline(),
        )
        .await
        .expect("create quiesce typed-replay deadline task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify quiesce typed-replay deadline actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("quiesce typed-replay deadline runner starts");
    wait_for_status(&store, task.id, TaskStatus::Running).await;

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
    let (review_identity, review_request) = match &pending {
        PendingDurableResult::RecordReview { identity, request } => (*identity, request.clone()),
        _ => unreachable!("the helper returns only RecordReview"),
    };
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 3)
        .await;
    let before = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect pre-quiesce typed replay")
        .expect("typed-replay task remains active");
    assert_eq!(before.phase, AdmissionPhase::Running);
    assert_eq!(before.stage, ActiveStopStageForTest::NoWinner);
    assert_eq!(before.active_count, 1);
    assert_eq!(before.available_permits, 0);
    assert_eq!(before.in_flight_mutations, 1);
    assert!(before.durable_sequence_blocked);
    assert_eq!(before.pending_record_review_replay_count, 1);
    assert_eq!(before.applied_record_review_count, 0);
    assert!(!before.cleanup_confirmed);
    assert!(!before.terminal_task_set);
    assert!(before.registry_owned);
    assert_eq!(
        before.permit_process_owner_id,
        before.process_scope_owner_id
    );

    let deadline = Instant::now() + Duration::from_millis(250);
    let quiesce = tokio::spawn({
        let manager = manager.clone();
        async move { manager.quiesce_and_interrupt(deadline).await }
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
    .expect("quiesce latches before its deadline");
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::RecordReview,
        ),
        3,
        "the typed replay backend remains paused across the quiesce deadline"
    );

    let mut active_handles = match tokio::time::timeout(Duration::from_secs(2), quiesce)
        .await
        .expect("quiesce deadline is actor-owned while typed replay remains paused")
        .expect("join quiesce typed-replay deadline request")
        .expect("quiesce typed-replay deadline request remains connected")
    {
        QuiesceResult::Frozen { active, error } => {
            assert!(matches!(error, StoreWriterError::DeadlineElapsed));
            active
        }
        QuiesceResult::Durable { .. } => {
            panic!("a paused typed replay cannot produce durable quiesce")
        }
    };
    assert_eq!(active_handles.len(), 1);
    let active_handle = active_handles
        .pop()
        .expect("quiesce returns the exact active runner handle");
    assert_eq!(active_handle.task_id, task.id);
    assert!(active_handle.cancellation.is_cancelled());

    assert_eq!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect retained quiesce typed replay"),
        vec![pending.clone()]
    );
    let frozen = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect frozen quiesce typed replay")
        .expect("frozen typed-replay task remains actor-owned");
    assert_eq!(frozen.phase, before.phase);
    assert_eq!(frozen.stage, before.stage);
    assert_eq!(frozen.active_count, 1);
    assert_eq!(frozen.available_permits, 0);
    assert_eq!(frozen.in_flight_mutations, 1);
    assert!(frozen.durable_sequence_blocked);
    assert_eq!(frozen.pending_record_review_replay_count, 1);
    assert_eq!(frozen.applied_record_review_count, 0);
    assert!(!frozen.cleanup_confirmed);
    assert!(!frozen.terminal_task_set);
    assert!(frozen.registry_owned);
    assert_eq!(
        frozen.permit_process_owner_id,
        before.permit_process_owner_id
    );
    assert_eq!(frozen.process_scope_owner_id, before.process_scope_owner_id);
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load frozen quiesce typed-replay task")
            .expect("frozen quiesce typed-replay task exists")
            .task
            .status,
        TaskStatus::Running
    );

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    tokio::time::timeout(Duration::from_secs(5), runner.review_applied.notified())
        .await
        .expect("the exact late typed replay is absorbed after quiesce freezes");
    let review_event_id = (*runner
        .review_result
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner))
    .expect("runner stores its late replay result")
    .expect("late exact replay resolves the original review caller");
    assert!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect late quiesce replay ownership")
            .is_empty(),
        "the exact late replay resolves only its canonical pending identity"
    );
    let after_replay = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect late quiesce replay state")
        .expect("late replay cannot release frozen actor ownership");
    assert_eq!(after_replay.phase, AdmissionPhase::Running);
    assert_eq!(after_replay.stage, ActiveStopStageForTest::NoWinner);
    assert_eq!(after_replay.active_count, 1);
    assert_eq!(after_replay.available_permits, 0);
    assert_eq!(after_replay.in_flight_mutations, 0);
    assert!(!after_replay.durable_sequence_blocked);
    assert_eq!(after_replay.pending_record_review_replay_count, 0);
    assert_eq!(after_replay.applied_record_review_count, 1);
    assert!(!after_replay.cleanup_confirmed);
    assert!(!after_replay.terminal_task_set);
    assert!(after_replay.registry_owned);
    assert_eq!(
        after_replay.permit_process_owner_id,
        before.permit_process_owner_id
    );
    assert_eq!(
        after_replay.process_scope_owner_id,
        before.process_scope_owner_id
    );

    let detail = store
        .task_detail(task.id)
        .await
        .expect("load exact late quiesce review")
        .expect("exact late quiesce review task exists");
    assert_eq!(detail.task.status, TaskStatus::Running);
    assert_eq!(detail.reviews.len(), 1);
    assert_eq!(
        manager
            .inject_record_review_completion_for_test(
                review_identity,
                review_request,
                DurableCompletion {
                    identity: DurableOperationIdentity::TaskMutation(review_identity),
                    sequence_disposition: MutationSequenceDisposition::AdvanceNext,
                    disposition: DurableDisposition::Confirmed(RecordReviewOutcome::Existing {
                        review: detail.reviews[0].clone(),
                        event_id: review_event_id,
                    }),
                },
            )
            .await
            .expect("inject duplicate exact late review completion"),
        Ok(review_event_id)
    );
    let after_duplicate = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect duplicate late quiesce completion")
        .expect("duplicate late completion cannot release ownership");
    assert_eq!(after_duplicate.active_count, 1);
    assert_eq!(after_duplicate.available_permits, 0);
    assert_eq!(after_duplicate.applied_record_review_count, 1);
    assert!(after_duplicate.registry_owned);
    assert_eq!(
        after_duplicate.process_scope_owner_id,
        before.process_scope_owner_id
    );
    assert!(!after_duplicate.terminal_task_set);

    drop(active_handle);
    runner.finish_release.notify_one();
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn quiesce_adopts_an_existing_degraded_replay_without_duplicate_submission() {
    let temp_dir = tempfile::tempdir().expect("create quiesce replay-adoption fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open quiesce replay-adoption store");
    store
        .migrate()
        .await
        .expect("migrate quiesce replay-adoption store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn quiesce replay-adoption dispatcher");
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
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::InterruptRemainingAfterStops),
                count: 1,
            },
        ])
        .expect("construct quiesce replay-adoption writer controller"),
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
                "quiesce adopts the exact in-flight typed replay",
            )
            .expect("construct quiesce replay-adoption task"),
            background_deadline(),
        )
        .await
        .expect("create quiesce replay-adoption task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify quiesce replay-adoption actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("quiesce replay-adoption runner starts");

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
    let before = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect pre-adoption replay")
        .expect("replay-adoption task remains active");
    assert!(before.pending_replay_in_flight);
    assert!(!before.hard_frozen);
    assert_eq!(before.active_count, 1);
    assert_eq!(before.available_permits, 0);

    let quiesce = tokio::spawn({
        let manager = manager.clone();
        async move {
            manager
                .quiesce_and_interrupt(Instant::now() + Duration::from_secs(5))
                .await
        }
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
    .expect("replay-adoption quiesce closes admission");
    assert!(manager.shutdown_latched_for_test());
    assert!(matches!(
        manager.notify_queued(task.id).await,
        Err(TaskManagerError::Frozen)
    ));
    let adopted = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect adopted replay")
        .expect("adopted replay remains active");
    assert!(
        !adopted.hard_frozen,
        "quiesce closes public admission without blocking internal typed progress"
    );
    assert!(adopted.pending_replay_in_flight);
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::RecordReview,
        ),
        3,
        "quiesce adopts the one existing replay backend attempt"
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::InterruptRemainingAfterStops,
        ),
        0,
        "generic recovery cannot overtake canonical typed pending"
    );
    assert!(!quiesce.is_finished());

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    tokio::time::timeout(Duration::from_secs(5), runner.review_applied.notified())
        .await
        .expect("the adopted exact replay resolves its original caller");
    assert!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect adopted replay canonical ownership")
            .is_empty()
    );
    let applied = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect adopted exact replay")
        .expect("adopted exact replay retains active ownership");
    assert!(!applied.pending_replay_in_flight);
    assert!(!applied.hard_frozen);
    assert_eq!(applied.in_flight_mutations, 0);
    assert_eq!(applied.pending_record_review_replay_count, 0);
    assert_eq!(applied.applied_record_review_count, 1);
    assert_eq!(applied.active_count, 1);
    assert_eq!(applied.available_permits, 0);
    assert!(applied.registry_owned);
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::RecordReview,
        ),
        3,
        "the adopted pending identity has exactly one replay backend hit"
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::InterruptRemainingAfterStops,
        ),
        0
    );

    runner.finish_release.notify_one();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 4)
        .await;
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::InterruptRemainingAfterStops,
        ),
        1,
        "generic recovery starts only after exact replay and runner cleanup barriers"
    );
    assert!(!quiesce.is_finished());
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );

    let result = tokio::time::timeout(Duration::from_secs(5), quiesce)
        .await
        .expect("adopted replay quiesce completes")
        .expect("join adopted replay quiesce")
        .expect("adopted replay quiesce remains connected");
    let active = match result {
        QuiesceResult::Durable { active, .. } => active,
        QuiesceResult::Frozen { error, .. } => {
            panic!("adopted replay quiesce unexpectedly froze: {error}")
        }
    };
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].task_id, task.id);
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load adopted replay terminal")
            .expect("adopted replay task exists")
            .task
            .status,
        TaskStatus::Interrupted
    );
    assert!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect final adopted replay ownership")
            .is_empty()
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::RecordReview,
        ),
        3
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::InterruptRemainingAfterStops,
        ),
        1
    );
    drop(pending);
}
