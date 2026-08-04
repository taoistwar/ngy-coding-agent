use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn quiesce_replays_unknown_stop_before_generic_recovery() {
    let temp_dir = tempfile::tempdir().expect("create quiesce unknown-stop fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open quiesce unknown-stop store");
    store
        .migrate()
        .await
        .expect("migrate quiesce unknown-stop store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn quiesce unknown-stop dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::PersistStopIntentBatch),
                count: 3,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailUnknownBeforeExecute,
                operation: Some(StoreWriterOperationKind::PersistStopIntentBatch),
                count: 2,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::InterruptRemainingAfterStops),
                count: 1,
            },
        ])
        .expect("construct quiesce unknown-stop writer controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
        controller.clone(),
    );
    let runner = Arc::new(DelayedCancellationRunner::default());
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
                "quiesce replays an unknown stop before generic recovery",
            )
            .expect("construct quiesce unknown-stop task"),
            background_deadline(),
        )
        .await
        .expect("create quiesce unknown-stop task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify quiesce unknown-stop actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("quiesce unknown-stop runner starts");
    wait_for_status(&store, task.id, TaskStatus::Running).await;

    let cancel = tokio::spawn({
        let manager = manager.clone();
        async move { manager.cancel(task.id).await }
    });
    for expected_pause in 1..=2 {
        controller
            .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, expected_pause)
            .await;
        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
    }
    let pending = wait_for_single_pending_stop_intent(&manager).await;
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 3)
        .await;
    let before = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect pre-quiesce unknown stop")
        .expect("unknown-stop task remains active");
    assert_eq!(before.stage, ActiveStopStageForTest::IntentWritePending);
    assert!(before.pending_replay_in_flight);
    assert!(!before.hard_frozen);

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
    .expect("unknown-stop quiesce closes admission");
    let adopted = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect adopted unknown stop")
        .expect("adopted unknown stop remains active");
    assert!(!adopted.hard_frozen);
    assert!(adopted.pending_replay_in_flight);
    assert_eq!(adopted.stage, ActiveStopStageForTest::IntentWritePending);
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::PersistStopIntentBatch,
        ),
        3,
        "quiesce adopts the one exact stop replay attempt"
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::InterruptRemainingAfterStops,
        ),
        0,
        "generic recovery cannot overtake the canonical unknown stop"
    );
    assert!(!quiesce.is_finished());

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = manager
                .active_stop_snapshot_for_test(task.id)
                .await
                .expect("inspect replayed unknown stop")
                .expect("replayed unknown stop remains active");
            if snapshot.stage == ActiveStopStageForTest::IntentDurable
                && !snapshot.pending_replay_in_flight
            {
                return snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the exact stop replay becomes durable before generic recovery");
    assert!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect replayed unknown-stop ownership")
            .is_empty()
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::InterruptRemainingAfterStops,
        ),
        0,
        "generic recovery remains blocked by runner cleanup and stop finalization"
    );
    assert!(matches!(
        cancel.await.expect("join unknown-stop cancel"),
        Err(TaskManagerError::StoreDegraded)
    ));

    runner.release.notify_one();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 4)
        .await;
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::PersistStopIntentBatch,
        ),
        3,
        "the canonical stop identity is replayed exactly once"
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::InterruptRemainingAfterStops,
        ),
        1,
        "generic recovery starts only after typed stop and cleanup barriers"
    );
    assert!(!quiesce.is_finished());
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );

    let result = tokio::time::timeout(Duration::from_secs(5), quiesce)
        .await
        .expect("unknown-stop quiesce completes")
        .expect("join unknown-stop quiesce")
        .expect("unknown-stop quiesce remains connected");
    let active = match result {
        QuiesceResult::Durable { active, .. } => active,
        QuiesceResult::Frozen { error, .. } => {
            panic!("unknown-stop quiesce unexpectedly froze: {error}")
        }
    };
    assert!(
        active.is_empty(),
        "the exact stop path releases its runner ownership before generic recovery completes"
    );
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load quiesced unknown-stop terminal")
            .expect("quiesced unknown-stop task exists")
            .task
            .status,
        TaskStatus::Cancelled
    );
    assert!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect final unknown-stop pending ownership")
            .is_empty()
    );
    drop(pending);
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn quiesce_never_runs_generic_interrupt_ahead_of_an_already_admitted_staged_stop() {
    let temp_dir = tempfile::tempdir().expect("create staged-review fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open staged-review store");
    store.migrate().await.expect("migrate staged-review store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn staged-review dispatcher");
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
        .expect("construct staged-review writer controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
        controller.clone(),
    );
    let runner = Arc::new(StagedReviewStopRunner::default());
    let hooks = Arc::new(ClaimTestHooks::new(
        ClaimPhase::PendingReplayBeforeActorDelivery,
    ));
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
                "typed review predecessor before stop",
            )
            .expect("construct staged-review task"),
            background_deadline(),
        )
        .await
        .expect("create staged-review task")
        .value
        .task()
        .clone();
    manager.notify_queued(task.id).await.expect("notify actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("staged-review runner starts after its plan is durable");
    wait_for_status(&store, task.id, TaskStatus::Running).await;

    runner.review_release.notify_one();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let (cancel_response, cancel_receiver) = oneshot::channel();
    manager
        .send(TaskManagerMessage::Cancel {
            task_id: task.id,
            response: cancel_response,
        })
        .await
        .expect("enqueue cancel behind the in-flight review");
    let before_unknown = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect pre-unknown actor state")
        .expect("staged-review task is active");
    assert_eq!(
        before_unknown.stage,
        ActiveStopStageForTest::IntentWritePending
    );
    assert_eq!(before_unknown.in_flight_mutations, 1);
    assert_eq!(before_unknown.pending_record_review_replay_count, 0);
    assert_eq!(before_unknown.staged_stop_completion_count, 0);
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
    let pending = wait_for_single_pending_record_review(&manager).await;
    let (review_identity, review_request) = match &pending {
        PendingDurableResult::RecordReview { identity, request } => (*identity, request.clone()),
        _ => unreachable!("the helper returns only RecordReview"),
    };
    assert_eq!(review_identity.task_id, task.id);
    assert_eq!(review_identity.kind, DurableOperationKind::RecordReview);
    assert_eq!(review_request.task_id, task.id);
    assert_eq!(review_request.expected_repository_id, repository.id);
    assert_eq!(review_request.expected_attempt, task.attempt);
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 3)
        .await;
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    hooks.wait_until_reached().await;
    let staged = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = manager
                .active_stop_snapshot_for_test(task.id)
                .await
                .expect("inspect staged stop completion")
                .expect("staged-review task remains active");
            if snapshot.staged_stop_completion_count == 1 {
                return snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the already-admitted stop completion is staged behind RecordReview");
    assert_eq!(staged.stage, ActiveStopStageForTest::IntentWritePending);
    assert_eq!(staged.in_flight_mutations, 1);
    assert!(staged.durable_sequence_blocked);
    assert_eq!(staged.pending_record_review_replay_count, 1);
    assert_eq!(staged.applied_record_review_count, 0);
    assert!(!staged.cleanup_confirmed);
    assert_eq!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect staged pending ownership"),
        vec![pending.clone()]
    );

    let quiesce = tokio::spawn({
        let manager = manager.clone();
        async move {
            manager
                .quiesce_and_interrupt(Instant::now() + Duration::from_secs(10))
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = manager
                .active_stop_snapshot_for_test(task.id)
                .await
                .expect("inspect staged stop while quiescing")
                .expect("staged stop remains actor-owned while quiescing");
            if manager.shutdown_latched_for_test() {
                assert_eq!(snapshot.staged_stop_completion_count, 1);
                assert_eq!(snapshot.pending_record_review_replay_count, 1);
                assert!(snapshot.pending_replay_in_flight);
                assert!(!snapshot.quiesce_recovery_running);
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("quiesce observes the exact staged-stop barriers");
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::PauseBeforeExecute,
            StoreWriterOperationKind::InterruptRemainingAfterStops,
        ),
        0,
        "generic interrupt cannot overtake the staged stop and its typed predecessor"
    );
    assert!(!quiesce.is_finished());

    hooks.resume();
    tokio::time::timeout(Duration::from_secs(5), runner.review_applied.notified())
        .await
        .expect("typed replay resolves the original runner review call");
    let review_event_id = (*runner
        .review_result
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner))
    .expect("runner stored its review result")
    .expect("typed replay returns the exact review event");
    let after_replay = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = manager
                .active_stop_snapshot_for_test(task.id)
                .await
                .expect("inspect post-replay actor state")
                .expect("runner remains active until the fixture releases it");
            if snapshot.stage == ActiveStopStageForTest::IntentDurable
                && snapshot.staged_stop_completion_count == 0
            {
                return snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("typed replay is applied before the staged stop completion");
    assert_eq!(after_replay.in_flight_mutations, 0);
    assert_eq!(after_replay.pending_record_review_replay_count, 0);
    assert_eq!(after_replay.applied_record_review_count, 1);
    assert!(
        !after_replay.durable_sequence_blocked,
        "exact predecessor replay clears the sequence block before the staged stop applies"
    );
    assert!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect post-replay canonical ownership")
            .is_empty(),
        "the exact predecessor and already-admitted stop leave no canonical unknown"
    );
    assert!(!after_replay.cleanup_confirmed);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), cancel_receiver)
            .await
            .expect("durable stop intent resolves the cancel caller")
            .expect("cancel response channel remains open"),
        Ok(CancelOutcome::Accepted { task: accepted }) if accepted.id == task.id
    ));

    let detail = store
        .task_detail(task.id)
        .await
        .expect("load staged-review detail")
        .expect("staged-review task exists");
    assert_eq!(detail.reviews.len(), 1);
    let late_completion = DurableCompletion {
        identity: DurableOperationIdentity::TaskMutation(review_identity),
        sequence_disposition: MutationSequenceDisposition::AdvanceNext,
        disposition: DurableDisposition::Confirmed(RecordReviewOutcome::Existing {
            review: detail.reviews[0].clone(),
            event_id: review_event_id,
        }),
    };
    assert_eq!(
        manager
            .inject_record_review_completion_for_test(
                review_identity,
                review_request,
                late_completion,
            )
            .await
            .expect("inject an exact late RecordReview receipt"),
        Ok(review_event_id)
    );
    let after_late = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect exact late receipt state")
        .expect("runner remains active after an exact late receipt");
    assert_eq!(after_late.stage, ActiveStopStageForTest::IntentDurable);
    assert_eq!(after_late.in_flight_mutations, 0);
    assert_eq!(after_late.pending_record_review_replay_count, 0);
    assert_eq!(after_late.applied_record_review_count, 1);
    assert_eq!(after_late.staged_stop_completion_count, 0);

    runner.finish_release.notify_one();
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    wait_for_status(&store, task.id, TaskStatus::Cancelled).await;
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    let result = tokio::time::timeout(Duration::from_secs(5), quiesce)
        .await
        .expect("staged-stop quiesce completes")
        .expect("join staged-stop quiesce")
        .expect("staged-stop quiesce remains connected");
    assert!(matches!(result, QuiesceResult::Durable { .. }));
    wait_for_claim_resources_released(&hooks).await;
    let events = store
        .task_events_after(task.id, EventCursor::ZERO, usize::MAX)
        .await
        .expect("load staged-review events")
        .events;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.payload.kind() == TaskEventKind::ReviewUpdated)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.payload.kind() == TaskEventKind::TaskCancelled)
            .count(),
        1
    );
    assert!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect final staged-review pending ownership")
            .is_empty()
    );
}
