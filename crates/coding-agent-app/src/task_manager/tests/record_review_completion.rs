use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn successor_review_late_exact_waits_for_its_canonical_predecessor() {
    let temp_dir = tempfile::tempdir().expect("create successor late-exact fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open successor late-exact store");
    store
        .migrate()
        .await
        .expect("migrate successor late-exact store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn successor late-exact dispatcher");
    let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
    let runner = Arc::new(ReleaseRunner::default());
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
                "successor exact waits for predecessor",
            )
            .expect("construct successor late-exact task"),
            background_deadline(),
        )
        .await
        .expect("create successor late-exact task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify successor late-exact actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("successor late-exact runner starts");
    let running = store
        .task_detail(task.id)
        .await
        .expect("load running historical review task")
        .expect("historical review task exists")
        .task;
    let request = RecordReviewRequest {
        task_id: task.id,
        expected_repository_id: repository.id,
        expected_attempt: running.attempt,
        evidence: staged_review_evidence(),
    };
    store
        .record_review(
            request.task_id,
            request.expected_repository_id,
            request.expected_attempt,
            request.evidence.clone(),
        )
        .await
        .expect("seed predecessor exact review outcome");
    let staged_pair = manager
        .stage_historical_record_review_pair_for_test(task.id, [request.clone(), request])
        .await
        .expect("stage historical canonical review pair");
    let [
        (predecessor_identity, predecessor_request),
        (successor_identity, successor_request),
    ] = staged_pair.entries.clone();
    let [predecessor_response, successor_response] = staged_pair.responses;
    tokio::time::timeout(Duration::from_secs(5), hooks.wait_until_reached())
        .await
        .expect("the predecessor replay reaches its actor-delivery hook");
    assert_eq!(
        manager
            .pending_durable_results_for_test()
            .await
            .expect("inspect historical canonical pair"),
        vec![
            PendingDurableResult::RecordReview {
                identity: predecessor_identity,
                request: predecessor_request,
            },
            PendingDurableResult::RecordReview {
                identity: successor_identity,
                request: successor_request.clone(),
            },
        ]
    );
    let successor_outcome = store
        .record_review(
            successor_request.task_id,
            successor_request.expected_repository_id,
            successor_request.expected_attempt,
            successor_request.evidence.clone(),
        )
        .await
        .expect("seed successor exact review outcome");
    let successor_event_id = record_review_outcome(&successor_request, &successor_outcome)
        .expect("seeded successor outcome is exact");
    let successor = tokio::spawn({
        let manager = manager.clone();
        let successor_request = successor_request.clone();
        async move {
            manager
                .inject_record_review_completion_for_test(
                    successor_identity,
                    successor_request,
                    DurableCompletion {
                        identity: DurableOperationIdentity::TaskMutation(successor_identity),
                        sequence_disposition: MutationSequenceDisposition::AdvanceNext,
                        disposition: DurableDisposition::Confirmed(successor_outcome),
                    },
                )
                .await
        }
    });
    let mut successor = successor;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut successor)
            .await
            .is_err(),
        "successor exact receipt remains actor-owned behind canonical predecessor"
    );
    let staged = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect deferred successor exact")
        .expect("deferred successor retains active ownership");
    assert!(!staged.hard_frozen);
    assert_eq!(staged.pending_record_review_replay_count, 2);
    assert_eq!(staged.in_flight_mutations, 2);

    let quiesce_deadline = Instant::now() + Duration::from_millis(100);
    let quiesce = tokio::spawn({
        let manager = manager.clone();
        async move { manager.quiesce_and_interrupt(quiesce_deadline).await }
    });
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), quiesce)
            .await
            .expect("successor hard-expiry quiesce returns")
            .expect("join successor hard-expiry quiesce")
            .expect("successor hard-expiry manager remains connected"),
        QuiesceResult::Frozen {
            error: StoreWriterError::DeadlineElapsed,
            ..
        }
    ));
    let expired = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect hard-expired deferred successor")
        .expect("hard expiry retains both logical reviews");
    assert!(expired.hard_frozen);
    assert_eq!(expired.pending_record_review_replay_count, 2);
    assert_eq!(expired.in_flight_mutations, 2);

    hooks.resume();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), &mut successor)
            .await
            .expect("successor exact resolves after predecessor")
            .expect("join successor exact injection")
            .expect("successor exact manager remains connected"),
        Ok(successor_event_id)
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(5), predecessor_response)
            .await
            .expect("predecessor logical response resolves")
            .expect("predecessor response sender remains owned")
            .is_ok()
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), successor_response)
            .await
            .expect("successor logical response resolves")
            .expect("successor response sender remains owned"),
        Ok(successor_event_id)
    );
    let converged = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect converged successor reviews")
        .expect("runner cleanup still retains active ownership");
    assert_eq!(converged.pending_record_review_replay_count, 0);
    assert_eq!(converged.in_flight_mutations, 0);
    assert_eq!(converged.applied_record_review_count, 2);
    assert!(converged.hard_frozen);
    assert_eq!(
        store
            .task_detail(task.id)
            .await
            .expect("load hard-expired successor task")
            .expect("hard-expired successor task exists")
            .task
            .status,
        TaskStatus::Running
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn ordinary_runner_event_busy_completion_never_allocates_n_plus_one() {
    let temp_dir = tempfile::tempdir().expect("create ordinary event no-retry fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open ordinary event no-retry store");
    store
        .migrate()
        .await
        .expect("migrate ordinary event no-retry store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn ordinary event no-retry dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::AppendRunningEvent),
                count: 7,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::BusyBeforeExecute,
                operation: Some(StoreWriterOperationKind::AppendRunningEvent),
                count: 6,
            },
        ])
        .expect("construct ordinary event no-retry controller"),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        store.clone(),
        Arc::new(dispatcher.clone()),
        8,
        controller.clone(),
    );
    let runner = Arc::new(GenericRecoveryLeaseRunner::default());
    let manager = TaskManagerHandle::spawn(
        store,
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
                "ordinary event never retries N plus one",
            )
            .expect("construct ordinary event no-retry task"),
            background_deadline(),
        )
        .await
        .expect("create ordinary event no-retry task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify ordinary event no-retry actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("ordinary event no-retry runner starts");
    runner.event_release.notify_one();
    tokio::time::timeout(
        Duration::from_secs(5),
        controller.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1),
    )
    .await
    .expect("the ordinary event reaches its first bounded writer attempt");
    for expected_attempt in 2..=6 {
        assert_eq!(
            controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
            1
        );
        tokio::time::timeout(
            Duration::from_secs(5),
            controller
                .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, expected_attempt),
        )
        .await
        .expect("the ordinary event reaches each bounded internal Busy attempt");
    }
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    tokio::time::timeout(Duration::from_secs(5), runner.event_completed.notified())
        .await
        .expect("ordinary event reports its one failed logical write");
    assert_eq!(
        runner
            .event_result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
        Some(Err(RunnerEventError::StoreDegraded))
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(500),
            controller.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 7),
        )
        .await
        .is_err(),
        "ordinary RunnerEvent never allocates a replacement sequence"
    );
}
