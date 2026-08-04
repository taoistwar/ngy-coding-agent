use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn original_exact_record_review_resolves_canonical_pending_before_replay_unknown_or_exact() {
    for followup in [
        OriginalFirstReplayFollowup::Unknown,
        OriginalFirstReplayFollowup::Exact,
    ] {
        let temp_dir = tempfile::tempdir().expect("create original-first review fixture directory");
        let store = Store::open(temp_dir.path().join("store.sqlite3"))
            .await
            .expect("open original-first review store");
        store
            .migrate()
            .await
            .expect("migrate original-first review store");
        let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
            .await
            .expect("spawn original-first review dispatcher");
        let (unknown_count, pause_count) = match followup {
            OriginalFirstReplayFollowup::Unknown => (4, 4),
            OriginalFirstReplayFollowup::Exact => (2, 3),
        };
        let controller = Arc::new(
            StoreWriterTestController::try_new([
                StoreWriterFaultSpec {
                    point: StoreWriterFaultPoint::PauseBeforeExecute,
                    operation: Some(StoreWriterOperationKind::RecordReview),
                    count: pause_count,
                },
                StoreWriterFaultSpec {
                    point: StoreWriterFaultPoint::FailUnknownBeforeExecute,
                    operation: Some(StoreWriterOperationKind::RecordReview),
                    count: unknown_count,
                },
            ])
            .expect("construct original-first review writer controller"),
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
                    format!("original-first review {followup:?}"),
                )
                .expect("construct original-first review task"),
                background_deadline(),
            )
            .await
            .expect("create original-first review task")
            .value
            .task()
            .clone();
        manager
            .notify_queued(task.id)
            .await
            .expect("notify original-first review actor");
        tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
            .await
            .expect("original-first review runner starts");

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
        let (identity, request) = match &pending {
            PendingDurableResult::RecordReview { identity, request } => {
                (*identity, request.clone())
            }
            _ => unreachable!("the helper returns only RecordReview"),
        };
        controller
            .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 3)
            .await;

        let exact_outcome = store
            .record_review(
                request.task_id,
                request.expected_repository_id,
                request.expected_attempt,
                request.evidence.clone(),
            )
            .await
            .expect("seed the exact original RecordReview result");
        let exact_event_id = record_review_outcome(&request, &exact_outcome)
            .expect("seeded review is exact for the canonical request");
        assert_eq!(
            manager
                .inject_record_review_completion_for_test(
                    identity,
                    request.clone(),
                    DurableCompletion {
                        identity: DurableOperationIdentity::TaskMutation(identity),
                        sequence_disposition: MutationSequenceDisposition::AdvanceNext,
                        disposition: DurableDisposition::Confirmed(exact_outcome),
                    },
                )
                .await
                .expect("inject original exact RecordReview completion"),
            Ok(exact_event_id)
        );
        tokio::time::timeout(Duration::from_secs(5), runner.review_applied.notified())
            .await
            .expect("original exact completion resolves the identity-owned caller");
        assert_eq!(
            runner
                .review_result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
            Some(Ok(exact_event_id))
        );
        assert!(
            manager
                .pending_durable_results_for_test()
                .await
                .expect("inspect original-resolved review ownership")
                .is_empty(),
            "the original exact winner must synchronously resolve canonical pending"
        );

        for expected_pause in 3..=pause_count {
            controller
                .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, expected_pause)
                .await;
            assert_eq!(
                controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
                1
            );
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if manager
                    .safety_snapshot_for_test()
                    .await
                    .is_ok_and(|snapshot| !snapshot.degraded_recovery_running)
                    && manager
                        .pending_durable_results_for_test()
                        .await
                        .is_ok_and(|pending| pending.is_empty())
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a late replay Unknown or exact receipt is a bounded no-op");

        runner.finish_release.notify_one();
    }
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn original_exact_stop_intent_resolves_canonical_pending_before_replay_unknown_or_exact() {
    for followup in [
        OriginalFirstReplayFollowup::Unknown,
        OriginalFirstReplayFollowup::Exact,
    ] {
        let temp_dir = tempfile::tempdir().expect("create original-first stop fixture directory");
        let store = Store::open(temp_dir.path().join("store.sqlite3"))
            .await
            .expect("open original-first stop store");
        store
            .migrate()
            .await
            .expect("migrate original-first stop store");
        let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
            .await
            .expect("spawn original-first stop dispatcher");
        let (unknown_count, pause_count) = match followup {
            OriginalFirstReplayFollowup::Unknown => (4, 4),
            OriginalFirstReplayFollowup::Exact => (2, 3),
        };
        let controller = Arc::new(
            StoreWriterTestController::try_new([
                StoreWriterFaultSpec {
                    point: StoreWriterFaultPoint::PauseBeforeExecute,
                    operation: Some(StoreWriterOperationKind::PersistStopIntentBatch),
                    count: pause_count,
                },
                StoreWriterFaultSpec {
                    point: StoreWriterFaultPoint::FailUnknownBeforeExecute,
                    operation: Some(StoreWriterOperationKind::PersistStopIntentBatch),
                    count: unknown_count,
                },
            ])
            .expect("construct original-first stop writer controller"),
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
                    format!("original-first stop {followup:?}"),
                )
                .expect("construct original-first stop task"),
                background_deadline(),
            )
            .await
            .expect("create original-first stop task")
            .value
            .task()
            .clone();
        manager
            .notify_queued(task.id)
            .await
            .expect("notify original-first stop actor");
        tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
            .await
            .expect("original-first stop runner starts");

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
        let request = match &pending {
            PendingDurableResult::PersistStopIntentBatch { requests, .. } => {
                assert_eq!(requests.len(), 1);
                requests[0]
            }
            _ => unreachable!("the helper returns only PersistStopIntentBatch"),
        };
        let exact_receipt = match store
            .persist_stop_intent(request)
            .await
            .expect("seed exact original stop intent")
        {
            PersistStopIntentOutcome::Applied(receipt)
            | PersistStopIntentOutcome::Existing(receipt) => receipt,
            other => panic!("seeded original stop intent must be durable, got {other:?}"),
        };
        let (identity, completion) = exact_late_stop_completion(&pending, exact_receipt);
        manager
            .inject_stop_intent_completion_for_test(identity, completion)
            .await
            .expect("inject original exact stop-intent completion");
        tokio::time::timeout(Duration::from_secs(5), runner.cancelled.notified())
            .await
            .expect("original exact stop intent cancels the runner");
        assert!(
            manager
                .pending_durable_results_for_test()
                .await
                .expect("inspect original-resolved stop ownership")
                .is_empty(),
            "the original exact winner must synchronously resolve stop canonical pending"
        );

        for expected_pause in 3..=pause_count {
            controller
                .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, expected_pause)
                .await;
            assert_eq!(
                controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
                1
            );
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if manager
                    .safety_snapshot_for_test()
                    .await
                    .is_ok_and(|snapshot| !snapshot.degraded_recovery_running)
                    && manager
                        .pending_durable_results_for_test()
                        .await
                        .is_ok_and(|pending| pending.is_empty())
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a late stop replay Unknown or exact receipt is a bounded no-op");

        assert!(matches!(
            cancel.await.expect("join original-first stop cancel"),
            Err(TaskManagerError::StoreDegraded) | Ok(CancelOutcome::Accepted { .. })
        ));
        if matches!(followup, OriginalFirstReplayFollowup::Exact) {
            runner.release.notify_one();
            wait_for_status(&store, task.id, TaskStatus::Cancelled).await;
        }
    }
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn original_exact_final_stop_resolves_canonical_pending_before_replay_unknown_or_exact() {
    for followup in [
        OriginalFirstReplayFollowup::Unknown,
        OriginalFirstReplayFollowup::Exact,
    ] {
        let temp_dir =
            tempfile::tempdir().expect("create original-first final-stop fixture directory");
        let store = Store::open(temp_dir.path().join("store.sqlite3"))
            .await
            .expect("open original-first final-stop store");
        store
            .migrate()
            .await
            .expect("migrate original-first final-stop store");
        let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
            .await
            .expect("spawn original-first final-stop dispatcher");
        let (unknown_count, pause_count) = match followup {
            OriginalFirstReplayFollowup::Unknown => (4, 4),
            OriginalFirstReplayFollowup::Exact => (2, 3),
        };
        let controller = Arc::new(
            StoreWriterTestController::try_new([
                StoreWriterFaultSpec {
                    point: StoreWriterFaultPoint::PauseBeforeExecute,
                    operation: Some(StoreWriterOperationKind::FinalizeStoppedTask),
                    count: pause_count,
                },
                StoreWriterFaultSpec {
                    point: StoreWriterFaultPoint::FailUnknownBeforeExecute,
                    operation: Some(StoreWriterOperationKind::FinalizeStoppedTask),
                    count: unknown_count,
                },
            ])
            .expect("construct original-first final-stop writer controller"),
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
                    format!("original-first final-stop {followup:?}"),
                )
                .expect("construct original-first final-stop task"),
                background_deadline(),
            )
            .await
            .expect("create original-first final-stop task")
            .value
            .task()
            .clone();
        manager
            .notify_queued(task.id)
            .await
            .expect("notify original-first final-stop actor");
        tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
            .await
            .expect("original-first final-stop runner starts");
        assert!(matches!(
            manager
                .cancel(task.id)
                .await
                .expect("persist user stop before final-stop fault"),
            CancelOutcome::Accepted { .. }
        ));
        tokio::time::timeout(Duration::from_secs(5), runner.cancelled.notified())
            .await
            .expect("runner observes durable user stop");
        runner.release.notify_one();

        for expected_pause in 1..=2 {
            controller
                .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, expected_pause)
                .await;
            assert_eq!(
                controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
                1
            );
        }
        let pending = wait_for_single_pending_final_stop(&manager).await;
        controller
            .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 3)
            .await;
        let (identity, request) = match pending {
            PendingDurableResult::FinalizeStoppedTask { identity, request } => (identity, request),
            _ => unreachable!("the helper returns only FinalizeStoppedTask"),
        };
        let exact_outcome = store
            .finalize_stopped_task(request)
            .await
            .expect("seed exact original final-stop result");
        assert!(matches!(
            exact_outcome,
            FinalizeStoppedTaskOutcome::Applied(_) | FinalizeStoppedTaskOutcome::Existing(_)
        ));
        manager
            .inject_final_stop_completion_for_test(
                identity,
                request,
                DurableCompletion {
                    identity: DurableOperationIdentity::TaskMutation(identity),
                    sequence_disposition: MutationSequenceDisposition::AdvanceNext,
                    disposition: DurableDisposition::Confirmed(exact_outcome),
                },
            )
            .await
            .expect("inject original exact final-stop completion");
        assert!(
            manager
                .pending_durable_results_for_test()
                .await
                .expect("inspect original-resolved final-stop ownership")
                .is_empty(),
            "the original exact final-stop winner must resolve canonical pending"
        );

        for expected_pause in 3..=pause_count {
            controller
                .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, expected_pause)
                .await;
            assert_eq!(
                controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
                1
            );
        }
        wait_for_status(&store, task.id, TaskStatus::Cancelled).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if manager
                    .safety_snapshot_for_test()
                    .await
                    .is_ok_and(|snapshot| {
                        snapshot.active_count == 0 && snapshot.available_permits == 1
                    })
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a late final-stop replay Unknown or exact receipt is a bounded no-op");
    }
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn replayed_exact_final_stop_makes_the_late_original_exact_completion_a_no_op() {
    let temp_dir = tempfile::tempdir().expect("create replay-first final-stop fixture directory");
    let store = Store::open(temp_dir.path().join("store.sqlite3"))
        .await
        .expect("open replay-first final-stop store");
    store
        .migrate()
        .await
        .expect("migrate replay-first final-stop store");
    let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
    let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
        .await
        .expect("spawn replay-first final-stop dispatcher");
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailUnknownBeforeExecute,
                operation: Some(StoreWriterOperationKind::FinalizeStoppedTask),
                count: 2,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
                operation: Some(StoreWriterOperationKind::FinalizeStoppedTask),
                count: 1,
            },
        ])
        .expect("construct replay-first final-stop writer controller"),
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
                "replay-first final-stop late original",
            )
            .expect("construct replay-first final-stop task"),
            background_deadline(),
        )
        .await
        .expect("create replay-first final-stop task")
        .value
        .task()
        .clone();
    manager
        .notify_queued(task.id)
        .await
        .expect("notify replay-first final-stop actor");
    tokio::time::timeout(Duration::from_secs(5), runner.started.notified())
        .await
        .expect("replay-first final-stop runner starts");
    assert!(matches!(
        manager
            .cancel(task.id)
            .await
            .expect("persist replay-first user stop"),
        CancelOutcome::Accepted { .. }
    ));
    tokio::time::timeout(Duration::from_secs(5), runner.cancelled.notified())
        .await
        .expect("runner observes replay-first durable user stop");
    runner.release.notify_one();

    let pending = wait_for_single_pending_final_stop(&manager).await;
    let (identity, request) = match pending {
        PendingDurableResult::FinalizeStoppedTask { identity, request } => (identity, request),
        _ => unreachable!("the helper returns only FinalizeStoppedTask"),
    };
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1)
        .await;
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
        1
    );
    hooks.wait_until_reached().await;
    let exact_outcome = store
        .finalize_stopped_task(request)
        .await
        .expect("load exact Existing final-stop result");
    manager
        .inject_final_stop_completion_for_test(
            identity,
            request,
            DurableCompletion {
                identity: DurableOperationIdentity::TaskMutation(identity),
                sequence_disposition: MutationSequenceDisposition::AdvanceNext,
                disposition: DurableDisposition::Confirmed(exact_outcome),
            },
        )
        .await
        .expect("inject late original exact final-stop completion");
    assert!(
        manager.safety_snapshot_for_test().await.is_ok(),
        "a replay-first exact final stop makes the late original exact receipt a no-op"
    );
    hooks.resume();
    wait_for_status(&store, task.id, TaskStatus::Cancelled).await;
    wait_for_claim_resources_released(&hooks).await;
}
