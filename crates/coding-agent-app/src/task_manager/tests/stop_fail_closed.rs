use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn hard_freeze_rejects_a_loaded_running_cancel_without_allocating_identity() {
    let fixture = running_hard_freeze_fixture("hard freeze rejects a loaded running cancel").await;
    let before = fixture
        .manager
        .active_stop_snapshot_for_test(fixture.task.id)
        .await
        .expect("inspect pre-freeze running cancel")
        .expect("hard-freeze cancel task remains active");
    let before_barrier = fixture
        .manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect pre-freeze exact barrier");
    fixture
        .manager
        .freeze_degraded_for_test()
        .await
        .expect("hard-freeze the running cancel fixture");

    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(2),
            fixture
                .manager
                .inject_running_user_cancel_after_lookup_for_test(fixture.task.id),
        )
        .await
        .expect("loaded running cancel returns after hard freeze"),
        Err(TaskManagerError::Frozen)
    ));

    let after = fixture
        .manager
        .active_stop_snapshot_for_test(fixture.task.id)
        .await
        .expect("inspect rejected hard-frozen cancel")
        .expect("rejected cancel retains active ownership");
    let after_barrier = fixture
        .manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect post-cancel exact barrier");
    assert_eq!(after.stage, ActiveStopStageForTest::NoWinner);
    assert_eq!(after.next_mutation_sequence, before.next_mutation_sequence);
    assert_eq!(after_barrier.barrier_epoch, before_barrier.barrier_epoch);
    assert!(
        fixture
            .store
            .scheduler_bootstrap_snapshot()
            .await
            .expect("load rejected-cancel bootstrap snapshot")
            .running_stop_intents
            .is_empty()
    );
    fixture.runner.release.notify_one();
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn hard_freeze_still_settles_a_precommitted_queued_cancel() {
    let fixture = paused_queued_cancel_fixture().await;
    let before = fixture
        .manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect pre-freeze queued cancel barrier");
    assert_eq!(before.detached_cancel_completions, 1);

    fixture
        .manager
        .freeze_degraded_for_test()
        .await
        .expect("hard-freeze the precommitted queued cancel");
    assert_eq!(
        fixture
            .controller
            .release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
        1
    );
    let outcome = tokio::time::timeout(Duration::from_secs(5), fixture.cancel)
        .await
        .expect("the precommitted queued cancel settles after hard freeze")
        .expect("join the queued cancel caller")
        .expect("the exact applied queued cancel remains accepted");
    let terminal = match outcome {
        CancelOutcome::Cancelled { task } if task.id == fixture.task.id => task,
        other => panic!("queued cancel returned an inexact outcome: {other:?}"),
    };
    let durable_scheduler = fixture
        .store
        .scheduler_bootstrap_snapshot()
        .await
        .expect("load the queued-cancel scheduler witness");
    let published_scheduler = fixture.manager.scheduler_state_reader().current();
    assert!(
        published_scheduler
            .public_state()
            .exactly_matches(&durable_scheduler)
    );
    assert_eq!(
        published_scheduler.as_of_event_id(),
        durable_scheduler.membership_event_id
    );
    assert!(published_scheduler.as_of_event_id().get() >= terminal.last_event_id.get());

    let after = fixture
        .manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect settled queued cancel barrier");
    assert_eq!(after.detached_cancel_completions, 0);
    assert!(after.hard_frozen);
    assert_eq!(
        fixture
            .store
            .task_detail(fixture.task.id)
            .await
            .expect("load hard-frozen queued cancel")
            .expect("hard-frozen queued cancel exists")
            .task
            .status,
        TaskStatus::Cancelled
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn hard_freeze_drains_all_staged_exact_stops_after_reverse_predecessor_resolution() {
    let fixture = two_task_hard_freeze_fixture().await;
    let (first_entry, first_predecessor) =
        staged_stop_completion_for_test(&fixture.store, &fixture.manager, &fixture.tasks[0]).await;
    let (second_entry, second_predecessor) =
        staged_stop_completion_for_test(&fixture.store, &fixture.manager, &fixture.tasks[1]).await;
    fixture
        .manager
        .install_staged_stop_completions_for_test(vec![first_entry, second_entry])
        .await
        .expect("install two staged exact stop completions");
    assert_eq!(
        fixture
            .manager
            .exact_barrier_snapshot_for_test()
            .await
            .expect("inspect staged exact stop barriers")
            .staged_stop_completion_count,
        2
    );
    fixture
        .manager
        .freeze_degraded_preserving_pending_for_test()
        .await
        .expect("hard-freeze both staged exact stops");

    fixture
        .manager
        .resolve_canonical_predecessor_for_test(second_predecessor)
        .await
        .expect("resolve the second staged stop predecessor first");
    assert_eq!(
        fixture
            .manager
            .exact_barrier_snapshot_for_test()
            .await
            .expect("inspect reverse-blocked staged exact stops")
            .staged_stop_completion_count,
        2
    );
    fixture
        .manager
        .resolve_canonical_predecessor_for_test(first_predecessor)
        .await
        .expect("resolve the first staged stop predecessor last");

    let after = fixture
        .manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect fully drained staged exact stops");
    assert_eq!(after.staged_stop_completion_count, 0);
    assert!(after.hard_frozen);
    for task in &fixture.tasks {
        assert_eq!(
            fixture
                .manager
                .active_stop_snapshot_for_test(task.id)
                .await
                .expect("inspect staged exact stop ownership")
                .expect("hard freeze retains exact stop ownership")
                .stage,
            ActiveStopStageForTest::IntentDurable
        );
    }
    assert_eq!(fixture.repository.id, fixture.tasks[0].repository_id);
    fixture.runner.release.notify_waiters();
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn hard_freeze_stops_staged_drain_at_a_structurally_invalid_completion() {
    let fixture = two_task_hard_freeze_fixture().await;
    let (mut first_entry, first_predecessor) =
        staged_stop_completion_for_test(&fixture.store, &fixture.manager, &fixture.tasks[0]).await;
    let (second_entry, second_predecessor) =
        staged_stop_completion_for_test(&fixture.store, &fixture.manager, &fixture.tasks[1]).await;
    first_entry.completion.disposition =
        DurableDisposition::Confirmed(StopIntentBatchReceipt { items: Vec::new() });
    fixture
        .manager
        .install_staged_stop_completions_for_test(vec![first_entry, second_entry])
        .await
        .expect("install one invalid and one exact staged stop completion");
    fixture
        .manager
        .freeze_degraded_preserving_pending_for_test()
        .await
        .expect("hard-freeze the staged stop completion fixture");

    fixture
        .manager
        .resolve_canonical_predecessor_for_test(second_predecessor)
        .await
        .expect("resolve the second staged stop predecessor first");
    fixture
        .manager
        .resolve_canonical_predecessor_for_test(first_predecessor)
        .await
        .expect_err("the invalid first completion stops the staged drain");

    let after = fixture
        .manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect the stopped staged completion drain");
    assert_eq!(after.staged_stop_completion_count, 1);
    assert!(after.hard_frozen);
    assert_eq!(
        fixture
            .manager
            .active_stop_snapshot_for_test(fixture.tasks[1].id)
            .await
            .expect("inspect the second staged stop ownership")
            .expect("hard freeze retains the second staged stop ownership")
            .stage,
        ActiveStopStageForTest::IntentWritePending
    );
    fixture.runner.release.notify_waiters();
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn direct_invalid_stop_completion_does_not_kick_eligible_staged_completions() {
    let fixture = two_task_hard_freeze_fixture().await;
    let (first_entry, first_predecessor) =
        staged_stop_completion_for_test(&fixture.store, &fixture.manager, &fixture.tasks[0]).await;
    let (second_entry, second_predecessor) =
        staged_stop_completion_for_test(&fixture.store, &fixture.manager, &fixture.tasks[1]).await;
    let invalid_identity = DurableOperationIdentity::stop_intent_batch(vec![first_entry.identity])
        .expect("one invalid direct stop identity is a valid batch");
    fixture
        .manager
        .install_staged_stop_completions_for_test(vec![first_entry, second_entry])
        .await
        .expect("install two staged exact stop completions");
    fixture
        .manager
        .release_canonical_predecessor_without_progress_for_test(first_predecessor)
        .await
        .expect("make the first staged completion eligible without draining");
    fixture
        .manager
        .release_canonical_predecessor_without_progress_for_test(second_predecessor)
        .await
        .expect("make the second staged completion eligible without draining");

    fixture
        .manager
        .inject_stop_intent_completion_for_test(
            invalid_identity.clone(),
            empty_confirmed_stop_completion(invalid_identity),
        )
        .await
        .expect("inject a structurally invalid direct completion");

    let after = fixture
        .manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect staged ownership after the direct completion stopped");
    assert_eq!(after.staged_stop_completion_count, 2);
    assert!(after.hard_frozen);
    for task in &fixture.tasks {
        assert_eq!(
            fixture
                .manager
                .active_stop_snapshot_for_test(task.id)
                .await
                .expect("inspect retained staged stop ownership")
                .expect("hard freeze retains staged stop ownership")
                .stage,
            ActiveStopStageForTest::IntentWritePending
        );
    }
    fixture.runner.release.notify_waiters();
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn fully_absent_stop_receipt_with_a_predecessor_is_an_immediate_stale_no_op() {
    let fixture =
        running_hard_freeze_fixture("fully absent stop receipt remains a stale no-op").await;
    let absent_task_id = TaskId::new();
    fixture
        .manager
        .freeze_degraded_for_test()
        .await
        .expect("hard-freeze the absent stop receipt fixture");
    fixture
        .manager
        .install_canonical_pending_for_test(test_record_review_predecessor(
            absent_task_id,
            fixture.repository.id,
            1,
        ))
        .await
        .expect("install an absent task predecessor");
    let before = fixture
        .manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect absent receipt barriers");
    let identity = test_stop_batch_identity(vec![absent_task_id], 2);

    fixture
        .manager
        .inject_stop_intent_completion_for_test(
            identity.clone(),
            empty_confirmed_stop_completion(identity),
        )
        .await
        .expect("inject a fully absent stop receipt");

    let after = fixture
        .manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect barriers after the absent receipt");
    assert_eq!(after.staged_stop_completion_count, 0);
    assert_eq!(after.barrier_epoch, before.barrier_epoch);
    fixture.runner.release.notify_one();
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn mixed_stop_receipt_with_a_predecessor_freezes_without_staging() {
    let fixture =
        running_hard_freeze_fixture("mixed stop receipt cannot stage behind a predecessor").await;
    let absent_task_id = TaskId::new();
    fixture
        .manager
        .freeze_degraded_for_test()
        .await
        .expect("hard-freeze the mixed stop receipt fixture");
    fixture
        .manager
        .install_canonical_pending_for_test(test_record_review_predecessor(
            absent_task_id,
            fixture.repository.id,
            1,
        ))
        .await
        .expect("install the mixed receipt predecessor");
    let before = fixture
        .manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect mixed receipt barriers");
    let identity = test_stop_batch_identity(vec![fixture.task.id, absent_task_id], 2);

    fixture
        .manager
        .inject_stop_intent_completion_for_test(
            identity.clone(),
            empty_confirmed_stop_completion(identity),
        )
        .await
        .expect("inject a mixed stop receipt");

    let after = fixture
        .manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect barriers after the mixed receipt");
    assert_eq!(after.staged_stop_completion_count, 0);
    assert_eq!(after.barrier_epoch, before.barrier_epoch);
    fixture.runner.release.notify_one();
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn current_mismatched_stop_receipt_with_a_predecessor_freezes_without_staging() {
    let fixture =
        running_hard_freeze_fixture("current mismatched stop receipt cannot be staged").await;
    fixture
        .manager
        .freeze_degraded_for_test()
        .await
        .expect("hard-freeze the current mismatch fixture");
    fixture
        .manager
        .install_canonical_pending_for_test(test_record_review_predecessor(
            fixture.task.id,
            fixture.repository.id,
            fixture.task.attempt,
        ))
        .await
        .expect("install the current task predecessor");
    let before = fixture
        .manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect current mismatch barriers");
    let identity = test_stop_batch_identity(vec![fixture.task.id], 2);

    fixture
        .manager
        .inject_stop_intent_completion_for_test(
            identity.clone(),
            empty_confirmed_stop_completion(identity),
        )
        .await
        .expect("inject a current mismatched stop receipt");

    let after = fixture
        .manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect barriers after the current mismatch");
    assert_eq!(after.staged_stop_completion_count, 0);
    assert_eq!(after.barrier_epoch, before.barrier_epoch);
    fixture.runner.release.notify_one();
}
