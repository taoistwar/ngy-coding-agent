use super::*;

#[cfg(feature = "test-support")]
#[tokio::test]
async fn terminal_projection_distinguishes_absent_from_current_mismatch() {
    let fixture = running_hard_freeze_fixture("terminal projection current mismatch freezes").await;
    let target = EventCursor::new(1).expect("one is a valid projection target");
    let absent_attempt = TerminalProjectionAttempt::try_new(
        TaskId::new(),
        1,
        1,
        target,
        TaskEventKind::TaskCompleted,
    )
    .expect("construct an absent terminal projection attempt");
    fixture
        .manager
        .inject_terminal_projection_for_test(TerminalProjectionCompletion::new(
            absent_attempt,
            Ok(()),
        ))
        .await
        .expect("inject an absent terminal projection");
    assert!(
        fixture.manager.safety_snapshot_for_test().await.is_ok(),
        "a fully absent terminal projection is a stale no-op"
    );

    let current_mismatch = TerminalProjectionAttempt::try_new(
        fixture.task.id,
        u64::MAX,
        1,
        target,
        TaskEventKind::TaskCompleted,
    )
    .expect("construct a current mismatched terminal projection attempt");
    fixture
        .manager
        .inject_terminal_projection_for_test(TerminalProjectionCompletion::new(
            current_mismatch,
            Ok(()),
        ))
        .await
        .expect("inject a current mismatched terminal projection");
    assert!(matches!(
        fixture.manager.safety_snapshot_for_test().await,
        Err(TaskManagerError::Frozen)
    ));
    fixture.runner.release.notify_one();
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn terminal_projection_store_error_retries_the_same_target_without_a_mutation() {
    let fixture =
        paused_terminal_projection_fixture("terminal projection retries the same target").await;
    let before = fixture
        .manager
        .active_stop_snapshot_for_test(fixture.task.id)
        .await
        .expect("inspect the initial projection barrier")
        .expect("the initial projection remains active");
    let initial = before
        .terminal_projection_attempt
        .expect("the initial projection attempt is actor-owned");

    let retry = fixture
        .manager
        .inject_terminal_projection_for_test(TerminalProjectionCompletion::new(
            initial,
            Err(EventDispatcherError::Store(Arc::new(
                StoreError::InvariantViolation("injected terminal projection read failure"),
            ))),
        ))
        .await
        .expect("inject a retryable terminal projection failure");
    let current = retry
        .current_attempt
        .expect("the retry projection attempt remains actor-owned");
    assert!(retry.active);
    assert_eq!(retry.phase, Some(AdmissionPhase::ProjectionPending));
    assert_eq!(current.task_id(), initial.task_id());
    assert_eq!(current.operation_nonce(), initial.operation_nonce());
    assert_eq!(current.target(), initial.target());
    assert_eq!(current.event_kind(), initial.event_kind());
    assert_eq!(
        current.attempt_id(),
        before.next_terminal_projection_attempt_id
    );
    assert_eq!(
        retry.next_attempt_id,
        current
            .attempt_id()
            .checked_add(1)
            .expect("the retry attempt remains in range")
    );
    assert_eq!(
        retry.next_typed_write_attempt_id,
        before.next_typed_write_attempt_id
    );
    assert_eq!(
        retry.next_mutation_sequence,
        Some(before.next_mutation_sequence)
    );
    assert!(retry.cleanup_available);
    assert!(retry.permit_active);
    assert!(retry.registry_owned);
    assert!(!retry.hard_frozen);

    fixture.hooks.resume();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if fixture
                .manager
                .active_stop_snapshot_for_test(fixture.task.id)
                .await
                .is_ok_and(|snapshot| snapshot.is_none())
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the exact retry projects and releases ownership");
    assert_eq!(
        fixture
            .store
            .task_detail(fixture.task.id)
            .await
            .expect("load the retried terminal task")
            .expect("the retried terminal task exists")
            .task
            .status,
        TaskStatus::Failed
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn current_terminal_projection_target_mismatch_freezes_and_retains_ownership() {
    let fixture =
        paused_terminal_projection_fixture("current projection target mismatch freezes").await;
    let before = fixture
        .manager
        .active_stop_snapshot_for_test(fixture.task.id)
        .await
        .expect("inspect the current projection barrier")
        .expect("the current projection remains active");
    let current = before
        .terminal_projection_attempt
        .expect("the current projection attempt is actor-owned");
    let mismatched_target = EventCursor::new(
        current
            .target()
            .get()
            .checked_add(1)
            .expect("the mismatched projection target remains in range"),
    )
    .expect("construct a nonzero mismatched projection target");
    let mismatch = TerminalProjectionAttempt::try_new(
        current.task_id(),
        current.operation_nonce(),
        current.attempt_id(),
        mismatched_target,
        current.event_kind(),
    )
    .expect("construct a current mismatched projection attempt");

    let after = fixture
        .manager
        .inject_terminal_projection_for_test(TerminalProjectionCompletion::new(mismatch, Ok(())))
        .await
        .expect("inject a current mismatched projection completion");
    assert!(after.active);
    assert_eq!(after.current_attempt, Some(current));
    assert!(after.cleanup_available);
    assert!(after.permit_active);
    assert!(after.registry_owned);
    assert!(after.hard_frozen);
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn closed_terminal_projection_freezes_and_retains_all_ownership() {
    let fixture = paused_terminal_projection_fixture("closed projection retains ownership").await;
    let before = fixture
        .manager
        .active_stop_snapshot_for_test(fixture.task.id)
        .await
        .expect("inspect the projection before dispatcher closure")
        .expect("the projection before dispatcher closure remains active");
    let current = before
        .terminal_projection_attempt
        .expect("the current projection attempt is actor-owned");

    let after = fixture
        .manager
        .inject_terminal_projection_for_test(TerminalProjectionCompletion::new(
            current,
            Err(EventDispatcherError::Closed),
        ))
        .await
        .expect("inject a closed dispatcher completion");
    assert!(after.active);
    assert_eq!(after.phase, Some(AdmissionPhase::ProjectionPending));
    assert_eq!(after.current_attempt, Some(current));
    assert!(after.cleanup_available);
    assert!(after.permit_active);
    assert!(after.registry_owned);
    assert!(after.hard_frozen);
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn terminal_release_preflight_failure_consumes_nothing() {
    let fixture =
        paused_terminal_projection_fixture("terminal release preflight is side-effect free").await;
    assert!(
        fixture
            .manager
            .corrupt_safety_registry_nonce_for_test(fixture.task.id),
        "the paused terminal remains registry-owned before corruption"
    );
    fixture.hooks.resume();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(
                fixture.manager.safety_snapshot_for_test().await,
                Err(TaskManagerError::Frozen)
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the corrupted release preflight freezes");

    let retained = fixture
        .manager
        .active_stop_snapshot_for_test(fixture.task.id)
        .await
        .expect("inspect retained ownership after release preflight failure")
        .expect("release preflight failure retains active ownership");
    assert_eq!(retained.phase, AdmissionPhase::ProjectionPending);
    assert_eq!(retained.active_count, 1);
    assert_eq!(retained.available_permits, 0);
    assert!(retained.cleanup_confirmed);
    assert!(retained.cleanup_available);
    assert!(retained.permit_active);
    assert!(retained.done_receiver_owned);
    assert!(retained.terminal_projection_attempt.is_some());
    assert!(retained.hard_frozen);
    assert_eq!(
        fixture.manager.safety_registry_snapshot_for_test(),
        SafetyRegistrySnapshotForTest {
            entry_count: 1,
            pending_critical_count: 0,
            safety_latched_count: 0,
        }
    );
    assert_eq!(fixture.hooks.active_count(), 1);
    assert_eq!(fixture.hooks.available_permits(), 0);
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn critical_fact_during_projection_is_cleared_only_by_exact_release() {
    let fixture =
        paused_terminal_projection_fixture("projection retains a critical safety fact").await;
    fixture
        .manager
        .notify_storage_critical_for_test(vec![MonitoredStorageScope::RepositoryGit(
            fixture.repository.id,
        )]);
    fixture
        .manager
        .handle_critical_wake_for_test()
        .await
        .expect("force actor delivery of the critical wake");
    assert_eq!(
        fixture.manager.safety_registry_snapshot_for_test(),
        SafetyRegistrySnapshotForTest {
            entry_count: 1,
            pending_critical_count: 1,
            safety_latched_count: 1,
        }
    );
    let retained = fixture
        .manager
        .active_stop_snapshot_for_test(fixture.task.id)
        .await
        .expect("inspect critical projection ownership")
        .expect("critical projection remains active");
    assert_eq!(retained.phase, AdmissionPhase::ProjectionPending);
    assert_eq!(retained.stage, ActiveStopStageForTest::NoWinner);
    assert!(retained.cleanup_available);
    assert!(retained.permit_active);

    fixture.hooks.resume();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if fixture
                .manager
                .active_stop_snapshot_for_test(fixture.task.id)
                .await
                .is_ok_and(|snapshot| snapshot.is_none())
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the exact projection clears critical ownership at final release");
    assert_eq!(
        fixture.manager.safety_registry_snapshot_for_test(),
        SafetyRegistrySnapshotForTest {
            entry_count: 0,
            pending_critical_count: 0,
            safety_latched_count: 0,
        }
    );
    assert_eq!(fixture.hooks.active_count(), 0);
    assert_eq!(fixture.hooks.available_permits(), 1);
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn hard_frozen_critical_wake_retains_the_registry_fact_and_identity_counters() {
    let fixture = running_hard_freeze_fixture("hard freeze retains a critical registry fact").await;
    let before = fixture
        .manager
        .active_stop_snapshot_for_test(fixture.task.id)
        .await
        .expect("inspect pre-freeze critical task")
        .expect("hard-freeze critical task remains active");
    let before_barrier = fixture
        .manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect pre-critical exact barrier");
    fixture
        .manager
        .freeze_degraded_for_test()
        .await
        .expect("hard-freeze the critical fixture");

    fixture.manager.notify_storage_critical_at_for_test(
        vec![MonitoredStorageScope::RepositoryGit(fixture.repository.id)],
        Instant::now(),
    );
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }

    assert_eq!(
        fixture.manager.safety_registry_snapshot_for_test(),
        SafetyRegistrySnapshotForTest {
            entry_count: 1,
            pending_critical_count: 1,
            safety_latched_count: 1,
        },
        "hard freeze must gate the actor before it takes a critical fact"
    );
    let after = fixture
        .manager
        .active_stop_snapshot_for_test(fixture.task.id)
        .await
        .expect("inspect hard-frozen critical task")
        .expect("hard-frozen critical ownership remains active");
    let after_barrier = fixture
        .manager
        .exact_barrier_snapshot_for_test()
        .await
        .expect("inspect post-critical exact barrier");
    assert_eq!(after.stage, ActiveStopStageForTest::NoWinner);
    assert_eq!(after.next_mutation_sequence, before.next_mutation_sequence);
    assert_eq!(after_barrier.barrier_epoch, before_barrier.barrier_epoch);
    fixture.runner.release.notify_one();
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn hard_freeze_blocks_final_stop_retry_but_accepts_the_precommitted_exact_result() {
    let fixture = paused_final_stop_fixture().await;
    let (identity, request) = match fixture.pending.clone() {
        PendingDurableResult::FinalizeStoppedTask { identity, request } => (identity, request),
        other => panic!("expected paused final-stop ownership, got {other:?}"),
    };
    let before = fixture
        .manager
        .active_stop_snapshot_for_test(fixture.task.id)
        .await
        .expect("inspect pre-freeze final stop")
        .expect("pre-freeze final stop remains active");
    fixture
        .controller
        .arm_fault(StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::FinalizeStoppedTask),
            count: 1,
        })
        .expect("arm an N+1 final-stop probe");
    fixture
        .manager
        .freeze_degraded_for_test()
        .await
        .expect("hard-freeze the paused final stop");
    fixture
        .manager
        .inject_final_stop_completion_for_test(
            identity,
            request,
            DurableCompletion {
                identity: DurableOperationIdentity::TaskMutation(identity),
                sequence_disposition: MutationSequenceDisposition::AdvanceNext,
                disposition: DurableDisposition::KnownNotApplied {
                    reason: KnownNotAppliedReason::BusyRolledBack,
                    outcome: None,
                    error: None,
                },
            },
        )
        .await
        .expect("inject a retryable final-stop completion after hard freeze");

    assert!(
        tokio::time::timeout(
            Duration::from_millis(300),
            fixture
                .controller
                .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 2),
        )
        .await
        .is_err(),
        "hard freeze must prevent a final-stop N+1 submission"
    );
    let retained = fixture
        .manager
        .active_stop_snapshot_for_test(fixture.task.id)
        .await
        .expect("inspect retained final stop")
        .expect("hard-frozen final stop retains ownership");
    assert_eq!(
        retained.stage,
        ActiveStopStageForTest::FinalStopWritePending
    );
    assert_eq!(
        retained.next_mutation_sequence,
        before.next_mutation_sequence
    );
    assert_eq!(retained.pending_terminal_identity, None);
    assert_eq!(
        fixture
            .manager
            .active_pending_stop_write_for_test(fixture.task.id)
            .await
            .expect("inspect retained final-stop identity"),
        Some(fixture.pending)
    );

    assert_eq!(
        fixture
            .controller
            .release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    wait_for_status(&fixture.store, fixture.task.id, TaskStatus::Cancelled).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if fixture
                .manager
                .active_stop_snapshot_for_test(fixture.task.id)
                .await
                .is_ok_and(|snapshot| snapshot.is_none())
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the precommitted exact final stop projects and releases ownership");
    assert!(matches!(
        fixture
            .cancel
            .await
            .expect("join paused final-stop cancel"),
        Ok(CancelOutcome::Accepted { task }) if task.id == fixture.task.id
    ));
}
