#![cfg(feature = "test-support")]

mod support;

use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;

use coding_agent_app::{
    DurableDisposition, DurableOperationIdentity, DurableOperationKind,
    FinalizeReviewedTaskRequest, FinalizeUnreviewedTaskRequest, KnownNotAppliedError,
    KnownNotAppliedReason, MutationSequence, MutationSequenceDisposition, OutcomeUnknownReason,
    PendingDurableResult, PendingReplayReceipt, RecordReviewRequest, ServiceState,
    ServiceStateController, StoreWriterError, StoreWriterFaultPoint, StoreWriterFaultSpec,
    StoreWriterHandle, StoreWriterOperationKind, StoreWriterPriority, StoreWriterSchedulingError,
    StoreWriterSchedulingHarness, StoreWriterSubmitError, StoreWriterTestController,
    TaskMutationIdentity,
};
use coding_agent_domain::{
    CanonicalPath, CheckActor, CheckEvidence, CheckEvidenceStatus, ClientRequestId,
    DeliveryReadiness, FindingSeverity, NewReviewEvidence, NewTask, PlanItem, PlanItemStatus,
    PlanSnapshot, RepositoryId, RequiredCheck, ReviewCoverageEvidence, ReviewDecisionSource,
    ReviewFinding, ReviewVerdict, Task, TaskEventPayload, TaskFailure, TaskId, TaskStatus,
    WorkspaceDigest,
};
use coding_agent_store::{
    AppendEventOutcome, AttemptArtifactIdentity, AttemptArtifactState, ClaimTaskOutcome,
    ClaimTaskReconciliationOutcome, ClaimTaskRequest, FinalizeReviewedTaskOutcome,
    FinalizeStoppedTaskOutcome, FinalizeStoppedTaskRequest, FinalizeUnreviewedTaskOutcome,
    PersistStopIntentOutcome, QueueLimitedCreateTaskOutcome, QueueLimitedRetryTaskOutcome,
    RecordReviewOutcome, RegisterRepositoryOutcome, ReserveAttemptArtifact,
    ReserveAttemptArtifactOutcome, StopIntentKind, StopIntentRequest, StoreError, TaskTransition,
    TransitionOutcome, UpdateAttemptArtifactOutcome,
};
use tokio::time::{Duration, Instant};

#[tokio::test]
async fn writer_serializes_concurrent_creates() {
    let fixture = support::writer_fixture().await;
    let a = fixture.writer.create_task(
        support::new_task(fixture.repository.id, "a"),
        support::deadline(),
    );
    let b = fixture.writer.create_task(
        support::new_task(fixture.repository.id, "b"),
        support::deadline(),
    );
    let (a, b) = tokio::join!(a, b);

    assert!(a.unwrap().event_id < b.unwrap().event_id);
}

#[tokio::test]
async fn expired_deadline_skips_transaction_and_leaves_task_uncommitted() {
    let fixture = support::writer_fixture().await;

    let result = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "must not commit"),
            Instant::now(),
        )
        .await;

    assert!(matches!(result, Err(StoreWriterError::Busy)));
    assert!(
        fixture
            .store
            .bootstrap_snapshot()
            .await
            .unwrap()
            .tasks
            .is_empty()
    );
    assert_eq!(fixture.wake.count(), 0);
}

#[tokio::test]
async fn real_sqlite_busy_exhaustion_is_uncommitted_and_does_not_wake() {
    let fixture = support::writer_fixture().await;
    let options = fixture
        .store
        .pool()
        .connect_options()
        .as_ref()
        .clone()
        .busy_timeout(Duration::ZERO);
    fixture.store.pool().set_connect_options(options);
    let existing_connections = fixture.store.pool().size();
    let mut legacy_connections = Vec::with_capacity(existing_connections as usize);
    for _ in 0..existing_connections {
        legacy_connections.push(
            fixture
                .store
                .pool()
                .acquire()
                .await
                .expect("reserve connection with the original busy timeout"),
        );
    }
    let transaction = fixture
        .store
        .pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("hold the SQLite writer lock");

    let result = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "real SQLite busy"),
            support::deadline(),
        )
        .await;

    assert!(matches!(result, Err(StoreWriterError::Busy)));
    assert!(
        fixture
            .store
            .bootstrap_snapshot()
            .await
            .unwrap()
            .tasks
            .is_empty()
    );
    assert_eq!(fixture.wake.count(), 0);
    transaction.rollback().await.unwrap();
    drop(legacy_connections);
}

#[tokio::test]
async fn repository_only_write_does_not_wake_dispatcher() {
    let fixture = support::writer_fixture().await;

    let receipt = fixture
        .writer
        .register_repository(fixture.repository_input("second"), support::deadline())
        .await
        .unwrap();

    assert!(matches!(
        receipt.value,
        RegisterRepositoryOutcome::Created(_)
    ));
    assert_eq!(receipt.event_id, None);
    assert_eq!(fixture.wake.count(), 0);
}

#[tokio::test]
async fn artifact_lifecycle_is_serialized_through_writer_without_event_wakes() {
    let fixture = support::writer_fixture().await;
    let created_task = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "artifact lifecycle"),
            support::deadline(),
        )
        .await
        .unwrap();
    let task = created_task.value.task().clone();
    assert_eq!(fixture.wake.count(), 1);
    let identity = AttemptArtifactIdentity {
        task_id: task.id,
        repository_id: task.repository_id,
        attempt: task.attempt,
    };
    let input = ReserveAttemptArtifact {
        identity,
        base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
        branch_name: format!("codex/task-{}-attempt-{}", task.id, task.attempt),
        worktree_path: CanonicalPath::try_from_canonical(
            fixture
                .repository
                .git_root
                .as_path()
                .join("attempt-artifacts")
                .join(task.id.to_string()),
        )
        .unwrap(),
    };

    let reserved = fixture
        .writer
        .reserve_attempt_artifact(input.clone(), support::deadline())
        .await
        .unwrap();
    assert!(matches!(
        reserved.value,
        ReserveAttemptArtifactOutcome::Created(ref artifact)
            if artifact.state == AttemptArtifactState::Reserved
    ));
    assert_eq!(reserved.event_id, None);
    assert_eq!(fixture.wake.count(), 1);

    let repeated = fixture
        .writer
        .reserve_attempt_artifact(input, support::deadline())
        .await
        .unwrap();
    assert!(matches!(
        repeated.value,
        ReserveAttemptArtifactOutcome::Existing(_)
    ));
    assert_eq!(fixture.wake.count(), 1);

    let ready = fixture
        .writer
        .mark_attempt_artifact_ready(identity, support::deadline())
        .await
        .unwrap();
    assert!(matches!(
        ready.value,
        UpdateAttemptArtifactOutcome::Applied(ref artifact)
            if artifact.state == AttemptArtifactState::Ready
    ));
    assert_eq!(ready.event_id, None);
    assert_eq!(fixture.wake.count(), 1);
}

#[tokio::test]
async fn review_receipts_rewake_applied_and_existing_at_the_original_event_watermark() {
    let fixture = support::writer_fixture().await;
    let task = running_review_task(
        &fixture.writer,
        fixture.repository.id,
        "intermediate review",
    )
    .await;
    let request = RecordReviewRequest {
        task_id: task.id,
        expected_repository_id: task.repository_id,
        expected_attempt: task.attempt,
        evidence: changes_requested(1),
    };
    let before_wakes = fixture.wake.count();

    let applied = fixture
        .writer
        .record_review(request.clone(), support::deadline())
        .await
        .expect("record review");
    let applied_event_id = match &applied.value {
        RecordReviewOutcome::Applied { event_id, .. } => *event_id,
        RecordReviewOutcome::Existing { .. } => panic!("first review must apply"),
    };
    assert_eq!(applied.event_id, Some(applied_event_id));
    assert_eq!(
        fixture
            .store
            .bootstrap_snapshot()
            .await
            .unwrap()
            .latest_event_id
            .get(),
        applied_event_id.get()
    );

    let existing = fixture
        .writer
        .record_review(request, support::deadline())
        .await
        .expect("replay exact review");
    assert!(matches!(
        existing.value,
        RecordReviewOutcome::Existing { event_id, .. } if event_id == applied_event_id
    ));
    assert_eq!(existing.event_id, Some(applied_event_id));
    assert_eq!(
        fixture
            .store
            .bootstrap_snapshot()
            .await
            .unwrap()
            .latest_event_id
            .get(),
        applied_event_id.get()
    );
    assert_eq!(fixture.wake.count(), before_wakes + 2);
}

#[tokio::test]
async fn finalization_receipts_use_terminal_high_watermark_and_rewake_existing() {
    let fixture = support::writer_fixture().await;
    let task = running_review_task(&fixture.writer, fixture.repository.id, "final review").await;
    let request = FinalizeReviewedTaskRequest {
        task_id: task.id,
        expected_repository_id: task.repository_id,
        expected_attempt: task.attempt,
        evidence: approved(1),
    };
    let before_wakes = fixture.wake.count();

    let applied = fixture
        .writer
        .finalize_reviewed_task(request.clone(), support::deadline())
        .await
        .expect("finalize reviewed task");
    let (review_event_id, terminal_event_id) = match &applied.value {
        FinalizeReviewedTaskOutcome::Applied {
            task,
            review_event_id,
            terminal_event_id,
            ..
        } => {
            assert_eq!(task.status, TaskStatus::Completed);
            assert_eq!(task.delivery_readiness, DeliveryReadiness::ReviewApproved);
            (*review_event_id, *terminal_event_id)
        }
        FinalizeReviewedTaskOutcome::Existing { .. } => panic!("first finalization must apply"),
    };
    assert!(review_event_id < terminal_event_id);
    assert_eq!(applied.event_id, Some(terminal_event_id));
    assert_eq!(
        fixture
            .store
            .bootstrap_snapshot()
            .await
            .unwrap()
            .latest_event_id
            .get(),
        terminal_event_id.get()
    );

    let existing = fixture
        .writer
        .finalize_reviewed_task(request, support::deadline())
        .await
        .expect("replay exact finalization");
    assert!(matches!(
        existing.value,
        FinalizeReviewedTaskOutcome::Existing {
            review_event_id: existing_review,
            terminal_event_id: existing_terminal,
            ..
        } if existing_review == review_event_id && existing_terminal == terminal_event_id
    ));
    assert_eq!(existing.event_id, Some(terminal_event_id));
    assert_eq!(
        fixture
            .store
            .bootstrap_snapshot()
            .await
            .unwrap()
            .latest_event_id
            .get(),
        terminal_event_id.get()
    );
    assert_eq!(fixture.wake.count(), before_wakes + 2);
}

#[tokio::test]
async fn review_fault_filters_distinguish_record_from_finalize() {
    let fixture = support::store_fixture().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailBeforeExecute,
                operation: Some(StoreWriterOperationKind::RecordReview),
                count: 1,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailBeforeExecute,
                operation: Some(StoreWriterOperationKind::FinalizeReviewedTask),
                count: 1,
            },
        ])
        .unwrap(),
    );
    let wake = Arc::new(support::CountingWake::default());
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store.clone(),
        wake,
        8,
        controller.clone(),
    );
    let task = running_review_task(&writer, fixture.repository.id, "fault kinds").await;

    let record = writer
        .record_review(
            RecordReviewRequest {
                task_id: task.id,
                expected_repository_id: task.repository_id,
                expected_attempt: task.attempt,
                evidence: changes_requested(1),
            },
            support::deadline(),
        )
        .await;
    assert!(matches!(record, Err(StoreWriterError::Store(_))));
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::FailBeforeExecute,
            StoreWriterOperationKind::RecordReview,
        ),
        1
    );
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::FailBeforeExecute,
            StoreWriterOperationKind::FinalizeReviewedTask,
        ),
        0
    );

    let finalize = writer
        .finalize_reviewed_task(
            FinalizeReviewedTaskRequest {
                task_id: task.id,
                expected_repository_id: task.repository_id,
                expected_attempt: task.attempt,
                evidence: approved(1),
            },
            support::deadline(),
        )
        .await;
    assert!(matches!(finalize, Err(StoreWriterError::Store(_))));
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::FailBeforeExecute,
            StoreWriterOperationKind::FinalizeReviewedTask,
        ),
        1
    );
}

#[tokio::test]
async fn committed_event_outcomes_wake_once_and_non_events_do_not() {
    let fixture = support::writer_fixture().await;
    let input = support::new_task(fixture.repository.id, "event wake matrix");
    let created = fixture
        .writer
        .create_task(input.clone(), support::deadline())
        .await
        .unwrap();
    let task = created.value.task().clone();
    assert_eq!(fixture.wake.count(), 1);

    let duplicate = fixture
        .writer
        .create_task(input, support::deadline())
        .await
        .unwrap();
    assert_eq!(duplicate.event_id, None);
    assert_eq!(fixture.wake.count(), 1);

    let running = fixture
        .writer
        .start_task(task.id, support::deadline())
        .await
        .unwrap();
    assert!(matches!(running.value, TransitionOutcome::Applied { .. }));
    assert_eq!(fixture.wake.count(), 2);

    let panel = fixture
        .writer
        .append_running_event(
            task.id,
            TaskEventPayload::PlanUpdated {
                plan: PlanSnapshot::legacy(1, Vec::new()),
            },
            support::deadline(),
        )
        .await
        .unwrap();
    assert!(matches!(panel.value, AppendEventOutcome::Applied { .. }));
    assert_eq!(fixture.wake.count(), 3);

    let conflict = fixture
        .writer
        .cancel_task(task.id, TaskStatus::Queued, support::deadline())
        .await
        .unwrap();
    assert!(matches!(conflict.value, TransitionOutcome::Conflict { .. }));
    assert_eq!(conflict.event_id, None);
    assert_eq!(fixture.wake.count(), 3);
}

#[tokio::test]
async fn typed_fail_task_owns_the_running_to_failed_transition() {
    let fixture = support::writer_fixture().await;
    let task = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "typed failure"),
            support::deadline(),
        )
        .await
        .unwrap()
        .value
        .task()
        .clone();
    fixture
        .writer
        .start_task(task.id, support::deadline())
        .await
        .unwrap();
    let failure = support::failure("TYPED_FAILURE");

    let receipt = fixture
        .writer
        .fail_task(task.id, failure.clone(), support::deadline())
        .await
        .unwrap();

    let TransitionOutcome::Applied { task, .. } = receipt.value else {
        panic!("typed fail must commit the running task");
    };
    assert_eq!(task.status, TaskStatus::Failed);
    assert_eq!(task.failure, Some(failure));
}

#[tokio::test]
async fn retry_task_is_idempotent_and_wakes_only_for_the_new_child_event() {
    let fixture = support::writer_fixture().await;
    let created = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "retry source"),
            support::deadline(),
        )
        .await
        .unwrap();
    let task = created.value.task().clone();
    fixture
        .writer
        .cancel_task(task.id, TaskStatus::Queued, support::deadline())
        .await
        .unwrap();
    let before = fixture.wake.count();

    let first = fixture
        .writer
        .retry_task(task.id, support::deadline())
        .await
        .unwrap();
    let second = fixture
        .writer
        .retry_task(task.id, support::deadline())
        .await
        .unwrap();

    assert!(first.event_id.is_some());
    assert_eq!(second.event_id, None);
    assert_eq!(first.value.task().id, second.value.task().id);
    assert_eq!(fixture.wake.count(), before + 1);
}

#[tokio::test]
async fn panicking_wake_cannot_turn_durable_commit_into_failure() {
    let fixture = support::store_fixture().await;
    let writer =
        StoreWriterHandle::spawn(fixture.store.clone(), Arc::new(support::PanickingWake), 4);

    let receipt = writer
        .create_task(
            support::new_task(fixture.repository.id, "wake panic"),
            support::deadline(),
        )
        .await
        .expect("durable receipt survives wake panic");

    assert!(receipt.event_id.is_some());
    assert_eq!(
        fixture
            .store
            .bootstrap_snapshot()
            .await
            .unwrap()
            .tasks
            .len(),
        1
    );
}

#[tokio::test]
async fn legacy_bulk_recovery_is_running_only_and_preserves_its_watermark() {
    let fixture = support::writer_fixture().await;
    let mut running_tasks = Vec::new();
    for prompt in ["recover a", "recover b"] {
        let queued = fixture
            .writer
            .create_task(
                support::new_task(fixture.repository.id, prompt),
                support::deadline(),
            )
            .await
            .unwrap()
            .value
            .task()
            .clone();
        let running = match fixture
            .writer
            .start_task(queued.id, support::deadline())
            .await
            .unwrap()
            .value
        {
            TransitionOutcome::Applied { task, .. } => task,
            TransitionOutcome::Conflict { .. } => panic!("fixture start must apply"),
        };
        running_tasks.push(running);
    }
    let queued = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "preserve queued"),
            support::deadline(),
        )
        .await
        .unwrap()
        .value
        .task()
        .clone();
    let before = fixture.wake.count();

    let receipt = fixture
        .writer
        .recover_incomplete(
            support::timestamp(),
            support::failure("APP_RESTARTED"),
            support::deadline(),
        )
        .await
        .unwrap();

    assert_eq!(receipt.value.interrupted_count, 2);
    assert_eq!(
        fixture
            .store
            .task_detail(queued.id)
            .await
            .unwrap()
            .unwrap()
            .task
            .status,
        TaskStatus::Queued
    );
    for task in running_tasks {
        assert_eq!(
            fixture
                .store
                .task_detail(task.id)
                .await
                .unwrap()
                .unwrap()
                .task
                .status,
            TaskStatus::Interrupted
        );
    }
    assert_eq!(receipt.event_id, receipt.value.last_event_id);
    assert_eq!(
        receipt.value.high_watermark.get(),
        receipt.value.last_event_id.unwrap().get()
    );
    assert_eq!(fixture.wake.count(), before + 1);
}

#[tokio::test]
async fn guarded_interrupt_requires_final_stops_and_preserves_the_stop_winner() {
    let fixture = support::writer_fixture().await;
    let stopped = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "guarded stop winner"),
            support::deadline(),
        )
        .await
        .unwrap()
        .value
        .task()
        .clone();
    let stopped = match fixture
        .writer
        .start_task(stopped.id, support::deadline())
        .await
        .unwrap()
        .value
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("fixture start must apply"),
    };
    let stop_kind = StopIntentKind::DiskPressureCritical;
    let stop_identity = DurableOperationIdentity::stop_intent_batch(vec![mutation_identity(
        stopped.id,
        1,
        DurableOperationKind::PersistStopIntent,
    )])
    .unwrap();
    let stop_completion = fixture
        .writer
        .submit_stop_intent_batch(
            stop_identity,
            vec![StopIntentRequest {
                task_id: stopped.id,
                expected_repository_id: stopped.repository_id,
                expected_attempt: stopped.attempt,
                kind: stop_kind,
            }],
            support::deadline(),
        )
        .unwrap()
        .completion()
        .await;
    assert!(matches!(
        stop_completion.disposition,
        DurableDisposition::Confirmed(_)
    ));
    let before_guard = fixture.store.scheduler_bootstrap_snapshot().await.unwrap();

    assert!(matches!(
        fixture
            .writer
            .interrupt_remaining_after_stops(
                support::failure("STORE_DEGRADED_RECOVERY"),
                support::deadline(),
            )
            .await
            .unwrap_err(),
        StoreWriterError::Store(StoreError::InvariantViolation(_))
    ));
    assert_eq!(
        fixture.store.scheduler_bootstrap_snapshot().await.unwrap(),
        before_guard
    );

    let final_stop = fixture
        .writer
        .submit_finalize_stopped_task(
            mutation_identity(stopped.id, 2, DurableOperationKind::FinalizeStoppedTask),
            FinalizeStoppedTaskRequest {
                task_id: stopped.id,
                expected_repository_id: stopped.repository_id,
                expected_attempt: stopped.attempt,
                expected_intent: stop_kind,
            },
            support::deadline(),
        )
        .unwrap()
        .completion()
        .await;
    let stopped_receipt = match final_stop.disposition {
        DurableDisposition::Confirmed(FinalizeStoppedTaskOutcome::Applied(receipt)) => receipt,
        other => panic!("final stop must remain typed, got {other:?}"),
    };
    let running = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "guarded running"),
            support::deadline(),
        )
        .await
        .unwrap()
        .value
        .task()
        .clone();
    let running = match fixture
        .writer
        .start_task(running.id, support::deadline())
        .await
        .unwrap()
        .value
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("fixture start must apply"),
    };
    let queued = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "guarded queued"),
            support::deadline(),
        )
        .await
        .unwrap()
        .value
        .task()
        .clone();
    let before_wake = fixture.wake.count();
    let failure = support::failure("STORE_DEGRADED_RECOVERY");

    let receipt = fixture
        .writer
        .interrupt_remaining_after_stops(failure.clone(), support::deadline())
        .await
        .unwrap();

    assert_eq!(receipt.value.finalized_stop_count, 0);
    assert_eq!(receipt.value.interrupted_count, 2);
    assert_eq!(receipt.event_id, receipt.value.last_event_id);
    assert_eq!(
        receipt.value.high_watermark,
        fixture.store.latest_event_id().await.unwrap()
    );
    assert_eq!(
        receipt.value.membership_high_watermark,
        fixture
            .store
            .scheduler_bootstrap_snapshot()
            .await
            .unwrap()
            .membership_event_id
    );
    assert_eq!(fixture.wake.count(), before_wake + 1);
    assert_eq!(
        fixture
            .store
            .task_detail(stopped.id)
            .await
            .unwrap()
            .unwrap()
            .task,
        stopped_receipt.task
    );
    for task in [running, queued] {
        let task = fixture
            .store
            .task_detail(task.id)
            .await
            .unwrap()
            .unwrap()
            .task;
        assert_eq!(task.status, TaskStatus::Interrupted);
        assert_eq!(task.failure, Some(failure.clone()));
    }
}

#[tokio::test]
async fn guarded_interrupt_commit_before_reply_replays_without_duplicate_events() {
    let fixture = support::store_fixture().await;
    let running = fixture
        .store
        .create_task(support::new_task(
            fixture.repository.id,
            "guarded reply-loss running",
        ))
        .await
        .unwrap()
        .task()
        .clone();
    fixture
        .store
        .transition_with_event(running.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .unwrap();
    let queued = fixture
        .store
        .create_task(support::new_task(
            fixture.repository.id,
            "guarded reply-loss queued",
        ))
        .await
        .unwrap()
        .task()
        .clone();
    let wake = Arc::new(support::CountingWake::default());
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            operation: Some(StoreWriterOperationKind::InterruptRemainingAfterStops),
            count: 1,
        }])
        .unwrap(),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store.clone(),
        wake.clone(),
        8,
        controller,
    );
    let failure = support::failure("APP_SHUTDOWN");

    assert!(matches!(
        writer
            .interrupt_remaining_after_stops(failure.clone(), support::deadline())
            .await,
        Err(StoreWriterError::Closed)
    ));
    for task in [&running, &queued] {
        let current = fixture
            .store
            .task_detail(task.id)
            .await
            .unwrap()
            .unwrap()
            .task;
        assert_eq!(current.status, TaskStatus::Interrupted);
        assert_eq!(current.failure, Some(failure.clone()));
    }
    let committed = fixture.store.scheduler_bootstrap_snapshot().await.unwrap();
    let committed_event_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_events WHERE kind = 'task.interrupted'")
            .fetch_one(fixture.store.pool())
            .await
            .unwrap();
    assert_eq!(committed_event_count, 2);
    assert_eq!(wake.count(), 0);

    let replay = writer
        .interrupt_remaining_after_stops(failure, support::deadline())
        .await
        .unwrap();

    assert_eq!(replay.value.finalized_stop_count, 0);
    assert_eq!(replay.value.interrupted_count, 0);
    assert_eq!(replay.value.first_event_id, None);
    assert_eq!(replay.value.last_event_id, None);
    assert_eq!(replay.value.high_watermark, committed.latest_event_id);
    assert_eq!(
        replay.value.membership_high_watermark,
        committed.membership_event_id
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_events WHERE kind = 'task.interrupted'"
        )
        .fetch_one(fixture.store.pool())
        .await
        .unwrap(),
        committed_event_count
    );
    assert_eq!(wake.count(), 1);
}

async fn running_review_task(
    writer: &StoreWriterHandle,
    repository_id: RepositoryId,
    prompt: &str,
) -> Task {
    let queued = writer
        .create_task(
            support::new_task(repository_id, prompt),
            support::deadline(),
        )
        .await
        .expect("create review task")
        .value
        .task()
        .clone();
    let running = match writer
        .start_task(queued.id, support::deadline())
        .await
        .expect("start review task")
        .value
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("fixture transition must apply"),
    };
    let plan = PlanSnapshot::try_structured(
        1,
        "Implement and review the approved plan",
        vec![
            PlanItem::try_structured(
                "step-1",
                "Implement",
                "Implement the requested behavior",
                vec!["All required checks pass".to_owned()],
                PlanItemStatus::Completed,
            )
            .unwrap(),
        ],
        vec![required_check()],
    )
    .unwrap();
    writer
        .append_running_event(
            running.id,
            TaskEventPayload::PlanUpdated { plan },
            support::deadline(),
        )
        .await
        .expect("persist structured plan");
    running
}

fn required_check() -> RequiredCheck {
    RequiredCheck::try_cargo_test(
        "project3-cargo-test",
        Some("coding-agent-app".to_owned()),
        None,
    )
    .unwrap()
}

fn review_digest(round: u8) -> WorkspaceDigest {
    let digit = char::from(b'a' + round - 1);
    WorkspaceDigest::try_new(digit.to_string().repeat(64)).unwrap()
}

fn passed_check(round: u8, check: &RequiredCheck, digest: &WorkspaceDigest) -> CheckEvidence {
    CheckEvidence::try_for_check(
        check,
        CheckActor::Executor,
        u32::from(round),
        u64::from(round),
        digest.clone(),
        CheckEvidenceStatus::Passed,
        10,
        "cargo test passed",
        false,
    )
    .unwrap()
}

fn changes_requested(round: u8) -> NewReviewEvidence {
    let digest = review_digest(round);
    let check = required_check();
    NewReviewEvidence::try_new(
        round,
        ReviewDecisionSource::Reviewer,
        u64::from(round),
        digest.clone(),
        ReviewVerdict::ChangesRequested,
        format!("round {round} changes requested"),
        vec![
            ReviewFinding::try_for_review(
                round,
                1,
                FindingSeverity::Blocking,
                "A blocking issue remains",
                Some("src/lib.rs".to_owned()),
                Some(1),
            )
            .unwrap(),
        ],
        Vec::new(),
        vec![check.clone()],
        vec![passed_check(round, &check, &digest)],
        None,
    )
    .unwrap()
}

fn approved(round: u8) -> NewReviewEvidence {
    let digest = review_digest(round);
    let check = required_check();
    NewReviewEvidence::try_new(
        round,
        ReviewDecisionSource::Reviewer,
        u64::from(round),
        digest.clone(),
        ReviewVerdict::Approved,
        format!("round {round} approved"),
        Vec::new(),
        Vec::new(),
        vec![check.clone()],
        vec![passed_check(round, &check, &digest)],
        Some(
            ReviewCoverageEvidence::try_new(u64::from(round), digest, "f".repeat(64), vec![0], 1)
                .unwrap(),
        ),
    )
    .unwrap()
}

#[tokio::test]
async fn controlled_create_uses_the_transactional_queue_limit() {
    let fixture = support::writer_fixture().await;
    let maximum = NonZeroU32::new(1).unwrap();
    fixture
        .store
        .create_task(support::new_task(fixture.repository.id, "fill queue"))
        .await
        .expect("fill the only queue slot");

    let completion = fixture
        .writer
        .submit_queue_limited_create(
            support::new_task(fixture.repository.id, "must be rejected"),
            maximum,
            support::deadline(),
        )
        .expect("bounded ingress accepts the create")
        .completion()
        .await;

    assert_eq!(
        completion.sequence_disposition,
        MutationSequenceDisposition::AdvanceNext
    );
    assert!(matches!(
        completion.disposition,
        DurableDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::ExactReconciliation,
            outcome: Some(QueueLimitedCreateTaskOutcome::QueueFull {
                queued_tasks: 1,
                max_queued_tasks,
            }),
            error: None,
        } if max_queued_tasks == maximum
    ));
    assert_eq!(fixture.wake.count(), 0);
}

#[tokio::test]
async fn controlled_create_checks_existing_before_queue_capacity() {
    let fixture = support::writer_fixture().await;
    let maximum = NonZeroU32::new(1).unwrap();
    let input = support::new_task(fixture.repository.id, "idempotent create");
    let existing = fixture
        .store
        .create_task(input.clone())
        .await
        .expect("seed the idempotent task")
        .task()
        .clone();

    let completion = fixture
        .writer
        .submit_queue_limited_create(input, maximum, support::deadline())
        .expect("bounded ingress accepts the replay")
        .completion()
        .await;

    assert_eq!(
        completion.sequence_disposition,
        MutationSequenceDisposition::AdvanceNext
    );
    assert!(matches!(
        completion.disposition,
        DurableDisposition::Confirmed(QueueLimitedCreateTaskOutcome::Existing { task })
            if task.id == existing.id
    ));
}

#[tokio::test]
async fn controlled_retry_uses_the_transactional_queue_limit() {
    let fixture = support::writer_fixture().await;
    let maximum = NonZeroU32::new(1).unwrap();
    let source = fixture
        .store
        .create_task(support::new_task(fixture.repository.id, "retry source"))
        .await
        .expect("create retry source")
        .task()
        .clone();
    fixture
        .store
        .transition_with_event(source.id, TaskStatus::Queued, TaskTransition::Cancelled)
        .await
        .expect("make source retryable");
    fixture
        .store
        .create_task(support::new_task(fixture.repository.id, "fill retry queue"))
        .await
        .expect("fill the only queue slot");

    let completion = fixture
        .writer
        .submit_queue_limited_retry(source.id, maximum, support::deadline())
        .expect("bounded ingress accepts the retry")
        .completion()
        .await;

    assert!(matches!(
        completion.disposition,
        DurableDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::ExactReconciliation,
            outcome: Some(QueueLimitedRetryTaskOutcome::QueueFull {
                queued_tasks: 1,
                max_queued_tasks,
            }),
            error: None,
        } if max_queued_tasks == maximum
    ));
}

#[tokio::test]
async fn typed_claim_completion_preserves_applied() {
    let fixture = support::writer_fixture().await;
    let task = fixture
        .store
        .create_task(support::new_task(fixture.repository.id, "claim applied"))
        .await
        .expect("create queued task")
        .task()
        .clone();
    let request = claim_request(&task);
    let identity = mutation_identity(task.id, 1, DurableOperationKind::ClaimTask);

    let completion = fixture
        .writer
        .submit_claim_task(identity, request, support::deadline())
        .expect("bounded ingress accepts the claim")
        .completion()
        .await;

    assert_eq!(
        completion.identity,
        DurableOperationIdentity::TaskMutation(identity)
    );
    assert!(matches!(
        completion.disposition,
        DurableDisposition::Confirmed(ClaimTaskOutcome::Applied(_))
    ));
}

#[tokio::test]
async fn typed_claim_completion_preserves_existing_applied() {
    let fixture = support::writer_fixture().await;
    let task = fixture
        .store
        .create_task(support::new_task(fixture.repository.id, "claim existing"))
        .await
        .expect("create queued task")
        .task()
        .clone();
    let request = claim_request(&task);
    assert!(matches!(
        fixture.store.claim_task(request.clone()).await.unwrap(),
        ClaimTaskOutcome::Applied(_)
    ));
    let identity = mutation_identity(task.id, 1, DurableOperationKind::ClaimTask);

    let completion = fixture
        .writer
        .submit_claim_task(identity, request, support::deadline())
        .expect("bounded ingress accepts the claim replay")
        .completion()
        .await;

    assert!(matches!(
        completion.disposition,
        DurableDisposition::Confirmed(ClaimTaskOutcome::ExistingApplied(_))
    ));
}

#[tokio::test]
async fn typed_claim_completion_preserves_known_not_applied() {
    let fixture = support::writer_fixture().await;
    let task = fixture
        .store
        .create_task(support::new_task(fixture.repository.id, "claim terminal"))
        .await
        .expect("create queued task")
        .task()
        .clone();
    let request = claim_request(&task);
    fixture
        .store
        .transition_with_event(task.id, TaskStatus::Queued, TaskTransition::Cancelled)
        .await
        .expect("terminal state wins before claim");
    let identity = mutation_identity(task.id, 1, DurableOperationKind::ClaimTask);

    let completion = fixture
        .writer
        .submit_claim_task(identity, request, support::deadline())
        .expect("bounded ingress accepts claim reconciliation")
        .completion()
        .await;

    assert!(matches!(
        completion.disposition,
        DurableDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::ExactReconciliation,
            outcome: Some(ClaimTaskOutcome::KnownNotApplied { current }),
            error: None,
        } if current.status == TaskStatus::Cancelled
    ));
}

#[tokio::test]
async fn typed_claim_completion_preserves_invariant_conflict() {
    let fixture = support::writer_fixture().await;
    let task = fixture
        .store
        .create_task(support::new_task(fixture.repository.id, "claim conflict"))
        .await
        .expect("create queued task")
        .task()
        .clone();
    let mut request = claim_request(&task);
    request.expected_attempt += 1;
    let identity = mutation_identity(task.id, 1, DurableOperationKind::ClaimTask);

    let completion = fixture
        .writer
        .submit_claim_task(identity, request, support::deadline())
        .expect("bounded ingress accepts conflicting claim")
        .completion()
        .await;

    assert!(matches!(
        completion.disposition,
        DurableDisposition::InvariantConflict {
            outcome: Some(ClaimTaskOutcome::InvariantConflict),
            ..
        }
    ));
}

#[tokio::test]
async fn explicit_claim_reconciliation_is_read_only_and_preserves_the_full_known_not_applied_outcome()
 {
    let fixture = support::store_fixture().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailBeforeExecute,
            operation: Some(StoreWriterOperationKind::StartTask),
            count: 1,
        }])
        .unwrap(),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store.clone(),
        Arc::new(support::CountingWake::default()),
        8,
        controller,
    );
    let task = fixture
        .store
        .create_task(support::new_task(
            fixture.repository.id,
            "read-only claim reconciliation",
        ))
        .await
        .expect("create queued task")
        .task()
        .clone();
    let request = claim_request(&task);
    let identity = mutation_identity(task.id, 1, DurableOperationKind::ClaimTask);

    let deferred = writer
        .submit_claim_task(identity, request.clone(), support::deadline())
        .expect("reserve the claim sequence")
        .completion()
        .await;
    assert_eq!(
        deferred.sequence_disposition,
        MutationSequenceDisposition::BlockUnknown
    );

    let reconciled = writer
        .submit_reconcile_claim_task(identity, request, support::deadline())
        .expect("reconcile the reserved claim sequence")
        .completion()
        .await;

    assert_eq!(
        reconciled.sequence_disposition,
        MutationSequenceDisposition::AdvanceNext
    );
    assert!(matches!(
        reconciled.disposition,
        DurableDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::ExactReconciliation,
            outcome: Some(ClaimTaskReconciliationOutcome::KnownNotApplied { current }),
            error: None,
        } if current.status == TaskStatus::Queued
    ));
    let stored = fixture
        .store
        .bootstrap_snapshot()
        .await
        .expect("read task after reconciliation")
        .tasks
        .into_iter()
        .find(|candidate| candidate.id == task.id)
        .expect("task remains present");
    assert_eq!(stored.status, TaskStatus::Queued);
}

#[tokio::test]
async fn explicit_claim_reconciliation_preserves_the_full_existing_applied_receipt() {
    let fixture = support::store_fixture().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailBeforeExecute,
            operation: Some(StoreWriterOperationKind::StartTask),
            count: 1,
        }])
        .unwrap(),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store.clone(),
        Arc::new(support::CountingWake::default()),
        8,
        controller,
    );
    let task = fixture
        .store
        .create_task(support::new_task(
            fixture.repository.id,
            "existing read-only claim reconciliation",
        ))
        .await
        .expect("create queued task")
        .task()
        .clone();
    let request = claim_request(&task);
    let identity = mutation_identity(task.id, 1, DurableOperationKind::ClaimTask);

    let deferred = writer
        .submit_claim_task(identity, request.clone(), support::deadline())
        .expect("reserve the claim sequence")
        .completion()
        .await;
    assert_eq!(
        deferred.sequence_disposition,
        MutationSequenceDisposition::BlockUnknown
    );
    let applied = fixture
        .store
        .claim_task(request.clone())
        .await
        .expect("apply the exact claim outside StoreWriter");
    let ClaimTaskOutcome::Applied(expected) = applied else {
        panic!("the direct claim must apply");
    };

    let reconciled = writer
        .submit_reconcile_claim_task(identity, request, support::deadline())
        .expect("reconcile the reserved claim sequence")
        .completion()
        .await;

    assert_eq!(
        reconciled.sequence_disposition,
        MutationSequenceDisposition::AdvanceNext
    );
    assert!(matches!(
        reconciled.disposition,
        DurableDisposition::Confirmed(
            ClaimTaskReconciliationOutcome::ExistingApplied(receipt)
        ) if receipt == expected
    ));
}

#[tokio::test]
async fn reconciliation_rejects_a_task_sequence_that_never_entered_writer_ingress() {
    let fixture = support::writer_fixture().await;
    let task = fixture
        .store
        .create_task(support::new_task(
            fixture.repository.id,
            "unsubmitted reconciliation",
        ))
        .await
        .expect("create queued task")
        .task()
        .clone();
    let identity = mutation_identity(task.id, 1, DurableOperationKind::ClaimTask);

    let result = fixture.writer.reconcile_pending(
        PendingDurableResult::ClaimTask {
            identity,
            request: claim_request(&task),
        },
        support::deadline(),
    );

    assert!(matches!(result, Err(StoreWriterSubmitError::SequenceGap)));
    let stored = fixture
        .store
        .bootstrap_snapshot()
        .await
        .unwrap()
        .tasks
        .into_iter()
        .find(|candidate| candidate.id == task.id)
        .unwrap();
    assert_eq!(stored.status, TaskStatus::Queued);
}

#[tokio::test]
async fn urgent_stop_batch_preserves_atomic_per_task_receipts_and_identity() {
    let fixture = support::writer_fixture().await;
    let mut requests = Vec::new();
    let mut identities = Vec::new();
    for prompt in ["urgent stop b", "urgent stop a"] {
        let task = fixture
            .store
            .create_task(support::new_task(fixture.repository.id, prompt))
            .await
            .expect("create urgent-stop task")
            .task()
            .clone();
        assert!(matches!(
            fixture
                .store
                .claim_task(claim_request(&task))
                .await
                .unwrap(),
            ClaimTaskOutcome::Applied(_)
        ));
        requests.push(StopIntentRequest {
            task_id: task.id,
            expected_repository_id: task.repository_id,
            expected_attempt: task.attempt,
            kind: StopIntentKind::DiskPressureCritical,
        });
        identities.push(mutation_identity(
            task.id,
            1,
            DurableOperationKind::PersistStopIntent,
        ));
    }
    let identity = DurableOperationIdentity::stop_intent_batch(identities).unwrap();

    let completion = fixture
        .writer
        .submit_stop_intent_batch(identity.clone(), requests, support::deadline())
        .expect("urgent ingress accepts the batch")
        .completion()
        .await;

    assert_eq!(completion.identity, identity);
    let DurableDisposition::Confirmed(receipt) = completion.disposition else {
        panic!("urgent batch must return a determinate typed receipt");
    };
    assert_eq!(receipt.items.len(), 2);
    assert!(receipt.items.windows(2).all(|items| {
        items[0].request.task_id.as_uuid().as_u128() < items[1].request.task_id.as_uuid().as_u128()
    }));
    assert!(
        receipt
            .items
            .iter()
            .all(|item| { matches!(item.outcome, PersistStopIntentOutcome::Applied(_)) })
    );
}

#[tokio::test]
async fn commit_before_reply_is_reconciled_to_existing_not_closed_or_unknown() {
    let fixture = support::store_fixture().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            operation: None,
            count: 1,
        }])
        .unwrap(),
    );
    let wake = Arc::new(support::CountingWake::default());
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store.clone(),
        wake.clone(),
        8,
        controller.clone(),
    );

    let completion = writer
        .submit_queue_limited_create(
            support::new_task(fixture.repository.id, "reply loss"),
            NonZeroU32::new(8).unwrap(),
            support::deadline(),
        )
        .expect("bounded ingress accepts the create")
        .completion()
        .await;

    assert!(matches!(
        completion.disposition,
        DurableDisposition::Confirmed(QueueLimitedCreateTaskOutcome::Existing { .. })
    ));
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            StoreWriterOperationKind::CreateTask,
        ),
        1
    );
    assert_eq!(
        fixture
            .store
            .bootstrap_snapshot()
            .await
            .unwrap()
            .tasks
            .len(),
        1
    );
    assert_eq!(wake.count(), 1);
}

#[tokio::test]
async fn deadline_before_typed_execution_is_distinct_from_busy_exhaustion() {
    let fixture = support::writer_fixture().await;

    let completion = fixture
        .writer
        .submit_queue_limited_create(
            support::new_task(fixture.repository.id, "expired typed create"),
            NonZeroU32::new(8).unwrap(),
            Instant::now(),
        )
        .expect("deadline classification is returned by typed completion")
        .completion()
        .await;

    assert!(matches!(
        completion.disposition,
        DurableDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::DeadlineBeforeStart,
            outcome: None,
            error: None,
        }
    ));
    assert!(
        fixture
            .store
            .bootstrap_snapshot()
            .await
            .unwrap()
            .tasks
            .is_empty()
    );
}

#[tokio::test]
async fn typed_ingress_full_and_closed_are_distinct_known_not_applied_results() {
    let fixture = support::store_fixture().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::CreateTask),
            count: 1,
        }])
        .unwrap(),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store.clone(),
        Arc::new(support::CountingWake::default()),
        1,
        controller.clone(),
    );
    let first = writer
        .submit_queue_limited_create(
            support::new_task(fixture.repository.id, "held first create"),
            NonZeroU32::new(8).unwrap(),
            support::deadline(),
        )
        .expect("first create enters the writer");
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let second = writer
        .submit_queue_limited_create(
            support::new_task(fixture.repository.id, "buffered second create"),
            NonZeroU32::new(8).unwrap(),
            support::deadline(),
        )
        .expect("second create fills the bounded ingress");
    let full = writer
        .submit_queue_limited_create(
            support::new_task(fixture.repository.id, "rejected third create"),
            NonZeroU32::new(8).unwrap(),
            support::deadline(),
        )
        .expect("full ingress is represented by an immediate typed completion")
        .completion()
        .await;
    assert_eq!(
        full.sequence_disposition,
        MutationSequenceDisposition::RetainSame
    );
    assert!(matches!(
        full.disposition,
        DurableDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::IngressFull,
            outcome: None,
            error: None,
        }
    ));
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    assert!(matches!(
        first.completion().await.disposition,
        DurableDisposition::Confirmed(QueueLimitedCreateTaskOutcome::Created { .. })
    ));
    assert!(matches!(
        second.completion().await.disposition,
        DurableDisposition::Confirmed(QueueLimitedCreateTaskOutcome::Created { .. })
    ));

    let closed = StoreWriterHandle::closed_for_test()
        .submit_queue_limited_create(
            support::new_task(fixture.repository.id, "closed ingress"),
            NonZeroU32::new(8).unwrap(),
            support::deadline(),
        )
        .expect("closed ingress is represented by an immediate typed completion")
        .completion()
        .await;
    assert_eq!(
        closed.sequence_disposition,
        MutationSequenceDisposition::RetainSame
    );
    assert!(matches!(
        closed.disposition,
        DurableDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::IngressClosed,
            outcome: None,
            error: None,
        }
    ));
}

#[tokio::test]
async fn known_store_rejections_retain_the_existing_api_classification() {
    let fixture = support::writer_fixture().await;
    let client_request_id = ClientRequestId::new();
    fixture
        .store
        .create_task(
            NewTask::try_new(client_request_id, fixture.repository.id, "canonical input").unwrap(),
        )
        .await
        .expect("seed canonical request");

    let completion = fixture
        .writer
        .submit_queue_limited_create(
            NewTask::try_new(
                client_request_id,
                fixture.repository.id,
                "conflicting input",
            )
            .unwrap(),
            NonZeroU32::new(8).unwrap(),
            support::deadline(),
        )
        .expect("typed create enters the writer")
        .completion()
        .await;

    assert_eq!(
        completion.sequence_disposition,
        MutationSequenceDisposition::AdvanceNext
    );
    assert!(matches!(
        completion.disposition,
        DurableDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::KnownRollback,
            outcome: None,
            error: Some(KnownNotAppliedError::IdempotencyConflict),
        }
    ));
}

#[tokio::test]
async fn ambiguous_commit_followed_by_busy_stays_unknown_and_reconciles_same_sequence() {
    let fixture = support::store_fixture().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
                operation: Some(StoreWriterOperationKind::FinalizeReviewedTask),
                count: 1,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
                operation: Some(StoreWriterOperationKind::FinalizeReviewedTask),
                count: 1,
            },
        ])
        .unwrap(),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store.clone(),
        Arc::new(support::CountingWake::default()),
        8,
        controller.clone(),
    );
    let task =
        running_review_task(&writer, fixture.repository.id, "ambiguous quality commit").await;
    let request = FinalizeReviewedTaskRequest {
        task_id: task.id,
        expected_repository_id: task.repository_id,
        expected_attempt: task.attempt,
        evidence: approved(1),
    };
    let identity = mutation_identity(task.id, 1, DurableOperationKind::FinalizeReviewedTask);

    let completion = writer
        .submit_finalize_reviewed_task(identity, request, support::deadline())
        .expect("typed finalization enters the writer");
    tokio::time::timeout(
        Duration::from_secs(3),
        controller.wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1),
    )
    .await
    .expect("the predecessor reaches the post-commit fault");
    let options = fixture
        .store
        .pool()
        .connect_options()
        .as_ref()
        .clone()
        .busy_timeout(Duration::ZERO);
    fixture.store.pool().set_connect_options(options);
    let existing_connections = fixture.store.pool().size();
    let mut legacy_connections = Vec::with_capacity(existing_connections as usize);
    for _ in 0..existing_connections {
        legacy_connections.push(
            fixture
                .store
                .pool()
                .acquire()
                .await
                .expect("reserve connection with the original busy timeout"),
        );
    }
    let transaction = fixture
        .store
        .pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect("hold the SQLite writer lock during inline reconciliation");
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
        1
    );
    let completion = completion.completion().await;
    assert_eq!(
        completion.sequence_disposition,
        MutationSequenceDisposition::BlockUnknown
    );
    let pending = match completion.disposition {
        DurableDisposition::OutcomeUnknown {
            reason: OutcomeUnknownReason::CommitStatusUnknown,
            pending: Some(pending),
        } => pending,
        other => panic!("ambiguous commit must stay unknown, got {other:?}"),
    };
    assert!(matches!(
        &pending,
        PendingDurableResult::FinalizeReviewedTask {
            identity: pending_identity,
            ..
        } if *pending_identity == identity
    ));

    transaction
        .rollback()
        .await
        .expect("release the SQLite writer lock");
    drop(legacy_connections);
    let replay = writer
        .reconcile_pending(pending, support::deadline())
        .expect("the unknown lane accepts the original sequence")
        .completion()
        .await;
    assert_eq!(
        replay.identity,
        DurableOperationIdentity::TaskMutation(identity)
    );
    assert!(matches!(
        replay.disposition,
        DurableDisposition::Confirmed(receipt) if receipt.event_id().is_some()
    ));
}

#[tokio::test]
async fn reconciliation_lane_unblocks_an_unknown_predecessor_behind_a_full_normal_scheduler() {
    let fixture = support::store_fixture().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseBeforeExecute,
                operation: Some(StoreWriterOperationKind::FinishTask),
                count: 1,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailUnknownBeforeExecute,
                operation: Some(StoreWriterOperationKind::FinishTask),
                count: 2,
            },
        ])
        .unwrap(),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store.clone(),
        Arc::new(support::CountingWake::default()),
        1,
        controller.clone(),
    );
    let task = running_review_task(
        &writer,
        fixture.repository.id,
        "reconciliation reserved scheduler slot",
    )
    .await;
    let first_identity =
        mutation_identity(task.id, 1, DurableOperationKind::FinalizeUnreviewedTask);
    let first_request = FinalizeUnreviewedTaskRequest {
        task_id: task.id,
        expected_repository_id: task.repository_id,
        expected_attempt: task.attempt,
        transition: TaskTransition::Failed(TaskFailure {
            code: "TEST_FAILURE".to_owned(),
            message: "exercise reconciliation scheduling".to_owned(),
            retryable: false,
        }),
    };
    let first = writer
        .submit_finalize_unreviewed_task(first_identity, first_request.clone(), support::deadline())
        .expect("the predecessor enters normal ingress");
    tokio::time::timeout(
        Duration::from_secs(3),
        controller.wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1),
    )
    .await
    .expect("the predecessor reaches the execution fault");

    let second_identity =
        mutation_identity(task.id, 2, DurableOperationKind::FinalizeUnreviewedTask);
    let second = writer
        .submit_finalize_unreviewed_task(
            second_identity,
            first_request.clone(),
            support::deadline(),
        )
        .expect("the successor occupies the capacity-one normal ingress");
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    let first = tokio::time::timeout(Duration::from_secs(3), first.completion())
        .await
        .expect("the ambiguous predecessor returns");
    let pending = match first.disposition {
        DurableDisposition::OutcomeUnknown {
            pending: Some(pending),
            ..
        } if first.sequence_disposition == MutationSequenceDisposition::BlockUnknown => pending,
        other => panic!("the predecessor must remain exactly replayable, got {other:?}"),
    };
    assert_eq!(
        pending,
        PendingDurableResult::FinalizeUnreviewedTask {
            identity: first_identity,
            request: first_request,
        }
    );

    let replay = writer
        .reconcile_pending(pending, support::deadline())
        .expect("reconciliation has an independent reserved ingress slot");
    let (replayed, successor) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(replay.completion(), second.completion())
    })
    .await
    .expect("reconciliation advances the predecessor and unblocks the queued successor");
    assert_eq!(
        replayed.sequence_disposition,
        MutationSequenceDisposition::AdvanceNext
    );
    assert!(matches!(
        replayed.disposition,
        DurableDisposition::Confirmed(PendingReplayReceipt::FinalizeUnreviewedTask(
            FinalizeUnreviewedTaskOutcome::Applied { .. }
                | FinalizeUnreviewedTaskOutcome::Existing { .. }
        ))
    ));
    assert_eq!(
        successor.sequence_disposition,
        MutationSequenceDisposition::AdvanceNext
    );
    assert!(matches!(
        successor.disposition,
        DurableDisposition::Confirmed(FinalizeUnreviewedTaskOutcome::Existing { .. })
    ));
}

#[tokio::test]
async fn ambiguous_commit_followed_by_deterministic_reconciliation_error_stays_unknown() {
    let fixture = support::store_fixture().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
                operation: Some(StoreWriterOperationKind::FinalizeReviewedTask),
                count: 1,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
                operation: Some(StoreWriterOperationKind::FinalizeReviewedTask),
                count: 1,
            },
        ])
        .unwrap(),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store.clone(),
        Arc::new(support::CountingWake::default()),
        8,
        controller.clone(),
    );
    let task = running_review_task(
        &writer,
        fixture.repository.id,
        "ambiguous then deterministic reconciliation",
    )
    .await;
    let request = FinalizeReviewedTaskRequest {
        task_id: task.id,
        expected_repository_id: task.repository_id,
        expected_attempt: task.attempt,
        evidence: approved(1),
    };
    let identity = mutation_identity(task.id, 1, DurableOperationKind::FinalizeReviewedTask);
    let completion = writer
        .submit_finalize_reviewed_task(identity, request, support::deadline())
        .expect("typed finalization enters the writer");

    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1)
        .await;
    controller
        .arm_fault(StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailBeforeExecute,
            operation: Some(StoreWriterOperationKind::FinalizeReviewedTask),
            count: 1,
        })
        .expect("arm deterministic reconciliation failure");
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
        1
    );

    let completion = completion.completion().await;
    assert_eq!(
        completion.sequence_disposition,
        MutationSequenceDisposition::BlockUnknown
    );
    let pending = match completion.disposition {
        DurableDisposition::OutcomeUnknown {
            reason: OutcomeUnknownReason::CommitStatusUnknown,
            pending: Some(pending),
        } => pending,
        other => panic!("prior ambiguity must dominate reconciliation errors, got {other:?}"),
    };
    assert!(matches!(
        &pending,
        PendingDurableResult::FinalizeReviewedTask {
            identity: pending_identity,
            ..
        } if *pending_identity == identity
    ));

    let reconciled = writer
        .reconcile_pending(pending, support::deadline())
        .expect("the original unresolved sequence remains admissible")
        .completion()
        .await;
    assert_eq!(
        reconciled.sequence_disposition,
        MutationSequenceDisposition::AdvanceNext
    );
    assert!(matches!(
        reconciled.disposition,
        DurableDisposition::Confirmed(receipt) if receipt.event_id().is_some()
    ));
}

#[tokio::test]
async fn completed_sequence_is_rejected_before_reconciliation_can_enter_the_scheduler() {
    let fixture = support::writer_fixture().await;
    let task = fixture
        .store
        .create_task(support::new_task(
            fixture.repository.id,
            "completed sequence replay",
        ))
        .await
        .expect("create task")
        .task()
        .clone();
    let identity = mutation_identity(task.id, 1, DurableOperationKind::ClaimTask);
    let request = claim_request(&task);
    let pending = PendingDurableResult::ClaimTask {
        identity,
        request: request.clone(),
    };
    let completion = fixture
        .writer
        .submit_claim_task(identity, request, support::deadline())
        .expect("claim enters writer")
        .completion()
        .await;
    assert!(matches!(
        completion.disposition,
        DurableDisposition::Confirmed(ClaimTaskOutcome::Applied(_))
    ));

    assert!(matches!(
        fixture
            .writer
            .reconcile_pending(pending, support::deadline()),
        Err(StoreWriterSubmitError::SequenceReversed)
    ));
}

#[tokio::test]
async fn completed_unreviewed_terminal_sequence_rejects_duplicate_submission_and_replay() {
    let fixture = support::writer_fixture().await;
    let queued = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "typed unreviewed sequence"),
            support::deadline(),
        )
        .await
        .expect("create unreviewed task")
        .value
        .task()
        .clone();
    let running = match fixture
        .writer
        .start_task(queued.id, support::deadline())
        .await
        .expect("start unreviewed task")
        .value
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("fixture start must apply"),
    };
    let identity = mutation_identity(running.id, 1, DurableOperationKind::FinalizeUnreviewedTask);
    let request = FinalizeUnreviewedTaskRequest {
        task_id: running.id,
        expected_repository_id: running.repository_id,
        expected_attempt: running.attempt,
        transition: TaskTransition::Failed(support::failure("TYPED_UNREVIEWED_SEQUENCE")),
    };
    let pending = PendingDurableResult::FinalizeUnreviewedTask {
        identity,
        request: request.clone(),
    };
    let completion = fixture
        .writer
        .submit_finalize_unreviewed_task(identity, request.clone(), support::deadline())
        .expect("submit exact unreviewed terminal")
        .completion()
        .await;
    assert_eq!(
        completion.sequence_disposition,
        MutationSequenceDisposition::AdvanceNext
    );
    assert!(matches!(
        completion.disposition,
        DurableDisposition::Confirmed(FinalizeUnreviewedTaskOutcome::Applied { .. })
    ));

    assert!(matches!(
        fixture
            .writer
            .submit_finalize_unreviewed_task(identity, request, support::deadline()),
        Err(StoreWriterSubmitError::SequenceReversed)
    ));
    assert!(matches!(
        fixture
            .writer
            .reconcile_pending(pending, support::deadline()),
        Err(StoreWriterSubmitError::SequenceReversed)
    ));
}

#[tokio::test]
async fn dropped_submission_still_marks_the_writer_completed_sequence() {
    let fixture = support::writer_fixture().await;
    let task = fixture
        .store
        .create_task(support::new_task(
            fixture.repository.id,
            "dropped submission ledger",
        ))
        .await
        .expect("create task")
        .task()
        .clone();
    let identity = mutation_identity(task.id, 1, DurableOperationKind::ClaimTask);
    let request = claim_request(&task);
    let pending = PendingDurableResult::ClaimTask {
        identity,
        request: request.clone(),
    };
    let submission = fixture
        .writer
        .submit_claim_task(identity, request, support::deadline())
        .expect("claim enters writer");
    drop(submission);

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let status = fixture
                .store
                .bootstrap_snapshot()
                .await
                .expect("read task after dropped receiver")
                .tasks
                .into_iter()
                .find(|candidate| candidate.id == task.id)
                .expect("task remains present")
                .status;
            if status == TaskStatus::Running {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("writer commits despite dropped receiver");

    assert!(matches!(
        fixture
            .writer
            .reconcile_pending(pending, support::deadline()),
        Err(StoreWriterSubmitError::SequenceReversed)
    ));
}

#[tokio::test]
async fn full_claim_reconciliation_is_read_only_before_a_new_sequence_claims() {
    let fixture = support::store_fixture().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::CreateTask),
            count: 1,
        }])
        .unwrap(),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store.clone(),
        Arc::new(support::CountingWake::default()),
        1,
        controller.clone(),
    );
    let task = fixture
        .store
        .create_task(support::new_task(
            fixture.repository.id,
            "full task mutation reconciliation",
        ))
        .await
        .expect("create task")
        .task()
        .clone();
    let first = writer
        .submit_queue_limited_create(
            support::new_task(fixture.repository.id, "paused create"),
            NonZeroU32::new(8).unwrap(),
            support::deadline(),
        )
        .expect("first create enters writer");
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let second = writer
        .submit_queue_limited_create(
            support::new_task(fixture.repository.id, "buffered create"),
            NonZeroU32::new(8).unwrap(),
            support::deadline(),
        )
        .expect("second create fills ingress");
    let identity = mutation_identity(task.id, 1, DurableOperationKind::ClaimTask);
    let full = writer
        .submit_claim_task(identity, claim_request(&task), support::deadline())
        .expect("full ingress is a typed completion")
        .completion()
        .await;
    let pending = match full.disposition {
        DurableDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::IngressFull,
            ..
        } => PendingDurableResult::ClaimTask {
            identity,
            request: claim_request(&task),
        },
        other => panic!("expected ingress-full pending mutation, got {other:?}"),
    };
    assert!(matches!(
        writer.submit_claim_task(
            mutation_identity(task.id, 2, DurableOperationKind::ClaimTask),
            claim_request(&task),
            support::deadline(),
        ),
        Err(StoreWriterSubmitError::SequenceGap)
    ));

    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );
    let _ = first.completion().await;
    let _ = second.completion().await;
    let reconciled = writer
        .reconcile_pending(pending, support::deadline())
        .expect("the reserved full sequence remains reconcilable")
        .completion()
        .await;
    assert_eq!(
        reconciled.sequence_disposition,
        MutationSequenceDisposition::AdvanceNext
    );
    assert!(matches!(
        reconciled.disposition,
        DurableDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::ExactReconciliation,
            outcome: Some(coding_agent_app::PendingReplayReceipt::ClaimTask(
                ClaimTaskOutcome::KnownNotApplied { current }
            )),
            error: None,
        } if current.status == TaskStatus::Queued
    ));
    let started_before_new_scan: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_events WHERE task_id = ? AND kind = 'task.started'",
    )
    .bind(task.id.to_string())
    .fetch_one(fixture.store.pool())
    .await
    .expect("count task.started events after read-only reconciliation");
    assert_eq!(started_before_new_scan, 0);
    assert_eq!(
        fixture
            .store
            .task_detail(task.id)
            .await
            .expect("load task after read-only reconciliation")
            .expect("task remains present")
            .task
            .status,
        TaskStatus::Queued
    );

    let new_scan_claim = writer
        .submit_claim_task(
            mutation_identity(task.id, 2, DurableOperationKind::ClaimTask),
            claim_request(&task),
            support::deadline(),
        )
        .expect("a later scan submits a new claim sequence")
        .completion()
        .await;
    assert!(matches!(
        new_scan_claim.disposition,
        DurableDisposition::Confirmed(ClaimTaskOutcome::Applied(_))
    ));
    let started_after_new_scan: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_events WHERE task_id = ? AND kind = 'task.started'",
    )
    .bind(task.id.to_string())
    .fetch_one(fixture.store.pool())
    .await
    .expect("count task.started events after the new claim");
    assert_eq!(started_after_new_scan, 1);
}

#[tokio::test]
async fn closed_task_mutation_ingress_retains_the_same_sequence_for_reconciliation() {
    let fixture = support::store_fixture().await;
    let task = fixture
        .store
        .create_task(support::new_task(
            fixture.repository.id,
            "closed task mutation reconciliation",
        ))
        .await
        .expect("create task")
        .task()
        .clone();
    let writer = StoreWriterHandle::closed_for_test();
    let identity = mutation_identity(task.id, 1, DurableOperationKind::ClaimTask);
    let first = writer
        .submit_claim_task(identity, claim_request(&task), support::deadline())
        .expect("closed ingress is a typed completion")
        .completion()
        .await;
    let pending = match first.disposition {
        DurableDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::IngressClosed,
            ..
        } => PendingDurableResult::ClaimTask {
            identity,
            request: claim_request(&task),
        },
        other => panic!("expected ingress-closed pending mutation, got {other:?}"),
    };

    let second = writer
        .reconcile_pending(pending.clone(), support::deadline())
        .expect("closed reconciliation retains the original sequence")
        .completion()
        .await;
    assert!(matches!(
        second.disposition,
        DurableDisposition::OutcomeUnknown {
            reason: OutcomeUnknownReason::ReconciliationFailed,
            pending: Some(returned),
        } if returned == pending
    ));
}

#[tokio::test]
async fn repeated_reply_loss_stays_unknown_until_the_original_pending_is_reconciled() {
    let fixture = support::store_fixture().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            operation: Some(StoreWriterOperationKind::CreateTask),
            count: 4,
        }])
        .unwrap(),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store.clone(),
        Arc::new(support::CountingWake::default()),
        8,
        controller,
    );

    let completion = writer
        .submit_queue_limited_create(
            support::new_task(fixture.repository.id, "two consecutive reply losses"),
            NonZeroU32::new(8).unwrap(),
            support::deadline(),
        )
        .expect("typed create enters the writer")
        .completion()
        .await;
    assert_eq!(
        completion.sequence_disposition,
        MutationSequenceDisposition::BlockUnknown
    );
    let pending = match completion.disposition {
        DurableDisposition::OutcomeUnknown {
            reason: OutcomeUnknownReason::CommitStatusUnknown,
            pending: Some(pending),
        } => pending,
        other => panic!("two reply losses must remain unknown, got {other:?}"),
    };

    let second = writer
        .reconcile_pending(pending, support::deadline())
        .expect("the original pending remains admissible")
        .completion()
        .await;
    assert_eq!(
        second.sequence_disposition,
        MutationSequenceDisposition::BlockUnknown
    );
    let pending = match second.disposition {
        DurableDisposition::OutcomeUnknown {
            reason: OutcomeUnknownReason::ReconciliationFailed,
            pending: Some(pending),
        } => pending,
        other => panic!("a second pair of reply losses must remain unknown, got {other:?}"),
    };

    let reconciled = writer
        .reconcile_pending(pending, support::deadline())
        .expect("a third attempt may still use the original sequence")
        .completion()
        .await;
    assert_eq!(
        reconciled.sequence_disposition,
        MutationSequenceDisposition::AdvanceNext
    );
    assert!(matches!(
        reconciled.disposition,
        DurableDisposition::Confirmed(receipt) if receipt.event_id().is_some()
    ));
    assert_eq!(
        fixture
            .store
            .bootstrap_snapshot()
            .await
            .unwrap()
            .tasks
            .len(),
        1
    );
}

#[tokio::test]
async fn typed_record_review_completion_preserves_operation_identity() {
    let fixture = support::writer_fixture().await;
    let task = running_review_task(
        &fixture.writer,
        fixture.repository.id,
        "typed record review",
    )
    .await;
    let identity = mutation_identity(task.id, 1, DurableOperationKind::RecordReview);
    let completion = fixture
        .writer
        .submit_record_review(
            identity,
            RecordReviewRequest {
                task_id: task.id,
                expected_repository_id: task.repository_id,
                expected_attempt: task.attempt,
                evidence: changes_requested(1),
            },
            support::deadline(),
        )
        .expect("typed review enters the writer")
        .completion()
        .await;

    assert_eq!(
        completion.identity,
        DurableOperationIdentity::TaskMutation(identity)
    );
    assert!(matches!(
        completion.disposition,
        DurableDisposition::Confirmed(RecordReviewOutcome::Applied { .. })
    ));
}

#[tokio::test]
async fn real_writer_finalization_in_flight_cannot_be_overtaken_by_same_task_urgent_stop() {
    let fixture = support::store_fixture().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseBeforeExecute,
            operation: Some(StoreWriterOperationKind::FinalizeReviewedTask),
            count: 1,
        }])
        .unwrap(),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store.clone(),
        Arc::new(support::CountingWake::default()),
        8,
        controller.clone(),
    );
    let task = running_review_task(&writer, fixture.repository.id, "quality before urgent").await;
    let finalization_identity =
        mutation_identity(task.id, 1, DurableOperationKind::FinalizeReviewedTask);
    let finalization = writer
        .submit_finalize_reviewed_task(
            finalization_identity,
            FinalizeReviewedTaskRequest {
                task_id: task.id,
                expected_repository_id: task.repository_id,
                expected_attempt: task.attempt,
                evidence: approved(1),
            },
            support::deadline(),
        )
        .expect("typed finalization enters normal ingress");
    controller
        .wait_until_reached(StoreWriterFaultPoint::PauseBeforeExecute, 1)
        .await;
    let stop_identity = DurableOperationIdentity::stop_intent_batch(vec![mutation_identity(
        task.id,
        2,
        DurableOperationKind::PersistStopIntent,
    )])
    .unwrap();
    let stop = writer
        .submit_stop_intent_batch(
            stop_identity,
            vec![StopIntentRequest {
                task_id: task.id,
                expected_repository_id: task.repository_id,
                expected_attempt: task.attempt,
                kind: StopIntentKind::UserCancelled,
            }],
            support::deadline(),
        )
        .expect("same-task urgent stop is accepted behind finalization");
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseBeforeExecute),
        1
    );

    assert!(matches!(
        finalization.completion().await.disposition,
        DurableDisposition::Confirmed(FinalizeReviewedTaskOutcome::Applied { .. })
    ));
    let DurableDisposition::Confirmed(stop_receipt) = stop.completion().await.disposition else {
        panic!("stop completion must remain typed");
    };
    assert!(matches!(
        stop_receipt.items.as_slice(),
        [item] if matches!(item.outcome, PersistStopIntentOutcome::TerminalWon { .. })
    ));
}

#[tokio::test]
async fn pending_stop_batch_replay_preserves_every_per_item_winner() {
    let fixture = support::store_fixture().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            operation: Some(StoreWriterOperationKind::PersistStopIntentBatch),
            count: 2,
        }])
        .unwrap(),
    );
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store.clone(),
        Arc::new(support::CountingWake::default()),
        8,
        controller,
    );

    let applied = fixture
        .store
        .create_task(support::new_task(
            fixture.repository.id,
            "stop replay applied then existing",
        ))
        .await
        .expect("create applied task")
        .task()
        .clone();
    fixture
        .store
        .claim_task(claim_request(&applied))
        .await
        .expect("claim applied task");

    let terminal = fixture
        .store
        .create_task(support::new_task(
            fixture.repository.id,
            "stop replay terminal winner",
        ))
        .await
        .expect("create terminal task")
        .task()
        .clone();
    fixture
        .store
        .transition_with_event(terminal.id, TaskStatus::Queued, TaskTransition::Cancelled)
        .await
        .expect("make terminal task");

    let conflict = fixture
        .store
        .create_task(support::new_task(
            fixture.repository.id,
            "stop replay intent conflict",
        ))
        .await
        .expect("create conflict task")
        .task()
        .clone();
    fixture
        .store
        .claim_task(claim_request(&conflict))
        .await
        .expect("claim conflict task");
    fixture
        .store
        .persist_stop_intent(StopIntentRequest {
            task_id: conflict.id,
            expected_repository_id: conflict.repository_id,
            expected_attempt: conflict.attempt,
            kind: StopIntentKind::DiskPressureCritical,
        })
        .await
        .expect("seed conflicting stop intent");

    let tasks = [&applied, &terminal, &conflict];
    let requests = tasks
        .iter()
        .map(|task| StopIntentRequest {
            task_id: task.id,
            expected_repository_id: task.repository_id,
            expected_attempt: task.attempt,
            kind: StopIntentKind::UserCancelled,
        })
        .collect::<Vec<_>>();
    let identity = DurableOperationIdentity::stop_intent_batch(
        tasks
            .iter()
            .map(|task| mutation_identity(task.id, 1, DurableOperationKind::PersistStopIntent))
            .collect(),
    )
    .unwrap();
    let unknown = writer
        .submit_stop_intent_batch(identity, requests, support::deadline())
        .expect("stop batch enters writer")
        .completion()
        .await;
    let pending = match unknown.disposition {
        DurableDisposition::OutcomeUnknown {
            pending: Some(pending),
            ..
        } => pending,
        other => panic!("two reply losses must retain the exact stop batch, got {other:?}"),
    };

    let replay = writer
        .reconcile_pending(pending, support::deadline())
        .expect("same stop-batch identities remain admissible")
        .completion()
        .await;
    let DurableDisposition::Confirmed(
        coding_agent_app::PendingReplayReceipt::PersistStopIntentBatch(receipt),
    ) = replay.disposition
    else {
        panic!("replay must preserve the typed stop-batch receipt");
    };
    assert!(receipt.items.iter().any(|item| {
        item.request.task_id == applied.id
            && matches!(item.outcome, PersistStopIntentOutcome::Existing(_))
    }));
    assert!(receipt.items.iter().any(|item| {
        item.request.task_id == terminal.id
            && matches!(item.outcome, PersistStopIntentOutcome::TerminalWon { .. })
    }));
    assert!(receipt.items.iter().any(|item| {
        item.request.task_id == conflict.id
            && matches!(
                item.outcome,
                PersistStopIntentOutcome::IntentConflict { .. }
            )
    }));
}

#[tokio::test]
async fn real_writer_typed_final_stop_preserves_identity_and_terminal_receipt() {
    let fixture = support::writer_fixture().await;
    let task = fixture
        .store
        .create_task(support::new_task(fixture.repository.id, "typed final stop"))
        .await
        .expect("create task")
        .task()
        .clone();
    fixture
        .store
        .claim_task(claim_request(&task))
        .await
        .expect("claim task");
    let stop_identity = DurableOperationIdentity::stop_intent_batch(vec![mutation_identity(
        task.id,
        1,
        DurableOperationKind::PersistStopIntent,
    )])
    .unwrap();
    fixture
        .writer
        .submit_stop_intent_batch(
            stop_identity,
            vec![StopIntentRequest {
                task_id: task.id,
                expected_repository_id: task.repository_id,
                expected_attempt: task.attempt,
                kind: StopIntentKind::UserCancelled,
            }],
            support::deadline(),
        )
        .expect("submit stop intent")
        .completion()
        .await;
    let identity = mutation_identity(task.id, 2, DurableOperationKind::FinalizeStoppedTask);
    let completion = fixture
        .writer
        .submit_finalize_stopped_task(
            identity,
            FinalizeStoppedTaskRequest {
                task_id: task.id,
                expected_repository_id: task.repository_id,
                expected_attempt: task.attempt,
                expected_intent: StopIntentKind::UserCancelled,
            },
            support::deadline(),
        )
        .expect("submit typed final stop")
        .completion()
        .await;

    assert_eq!(
        completion.identity,
        DurableOperationIdentity::TaskMutation(identity)
    );
    assert!(matches!(
        completion.disposition,
        DurableDisposition::Confirmed(FinalizeStoppedTaskOutcome::Applied(_))
    ));
}

#[test]
fn normal_ingress_is_bounded() {
    let mut scheduler = StoreWriterSchedulingHarness::new(1, 1);
    assert_eq!(
        scheduler.try_enqueue_normal(mutation_identity(
            TaskId::new(),
            1,
            DurableOperationKind::ClaimTask,
        )),
        Ok(())
    );
    assert_eq!(
        scheduler.try_enqueue_normal(mutation_identity(
            TaskId::new(),
            1,
            DurableOperationKind::ClaimTask,
        )),
        Err(StoreWriterSchedulingError::NormalIngressFull)
    );
}

#[test]
fn urgent_ingress_is_bounded_independently_from_normal() {
    let mut scheduler = StoreWriterSchedulingHarness::new(1, 1);
    scheduler
        .try_enqueue_normal(mutation_identity(
            TaskId::new(),
            1,
            DurableOperationKind::ClaimTask,
        ))
        .unwrap();
    scheduler
        .try_enqueue_urgent(urgent_identity(TaskId::new(), 1))
        .expect("urgent ingress remains available while normal is full");
    assert_eq!(
        scheduler.try_enqueue_urgent(urgent_identity(TaskId::new(), 1)),
        Err(StoreWriterSchedulingError::UrgentIngressFull)
    );
}

#[test]
fn task_sequence_gap_and_reversal_fail_closed() {
    let task_id = TaskId::new();
    let mut gap = StoreWriterSchedulingHarness::new(4, 4);
    assert_eq!(
        gap.try_enqueue_normal(mutation_identity(
            task_id,
            2,
            DurableOperationKind::ClaimTask,
        )),
        Err(StoreWriterSchedulingError::SequenceGap)
    );

    let mut reversal = StoreWriterSchedulingHarness::new(4, 4);
    reversal
        .try_enqueue_normal(mutation_identity(
            task_id,
            1,
            DurableOperationKind::ClaimTask,
        ))
        .unwrap();
    assert_eq!(
        reversal.try_enqueue_urgent(urgent_identity(task_id, 1)),
        Err(StoreWriterSchedulingError::SequenceReversed)
    );
}

#[test]
fn urgent_cannot_pass_an_earlier_sequence_for_the_same_task() {
    let task_id = TaskId::new();
    let mut scheduler = StoreWriterSchedulingHarness::new(4, 4);
    scheduler
        .try_enqueue_normal(mutation_identity(
            task_id,
            1,
            DurableOperationKind::FinalizeReviewedTask,
        ))
        .unwrap();
    scheduler
        .try_enqueue_urgent(urgent_identity(task_id, 2))
        .unwrap();

    assert_eq!(scheduler.pop_next(), Some(StoreWriterPriority::Normal));
}

#[test]
fn urgent_overtakes_unstarted_normal_for_another_task() {
    let mut scheduler = StoreWriterSchedulingHarness::new(4, 4);
    scheduler
        .try_enqueue_normal(mutation_identity(
            TaskId::new(),
            1,
            DurableOperationKind::RecordReview,
        ))
        .unwrap();
    scheduler
        .try_enqueue_urgent(urgent_identity(TaskId::new(), 1))
        .unwrap();

    assert_eq!(scheduler.pop_next(), Some(StoreWriterPriority::Urgent));
}

#[test]
fn sustained_urgent_work_has_a_bounded_four_transaction_burst() {
    let mut scheduler = StoreWriterSchedulingHarness::new(8, 8);
    scheduler
        .try_enqueue_normal(mutation_identity(
            TaskId::new(),
            1,
            DurableOperationKind::RecordReview,
        ))
        .unwrap();
    for _ in 0..5 {
        scheduler
            .try_enqueue_urgent(urgent_identity(TaskId::new(), 1))
            .unwrap();
    }

    for _ in 0..4 {
        assert_eq!(scheduler.pop_next(), Some(StoreWriterPriority::Urgent));
    }
    assert_eq!(scheduler.pop_next(), Some(StoreWriterPriority::Normal));
}

fn claim_request(task: &Task) -> ClaimTaskRequest {
    ClaimTaskRequest {
        task_id: task.id,
        expected_repository_id: task.repository_id,
        expected_attempt: task.attempt,
        expected_queued_event_id: task.last_event_id,
    }
}

fn mutation_identity(
    task_id: TaskId,
    sequence: u64,
    kind: DurableOperationKind,
) -> TaskMutationIdentity {
    TaskMutationIdentity {
        task_id,
        sequence: MutationSequence::new(NonZeroU64::new(sequence).unwrap()),
        kind,
    }
}

fn urgent_identity(task_id: TaskId, sequence: u64) -> DurableOperationIdentity {
    DurableOperationIdentity::stop_intent_batch(vec![mutation_identity(
        task_id,
        sequence,
        DurableOperationKind::PersistStopIntent,
    )])
    .unwrap()
}

#[tokio::test]
async fn service_state_generation_never_moves_backwards() {
    let state = ServiceStateController::new(ServiceState::Ready);
    let a = state.set(ServiceState::StoreDegraded).unwrap();
    let b = state.set(ServiceState::Ready).unwrap();
    assert_eq!(a.generation + 1, b.generation);
    assert_eq!(state.current(), b);
}

#[tokio::test]
async fn service_state_same_value_is_unchanged_and_quiescing_is_terminal() {
    let state = ServiceStateController::new(ServiceState::Ready);
    let mut changes = state.subscribe();
    let initial = state.current();
    assert_eq!(state.set(ServiceState::Ready).unwrap(), initial);
    assert!(!changes.has_changed().unwrap());

    let degraded = state.set(ServiceState::StoreDegraded).unwrap();
    assert_eq!(changes.changed().await.unwrap(), ());
    assert_eq!(*changes.borrow_and_update(), degraded);
    let quiescing = state.set(ServiceState::Quiescing).unwrap();
    assert_eq!(quiescing.generation, degraded.generation + 1);
    assert!(state.set(ServiceState::Ready).is_err());
    assert_eq!(state.current(), quiescing);
}
