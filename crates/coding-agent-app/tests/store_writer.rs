mod support;

use std::sync::Arc;

use coding_agent_app::{
    FinalizeReviewedTaskRequest, RecordReviewRequest, ServiceState, ServiceStateController,
    StoreWriterError, StoreWriterFaultPoint, StoreWriterFaultSpec, StoreWriterHandle,
    StoreWriterOperationKind, StoreWriterTestController,
};
use coding_agent_domain::{
    CanonicalPath, CheckActor, CheckEvidence, CheckEvidenceStatus, DeliveryReadiness,
    FindingSeverity, NewReviewEvidence, PlanItem, PlanItemStatus, PlanSnapshot, RepositoryId,
    RequiredCheck, ReviewCoverageEvidence, ReviewDecisionSource, ReviewFinding, ReviewVerdict,
    Task, TaskEventPayload, TaskStatus, WorkspaceDigest,
};
use coding_agent_store::{
    AppendEventOutcome, AttemptArtifactIdentity, AttemptArtifactState, FinalizeReviewedTaskOutcome,
    RecordReviewOutcome, RegisterRepositoryOutcome, ReserveAttemptArtifact,
    ReserveAttemptArtifactOutcome, TransitionOutcome, UpdateAttemptArtifactOutcome,
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
async fn bulk_recovery_preserves_outcome_watermark_and_wakes_once() {
    let fixture = support::writer_fixture().await;
    for prompt in ["recover a", "recover b"] {
        fixture
            .writer
            .create_task(
                support::new_task(fixture.repository.id, prompt),
                support::deadline(),
            )
            .await
            .unwrap();
    }
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
    assert_eq!(receipt.event_id, receipt.value.last_event_id);
    assert_eq!(
        receipt.value.high_watermark.get(),
        receipt.value.last_event_id.unwrap().get()
    );
    assert_eq!(fixture.wake.count(), before + 1);
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
