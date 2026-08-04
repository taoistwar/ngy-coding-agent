#[cfg(feature = "test-support")]
fn test_stop_identity(task_id: TaskId, sequence: u64) -> TaskMutationIdentity {
    TaskMutationIdentity {
        task_id,
        sequence: MutationSequence::new(
            NonZeroU64::new(sequence).expect("test stop sequence is nonzero"),
        ),
        kind: DurableOperationKind::PersistStopIntent,
    }
}

#[cfg(feature = "test-support")]
fn test_stop_batch_identity(mut task_ids: Vec<TaskId>, sequence: u64) -> DurableOperationIdentity {
    task_ids.sort_by_key(|task_id| task_id.as_uuid().as_u128());
    DurableOperationIdentity::stop_intent_batch(
        task_ids
            .into_iter()
            .map(|task_id| test_stop_identity(task_id, sequence))
            .collect(),
    )
    .expect("construct test stop batch identity")
}

#[cfg(feature = "test-support")]
fn test_record_review_predecessor(
    task_id: TaskId,
    repository_id: RepositoryId,
    attempt: u32,
) -> PendingDurableResult {
    PendingDurableResult::RecordReview {
        identity: TaskMutationIdentity {
            task_id,
            sequence: MutationSequence::new(
                NonZeroU64::new(1).expect("one is a nonzero mutation sequence"),
            ),
            kind: DurableOperationKind::RecordReview,
        },
        request: RecordReviewRequest {
            task_id,
            expected_repository_id: repository_id,
            expected_attempt: attempt,
            evidence: staged_review_evidence(),
        },
    }
}

#[cfg(feature = "test-support")]
fn empty_confirmed_stop_completion(
    identity: DurableOperationIdentity,
) -> DurableCompletion<StopIntentBatchReceipt> {
    DurableCompletion {
        identity,
        sequence_disposition: MutationSequenceDisposition::AdvanceNext,
        disposition: DurableDisposition::Confirmed(StopIntentBatchReceipt { items: Vec::new() }),
    }
}

#[cfg(feature = "test-support")]
async fn staged_stop_completion_for_test(
    store: &Store,
    manager: &TaskManagerHandle,
    task: &Task,
) -> (StagedStopCompletionForTest, PendingDurableResult) {
    let snapshot = manager
        .active_stop_snapshot_for_test(task.id)
        .await
        .expect("inspect staged stop fixture task")
        .expect("staged stop fixture task remains active");
    let predecessor_sequence = snapshot.next_mutation_sequence;
    let stop_sequence = predecessor_sequence
        .checked_add(1)
        .expect("staged stop sequence remains in range");
    let predecessor = PendingDurableResult::RecordReview {
        identity: TaskMutationIdentity {
            task_id: task.id,
            sequence: MutationSequence::new(
                NonZeroU64::new(predecessor_sequence)
                    .expect("staged predecessor sequence is nonzero"),
            ),
            kind: DurableOperationKind::RecordReview,
        },
        request: RecordReviewRequest {
            task_id: task.id,
            expected_repository_id: task.repository_id,
            expected_attempt: task.attempt,
            evidence: staged_review_evidence(),
        },
    };
    let identity = test_stop_identity(task.id, stop_sequence);
    let request = StopIntentRequest {
        task_id: task.id,
        expected_repository_id: task.repository_id,
        expected_attempt: task.attempt,
        kind: StopIntentKind::UserCancelled,
    };
    let receipt = store
        .persist_stop_intent_batch(vec![request])
        .await
        .expect("persist staged exact stop fixture receipt");
    let batch_identity = DurableOperationIdentity::stop_intent_batch(vec![identity])
        .expect("one staged exact stop identity is a valid batch");
    (
        StagedStopCompletionForTest {
            identity,
            request,
            predecessor: predecessor.clone(),
            completion: DurableCompletion {
                identity: batch_identity,
                sequence_disposition: MutationSequenceDisposition::AdvanceNext,
                disposition: DurableDisposition::Confirmed(receipt),
            },
        },
        predecessor,
    )
}

#[cfg(feature = "test-support")]
async fn persist_recovered_terminal(store: &Store, running: &Task) -> Task {
    let recovery = store
        .interrupt_remaining_after_stops(shutdown_failure())
        .await
        .expect("persist a recovered terminal task");
    assert_eq!(recovery.interrupted_count, 1);
    let terminal = store
        .task_detail(running.id)
        .await
        .expect("load the recovered terminal task")
        .expect("the recovered terminal task exists")
        .task;
    assert_eq!(terminal.repository_id, running.repository_id);
    assert_eq!(terminal.attempt, running.attempt);
    assert_eq!(terminal.status, TaskStatus::Interrupted);
    terminal
}
