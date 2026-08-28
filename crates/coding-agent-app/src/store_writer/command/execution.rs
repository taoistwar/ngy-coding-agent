use super::*;

pub(super) fn send_typed_completion<T>(
    response: oneshot::Sender<DurableCompletion<T>>,
    identity: DurableOperationIdentity,
    disposition: DurableDisposition<T>,
    sequence_guard: Option<&mut MutationSequenceGuard>,
) -> bool {
    let sequence_disposition = sequence_disposition(&disposition);
    if let Some(sequence_guard) = sequence_guard {
        sequence_guard.resolve(sequence_disposition);
    }
    let advances = sequence_disposition == MutationSequenceDisposition::AdvanceNext;
    let _ = response.send(DurableCompletion {
        identity,
        sequence_disposition,
        disposition,
    });
    advances
}

pub(super) fn disposition_advances_sequence<T>(disposition: &DurableDisposition<T>) -> bool {
    sequence_disposition(disposition) == MutationSequenceDisposition::AdvanceNext
}

pub(super) fn sequence_disposition<T>(
    disposition: &DurableDisposition<T>,
) -> MutationSequenceDisposition {
    match disposition {
        DurableDisposition::Confirmed(_) => MutationSequenceDisposition::AdvanceNext,
        DurableDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::IngressClosed | KnownNotAppliedReason::IngressFull,
            ..
        } => MutationSequenceDisposition::RetainSame,
        DurableDisposition::KnownNotApplied { .. } => MutationSequenceDisposition::AdvanceNext,
        DurableDisposition::OutcomeUnknown { .. }
        | DurableDisposition::InvariantConflict { .. } => MutationSequenceDisposition::BlockUnknown,
    }
}

pub(super) async fn execute(
    backend: &dyn StoreWriterBackend,
    operation: StoreWriterOperation,
    deadline: Instant,
) -> Result<StoreWriterOperationOutcome, StoreWriterError> {
    if Instant::now() >= deadline {
        return Err(StoreWriterError::Busy);
    }

    let mut retry = 0;
    loop {
        match backend.execute(operation.clone()).await {
            Err(StoreWriterError::Busy) if retry < RETRY_DELAYS.len() => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(StoreWriterError::Busy);
                }
                let retry_at = now + RETRY_DELAYS[retry];
                retry += 1;
                if retry_at >= deadline {
                    sleep_until(deadline).await;
                    return Err(StoreWriterError::Busy);
                }
                sleep_until(retry_at).await;
                if Instant::now() >= deadline {
                    return Err(StoreWriterError::Busy);
                }
            }
            result => return result,
        }
    }
}

#[derive(Debug)]
pub(super) enum TypedExecutionError {
    DeadlineBeforeStart,
    Known(StoreWriterError),
    OutcomeUnknown(OutcomeUnknownReason),
}

pub(super) struct TypedExecution {
    pub(super) result: Result<StoreWriterOperationOutcome, TypedExecutionError>,
    pub(super) reconciled: bool,
}

pub(super) async fn execute_with_query_first_replay(
    backend: &dyn StoreWriterBackend,
    operation: StoreWriterOperation,
    deadline: Instant,
    reconciliation_lane: bool,
) -> TypedExecution {
    execute_typed(
        backend,
        operation.clone(),
        Some(operation),
        deadline,
        reconciliation_lane,
    )
    .await
}

pub(super) async fn execute_typed(
    backend: &dyn StoreWriterBackend,
    operation: StoreWriterOperation,
    reconciliation: Option<StoreWriterOperation>,
    deadline: Instant,
    reconciliation_lane: bool,
) -> TypedExecution {
    if Instant::now() >= deadline {
        return TypedExecution {
            result: if reconciliation_lane {
                Err(TypedExecutionError::OutcomeUnknown(
                    OutcomeUnknownReason::ReconciliationFailed,
                ))
            } else {
                Err(TypedExecutionError::DeadlineBeforeStart)
            },
            reconciled: false,
        };
    }
    let first = execute(backend, operation.clone(), deadline).await;
    if first.as_ref().is_err_and(outcome_may_be_unknown) {
        let Some(reconciliation) = reconciliation else {
            return TypedExecution {
                result: Err(TypedExecutionError::OutcomeUnknown(
                    OutcomeUnknownReason::NonReplayableOperation,
                )),
                reconciled: false,
            };
        };
        return match execute(backend, reconciliation, deadline).await {
            Ok(outcome) => TypedExecution {
                result: Ok(outcome),
                reconciled: true,
            },
            Err(error) => TypedExecution {
                result: Err(ambiguity_reconciliation_failure(error, reconciliation_lane)),
                reconciled: true,
            },
        };
    }
    TypedExecution {
        result: first.map_err(|error| {
            if reconciliation_lane {
                reconciliation_failure(error, true)
            } else {
                TypedExecutionError::Known(error)
            }
        }),
        reconciled: false,
    }
}

fn ambiguity_reconciliation_failure(
    _error: StoreWriterError,
    reconciliation_lane: bool,
) -> TypedExecutionError {
    TypedExecutionError::OutcomeUnknown(if reconciliation_lane {
        OutcomeUnknownReason::ReconciliationFailed
    } else {
        OutcomeUnknownReason::CommitStatusUnknown
    })
}

fn reconciliation_failure(
    error: StoreWriterError,
    reconciliation_lane: bool,
) -> TypedExecutionError {
    if matches!(error, StoreWriterError::Busy) || outcome_may_be_unknown(&error) {
        TypedExecutionError::OutcomeUnknown(if reconciliation_lane {
            OutcomeUnknownReason::ReconciliationFailed
        } else {
            OutcomeUnknownReason::CommitStatusUnknown
        })
    } else {
        TypedExecutionError::Known(error)
    }
}

fn outcome_may_be_unknown(error: &StoreWriterError) -> bool {
    matches!(
        error,
        StoreWriterError::Closed | StoreWriterError::Store(StoreError::Database(_))
    )
}

pub(in crate::store_writer) fn claim_outcome_from_reconciliation(
    outcome: ClaimTaskReconciliationOutcome,
) -> ClaimTaskOutcome {
    match outcome {
        ClaimTaskReconciliationOutcome::ExistingApplied(receipt) => {
            ClaimTaskOutcome::ExistingApplied(receipt)
        }
        ClaimTaskReconciliationOutcome::KnownNotApplied { current } => {
            ClaimTaskOutcome::KnownNotApplied { current }
        }
        ClaimTaskReconciliationOutcome::InvariantConflict => ClaimTaskOutcome::InvariantConflict,
    }
}

pub(in crate::store_writer) async fn receive<T>(
    receiver: oneshot::Receiver<Result<WriteReceipt<T>, StoreWriterError>>,
) -> Result<WriteReceipt<T>, StoreWriterError> {
    receiver.await.map_err(|_| StoreWriterError::Closed)?
}

pub(super) fn typed_error_disposition<T>(
    error: TypedExecutionError,
    pending: Option<PendingDurableResult>,
) -> DurableDisposition<T> {
    match error {
        TypedExecutionError::DeadlineBeforeStart => DurableDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::DeadlineBeforeStart,
            outcome: None,
            error: None,
        },
        TypedExecutionError::OutcomeUnknown(reason) => {
            DurableDisposition::OutcomeUnknown { reason, pending }
        }
        TypedExecutionError::Known(StoreWriterError::Busy) => DurableDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::BusyRolledBack,
            outcome: None,
            error: None,
        },
        TypedExecutionError::Known(StoreWriterError::DeadlineElapsed) => {
            DurableDisposition::KnownNotApplied {
                reason: KnownNotAppliedReason::DeadlineBeforeStart,
                outcome: None,
                error: None,
            }
        }
        TypedExecutionError::Known(StoreWriterError::Closed) => {
            DurableDisposition::OutcomeUnknown {
                reason: OutcomeUnknownReason::CommitStatusUnknown,
                pending,
            }
        }
        TypedExecutionError::Known(StoreWriterError::Store(error)) => {
            match classify_store_failure(error) {
                StoreFailureClassification::Known(error) => DurableDisposition::KnownNotApplied {
                    reason: KnownNotAppliedReason::KnownRollback,
                    outcome: None,
                    error: Some(error),
                },
                StoreFailureClassification::OutcomeUnknown => DurableDisposition::OutcomeUnknown {
                    reason: OutcomeUnknownReason::CommitStatusUnknown,
                    pending,
                },
                StoreFailureClassification::Invariant(message) => {
                    DurableDisposition::InvariantConflict {
                        message,
                        outcome: None,
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::store_writer) enum StoreFailureClassification {
    Known(KnownNotAppliedError),
    OutcomeUnknown,
    Invariant(&'static str),
}

pub(in crate::store_writer) fn classify_store_failure(
    error: StoreError,
) -> StoreFailureClassification {
    let known = match error {
        StoreError::Database(_) => return StoreFailureClassification::OutcomeUnknown,
        StoreError::InvariantViolation(message) => {
            return StoreFailureClassification::Invariant(message);
        }
        StoreError::Delivery(_)
        | StoreError::TaskNotMergeEligible
        | StoreError::DeliveryOperationInProgress
        | StoreError::DeliveryReconciliationRequired => {
            return StoreFailureClassification::Invariant(
                "delivery store outcome reached the writer before delivery command integration",
            );
        }
        StoreError::Domain(error) => KnownNotAppliedError::Domain(error),
        StoreError::InvalidRepositoryId(error) => {
            KnownNotAppliedError::InvalidRepositoryId(error.to_string())
        }
        StoreError::InvalidTaskId(error) => KnownNotAppliedError::InvalidTaskId(error.to_string()),
        StoreError::InvalidClientRequestId(error) => {
            KnownNotAppliedError::InvalidClientRequestId(error.to_string())
        }
        StoreError::InvalidTaskStatus(status) => KnownNotAppliedError::InvalidTaskStatus(status),
        StoreError::InvalidDeliveryReadiness(readiness) => {
            KnownNotAppliedError::InvalidDeliveryReadiness(readiness)
        }
        StoreError::InvalidEventKind(kind) => KnownNotAppliedError::InvalidEventKind(kind),
        StoreError::InvalidEventSchemaVersion(version) => {
            KnownNotAppliedError::InvalidEventSchemaVersion(version)
        }
        StoreError::InvalidArtifactState(state) => {
            KnownNotAppliedError::InvalidArtifactState(state)
        }
        StoreError::DatabaseSchemaUnsupported => KnownNotAppliedError::DatabaseSchemaUnsupported,
        StoreError::DatabaseMigration(error) => {
            KnownNotAppliedError::DatabaseMigration(error.to_string())
        }
        StoreError::Json(error) => KnownNotAppliedError::Json(error.to_string()),
        StoreError::IllegalTransition { from, to } => {
            KnownNotAppliedError::IllegalTransition { from, to }
        }
        StoreError::IdempotencyConflict => KnownNotAppliedError::IdempotencyConflict,
        StoreError::TaskNotFound => KnownNotAppliedError::TaskNotFound,
        StoreError::InvalidArtifactInput => KnownNotAppliedError::InvalidArtifactInput,
        StoreError::ArtifactIdentityConflict => KnownNotAppliedError::ArtifactIdentityConflict,
        StoreError::ArtifactNotFound => KnownNotAppliedError::ArtifactNotFound,
        StoreError::ArtifactStateConflict => KnownNotAppliedError::ArtifactStateConflict,
        StoreError::TaskNotRetryable => KnownNotAppliedError::TaskNotRetryable,
        StoreError::InvalidRunningEvent => KnownNotAppliedError::InvalidRunningEvent,
        StoreError::TaskAttemptOverflow => KnownNotAppliedError::TaskAttemptOverflow,
        StoreError::WalCheckpointIncomplete {
            busy,
            log_frames,
            checkpointed_frames,
        } => KnownNotAppliedError::WalCheckpointIncomplete {
            busy,
            log_frames,
            checkpointed_frames,
        },
    };
    StoreFailureClassification::Known(known)
}

pub(super) fn wake_event(event_id: Option<EventId>, wake: &dyn EventWake) {
    if event_id.is_some() && catch_unwind(AssertUnwindSafe(|| wake.wake())).is_err() {
        tracing::warn!("event wake panicked after a durable store commit");
    }
}

pub(super) fn receipt_and_wake<T>(
    value: T,
    event_id: Option<EventId>,
    wake: &dyn EventWake,
) -> WriteReceipt<T> {
    wake_event(event_id, wake);
    WriteReceipt { value, event_id }
}

pub(super) fn recovery_receipt_and_wake(
    value: RecoveryReceipt,
    wake: &dyn EventWake,
) -> WriteReceipt<RecoveryReceipt> {
    let event_id = value.last_event_id;
    // A commit-before-reply replay is a no-op, but its committed watermark
    // must still be observable by a dispatcher whose original wake was lost.
    let observed_event_id = event_id.or_else(|| EventId::new(value.high_watermark.get()).ok());
    wake_event(observed_event_id, wake);
    WriteReceipt { value, event_id }
}

pub(in crate::store_writer) fn classify_store_error(error: StoreError) -> StoreWriterError {
    if let StoreError::Database(database) = &error
        && let Some(code) = database.as_database_error().and_then(|error| error.code())
        && sqlite_code_is_retryable(&code)
    {
        return StoreWriterError::Busy;
    }
    StoreWriterError::Store(error)
}

pub(crate) fn sqlite_code_is_retryable(code: &str) -> bool {
    code.parse::<i32>()
        .is_ok_and(|code| matches!(code & 0xff, 5 | 6))
}

fn unexpected_outcome() -> StoreWriterError {
    StoreWriterError::Store(StoreError::InvariantViolation(
        "store writer backend returned a mismatched outcome",
    ))
}

pub(in crate::store_writer) fn completed_transition_bypass_error() -> StoreError {
    StoreError::InvariantViolation(COMPLETED_TRANSITION_BYPASS)
}

pub(super) fn expect_repository(
    outcome: StoreWriterOperationOutcome,
) -> Result<RegisterRepositoryOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::RegisterRepository(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

pub(super) fn expect_create(
    outcome: StoreWriterOperationOutcome,
) -> Result<CreateTaskOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::CreateTask(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

pub(super) fn expect_retry(
    outcome: StoreWriterOperationOutcome,
) -> Result<RetryTaskOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::RetryTask(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

pub(super) fn expect_queue_limited_create(
    outcome: StoreWriterOperationOutcome,
) -> Result<QueueLimitedCreateTaskOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::QueueLimitedCreate(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

pub(super) fn expect_queue_limited_retry(
    outcome: StoreWriterOperationOutcome,
) -> Result<QueueLimitedRetryTaskOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::QueueLimitedRetry(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

pub(super) fn expect_claim(
    outcome: StoreWriterOperationOutcome,
) -> Result<ClaimTaskOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::ClaimTask(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

pub(super) fn expect_reconcile_claim(
    outcome: StoreWriterOperationOutcome,
) -> Result<ClaimTaskReconciliationOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::ReconcileClaimTask(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

pub(super) fn expect_stop_intent_batch(
    outcome: StoreWriterOperationOutcome,
) -> Result<StopIntentBatchReceipt, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::PersistStopIntentBatch(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

pub(super) fn expect_finalize_stopped(
    outcome: StoreWriterOperationOutcome,
) -> Result<FinalizeStoppedTaskOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::FinalizeStoppedTask(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

pub(super) fn expect_transition(
    outcome: StoreWriterOperationOutcome,
) -> Result<TransitionOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::TransitionWithEvent(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

pub(super) fn expect_append(
    outcome: StoreWriterOperationOutcome,
) -> Result<AppendEventOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::AppendRunningEvent(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

pub(super) fn expect_record_review(
    outcome: StoreWriterOperationOutcome,
) -> Result<RecordReviewOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::RecordReview(value) => Ok(*value),
        _ => Err(unexpected_outcome()),
    }
}

pub(super) fn expect_finalize_reviewed_task(
    outcome: StoreWriterOperationOutcome,
) -> Result<FinalizeReviewedTaskOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::FinalizeReviewedTask(value) => Ok(*value),
        _ => Err(unexpected_outcome()),
    }
}

pub(super) fn expect_finalize_unreviewed(
    outcome: StoreWriterOperationOutcome,
) -> Result<FinalizeUnreviewedTaskOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::FinalizeUnreviewedTask(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

pub(super) fn expect_recovery(
    outcome: StoreWriterOperationOutcome,
) -> Result<RecoveryOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::RecoverIncomplete(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

pub(super) fn expect_interrupt_remaining_after_stops(
    outcome: StoreWriterOperationOutcome,
) -> Result<RecoveryReceipt, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::InterruptRemainingAfterStops(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

pub(super) fn expect_reserve_artifact(
    outcome: StoreWriterOperationOutcome,
) -> Result<ReserveAttemptArtifactOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::ReserveAttemptArtifact(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}

pub(super) fn expect_update_artifact(
    outcome: StoreWriterOperationOutcome,
) -> Result<UpdateAttemptArtifactOutcome, StoreWriterError> {
    match outcome {
        StoreWriterOperationOutcome::UpdateAttemptArtifact(value) => Ok(value),
        _ => Err(unexpected_outcome()),
    }
}
