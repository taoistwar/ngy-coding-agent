use super::*;

pub(super) mod execution;

use execution::*;

pub(super) enum WriteCommand {
    RegisterRepository {
        input: NewRepository,
        deadline: Instant,
        response:
            oneshot::Sender<Result<WriteReceipt<RegisterRepositoryOutcome>, StoreWriterError>>,
    },
    CreateTask {
        input: NewTask,
        deadline: Instant,
        response: oneshot::Sender<Result<WriteReceipt<CreateTaskOutcome>, StoreWriterError>>,
    },
    RetryTask {
        task_id: TaskId,
        deadline: Instant,
        response: oneshot::Sender<Result<WriteReceipt<RetryTaskOutcome>, StoreWriterError>>,
    },
    QueueLimitedCreate {
        identity: DurableOperationIdentity,
        input: NewTask,
        max_queued_tasks: NonZeroU32,
        deadline: Instant,
        pending: PendingDurableResult,
        reconciliation_lane: bool,
        response: oneshot::Sender<DurableCompletion<QueueLimitedCreateTaskOutcome>>,
    },
    QueueLimitedRetry {
        identity: DurableOperationIdentity,
        source_task_id: TaskId,
        max_queued_tasks: NonZeroU32,
        deadline: Instant,
        pending: PendingDurableResult,
        reconciliation_lane: bool,
        response: oneshot::Sender<DurableCompletion<QueueLimitedRetryTaskOutcome>>,
    },
    ClaimTask {
        identity: DurableOperationIdentity,
        sequence_guard: MutationSequenceGuard,
        request: ClaimTaskRequest,
        deadline: Instant,
        pending: PendingDurableResult,
        reconciliation_lane: bool,
        response: oneshot::Sender<DurableCompletion<ClaimTaskOutcome>>,
    },
    ReconcileClaimTask {
        identity: DurableOperationIdentity,
        sequence_guard: MutationSequenceGuard,
        request: ClaimTaskRequest,
        deadline: Instant,
        pending: PendingDurableResult,
        response: oneshot::Sender<DurableCompletion<ClaimTaskReconciliationOutcome>>,
    },
    PersistStopIntentBatch {
        identity: DurableOperationIdentity,
        sequence_guard: MutationSequenceGuard,
        requests: Vec<StopIntentRequest>,
        deadline: Instant,
        pending: PendingDurableResult,
        reconciliation_lane: bool,
        response: oneshot::Sender<DurableCompletion<StopIntentBatchReceipt>>,
    },
    FinalizeStoppedTask {
        identity: DurableOperationIdentity,
        sequence_guard: MutationSequenceGuard,
        request: FinalizeStoppedTaskRequest,
        deadline: Instant,
        pending: PendingDurableResult,
        reconciliation_lane: bool,
        response: oneshot::Sender<DurableCompletion<FinalizeStoppedTaskOutcome>>,
    },
    TypedAppendRunningEvent {
        identity: DurableOperationIdentity,
        sequence_guard: MutationSequenceGuard,
        task_id: TaskId,
        payload: TaskEventPayload,
        deadline: Instant,
        response: oneshot::Sender<DurableCompletion<AppendEventOutcome>>,
    },
    TypedRecordReview {
        identity: DurableOperationIdentity,
        sequence_guard: MutationSequenceGuard,
        request: RecordReviewRequest,
        deadline: Instant,
        pending: PendingDurableResult,
        reconciliation_lane: bool,
        response: oneshot::Sender<DurableCompletion<RecordReviewOutcome>>,
    },
    TypedFinalizeReviewedTask {
        identity: DurableOperationIdentity,
        sequence_guard: MutationSequenceGuard,
        request: FinalizeReviewedTaskRequest,
        deadline: Instant,
        pending: PendingDurableResult,
        reconciliation_lane: bool,
        response: oneshot::Sender<DurableCompletion<FinalizeReviewedTaskOutcome>>,
    },
    TypedFinalizeUnreviewedTask {
        identity: DurableOperationIdentity,
        sequence_guard: MutationSequenceGuard,
        request: FinalizeUnreviewedTaskRequest,
        deadline: Instant,
        pending: PendingDurableResult,
        reconciliation_lane: bool,
        response: oneshot::Sender<DurableCompletion<FinalizeUnreviewedTaskOutcome>>,
    },
    TransitionWithEvent {
        task_id: TaskId,
        expected: TaskStatus,
        transition: TaskTransition,
        deadline: Instant,
        response: oneshot::Sender<Result<WriteReceipt<TransitionOutcome>, StoreWriterError>>,
    },
    AppendRunningEvent {
        task_id: TaskId,
        payload: TaskEventPayload,
        deadline: Instant,
        response: oneshot::Sender<Result<WriteReceipt<AppendEventOutcome>, StoreWriterError>>,
    },
    RecordReview {
        request: RecordReviewRequest,
        deadline: Instant,
        response: oneshot::Sender<Result<WriteReceipt<RecordReviewOutcome>, StoreWriterError>>,
    },
    FinalizeReviewedTask {
        request: FinalizeReviewedTaskRequest,
        deadline: Instant,
        response:
            oneshot::Sender<Result<WriteReceipt<FinalizeReviewedTaskOutcome>, StoreWriterError>>,
    },
    ReserveAttemptArtifact {
        input: ReserveAttemptArtifact,
        deadline: Instant,
        response:
            oneshot::Sender<Result<WriteReceipt<ReserveAttemptArtifactOutcome>, StoreWriterError>>,
    },
    MarkAttemptArtifactReady {
        identity: AttemptArtifactIdentity,
        deadline: Instant,
        response:
            oneshot::Sender<Result<WriteReceipt<UpdateAttemptArtifactOutcome>, StoreWriterError>>,
    },
    MarkAttemptArtifactInconsistent {
        identity: AttemptArtifactIdentity,
        failure_code: String,
        deadline: Instant,
        response:
            oneshot::Sender<Result<WriteReceipt<UpdateAttemptArtifactOutcome>, StoreWriterError>>,
    },
    InterruptRemainingAfterStops {
        failure: TaskFailure,
        deadline: Instant,
        response: oneshot::Sender<Result<WriteReceipt<RecoveryReceipt>, StoreWriterError>>,
    },
    RecoverIncomplete {
        now: UtcTimestamp,
        failure: TaskFailure,
        deadline: Instant,
        response: oneshot::Sender<Result<WriteReceipt<RecoveryOutcome>, StoreWriterError>>,
    },
}

impl WriteCommand {
    fn mutation_identities(&self) -> Vec<TaskMutationIdentity> {
        match self {
            Self::ClaimTask { identity, .. }
            | Self::ReconcileClaimTask { identity, .. }
            | Self::PersistStopIntentBatch { identity, .. }
            | Self::FinalizeStoppedTask { identity, .. }
            | Self::TypedAppendRunningEvent { identity, .. }
            | Self::TypedRecordReview { identity, .. }
            | Self::TypedFinalizeReviewedTask { identity, .. }
            | Self::TypedFinalizeUnreviewedTask { identity, .. } => {
                task_mutation_identities(identity)
            }
            Self::RegisterRepository { .. }
            | Self::CreateTask { .. }
            | Self::RetryTask { .. }
            | Self::QueueLimitedCreate { .. }
            | Self::QueueLimitedRetry { .. }
            | Self::TransitionWithEvent { .. }
            | Self::AppendRunningEvent { .. }
            | Self::RecordReview { .. }
            | Self::FinalizeReviewedTask { .. }
            | Self::ReserveAttemptArtifact { .. }
            | Self::MarkAttemptArtifactReady { .. }
            | Self::MarkAttemptArtifactInconsistent { .. }
            | Self::InterruptRemainingAfterStops { .. }
            | Self::RecoverIncomplete { .. } => Vec::new(),
        }
    }
}

pub(super) async fn run_writer(
    mut normal_receiver: mpsc::Receiver<WriteCommand>,
    mut urgent_receiver: mpsc::Receiver<WriteCommand>,
    mut reconciliation_receiver: mpsc::Receiver<WriteCommand>,
    backend: Arc<dyn StoreWriterBackend>,
    wake: Arc<dyn EventWake>,
    capacity: usize,
) {
    let mut scheduler = PriorityScheduler::new(capacity, capacity);
    let mut normal_closed = false;
    let mut urgent_closed = false;
    let mut reconciliation_closed = false;

    loop {
        while scheduler.has_reconciliation_capacity() && !reconciliation_closed {
            match reconciliation_receiver.try_recv() {
                Ok(command) => {
                    let identities = command.mutation_identities();
                    scheduler
                        .enqueue_reconciliation(identities, command)
                        .expect("reconciliation scheduler capacity checked before enqueue");
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    reconciliation_closed = true;
                }
            }
        }
        while scheduler.has_normal_capacity() && !normal_closed {
            match normal_receiver.try_recv() {
                Ok(command) => {
                    let identities = command.mutation_identities();
                    scheduler
                        .enqueue(StoreWriterPriority::Normal, identities, command)
                        .expect("normal scheduler capacity checked before enqueue");
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    normal_closed = true;
                }
            }
        }
        while scheduler.has_urgent_capacity() && !urgent_closed {
            match urgent_receiver.try_recv() {
                Ok(command) => {
                    let identities = command.mutation_identities();
                    scheduler
                        .enqueue(StoreWriterPriority::Urgent, identities, command)
                        .expect("urgent scheduler capacity checked before enqueue");
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    urgent_closed = true;
                }
            }
        }

        if let Some(command) = scheduler.pop_next() {
            let advance_sequence = process_write_command(command.value, &*backend, &*wake).await;
            scheduler.complete(&command.identities, advance_sequence);
            continue;
        }

        if normal_closed && urgent_closed && reconciliation_closed && scheduler.is_empty() {
            break;
        }

        tokio::select! {
            command = reconciliation_receiver.recv(),
                if scheduler.has_reconciliation_capacity() && !reconciliation_closed =>
            {
                match command {
                    Some(command) => {
                        let identities = command.mutation_identities();
                        scheduler
                            .enqueue_reconciliation(identities, command)
                            .expect("reconciliation scheduler capacity checked before receive");
                    }
                    None => reconciliation_closed = true,
                }
            }
            command = normal_receiver.recv(),
                if scheduler.has_normal_capacity() && !normal_closed =>
            {
                match command {
                    Some(command) => {
                        let identities = command.mutation_identities();
                        scheduler
                            .enqueue(StoreWriterPriority::Normal, identities, command)
                            .expect("normal scheduler capacity checked before receive");
                    }
                    None => normal_closed = true,
                }
            }
            command = urgent_receiver.recv(),
                if scheduler.has_urgent_capacity() && !urgent_closed =>
            {
                match command {
                    Some(command) => {
                        let identities = command.mutation_identities();
                        scheduler
                            .enqueue(StoreWriterPriority::Urgent, identities, command)
                            .expect("urgent scheduler capacity checked before receive");
                    }
                    None => urgent_closed = true,
                }
            }
            else => std::future::pending::<()>().await,
        }
    }
}

#[derive(Clone, Copy)]
enum WriteCommandClass {
    DirectLifecycle,
    Maintenance,
    QueueAdmission,
    Claim,
    StopAndActivity,
    QualityFinalization,
}

fn command_class(command: &WriteCommand) -> WriteCommandClass {
    match command {
        WriteCommand::CreateTask { .. }
        | WriteCommand::RetryTask { .. }
        | WriteCommand::TransitionWithEvent { .. }
        | WriteCommand::AppendRunningEvent { .. }
        | WriteCommand::RecordReview { .. }
        | WriteCommand::FinalizeReviewedTask { .. } => WriteCommandClass::DirectLifecycle,
        WriteCommand::RegisterRepository { .. }
        | WriteCommand::InterruptRemainingAfterStops { .. }
        | WriteCommand::RecoverIncomplete { .. }
        | WriteCommand::ReserveAttemptArtifact { .. }
        | WriteCommand::MarkAttemptArtifactReady { .. }
        | WriteCommand::MarkAttemptArtifactInconsistent { .. } => WriteCommandClass::Maintenance,
        WriteCommand::QueueLimitedCreate { .. } | WriteCommand::QueueLimitedRetry { .. } => {
            WriteCommandClass::QueueAdmission
        }
        WriteCommand::ClaimTask { .. } | WriteCommand::ReconcileClaimTask { .. } => {
            WriteCommandClass::Claim
        }
        WriteCommand::PersistStopIntentBatch { .. }
        | WriteCommand::FinalizeStoppedTask { .. }
        | WriteCommand::TypedAppendRunningEvent { .. } => WriteCommandClass::StopAndActivity,
        WriteCommand::TypedRecordReview { .. }
        | WriteCommand::TypedFinalizeReviewedTask { .. }
        | WriteCommand::TypedFinalizeUnreviewedTask { .. } => {
            WriteCommandClass::QualityFinalization
        }
    }
}

async fn process_write_command(
    command: WriteCommand,
    backend: &dyn StoreWriterBackend,
    wake: &dyn EventWake,
) -> bool {
    match command_class(&command) {
        WriteCommandClass::DirectLifecycle => {
            process_direct_lifecycle_command(command, backend, wake).await
        }
        WriteCommandClass::Maintenance => process_maintenance_command(command, backend, wake).await,
        WriteCommandClass::QueueAdmission => {
            process_queue_admission_command(command, backend, wake).await
        }
        WriteCommandClass::Claim => process_claim_command(command, backend, wake).await,
        WriteCommandClass::StopAndActivity => {
            process_stop_and_activity_command(command, backend, wake).await
        }
        WriteCommandClass::QualityFinalization => {
            process_quality_finalization_command(command, backend, wake).await
        }
    }
}

async fn process_direct_lifecycle_command(
    command: WriteCommand,
    backend: &dyn StoreWriterBackend,
    wake: &dyn EventWake,
) -> bool {
    let advance_sequence = true;
    match command {
        WriteCommand::CreateTask {
            input,
            deadline,
            response,
        } => {
            let result = execute(backend, StoreWriterOperation::CreateTask(input), deadline)
                .await
                .and_then(expect_create)
                .map(|value| {
                    let event_id = match &value {
                        CreateTaskOutcome::Created { event_id, .. } => Some(*event_id),
                        CreateTaskOutcome::Existing { .. } => None,
                    };
                    receipt_and_wake(value, event_id, wake)
                });
            let _ = response.send(result);
        }
        WriteCommand::RetryTask {
            task_id,
            deadline,
            response,
        } => {
            let result = execute(backend, StoreWriterOperation::RetryTask(task_id), deadline)
                .await
                .and_then(expect_retry)
                .map(|value| {
                    let event_id = match &value {
                        RetryTaskOutcome::Created { event_id, .. } => Some(*event_id),
                        RetryTaskOutcome::Existing { .. } => None,
                    };
                    receipt_and_wake(value, event_id, wake)
                });
            let _ = response.send(result);
        }
        WriteCommand::TransitionWithEvent {
            task_id,
            expected,
            transition,
            deadline,
            response,
        } => {
            if matches!(transition, TaskTransition::Completed) {
                let _ = response.send(Err(completed_transition_bypass_error().into()));
                return advance_sequence;
            }
            let operation = StoreWriterOperation::TransitionWithEvent {
                task_id,
                expected,
                transition,
            };
            let result = execute(backend, operation, deadline)
                .await
                .and_then(expect_transition)
                .map(|value| {
                    let event_id = match &value {
                        TransitionOutcome::Applied { event_id, .. } => Some(*event_id),
                        TransitionOutcome::Conflict { .. } => None,
                    };
                    receipt_and_wake(value, event_id, wake)
                });
            let _ = response.send(result);
        }
        WriteCommand::AppendRunningEvent {
            task_id,
            payload,
            deadline,
            response,
        } => {
            let operation = StoreWriterOperation::AppendRunningEvent { task_id, payload };
            let result = execute(backend, operation, deadline)
                .await
                .and_then(expect_append)
                .map(|value| {
                    let event_id = match &value {
                        AppendEventOutcome::Applied { event_id } => Some(*event_id),
                        AppendEventOutcome::NotRunning { .. } => None,
                    };
                    receipt_and_wake(value, event_id, wake)
                });
            let _ = response.send(result);
        }
        WriteCommand::RecordReview {
            request,
            deadline,
            response,
        } => {
            let result = execute(
                backend,
                StoreWriterOperation::RecordReview(request),
                deadline,
            )
            .await
            .and_then(expect_record_review)
            .map(|value| {
                let event_id = match &value {
                    RecordReviewOutcome::Applied { event_id, .. }
                    | RecordReviewOutcome::Existing { event_id, .. } => Some(*event_id),
                };
                receipt_and_wake(value, event_id, wake)
            });
            let _ = response.send(result);
        }
        WriteCommand::FinalizeReviewedTask {
            request,
            deadline,
            response,
        } => {
            let result = execute(
                backend,
                StoreWriterOperation::FinalizeReviewedTask(request),
                deadline,
            )
            .await
            .and_then(expect_finalize_reviewed_task)
            .map(|value| {
                let event_id = match &value {
                    FinalizeReviewedTaskOutcome::Applied {
                        terminal_event_id, ..
                    }
                    | FinalizeReviewedTaskOutcome::Existing {
                        terminal_event_id, ..
                    } => Some(*terminal_event_id),
                };
                receipt_and_wake(value, event_id, wake)
            });
            let _ = response.send(result);
        }
        _ => unreachable!("command classification and dispatch diverged"),
    }
    advance_sequence
}

async fn process_maintenance_command(
    command: WriteCommand,
    backend: &dyn StoreWriterBackend,
    wake: &dyn EventWake,
) -> bool {
    match command {
        WriteCommand::RegisterRepository {
            input,
            deadline,
            response,
        } => {
            let result = execute(
                backend,
                StoreWriterOperation::RegisterRepository(input),
                deadline,
            )
            .await
            .and_then(expect_repository)
            .map(|value| WriteReceipt {
                value,
                event_id: None,
            });
            let _ = response.send(result);
        }
        WriteCommand::InterruptRemainingAfterStops {
            failure,
            deadline,
            response,
        } => {
            let result = execute(
                backend,
                StoreWriterOperation::InterruptRemainingAfterStops(failure),
                deadline,
            )
            .await
            .and_then(expect_interrupt_remaining_after_stops)
            .map(|value| recovery_receipt_and_wake(value, wake));
            let _ = response.send(result);
        }
        WriteCommand::RecoverIncomplete {
            now,
            failure,
            deadline,
            response,
        } => {
            let operation = StoreWriterOperation::RecoverIncomplete { now, failure };
            let result = execute(backend, operation, deadline)
                .await
                .and_then(expect_recovery)
                .map(|value| {
                    let event_id = value.last_event_id;
                    receipt_and_wake(value, event_id, wake)
                });
            let _ = response.send(result);
        }
        WriteCommand::ReserveAttemptArtifact {
            input,
            deadline,
            response,
        } => {
            let result = execute(
                backend,
                StoreWriterOperation::ReserveAttemptArtifact(input),
                deadline,
            )
            .await
            .and_then(expect_reserve_artifact)
            .map(|value| WriteReceipt {
                value,
                event_id: None,
            });
            let _ = response.send(result);
        }
        WriteCommand::MarkAttemptArtifactReady {
            identity,
            deadline,
            response,
        } => {
            let result = execute(
                backend,
                StoreWriterOperation::MarkAttemptArtifactReady(identity),
                deadline,
            )
            .await
            .and_then(expect_update_artifact)
            .map(|value| WriteReceipt {
                value,
                event_id: None,
            });
            let _ = response.send(result);
        }
        WriteCommand::MarkAttemptArtifactInconsistent {
            identity,
            failure_code,
            deadline,
            response,
        } => {
            let result = execute(
                backend,
                StoreWriterOperation::MarkAttemptArtifactInconsistent {
                    identity,
                    failure_code,
                },
                deadline,
            )
            .await
            .and_then(expect_update_artifact)
            .map(|value| WriteReceipt {
                value,
                event_id: None,
            });
            let _ = response.send(result);
        }
        _ => unreachable!("command classification and dispatch diverged"),
    }
    true
}

async fn process_queue_admission_command(
    command: WriteCommand,
    backend: &dyn StoreWriterBackend,
    wake: &dyn EventWake,
) -> bool {
    let advance_sequence;
    match command {
        WriteCommand::QueueLimitedCreate {
            identity,
            input,
            max_queued_tasks,
            deadline,
            pending,
            reconciliation_lane,
            response,
        } => {
            let operation = StoreWriterOperation::QueueLimitedCreate {
                input,
                max_queued_tasks,
            };
            let execution =
                execute_with_query_first_replay(backend, operation, deadline, reconciliation_lane)
                    .await;
            let disposition = match execution.result.and_then(|outcome| {
                expect_queue_limited_create(outcome).map_err(TypedExecutionError::Known)
            }) {
                Ok(value @ QueueLimitedCreateTaskOutcome::Created { event_id, .. }) => {
                    wake_event(Some(event_id), wake);
                    DurableDisposition::Confirmed(value)
                }
                Ok(QueueLimitedCreateTaskOutcome::Existing { task }) => {
                    wake_event(Some(task.last_event_id), wake);
                    DurableDisposition::Confirmed(QueueLimitedCreateTaskOutcome::Existing { task })
                }
                Ok(value @ QueueLimitedCreateTaskOutcome::QueueFull { .. }) => {
                    DurableDisposition::KnownNotApplied {
                        reason: KnownNotAppliedReason::ExactReconciliation,
                        outcome: Some(value),
                        error: None,
                    }
                }
                Err(error) => typed_error_disposition(error, Some(pending)),
            };
            advance_sequence = disposition_advances_sequence(&disposition);

            let _ = response.send(DurableCompletion {
                identity,
                sequence_disposition: sequence_disposition(&disposition),
                disposition,
            });
        }
        WriteCommand::QueueLimitedRetry {
            identity,
            source_task_id,
            max_queued_tasks,
            deadline,
            pending,
            reconciliation_lane,
            response,
        } => {
            let operation = StoreWriterOperation::QueueLimitedRetry {
                source_task_id,
                max_queued_tasks,
            };
            let execution =
                execute_with_query_first_replay(backend, operation, deadline, reconciliation_lane)
                    .await;
            let disposition = match execution.result.and_then(|outcome| {
                expect_queue_limited_retry(outcome).map_err(TypedExecutionError::Known)
            }) {
                Ok(value @ QueueLimitedRetryTaskOutcome::Created { event_id, .. }) => {
                    wake_event(Some(event_id), wake);
                    DurableDisposition::Confirmed(value)
                }
                Ok(QueueLimitedRetryTaskOutcome::Existing { task }) => {
                    wake_event(Some(task.last_event_id), wake);
                    DurableDisposition::Confirmed(QueueLimitedRetryTaskOutcome::Existing { task })
                }
                Ok(value @ QueueLimitedRetryTaskOutcome::QueueFull { .. }) => {
                    DurableDisposition::KnownNotApplied {
                        reason: KnownNotAppliedReason::ExactReconciliation,
                        outcome: Some(value),
                        error: None,
                    }
                }
                Err(error) => typed_error_disposition(error, Some(pending)),
            };
            advance_sequence = disposition_advances_sequence(&disposition);
            let _ = response.send(DurableCompletion {
                identity,
                sequence_disposition: sequence_disposition(&disposition),
                disposition,
            });
        }
        _ => unreachable!("command classification and dispatch diverged"),
    }
    advance_sequence
}

async fn process_claim_command(
    command: WriteCommand,
    backend: &dyn StoreWriterBackend,
    wake: &dyn EventWake,
) -> bool {
    let advance_sequence;
    match command {
        WriteCommand::ClaimTask {
            identity,
            mut sequence_guard,
            request,
            deadline,
            pending,
            reconciliation_lane,
            response,
        } => {
            let execution = execute_typed(
                backend,
                StoreWriterOperation::ClaimTask(request.clone()),
                Some(StoreWriterOperation::ReconcileClaimTask(request)),
                deadline,
                reconciliation_lane,
            )
            .await;
            let reconciled = execution.reconciled;
            let result = execution.result.and_then(|outcome| {
                if reconciled {
                    expect_reconcile_claim(outcome)
                        .map(claim_outcome_from_reconciliation)
                        .map_err(TypedExecutionError::Known)
                } else {
                    expect_claim(outcome).map_err(TypedExecutionError::Known)
                }
            });
            let disposition = match result {
                Ok(value) => {
                    let event_id = match &value {
                        ClaimTaskOutcome::Applied(receipt)
                        | ClaimTaskOutcome::ExistingApplied(receipt) => {
                            Some(receipt.started_event_id)
                        }
                        ClaimTaskOutcome::KnownNotApplied { .. }
                        | ClaimTaskOutcome::InvariantConflict => None,
                    };
                    match value {
                        applied @ (ClaimTaskOutcome::Applied(_)
                        | ClaimTaskOutcome::ExistingApplied(_)) => {
                            wake_event(event_id, wake);
                            DurableDisposition::Confirmed(applied)
                        }
                        known @ ClaimTaskOutcome::KnownNotApplied { .. } => {
                            DurableDisposition::KnownNotApplied {
                                reason: KnownNotAppliedReason::ExactReconciliation,
                                outcome: Some(known),
                                error: None,
                            }
                        }
                        conflict @ ClaimTaskOutcome::InvariantConflict => {
                            DurableDisposition::InvariantConflict {
                                message: "claim tuple is inconsistent",
                                outcome: Some(conflict),
                            }
                        }
                    }
                }
                Err(error) => typed_error_disposition(error, Some(pending)),
            };
            advance_sequence =
                send_typed_completion(response, identity, disposition, Some(&mut sequence_guard));
        }
        WriteCommand::ReconcileClaimTask {
            identity,
            mut sequence_guard,
            request,
            deadline,
            pending,
            response,
        } => {
            let execution = execute_with_query_first_replay(
                backend,
                StoreWriterOperation::ReconcileClaimTask(request),
                deadline,
                true,
            )
            .await;
            let disposition = match execution.result.and_then(|outcome| {
                expect_reconcile_claim(outcome).map_err(TypedExecutionError::Known)
            }) {
                Ok(value @ ClaimTaskReconciliationOutcome::ExistingApplied(_)) => {
                    let event_id = match &value {
                        ClaimTaskReconciliationOutcome::ExistingApplied(receipt) => {
                            receipt.started_event_id
                        }
                        _ => unreachable!("matched existing claim reconciliation"),
                    };
                    wake_event(Some(event_id), wake);
                    DurableDisposition::Confirmed(value)
                }
                Ok(value @ ClaimTaskReconciliationOutcome::KnownNotApplied { .. }) => {
                    DurableDisposition::KnownNotApplied {
                        reason: KnownNotAppliedReason::ExactReconciliation,
                        outcome: Some(value),
                        error: None,
                    }
                }
                Ok(value @ ClaimTaskReconciliationOutcome::InvariantConflict) => {
                    DurableDisposition::InvariantConflict {
                        message: "claim tuple is inconsistent",
                        outcome: Some(value),
                    }
                }
                Err(error) => typed_error_disposition(error, Some(pending)),
            };
            advance_sequence =
                send_typed_completion(response, identity, disposition, Some(&mut sequence_guard));
        }
        _ => unreachable!("command classification and dispatch diverged"),
    }
    advance_sequence
}

async fn process_stop_and_activity_command(
    command: WriteCommand,
    backend: &dyn StoreWriterBackend,
    wake: &dyn EventWake,
) -> bool {
    let advance_sequence;
    match command {
        WriteCommand::PersistStopIntentBatch {
            identity,
            mut sequence_guard,
            requests,
            deadline,
            pending,
            reconciliation_lane,
            response,
        } => {
            let execution = execute_with_query_first_replay(
                backend,
                StoreWriterOperation::PersistStopIntentBatch(requests),
                deadline,
                reconciliation_lane,
            )
            .await;
            let disposition = execution
                .result
                .and_then(|outcome| {
                    expect_stop_intent_batch(outcome).map_err(TypedExecutionError::Known)
                })
                .map_or_else(
                    |error| typed_error_disposition(error, Some(pending)),
                    DurableDisposition::Confirmed,
                );
            advance_sequence =
                send_typed_completion(response, identity, disposition, Some(&mut sequence_guard));
        }
        WriteCommand::FinalizeStoppedTask {
            identity,
            mut sequence_guard,
            request,
            deadline,
            pending,
            reconciliation_lane,
            response,
        } => {
            let execution = execute_with_query_first_replay(
                backend,
                StoreWriterOperation::FinalizeStoppedTask(request),
                deadline,
                reconciliation_lane,
            )
            .await;
            let disposition = match execution.result.and_then(|outcome| {
                expect_finalize_stopped(outcome).map_err(TypedExecutionError::Known)
            }) {
                Ok(value) => {
                    let event_id = match &value {
                        FinalizeStoppedTaskOutcome::Applied(receipt)
                        | FinalizeStoppedTaskOutcome::Existing(receipt) => {
                            Some(receipt.terminal_event_id)
                        }
                        FinalizeStoppedTaskOutcome::InvariantConflict => None,
                    };
                    match value {
                        confirmed @ (FinalizeStoppedTaskOutcome::Applied(_)
                        | FinalizeStoppedTaskOutcome::Existing(_)) => {
                            wake_event(event_id, wake);
                            DurableDisposition::Confirmed(confirmed)
                        }
                        conflict @ FinalizeStoppedTaskOutcome::InvariantConflict => {
                            DurableDisposition::InvariantConflict {
                                message: "final-stop tuple is inconsistent",
                                outcome: Some(conflict),
                            }
                        }
                    }
                }
                Err(error) => typed_error_disposition(error, Some(pending)),
            };
            advance_sequence =
                send_typed_completion(response, identity, disposition, Some(&mut sequence_guard));
        }
        WriteCommand::TypedAppendRunningEvent {
            identity,
            mut sequence_guard,
            task_id,
            payload,
            deadline,
            response,
        } => {
            let execution = execute_typed(
                backend,
                StoreWriterOperation::AppendRunningEvent { task_id, payload },
                None,
                deadline,
                false,
            )
            .await;
            let disposition = match execution
                .result
                .and_then(|outcome| expect_append(outcome).map_err(TypedExecutionError::Known))
            {
                Ok(value @ AppendEventOutcome::Applied { event_id }) => {
                    wake_event(Some(event_id), wake);
                    DurableDisposition::Confirmed(value)
                }
                Ok(value @ AppendEventOutcome::NotRunning { .. }) => {
                    DurableDisposition::KnownNotApplied {
                        reason: KnownNotAppliedReason::ExactReconciliation,
                        outcome: Some(value),
                        error: None,
                    }
                }
                Err(error) => typed_error_disposition(error, None),
            };
            advance_sequence =
                send_typed_completion(response, identity, disposition, Some(&mut sequence_guard));
        }
        _ => unreachable!("command classification and dispatch diverged"),
    }
    advance_sequence
}

async fn process_quality_finalization_command(
    command: WriteCommand,
    backend: &dyn StoreWriterBackend,
    wake: &dyn EventWake,
) -> bool {
    let advance_sequence;
    match command {
        WriteCommand::TypedRecordReview {
            identity,
            mut sequence_guard,
            request,
            deadline,
            pending,
            reconciliation_lane,
            response,
        } => {
            let execution = execute_with_query_first_replay(
                backend,
                StoreWriterOperation::RecordReview(request),
                deadline,
                reconciliation_lane,
            )
            .await;
            let disposition = match execution.result.and_then(|outcome| {
                expect_record_review(outcome).map_err(TypedExecutionError::Known)
            }) {
                Ok(value) => {
                    let event_id = match &value {
                        RecordReviewOutcome::Applied { event_id, .. }
                        | RecordReviewOutcome::Existing { event_id, .. } => *event_id,
                    };
                    wake_event(Some(event_id), wake);
                    DurableDisposition::Confirmed(value)
                }
                Err(error) => typed_error_disposition(error, Some(pending)),
            };
            advance_sequence =
                send_typed_completion(response, identity, disposition, Some(&mut sequence_guard));
        }
        WriteCommand::TypedFinalizeReviewedTask {
            identity,
            mut sequence_guard,
            request,
            deadline,
            pending,
            reconciliation_lane,

            response,
        } => {
            let execution = execute_with_query_first_replay(
                backend,
                StoreWriterOperation::FinalizeReviewedTask(request),
                deadline,
                reconciliation_lane,
            )
            .await;
            let disposition = match execution.result.and_then(|outcome| {
                expect_finalize_reviewed_task(outcome).map_err(TypedExecutionError::Known)
            }) {
                Ok(value) => {
                    let terminal_event_id = match &value {
                        FinalizeReviewedTaskOutcome::Applied {
                            terminal_event_id, ..
                        }
                        | FinalizeReviewedTaskOutcome::Existing {
                            terminal_event_id, ..
                        } => *terminal_event_id,
                    };
                    wake_event(Some(terminal_event_id), wake);
                    DurableDisposition::Confirmed(value)
                }
                Err(error) => typed_error_disposition(error, Some(pending)),
            };
            advance_sequence =
                send_typed_completion(response, identity, disposition, Some(&mut sequence_guard));
        }
        WriteCommand::TypedFinalizeUnreviewedTask {
            identity,
            mut sequence_guard,
            request,
            deadline,
            pending,
            reconciliation_lane,
            response,
        } => {
            let execution = execute_with_query_first_replay(
                backend,
                StoreWriterOperation::FinalizeUnreviewedTask(request),
                deadline,
                reconciliation_lane,
            )
            .await;
            let disposition = match execution.result.and_then(|outcome| {
                expect_finalize_unreviewed(outcome).map_err(TypedExecutionError::Known)
            }) {
                Ok(
                    value @ (FinalizeUnreviewedTaskOutcome::Applied { .. }
                    | FinalizeUnreviewedTaskOutcome::Existing { .. }),
                ) => {
                    let event_id = match &value {
                        FinalizeUnreviewedTaskOutcome::Applied { event_id, .. }
                        | FinalizeUnreviewedTaskOutcome::Existing { event_id, .. } => *event_id,
                        FinalizeUnreviewedTaskOutcome::InvariantConflict => {
                            unreachable!("matched exact unreviewed terminal")
                        }
                    };
                    wake_event(Some(event_id), wake);
                    DurableDisposition::Confirmed(value)
                }
                Ok(value @ FinalizeUnreviewedTaskOutcome::InvariantConflict) => {
                    DurableDisposition::InvariantConflict {
                        message: "unreviewed terminal tuple is inconsistent",
                        outcome: Some(value),
                    }
                }
                Err(error) => typed_error_disposition(error, Some(pending)),
            };
            advance_sequence =
                send_typed_completion(response, identity, disposition, Some(&mut sequence_guard));
        }
        _ => unreachable!("command classification and dispatch diverged"),
    }
    advance_sequence
}
