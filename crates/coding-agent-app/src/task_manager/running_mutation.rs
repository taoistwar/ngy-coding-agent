use super::*;
type RunnerReviewObserver = Option<oneshot::Sender<Result<EventId, RunnerEventError>>>;

#[derive(Clone, Copy)]
struct RunnerReviewCompletionKey {
    task_id: TaskId,
    operation_nonce: u64,
    lineage_id: u64,
    attempt_id: u64,
    identity: TaskMutationIdentity,
}

struct CurrentRunnerReviewWrite {
    attempt_id: u64,
    identity: TaskMutationIdentity,
    request: RecordReviewRequest,
    deadline: Instant,
    retry_available: bool,
}

fn reject_runner_review_observer(observer: &mut RunnerReviewObserver) {
    if let Some(response) = observer.take() {
        let _ = response.send(Err(RunnerEventError::StoreDegraded));
    }
}

fn runner_event_is_allowed_during_stop(stop_state: &ActiveStopState, event: &RunnerEvent) -> bool {
    match stop_state {
        ActiveStopState::NoWinner => true,
        ActiveStopState::IntentSubmissionDeferred { .. }
        | ActiveStopState::IntentWritePending { .. }
        | ActiveStopState::IntentDurable { .. } => matches!(
            event,
            RunnerEvent::ActivityAppended(_)
                | RunnerEvent::DiffUpdated(_)
                | RunnerEvent::TestUpdated(_)
        ),
        ActiveStopState::FinalStopWritePending { .. }
        | ActiveStopState::StopTerminal { .. }
        | ActiveStopState::TerminalWon { .. } => false,
    }
}

impl TaskManager {
    pub(super) fn submit_runner_event(
        &mut self,
        task_id: TaskId,
        event: RunnerEvent,
        response: oneshot::Sender<Result<EventId, RunnerEventError>>,
    ) {
        if self.is_frozen()
            || self.degraded
            || self.service_state.current().state != ServiceState::Ready
        {
            let _ = response.send(Err(RunnerEventError::StoreDegraded));
            return;
        }
        let Some((operation_nonce, next_in_flight)) =
            self.active.get(&task_id).and_then(|active| {
                (active.phase == AdmissionPhase::Running
                    && active.preparation_complete
                    && runner_event_is_allowed_during_stop(&active.stop_state, &event)
                    && !active.durable_sequence_blocked)
                    .then(|| {
                        active
                            .in_flight_mutations
                            .checked_add(1)
                            .map(|count| (active.operation_nonce, count))
                    })
                    .flatten()
            })
        else {
            let _ = response.send(Err(RunnerEventError::TaskNotRunning));
            return;
        };
        let deadline = self.current_persistence_deadline();
        if self.is_frozen() || Instant::now() >= deadline {
            let _ = response.send(Err(RunnerEventError::StoreDegraded));
            return;
        }
        let Some(active) = self.active.get_mut(&task_id) else {
            self.freeze_degraded();
            let _ = response.send(Err(RunnerEventError::StoreDegraded));
            return;
        };
        let logical_id = active.next_runner_mutation_id;
        let Some(next_logical_id) = logical_id.checked_add(1) else {
            self.freeze_degraded();
            let _ = response.send(Err(RunnerEventError::StoreDegraded));
            return;
        };
        if active.operation_nonce != operation_nonce {
            self.freeze_degraded();
            let _ = response.send(Err(RunnerEventError::StoreDegraded));
            return;
        }
        active.next_runner_mutation_id = next_logical_id;
        if active
            .pending_runner_event_writes
            .insert(
                logical_id,
                PendingRunnerEventWrite {
                    stage: RunnerEventWriteStage::Deferred(event),
                    deadline,
                    response: Some(response),
                },
            )
            .is_some()
        {
            self.freeze_degraded();
            return;
        }
        active.in_flight_mutations = next_in_flight;
        self.drive_next_running_mutation(task_id, operation_nonce);
    }

    fn submit_runner_event_write(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        logical_id: u64,
        identity: TaskMutationIdentity,
        event: RunnerEvent,
        deadline: Instant,
    ) {
        let submission =
            match self
                .writer
                .submit_append_running_event(identity, event.into_payload(), deadline)
            {
                Ok(submission) => submission,
                Err(error) => {
                    tracing::error!(%task_id, %error, "runner event submission failed");
                    if let Some(active) = self.active.get_mut(&task_id) {
                        active.durable_sequence_blocked = true;
                        if let Some(response) = active
                            .pending_runner_event_writes
                            .get_mut(&logical_id)
                            .and_then(|pending| pending.response.take())
                        {
                            let _ = response.send(Err(RunnerEventError::StoreDegraded));
                        }
                    }
                    if self
                        .settle_deferred_running_mutations(task_id, operation_nonce)
                        .is_none()
                    {
                        self.freeze_degraded();
                        return;
                    }
                    self.freeze_degraded();
                    return;
                }
            };
        let completion_sender = self.completion_sender.clone();
        tokio::spawn(async move {
            let completion = submission.completion().await;
            let _ = completion_sender
                .send(TaskManagerCompletion::RunnerEventPersisted {
                    task_id,
                    operation_nonce,
                    logical_id,
                    identity,
                    completion,
                })
                .await;
        });
    }

    pub(super) fn handle_runner_event_persisted(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        logical_id: u64,
        identity: TaskMutationIdentity,
        completion: DurableCompletion<AppendEventOutcome>,
    ) {
        let current_is_exact = self.active.get(&task_id).is_some_and(|active| {
            active.operation_nonce == operation_nonce
                && active
                    .pending_runner_event_writes
                    .get(&logical_id)
                    .is_some_and(|pending| {
                        matches!(
                            pending.stage,
                            RunnerEventWriteStage::Submitted {
                                identity: owned_identity,
                            } if owned_identity == identity
                        )
                    })
        });
        if !current_is_exact
            || !self.running_mutation_completion_is_current(task_id, operation_nonce, identity)
            || identity.kind != DurableOperationKind::AppendRunningEvent
            || completion.identity != DurableOperationIdentity::TaskMutation(identity)
        {
            self.freeze_degraded();
            if let Some(response) = self
                .active
                .get_mut(&task_id)
                .and_then(|active| active.pending_runner_event_writes.get_mut(&logical_id))
                .and_then(|pending| pending.response.take())
            {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
            return;
        }
        let result = match (completion.sequence_disposition, completion.disposition) {
            (
                MutationSequenceDisposition::AdvanceNext,
                DurableDisposition::Confirmed(AppendEventOutcome::Applied { event_id }),
            ) => Ok(event_id),
            (
                MutationSequenceDisposition::AdvanceNext,
                DurableDisposition::KnownNotApplied {
                    outcome: Some(AppendEventOutcome::NotRunning { .. }),
                    ..
                },
            ) => Err(RunnerEventError::TaskNotRunning),
            _ => Err(RunnerEventError::StoreDegraded),
        };
        let Some(mut pending) = self
            .active
            .get_mut(&task_id)
            .and_then(|active| active.pending_runner_event_writes.remove(&logical_id))
        else {
            self.freeze_degraded();
            return;
        };
        let mut continuation = match self.complete_running_mutation(task_id, operation_nonce) {
            Some(continuation) => continuation,
            None => {
                self.freeze_degraded();
                if let Some(response) = pending.response.take() {
                    let _ = response.send(Err(RunnerEventError::StoreDegraded));
                }
                return;
            }
        };
        if result.is_err() {
            let Some(settled_continuation) =
                self.settle_deferred_running_mutations(task_id, operation_nonce)
            else {
                self.freeze_degraded();
                if let Some(response) = pending.response.take() {
                    let _ = response.send(Err(RunnerEventError::StoreDegraded));
                }
                return;
            };
            if settled_continuation.is_some() {
                continuation = settled_continuation;
            }
        }
        if result == Err(RunnerEventError::StoreDegraded) {
            tracing::error!(%task_id, "runner event persistence was not confirmed");
            if let Some(active) = self.active.get_mut(&task_id) {
                active.durable_sequence_blocked = true;
            }
            self.enter_degraded(None);
        }
        if let Some(response) = pending.response.take() {
            let _ = response.send(result);
        }
        self.continue_after_running_mutation(task_id, operation_nonce, continuation);
    }

    pub(super) fn submit_runner_review(
        &mut self,
        request: RecordReviewRequest,
        response: oneshot::Sender<Result<EventId, RunnerEventError>>,
    ) {
        if self.is_frozen()
            || self.degraded
            || self.service_state.current().state != ServiceState::Ready
        {
            let _ = response.send(Err(RunnerEventError::StoreDegraded));
            return;
        }
        let Some((operation_nonce, next_in_flight)) =
            self.active.get(&request.task_id).and_then(|active| {
                let valid = active.repository_id == request.expected_repository_id
                    && active.attempt == request.expected_attempt
                    && matches!(&active.stop_state, ActiveStopState::NoWinner)
                    && !active.durable_sequence_blocked
                    && active.phase == AdmissionPhase::Running
                    && active.preparation_complete;
                valid
                    .then(|| {
                        active
                            .in_flight_mutations
                            .checked_add(1)
                            .map(|count| (active.operation_nonce, count))
                    })
                    .flatten()
            })
        else {
            let _ = response.send(Err(RunnerEventError::TaskNotRunning));
            return;
        };
        let deadline = self.current_persistence_deadline();
        if self.is_frozen() || Instant::now() >= deadline {
            let _ = response.send(Err(RunnerEventError::StoreDegraded));
            return;
        }
        let task_id = request.task_id;
        let Some(active) = self.active.get_mut(&task_id) else {
            self.freeze_degraded();
            let _ = response.send(Err(RunnerEventError::StoreDegraded));
            return;
        };
        let lineage_id = active.next_runner_mutation_id;
        let Some(next_lineage_id) = lineage_id.checked_add(1) else {
            self.freeze_degraded();
            let _ = response.send(Err(RunnerEventError::StoreDegraded));
            return;
        };
        if active.operation_nonce != operation_nonce {
            self.freeze_degraded();
            let _ = response.send(Err(RunnerEventError::StoreDegraded));
            return;
        }
        active.next_runner_mutation_id = next_lineage_id;
        if active
            .pending_record_review_writes
            .insert(
                lineage_id,
                PendingRecordReviewWrite {
                    stage: RecordReviewWriteStage::Deferred,
                    request,
                    deadline,
                    response: Some(response),
                },
            )
            .is_some()
        {
            self.freeze_degraded();
            return;
        }
        active.in_flight_mutations = next_in_flight;
        self.drive_next_running_mutation(task_id, operation_nonce);
    }

    pub(super) fn drive_next_running_mutation(&mut self, task_id: TaskId, operation_nonce: u64) {
        enum Candidate {
            Event(u64, Instant),
            Review(u64, Instant),
        }

        let candidate = self.active.get(&task_id).and_then(|active| {
            if active.operation_nonce != operation_nonce
                || active.durable_sequence_blocked
                || !active.pending_record_review_replays.is_empty()
                || active
                    .pending_runner_event_writes
                    .values()
                    .any(|pending| matches!(pending.stage, RunnerEventWriteStage::Submitted { .. }))
                || active.pending_record_review_writes.values().any(|pending| {
                    matches!(pending.stage, RecordReviewWriteStage::Submitted { .. })
                })
            {
                return None;
            }
            let review_owned = active
                .pending_runner_event_writes
                .len()
                .checked_add(active.pending_record_review_writes.len())?
                .checked_add(active.pending_record_review_replays.len())?;
            if active.in_flight_mutations != review_owned {
                return None;
            }
            let event = active
                .pending_runner_event_writes
                .iter()
                .filter(|(_, pending)| matches!(pending.stage, RunnerEventWriteStage::Deferred(_)))
                .map(|(logical_id, pending)| (*logical_id, pending.deadline))
                .min_by_key(|(logical_id, _)| *logical_id);
            let review = active
                .pending_record_review_writes
                .iter()
                .filter(|(_, pending)| pending.stage == RecordReviewWriteStage::Deferred)
                .min_by_key(|(lineage_id, _)| *lineage_id)
                .map(|(lineage_id, pending)| (*lineage_id, pending.deadline));
            match (event, review) {
                (Some((logical_id, event_deadline)), Some((lineage_id, deadline))) => {
                    if logical_id < lineage_id {
                        Some(Candidate::Event(logical_id, event_deadline))
                    } else {
                        Some(Candidate::Review(lineage_id, deadline))
                    }
                }
                (Some((logical_id, deadline)), None) => {
                    Some(Candidate::Event(logical_id, deadline))
                }
                (None, Some((lineage_id, deadline))) => {
                    Some(Candidate::Review(lineage_id, deadline))
                }
                (None, None) => None,
            }
        });
        let Some(candidate) = candidate else {
            return;
        };

        match candidate {
            Candidate::Event(logical_id, deadline) => {
                if self.frozen || Instant::now() >= deadline {
                    self.fail_deferred_running_mutations(task_id, operation_nonce);
                    return;
                }
                let Some(identity) =
                    self.next_mutation_identity(task_id, DurableOperationKind::AppendRunningEvent)
                else {
                    self.freeze_degraded();
                    return;
                };
                let event = self.active.get_mut(&task_id).and_then(|active| {
                    let pending = active.pending_runner_event_writes.get_mut(&logical_id)?;
                    if active.operation_nonce != operation_nonce {
                        return None;
                    }
                    let stage = std::mem::replace(
                        &mut pending.stage,
                        RunnerEventWriteStage::Submitted { identity },
                    );
                    match stage {
                        RunnerEventWriteStage::Deferred(event) => Some(event),
                        RunnerEventWriteStage::Submitted { .. } => None,
                    }
                });
                let Some(event) = event else {
                    self.freeze_degraded();
                    return;
                };
                self.submit_runner_event_write(
                    task_id,
                    operation_nonce,
                    logical_id,
                    identity,
                    event,
                    deadline,
                );
            }
            Candidate::Review(lineage_id, deadline) => {
                if self.frozen || Instant::now() >= deadline {
                    self.fail_deferred_running_mutations(task_id, operation_nonce);
                    return;
                }
                let Some(attempt_id) = self.next_typed_write_attempt_id() else {
                    self.freeze_degraded();
                    return;
                };
                let Some(identity) =
                    self.next_mutation_identity(task_id, DurableOperationKind::RecordReview)
                else {
                    self.freeze_degraded();
                    return;
                };
                let replaced = self.active.get_mut(&task_id).is_some_and(|active| {
                    let Some(pending) = active.pending_record_review_writes.get_mut(&lineage_id)
                    else {
                        return false;
                    };
                    if active.operation_nonce != operation_nonce
                        || pending.stage != RecordReviewWriteStage::Deferred
                    {
                        return false;
                    }
                    pending.stage = RecordReviewWriteStage::Submitted {
                        attempt_id,
                        identity,
                        retry_available: true,
                    };
                    true
                });
                if !replaced {
                    self.freeze_degraded();
                    return;
                }
                self.submit_record_review_write(task_id, operation_nonce, lineage_id);
            }
        }
    }

    fn submit_record_review_write(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        lineage_id: u64,
    ) {
        let Some((attempt_id, identity, request, deadline)) =
            self.active.get(&task_id).and_then(|active| {
                active
                    .pending_record_review_writes
                    .get(&lineage_id)
                    .and_then(|pending| {
                        let RecordReviewWriteStage::Submitted {
                            attempt_id,
                            identity,
                            ..
                        } = pending.stage
                        else {
                            return None;
                        };
                        Some((
                            attempt_id,
                            identity,
                            pending.request.clone(),
                            pending.deadline,
                        ))
                    })
            })
        else {
            self.freeze_degraded();
            return;
        };
        let submission = match self
            .writer
            .submit_record_review(identity, request, deadline)
        {
            Ok(submission) => submission,
            Err(error) => {
                tracing::error!(
                    %task_id,
                    %error,
                    "runner review submission violated the mutation sequence contract"
                );
                if let Some(active) = self.active.get_mut(&task_id) {
                    active.durable_sequence_blocked = true;
                    if let Some(response) = active
                        .pending_record_review_writes
                        .get_mut(&lineage_id)
                        .and_then(|pending| pending.response.take())
                    {
                        let _ = response.send(Err(RunnerEventError::StoreDegraded));
                    }
                }
                if self
                    .settle_deferred_running_mutations(task_id, operation_nonce)
                    .is_none()
                {
                    self.freeze_degraded();
                    return;
                }
                self.freeze_degraded();
                return;
            }
        };
        let completion_sender = self.completion_sender.clone();
        tokio::spawn(async move {
            let completion = submission.completion().await;
            let _ = completion_sender
                .send(TaskManagerCompletion::RunnerReviewPersisted {
                    task_id,
                    operation_nonce,
                    lineage_id,
                    attempt_id,
                    identity,
                    completion,
                })
                .await;
        });
    }

    pub(super) fn current_typed_write_attempt_owner(
        &self,
        attempt_id: u64,
    ) -> Result<Option<TypedWriteAttemptOwner>, ()> {
        let mut owner = None;
        for (owned_task_id, active) in &self.active {
            if let Some(pending) = active
                .pending_terminal_write
                .as_ref()
                .filter(|pending| pending.attempt_id == attempt_id)
                && owner
                    .replace(TypedWriteAttemptOwner::Terminal {
                        task_id: *owned_task_id,
                        operation_nonce: active.operation_nonce,
                        identity: pending.identity,
                        stage: pending.stage,
                    })
                    .is_some()
            {
                return Err(());
            }
            for (owned_lineage_id, pending) in &active.pending_record_review_writes {
                let RecordReviewWriteStage::Submitted {
                    attempt_id: owned_attempt_id,
                    identity: owned_identity,
                    ..
                } = pending.stage
                else {
                    continue;
                };
                if owned_attempt_id != attempt_id {
                    continue;
                }
                if owner
                    .replace(TypedWriteAttemptOwner::RecordReview {
                        task_id: *owned_task_id,
                        operation_nonce: active.operation_nonce,
                        lineage_id: *owned_lineage_id,
                        identity: owned_identity,
                    })
                    .is_some()
                {
                    return Err(());
                }
            }
            for (owned_identity, pending) in &active.pending_record_review_replays {
                if pending.attempt_id != attempt_id {
                    continue;
                }
                if owner
                    .replace(TypedWriteAttemptOwner::RecordReview {
                        task_id: *owned_task_id,
                        operation_nonce: active.operation_nonce,
                        lineage_id: pending.lineage_id,
                        identity: *owned_identity,
                    })
                    .is_some()
                {
                    return Err(());
                }
            }
        }
        Ok(owner)
    }

    fn current_typed_write_attempt_matches_record_review(
        &self,
        task_id: TaskId,
        operation_nonce: u64,
        lineage_id: u64,
        attempt_id: u64,
        identity: TaskMutationIdentity,
    ) -> Option<bool> {
        match self.current_typed_write_attempt_owner(attempt_id) {
            Ok(None) => None,
            Ok(Some(TypedWriteAttemptOwner::RecordReview {
                task_id: owned_task_id,
                operation_nonce: owned_operation_nonce,
                lineage_id: owned_lineage_id,
                identity: owned_identity,
            })) => Some(
                owned_task_id == task_id
                    && owned_operation_nonce == operation_nonce
                    && owned_lineage_id == lineage_id
                    && owned_identity == identity,
            ),
            Ok(Some(TypedWriteAttemptOwner::Terminal { .. })) | Err(()) => Some(false),
        }
    }

    // The completion channel owns this fixed identity tuple; grouping it before
    // dispatch would add a second wire representation for the same receipt.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn handle_runner_review_persisted(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        lineage_id: u64,
        attempt_id: u64,
        identity: TaskMutationIdentity,
        completion: DurableCompletion<RecordReviewOutcome>,
        mut observer: RunnerReviewObserver,
    ) {
        let key = RunnerReviewCompletionKey {
            task_id,
            operation_nonce,
            lineage_id,
            attempt_id,
            identity,
        };
        if self.runner_review_attempt_conflicts(key) {
            self.freeze_and_reject_runner_review_observer(&mut observer);
            return;
        }
        if self.respond_to_applied_runner_review(key, &completion, &mut observer) {
            return;
        }

        let Some(current) = self.current_runner_review_write(key) else {
            self.handle_late_record_review_original(
                task_id,
                operation_nonce,
                lineage_id,
                attempt_id,
                identity,
                completion,
                observer,
            );
            return;
        };
        if current.attempt_id != attempt_id {
            reject_runner_review_observer(&mut observer);
            return;
        }
        if !self.runner_review_completion_matches_current(key, &current, &completion) {
            self.freeze_and_reject_runner_review_observer(&mut observer);
            return;
        }

        if completion.sequence_disposition == MutationSequenceDisposition::AdvanceNext
            && let DurableDisposition::Confirmed(outcome) = completion.disposition
        {
            self.apply_confirmed_runner_review(key, current, outcome, observer);
            return;
        }
        self.handle_unconfirmed_runner_review(key, current, completion, observer);
    }

    fn runner_review_attempt_conflicts(&self, key: RunnerReviewCompletionKey) -> bool {
        matches!(
            self.current_typed_write_attempt_matches_record_review(
                key.task_id,
                key.operation_nonce,
                key.lineage_id,
                key.attempt_id,
                key.identity,
            ),
            Some(false)
        )
    }

    fn respond_to_applied_runner_review(
        &mut self,
        key: RunnerReviewCompletionKey,
        completion: &DurableCompletion<RecordReviewOutcome>,
        observer: &mut RunnerReviewObserver,
    ) -> bool {
        let Some(applied) = self
            .active
            .get(&key.task_id)
            .and_then(|active| active.applied_record_reviews.get(&key.identity))
            .cloned()
        else {
            return false;
        };
        let exact = key.identity.task_id == key.task_id
            && key.identity.kind == DurableOperationKind::RecordReview
            && completion.identity == DurableOperationIdentity::TaskMutation(key.identity)
            && completion.sequence_disposition == MutationSequenceDisposition::AdvanceNext
            && matches!(
                &completion.disposition,
                DurableDisposition::Confirmed(outcome)
                    if record_review_outcome(&applied.request, outcome) == Some(applied.event_id)
            );
        if exact {
            if let Some(response) = observer.take() {
                let _ = response.send(Ok(applied.event_id));
            }
        } else {
            self.freeze_and_reject_runner_review_observer(observer);
        }
        true
    }

    fn current_runner_review_write(
        &self,
        key: RunnerReviewCompletionKey,
    ) -> Option<CurrentRunnerReviewWrite> {
        self.active
            .get(&key.task_id)?
            .pending_record_review_writes
            .get(&key.lineage_id)
            .and_then(|pending| {
                let RecordReviewWriteStage::Submitted {
                    attempt_id,
                    identity,
                    retry_available,
                } = pending.stage
                else {
                    return None;
                };
                Some(CurrentRunnerReviewWrite {
                    attempt_id,
                    identity,
                    request: pending.request.clone(),
                    deadline: pending.deadline,
                    retry_available,
                })
            })
    }

    fn runner_review_completion_matches_current(
        &self,
        key: RunnerReviewCompletionKey,
        current: &CurrentRunnerReviewWrite,
        completion: &DurableCompletion<RecordReviewOutcome>,
    ) -> bool {
        self.running_mutation_completion_is_current(key.task_id, key.operation_nonce, key.identity)
            && key.identity.kind == DurableOperationKind::RecordReview
            && current.identity == key.identity
            && completion.identity == DurableOperationIdentity::TaskMutation(key.identity)
    }

    fn apply_confirmed_runner_review(
        &mut self,
        key: RunnerReviewCompletionKey,
        current: CurrentRunnerReviewWrite,
        outcome: RecordReviewOutcome,
        mut observer: RunnerReviewObserver,
    ) {
        let Some(event_id) = record_review_outcome(&current.request, &outcome) else {
            self.freeze_and_reject_runner_review_observer(&mut observer);
            return;
        };
        let Some(active) = self.active.get_mut(&key.task_id) else {
            self.freeze_and_reject_runner_review_observer(&mut observer);
            return;
        };
        let Some(mut pending_write) = active.pending_record_review_writes.remove(&key.lineage_id)
        else {
            self.freeze_and_reject_runner_review_observer(&mut observer);
            return;
        };
        let expected_stage = RecordReviewWriteStage::Submitted {
            attempt_id: key.attempt_id,
            identity: key.identity,
            retry_available: current.retry_available,
        };
        if pending_write.stage != expected_stage || pending_write.request != current.request {
            self.freeze_degraded();
            if let Some(response) = pending_write.response.take() {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
            reject_runner_review_observer(&mut observer);
            return;
        }
        if active
            .applied_record_reviews
            .insert(
                key.identity,
                AppliedRecordReview {
                    request: current.request.clone(),
                    event_id,
                },
            )
            .is_some()
        {
            self.freeze_degraded();
            if let Some(response) = pending_write.response.take() {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
            reject_runner_review_observer(&mut observer);
            return;
        }

        let pending = PendingDurableResult::RecordReview {
            identity: key.identity,
            request: current.request,
        };
        if !self.resolve_canonical_pending_from_original(
            pending,
            PendingReplayReceipt::RecordReview(outcome),
        ) {
            self.freeze_degraded();
            if let Some(response) = pending_write.response.take() {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
            reject_runner_review_observer(&mut observer);
            return;
        }
        let Some(continuation) = self.complete_running_mutation(key.task_id, key.operation_nonce)
        else {
            self.freeze_degraded();
            if let Some(response) = pending_write.response.take() {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
            reject_runner_review_observer(&mut observer);
            return;
        };
        if let Some(response) = pending_write.response.take() {
            let _ = response.send(Ok(event_id));
        }
        if let Some(response) = observer.take() {
            let _ = response.send(Ok(event_id));
        }
        self.continue_after_running_mutation(key.task_id, key.operation_nonce, continuation);
    }

    fn handle_unconfirmed_runner_review(
        &mut self,
        key: RunnerReviewCompletionKey,
        current: CurrentRunnerReviewWrite,
        completion: DurableCompletion<RecordReviewOutcome>,
        mut observer: RunnerReviewObserver,
    ) {
        let pending = PendingDurableResult::RecordReview {
            identity: key.identity,
            request: current.request.clone(),
        };
        let retry_allowed =
            current.retry_available && !self.frozen && Instant::now() < current.deadline;
        let failure = classify_quality_write_failure(&completion, &pending, retry_allowed);
        match failure {
            QualityWriteFailure::RetryNextSequence => {
                self.retry_record_review_write(key.task_id, key.operation_nonce, key.lineage_id);
                reject_runner_review_observer(&mut observer);
            }
            QualityWriteFailure::Replay(returned) => {
                self.stage_runner_review_replay(key, current, pending, returned, observer);
            }
            failure => {
                self.finish_failed_runner_review(key, failure, observer);
            }
        }
    }

    fn stage_runner_review_replay(
        &mut self,
        key: RunnerReviewCompletionKey,
        current: CurrentRunnerReviewWrite,
        pending: PendingDurableResult,
        returned: PendingDurableResult,
        mut observer: RunnerReviewObserver,
    ) {
        if returned != pending {
            self.freeze_and_reject_runner_review_observer(&mut observer);
            return;
        }
        let Some(active) = self.active.get_mut(&key.task_id) else {
            self.freeze_and_reject_runner_review_observer(&mut observer);
            return;
        };
        let Some(mut pending_write) = active.pending_record_review_writes.remove(&key.lineage_id)
        else {
            self.freeze_and_reject_runner_review_observer(&mut observer);
            return;
        };
        if active.operation_nonce != key.operation_nonce
            || !matches!(
                pending_write.stage,
                RecordReviewWriteStage::Submitted {
                    attempt_id: owned_attempt_id,
                    identity: owned_identity,
                    ..
                } if owned_attempt_id == key.attempt_id && owned_identity == key.identity
            )
            || pending_write.request != current.request
            || active
                .pending_record_review_replays
                .contains_key(&key.identity)
        {
            self.freeze_degraded();
            if let Some(response) = pending_write.response.take() {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
            reject_runner_review_observer(&mut observer);
            return;
        }
        active.pending_record_review_replays.insert(
            key.identity,
            PendingRecordReviewReplay {
                lineage_id: key.lineage_id,
                attempt_id: key.attempt_id,
                operation_nonce: key.operation_nonce,
                request: current.request,
                deadline: current.deadline,
                response: pending_write.response.take(),
                deferred_original: None,
                deferred_observers: Vec::new(),
            },
        );
        active.durable_sequence_blocked = true;
        self.enter_degraded(Some(returned));
        reject_runner_review_observer(&mut observer);
    }

    fn finish_failed_runner_review(
        &mut self,
        key: RunnerReviewCompletionKey,
        failure: QualityWriteFailure,
        mut observer: RunnerReviewObserver,
    ) {
        let response = self
            .active
            .get_mut(&key.task_id)
            .and_then(|active| active.pending_record_review_writes.remove(&key.lineage_id))
            .and_then(|mut pending| pending.response.take());
        let mut continuation = self.complete_running_mutation(key.task_id, key.operation_nonce);
        let Some(settled_continuation) =
            self.settle_deferred_running_mutations(key.task_id, key.operation_nonce)
        else {
            self.freeze_degraded();
            if let Some(response) = response {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
            reject_runner_review_observer(&mut observer);
            return;
        };
        if settled_continuation.is_some() {
            continuation = Some(settled_continuation);
        }
        self.apply_quality_write_failure(
            key.task_id,
            "runner review persistence was not confirmed",
            failure,
        );
        if let Some(response) = response {
            let _ = response.send(Err(RunnerEventError::StoreDegraded));
        }
        reject_runner_review_observer(&mut observer);
        if let Some(continuation) = continuation {
            self.continue_after_running_mutation(key.task_id, key.operation_nonce, continuation);
        } else {
            self.freeze_degraded();
        }
    }

    fn freeze_and_reject_runner_review_observer(&mut self, observer: &mut RunnerReviewObserver) {
        self.freeze_degraded();
        reject_runner_review_observer(observer);
    }

    fn retry_record_review_write(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        lineage_id: u64,
    ) {
        let decision_time = Instant::now();
        let allowed = self.active.get(&task_id).is_some_and(|active| {
            active.operation_nonce == operation_nonce
                && active
                    .pending_record_review_writes
                    .get(&lineage_id)
                    .is_some_and(|pending| {
                        matches!(
                            pending.stage,
                            RecordReviewWriteStage::Submitted {
                                retry_available: true,
                                ..
                            }
                        ) && !self.frozen
                            && decision_time < pending.deadline
                    })
        });
        if !allowed {
            self.freeze_degraded();
            return;
        }
        let Some(attempt_id) = self.next_typed_write_attempt_id() else {
            self.freeze_degraded();
            return;
        };
        let Some(identity) =
            self.next_mutation_identity(task_id, DurableOperationKind::RecordReview)
        else {
            self.freeze_degraded();
            return;
        };
        let replaced = self.active.get_mut(&task_id).is_some_and(|active| {
            let Some(pending) = active.pending_record_review_writes.get_mut(&lineage_id) else {
                return false;
            };
            if active.operation_nonce != operation_nonce
                || self.frozen
                || decision_time >= pending.deadline
                || !matches!(
                    pending.stage,
                    RecordReviewWriteStage::Submitted {
                        retry_available: true,
                        ..
                    }
                )
            {
                return false;
            }
            pending.stage = RecordReviewWriteStage::Submitted {
                attempt_id,
                identity,
                retry_available: false,
            };
            true
        });
        if !replaced {
            self.freeze_degraded();
            return;
        }
        self.submit_record_review_write(task_id, operation_nonce, lineage_id);
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_late_record_review_original(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        lineage_id: u64,
        attempt_id: u64,
        identity: TaskMutationIdentity,
        completion: DurableCompletion<RecordReviewOutcome>,
        mut observer_response: Option<oneshot::Sender<Result<EventId, RunnerEventError>>>,
    ) {
        let staged_is_current = self.active.get(&task_id).is_some_and(|active| {
            active.operation_nonce == operation_nonce
                && active.in_flight_mutations > 0
                && active
                    .pending_record_review_replays
                    .get(&identity)
                    .is_some_and(|staged| {
                        staged.lineage_id == lineage_id
                            && staged.attempt_id == attempt_id
                            && staged.operation_nonce == operation_nonce
                    })
        });
        if !staged_is_current {
            if self
                .current_typed_write_attempt_matches_record_review(
                    task_id,
                    operation_nonce,
                    lineage_id,
                    attempt_id,
                    identity,
                )
                .is_some()
            {
                self.freeze_degraded();
            }
            if let Some(response) = observer_response.take() {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
            return;
        }
        let request = self
            .active
            .get(&task_id)
            .and_then(|active| active.pending_record_review_replays.get(&identity))
            .map(|staged| staged.request.clone())
            .expect("checked staged review remains actor-owned");
        let exact_outcome = match (completion.sequence_disposition, completion.disposition) {
            (MutationSequenceDisposition::AdvanceNext, DurableDisposition::Confirmed(outcome))
                if completion.identity == DurableOperationIdentity::TaskMutation(identity)
                    && identity.kind == DurableOperationKind::RecordReview
                    && record_review_outcome(&request, &outcome).is_some() =>
            {
                outcome
            }
            _ => {
                self.freeze_degraded();
                if let Some(response) = observer_response.take() {
                    let _ = response.send(Err(RunnerEventError::StoreDegraded));
                }
                return;
            }
        };
        let pending = PendingDurableResult::RecordReview { identity, request };

        match self.canonical_pending_state(&pending) {
            Some(CanonicalPendingState::Blocked) => {
                let receipt = PendingReplayReceipt::RecordReview(exact_outcome.clone());
                let conflict = self
                    .active
                    .get_mut(&task_id)
                    .and_then(|active| active.pending_record_review_replays.get_mut(&identity))
                    .is_none_or(|staged| {
                        if let Some(deferred) = &staged.deferred_original {
                            !pending_replay_receipts_are_equivalent(
                                &pending,
                                &PendingReplayReceipt::RecordReview(deferred.clone()),
                                &receipt,
                            )
                        } else {
                            staged.deferred_original = Some(exact_outcome);
                            false
                        }
                    });
                if conflict {
                    self.freeze_pending_replay(&pending);
                    if let Some(response) = observer_response.take() {
                        let _ = response.send(Err(RunnerEventError::StoreDegraded));
                    }
                    return;
                }
                if let Some(response) = observer_response.take()
                    && let Some(staged) = self
                        .active
                        .get_mut(&task_id)
                        .and_then(|active| active.pending_record_review_replays.get_mut(&identity))
                {
                    staged.deferred_observers.push(response);
                }
                return;
            }
            Some(CanonicalPendingState::Ready) => {}
            Some(CanonicalPendingState::Absent) | None => {
                self.freeze_pending_replay(&pending);
                if let Some(response) = observer_response.take() {
                    let _ = response.send(Err(RunnerEventError::StoreDegraded));
                }
                return;
            }
        }

        if !self.apply_ready_record_review_original(
            &pending,
            exact_outcome,
            observer_response.take(),
        ) {
            self.freeze_pending_replay(&pending);
            return;
        }
        self.drain_deferred_record_review_originals();
    }

    fn apply_ready_record_review_original(
        &mut self,
        pending: &PendingDurableResult,
        exact_outcome: RecordReviewOutcome,
        mut observer_response: Option<oneshot::Sender<Result<EventId, RunnerEventError>>>,
    ) -> bool {
        let PendingDurableResult::RecordReview { identity, request } = pending else {
            if let Some(response) = observer_response.take() {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
            return false;
        };
        let Some(event_id) = record_review_outcome(request, &exact_outcome) else {
            if let Some(response) = observer_response.take() {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
            return false;
        };
        if self.canonical_pending_state(pending) != Some(CanonicalPendingState::Ready) {
            if let Some(response) = observer_response.take() {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
            return false;
        }
        let Some((operation_nonce, deferred_is_exact)) =
            self.active.get(&identity.task_id).and_then(|active| {
                let staged = active.pending_record_review_replays.get(identity)?;
                let deferred_is_exact = staged.deferred_original.as_ref().is_none_or(|deferred| {
                    pending_replay_receipts_are_equivalent(
                        pending,
                        &PendingReplayReceipt::RecordReview(deferred.clone()),
                        &PendingReplayReceipt::RecordReview(exact_outcome.clone()),
                    )
                });
                (staged.request == *request
                    && staged.operation_nonce == active.operation_nonce
                    && active.in_flight_mutations > 0
                    && !active.applied_record_reviews.contains_key(identity))
                .then_some((staged.operation_nonce, deferred_is_exact))
            })
        else {
            if let Some(response) = observer_response.take() {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
            return false;
        };
        if !deferred_is_exact
            || !self.resolve_canonical_pending_from_original(
                pending.clone(),
                PendingReplayReceipt::RecordReview(exact_outcome),
            )
        {
            if let Some(response) = observer_response.take() {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
            return false;
        }

        let Some(active) = self.active.get_mut(&identity.task_id) else {
            if let Some(response) = observer_response.take() {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
            return false;
        };
        let Some(mut staged) = active.pending_record_review_replays.remove(identity) else {
            if let Some(response) = observer_response.take() {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
            return false;
        };
        active.applied_record_reviews.insert(
            *identity,
            AppliedRecordReview {
                request: request.clone(),
                event_id,
            },
        );
        let Some(continuation) = self.complete_running_mutation(identity.task_id, operation_nonce)
        else {
            if let Some(response) = observer_response.take() {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
            return false;
        };
        if let Some(response) = staged.response.take() {
            let _ = response.send(Ok(event_id));
        }
        for response in std::mem::take(&mut staged.deferred_observers) {
            let _ = response.send(Ok(event_id));
        }
        if let Some(response) = observer_response.take() {
            let _ = response.send(Ok(event_id));
        }
        self.continue_after_running_mutation(identity.task_id, operation_nonce, continuation);
        true
    }

    pub(super) fn drain_deferred_record_review_originals(&mut self) {
        loop {
            let Some(pending @ PendingDurableResult::RecordReview { identity, .. }) =
                self.pending_durable_results.first().cloned()
            else {
                return;
            };
            let Some(exact_outcome) = self
                .active
                .get(&identity.task_id)
                .and_then(|active| active.pending_record_review_replays.get(&identity))
                .and_then(|staged| staged.deferred_original.clone())
            else {
                return;
            };
            if !self.apply_ready_record_review_original(&pending, exact_outcome, None) {
                self.freeze_pending_replay(&pending);
                return;
            }
        }
    }

    fn running_mutation_completion_is_current(
        &self,
        task_id: TaskId,
        operation_nonce: u64,
        identity: TaskMutationIdentity,
    ) -> bool {
        identity.task_id == task_id
            && self.active.get(&task_id).is_some_and(|active| {
                active.operation_nonce == operation_nonce && active.in_flight_mutations > 0
            })
    }

    pub(super) fn complete_running_mutation(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
    ) -> Option<Option<RunnerOutcome>> {
        let active = self.active.get_mut(&task_id)?;
        if active.operation_nonce != operation_nonce || active.in_flight_mutations == 0 {
            return None;
        }
        active.in_flight_mutations -= 1;
        Some(
            if active.in_flight_mutations == 0 && active.cleanup_confirmation.is_some() {
                active.pending_runner_outcome.take()
            } else {
                None
            },
        )
    }

    fn settle_deferred_running_mutations(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
    ) -> Option<Option<RunnerOutcome>> {
        let active = self.active.get_mut(&task_id)?;
        if active.operation_nonce != operation_nonce {
            return None;
        }
        let deferred_event_ids = active
            .pending_runner_event_writes
            .iter()
            .filter_map(|(logical_id, pending)| {
                matches!(pending.stage, RunnerEventWriteStage::Deferred(_)).then_some(*logical_id)
            })
            .collect::<Vec<_>>();
        let deferred_review_ids = active
            .pending_record_review_writes
            .iter()
            .filter_map(|(lineage_id, pending)| {
                (pending.stage == RecordReviewWriteStage::Deferred).then_some(*lineage_id)
            })
            .collect::<Vec<_>>();
        let settled_count = deferred_event_ids
            .len()
            .checked_add(deferred_review_ids.len())?;
        if settled_count > active.in_flight_mutations {
            return None;
        }
        for logical_id in deferred_event_ids {
            if let Some(mut pending) = active.pending_runner_event_writes.remove(&logical_id)
                && let Some(response) = pending.response.take()
            {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
        }
        for lineage_id in deferred_review_ids {
            if let Some(mut pending) = active.pending_record_review_writes.remove(&lineage_id)
                && let Some(response) = pending.response.take()
            {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
        }
        active.in_flight_mutations -= settled_count;
        Some(
            if active.in_flight_mutations == 0 && active.cleanup_confirmation.is_some() {
                active.pending_runner_outcome.take()
            } else {
                None
            },
        )
    }

    fn fail_deferred_running_mutations(&mut self, task_id: TaskId, operation_nonce: u64) {
        let Some(continuation) = self.settle_deferred_running_mutations(task_id, operation_nonce)
        else {
            self.freeze_degraded();
            return;
        };
        if !self.frozen {
            self.freeze_degraded();
        }
        self.continue_after_running_mutation(task_id, operation_nonce, continuation);
    }

    pub(super) fn continue_after_running_mutation(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        pending_outcome: Option<RunnerOutcome>,
    ) {
        self.handle_critical_wake();
        self.drive_next_running_mutation(task_id, operation_nonce);
        let Some(active) = self.active.get(&task_id) else {
            return;
        };
        if active.operation_nonce != operation_nonce
            || active.phase != AdmissionPhase::RunnerReturned
            || active.in_flight_mutations != 0
            || active.cleanup_confirmation.is_none()
        {
            return;
        }
        let no_stop_winner = matches!(&active.stop_state, ActiveStopState::NoWinner);
        if no_stop_winner && !self.is_frozen() && !self.degraded {
            if let Some(outcome) = pending_outcome {
                self.start_terminal_persistence(task_id, operation_nonce, outcome);
            }
            return;
        }
        if no_stop_winner {
            if let Some(active) = self.active.get_mut(&task_id) {
                active.recovery_release_ready = true;
            }
        } else {
            self.advance_stop_after_barriers(task_id, operation_nonce);
        }
        self.try_start_quiesce_recovery();
        self.maybe_start_degraded_recovery();
    }
}

#[cfg(test)]
mod tests;
