use super::*;

impl TaskManager {
    pub(super) fn maybe_start_degraded_recovery(&mut self) {
        if !self.degraded || self.frozen {
            return;
        }
        if let Some(pending) = self.pending_durable_results.first().cloned() {
            if self.generic_recovery_attempt.is_some() {
                return;
            }
            if let Some(attempt) = self.pending_replay_in_flight.clone() {
                let resolved = self
                    .resolved_pending_replays
                    .get(&attempt.pending.identity())
                    .cloned();
                if resolved
                    .as_ref()
                    .is_some_and(|resolved| resolved.pending != attempt.pending)
                {
                    self.freeze_pending_replay(&attempt.pending);
                    return;
                }
                let attempt_was_resolved_by_original = resolved.is_some();
                if attempt.pending != pending && !attempt_was_resolved_by_original {
                    self.freeze_pending_replay(&pending);
                }
                return;
            }
            let deadline = self.pending_replay_deadline(&pending);
            self.start_pending_replay_attempt(pending, deadline);
            return;
        }
        if self.pending_replay_in_flight.is_some() {
            return;
        }
        if self.pending_quiesce.is_some() {
            self.try_start_quiesce_recovery();
            return;
        }
        if !self.generic_recovery_barriers_clear() || self.generic_recovery_attempt.is_some() {
            return;
        }
        let attempt_id = self.next_generic_recovery_attempt_id;
        let Some(next_attempt_id) = attempt_id.checked_add(1) else {
            self.freeze_degraded();
            return;
        };
        self.next_generic_recovery_attempt_id = next_attempt_id;
        let barrier_epoch = self.exact_barrier_epoch;
        let safety_generation = match self.checked_recovery_safety_gate() {
            RecoverySafetyGate::Exact(safety_generation) => safety_generation,
            RecoverySafetyGate::CriticalPending => {
                self.handle_critical_wake();
                return;
            }
            RecoverySafetyGate::Conflict => {
                self.freeze_degraded();
                return;
            }
        };
        self.generic_recovery_attempt = Some(GenericRecoveryAttempt {
            attempt_id,
            barrier_epoch,
            safety_generation,
        });
        let coordinator = self.coordinator.clone();
        let completion_sender = self.completion_sender.clone();
        let replayed_pending_count = self.degraded_replayed_pending_count;
        let replay_high_watermark = self.degraded_replay_high_watermark;
        tokio::spawn(async move {
            let result = coordinator
                .run_after_replay(
                    attempt_id,
                    barrier_epoch,
                    replayed_pending_count,
                    replay_high_watermark,
                )
                .await;
            if let Err(error) = &result
                && *error != DegradedCoordinatorError::Quiescing
                && *error != DegradedCoordinatorError::Superseded
                && *error != DegradedCoordinatorError::TypedConflict
            {
                tracing::error!(error = %error, "degraded recovery coordinator stopped");
            }
            let _ = completion_sender
                .send(TaskManagerCompletion::GenericRecoveryCompleted {
                    attempt_id,
                    barrier_epoch,
                    result,
                })
                .await;
        });
    }

    pub(super) fn handle_generic_recovery_completed(
        &mut self,
        attempt_id: u64,
        barrier_epoch: u64,
        _result: Result<DegradedRecoveryResult, DegradedCoordinatorError>,
    ) {
        if !self.generic_recovery_attempt_is_exact(attempt_id, barrier_epoch) {
            return;
        }
        self.generic_recovery_attempt = None;
        if self.frozen {
            return;
        }
        self.kick_exact_barrier_progress();
    }

    pub(super) fn pending_replay_deadline(&self, pending: &PendingDurableResult) -> Instant {
        let current = self.current_persistence_deadline();
        match pending {
            PendingDurableResult::RecordReview { identity, .. } => self
                .active
                .get(&identity.task_id)
                .and_then(|active| active.pending_record_review_replays.get(identity))
                .map_or(current, |staged| staged.deadline.min(current)),
            PendingDurableResult::QueueLimitedCreate { .. }
            | PendingDurableResult::QueueLimitedRetry { .. }
            | PendingDurableResult::ClaimTask { .. }
            | PendingDurableResult::PersistStopIntentBatch { .. }
            | PendingDurableResult::FinalizeStoppedTask { .. }
            | PendingDurableResult::FinalizeReviewedTask { .. }
            | PendingDurableResult::FinalizeUnreviewedTask { .. } => current,
        }
    }

    pub(super) fn start_pending_replay_attempt(
        &mut self,
        pending: PendingDurableResult,
        deadline: Instant,
    ) {
        if self.pending_replay_in_flight.is_some() || self.frozen {
            return;
        }
        if Instant::now() >= deadline {
            self.expire_pending_quiesce(Instant::now());
            if !self.frozen {
                self.freeze_pending_replay(&pending);
            }
            return;
        }
        let attempt_id = self.next_pending_replay_attempt_id;
        let Some(next_attempt_id) = attempt_id.checked_add(1) else {
            self.freeze_pending_replay(&pending);
            return;
        };
        self.next_pending_replay_attempt_id = next_attempt_id;
        self.pending_replay_in_flight = Some(PendingReplayAttempt {
            attempt_id,
            pending: pending.clone(),
            deadline,
        });
        let writer = self.writer.clone();
        let completion_sender = self.completion_sender.clone();
        #[cfg(test)]
        let claim_hooks = self.claim_hooks.clone();
        tokio::spawn(async move {
            let result = match writer.reconcile_pending(pending.clone(), deadline) {
                Ok(submission) => Ok(submission.completion().await),
                Err(error) => Err(error),
            };
            #[cfg(test)]
            if matches!(
                &result,
                Ok(completion)
                    if completion.sequence_disposition
                        == MutationSequenceDisposition::AdvanceNext
                        && matches!(
                            &completion.disposition,
                            DurableDisposition::Confirmed(_)
                                | DurableDisposition::KnownNotApplied {
                                    reason: KnownNotAppliedReason::ExactReconciliation,
                                    outcome: Some(_),
                                    error: None,
                                }
                        )
            ) && let Some(hooks) = claim_hooks
            {
                hooks
                    .pause(ClaimPhase::PendingReplayBeforeActorDelivery)
                    .await;
            }
            let _ = completion_sender
                .send(TaskManagerCompletion::PendingReplayCompleted {
                    attempt_id,
                    pending,
                    result,
                })
                .await;
        });
    }

    pub(super) fn handle_pending_replay_completed(
        &mut self,
        attempt_id: u64,
        pending: PendingDurableResult,
        result: Result<DurableCompletion<PendingReplayReceipt>, StoreWriterSubmitError>,
    ) {
        let Some(attempt) = self.pending_replay_in_flight.clone() else {
            return;
        };
        if attempt.attempt_id != attempt_id {
            return;
        }
        if attempt.pending != pending {
            self.freeze_pending_replay(&attempt.pending);
            return;
        }
        let identity = pending.identity();
        if let Some(resolved) = self.resolved_pending_replays.get(&identity).cloned() {
            let exact_late_result = resolved.pending == pending
                && match result {
                    Err(
                        StoreWriterSubmitError::Full
                        | StoreWriterSubmitError::Closed
                        | StoreWriterSubmitError::SequenceReversed,
                    ) => true,
                    Err(
                        StoreWriterSubmitError::InvalidIdentity
                        | StoreWriterSubmitError::SequenceGap,
                    ) => false,
                    Ok(completion) if completion.identity == identity => {
                        match (completion.sequence_disposition, completion.disposition) {
                            (
                                MutationSequenceDisposition::AdvanceNext,
                                DurableDisposition::Confirmed(receipt),
                            )
                            | (
                                MutationSequenceDisposition::AdvanceNext,
                                DurableDisposition::KnownNotApplied {
                                    reason: KnownNotAppliedReason::ExactReconciliation,
                                    outcome: Some(receipt),
                                    error: None,
                                },
                            ) => {
                                pending_replay_receipt_matches(&pending, &receipt)
                                    && pending_replay_receipts_are_equivalent(
                                        &pending,
                                        &resolved.receipt,
                                        &receipt,
                                    )
                            }
                            (
                                MutationSequenceDisposition::BlockUnknown,
                                DurableDisposition::OutcomeUnknown {
                                    pending: returned, ..
                                },
                            ) => returned
                                .as_ref()
                                .is_none_or(|returned| returned == &pending),
                            _ => false,
                        }
                    }
                    Ok(_) => false,
                };
            if !exact_late_result {
                self.freeze_pending_replay(&pending);
                return;
            }
            self.resolved_pending_replays.remove(&identity);
            self.pending_replay_in_flight = None;
            self.maybe_start_degraded_recovery();
            return;
        }
        if self.pending_durable_results.first() != Some(&pending) {
            self.freeze_pending_replay(&pending);
            return;
        }
        let completion = match result {
            Ok(completion) => completion,
            Err(StoreWriterSubmitError::Full | StoreWriterSubmitError::Closed) => {
                self.schedule_pending_replay_retry(attempt_id, attempt.deadline);
                return;
            }
            Err(
                StoreWriterSubmitError::InvalidIdentity
                | StoreWriterSubmitError::SequenceGap
                | StoreWriterSubmitError::SequenceReversed,
            ) => {
                self.freeze_pending_replay(&pending);
                return;
            }
        };
        if completion.identity != pending.identity() {
            self.freeze_pending_replay(&pending);
            return;
        }
        let receipt = match (completion.sequence_disposition, completion.disposition) {
            (MutationSequenceDisposition::AdvanceNext, DurableDisposition::Confirmed(receipt))
            | (
                MutationSequenceDisposition::AdvanceNext,
                DurableDisposition::KnownNotApplied {
                    reason: KnownNotAppliedReason::ExactReconciliation,
                    outcome: Some(receipt),
                    error: None,
                },
            ) => receipt,
            (
                MutationSequenceDisposition::BlockUnknown,
                DurableDisposition::OutcomeUnknown {
                    pending: returned, ..
                },
            ) if returned
                .as_ref()
                .is_none_or(|returned| returned == &pending) =>
            {
                self.schedule_pending_replay_retry(attempt_id, attempt.deadline);
                return;
            }
            _ => {
                self.freeze_pending_replay(&pending);
                return;
            }
        };
        if !pending_replay_receipt_matches(&pending, &receipt)
            || !self.apply_pending_replay(&pending, &receipt)
        {
            self.freeze_pending_replay(&pending);
            return;
        }
        self.pending_replay_in_flight = None;
        self.pending_durable_results.remove(0);
        let task_ids = match &identity {
            DurableOperationIdentity::TaskMutation(identity) => vec![identity.task_id],
            DurableOperationIdentity::StopIntentBatch { items } => {
                items.iter().map(|identity| identity.task_id).collect()
            }
            DurableOperationIdentity::CreateTask { .. }
            | DurableOperationIdentity::RetryTask { .. } => Vec::new(),
        };
        for task_id in &task_ids {
            let remains_blocked = self
                .pending_durable_results
                .iter()
                .any(|pending| durable_identity_contains_task(&pending.identity(), *task_id));
            if let Some(active) = self.active.get_mut(task_id) {
                active.durable_sequence_blocked = remains_blocked;
            }
        }
        self.drain_deferred_record_review_originals();
        for task_id in task_ids {
            if let Some(operation_nonce) = self
                .active
                .get(&task_id)
                .map(|active| active.operation_nonce)
            {
                self.drive_next_running_mutation(task_id, operation_nonce);
            }
        }
        let Some(replayed_pending_count) = self.degraded_replayed_pending_count.checked_add(1)
        else {
            self.freeze_degraded();
            return;
        };
        self.degraded_replayed_pending_count = replayed_pending_count;
        if let Some(event_id) = receipt.event_id() {
            self.degraded_replay_high_watermark = Some(
                self.degraded_replay_high_watermark
                    .map_or(event_id, |current| current.max(event_id)),
            );
        }
        if self.drain_staged_stop_intent_completions() == StopCompletionDrain::Stop {
            return;
        }
        if !self.frozen {
            self.drain_deferred_stop_submissions();
            if !self.frozen {
                self.try_start_quiesce_recovery();
                self.maybe_start_degraded_recovery();
            }
        }
    }

    pub(super) fn freeze_pending_replay(&mut self, pending: &PendingDurableResult) {
        if let PendingDurableResult::RecordReview { identity, .. } = pending
            && let Some(staged) = self
                .active
                .get_mut(&identity.task_id)
                .and_then(|active| active.pending_record_review_replays.get_mut(identity))
        {
            if let Some(response) = staged.response.take() {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
            for response in std::mem::take(&mut staged.deferred_observers) {
                let _ = response.send(Err(RunnerEventError::StoreDegraded));
            }
        }
        self.freeze_degraded();
    }

    pub(super) fn schedule_pending_replay_retry(&mut self, attempt_id: u64, deadline: Instant) {
        let wake_at = (Instant::now() + PENDING_REPLAY_RETRY_INTERVAL).min(deadline);
        let completion_sender = self.completion_sender.clone();
        tokio::spawn(async move {
            tokio::time::sleep_until(wake_at).await;
            let _ = completion_sender
                .send(TaskManagerCompletion::PendingReplayRetry { attempt_id })
                .await;
        });
    }

    pub(super) fn handle_pending_replay_retry(&mut self, attempt_id: u64) {
        let Some(attempt) = self.pending_replay_in_flight.clone() else {
            return;
        };
        if attempt.attempt_id != attempt_id {
            return;
        }
        let identity = attempt.pending.identity();
        if let Some(resolved) = self.resolved_pending_replays.get(&identity).cloned() {
            if resolved.pending != attempt.pending {
                self.freeze_pending_replay(&attempt.pending);
                return;
            }
            self.resolved_pending_replays.remove(&identity);
            self.pending_replay_in_flight = None;
            if !self.frozen {
                self.maybe_start_degraded_recovery();
            }
            return;
        }
        if self.frozen {
            return;
        }
        if Instant::now() >= attempt.deadline {
            self.expire_pending_quiesce(Instant::now());
            if !self.frozen {
                self.freeze_pending_replay(&attempt.pending);
            }
            return;
        }
        if self.pending_durable_results.first() != Some(&attempt.pending) {
            self.freeze_pending_replay(&attempt.pending);
            return;
        }
        self.pending_replay_in_flight = None;
        self.start_pending_replay_attempt(attempt.pending, attempt.deadline);
    }

    pub(super) fn apply_pending_replay(
        &mut self,
        pending: &PendingDurableResult,
        receipt: &PendingReplayReceipt,
    ) -> bool {
        match (pending, receipt) {
            (
                PendingDurableResult::PersistStopIntentBatch { identity, requests },
                PendingReplayReceipt::PersistStopIntentBatch(receipt),
            ) => self.apply_stop_intent_batch_receipt(identity, requests, receipt.clone()),
            (
                PendingDurableResult::FinalizeStoppedTask { identity, request },
                PendingReplayReceipt::FinalizeStoppedTask(outcome),
            ) => self.apply_final_stop_replay(*identity, *request, outcome.clone()),
            (
                PendingDurableResult::RecordReview { identity, request },
                PendingReplayReceipt::RecordReview(outcome),
            ) => self.apply_record_review_replay(*identity, request, outcome),
            _ => true,
        }
    }

    pub(super) fn apply_record_review_replay(
        &mut self,
        identity: TaskMutationIdentity,
        request: &RecordReviewRequest,
        outcome: &RecordReviewOutcome,
    ) -> bool {
        let Some(event_id) = record_review_outcome(request, outcome) else {
            return false;
        };
        let Some(active) = self.active.get(&identity.task_id) else {
            return false;
        };
        if identity.kind != DurableOperationKind::RecordReview {
            return false;
        }
        if let Some(applied) = active.applied_record_reviews.get(&identity) {
            return applied.request == *request && applied.event_id == event_id;
        }
        let Some(staged) = active.pending_record_review_replays.get(&identity) else {
            return false;
        };
        if staged.request != *request
            || staged.operation_nonce != active.operation_nonce
            || active.in_flight_mutations == 0
        {
            return false;
        }
        if let Some(deferred) = &staged.deferred_original {
            let pending = PendingDurableResult::RecordReview {
                identity,
                request: request.clone(),
            };
            if !pending_replay_receipts_are_equivalent(
                &pending,
                &PendingReplayReceipt::RecordReview(deferred.clone()),
                &PendingReplayReceipt::RecordReview(outcome.clone()),
            ) {
                return false;
            }
        }
        let operation_nonce = staged.operation_nonce;
        let Some(active) = self.active.get_mut(&identity.task_id) else {
            return false;
        };
        let Some(mut staged) = active.pending_record_review_replays.remove(&identity) else {
            return false;
        };
        if active
            .applied_record_reviews
            .insert(
                identity,
                AppliedRecordReview {
                    request: request.clone(),
                    event_id,
                },
            )
            .is_some()
        {
            return false;
        }
        let Some(continuation) = self.complete_running_mutation(identity.task_id, operation_nonce)
        else {
            return false;
        };
        if let Some(response) = staged.response.take() {
            let _ = response.send(Ok(event_id));
        }
        for response in std::mem::take(&mut staged.deferred_observers) {
            let _ = response.send(Ok(event_id));
        }
        self.continue_after_running_mutation(identity.task_id, operation_nonce, continuation);
        true
    }
}
