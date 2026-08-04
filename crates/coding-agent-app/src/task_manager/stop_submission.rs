use super::*;

impl TaskManager {
    pub(super) fn drain_deferred_stop_submissions(&mut self) {
        if self.frozen {
            return;
        }
        let mut ready = self
            .active
            .iter()
            .filter_map(|(task_id, active)| {
                let ActiveStopState::IntentSubmissionDeferred {
                    kind,
                    identity,
                    request,
                    deadline,
                    retries_remaining,
                } = &active.stop_state
                else {
                    return None;
                };
                let batch_identity = DurableOperationIdentity::stop_intent_batch(vec![*identity])
                    .expect("one deferred stop identity is a valid batch");
                (!active.durable_sequence_blocked
                    && !self.stop_completion_has_pending_predecessor(&batch_identity))
                .then_some((
                    *task_id,
                    *kind,
                    *identity,
                    *request,
                    *deadline,
                    *retries_remaining,
                ))
            })
            .collect::<Vec<_>>();
        ready.sort_by_key(|(task_id, ..)| task_id.as_uuid().as_u128());
        let mut critical = Vec::new();
        for (task_id, kind, identity, request, deadline, retries_remaining) in ready {
            if Instant::now() >= deadline {
                self.freeze_degraded();
                return;
            }
            let Some(active) = self.active.get_mut(&task_id) else {
                self.freeze_degraded();
                return;
            };
            if !matches!(
                &active.stop_state,
                ActiveStopState::IntentSubmissionDeferred {
                    identity: active_identity,
                    request: active_request,
                    deadline: active_deadline,
                    retries_remaining: active_retries,
                    ..
                } if *active_identity == identity
                    && *active_request == request
                    && *active_deadline == deadline
                    && *active_retries == retries_remaining
            ) {
                self.freeze_degraded();
                return;
            }
            active.stop_state = ActiveStopState::IntentWritePending {
                kind,
                identity,
                request,
                deadline,
                retries_remaining,
            };
            match kind {
                StopIntentKind::UserCancelled => {
                    match self
                        .writer
                        .submit_user_stop_intent(identity, request, deadline)
                    {
                        Ok(submission) => {
                            let completion_sender = self.completion_sender.clone();
                            tokio::spawn(async move {
                                let completion = submission.completion().await;
                                let identity = completion.identity.clone();
                                let _ = completion_sender
                                    .send(TaskManagerCompletion::StopIntentPersisted {
                                        identity,
                                        completion,
                                    })
                                    .await;
                            });
                        }
                        Err(error) => {
                            tracing::error!(
                                %task_id,
                                %error,
                                "deferred user stop-intent submission failed"
                            );
                            self.fail_user_cancel_waiters(task_id, TaskManagerError::StoreDegraded);
                            self.freeze_degraded();
                            return;
                        }
                    }
                }
                StopIntentKind::DiskPressureCritical => {
                    critical.push((identity, request, deadline));
                }
            }
        }
        for chunk in critical.chunks(4) {
            let deadline = chunk
                .iter()
                .map(|(_, _, deadline)| *deadline)
                .min()
                .expect("a deferred critical chunk is non-empty");
            let batch = chunk
                .iter()
                .map(|(identity, request, _)| (*identity, *request))
                .collect();
            self.submit_critical_stop_batch(batch, deadline);
            if self.frozen {
                return;
            }
        }
    }

    pub(super) fn drain_staged_stop_intent_completions(&mut self) -> StopCompletionDrain {
        loop {
            let Some(staged) = self.staged_stop_intent_completions.front() else {
                return StopCompletionDrain::Continue;
            };
            if self.stop_completion_has_pending_predecessor(&staged.identity) {
                return StopCompletionDrain::Continue;
            }
            let staged = self
                .staged_stop_intent_completions
                .pop_front()
                .expect("the staged stop completion was checked above");
            if self.handle_stop_intent_persisted(staged.identity, staged.completion)
                == StopCompletionDrain::Stop
            {
                return StopCompletionDrain::Stop;
            }
        }
    }

    pub(super) fn handle_stop_intent_persisted(
        &mut self,
        identity: DurableOperationIdentity,
        completion: DurableCompletion<StopIntentBatchReceipt>,
    ) -> StopCompletionDrain {
        if completion.identity != identity {
            self.freeze_degraded();
            return StopCompletionDrain::Stop;
        }
        let DurableOperationIdentity::StopIntentBatch { items: identities } = &identity else {
            self.freeze_degraded();
            return StopCompletionDrain::Stop;
        };
        if identities.is_empty()
            || identities.len() > 4
            || identities.windows(2).any(|pair| {
                pair[0].task_id.as_uuid().as_u128() >= pair[1].task_id.as_uuid().as_u128()
            })
        {
            self.freeze_degraded();
            return StopCompletionDrain::Stop;
        }
        match self.stop_completion_ownership(identities) {
            StopCompletionOwnership::FullyAbsent => return StopCompletionDrain::Continue,
            StopCompletionOwnership::Mixed => {
                self.freeze_degraded();
                return StopCompletionDrain::Stop;
            }
            StopCompletionOwnership::FullyOwned => {}
        }
        let mut requests = Vec::with_capacity(identities.len());
        for expected_identity in identities {
            let Some(active) = self.active.get(&expected_identity.task_id) else {
                self.freeze_degraded();
                return StopCompletionDrain::Stop;
            };
            let Some(request) = stop_request_for_completion(active, *expected_identity) else {
                self.freeze_degraded();
                return StopCompletionDrain::Stop;
            };
            requests.push(request);
        }
        if self.stop_completion_has_pending_predecessor(&identity) {
            if self
                .staged_stop_intent_completions
                .iter()
                .any(|staged| staged.identity == identity)
            {
                self.freeze_degraded();
                return StopCompletionDrain::Stop;
            }
            if !self.advance_exact_barrier_epoch() {
                return StopCompletionDrain::Stop;
            }
            self.staged_stop_intent_completions
                .push_back(StagedStopIntentCompletion {
                    identity,
                    completion,
                });
            return StopCompletionDrain::Continue;
        }
        let pending = PendingDurableResult::PersistStopIntentBatch {
            identity: identity.clone(),
            requests: requests.clone(),
        };
        match (completion.sequence_disposition, completion.disposition) {
            (MutationSequenceDisposition::AdvanceNext, DurableDisposition::Confirmed(receipt)) => {
                let replay_receipt = PendingReplayReceipt::PersistStopIntentBatch(receipt.clone());
                if !self.apply_stop_intent_batch_receipt(&identity, &requests, receipt)
                    || !self.resolve_canonical_pending_from_original(pending, replay_receipt)
                {
                    self.freeze_degraded();
                    StopCompletionDrain::Stop
                } else {
                    StopCompletionDrain::Continue
                }
            }
            (
                MutationSequenceDisposition::RetainSame,
                DurableDisposition::KnownNotApplied {
                    reason:
                        KnownNotAppliedReason::IngressClosed | KnownNotAppliedReason::IngressFull,
                    outcome: None,
                    error: None,
                },
            )
            | (
                MutationSequenceDisposition::BlockUnknown,
                DurableDisposition::OutcomeUnknown { pending: None, .. },
            ) => {
                let was_frozen = self.frozen;
                self.fail_stop_submission(pending);
                self.stop_completion_drain_after_mutation(was_frozen)
            }
            (
                MutationSequenceDisposition::BlockUnknown,
                DurableDisposition::OutcomeUnknown {
                    pending: Some(returned),
                    ..
                },
            ) if returned == pending => {
                let was_frozen = self.frozen;
                self.fail_stop_submission(returned);
                self.stop_completion_drain_after_mutation(was_frozen)
            }
            (
                MutationSequenceDisposition::AdvanceNext,
                DurableDisposition::KnownNotApplied {
                    reason:
                        KnownNotAppliedReason::DeadlineBeforeStart
                        | KnownNotAppliedReason::BusyRolledBack,
                    outcome: None,
                    error: None,
                },
            ) => {
                let was_frozen = self.frozen;
                self.retry_stop_intent_batch(&identity, requests);
                self.stop_completion_drain_after_mutation(was_frozen)
            }
            (
                MutationSequenceDisposition::AdvanceNext,
                DurableDisposition::KnownNotApplied {
                    reason: KnownNotAppliedReason::KnownRollback,
                    outcome: None,
                    error: Some(_),
                },
            ) => {
                for request in requests {
                    if request.kind == StopIntentKind::UserCancelled {
                        self.fail_user_cancel_waiters(
                            request.task_id,
                            TaskManagerError::StoreDegraded,
                        );
                    }
                }
                self.freeze_degraded();
                StopCompletionDrain::Stop
            }
            _ => {
                self.freeze_degraded();
                StopCompletionDrain::Stop
            }
        }
    }

    pub(super) fn stop_completion_drain_after_mutation(
        &self,
        was_frozen: bool,
    ) -> StopCompletionDrain {
        if !was_frozen && self.frozen {
            StopCompletionDrain::Stop
        } else {
            StopCompletionDrain::Continue
        }
    }

    pub(super) fn retry_stop_intent_batch(
        &mut self,
        previous_identity: &DurableOperationIdentity,
        requests: Vec<StopIntentRequest>,
    ) {
        if self.frozen {
            return;
        }
        let DurableOperationIdentity::StopIntentBatch {
            items: previous_items,
        } = previous_identity
        else {
            self.freeze_degraded();
            return;
        };
        if previous_items.len() != requests.len() {
            self.freeze_degraded();
            return;
        }
        let mut next_items = Vec::with_capacity(requests.len());
        let mut retry_context = Vec::with_capacity(requests.len());
        for (previous, request) in previous_items.iter().zip(&requests) {
            let Some((deadline, retries_remaining)) =
                self.active
                    .get(&request.task_id)
                    .and_then(|active| match &active.stop_state {
                        ActiveStopState::IntentWritePending {
                            identity,
                            request: active_request,
                            deadline,
                            retries_remaining,
                            ..
                        } if identity == previous && active_request == request => {
                            Some((*deadline, *retries_remaining))
                        }
                        _ => None,
                    })
            else {
                self.freeze_degraded();
                return;
            };
            if previous.task_id != request.task_id
                || retries_remaining == 0
                || Instant::now() >= deadline
            {
                if request.kind == StopIntentKind::UserCancelled {
                    self.fail_user_cancel_waiters(request.task_id, TaskManagerError::StoreDegraded);
                }
                self.freeze_degraded();
                return;
            }
            let Some(identity) = self
                .next_mutation_identity(request.task_id, DurableOperationKind::PersistStopIntent)
            else {
                self.freeze_degraded();
                return;
            };
            next_items.push(identity);
            retry_context.push((deadline, retries_remaining - 1));
        }
        let Some(deadline) = retry_context.iter().map(|(deadline, _)| *deadline).min() else {
            self.freeze_degraded();
            return;
        };
        for ((identity, request), (_, retries_remaining)) in
            next_items.iter().zip(&requests).zip(&retry_context)
        {
            let Some(active) = self.active.get_mut(&request.task_id) else {
                self.freeze_degraded();
                return;
            };
            active.stop_state = ActiveStopState::IntentWritePending {
                kind: request.kind,
                identity: *identity,
                request: *request,
                deadline,
                retries_remaining: *retries_remaining,
            };
        }
        if requests
            .iter()
            .any(|request| request.kind == StopIntentKind::DiskPressureCritical)
        {
            if requests
                .iter()
                .any(|request| request.kind != StopIntentKind::DiskPressureCritical)
            {
                self.freeze_degraded();
                return;
            }
            let deadline_entries = next_items
                .iter()
                .copied()
                .zip(requests.iter().copied())
                .collect::<Vec<_>>();
            if Instant::now() >= deadline {
                self.handle_critical_stop_deadline_elapsed(deadline_entries, deadline);
                return;
            }
            self.schedule_critical_stop_deadline(deadline_entries, deadline);
        }
        let identity = DurableOperationIdentity::stop_intent_batch(next_items)
            .expect("retried stop identities preserve canonical task order");
        let submission = if requests.len() == 1 && requests[0].kind == StopIntentKind::UserCancelled
        {
            let DurableOperationIdentity::StopIntentBatch { items } = &identity else {
                unreachable!("constructed stop batch identity");
            };
            self.writer
                .submit_user_stop_intent(items[0], requests[0], deadline)
        } else {
            self.writer
                .submit_stop_intent_batch(identity.clone(), requests, deadline)
        };
        match submission {
            Ok(submission) => {
                let completion_sender = self.completion_sender.clone();
                tokio::spawn(async move {
                    let completion = submission.completion().await;
                    let identity = completion.identity.clone();
                    let _ = completion_sender
                        .send(TaskManagerCompletion::StopIntentPersisted {
                            identity,
                            completion,
                        })
                        .await;
                });
            }
            Err(
                StoreWriterSubmitError::Full
                | StoreWriterSubmitError::Closed
                | StoreWriterSubmitError::InvalidIdentity
                | StoreWriterSubmitError::SequenceGap
                | StoreWriterSubmitError::SequenceReversed,
            ) => {
                tracing::error!("retried stop-intent submission was not admitted");
                self.freeze_degraded();
            }
        }
    }
}
