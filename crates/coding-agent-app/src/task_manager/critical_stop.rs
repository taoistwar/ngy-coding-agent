use super::*;

impl TaskManager {
    pub(super) fn handle_critical_wake(&mut self) {
        if self.frozen {
            return;
        }
        let retained_for_terminal_release = self
            .active
            .iter()
            .filter_map(|(task_id, active)| {
                matches!(
                    active.phase,
                    AdmissionPhase::TerminalWritePending | AdmissionPhase::ProjectionPending
                )
                .then_some(*task_id)
            })
            .collect::<HashSet<_>>();
        let (facts, more_pending) = self
            .safety_registry
            .take_actionable_pending_critical(&retained_for_terminal_release);
        if more_pending {
            self.critical_wake.notify();
        }
        let mut facts = facts
            .into_iter()
            .filter(|(task_id, fact)| {
                self.active.get(task_id).is_some_and(|active| {
                    active.operation_nonce == fact.operation_nonce
                        && matches!(&active.stop_state, ActiveStopState::NoWinner)
                        && !matches!(
                            active.phase,
                            AdmissionPhase::TerminalWritePending
                                | AdmissionPhase::ProjectionPending
                        )
                })
            })
            .collect::<Vec<_>>();
        facts.sort_by_key(|(task_id, _)| task_id.as_uuid().as_u128());
        let Some(deadline) = facts
            .iter()
            .map(|(_, fact)| fact.observed_at + self.critical_stop_persistence_budget)
            .min()
        else {
            return;
        };
        let mut ready = Vec::new();
        let mut deadline_entries = Vec::with_capacity(facts.len());
        for (task_id, fact) in facts {
            let Some(identity) =
                self.next_mutation_identity(task_id, DurableOperationKind::PersistStopIntent)
            else {
                self.freeze_degraded();
                return;
            };
            let batch_identity = DurableOperationIdentity::stop_intent_batch(vec![identity])
                .expect("one exact critical stop identity is a valid batch");
            let defer_submission = self
                .active
                .get(&task_id)
                .is_some_and(|active| active.durable_sequence_blocked)
                || self.stop_completion_has_pending_predecessor(&batch_identity);
            let Some(active) = self.active.get_mut(&task_id) else {
                self.freeze_degraded();
                return;
            };
            if active.operation_nonce != fact.operation_nonce {
                self.freeze_degraded();
                return;
            }
            let request = StopIntentRequest {
                task_id,
                expected_repository_id: active.repository_id,
                expected_attempt: active.attempt,
                kind: StopIntentKind::DiskPressureCritical,
            };
            active.stop_state = if defer_submission {
                ActiveStopState::IntentSubmissionDeferred {
                    kind: request.kind,
                    identity,
                    request,
                    deadline,
                    retries_remaining: STOP_WRITE_RETRY_LIMIT,
                }
            } else {
                ActiveStopState::IntentWritePending {
                    kind: request.kind,
                    identity,
                    request,
                    deadline,
                    retries_remaining: STOP_WRITE_RETRY_LIMIT,
                }
            };
            deadline_entries.push((identity, request));
            if !defer_submission {
                ready.push((identity, request));
            }
        }
        if Instant::now() >= deadline {
            self.handle_critical_stop_deadline_elapsed(deadline_entries, deadline);
            return;
        }
        self.schedule_critical_stop_deadline(deadline_entries, deadline);
        if ready.is_empty() {
            return;
        }
        self.submit_critical_stop_batch(ready, deadline);
    }

    pub(super) fn schedule_critical_stop_deadline(
        &self,
        entries: Vec<(TaskMutationIdentity, StopIntentRequest)>,
        deadline: Instant,
    ) {
        if entries.is_empty() {
            return;
        }
        let completion_sender = self.completion_sender.clone();
        tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            let _ = completion_sender
                .send(TaskManagerCompletion::CriticalStopDeadlineElapsed { entries, deadline })
                .await;
        });
    }

    pub(super) fn handle_critical_stop_deadline_elapsed(
        &mut self,
        entries: Vec<(TaskMutationIdentity, StopIntentRequest)>,
        deadline: Instant,
    ) {
        if Instant::now() < deadline {
            self.schedule_critical_stop_deadline(entries, deadline);
            return;
        }
        if self.frozen {
            return;
        }
        let exact_pending_identity_remains = entries.iter().any(|(identity, request)| {
            identity.task_id == request.task_id
                && request.kind == StopIntentKind::DiskPressureCritical
                && self.active.get(&identity.task_id).is_some_and(|active| {
                    matches!(
                        &active.stop_state,
                        ActiveStopState::IntentSubmissionDeferred {
                            kind: StopIntentKind::DiskPressureCritical,
                            identity: active_identity,
                            request: active_request,
                            deadline: active_deadline,
                            ..
                        }
                        | ActiveStopState::IntentWritePending {
                            kind: StopIntentKind::DiskPressureCritical,
                            identity: active_identity,
                            request: active_request,
                            deadline: active_deadline,
                            ..
                        } if active_identity == identity
                            && active_request == request
                            && *active_deadline == deadline
                    )
                })
        });
        if exact_pending_identity_remains {
            self.freeze_degraded();
        }
    }

    pub(super) fn expire_critical_stop_deadlines(&mut self, now: Instant) {
        if self.frozen {
            return;
        }
        let expired = self.active.values().any(|active| {
            matches!(
                &active.stop_state,
                ActiveStopState::IntentSubmissionDeferred {
                    kind: StopIntentKind::DiskPressureCritical,
                    deadline,
                    ..
                }
                | ActiveStopState::IntentWritePending {
                    kind: StopIntentKind::DiskPressureCritical,
                    deadline,
                    ..
                } if now >= *deadline
            )
        });
        if expired {
            self.freeze_degraded();
        }
    }

    pub(super) fn submit_critical_stop_batch(
        &mut self,
        ready: Vec<(TaskMutationIdentity, StopIntentRequest)>,
        deadline: Instant,
    ) {
        if self.frozen {
            return;
        }
        if ready.is_empty() || ready.len() > 4 || Instant::now() >= deadline {
            self.freeze_degraded();
            return;
        }
        let (identities, requests): (Vec<_>, Vec<_>) = ready.into_iter().unzip();
        for identity in &identities {
            let Some(active) = self.active.get_mut(&identity.task_id) else {
                self.freeze_degraded();
                return;
            };
            let ActiveStopState::IntentWritePending {
                identity: active_identity,
                deadline: active_deadline,
                ..
            } = &mut active.stop_state
            else {
                self.freeze_degraded();
                return;
            };
            if active_identity != identity || deadline > *active_deadline {
                self.freeze_degraded();
                return;
            }
            *active_deadline = deadline;
        }
        let identity = DurableOperationIdentity::stop_intent_batch(identities)
            .expect("critical stop facts are canonical unique active tasks");
        match self
            .writer
            .submit_stop_intent_batch(identity, requests, deadline)
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
                tracing::error!(%error, "critical stop-intent batch submission failed");
                self.freeze_degraded();
            }
        }
    }

    pub(super) fn stop_completion_has_pending_predecessor(
        &self,
        identity: &DurableOperationIdentity,
    ) -> bool {
        let DurableOperationIdentity::StopIntentBatch { items: stop_items } = identity else {
            return false;
        };
        self.pending_durable_results.iter().any(|pending| {
            let pending_identity = pending.identity();
            stop_items.iter().any(|stop| match &pending_identity {
                DurableOperationIdentity::TaskMutation(pending) => {
                    pending.task_id == stop.task_id && pending.sequence.get() < stop.sequence.get()
                }
                DurableOperationIdentity::StopIntentBatch { items } => {
                    items.iter().any(|pending| {
                        pending.task_id == stop.task_id
                            && pending.sequence.get() < stop.sequence.get()
                    })
                }
                DurableOperationIdentity::CreateTask { .. }
                | DurableOperationIdentity::RetryTask { .. } => false,
            })
        })
    }

    pub(super) fn stop_completion_ownership(
        &self,
        identities: &[TaskMutationIdentity],
    ) -> StopCompletionOwnership {
        let owned = identities
            .iter()
            .filter(|identity| self.active.contains_key(&identity.task_id))
            .count();
        match owned {
            0 => StopCompletionOwnership::FullyAbsent,
            count if count == identities.len() => StopCompletionOwnership::FullyOwned,
            _ => StopCompletionOwnership::Mixed,
        }
    }
}
