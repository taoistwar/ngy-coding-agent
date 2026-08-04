use super::*;

impl TaskManager {
    pub(super) fn start_stop_finalization(&mut self, task_id: TaskId, operation_nonce: u64) {
        if self.frozen {
            return;
        }
        let Some(active) = self.active.get(&task_id) else {
            return;
        };
        let ActiveStopState::IntentDurable { receipt, .. } = &active.stop_state else {
            return;
        };
        let receipt = *receipt;
        if active.operation_nonce != operation_nonce
            || active.phase != AdmissionPhase::RunnerReturned
            || active.cleanup_confirmation.is_none()
            || active.in_flight_mutations != 0
        {
            return;
        }
        let Some(identity) =
            self.next_mutation_identity(task_id, DurableOperationKind::FinalizeStoppedTask)
        else {
            self.freeze_degraded();
            return;
        };
        let request = FinalizeStoppedTaskRequest {
            task_id,
            expected_repository_id: receipt.repository_id,
            expected_attempt: receipt.attempt,
            expected_intent: receipt.kind,
        };
        let deadline = self.current_persistence_deadline();
        let Some(active) = self.active.get_mut(&task_id) else {
            self.freeze_degraded();
            return;
        };
        active.stop_state = ActiveStopState::FinalStopWritePending {
            kind: receipt.kind,
            receipt: Some(receipt),
            identity,
            request,
            deadline,
            retries_remaining: STOP_WRITE_RETRY_LIMIT,
        };
        active.phase = AdmissionPhase::TerminalWritePending;
        match self
            .writer
            .submit_finalize_stopped_task(identity, request, deadline)
        {
            Ok(submission) => {
                let completion_sender = self.completion_sender.clone();
                tokio::spawn(async move {
                    let completion = submission.completion().await;
                    let _ = completion_sender
                        .send(TaskManagerCompletion::FinalStopPersisted {
                            task_id,
                            operation_nonce,
                            identity,
                            completion,
                        })
                        .await;
                });
            }
            Err(error) => {
                tracing::error!(%task_id, %error, "final-stop submission failed");
                self.freeze_degraded();
            }
        }
    }

    pub(super) fn handle_final_stop_persisted(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        identity: TaskMutationIdentity,
        completion: DurableCompletion<FinalizeStoppedTaskOutcome>,
    ) {
        let Some(active) = self.active.get(&task_id) else {
            return;
        };
        if let Some(applied) = active.applied_final_stop.clone() {
            let pending = PendingDurableResult::FinalizeStoppedTask {
                identity: applied.identity,
                request: applied.request,
            };
            let exact = active.operation_nonce == operation_nonce
                && applied.identity == identity
                && completion.identity == DurableOperationIdentity::TaskMutation(identity)
                && completion.sequence_disposition == MutationSequenceDisposition::AdvanceNext
                && matches!(
                    &completion.disposition,
                    DurableDisposition::Confirmed(outcome)
                        if pending_replay_receipt_matches(
                            &pending,
                            &PendingReplayReceipt::FinalizeStoppedTask(outcome.clone()),
                        )
                            && pending_replay_receipts_are_equivalent(
                                &pending,
                                &PendingReplayReceipt::FinalizeStoppedTask(
                                    applied.outcome.clone(),
                                ),
                                &PendingReplayReceipt::FinalizeStoppedTask(outcome.clone()),
                            )
                );
            if !exact {
                self.freeze_degraded();
            }
            return;
        }
        let (active_identity, request) = match &active.stop_state {
            ActiveStopState::FinalStopWritePending {
                identity, request, ..
            } => (*identity, *request),
            _ => {
                self.freeze_degraded();
                return;
            }
        };
        if active.operation_nonce != operation_nonce
            || active.phase != AdmissionPhase::TerminalWritePending
            || active_identity != identity
            || completion.identity != DurableOperationIdentity::TaskMutation(identity)
        {
            self.freeze_degraded();
            return;
        }
        let pending = PendingDurableResult::FinalizeStoppedTask { identity, request };
        match (completion.sequence_disposition, completion.disposition) {
            (MutationSequenceDisposition::AdvanceNext, DurableDisposition::Confirmed(outcome)) => {
                let replay_receipt = PendingReplayReceipt::FinalizeStoppedTask(outcome.clone());
                if !self.apply_final_stop_outcome(
                    task_id,
                    operation_nonce,
                    identity,
                    request,
                    outcome,
                ) || !self.resolve_canonical_pending_from_original(pending, replay_receipt)
                {
                    self.freeze_degraded();
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
            ) => self.fail_stop_submission(pending),
            (
                MutationSequenceDisposition::BlockUnknown,
                DurableDisposition::OutcomeUnknown {
                    pending: Some(returned),
                    ..
                },
            ) if returned == pending => self.fail_stop_submission(returned),
            (
                MutationSequenceDisposition::AdvanceNext,
                DurableDisposition::KnownNotApplied {
                    reason:
                        KnownNotAppliedReason::DeadlineBeforeStart
                        | KnownNotAppliedReason::BusyRolledBack,
                    outcome: None,
                    error: None,
                },
            ) => self.retry_final_stop(task_id, operation_nonce, identity, request),
            (
                MutationSequenceDisposition::AdvanceNext,
                DurableDisposition::KnownNotApplied {
                    reason: KnownNotAppliedReason::KnownRollback,
                    outcome: None,
                    error: Some(_),
                },
            ) => self.freeze_degraded(),
            _ => self.freeze_degraded(),
        }
    }

    pub(super) fn retry_final_stop(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        previous_identity: TaskMutationIdentity,
        request: FinalizeStoppedTaskRequest,
    ) {
        if self.frozen {
            return;
        }
        let Some(active) = self.active.get(&task_id) else {
            self.freeze_degraded();
            return;
        };
        let Some((kind, receipt, deadline, retries_remaining)) = (active.operation_nonce
            == operation_nonce)
            .then(|| match &active.stop_state {
                ActiveStopState::FinalStopWritePending {
                    kind,
                    receipt,
                    identity,
                    request: active_request,
                    deadline,
                    retries_remaining,
                } if *identity == previous_identity && *active_request == request => {
                    Some((*kind, *receipt, *deadline, *retries_remaining))
                }
                _ => None,
            })
            .flatten()
        else {
            self.freeze_degraded();
            return;
        };
        if retries_remaining == 0 || Instant::now() >= deadline {
            self.freeze_degraded();
            return;
        }
        let Some(identity) =
            self.next_mutation_identity(task_id, DurableOperationKind::FinalizeStoppedTask)
        else {
            self.freeze_degraded();
            return;
        };
        let Some(active) = self.active.get_mut(&task_id) else {
            self.freeze_degraded();
            return;
        };
        active.stop_state = ActiveStopState::FinalStopWritePending {
            kind,
            receipt,
            identity,
            request,
            deadline,
            retries_remaining: retries_remaining - 1,
        };
        match self
            .writer
            .submit_finalize_stopped_task(identity, request, deadline)
        {
            Ok(submission) => {
                let completion_sender = self.completion_sender.clone();
                tokio::spawn(async move {
                    let completion = submission.completion().await;
                    let _ = completion_sender
                        .send(TaskManagerCompletion::FinalStopPersisted {
                            task_id,
                            operation_nonce,
                            identity,
                            completion,
                        })
                        .await;
                });
            }
            Err(_) => self.freeze_degraded(),
        }
    }

    pub(super) fn apply_final_stop_replay(
        &mut self,
        identity: TaskMutationIdentity,
        request: FinalizeStoppedTaskRequest,
        outcome: FinalizeStoppedTaskOutcome,
    ) -> bool {
        let Some(operation_nonce) = self
            .active
            .get(&identity.task_id)
            .map(|active| active.operation_nonce)
        else {
            return false;
        };
        self.apply_final_stop_outcome(
            identity.task_id,
            operation_nonce,
            identity,
            request,
            outcome,
        )
    }

    pub(super) fn apply_final_stop_outcome(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        identity: TaskMutationIdentity,
        request: FinalizeStoppedTaskRequest,
        outcome: FinalizeStoppedTaskOutcome,
    ) -> bool {
        let Some(active) = self.active.get(&task_id) else {
            return false;
        };
        let kind = match &active.stop_state {
            ActiveStopState::FinalStopWritePending {
                kind,
                identity: active_identity,
                request: active_request,
                ..
            } if *active_identity == identity && *active_request == request => *kind,
            _ => return false,
        };
        if active.operation_nonce != operation_nonce
            || active.phase != AdmissionPhase::TerminalWritePending
        {
            return false;
        }
        let receipt = match outcome {
            FinalizeStoppedTaskOutcome::Applied(receipt)
            | FinalizeStoppedTaskOutcome::Existing(receipt)
                if receipt.intent.kind == kind
                    && stop_receipt_matches_final_request(receipt.intent, request)
                    && stopped_terminal_matches_active_intent(
                        active,
                        &receipt.task,
                        receipt.intent,
                    )
                    && terminal_receipt_is_exact(
                        self.active.get(&task_id),
                        &receipt.task,
                        terminal_event_kind(receipt.task.status)
                            .expect("checked final-stop terminal status"),
                        receipt.terminal_event_id,
                    ) =>
            {
                receipt
            }
            FinalizeStoppedTaskOutcome::Applied(_)
            | FinalizeStoppedTaskOutcome::Existing(_)
            | FinalizeStoppedTaskOutcome::InvariantConflict => return false,
        };
        let event_kind = terminal_event_kind(receipt.task.status)
            .expect("validated final-stop receipt has a terminal status");
        let Some(active) = self.active.get_mut(&task_id) else {
            return false;
        };
        active.applied_final_stop = Some(AppliedFinalStop {
            identity,
            request,
            outcome: FinalizeStoppedTaskOutcome::Existing(receipt.clone()),
        });
        active.stop_state = ActiveStopState::StopTerminal {
            receipt: receipt.intent,
            task: receipt.task.clone(),
            terminal_event_id: receipt.terminal_event_id,
        };
        active.terminal_task = Some(receipt.task.clone());
        self.start_terminal_projection(
            task_id,
            operation_nonce,
            receipt.task,
            event_kind,
            receipt.terminal_event_id,
        );
        true
    }
}
