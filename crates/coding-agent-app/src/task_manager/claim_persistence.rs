use super::*;

impl TaskManager {
    pub(super) async fn handle_claim_completion(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        completion: DurableCompletion<ClaimTaskOutcome>,
    ) {
        let Some(active) = self.active.get(&task_id) else {
            return;
        };
        if active.operation_nonce != operation_nonce || active.phase != AdmissionPhase::ClaimPending
        {
            return;
        }
        if completion.identity != DurableOperationIdentity::TaskMutation(active.claim_identity) {
            self.freeze_degraded();
            return;
        }
        match completion.disposition {
            DurableDisposition::Confirmed(
                outcome @ (ClaimTaskOutcome::Applied(_) | ClaimTaskOutcome::ExistingApplied(_)),
            ) if completion.sequence_disposition == MutationSequenceDisposition::AdvanceNext => {
                let receipt = match outcome {
                    ClaimTaskOutcome::Applied(receipt)
                    | ClaimTaskOutcome::ExistingApplied(receipt) => receipt,
                    ClaimTaskOutcome::KnownNotApplied { .. }
                    | ClaimTaskOutcome::InvariantConflict => unreachable!("guarded claim outcome"),
                };
                self.adopt_claim_and_launch(task_id, operation_nonce, receipt)
                    .await;
            }
            DurableDisposition::KnownNotApplied {
                reason: KnownNotAppliedReason::IngressFull,
                outcome: None,
                error: None,
            } if completion.sequence_disposition == MutationSequenceDisposition::RetainSame => {
                self.retain_claim_for_reconciliation(task_id, operation_nonce)
                    .await;
            }
            DurableDisposition::KnownNotApplied {
                reason: KnownNotAppliedReason::IngressClosed,
                outcome: None,
                error: None,
            } if completion.sequence_disposition == MutationSequenceDisposition::RetainSame => {
                self.fail_closed_claim_ingress(task_id, operation_nonce);
            }
            DurableDisposition::Confirmed(ClaimTaskOutcome::KnownNotApplied { .. })
            | DurableDisposition::KnownNotApplied {
                outcome: None | Some(ClaimTaskOutcome::KnownNotApplied { .. }),
                ..
            } => {
                self.release_known_not_applied_claim(
                    task_id,
                    operation_nonce,
                    completion.sequence_disposition,
                );
            }
            DurableDisposition::OutcomeUnknown { .. }
                if completion.sequence_disposition == MutationSequenceDisposition::BlockUnknown =>
            {
                self.retain_claim_for_reconciliation(task_id, operation_nonce)
                    .await;
            }
            DurableDisposition::Confirmed(ClaimTaskOutcome::InvariantConflict)
            | DurableDisposition::KnownNotApplied {
                outcome: Some(ClaimTaskOutcome::InvariantConflict),
                ..
            }
            | DurableDisposition::InvariantConflict { .. }
            | DurableDisposition::Confirmed(
                ClaimTaskOutcome::Applied(_) | ClaimTaskOutcome::ExistingApplied(_),
            )
            | DurableDisposition::KnownNotApplied {
                outcome: Some(ClaimTaskOutcome::Applied(_) | ClaimTaskOutcome::ExistingApplied(_)),
                ..
            }
            | DurableDisposition::OutcomeUnknown { .. } => self.freeze_degraded(),
        }
    }

    pub(super) async fn handle_claim_reconciliation(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        completion: DurableCompletion<ClaimTaskReconciliationOutcome>,
    ) {
        let Some(active) = self.active.get(&task_id) else {
            return;
        };
        if active.operation_nonce != operation_nonce || active.phase != AdmissionPhase::ClaimUnknown
        {
            return;
        }
        if completion.identity != DurableOperationIdentity::TaskMutation(active.claim_identity) {
            self.freeze_degraded();
            return;
        }
        match completion.disposition {
            DurableDisposition::Confirmed(ClaimTaskReconciliationOutcome::ExistingApplied(
                receipt,
            )) if completion.sequence_disposition == MutationSequenceDisposition::AdvanceNext => {
                self.adopt_claim_and_launch(task_id, operation_nonce, receipt)
                    .await;
            }
            DurableDisposition::Confirmed(ClaimTaskReconciliationOutcome::KnownNotApplied {
                ..
            })
            | DurableDisposition::KnownNotApplied {
                outcome: None | Some(ClaimTaskReconciliationOutcome::KnownNotApplied { .. }),
                ..
            } => {
                self.release_known_not_applied_claim(
                    task_id,
                    operation_nonce,
                    completion.sequence_disposition,
                );
            }
            DurableDisposition::OutcomeUnknown { .. }
                if completion.sequence_disposition == MutationSequenceDisposition::BlockUnknown =>
            {
                self.schedule_claim_reconciliation_retry(task_id, operation_nonce);
            }
            DurableDisposition::Confirmed(ClaimTaskReconciliationOutcome::InvariantConflict)
            | DurableDisposition::KnownNotApplied {
                outcome: Some(ClaimTaskReconciliationOutcome::InvariantConflict),
                ..
            }
            | DurableDisposition::InvariantConflict { .. }
            | DurableDisposition::Confirmed(ClaimTaskReconciliationOutcome::ExistingApplied(_))
            | DurableDisposition::KnownNotApplied {
                outcome: Some(ClaimTaskReconciliationOutcome::ExistingApplied(_)),
                ..
            }
            | DurableDisposition::OutcomeUnknown { .. } => self.freeze_degraded(),
        }
    }

    pub(super) fn submit_claim_reconciliation(&mut self, task_id: TaskId, operation_nonce: u64) {
        let Some(active) = self.active.get(&task_id) else {
            return;
        };
        if active.operation_nonce != operation_nonce || active.phase != AdmissionPhase::ClaimUnknown
        {
            return;
        }
        let identity = active.claim_identity;
        let request = active.claim_request.clone();
        let submission =
            self.writer
                .submit_reconcile_claim_task(identity, request, background_deadline());
        match submission {
            Ok(submission) => {
                let completion_sender = self.completion_sender.clone();
                tokio::spawn(async move {
                    let completion = submission.completion().await;
                    let _ = completion_sender
                        .send(TaskManagerCompletion::ClaimReconciled {
                            task_id,
                            operation_nonce,
                            completion,
                        })
                        .await;
                });
            }
            Err(
                StoreWriterSubmitError::InvalidIdentity
                | StoreWriterSubmitError::SequenceGap
                | StoreWriterSubmitError::SequenceReversed,
            ) => self.freeze_degraded(),
            Err(StoreWriterSubmitError::Full | StoreWriterSubmitError::Closed) => {
                unreachable!("writer ingress Full/Closed are returned as typed completions");
            }
        }
    }

    pub(super) fn schedule_claim_reconciliation_retry(
        &self,
        task_id: TaskId,
        operation_nonce: u64,
    ) {
        let completion_sender = self.completion_sender.clone();
        tokio::spawn(async move {
            tokio::time::sleep(RECONCILE_INTERVAL).await;
            let _ = completion_sender
                .send(TaskManagerCompletion::ClaimReconciliationRetry {
                    task_id,
                    operation_nonce,
                })
                .await;
        });
    }

    pub(super) async fn retain_claim_for_reconciliation(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
    ) {
        let retained = self.active.get_mut(&task_id).is_some_and(|active| {
            if active.operation_nonce != operation_nonce
                || active.phase != AdmissionPhase::ClaimPending
                || active.permit.retain_outcome_unknown().is_err()
            {
                return false;
            }
            active.phase = AdmissionPhase::ClaimUnknown;
            active.durable_sequence_blocked = true;
            true
        });
        if !retained {
            self.freeze_degraded();
            return;
        }
        #[cfg(test)]
        self.pause_claim(ClaimPhase::ClaimRetainedForReconciliation)
            .await;
        self.submit_claim_reconciliation(task_id, operation_nonce);
    }

    pub(super) fn fail_closed_claim_ingress(&mut self, task_id: TaskId, operation_nonce: u64) {
        let retained = self.active.get_mut(&task_id).is_some_and(|active| {
            if active.operation_nonce != operation_nonce
                || active.phase != AdmissionPhase::ClaimPending
                || active.permit.retain_outcome_unknown().is_err()
            {
                return false;
            }
            active.phase = AdmissionPhase::ClaimUnknown;
            active.durable_sequence_blocked = true;
            true
        });
        if !retained {
            self.freeze_degraded();
            return;
        }

        // A closed ingress cannot recover through the same writer handle. Freeze before
        // releasing the never-spawned provisional ownership so no later scan can reuse
        // the unresolved mutation sequence.
        self.freeze_degraded();
        let released = self.active.get_mut(&task_id).is_some_and(|active| {
            if active.operation_nonce != operation_nonce
                || active.phase != AdmissionPhase::ClaimUnknown
                || active.permit.release_known_not_applied().is_err()
            {
                return false;
            }
            active
                .control_lease
                .take()
                .is_some_and(|lease| lease.clean_release().is_ok())
        });
        if !released {
            self.freeze_degraded();
            return;
        }
        self.remove_active(task_id);
    }

    pub(super) fn release_known_not_applied_claim(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        sequence_disposition: MutationSequenceDisposition,
    ) {
        let Some(active) = self.active.get(&task_id) else {
            return;
        };
        if active.operation_nonce != operation_nonce
            || !matches!(
                active.phase,
                AdmissionPhase::ClaimPending | AdmissionPhase::ClaimUnknown
            )
        {
            return;
        }
        let sequence = active.claim_identity.sequence.get();
        if sequence_disposition != MutationSequenceDisposition::AdvanceNext {
            self.freeze_degraded();
            return;
        }
        let next_sequence = match sequence.checked_add(1) {
            Some(next) => next,
            None => {
                self.freeze_degraded();
                return;
            }
        };
        let Some(active) = self.active.get_mut(&task_id) else {
            return;
        };
        let Some(lease) = active.control_lease.take() else {
            self.freeze_degraded();
            return;
        };
        if lease.clean_release().is_err() || active.permit.release_known_not_applied().is_err() {
            self.freeze_degraded();
            return;
        }
        self.mutation_sequences.insert(task_id, next_sequence);
        self.remove_active(task_id);
        if self.claims_allowed() {
            self.scan_requested = true;
        } else {
            self.try_start_quiesce_recovery();
            self.maybe_start_degraded_recovery();
        }
    }

    pub(super) async fn adopt_claim_and_launch(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        receipt: ClaimTaskReceipt,
    ) {
        let receipt_is_exact = self.active.get(&task_id).is_some_and(|active| {
            active.operation_nonce == operation_nonce
                && matches!(
                    active.phase,
                    AdmissionPhase::ClaimPending | AdmissionPhase::ClaimUnknown
                )
                && claim_receipt_is_exact(active, &receipt)
        });
        if !receipt_is_exact {
            self.freeze_degraded();
            return;
        }
        let scheduler_snapshot = match self.refresh_scheduler_projection().await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::error!(
                    %task_id,
                    %error,
                    "committed claim scheduler projection failed"
                );
                self.freeze_degraded();
                return;
            }
        };
        if !scheduler_snapshot
            .tasks
            .iter()
            .any(|task| task == &receipt.task)
            || receipt.task.status != TaskStatus::Running
            || scheduler_snapshot.membership_event_id.get() < receipt.task.last_event_id.get()
        {
            tracing::error!(
                %task_id,
                "committed claim is absent from the exact scheduler Store snapshot"
            );
            self.freeze_degraded();
            return;
        }
        let next_sequence = match self
            .active
            .get(&task_id)
            .and_then(|active| active.claim_identity.sequence.get().checked_add(1))
        {
            Some(next) => next,
            None => {
                self.freeze_degraded();
                return;
            }
        };
        let adopted = self.active.get_mut(&task_id).is_some_and(|active| {
            if active.permit.adopt().is_err() {
                return false;
            }
            active.claimed_task = Some(receipt.task.clone());
            active.next_mutation_sequence = next_sequence;
            active.durable_sequence_blocked = false;
            true
        });
        if !adopted {
            self.freeze_degraded();
            return;
        }
        self.mutation_sequences.insert(task_id, next_sequence);

        let (coordination_key, cancellation) = {
            let active = self
                .active
                .get(&task_id)
                .expect("the adopted claim remains actor-owned");
            (
                active.permit.coordination_key(),
                active.cancellation.clone(),
            )
        };
        let critical_pending = match self.safety_registry.publish(
            task_id,
            ActiveSafetyEntry {
                operation_nonce,
                repository_id: receipt.task.repository_id,
                coordination_key,
                stop: StopControl::new(cancellation),
            },
        ) {
            Ok(critical_pending) => critical_pending,
            Err(_) => {
                self.freeze_degraded();
                return;
            }
        };
        if critical_pending {
            self.critical_wake.notify();
        }
        let Some(stop_state) =
            self.safety_registry
                .launch_stop_state(task_id, operation_nonce, coordination_key)
        else {
            self.freeze_degraded();
            return;
        };
        let user_stop_accepted = self
            .active
            .get(&task_id)
            .is_some_and(|active| active.stop_state.kind() == Some(StopIntentKind::UserCancelled));
        let suppression = match classify_launch_suppression(
            self.claims_allowed(),
            self.storage_admission
                .launch_allowed(receipt.task.repository_id),
            stop_state,
            user_stop_accepted,
        ) {
            Ok(suppression) => suppression,
            Err(()) => {
                self.freeze_degraded();
                return;
            }
        };
        if let Some(reason) = suppression {
            self.suppress_claimed_launch(task_id, operation_nonce, reason);
            return;
        }
        if let Some(active) = self.active.get_mut(&task_id) {
            active.phase = AdmissionPhase::LaunchGatePending;
        }
        #[cfg(test)]
        if let Some(hooks) = &self.claim_hooks {
            let hooks = hooks.clone();
            let completion_sender = self.completion_sender.clone();
            tokio::spawn(async move {
                hooks.pause(ClaimPhase::RunningCommitted).await;
                let _ = completion_sender
                    .send(TaskManagerCompletion::LaunchGateReady {
                        task_id,
                        operation_nonce,
                    })
                    .await;
            });
            return;
        }
        #[cfg(feature = "test-support")]
        {
            let actor_pauses = self.actor_pauses.clone();
            let completion_sender = self.completion_sender.clone();
            tokio::spawn(async move {
                if let Some(actor_pauses) = actor_pauses {
                    actor_pauses
                        .pause(ActorPausePoint::ClaimRunningCommitted)
                        .await;
                    actor_pauses
                        .pause(ActorPausePoint::AfterFinalGateBeforeSpawn)
                        .await;
                }
                let _ = completion_sender
                    .send(TaskManagerCompletion::LaunchGateReady {
                        task_id,
                        operation_nonce,
                    })
                    .await;
            });
        }
        #[cfg(not(feature = "test-support"))]
        self.finish_claim_launch(task_id, operation_nonce);
    }
}
