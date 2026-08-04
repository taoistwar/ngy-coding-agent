use super::*;
use crate::scheduler::PermitToken;

struct PendingClaimResources {
    permit: SharedPermitOwnership,
    control_lease: RepositoryControlLease,
    control_recovery: RepositoryControlRecoveryWitness,
    process_scope: TaskProcessScopeOwnership,
}

struct PendingClaimRegistration {
    identity: TaskMutationIdentity,
    request: ClaimTaskRequest,
}

impl TaskManager {
    pub(super) async fn claim_ready_candidate(&mut self, admitted: StorageAdmissionCandidate) {
        let StorageAdmissionCandidate {
            operation_nonce,
            task,
            repository,
            coordination_key,
            ..
        } = admitted;
        if !self.claims_allowed() {
            self.finish_scan();
            return;
        }

        let Some(token) = self.reserve_candidate_permit(task.id, coordination_key) else {
            return;
        };
        #[cfg(feature = "test-support")]
        self.pause_process_actor(ActorPausePoint::ClaimPermitAcquired)
            .await;
        #[cfg(test)]
        self.pause_claim(ClaimPhase::PermitAcquired).await;
        if self.drain_buffered_messages() {
            if self.permit_ledger.release_unsubmitted(&token).is_err() {
                self.freeze_degraded();
                return;
            }
            self.finish_scan();
            return;
        }
        if !self.claims_allowed() || self.sender.upgrade().is_none() {
            if self.permit_ledger.release_unsubmitted(&token).is_err() {
                self.freeze_degraded();
            }
            self.finish_scan();
            return;
        }

        let Some(resources) =
            self.acquire_pending_claim_resources(&task, coordination_key, operation_nonce, token)
        else {
            return;
        };
        let Some(registration) =
            self.register_pending_claim(&task, repository, operation_nonce, resources)
        else {
            return;
        };

        #[cfg(feature = "test-support")]
        self.pause_process_actor(ActorPausePoint::ClaimHandleRegistered)
            .await;
        #[cfg(test)]
        self.pause_claim(ClaimPhase::HandleRegistered).await;
        if self.drain_buffered_messages() {
            if !self.abort_registered_claim_before_submission(task.id, operation_nonce) {
                self.freeze_degraded();
            }
            return;
        }

        let actor_liveness = if self.claims_allowed() {
            self.sender.upgrade()
        } else {
            None
        };
        let Some(actor_liveness) = actor_liveness else {
            self.release_registered_claim_without_actor(task.id, operation_nonce);
            return;
        };
        #[cfg(test)]
        self.pause_claim(ClaimPhase::ActorLivenessAcquired).await;
        if self.drain_buffered_messages() {
            if !self.abort_registered_claim_before_submission(task.id, operation_nonce) {
                self.freeze_degraded();
            }
            return;
        }

        if !self.attach_claim_actor_liveness(task.id, operation_nonce, actor_liveness) {
            self.freeze_degraded();
            return;
        }
        self.submit_registered_claim(task.id, operation_nonce, registration);
    }

    fn reserve_candidate_permit(
        &mut self,
        task_id: TaskId,
        coordination_key: RepositoryCoordinationKey,
    ) -> Option<PermitToken> {
        match self.permit_ledger.reserve(task_id, coordination_key) {
            Ok(token) => Some(token),
            Err(PermitLedgerError::GlobalCapacity | PermitLedgerError::RepositoryCapacity) => {
                self.finish_scan();
                None
            }
            Err(_) => {
                self.freeze_degraded();
                None
            }
        }
    }

    fn acquire_pending_claim_resources(
        &mut self,
        task: &Task,
        coordination_key: RepositoryCoordinationKey,
        operation_nonce: u64,
        token: PermitToken,
    ) -> Option<PendingClaimResources> {
        let (control_lease, control_recovery) = match self
            .repository_control
            .try_acquire_with_recovery_witness(coordination_key)
        {
            Ok(resources) => resources,
            Err(RepositoryControlError::Busy) => {
                let _ = self.permit_ledger.release_unsubmitted(&token);
                self.scan_gates
                    .set_repository_control_busy(coordination_key, true);
                self.start_next_storage_admission();
                return None;
            }
            Err(RepositoryControlError::Poisoned) => {
                let _ = self.permit_ledger.release_unsubmitted(&token);
                self.scan_gates
                    .set_repository_control_busy(coordination_key, true);
                tracing::warn!(
                    task_id = %task.id,
                    "poisoned repository control identity remains fail-closed"
                );
                self.start_next_storage_admission();
                return None;
            }
            Err(error) => {
                let _ = self.permit_ledger.release_unsubmitted(&token);
                tracing::error!(task_id = %task.id, error = %error, "control lease failed");
                self.freeze_degraded();
                return None;
            }
        };
        let process_scope = match TaskProcessScopeOwnership::derive(
            &self.instance_process_scope,
            task.id,
            operation_nonce,
        ) {
            Ok(scope) => scope,
            Err(_) => {
                let _ = self.permit_ledger.release_unsubmitted(&token);
                let _ = control_lease.clean_release();
                self.freeze_degraded();
                return None;
            }
        };
        let permit = match SharedPermitOwnership::new(
            self.permit_ledger.clone(),
            token,
            operation_nonce,
            process_scope.owner_id(),
        ) {
            Ok(permit) => permit,
            Err(_) => {
                let _ = control_lease.clean_release();
                self.freeze_degraded();
                return None;
            }
        };
        Some(PendingClaimResources {
            permit,
            control_lease,
            control_recovery,
            process_scope,
        })
    }

    fn register_pending_claim(
        &mut self,
        task: &Task,
        repository: Repository,
        operation_nonce: u64,
        resources: PendingClaimResources,
    ) -> Option<PendingClaimRegistration> {
        let sequence = self.mutation_sequences.get(&task.id).copied().unwrap_or(1);
        let Some(sequence) = NonZeroU64::new(sequence) else {
            self.freeze_degraded();
            return None;
        };
        let identity = TaskMutationIdentity {
            task_id: task.id,
            sequence: MutationSequence::new(sequence),
            kind: DurableOperationKind::ClaimTask,
        };
        let request = ClaimTaskRequest {
            task_id: task.id,
            expected_repository_id: task.repository_id,
            expected_attempt: task.attempt,
            expected_queued_event_id: task.last_event_id,
        };
        let cancellation = self.shutdown.cancellation.child_token();
        let (done_sender, done_receiver) = oneshot::channel();
        let coordination_key = resources.permit.coordination_key();
        let process_owner_id = resources.permit.process_owner_id();
        self.active.insert(
            task.id,
            ActiveRunner {
                actor_liveness: None,
                cancellation,
                phase: AdmissionPhase::ClaimPending,
                operation_nonce,
                permit: resources.permit,
                control_lease: Some(resources.control_lease),
                control_recovery: Some(ActiveRepositoryControlRecovery::new(
                    task.id,
                    task.repository_id,
                    task.attempt,
                    operation_nonce,
                    process_owner_id,
                    coordination_key,
                    resources.control_recovery,
                )),
                repository,
                claimed_task: None,
                claim_identity: identity,
                claim_request: request.clone(),
                process_scope: resources.process_scope,
                cleanup_confirmation: None,
                terminal_event: None,
                terminal_projection_barrier: None,
                preparation_complete: false,
                repository_id: task.repository_id,
                attempt: task.attempt,
                stop_state: ActiveStopState::NoWinner,
                stop_intent_lineage: None,
                applied_final_stop: None,
                pending_terminal_write: None,
                pending_runner_event_writes: HashMap::new(),
                pending_record_review_writes: HashMap::new(),
                pending_record_review_replays: HashMap::new(),
                applied_record_reviews: HashMap::new(),
                next_runner_mutation_id: 1,
                user_cancel_waiters: Vec::new(),
                terminal_cancel_waiters: Vec::new(),
                accepted_stop_task: None,
                accepted_stop_task_load_in_flight: false,
                next_mutation_sequence: sequence.get(),
                durable_sequence_blocked: false,
                in_flight_mutations: 0,
                pending_runner_outcome: None,
                runner_returned: None,
                cleanup_retry_scheduled: false,
                launch_suppression: None,
                recovery_release_ready: false,
                terminal_task: None,
                done_sender: Some(done_sender),
                done_receiver: Some(done_receiver),
            },
        );
        #[cfg(test)]
        if let Some(hooks) = &self.claim_hooks {
            hooks
                .active_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        Some(PendingClaimRegistration { identity, request })
    }

    fn release_registered_claim_without_actor(&mut self, task_id: TaskId, operation_nonce: u64) {
        let released = self.active.get_mut(&task_id).is_some_and(|active| {
            if active.operation_nonce != operation_nonce
                || active.phase != AdmissionPhase::ClaimPending
                || active.permit.release_unsubmitted().is_err()
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
        }
        self.remove_active(task_id);
        self.finish_scan();
    }

    fn attach_claim_actor_liveness(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        actor_liveness: mpsc::Sender<TaskManagerMessage>,
    ) -> bool {
        let Some(active) = self.active.get_mut(&task_id) else {
            return false;
        };
        if active.operation_nonce != operation_nonce
            || active.phase != AdmissionPhase::ClaimPending
            || active.actor_liveness.is_some()
        {
            return false;
        }
        active.actor_liveness = Some(actor_liveness);
        true
    }

    fn submit_registered_claim(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        registration: PendingClaimRegistration,
    ) {
        let PendingClaimRegistration { identity, request } = registration;
        let submission = self
            .writer
            .submit_claim_task(identity, request, background_deadline());
        match submission {
            Ok(submission) => {
                let Some(active) = self.active.get(&task_id) else {
                    self.freeze_degraded();
                    return;
                };
                if active.operation_nonce != operation_nonce
                    || active.permit.mark_submitted().is_err()
                {
                    self.freeze_degraded();
                    return;
                }
                let completion_sender = self.completion_sender.clone();
                tokio::spawn(async move {
                    let completion = submission.completion().await;
                    let _ = completion_sender
                        .send(TaskManagerCompletion::ClaimCompleted {
                            task_id: identity.task_id,
                            operation_nonce,
                            completion,
                        })
                        .await;
                });
                self.finish_scan();
            }
            Err(error) => {
                tracing::error!(task_id = %task_id, error = %error, "claim submission failed");
                if let Some(mut active) = self.active.remove(&task_id) {
                    let _ = active.permit.release_unsubmitted();
                    if let Some(lease) = active.control_lease.take() {
                        let _ = lease.clean_release();
                    }
                    if let Some(done) = active.done_sender.take() {
                        let _ = done.send(());
                    }
                }
                match error {
                    StoreWriterSubmitError::InvalidIdentity
                    | StoreWriterSubmitError::SequenceGap
                    | StoreWriterSubmitError::SequenceReversed => self.freeze_degraded(),
                    StoreWriterSubmitError::Full | StoreWriterSubmitError::Closed => {
                        unreachable!(
                            "writer ingress Full/Closed are returned as typed completions"
                        );
                    }
                }
            }
        }
    }

    fn abort_registered_claim_before_submission(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
    ) -> bool {
        let released = self.active.get_mut(&task_id).is_some_and(|active| {
            active.operation_nonce == operation_nonce
                && active.phase == AdmissionPhase::ClaimPending
                && active.actor_liveness.is_none()
                && active.permit.release_unsubmitted().is_ok()
                && active
                    .control_lease
                    .take()
                    .is_some_and(|lease| lease.clean_release().is_ok())
        });
        if released {
            self.remove_active(task_id);
            self.finish_scan();
        }
        released
    }
}
