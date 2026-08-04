use super::*;

impl TaskManager {
    #[cfg(feature = "test-support")]
    pub(super) async fn pause_process_actor(&self, point: ActorPausePoint) {
        if let Some(pauses) = &self.actor_pauses {
            pauses.pause(point).await;
        }
    }

    #[cfg(test)]
    pub(super) fn terminal_projection_snapshot_for_test(
        &self,
        task_id: TaskId,
    ) -> TerminalProjectionSnapshotForTest {
        let active = self.active.get(&task_id);
        TerminalProjectionSnapshotForTest {
            active: active.is_some(),
            phase: active.map(|active| active.phase),
            current_attempt: active
                .and_then(|active| active.terminal_projection_barrier.as_ref())
                .map(TerminalProjectionBarrier::current),
            next_attempt_id: self.next_terminal_projection_attempt_id,
            next_typed_write_attempt_id: self.next_typed_write_attempt_id,
            next_mutation_sequence: active.map(|active| active.next_mutation_sequence),
            cleanup_available: active
                .and_then(|active| active.cleanup_confirmation.as_ref())
                .is_some_and(TaskProcessCleanupConfirmation::is_available_for_terminal_release),
            permit_active: active.is_some_and(|active| {
                active.permit.state() == Ok(crate::PermitOwnershipState::Active)
            }),
            registry_owned: active.is_some_and(|active| {
                self.safety_registry
                    .launch_stop_state(
                        task_id,
                        active.operation_nonce,
                        active.permit.coordination_key(),
                    )
                    .is_some()
            }),
            hard_frozen: self.frozen,
        }
    }

    #[cfg(test)]
    pub(super) async fn pause_claim(&self, phase: ClaimPhase) {
        if let Some(hooks) = &self.claim_hooks {
            hooks.pause(phase).await;
        }
    }

    #[cfg(test)]
    pub(super) fn install_staged_stop_completions_for_test(
        &mut self,
        entries: Vec<StagedStopCompletionForTest>,
    ) -> bool {
        if entries.len() != 2 {
            return false;
        }
        let mut task_ids = Vec::with_capacity(entries.len());
        for entry in &entries {
            let batch_identity = DurableOperationIdentity::stop_intent_batch(vec![entry.identity])
                .expect("one test stop identity is a valid batch");
            let predecessor_identity = entry.predecessor.identity();
            let predecessor_is_earlier = match predecessor_identity {
                DurableOperationIdentity::TaskMutation(predecessor) => {
                    predecessor.task_id == entry.identity.task_id
                        && predecessor.sequence.get() < entry.identity.sequence.get()
                }
                DurableOperationIdentity::StopIntentBatch { .. }
                | DurableOperationIdentity::CreateTask { .. }
                | DurableOperationIdentity::RetryTask { .. } => false,
            };
            let active_is_ready = self
                .active
                .get(&entry.identity.task_id)
                .is_some_and(|active| {
                    active.phase == AdmissionPhase::Running
                        && active.preparation_complete
                        && active.in_flight_mutations == 0
                        && matches!(active.stop_state, ActiveStopState::NoWinner)
                        && entry.request.task_id == entry.identity.task_id
                        && entry.request.expected_repository_id == active.repository_id
                        && entry.request.expected_attempt == active.attempt
                        && entry.identity.kind == DurableOperationKind::PersistStopIntent
                        && entry.completion.identity == batch_identity
                });
            if !predecessor_is_earlier
                || !active_is_ready
                || entry.identity.sequence.get().checked_add(1).is_none()
                || task_ids.contains(&entry.identity.task_id)
                || self.pending_durable_results.contains(&entry.predecessor)
            {
                return false;
            }
            task_ids.push(entry.identity.task_id);
        }
        let Ok(entry_count) = u64::try_from(entries.len()) else {
            return false;
        };
        let Some(next_barrier_epoch) = self.exact_barrier_epoch.checked_add(entry_count) else {
            return false;
        };

        for entry in entries {
            let next_sequence = entry
                .identity
                .sequence
                .get()
                .checked_add(1)
                .expect("test stop sequence was preflighted");
            let Some(active) = self.active.get_mut(&entry.identity.task_id) else {
                return false;
            };
            active.stop_state = ActiveStopState::IntentWritePending {
                kind: entry.request.kind,
                identity: entry.identity,
                request: entry.request,
                deadline: Instant::now() + Duration::from_secs(10),
                retries_remaining: STOP_WRITE_RETRY_LIMIT,
            };
            active.next_mutation_sequence = next_sequence;
            active.durable_sequence_blocked = true;
            active.in_flight_mutations = 1;
            self.pending_durable_results.push(entry.predecessor);
            self.staged_stop_intent_completions
                .push_back(StagedStopIntentCompletion {
                    identity: DurableOperationIdentity::stop_intent_batch(vec![entry.identity])
                        .expect("one staged test stop identity is a valid batch"),
                    completion: entry.completion,
                });
        }
        self.exact_barrier_epoch = next_barrier_epoch;
        true
    }

    #[cfg(test)]
    pub(super) fn resolve_canonical_predecessor_for_test(
        &mut self,
        predecessor: &PendingDurableResult,
    ) -> bool {
        if !self.release_canonical_predecessor_for_test(predecessor) {
            return false;
        }
        self.drain_staged_stop_intent_completions() == StopCompletionDrain::Continue
    }

    #[cfg(test)]
    pub(super) fn release_canonical_predecessor_for_test(
        &mut self,
        predecessor: &PendingDurableResult,
    ) -> bool {
        let Some(index) = self
            .pending_durable_results
            .iter()
            .position(|pending| pending == predecessor)
        else {
            return false;
        };
        let task_id = match predecessor.identity() {
            DurableOperationIdentity::TaskMutation(identity) => identity.task_id,
            DurableOperationIdentity::StopIntentBatch { .. }
            | DurableOperationIdentity::CreateTask { .. }
            | DurableOperationIdentity::RetryTask { .. } => return false,
        };
        self.pending_durable_results.remove(index);
        let Some(active) = self.active.get_mut(&task_id) else {
            return false;
        };
        let Some(in_flight_mutations) = active.in_flight_mutations.checked_sub(1) else {
            return false;
        };
        active.in_flight_mutations = in_flight_mutations;
        active.durable_sequence_blocked = self
            .pending_durable_results
            .iter()
            .any(|pending| durable_identity_contains_task(&pending.identity(), task_id));
        true
    }

    #[cfg(test)]
    pub(super) fn install_historical_record_review_pair_for_test(
        &mut self,
        task_id: TaskId,
        requests: [RecordReviewRequest; 2],
        review_responses: [oneshot::Sender<Result<EventId, RunnerEventError>>; 2],
    ) -> Option<[(TaskMutationIdentity, RecordReviewRequest); 2]> {
        let valid = self.active.get(&task_id).is_some_and(|active| {
            active.phase == AdmissionPhase::Running
                && active.preparation_complete
                && active.in_flight_mutations == 0
                && active.pending_runner_event_writes.is_empty()
                && active.pending_record_review_writes.is_empty()
                && active.pending_record_review_replays.is_empty()
                && requests.iter().all(|request| {
                    request.task_id == task_id
                        && request.expected_repository_id == active.repository_id
                        && request.expected_attempt == active.attempt
                })
        });
        if !valid {
            return None;
        }
        let operation_nonce = self.active.get(&task_id)?.operation_nonce;
        let deadline = self.current_persistence_deadline();
        let mut staged = Vec::with_capacity(2);
        for (request, response) in requests.into_iter().zip(review_responses) {
            let lineage_id = {
                let active = self.active.get_mut(&task_id)?;
                let lineage_id = active.next_runner_mutation_id;
                active.next_runner_mutation_id = lineage_id.checked_add(1)?;
                lineage_id
            };
            let attempt_id = self.next_typed_write_attempt_id()?;
            let identity =
                self.next_mutation_identity(task_id, DurableOperationKind::RecordReview)?;
            staged.push((
                lineage_id,
                identity,
                PendingRecordReviewReplay {
                    lineage_id,
                    attempt_id,
                    operation_nonce,
                    request,
                    deadline,
                    response: Some(response),
                    deferred_original: None,
                    deferred_observers: Vec::new(),
                },
            ));
        }
        let entries = [
            (staged[0].1, staged[0].2.request.clone()),
            (staged[1].1, staged[1].2.request.clone()),
        ];
        self.writer
            .stage_unresolved_mutations_for_test(&[entries[0].0, entries[1].0])
            .ok()?;
        {
            let active = self.active.get_mut(&task_id)?;
            active.in_flight_mutations = active.in_flight_mutations.checked_add(staged.len())?;
            active.durable_sequence_blocked = true;
            for (_, identity, pending) in staged {
                if active
                    .pending_record_review_replays
                    .insert(identity, pending)
                    .is_some()
                {
                    return None;
                }
            }
        }
        for (identity, request) in &entries {
            self.enter_degraded(Some(PendingDurableResult::RecordReview {
                identity: *identity,
                request: request.clone(),
            }));
        }
        Some(entries)
    }
}
