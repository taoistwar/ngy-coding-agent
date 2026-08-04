use super::*;

impl TaskManager {
    pub(super) fn next_typed_write_attempt_id(&mut self) -> Option<u64> {
        let attempt_id = self.next_typed_write_attempt_id;
        let next_attempt_id = attempt_id.checked_add(1)?;
        self.next_typed_write_attempt_id = next_attempt_id;
        Some(attempt_id)
    }

    pub(super) fn next_terminal_projection_attempt_id(&mut self) -> Option<u64> {
        let attempt_id = self.next_terminal_projection_attempt_id;
        let next_attempt_id = attempt_id.checked_add(1)?;
        self.next_terminal_projection_attempt_id = next_attempt_id;
        Some(attempt_id)
    }

    pub(super) fn next_mutation_identity(
        &mut self,
        task_id: TaskId,
        kind: DurableOperationKind,
    ) -> Option<TaskMutationIdentity> {
        if self.frozen {
            return None;
        }
        let identity = {
            let active = self.active.get_mut(&task_id)?;
            let sequence = NonZeroU64::new(active.next_mutation_sequence)?;
            active.next_mutation_sequence = active.next_mutation_sequence.checked_add(1)?;
            TaskMutationIdentity {
                task_id,
                sequence: MutationSequence::new(sequence),
                kind,
            }
        };
        if !self.advance_exact_barrier_epoch() {
            return None;
        }
        Some(identity)
    }

    pub(super) fn advance_exact_barrier_epoch(&mut self) -> bool {
        let Some(next_epoch) = self.exact_barrier_epoch.checked_add(1) else {
            self.freeze_degraded();
            return false;
        };
        self.exact_barrier_epoch = next_epoch;
        true
    }

    pub(super) fn exact_recovery_barriers_clear(&self) -> bool {
        self.detached_cancel_completions == 0
            && self.staged_stop_intent_completions.is_empty()
            && self.pending_durable_results.is_empty()
            && self.pending_replay_in_flight.is_none()
            && self.active.values().all(|active| {
                active.cleanup_confirmation.is_some()
                    && active.in_flight_mutations == 0
                    && active.pending_terminal_write.is_none()
                    && active.pending_runner_event_writes.is_empty()
                    && active.pending_record_review_writes.is_empty()
                    && active.phase == AdmissionPhase::RunnerReturned
                    && matches!(&active.stop_state, ActiveStopState::NoWinner)
                    && !active.accepted_stop_task_load_in_flight
                    && active.pending_record_review_replays.is_empty()
            })
    }

    pub(super) fn generic_recovery_barriers_clear(&self) -> bool {
        self.exact_recovery_barriers_clear()
            && self
                .active
                .values()
                .all(|active| active.recovery_release_ready)
    }

    pub(super) fn generic_recovery_attempt_is_exact(
        &self,
        attempt_id: u64,
        barrier_epoch: u64,
    ) -> bool {
        self.generic_recovery_attempt.is_some_and(|attempt| {
            attempt.attempt_id == attempt_id && attempt.barrier_epoch == barrier_epoch
        })
    }

    pub(super) fn generic_recovery_safety_generation(
        &self,
        attempt_id: u64,
        barrier_epoch: u64,
    ) -> Option<u64> {
        self.generic_recovery_attempt
            .filter(|attempt| {
                attempt.attempt_id == attempt_id && attempt.barrier_epoch == barrier_epoch
            })
            .map(|attempt| attempt.safety_generation)
    }

    pub(super) fn checked_recovery_safety_gate(&self) -> RecoverySafetyGate {
        let launch_barrier = Arc::clone(&self.shutdown.launch_barrier);
        let _launch_guard = launch_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let safety_registry = self.safety_registry.clone();
        let safety_guard = safety_registry.lock();
        recovery_safety_gate(&safety_guard, &self.active)
    }

    pub(super) fn supersede_generic_recovery_for_critical(
        &mut self,
        attempt_id: u64,
        barrier_epoch: u64,
    ) {
        if self.generic_recovery_attempt_is_exact(attempt_id, barrier_epoch) {
            self.generic_recovery_attempt = None;
        }
        self.handle_critical_wake();
        if !self.frozen {
            self.kick_exact_barrier_progress();
        }
    }

    pub(super) fn supersede_quiesce_recovery_for_critical(&mut self, quiesce_id: u64) {
        if let Some(pending) = self
            .pending_quiesce
            .as_mut()
            .filter(|pending| pending.quiesce_id == quiesce_id && pending.recovery_started)
        {
            pending.recovery_started = false;
            pending.recovery_safety_generation = None;
        } else {
            return;
        }
        self.handle_critical_wake();
        if !self.frozen {
            self.kick_exact_barrier_progress();
        }
    }

    pub(super) fn kick_exact_barrier_progress(&mut self) {
        if self.drain_staged_stop_intent_completions() == StopCompletionDrain::Stop {
            return;
        }
        if self.frozen {
            return;
        }

        self.maybe_start_degraded_recovery();
        if self.frozen
            || !self.pending_durable_results.is_empty()
            || self.pending_replay_in_flight.is_some()
            || self.generic_recovery_attempt.is_some()
        {
            return;
        }

        if self.drain_staged_stop_intent_completions() == StopCompletionDrain::Stop || self.frozen {
            return;
        }
        self.drain_deferred_stop_submissions();
        if self.frozen {
            return;
        }
        self.try_start_quiesce_recovery();
        self.maybe_start_degraded_recovery();
    }
}
