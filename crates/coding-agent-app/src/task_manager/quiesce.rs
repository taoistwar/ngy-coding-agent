use super::*;

impl TaskManager {
    pub(super) fn begin_quiesce(
        &mut self,
        deadline: Instant,
        response: oneshot::Sender<Result<QuiesceResult, TaskManagerError>>,
    ) {
        if self.frozen {
            let _ = response.send(Err(TaskManagerError::Frozen));
            return;
        }
        if !self.shutdown.try_freeze() {
            let _ = response.send(Err(TaskManagerError::Frozen));
            return;
        }
        self.start_pending_quiesce(deadline, response);
    }

    pub(super) fn begin_shutdown_finalization(
        &mut self,
        proof: ShutdownProcessCleanupProof,
        deadline: Instant,
        response: oneshot::Sender<Result<QuiesceResult, TaskManagerError>>,
    ) {
        let proof_is_exact = proof.tracker_id == self.shutdown.process_cleanup.id
            && self.shutdown.process_cleanup.all_registered_confirmed();
        if !proof_is_exact {
            let _ = response.send(Err(TaskManagerError::Invariant(
                "shutdown process cleanup proof did not match the frozen launch set",
            )));
            return;
        }
        if self.frozen {
            let _ = response.send(Err(TaskManagerError::Frozen));
            return;
        }
        if !self.shutdown.is_frozen() {
            let _ = response.send(Err(TaskManagerError::Invariant(
                "shutdown finalization requires a frozen launch set",
            )));
            return;
        }
        if self.pending_quiesce.is_some() {
            let _ = response.send(Err(TaskManagerError::Invariant(
                "shutdown finalization already has a pending quiesce",
            )));
            return;
        }
        self.start_pending_quiesce(deadline, response);
    }

    pub(super) fn start_pending_quiesce(
        &mut self,
        deadline: Instant,
        response: oneshot::Sender<Result<QuiesceResult, TaskManagerError>>,
    ) {
        let quiesce_id = self.next_quiesce_id;
        let Some(next_quiesce_id) = quiesce_id.checked_add(1) else {
            let _ = response.send(Err(TaskManagerError::Invariant(
                "quiesce identity overflow",
            )));
            return;
        };
        self.next_quiesce_id = next_quiesce_id;
        self.scan_requested = false;
        self.finish_scan();
        let _ = self.service_state.set(ServiceState::Quiescing);
        for active in self.active.values() {
            active.cancellation.cancel();
        }
        self.pending_quiesce = Some(PendingQuiesce {
            quiesce_id,
            deadline,
            response,
            recovery_started: false,
            recovery_safety_generation: None,
        });
        self.clamp_persistence_deadlines_to_quiesce(deadline);
        if Instant::now() >= deadline {
            self.expire_pending_quiesce(Instant::now());
            return;
        }
        self.schedule_quiesce_deadline(quiesce_id, deadline);
        self.maybe_start_degraded_recovery();
        self.try_start_quiesce_recovery();
    }

    pub(super) fn clamp_persistence_deadlines_to_quiesce(&mut self, deadline: Instant) {
        if let Some(attempt) = self.pending_replay_in_flight.as_mut() {
            attempt.deadline = attempt.deadline.min(deadline);
        }
        for active in self.active.values_mut() {
            if let Some(pending) = active.pending_terminal_write.as_mut() {
                pending.deadline = pending.deadline.min(deadline);
            }
            for pending in active.pending_runner_event_writes.values_mut() {
                pending.deadline = pending.deadline.min(deadline);
            }
            for pending in active.pending_record_review_writes.values_mut() {
                pending.deadline = pending.deadline.min(deadline);
            }
            for pending in active.pending_record_review_replays.values_mut() {
                pending.deadline = pending.deadline.min(deadline);
            }
            match &mut active.stop_state {
                ActiveStopState::IntentSubmissionDeferred {
                    deadline: active_deadline,
                    ..
                }
                | ActiveStopState::IntentWritePending {
                    deadline: active_deadline,
                    ..
                }
                | ActiveStopState::FinalStopWritePending {
                    deadline: active_deadline,
                    ..
                } => {
                    *active_deadline = (*active_deadline).min(deadline);
                }
                ActiveStopState::NoWinner
                | ActiveStopState::IntentDurable { .. }
                | ActiveStopState::StopTerminal { .. }
                | ActiveStopState::TerminalWon { .. } => {}
            }
        }
    }

    pub(super) fn schedule_quiesce_deadline(&self, quiesce_id: u64, deadline: Instant) {
        let completion_sender = self.completion_sender.clone();
        tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            let _ = completion_sender
                .send(TaskManagerCompletion::QuiesceDeadlineElapsed {
                    quiesce_id,
                    deadline,
                })
                .await;
        });
    }

    pub(super) fn handle_quiesce_deadline_elapsed(&mut self, quiesce_id: u64, deadline: Instant) {
        let exact = self.pending_quiesce.as_ref().is_some_and(|pending| {
            pending.quiesce_id == quiesce_id && pending.deadline == deadline
        });
        if !exact {
            return;
        }
        if Instant::now() < deadline {
            self.schedule_quiesce_deadline(quiesce_id, deadline);
            return;
        }
        self.expire_pending_quiesce(Instant::now());
    }

    pub(super) fn expire_pending_quiesce(&mut self, now: Instant) {
        let expired = self
            .pending_quiesce
            .as_ref()
            .is_some_and(|pending| now >= pending.deadline);
        if !expired {
            return;
        }
        let PendingQuiesce { response, .. } = self
            .pending_quiesce
            .take()
            .expect("expired quiesce remains actor-owned");
        self.frozen = true;
        let active = self.take_shutdown_handles();
        let _ = response.send(Ok(QuiesceResult::Frozen {
            active,
            error: StoreWriterError::DeadlineElapsed,
        }));
    }

    pub(super) fn try_start_quiesce_recovery(&mut self) {
        self.expire_pending_quiesce(Instant::now());
        if self.pending_quiesce.is_none() {
            return;
        }
        if !self.pending_durable_results.is_empty()
            || self.pending_replay_in_flight.is_some()
            || self.generic_recovery_attempt.is_some()
        {
            return;
        }
        let stop_candidates = self
            .active
            .iter()
            .filter_map(|(task_id, active)| {
                (active.cleanup_confirmation.is_some()
                    && active.in_flight_mutations == 0
                    && !matches!(&active.stop_state, ActiveStopState::NoWinner))
                .then_some((*task_id, active.operation_nonce))
            })
            .collect::<Vec<_>>();
        for (task_id, operation_nonce) in stop_candidates {
            self.advance_stop_after_barriers(task_id, operation_nonce);
        }
        if self.pending_quiesce.is_none() || !self.exact_recovery_barriers_clear() {
            return;
        }
        let (quiesce_id, deadline, recovery_started) = self
            .pending_quiesce
            .as_ref()
            .map(|pending| {
                (
                    pending.quiesce_id,
                    pending.deadline,
                    pending.recovery_started,
                )
            })
            .expect("quiesce recovery was checked above");
        if recovery_started {
            return;
        }
        if Instant::now() >= deadline {
            self.expire_pending_quiesce(Instant::now());
            return;
        }
        let recovery_safety_generation = match self.checked_recovery_safety_gate() {
            RecoverySafetyGate::Exact(safety_generation) => safety_generation,
            RecoverySafetyGate::CriticalPending => {
                self.handle_critical_wake();
                return;
            }
            RecoverySafetyGate::Conflict => {
                let PendingQuiesce { response, .. } = self
                    .pending_quiesce
                    .take()
                    .expect("conflicted quiesce remains actor-owned");
                self.freeze_degraded();
                let _ = response.send(Err(TaskManagerError::Invariant(
                    "quiesce recovery safety registry conflicted",
                )));
                return;
            }
        };
        let pending_quiesce = self
            .pending_quiesce
            .as_mut()
            .expect("quiesce remains actor-owned before recovery spawn");
        pending_quiesce.recovery_started = true;
        pending_quiesce.recovery_safety_generation = Some(recovery_safety_generation);
        let task_ids = self.active.keys().copied().collect::<Vec<_>>();
        let writer = self.writer.clone();
        let store = self.store.clone();
        let dispatcher = self.dispatcher.clone();
        let scheduler_snapshot_read_gate = Arc::clone(&self.scheduler_snapshot_read_gate);
        let completion_sender = self.completion_sender.clone();
        #[cfg(feature = "test-support")]
        let actor_pauses = self.actor_pauses.clone();
        tokio::spawn(async move {
            #[cfg(feature = "test-support")]
            if let Some(actor_pauses) = actor_pauses {
                actor_pauses
                    .pause(ActorPausePoint::QuiesceBeforeRecovery)
                    .await;
            }
            let result = match writer
                .interrupt_remaining_after_stops(shutdown_failure(), deadline)
                .await
            {
                Ok(receipt) => {
                    let generic = receipt.value;
                    let recovery = coding_agent_store::RecoveryOutcome {
                        interrupted_count: generic.interrupted_count,
                        first_event_id: generic.first_event_id,
                        last_event_id: generic.last_event_id,
                        high_watermark: generic.high_watermark,
                    };
                    if dispatcher.flush_to(recovery.high_watermark).await.is_err() {
                        Err(StoreWriterError::Closed)
                    } else {
                        let _read_guard = scheduler_snapshot_read_gate.lock().await;
                        match store.scheduler_bootstrap_snapshot().await {
                            Ok(scheduler_snapshot) => {
                                match terminal_tasks_from_scheduler_snapshot(
                                    &scheduler_snapshot,
                                    &task_ids,
                                ) {
                                    Some(terminal_tasks) => Ok(QuiesceRecoveryReceipt {
                                        projection: recovery.high_watermark,
                                        recovery,
                                        terminal_tasks,
                                        scheduler_snapshot,
                                    }),
                                    None => Err(StoreWriterError::Store(
                                        StoreError::InvariantViolation(
                                            "quiesce recovery snapshot did not contain the exact active terminal set",
                                        ),
                                    )),
                                }
                            }
                            Err(error) => Err(StoreWriterError::Store(error)),
                        }
                    }
                }
                Err(error) => Err(error),
            };
            let _ = completion_sender
                .send(TaskManagerCompletion::QuiesceRecovered { quiesce_id, result })
                .await;
        });
    }

    pub(super) fn handle_quiesce_recovered(
        &mut self,
        quiesce_id: u64,
        result: Result<QuiesceRecoveryReceipt, StoreWriterError>,
    ) {
        let Some((expected_safety_generation, deadline)) = self
            .pending_quiesce
            .as_ref()
            .filter(|pending| pending.quiesce_id == quiesce_id && pending.recovery_started)
            .and_then(|pending| {
                pending
                    .recovery_safety_generation
                    .map(|generation| (generation, pending.deadline))
            })
        else {
            return;
        };

        let launch_barrier = Arc::clone(&self.shutdown.launch_barrier);
        let launch_guard = launch_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let safety_registry = self.safety_registry.clone();
        let mut safety_guard = safety_registry.lock();
        match recovery_safety_gate(&safety_guard, &self.active) {
            RecoverySafetyGate::Exact(generation)
                if generation == expected_safety_generation
                    && self.exact_recovery_barriers_clear() => {}
            RecoverySafetyGate::Exact(_) | RecoverySafetyGate::CriticalPending => {
                drop(safety_guard);
                drop(launch_guard);
                self.supersede_quiesce_recovery_for_critical(quiesce_id);
                return;
            }
            RecoverySafetyGate::Conflict => {
                drop(safety_guard);
                drop(launch_guard);
                let PendingQuiesce { response, .. } = self
                    .pending_quiesce
                    .take()
                    .expect("conflicted quiesce completion remains actor-owned");
                self.freeze_degraded();
                let _ = response.send(Err(TaskManagerError::Invariant(
                    "quiesce recovery safety registry conflicted before final release",
                )));
                return;
            }
        }
        if Instant::now() >= deadline {
            drop(safety_guard);
            drop(launch_guard);
            self.expire_pending_quiesce(Instant::now());
            return;
        }

        let receipt = match result {
            Ok(receipt) => receipt,
            Err(error) => {
                drop(safety_guard);
                drop(launch_guard);
                let PendingQuiesce { response, .. } = self
                    .pending_quiesce
                    .take()
                    .expect("failed quiesce completion remains actor-owned");
                self.frozen = true;
                let active = self.take_shutdown_handles();
                let _ = response.send(Ok(QuiesceResult::Frozen { active, error }));
                return;
            }
        };
        if receipt.scheduler_snapshot.latest_event_id < receipt.recovery.high_watermark {
            drop(safety_guard);
            drop(launch_guard);
            let PendingQuiesce { response, .. } = self
                .pending_quiesce
                .take()
                .expect("inexact quiesce publication remains actor-owned");
            self.freeze_degraded();
            let _ = response.send(Err(TaskManagerError::Invariant(
                "quiesce scheduler snapshot is behind recovery",
            )));
            return;
        }
        let published_membership =
            match self.publish_scheduler_snapshot(&receipt.scheduler_snapshot) {
                Ok(published) => published,
                Err(error) => {
                    tracing::error!(%error, "quiesce scheduler projection publication failed");
                    drop(safety_guard);
                    drop(launch_guard);
                    let PendingQuiesce { response, .. } = self
                        .pending_quiesce
                        .take()
                        .expect("failed quiesce publication remains actor-owned");
                    self.freeze_degraded();
                    let _ = response.send(Err(TaskManagerError::Invariant(
                        "quiesce scheduler membership publication failed",
                    )));
                    return;
                }
            };
        let permit_ledger = self.permit_ledger.clone();
        let release = commit_recovery_terminal_release(
            &permit_ledger,
            &mut self.active,
            &mut safety_guard,
            RecoveryTerminalReleaseRequest::for_quiesce(
                &receipt.terminal_tasks,
                receipt.projection,
                published_membership,
                expected_safety_generation,
            ),
        );
        drop(safety_guard);
        drop(launch_guard);
        let commit = match release {
            Ok(commit) => commit,
            Err(error) => {
                tracing::error!(%error, "atomic quiesce terminal release failed");
                let PendingQuiesce { response, .. } = self
                    .pending_quiesce
                    .take()
                    .expect("failed quiesce release remains actor-owned");
                self.freeze_degraded();
                let _ = response.send(Err(TaskManagerError::Invariant(
                    "quiesce terminal ownership release failed",
                )));
                return;
            }
        };
        let active = self.finalize_terminal_release_commit(commit);
        if let Err(error) = self.publish_scheduler_snapshot(&receipt.scheduler_snapshot) {
            tracing::error!(
                %error,
                "quiesce released-permit scheduler projection failed"
            );
            let PendingQuiesce { response, .. } = self
                .pending_quiesce
                .take()
                .expect("failed quiesce republish remains actor-owned");
            self.freeze_degraded();
            let _ = response.send(Err(TaskManagerError::Invariant(
                "quiesce released-permit scheduler publication failed",
            )));
            return;
        }
        let PendingQuiesce { response, .. } = self
            .pending_quiesce
            .take()
            .expect("committed quiesce completion remains actor-owned");
        let _ = response.send(Ok(QuiesceResult::Durable {
            recovery: receipt.recovery,
            active,
        }));
    }
}
