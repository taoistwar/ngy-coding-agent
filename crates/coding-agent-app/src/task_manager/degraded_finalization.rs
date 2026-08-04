use super::*;

impl TaskManager {
    pub(super) fn handle_degraded_finalization_load_error(
        &mut self,
        attempt_id: u64,
        barrier_epoch: u64,
        error: StoreError,
    ) -> Result<DegradedRecoveryResult, DegradedCoordinatorError> {
        let Some(expected_safety_generation) =
            self.generic_recovery_safety_generation(attempt_id, barrier_epoch)
        else {
            return Err(DegradedCoordinatorError::Superseded);
        };
        let launch_barrier = Arc::clone(&self.shutdown.launch_barrier);
        let launch_guard = launch_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let safety_registry = self.safety_registry.clone();
        let safety_guard = safety_registry.lock();
        match recovery_safety_gate(&safety_guard, &self.active) {
            RecoverySafetyGate::Exact(generation) if generation == expected_safety_generation => {}
            RecoverySafetyGate::Exact(_) | RecoverySafetyGate::CriticalPending => {
                drop(safety_guard);
                drop(launch_guard);
                self.supersede_generic_recovery_for_critical(attempt_id, barrier_epoch);
                return Err(DegradedCoordinatorError::Superseded);
            }
            RecoverySafetyGate::Conflict => {
                self.freeze_degraded_while_launch_barrier_held();
                drop(safety_guard);
                drop(launch_guard);
                return Err(DegradedCoordinatorError::TypedConflict);
            }
        }
        if self.exact_barrier_epoch != barrier_epoch || !self.generic_recovery_barriers_clear() {
            drop(safety_guard);
            drop(launch_guard);
            return Err(DegradedCoordinatorError::Superseded);
        }
        if self.is_frozen() || self.service_state.current().state == ServiceState::Quiescing {
            drop(safety_guard);
            drop(launch_guard);
            return Err(DegradedCoordinatorError::Quiescing);
        }
        tracing::error!(error = %error, "degraded finalization task load failed");
        self.freeze_degraded_while_launch_barrier_held();
        drop(safety_guard);
        drop(launch_guard);
        Err(DegradedCoordinatorError::TypedConflict)
    }

    pub(super) fn start_finalize_degraded(
        &mut self,
        attempt_id: u64,
        barrier_epoch: u64,
        recovery: coding_agent_store::RecoveryOutcome,
        replayed_pending_count: usize,
        high_watermark: EventCursor,
        response: oneshot::Sender<Result<DegradedRecoveryResult, DegradedCoordinatorError>>,
    ) {
        let Some(expected_safety_generation) =
            self.generic_recovery_safety_generation(attempt_id, barrier_epoch)
        else {
            let _ = response.send(Err(DegradedCoordinatorError::Superseded));
            return;
        };
        match self.checked_recovery_safety_gate() {
            RecoverySafetyGate::Exact(generation) if generation == expected_safety_generation => {}
            RecoverySafetyGate::Exact(_) | RecoverySafetyGate::CriticalPending => {
                self.supersede_generic_recovery_for_critical(attempt_id, barrier_epoch);
                let _ = response.send(Err(DegradedCoordinatorError::Superseded));
                return;
            }
            RecoverySafetyGate::Conflict => {
                self.freeze_degraded();
                let _ = response.send(Err(DegradedCoordinatorError::TypedConflict));
                return;
            }
        }
        if self.exact_barrier_epoch != barrier_epoch || !self.generic_recovery_barriers_clear() {
            let _ = response.send(Err(DegradedCoordinatorError::Superseded));
            return;
        }
        if self.is_frozen() || self.service_state.current().state == ServiceState::Quiescing {
            let _ = response.send(Err(DegradedCoordinatorError::Quiescing));
            return;
        }
        if !self.degraded {
            let _ = response.send(Err(DegradedCoordinatorError::ManagerClosed));
            return;
        }
        if high_watermark < recovery.high_watermark {
            self.freeze_degraded();
            let _ = response.send(Err(DegradedCoordinatorError::TypedConflict));
            return;
        }
        let task_ids = self.active.keys().copied().collect::<Vec<_>>();
        let store = self.store.clone();
        let scheduler_snapshot_read_gate = Arc::clone(&self.scheduler_snapshot_read_gate);
        let completion_sender = self.completion_sender.clone();
        tokio::spawn(async move {
            let read_guard = scheduler_snapshot_read_gate.lock().await;
            let result = match store.scheduler_bootstrap_snapshot().await {
                Ok(scheduler_snapshot) => terminal_tasks_from_scheduler_snapshot(
                    &scheduler_snapshot,
                    &task_ids,
                )
                .map(|terminal_tasks| DegradedFinalizationReceipt {
                    recovery,
                    replayed_pending_count,
                    terminal_tasks,
                    projection: high_watermark,
                    scheduler_snapshot,
                })
                .ok_or(StoreError::InvariantViolation(
                    "degraded recovery snapshot did not contain the exact active terminal set",
                )),
                Err(error) => Err(error),
            };
            drop(read_guard);
            let _ = completion_sender
                .send(TaskManagerCompletion::DegradedFinalizationLoaded {
                    attempt_id,
                    barrier_epoch,
                    result,
                    response,
                })
                .await;
        });
    }

    pub(super) fn finalize_degraded(
        &mut self,
        attempt_id: u64,
        barrier_epoch: u64,
        receipt: DegradedFinalizationReceipt,
    ) -> Result<DegradedRecoveryResult, DegradedCoordinatorError> {
        let Some(expected_safety_generation) =
            self.generic_recovery_safety_generation(attempt_id, barrier_epoch)
        else {
            return Err(DegradedCoordinatorError::Superseded);
        };
        match self.checked_recovery_safety_gate() {
            RecoverySafetyGate::Exact(generation) if generation == expected_safety_generation => {}
            RecoverySafetyGate::Exact(_) | RecoverySafetyGate::CriticalPending => {
                self.supersede_generic_recovery_for_critical(attempt_id, barrier_epoch);
                return Err(DegradedCoordinatorError::Superseded);
            }
            RecoverySafetyGate::Conflict => {
                self.freeze_degraded();
                return Err(DegradedCoordinatorError::TypedConflict);
            }
        }
        if self.exact_barrier_epoch != barrier_epoch || !self.generic_recovery_barriers_clear() {
            return Err(DegradedCoordinatorError::Superseded);
        }
        if self.is_frozen() || self.service_state.current().state == ServiceState::Quiescing {
            return Err(DegradedCoordinatorError::Quiescing);
        }
        if !self.degraded || receipt.projection < receipt.recovery.high_watermark {
            self.freeze_degraded();
            return Err(DegradedCoordinatorError::TypedConflict);
        }

        let launch_barrier = Arc::clone(&self.shutdown.launch_barrier);
        let launch_guard = launch_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let safety_registry = self.safety_registry.clone();
        let mut safety_guard = safety_registry.lock();
        match recovery_safety_gate(&safety_guard, &self.active) {
            RecoverySafetyGate::Exact(generation)
                if generation == expected_safety_generation
                    && self.exact_barrier_epoch == barrier_epoch
                    && self.generic_recovery_barriers_clear() => {}
            RecoverySafetyGate::Exact(_) | RecoverySafetyGate::CriticalPending => {
                drop(safety_guard);
                drop(launch_guard);
                self.supersede_generic_recovery_for_critical(attempt_id, barrier_epoch);
                return Err(DegradedCoordinatorError::Superseded);
            }
            RecoverySafetyGate::Conflict => {
                drop(safety_guard);
                drop(launch_guard);
                self.freeze_degraded();
                return Err(DegradedCoordinatorError::TypedConflict);
            }
        }

        if receipt.scheduler_snapshot.latest_event_id < receipt.projection {
            drop(safety_guard);
            drop(launch_guard);
            self.freeze_degraded();
            return Err(DegradedCoordinatorError::TypedConflict);
        }
        let published_membership =
            match self.publish_scheduler_snapshot(&receipt.scheduler_snapshot) {
                Ok(published) => published,
                Err(error) => {
                    tracing::error!(
                        %error,
                        "degraded recovery scheduler projection publication failed"
                    );
                    drop(safety_guard);
                    drop(launch_guard);
                    self.freeze_degraded();
                    return Err(DegradedCoordinatorError::TypedConflict);
                }
            };
        let permit_ledger = self.permit_ledger.clone();
        let release = commit_recovery_terminal_release(
            &permit_ledger,
            &mut self.active,
            &mut safety_guard,
            RecoveryTerminalReleaseRequest::for_degraded(
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
                tracing::error!(%error, "atomic degraded terminal release failed");
                self.freeze_degraded();
                return Err(DegradedCoordinatorError::TypedConflict);
            }
        };
        let shutdown_handles = self.finalize_terminal_release_commit(commit);
        debug_assert!(shutdown_handles.is_empty());

        self.degraded_replayed_pending_count = 0;
        self.degraded_replay_high_watermark = None;
        // Open the actor-local gate while ServiceState is still non-Ready.
        // The actor cannot process another claim until this method returns,
        // and external mutation entry remains closed until `set(Ready)`.
        self.degraded = false;
        let ready = match self.service_state.set(ServiceState::Ready) {
            Ok(ready) => ready,
            Err(_) => {
                self.degraded = true;
                self.scan_requested = false;
                return Err(DegradedCoordinatorError::Quiescing);
            }
        };
        if let Err(error) = self.publish_scheduler_snapshot(&receipt.scheduler_snapshot) {
            tracing::error!(
                %error,
                "ready service generation scheduler publication failed"
            );
            self.scan_requested = false;
            self.freeze_degraded();
            return Err(DegradedCoordinatorError::TypedConflict);
        }
        self.scan_requested = !self.main_closed;
        let result = DegradedRecoveryResult {
            recovery: receipt.recovery,
            replayed_pending_count: receipt.replayed_pending_count,
            ready_generation: ready.generation,
        };
        let _ = self.degraded_recoveries.send(result.clone());
        Ok(result)
    }
}
