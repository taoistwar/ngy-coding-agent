use super::*;

impl TaskManager {
    pub(super) async fn handle_runner_returned(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        outcome: RunnerOutcome,
    ) {
        let cleanup_was_paused = self.process_cleanup_pauses_scheduler();
        let recovery_was_paused =
            self.exact_repository_control_recovery_pauses_admission(task_id, operation_nonce);
        let current = self.active.get(&task_id).is_some_and(|active| {
            active.operation_nonce == operation_nonce
                && matches!(
                    active.phase,
                    AdmissionPhase::Preparing | AdmissionPhase::Running
                )
        });
        if !current {
            return;
        }
        if !self.runner_return_matches_repository_control_recovery(
            task_id,
            operation_nonce,
            &outcome,
        ) {
            self.freeze_degraded();
            return;
        }
        let Some(active) = self.active.get_mut(&task_id) else {
            return;
        };
        active.runner_returned = Some(RunnerReturnedState::new(&active.process_scope));
        active.pending_runner_outcome = Some(outcome);
        active.phase = AdmissionPhase::RunnerReturned;
        self.try_confirm_process_cleanup(task_id, operation_nonce);
        self.refresh_scheduler_after_process_cleanup_change(
            cleanup_was_paused,
            recovery_was_paused,
        )
        .await;
    }

    pub(super) async fn handle_suppressed_launch_returned(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
    ) {
        let cleanup_was_paused = self.process_cleanup_pauses_scheduler();
        let (repository_id, coordination_key, user_stop_accepted) = {
            let Some(active) = self.active.get(&task_id) else {
                return;
            };
            if active.operation_nonce != operation_nonce
                || active.phase != AdmissionPhase::LaunchSuppressed
            {
                return;
            }
            let Some(reason) = active.launch_suppression else {
                self.freeze_degraded();
                return;
            };
            let _ = reason;
            (
                active.repository_id,
                active.permit.coordination_key(),
                active.stop_state.kind() == Some(StopIntentKind::UserCancelled),
            )
        };
        let Some(stop_state) =
            self.safety_registry
                .launch_stop_state(task_id, operation_nonce, coordination_key)
        else {
            self.freeze_degraded();
            return;
        };
        match classify_launch_suppression(
            self.claims_allowed(),
            self.storage_admission.launch_allowed(repository_id),
            stop_state,
            user_stop_accepted,
        ) {
            Ok(Some(reason)) => reason,
            Ok(None) | Err(()) => {
                self.freeze_degraded();
                return;
            }
        };
        let Some(active) = self.active.get_mut(&task_id) else {
            return;
        };
        active.launch_suppression = None;
        active.runner_returned = Some(RunnerReturnedState::new(&active.process_scope));
        active.pending_runner_outcome = Some(RunnerOutcome::Cancelled);
        active.phase = AdmissionPhase::RunnerReturned;
        self.try_confirm_process_cleanup(task_id, operation_nonce);
        self.refresh_scheduler_after_process_cleanup_change(cleanup_was_paused, false)
            .await;
    }

    pub(super) async fn handle_process_cleanup_retry(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
    ) {
        let cleanup_was_paused = self.process_cleanup_pauses_scheduler();
        let Some(active) = self.active.get_mut(&task_id) else {
            return;
        };
        if active.operation_nonce != operation_nonce
            || active.phase != AdmissionPhase::RunnerReturned
            || !active.cleanup_retry_scheduled
        {
            return;
        }
        active.cleanup_retry_scheduled = false;
        self.try_confirm_process_cleanup(task_id, operation_nonce);
        self.refresh_scheduler_after_process_cleanup_change(cleanup_was_paused, false)
            .await;
    }

    pub(super) fn try_confirm_process_cleanup(&mut self, task_id: TaskId, operation_nonce: u64) {
        let (returned, process_scope) = {
            let Some(active) = self.active.get(&task_id) else {
                return;
            };
            if active.operation_nonce != operation_nonce
                || active.phase != AdmissionPhase::RunnerReturned
                || active.cleanup_confirmation.is_some()
            {
                return;
            }
            let Some(returned) = active.runner_returned else {
                self.freeze_degraded();
                return;
            };
            (returned, active.process_scope.clone())
        };
        match TaskProcessCleanupConfirmation::try_new(returned, &process_scope) {
            Ok(cleanup) => {
                if !self.settle_repository_control_after_process_cleanup(task_id, operation_nonce) {
                    self.freeze_degraded();
                    return;
                }
                let pending_outcome = {
                    let Some(active) = self.active.get_mut(&task_id) else {
                        return;
                    };
                    if active.operation_nonce != operation_nonce
                        || active.phase != AdmissionPhase::RunnerReturned
                    {
                        return;
                    }
                    active.cleanup_confirmation = Some(cleanup);
                    active.cleanup_retry_scheduled = false;
                    (active.in_flight_mutations == 0)
                        .then(|| active.pending_runner_outcome.take())
                        .flatten()
                };
                self.continue_after_running_mutation(task_id, operation_nonce, pending_outcome);
            }
            Err(error) => {
                tracing::warn!(
                    %task_id,
                    error = %error,
                    "runner process cleanup remains unconfirmed"
                );
                let Some(active) = self.active.get_mut(&task_id) else {
                    return;
                };
                if active.operation_nonce != operation_nonce
                    || active.phase != AdmissionPhase::RunnerReturned
                    || active.cleanup_retry_scheduled
                {
                    return;
                }
                active.cleanup_retry_scheduled = true;
                let completion_sender = self.completion_sender.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(PROCESS_CLEANUP_RETRY_INTERVAL).await;
                    let _ = completion_sender
                        .send(TaskManagerCompletion::ProcessCleanupRetry {
                            task_id,
                            operation_nonce,
                        })
                        .await;
                });
            }
        }
    }
}
