use std::panic::AssertUnwindSafe;

use futures_util::FutureExt as _;

use super::*;

struct PreparedClaimLaunch {
    task: Task,
    context: RunContext,
    shutdown_process_scope: TaskProcessScopeOwnership,
}

enum ClaimLaunchPreparationError {
    OwnershipInvariant,
    Context(crate::run_context::RunContextOwnershipError),
}

impl TaskManager {
    pub(super) fn suppress_claimed_launch(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        reason: LaunchSuppressionReason,
    ) {
        let clean_release = self.active.get_mut(&task_id).is_some_and(|active| {
            if active.operation_nonce != operation_nonce
                || !matches!(
                    active.phase,
                    AdmissionPhase::ClaimPending
                        | AdmissionPhase::ClaimUnknown
                        | AdmissionPhase::LaunchGatePending
                )
            {
                return false;
            }
            let Some(lease) = active.control_lease.take() else {
                return false;
            };
            if lease.clean_release().is_err() {
                return false;
            }
            active.phase = AdmissionPhase::LaunchSuppressed;
            active.launch_suppression = Some(reason);
            true
        });
        if !clean_release {
            self.freeze_degraded();
            return;
        }
        let completion_sender = self.completion_sender.clone();
        tokio::spawn(async move {
            let _ = completion_sender
                .send(TaskManagerCompletion::SuppressedLaunchReturned {
                    task_id,
                    operation_nonce,
                })
                .await;
        });
    }

    pub(super) fn finish_claim_launch(&mut self, task_id: TaskId, operation_nonce: u64) {
        if self.defer_claim_launch_for_buffered_messages(task_id, operation_nonce) {
            return;
        }
        let Some(active) = self.active.get(&task_id) else {
            return;
        };
        if active.operation_nonce != operation_nonce
            || active.phase != AdmissionPhase::LaunchGatePending
        {
            return;
        }
        let Some(message_sender) = self.sender.upgrade() else {
            self.freeze_degraded();
            return;
        };

        let shutdown = Arc::clone(&self.shutdown);
        let launch_barrier = shutdown
            .launch_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let safety_registry = self.safety_registry.clone();
        let safety_gate = safety_registry.lock();
        let (repository_id, coordination_key, user_stop_accepted) = {
            let active = self
                .active
                .get(&task_id)
                .expect("the launch-gated claim remains actor-owned");
            (
                active.repository_id,
                active.permit.coordination_key(),
                active.stop_state.kind() == Some(StopIntentKind::UserCancelled),
            )
        };
        let Some(stop_state) =
            safety_gate.launch_stop_state(task_id, operation_nonce, coordination_key)
        else {
            drop(safety_gate);
            drop(launch_barrier);
            self.freeze_degraded();
            return;
        };
        let suppression = match classify_launch_suppression(
            self.claims_allowed(),
            self.storage_admission.launch_allowed(repository_id),
            stop_state,
            user_stop_accepted,
        ) {
            Ok(suppression) => suppression,
            Err(()) => {
                drop(safety_gate);
                drop(launch_barrier);
                self.freeze_degraded();
                return;
            }
        };
        if let Some(reason) = suppression {
            drop(safety_gate);
            drop(launch_barrier);
            self.suppress_claimed_launch(task_id, operation_nonce, reason);
            return;
        }

        let prepared = match self.prepare_claimed_run(task_id) {
            Ok(prepared) => prepared,
            Err(ClaimLaunchPreparationError::Context(error)) => {
                drop(safety_gate);
                drop(launch_barrier);
                tracing::error!(%task_id, error = %error, "run context adoption failed");
                self.freeze_degraded();
                return;
            }
            Err(ClaimLaunchPreparationError::OwnershipInvariant) => {
                drop(safety_gate);
                drop(launch_barrier);
                self.freeze_degraded();
                return;
            }
        };
        if !self.spawn_claimed_runner(task_id, operation_nonce, message_sender, prepared) {
            drop(safety_gate);
            drop(launch_barrier);
            self.freeze_degraded();
            return;
        }
        drop(safety_gate);
        drop(launch_barrier);
    }

    fn defer_claim_launch_for_buffered_messages(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
    ) -> bool {
        if !self.drain_buffered_messages() {
            return false;
        }
        self.deferred_messages
            .push_back(TaskManagerMessage::ResumeLaunch {
                task_id,
                operation_nonce,
            });
        true
    }

    fn prepare_claimed_run(
        &mut self,
        task_id: TaskId,
    ) -> Result<PreparedClaimLaunch, ClaimLaunchPreparationError> {
        let (task, repository, cancellation, control_lease, process_scope, permit_witness) = {
            let active = self
                .active
                .get_mut(&task_id)
                .expect("the adopted claim remains actor-owned");
            let Some(control_lease) = active.control_lease.take() else {
                return Err(ClaimLaunchPreparationError::OwnershipInvariant);
            };
            let Some(task) = active.claimed_task.clone() else {
                return Err(ClaimLaunchPreparationError::OwnershipInvariant);
            };
            active.phase = AdmissionPhase::Preparing;
            (
                task,
                active.repository.clone(),
                active.cancellation.clone(),
                control_lease,
                active.process_scope.clone(),
                active.permit.witness(),
            )
        };
        let shutdown_process_scope = process_scope.clone();
        #[cfg(feature = "test-support")]
        let context = {
            let launch_ordinal = self.next_launch_ordinal;
            let Some(next_launch_ordinal) = launch_ordinal.checked_add(1) else {
                return Err(ClaimLaunchPreparationError::OwnershipInvariant);
            };
            self.next_launch_ordinal = next_launch_ordinal;
            RunContext::adopt_with_launch_ordinal(
                task.clone(),
                repository,
                cancellation,
                control_lease,
                process_scope,
                permit_witness,
                self.preparation_sender.clone(),
                launch_ordinal,
            )
        };
        #[cfg(not(feature = "test-support"))]
        let context = RunContext::adopt(
            task.clone(),
            repository,
            cancellation,
            control_lease,
            process_scope,
            permit_witness,
            self.preparation_sender.clone(),
        );
        let context = context.map_err(ClaimLaunchPreparationError::Context)?;
        Ok(PreparedClaimLaunch {
            task,
            context,
            shutdown_process_scope,
        })
    }

    fn spawn_claimed_runner(
        &self,
        task_id: TaskId,
        operation_nonce: u64,
        message_sender: mpsc::Sender<TaskManagerMessage>,
        prepared: PreparedClaimLaunch,
    ) -> bool {
        let PreparedClaimLaunch {
            task,
            context,
            shutdown_process_scope,
        } = prepared;
        let runner = Arc::clone(&self.runner);
        let completion_sender = self.completion_sender.clone();
        let process_cleanup = Arc::clone(&self.shutdown.process_cleanup);
        let sink = RunnerEventSink {
            task_id,
            repository_id: task.repository_id,
            attempt: task.attempt,
            sender: message_sender,
        };
        if !process_cleanup.register_spawned_runner(&shutdown_process_scope) {
            return false;
        }
        tokio::spawn(async move {
            let outcome = AssertUnwindSafe(runner.run(context, sink))
                .catch_unwind()
                .await
                .unwrap_or_else(|_| RunnerOutcome::Failed(runner_panicked_failure()));
            process_cleanup.runner_returned(shutdown_process_scope);
            let _ = completion_sender
                .send(TaskManagerCompletion::RunnerReturned {
                    task_id,
                    operation_nonce,
                    outcome,
                })
                .await;
        });
        true
    }
}
