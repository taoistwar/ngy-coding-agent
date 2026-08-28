use std::future::Future;
use std::io;
use std::panic::AssertUnwindSafe;

use coding_agent_runtime::{ProcessCleanupProof, ProcessLivenessError};
use futures_util::FutureExt as _;
use tokio::time::Instant;

use super::*;

impl RuntimeShutdown {
    pub(super) async fn run_supervised(
        &self,
        started: Instant,
        emergency_only: bool,
    ) -> ShutdownOutcome {
        let total_deadline = started + SHUTDOWN_TOTAL_BUDGET;
        if !emergency_only
            && let Ok(outcome) = AssertUnwindSafe(self.shutdown(started))
                .catch_unwind()
                .await
        {
            return outcome;
        }
        loop {
            match AssertUnwindSafe(self.emergency_shutdown(total_deadline))
                .catch_unwind()
                .await
            {
                Ok(outcome) => return outcome,
                Err(_) => {
                    self.begin_emergency_cleanup_now();
                    let retry_at =
                        (Instant::now() + SHUTDOWN_FAILSAFE_RETRY_INTERVAL).min(total_deadline);
                    tokio::time::sleep_until(retry_at).await;
                }
            }
        }
    }

    async fn shutdown(&self, started: Instant) -> ShutdownOutcome {
        let total_deadline = started + SHUTDOWN_TOTAL_BUDGET;
        let mutation_drain_deadline = total_deadline
            .checked_sub(SHUTDOWN_FINALIZE_RESERVE)
            .unwrap_or(total_deadline);
        self.mutation_gate.begin_quiescing();
        let delivery_join = self.delivery.begin();
        let budget_enforcer = self.spawn_runtime_budget_enforcer(total_deadline);
        let prerequisites = self
            .await_shutdown_prerequisites(delivery_join, mutation_drain_deadline, total_deadline)
            .await;
        self.complete_after_process_cleanup(prerequisites, budget_enforcer, false, total_deadline)
            .await
    }

    async fn emergency_shutdown(&self, total_deadline: Instant) -> ShutdownOutcome {
        tracing::error!(
            error_code = "SHUTDOWN_COORDINATOR_PANICKED",
            "shutdown coordinator panicked; forcing degraded cleanup"
        );
        self.begin_emergency_cleanup_now();
        let mutation_drain_deadline = total_deadline
            .checked_sub(SHUTDOWN_FINALIZE_RESERVE)
            .unwrap_or(total_deadline);
        let delivery_join = self.delivery.begin();
        let prerequisites = self
            .await_shutdown_prerequisites(delivery_join, mutation_drain_deadline, total_deadline)
            .await;
        if Instant::now() >= total_deadline && !prerequisites.process_cleanup_outlived_deadline {
            return self.complete_fail_closed_after_panic(prerequisites.proof);
        }
        self.complete_after_process_cleanup(prerequisites, None, true, total_deadline)
            .await
    }

    pub(super) fn begin_emergency_cleanup_now(&self) {
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.mutation_gate.begin_quiescing();
        }));
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.mutation_gate.force_cancel_in_flight();
        }));
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.task_manager.freeze_and_cancel();
        }));
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.delivery.close_intake();
        }));
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.cleanup.stop_http_now();
        }));
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.cleanup.unpublish_descriptor();
        }));
    }

    fn spawn_runtime_budget_enforcer(
        &self,
        deadline: Instant,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let cleanup = self.cleanup.clone();
        let mutation_gate = self.mutation_gate.clone();
        Some(tokio::spawn(async move {
            let mutation_cancel_deadline = deadline
                .checked_sub(SHUTDOWN_FINALIZE_RESERVE)
                .unwrap_or(deadline)
                .checked_sub(SHUTDOWN_MUTATION_CANCEL_GRACE)
                .unwrap_or(deadline);
            tokio::time::sleep_until(mutation_cancel_deadline).await;
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
                mutation_gate.force_cancel_in_flight();
            }));
            if Instant::now() < deadline {
                tokio::time::sleep_until(deadline).await;
            }
            let _ = AssertUnwindSafe(cleanup.stop_http(Instant::now()))
                .catch_unwind()
                .await;
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| cleanup.stop_http_now()));
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
                cleanup.unpublish_descriptor();
            }));
        }))
    }

    async fn await_shutdown_prerequisites(
        &self,
        delivery_join: super::delivery::DeliveryShutdownJoin,
        mutation_deadline: Instant,
        total_deadline: Instant,
    ) -> ShutdownPrerequisites {
        let mutation_drain = self.mutation_gate.drain_until(mutation_deadline).await;
        let delivery_cleanup = delivery_join.wait();
        let task_cleanup = self
            .task_manager
            .freeze_and_wait_for_process_cleanup_until(total_deadline);
        let (delivery, (task_processes, task_cleanup_outlived_deadline)) =
            tokio::join!(delivery_cleanup, task_cleanup);
        let (instance_processes, instance_cleanup_outlived_deadline) =
            self.wait_for_instance_process_cleanup(total_deadline).await;
        ShutdownPrerequisites {
            proof: ShutdownRuntimeCleanupProof {
                _delivery: delivery,
                task_processes,
                _instance_processes: instance_processes,
            },
            mutation_outcome_unknown: mutation_drain == MutationDrainOutcome::Unproven,
            process_cleanup_outlived_deadline: task_cleanup_outlived_deadline
                || instance_cleanup_outlived_deadline,
        }
    }

    async fn wait_for_instance_process_cleanup(
        &self,
        total_deadline: Instant,
    ) -> (ConfirmedInstanceProcessCleanup, bool) {
        let mut last_observation = None;
        let mut cleanup_outlived_deadline = false;
        let sealed_scope = loop {
            match self.instance_process_scope.seal_instance_scope() {
                Ok(scope) => break scope,
                Err(error) => {
                    let observation = Err(error);
                    log_instance_cleanup_change(&mut last_observation, observation);
                    cleanup_outlived_deadline |= Instant::now() >= total_deadline;
                    tokio::time::sleep(SHUTDOWN_FAILSAFE_RETRY_INTERVAL).await;
                }
            }
        };
        loop {
            match sealed_scope.cleanup_proof() {
                Ok(ProcessCleanupProof::Confirmed) => {
                    return (
                        ConfirmedInstanceProcessCleanup {
                            _sealed_scope: sealed_scope,
                        },
                        cleanup_outlived_deadline,
                    );
                }
                Ok(proof @ (ProcessCleanupProof::Held | ProcessCleanupProof::Unknown)) => {
                    let observation = Ok(proof);
                    log_instance_cleanup_change(&mut last_observation, observation);
                    cleanup_outlived_deadline |= Instant::now() >= total_deadline;
                }
                Err(error) => {
                    let observation = Err(error);
                    log_instance_cleanup_change(&mut last_observation, observation);
                    cleanup_outlived_deadline |= Instant::now() >= total_deadline;
                }
            }
            tokio::time::sleep(SHUTDOWN_FAILSAFE_RETRY_INTERVAL).await;
        }
    }

    async fn complete_after_process_cleanup(
        &self,
        prerequisites: ShutdownPrerequisites,
        budget_enforcer: Option<tokio::task::JoinHandle<()>>,
        force_degraded: bool,
        total_deadline: Instant,
    ) -> ShutdownOutcome {
        let ShutdownPrerequisites {
            proof,
            mutation_outcome_unknown,
            process_cleanup_outlived_deadline,
        } = prerequisites;
        if mutation_outcome_unknown {
            return self
                .complete_after_budget_exhausted(
                    proof,
                    budget_enforcer,
                    ShutdownLockDisposition::RetainUntilProcessExit,
                    total_deadline,
                    process_cleanup_outlived_deadline,
                )
                .await;
        }
        if Instant::now() >= total_deadline {
            return self
                .complete_after_budget_exhausted(
                    proof,
                    budget_enforcer,
                    ShutdownLockDisposition::RetainUntilProcessExit,
                    total_deadline,
                    process_cleanup_outlived_deadline,
                )
                .await;
        }
        let persistence_deadline = (Instant::now() + SHUTDOWN_PERSISTENCE_BUDGET).min(
            total_deadline
                .checked_sub(SHUTDOWN_FINALIZE_RESERVE)
                .unwrap_or(total_deadline),
        );
        let persistence_durable = self
            .finalize_persistence_after_cleanup(&proof, persistence_deadline)
            .await;
        let (persistence_durable, store_closed) = self
            .close_runtime_after_persistence(
                persistence_durable,
                persistence_deadline,
                total_deadline,
            )
            .await;
        let recovery_required = !persistence_durable;
        self.finish_runtime_handoff(
            proof,
            budget_enforcer,
            total_deadline,
            store_closed,
            recovery_required,
        )
        .await;

        if recovery_required {
            tracing::error!(
                error_code = SHUTDOWN_MARKER_ERROR_CODE,
                "Some terminal task states could not be persisted. They will be recovered the next time Coding Agent starts."
            );
        }
        if force_degraded || recovery_required {
            ShutdownOutcome::Degraded
        } else {
            ShutdownOutcome::Clean
        }
    }

    async fn finalize_persistence_after_cleanup(
        &self,
        proof: &ShutdownRuntimeCleanupProof,
        persistence_deadline: Instant,
    ) -> bool {
        let finalization = self
            .task_manager
            .finalize_shutdown_after_process_cleanup(&proof.task_processes, persistence_deadline)
            .await;
        let mut persistence_durable = false;
        let mut recovery = None;
        match finalization {
            Ok(QuiesceResult::Durable {
                recovery: receipt,
                active: _,
            }) => {
                persistence_durable = true;
                recovery = Some(receipt);
            }
            Ok(QuiesceResult::Frozen { active: _, error }) => {
                tracing::error!(error = %error, error_code = SHUTDOWN_MARKER_ERROR_CODE, "shutdown persistence failed after process cleanup");
            }
            Err(error) => {
                tracing::error!(error = %error, error_code = SHUTDOWN_MARKER_ERROR_CODE, "task manager shutdown finalization failed after process cleanup");
            }
        }

        if let Some(recovery) = recovery
            && !result_until(
                self.dispatcher.flush_to(recovery.high_watermark),
                persistence_deadline,
            )
            .await
        {
            tracing::warn!(
                error_code = "SHUTDOWN_EVENT_FLUSH_INCOMPLETE",
                "shutdown event flush failed"
            );
            persistence_durable = false;
        }
        persistence_durable
    }

    async fn close_runtime_after_persistence(
        &self,
        mut persistence_durable: bool,
        persistence_deadline: Instant,
        runtime_close_deadline: Instant,
    ) -> (bool, bool) {
        if !result_until(self.dispatcher.close(), runtime_close_deadline).await {
            tracing::warn!(
                error_code = "EVENT_DISPATCHER_CLOSE_FAILED",
                "event dispatcher did not close cleanly"
            );
            persistence_durable = false;
        }
        self.stop_http_before_checkpoint(persistence_deadline).await;
        let checkpoint_closed = persistence_durable
            && result_until(self.store.checkpoint_and_close(), runtime_close_deadline).await;
        if persistence_durable && !checkpoint_closed {
            tracing::warn!(
                error_code = "SHUTDOWN_CHECKPOINT_INCOMPLETE",
                "SQLite checkpoint or close did not complete before final cleanup"
            );
            persistence_durable = false;
        }
        let store_closed = if persistence_durable {
            checkpoint_closed
        } else {
            close_pool_until(&self.store, runtime_close_deadline).await
        };
        (persistence_durable, store_closed)
    }

    async fn finish_runtime_handoff(
        &self,
        proof: ShutdownRuntimeCleanupProof,
        budget_enforcer: Option<tokio::task::JoinHandle<()>>,
        handoff_deadline: Instant,
        store_closed: bool,
        recovery_required: bool,
    ) {
        let marker_write = recovery_required.then(|| {
            tokio::spawn(write_shutdown_marker_until(
                self.marker_writer.clone(),
                self.marker_path.clone(),
                self.instance_id,
                self.wall_clock.now_utc(),
                handoff_deadline,
            ))
        });
        let message_publish = recovery_required.then(|| {
            tokio::spawn(publish_degraded_message_until(
                self.messages.clone(),
                handoff_deadline,
            ))
        });
        if let Some(enforcer) = budget_enforcer {
            enforcer.abort();
            let _ = enforcer.await;
        }
        self.cleanup.stop_http_now();
        self.cleanup.unpublish_descriptor();
        await_marker_write(marker_write).await;
        await_message_publish(message_publish).await;
        self.cleanup.finish_lock(
            proof,
            if store_closed {
                ShutdownLockDisposition::ReleaseNow
            } else {
                ShutdownLockDisposition::RetainUntilProcessExit
            },
        );
    }

    async fn stop_http_before_checkpoint(&self, deadline: Instant) {
        // Graceful HTTP shutdown drops SSE bodies and their in-flight Store reads.
        // The unconditional stop is the bounded fallback when a client does not
        // cooperate, so SQLite never checkpoints behind a live request.
        self.cleanup.stop_http(deadline).await;
        self.cleanup.stop_http_now();
    }

    async fn complete_after_budget_exhausted(
        &self,
        proof: ShutdownRuntimeCleanupProof,
        budget_enforcer: Option<tokio::task::JoinHandle<()>>,
        lock_disposition: ShutdownLockDisposition,
        total_deadline: Instant,
        allow_post_proof_handoff: bool,
    ) -> ShutdownOutcome {
        let handoff_deadline = if allow_post_proof_handoff && Instant::now() >= total_deadline {
            Instant::now() + SHUTDOWN_FINALIZE_RESERVE
        } else {
            total_deadline
        };
        let marker_write = tokio::spawn(write_shutdown_marker_until(
            self.marker_writer.clone(),
            self.marker_path.clone(),
            self.instance_id,
            self.wall_clock.now_utc(),
            handoff_deadline,
        ));
        let message_publish = tokio::spawn(publish_degraded_message_until(
            self.messages.clone(),
            handoff_deadline,
        ));
        if let Some(enforcer) = budget_enforcer {
            enforcer.abort();
            let _ = enforcer.await;
        }
        self.cleanup.stop_http(handoff_deadline).await;
        self.cleanup.stop_http_now();
        self.cleanup.unpublish_descriptor();
        await_marker_write(Some(marker_write)).await;
        await_message_publish(Some(message_publish)).await;
        self.cleanup.finish_lock(proof, lock_disposition);
        tracing::error!(
            error_code = SHUTDOWN_MARKER_ERROR_CODE,
            ?lock_disposition,
            "The shutdown budget elapsed before durable finalization completed. Recovery will resume from committed facts."
        );
        ShutdownOutcome::Degraded
    }

    fn complete_fail_closed_after_panic(
        &self,
        proof: ShutdownRuntimeCleanupProof,
    ) -> ShutdownOutcome {
        self.begin_emergency_cleanup_now();
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.cleanup
                .finish_lock(proof, ShutdownLockDisposition::RetainUntilProcessExit);
        }));
        tracing::error!(
            error_code = SHUTDOWN_MARKER_ERROR_CODE,
            "shutdown exhausted its absolute deadline after a coordinator panic; the instance lease is retained until process exit"
        );
        ShutdownOutcome::Degraded
    }
}

fn log_instance_cleanup_change(
    last_observation: &mut Option<Result<ProcessCleanupProof, ProcessLivenessError>>,
    observation: Result<ProcessCleanupProof, ProcessLivenessError>,
) {
    if *last_observation == Some(observation) {
        return;
    }
    *last_observation = Some(observation);
    match observation {
        Ok(proof) => tracing::warn!(
            error_code = "PROCESS_TREE_CLEANUP_PENDING",
            ?proof,
            "shutdown is retaining the primary lock while instance process cleanup remains unproven"
        ),
        Err(error) => tracing::warn!(
            error_code = "PROCESS_TREE_CLEANUP_PROBE_UNAVAILABLE",
            %error,
            "shutdown is retaining the primary lock while the instance cleanup probe is unavailable"
        ),
    }
}

async fn result_until<F, T, E>(future: F, deadline: Instant) -> bool
where
    F: Future<Output = Result<T, E>>,
{
    tokio::pin!(future);
    tokio::select! {
        biased;
        result = &mut future => result.is_ok(),
        () = tokio::time::sleep_until(deadline) => false,
    }
}

async fn close_pool_until(store: &Store, deadline: Instant) -> bool {
    let close = store.close();
    tokio::pin!(close);
    tokio::select! {
        biased;
        () = &mut close => true,
        () = tokio::time::sleep_until(deadline) => false,
    }
}

async fn await_marker_write(
    marker_write: Option<tokio::task::JoinHandle<Result<(), ShutdownMarkerWriteError>>>,
) {
    let Some(marker_write) = marker_write else {
        return;
    };
    match marker_write.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(error = %error, error_code = "SHUTDOWN_MARKER_WRITE_FAILED", "unclean shutdown marker could not be written");
        }
        Err(error) => {
            tracing::warn!(error = %error, error_code = "SHUTDOWN_MARKER_WORKER_FAILED", "unclean shutdown marker worker failed");
        }
    }
}

async fn await_message_publish(message_publish: Option<tokio::task::JoinHandle<io::Result<()>>>) {
    let Some(message_publish) = message_publish else {
        return;
    };
    match message_publish.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(error = %error, error_code = "SHUTDOWN_WARNING_HANDOFF_FAILED", "degraded shutdown warning could not be handed off");
        }
        Err(error) => {
            tracing::warn!(error = %error, error_code = "SHUTDOWN_WARNING_WORKER_FAILED", "degraded shutdown warning worker failed");
        }
    }
}
