use super::*;

mod injections;
mod snapshots;

impl TaskManager {
    pub(super) async fn handle_test_message(
        &mut self,
        message: TaskManagerMessage,
    ) -> Option<TaskManagerMessage> {
        match message {
            #[cfg(test)]
            TaskManagerMessage::InstallExitProbe { exited, installed } => {
                self.exit_probe = Some(exited);
                let _ = installed.send(());
            }
            #[cfg(test)]
            TaskManagerMessage::InspectStorageActivitySyncForTest { response } => {
                let _ = response.send(self.storage_activity_sync.snapshot_for_test());
            }
            #[cfg(test)]
            TaskManagerMessage::PauseNextStorageIdleCompletionForTest { response } => {
                let pause = self
                    .storage_activity_sync
                    .pause_next_idle_completion_for_test();
                self.storage_activity_exit_pause = Some(pause.clone());
                let _ = response.send(pause);
            }
            #[cfg(test)]
            TaskManagerMessage::InjectStopIntentCompletion {
                identity,
                completion,
                response,
            } => {
                if self
                    .project_and_handle_stop_intent_persisted(identity, completion)
                    .await
                    == StopCompletionDrain::Continue
                {
                    self.kick_exact_barrier_progress();
                }
                let _ = response.send(());
            }
            #[cfg(test)]
            TaskManagerMessage::InspectActiveStop { task_id, response } => {
                let _ = response.send(self.active_stop_snapshot_for_test(task_id));
            }
            #[cfg(test)]
            TaskManagerMessage::InspectExactBarriers { response } => {
                let _ = response.send(self.exact_barrier_snapshot_for_test());
            }
            #[cfg(test)]
            TaskManagerMessage::InstallGenericRecoveryLeaseForTest {
                attempt_id,
                barrier_epoch,
                response,
            } => {
                self.degraded = true;
                let _ = self.service_state.set(ServiceState::StoreDegraded);
                self.exact_barrier_epoch = barrier_epoch;
                match self.checked_recovery_safety_gate() {
                    RecoverySafetyGate::Exact(safety_generation) => {
                        self.generic_recovery_attempt = Some(GenericRecoveryAttempt {
                            attempt_id,
                            barrier_epoch,
                            safety_generation,
                        });
                    }
                    RecoverySafetyGate::CriticalPending => self.handle_critical_wake(),
                    RecoverySafetyGate::Conflict => self.freeze_degraded(),
                }
                let _ = response.send(());
            }
            #[cfg(test)]
            TaskManagerMessage::PauseNextDegradedFinalizationForTest { pause, response } => {
                assert!(
                    self.degraded_finalization_pause.replace(pause).is_none(),
                    "only one degraded finalization pause may be armed"
                );
                let _ = response.send(());
            }
            #[cfg(test)]
            TaskManagerMessage::PauseNextQuiesceFinalizationForTest { pause, response } => {
                assert!(
                    self.quiesce_finalization_pause.replace(pause).is_none(),
                    "only one quiesce finalization pause may be armed"
                );
                let _ = response.send(());
            }
            #[cfg(test)]
            TaskManagerMessage::InstallCanonicalPendingForTest { pending, response } => {
                self.enter_degraded(Some(pending));
                let _ = response.send(());
            }
            #[cfg(test)]
            TaskManagerMessage::InstallStagedStopCompletionsForTest { entries, response } => {
                let installed = self.install_staged_stop_completions_for_test(entries);
                if !installed {
                    self.freeze_degraded();
                }
                let _ = response.send(installed);
            }
            #[cfg(test)]
            TaskManagerMessage::ResolveCanonicalPredecessorForTest {
                predecessor,
                response,
            } => {
                let resolved = self.resolve_canonical_predecessor_for_test(&predecessor);
                if !resolved {
                    self.freeze_degraded();
                }
                let _ = response.send(resolved);
            }
            #[cfg(test)]
            TaskManagerMessage::ReleaseCanonicalPredecessorWithoutProgressForTest {
                predecessor,
                response,
            } => {
                let released = self.release_canonical_predecessor_for_test(&predecessor);
                if !released {
                    self.freeze_degraded();
                }
                let _ = response.send(released);
            }
            #[cfg(test)]
            TaskManagerMessage::FreezeDegradedPreservingPendingForTest { response } => {
                self.freeze_degraded();
                let _ = response.send(());
            }
            #[cfg(test)]
            TaskManagerMessage::HandleCriticalWakeForTest { response } => {
                self.handle_critical_wake();
                let _ = response.send(());
            }
            #[cfg(test)]
            TaskManagerMessage::StageHistoricalRecordReviewPairForTest {
                task_id,
                requests,
                review_responses,
                response,
            } => {
                if let Some(entries) = self.install_historical_record_review_pair_for_test(
                    task_id,
                    requests,
                    review_responses,
                ) {
                    let _ = response.send(entries);
                } else {
                    self.freeze_degraded();
                }
            }
            #[cfg(test)]
            TaskManagerMessage::InjectFinalizeDegradedForTest {
                attempt_id,
                barrier_epoch,
                recovery,
                replayed_pending_count,
                high_watermark,
                response,
            } => {
                self.start_finalize_degraded(
                    attempt_id,
                    barrier_epoch,
                    recovery,
                    replayed_pending_count,
                    high_watermark,
                    response,
                );
            }
            #[cfg(test)]
            TaskManagerMessage::InspectActivePendingStopWrite { task_id, response } => {
                let _ = response.send(self.active_pending_stop_write_for_test(task_id));
            }
            #[cfg(test)]
            TaskManagerMessage::InjectRecordReviewCompletion {
                identity,
                request,
                completion,
                response,
            } => self
                .inject_record_review_completion_for_test(identity, request, completion, response),
            #[cfg(test)]
            TaskManagerMessage::InjectTerminalWriteCompletion {
                task_id,
                attempt_id,
                identity,
                stage,
                completion,
                response,
            } => {
                if let Some(operation_nonce) = self
                    .active
                    .get(&task_id)
                    .map(|active| active.operation_nonce)
                {
                    self.handle_terminal_persisted(
                        task_id,
                        operation_nonce,
                        attempt_id,
                        identity,
                        stage,
                        completion,
                    );
                }
                let _ = response.send(());
            }
            #[cfg(test)]
            TaskManagerMessage::InjectFinalStopCompletion {
                identity,
                request,
                completion,
                response,
            } => {
                self.inject_final_stop_completion_for_test(identity, request, completion, response)
            }
            #[cfg(test)]
            TaskManagerMessage::InjectPendingReplayCompletion {
                attempt_id,
                pending,
                result,
                response,
            } => {
                self.handle_pending_replay_completed(attempt_id, pending, result);
                let _ = response.send(());
            }
            #[cfg(test)]
            TaskManagerMessage::InjectPendingReplayRetry {
                attempt_id,
                response,
            } => {
                self.handle_pending_replay_retry(attempt_id);
                let _ = response.send(());
            }
            #[cfg(test)]
            TaskManagerMessage::InjectGenericRecoveryCompletion {
                attempt_id,
                result,
                response,
            } => {
                let barrier_epoch = self
                    .generic_recovery_attempt
                    .filter(|attempt| attempt.attempt_id == attempt_id)
                    .map_or(self.exact_barrier_epoch, |attempt| attempt.barrier_epoch);
                self.handle_generic_recovery_completed(attempt_id, barrier_epoch, result);
                let _ = response.send(());
            }
            #[cfg(test)]
            TaskManagerMessage::InjectRunningUserCancelAfterLookup { task_id, response } => {
                let Some(task) = self
                    .active
                    .get(&task_id)
                    .and_then(|active| active.claimed_task.clone())
                else {
                    let _ = response.send(Err(TaskManagerError::TaskNotFound));
                    return None;
                };
                self.route_cancel_task(task, response);
            }
            #[cfg(test)]
            TaskManagerMessage::InjectStaleCancelTaskLoaded { task, response } => {
                self.detached_cancel_completions = self
                    .detached_cancel_completions
                    .checked_add(1)
                    .expect("a test cancel lookup completion retains actor ownership");
                assert!(
                    self.advance_exact_barrier_epoch(),
                    "a test cancel lookup advances its exact barrier"
                );
                self.handle_cancel_task_loaded(
                    task.id,
                    CancelTaskLookupKind::MayPredateActiveRelease,
                    Ok(Some(task)),
                    response,
                );
            }
            #[cfg(test)]
            TaskManagerMessage::InjectTerminalProjection {
                completion,
                response,
            } => {
                let task_id = completion.attempt().task_id();
                self.handle_terminal_projected(completion).await;
                let _ = response.send(self.terminal_projection_snapshot_for_test(task_id));
            }
            message => return Some(message),
        }
        None
    }
}
