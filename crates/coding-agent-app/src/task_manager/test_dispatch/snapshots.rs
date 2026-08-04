use super::super::*;

impl TaskManager {
    pub(super) fn active_stop_snapshot_for_test(
        &self,
        task_id: TaskId,
    ) -> Option<ActiveStopSnapshotForTest> {
        let staged_stop_completion_count = self
            .staged_stop_intent_completions
            .iter()
            .filter(|staged| match &staged.identity {
                DurableOperationIdentity::StopIntentBatch { items } => {
                    items.iter().any(|identity| identity.task_id == task_id)
                }
                DurableOperationIdentity::CreateTask { .. }
                | DurableOperationIdentity::RetryTask { .. }
                | DurableOperationIdentity::TaskMutation(_) => false,
            })
            .count();
        self.active.get(&task_id).map(|active| {
            let stage = match &active.stop_state {
                ActiveStopState::NoWinner => ActiveStopStageForTest::NoWinner,
                ActiveStopState::IntentSubmissionDeferred { .. } => {
                    ActiveStopStageForTest::IntentSubmissionDeferred
                }
                ActiveStopState::IntentWritePending { .. } => {
                    ActiveStopStageForTest::IntentWritePending
                }
                ActiveStopState::IntentDurable { .. } => ActiveStopStageForTest::IntentDurable,
                ActiveStopState::FinalStopWritePending { .. } => {
                    ActiveStopStageForTest::FinalStopWritePending
                }
                ActiveStopState::StopTerminal { .. } => ActiveStopStageForTest::StopTerminal,
                ActiveStopState::TerminalWon { .. } => ActiveStopStageForTest::TerminalWon,
            };
            ActiveStopSnapshotForTest {
                phase: active.phase,
                stage,
                active_count: self.active.len(),
                available_permits: usize::try_from(
                    self.permit_ledger
                        .snapshot()
                        .limits()
                        .global()
                        .get()
                        .saturating_sub(self.permit_ledger.snapshot().global_owned()),
                )
                .unwrap_or(0),
                cleanup_confirmed: active.cleanup_confirmation.is_some(),
                cleanup_available: active
                    .cleanup_confirmation
                    .as_ref()
                    .is_some_and(TaskProcessCleanupConfirmation::is_available_for_terminal_release),
                permit_active: active.permit.state() == Ok(crate::PermitOwnershipState::Active),
                done_receiver_owned: active.done_receiver.is_some(),
                in_flight_mutations: active.in_flight_mutations,
                durable_sequence_blocked: active.durable_sequence_blocked,
                pending_runner_event_write_count: active.pending_runner_event_writes.len(),
                pending_runner_event_identity: (active.pending_runner_event_writes.len() == 1)
                    .then(|| {
                        active
                            .pending_runner_event_writes
                            .values()
                            .next()
                            .and_then(|pending| match pending.stage {
                                RunnerEventWriteStage::Deferred(_) => None,
                                RunnerEventWriteStage::Submitted { identity } => Some(identity),
                            })
                    })
                    .flatten(),
                pending_record_review_replay_count: active.pending_record_review_replays.len(),
                pending_record_review_write_count: active.pending_record_review_writes.len(),
                pending_record_review_attempt_id: (active.pending_record_review_writes.len() == 1)
                    .then(|| {
                        active
                            .pending_record_review_writes
                            .values()
                            .next()
                            .and_then(|pending| match pending.stage {
                                RecordReviewWriteStage::Deferred => None,
                                RecordReviewWriteStage::Submitted { attempt_id, .. } => {
                                    Some(attempt_id)
                                }
                            })
                    })
                    .flatten(),
                pending_record_review_identity: (active.pending_record_review_writes.len() == 1)
                    .then(|| {
                        active
                            .pending_record_review_writes
                            .values()
                            .next()
                            .and_then(|pending| match pending.stage {
                                RecordReviewWriteStage::Deferred => None,
                                RecordReviewWriteStage::Submitted { identity, .. } => {
                                    Some(identity)
                                }
                            })
                    })
                    .flatten(),
                pending_record_review_deadline: (active.pending_record_review_writes.len() == 1)
                    .then(|| {
                        active
                            .pending_record_review_writes
                            .values()
                            .next()
                            .map(|pending| pending.deadline)
                    })
                    .flatten(),
                pending_record_review_retry_available: (active.pending_record_review_writes.len()
                    == 1)
                    .then(|| {
                        active
                            .pending_record_review_writes
                            .values()
                            .next()
                            .and_then(|pending| match pending.stage {
                                RecordReviewWriteStage::Deferred => None,
                                RecordReviewWriteStage::Submitted {
                                    retry_available, ..
                                } => Some(retry_available),
                            })
                    })
                    .flatten(),
                next_typed_write_attempt_id: self.next_typed_write_attempt_id,
                next_terminal_projection_attempt_id: self.next_terminal_projection_attempt_id,
                next_mutation_sequence: active.next_mutation_sequence,
                applied_record_review_count: active.applied_record_reviews.len(),
                pending_terminal_attempt_id: active
                    .pending_terminal_write
                    .as_ref()
                    .map(|pending| pending.attempt_id),
                pending_terminal_identity: active
                    .pending_terminal_write
                    .as_ref()
                    .map(|pending| pending.identity),
                pending_terminal_stage: active
                    .pending_terminal_write
                    .as_ref()
                    .map(|pending| pending.stage),
                pending_terminal_deadline: active
                    .pending_terminal_write
                    .as_ref()
                    .map(|pending| pending.deadline),
                pending_terminal_retry_available: active
                    .pending_terminal_write
                    .as_ref()
                    .map(|pending| pending.retry_available),
                staged_stop_completion_count,
                terminal_task_set: active.terminal_task.is_some(),
                terminal_projection_attempt: active
                    .terminal_projection_barrier
                    .as_ref()
                    .map(TerminalProjectionBarrier::current),
                registry_owned: self
                    .safety_registry
                    .launch_stop_state(
                        task_id,
                        active.operation_nonce,
                        active.permit.coordination_key(),
                    )
                    .is_some(),
                permit_process_owner_id: active.permit.process_owner_id(),
                process_scope_owner_id: active.process_scope.owner_id(),
                hard_frozen: self.frozen,
                pending_replay_in_flight: self.pending_replay_in_flight.is_some(),
                pending_replay_attempt_id: self
                    .pending_replay_in_flight
                    .as_ref()
                    .map(|attempt| attempt.attempt_id),
                pending_replay_deadline: self
                    .pending_replay_in_flight
                    .as_ref()
                    .map(|attempt| attempt.deadline),
                generic_recovery_attempt_id: self
                    .generic_recovery_attempt
                    .map(|attempt| attempt.attempt_id),
                quiesce_recovery_running: self
                    .pending_quiesce
                    .as_ref()
                    .is_some_and(|pending| pending.recovery_started),
            }
        })
    }

    pub(super) fn exact_barrier_snapshot_for_test(&self) -> ExactBarrierSnapshotForTest {
        ExactBarrierSnapshotForTest {
            detached_cancel_completions: self.detached_cancel_completions,
            staged_stop_completion_count: self.staged_stop_intent_completions.len(),
            pending_durable_result_count: self.pending_durable_results.len(),
            pending_replay_in_flight: self.pending_replay_in_flight.is_some(),
            barrier_epoch: self.exact_barrier_epoch,
            generic_recovery_attempt_id: self
                .generic_recovery_attempt
                .map(|attempt| attempt.attempt_id),
            generic_recovery_barrier_epoch: self
                .generic_recovery_attempt
                .map(|attempt| attempt.barrier_epoch),
            quiesce_recovery_running: self
                .pending_quiesce
                .as_ref()
                .is_some_and(|pending| pending.recovery_started),
            hard_frozen: self.frozen,
        }
    }

    pub(super) fn active_pending_stop_write_for_test(
        &self,
        task_id: TaskId,
    ) -> Option<PendingDurableResult> {
        self.active
            .get(&task_id)
            .and_then(|active| match &active.stop_state {
                ActiveStopState::IntentWritePending {
                    identity, request, ..
                }
                | ActiveStopState::IntentSubmissionDeferred {
                    identity, request, ..
                } => Some(PendingDurableResult::PersistStopIntentBatch {
                    identity: DurableOperationIdentity::stop_intent_batch(vec![*identity])
                        .expect("one active stop identity is a valid batch"),
                    requests: vec![*request],
                }),
                ActiveStopState::FinalStopWritePending {
                    identity, request, ..
                } => Some(PendingDurableResult::FinalizeStoppedTask {
                    identity: *identity,
                    request: *request,
                }),
                _ => None,
            })
    }
}
