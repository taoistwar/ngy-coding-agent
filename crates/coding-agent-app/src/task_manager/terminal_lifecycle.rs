use super::*;

impl TaskManager {
    pub(super) fn start_terminal_persistence(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        outcome: RunnerOutcome,
    ) {
        let safety_latched = self.active.get(&task_id).is_some_and(|active| {
            self.safety_registry.launch_stop_state(
                task_id,
                operation_nonce,
                active.permit.coordination_key(),
            ) == Some(LaunchStopState::SafetyLatched)
        });
        if safety_latched {
            self.handle_critical_wake();
        }
        let Some(active) = self.active.get(&task_id) else {
            return;
        };
        if active.operation_nonce != operation_nonce
            || active.phase != AdmissionPhase::RunnerReturned
        {
            return;
        }
        if !matches!(&active.stop_state, ActiveStopState::NoWinner) {
            self.advance_stop_after_barriers(task_id, operation_nonce);
            return;
        }
        let repository_id = active.repository_id;
        let attempt = active.attempt;
        if let Some(request) = final_review_request(task_id, repository_id, attempt, &outcome) {
            self.start_reviewed_terminal_persistence(task_id, operation_nonce, request);
            return;
        }
        let transition = match outcome {
            RunnerOutcome::Approved(_) | RunnerOutcome::Rejected(_) => {
                TaskTransition::Failed(quality_evidence_mismatch_failure())
            }
            RunnerOutcome::Failed(failure) => TaskTransition::Failed(failure),
            RunnerOutcome::Cancelled => TaskTransition::Cancelled,
            RunnerOutcome::ProcessCleanupUnproven => {
                TaskTransition::Failed(process_tree_cleanup_unproven_failure())
            }
        };
        self.start_unreviewed_terminal_persistence(
            task_id,
            operation_nonce,
            FinalizeUnreviewedTaskRequest {
                task_id,
                expected_repository_id: repository_id,
                expected_attempt: attempt,
                transition,
            },
        );
    }

    pub(super) fn start_unreviewed_terminal_persistence(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        request: FinalizeUnreviewedTaskRequest,
    ) {
        self.start_terminal_write(
            task_id,
            operation_nonce,
            PendingTerminalWriteKind::Unreviewed(request),
        );
    }

    pub(super) fn start_reviewed_terminal_persistence(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        request: FinalizeReviewedTaskRequest,
    ) {
        self.start_terminal_write(
            task_id,
            operation_nonce,
            PendingTerminalWriteKind::Reviewed(request),
        );
    }

    pub(super) fn start_terminal_write(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        kind: PendingTerminalWriteKind,
    ) {
        let is_current = self.active.get(&task_id).is_some_and(|active| {
            active.operation_nonce == operation_nonce
                && active.phase == AdmissionPhase::RunnerReturned
                && active.pending_terminal_write.is_none()
                && match &kind {
                    PendingTerminalWriteKind::Reviewed(request) => {
                        active.repository_id == request.expected_repository_id
                            && active.attempt == request.expected_attempt
                    }
                    PendingTerminalWriteKind::Unreviewed(request) => {
                        active.repository_id == request.expected_repository_id
                            && active.attempt == request.expected_attempt
                    }
                }
        });
        if !is_current {
            self.freeze_degraded();
            return;
        }
        let deadline = self.current_persistence_deadline();
        if self.frozen || Instant::now() >= deadline {
            self.freeze_degraded();
            return;
        };
        let Some(attempt_id) = self.next_typed_write_attempt_id() else {
            self.freeze_degraded();
            return;
        };
        let operation_kind = match &kind {
            PendingTerminalWriteKind::Reviewed(_) => DurableOperationKind::FinalizeReviewedTask,
            PendingTerminalWriteKind::Unreviewed(_) => DurableOperationKind::FinalizeUnreviewedTask,
        };
        let Some(identity) = self.next_mutation_identity(task_id, operation_kind) else {
            self.freeze_degraded();
            return;
        };
        let pending = PendingTerminalWrite {
            attempt_id,
            identity,
            kind,
            stage: TerminalWriteStage::SubmitPending,
            deadline,
            retry_available: true,
        };
        let Some(active) = self.active.get_mut(&task_id) else {
            self.freeze_degraded();
            return;
        };
        if active.operation_nonce != operation_nonce
            || active.phase != AdmissionPhase::RunnerReturned
            || active.pending_terminal_write.is_some()
        {
            self.freeze_degraded();
            return;
        }
        active.phase = AdmissionPhase::TerminalWritePending;
        active.pending_terminal_write = Some(pending.clone());
        #[cfg(feature = "test-support")]
        if let Some(actor_pauses) = self.actor_pauses.clone() {
            let completion_sender = self.completion_sender.clone();
            tokio::spawn(async move {
                actor_pauses.pause(ActorPausePoint::ResultBeforeWrite).await;
                let _ = completion_sender
                    .send(TaskManagerCompletion::TerminalWriteReady {
                        task_id,
                        operation_nonce,
                        attempt_id,
                    })
                    .await;
            });
            return;
        }
        self.submit_terminal_write(task_id, operation_nonce, pending);
    }

    #[cfg(feature = "test-support")]
    pub(super) fn submit_terminal_write_after_process_pause(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        attempt_id: u64,
    ) {
        let Some(pending) = self.active.get(&task_id).and_then(|active| {
            (active.operation_nonce == operation_nonce
                && active.phase == AdmissionPhase::TerminalWritePending)
                .then(|| active.pending_terminal_write.clone())
                .flatten()
        }) else {
            return;
        };
        if pending.attempt_id != attempt_id {
            return;
        }
        self.submit_terminal_write(task_id, operation_nonce, pending);
    }

    pub(super) fn submit_terminal_write(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        pending: PendingTerminalWrite,
    ) {
        let attempt_id = pending.attempt_id;
        let identity = pending.identity;
        let stage = pending.stage;
        let deadline = pending.deadline;
        let pending_result = pending.kind.pending(identity);
        let completion_sender = self.completion_sender.clone();
        match pending.kind {
            PendingTerminalWriteKind::Reviewed(request) => {
                let submission = match stage {
                    TerminalWriteStage::SubmitPending => self
                        .writer
                        .submit_finalize_reviewed_task(identity, request, deadline)
                        .map(PendingDurableSubmission::FinalizeReviewedTask),
                    TerminalWriteStage::ReconcileSamePending => {
                        self.writer.reconcile_pending(pending_result, deadline)
                    }
                };
                let submission = match submission {
                    Ok(PendingDurableSubmission::FinalizeReviewedTask(submission)) => submission,
                    Ok(_) | Err(_) => {
                        self.freeze_degraded();
                        return;
                    }
                };
                tokio::spawn(async move {
                    let completion = submission.completion().await;
                    let _ = completion_sender
                        .send(TaskManagerCompletion::TerminalPersisted {
                            task_id,
                            operation_nonce,
                            attempt_id,
                            identity,
                            stage,
                            completion: TerminalWriteCompletion::Reviewed(completion),
                        })
                        .await;
                });
            }
            PendingTerminalWriteKind::Unreviewed(request) => {
                let submission = match stage {
                    TerminalWriteStage::SubmitPending => self
                        .writer
                        .submit_finalize_unreviewed_task(identity, request, deadline)
                        .map(PendingDurableSubmission::FinalizeUnreviewedTask),
                    TerminalWriteStage::ReconcileSamePending => {
                        self.writer.reconcile_pending(pending_result, deadline)
                    }
                };
                let submission = match submission {
                    Ok(PendingDurableSubmission::FinalizeUnreviewedTask(submission)) => submission,
                    Ok(_) | Err(_) => {
                        self.freeze_degraded();
                        return;
                    }
                };
                tokio::spawn(async move {
                    let completion = submission.completion().await;
                    let _ = completion_sender
                        .send(TaskManagerCompletion::TerminalPersisted {
                            task_id,
                            operation_nonce,
                            attempt_id,
                            identity,
                            stage,
                            completion: TerminalWriteCompletion::Unreviewed(completion),
                        })
                        .await;
                });
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn handle_terminal_persisted(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        attempt_id: u64,
        identity: TaskMutationIdentity,
        stage: TerminalWriteStage,
        completion: TerminalWriteCompletion,
    ) {
        match self.current_typed_write_attempt_owner(attempt_id) {
            Ok(None) => return,
            Ok(Some(TypedWriteAttemptOwner::Terminal {
                task_id: owned_task_id,
                operation_nonce: owned_operation_nonce,
                identity: owned_identity,
                stage: owned_stage,
            })) if owned_task_id == task_id
                && owned_operation_nonce == operation_nonce
                && owned_identity == identity
                && owned_stage == stage => {}
            Ok(Some(_)) | Err(()) => {
                self.freeze_degraded();
                return;
            }
        }
        let Some(pending) = self
            .active
            .get(&task_id)
            .and_then(|active| active.pending_terminal_write.clone())
        else {
            self.freeze_degraded();
            return;
        };
        let current_is_exact = self.active.get(&task_id).is_some_and(|active| {
            active.operation_nonce == operation_nonce
                && active.phase == AdmissionPhase::TerminalWritePending
                && active.in_flight_mutations == 0
                && pending.identity == identity
                && pending.stage == stage
        });
        if !current_is_exact {
            self.freeze_degraded();
            return;
        }
        match (pending.kind.clone(), completion) {
            (
                PendingTerminalWriteKind::Reviewed(request),
                TerminalWriteCompletion::Reviewed(completion),
            ) => {
                let pending_result = PendingDurableResult::FinalizeReviewedTask {
                    identity,
                    request: request.clone(),
                };
                match classify_reviewed_terminal_completion(
                    identity,
                    &request,
                    &pending_result,
                    completion,
                ) {
                    ReviewedTerminalCompletion::Applied {
                        task,
                        event_kind,
                        event_id,
                        outcome,
                    } => {
                        if !terminal_receipt_is_exact(
                            self.active.get(&task_id),
                            &task,
                            event_kind,
                            event_id,
                        ) || !self.resolve_canonical_pending_from_original(
                            pending_result,
                            PendingReplayReceipt::FinalizeReviewedTask(outcome),
                        ) || !self.consume_pending_terminal_write(
                            task_id,
                            operation_nonce,
                            attempt_id,
                            identity,
                            stage,
                        ) {
                            self.freeze_degraded();
                            return;
                        }
                        self.start_terminal_projection(
                            task_id,
                            operation_nonce,
                            task,
                            event_kind,
                            event_id,
                        );
                    }
                    ReviewedTerminalCompletion::ReplaySame => {
                        self.reconcile_terminal_write(task_id, operation_nonce, pending);
                    }
                    ReviewedTerminalCompletion::RetryNext => {
                        self.retry_terminal_write(task_id, operation_nonce, pending);
                    }
                    ReviewedTerminalCompletion::Freeze => self.freeze_degraded(),
                }
            }
            (
                PendingTerminalWriteKind::Unreviewed(request),
                TerminalWriteCompletion::Unreviewed(completion),
            ) => {
                let pending_result = PendingDurableResult::FinalizeUnreviewedTask {
                    identity,
                    request: request.clone(),
                };
                match classify_unreviewed_terminal_completion(
                    identity,
                    &request,
                    &pending_result,
                    completion,
                ) {
                    UnreviewedTerminalCompletion::Applied {
                        task,
                        event_kind,
                        event_id,
                        outcome,
                    } => {
                        if !terminal_receipt_is_exact(
                            self.active.get(&task_id),
                            &task,
                            event_kind,
                            event_id,
                        ) || !self.resolve_canonical_pending_from_original(
                            pending_result,
                            PendingReplayReceipt::FinalizeUnreviewedTask(outcome),
                        ) || !self.consume_pending_terminal_write(
                            task_id,
                            operation_nonce,
                            attempt_id,
                            identity,
                            stage,
                        ) {
                            self.freeze_degraded();
                            return;
                        }
                        self.start_terminal_projection(
                            task_id,
                            operation_nonce,
                            task,
                            event_kind,
                            event_id,
                        );
                    }
                    UnreviewedTerminalCompletion::ReplaySame => {
                        self.reconcile_terminal_write(task_id, operation_nonce, pending);
                    }
                    UnreviewedTerminalCompletion::RetryNext => {
                        self.retry_terminal_write(task_id, operation_nonce, pending);
                    }
                    UnreviewedTerminalCompletion::Freeze => self.freeze_degraded(),
                }
            }
            _ => self.freeze_degraded(),
        }
    }

    pub(super) fn consume_pending_terminal_write(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        attempt_id: u64,
        identity: TaskMutationIdentity,
        stage: TerminalWriteStage,
    ) -> bool {
        let Some(active) = self.active.get_mut(&task_id) else {
            return false;
        };
        if active.operation_nonce != operation_nonce
            || active.phase != AdmissionPhase::TerminalWritePending
            || !active
                .pending_terminal_write
                .as_ref()
                .is_some_and(|pending| {
                    pending.attempt_id == attempt_id
                        && pending.identity == identity
                        && pending.stage == stage
                })
        {
            return false;
        }
        active.pending_terminal_write = None;
        true
    }

    pub(super) fn reconcile_terminal_write(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        pending: PendingTerminalWrite,
    ) {
        if pending.stage != TerminalWriteStage::SubmitPending
            || self.frozen
            || Instant::now() >= pending.deadline
        {
            self.freeze_degraded();
            return;
        }
        let Some(attempt_id) = self.next_typed_write_attempt_id() else {
            self.freeze_degraded();
            return;
        };
        let replacement = PendingTerminalWrite {
            attempt_id,
            stage: TerminalWriteStage::ReconcileSamePending,
            ..pending
        };
        let replaced = self.active.get_mut(&task_id).is_some_and(|active| {
            if active.operation_nonce != operation_nonce
                || active.phase != AdmissionPhase::TerminalWritePending
            {
                return false;
            }
            active.pending_terminal_write = Some(replacement.clone());
            true
        });
        if !replaced {
            self.freeze_degraded();
            return;
        }
        self.submit_terminal_write(task_id, operation_nonce, replacement);
    }

    pub(super) fn retry_terminal_write(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        pending: PendingTerminalWrite,
    ) {
        if !pending.retry_available || self.frozen || Instant::now() >= pending.deadline {
            self.freeze_degraded();
            return;
        }
        let Some(attempt_id) = self.next_typed_write_attempt_id() else {
            self.freeze_degraded();
            return;
        };
        let operation_kind = match &pending.kind {
            PendingTerminalWriteKind::Reviewed(_) => DurableOperationKind::FinalizeReviewedTask,
            PendingTerminalWriteKind::Unreviewed(_) => DurableOperationKind::FinalizeUnreviewedTask,
        };
        let Some(identity) = self.next_mutation_identity(task_id, operation_kind) else {
            self.freeze_degraded();
            return;
        };
        let replacement = PendingTerminalWrite {
            attempt_id,
            identity,
            stage: TerminalWriteStage::SubmitPending,
            retry_available: false,
            ..pending
        };
        let replaced = self.active.get_mut(&task_id).is_some_and(|active| {
            if active.operation_nonce != operation_nonce
                || active.phase != AdmissionPhase::TerminalWritePending
            {
                return false;
            }
            active.pending_terminal_write = Some(replacement.clone());
            true
        });
        if !replaced {
            self.freeze_degraded();
            return;
        }
        self.submit_terminal_write(task_id, operation_nonce, replacement);
    }

    pub(super) fn start_terminal_projection(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        task: Task,
        event_kind: TaskEventKind,
        event_id: EventId,
    ) {
        if !terminal_receipt_is_exact(self.active.get(&task_id), &task, event_kind, event_id) {
            self.freeze_degraded();
            return;
        }
        let projection_state_is_ready = self.active.get(&task_id).is_some_and(|active| {
            active.operation_nonce == operation_nonce
                && active.phase == AdmissionPhase::TerminalWritePending
                && active.pending_terminal_write.is_none()
                && active.terminal_projection_barrier.is_none()
        });
        if !projection_state_is_ready {
            self.freeze_degraded();
            return;
        }
        let target =
            EventCursor::new(event_id.get()).expect("a terminal event ID is a valid cursor");
        let Some(attempt_id) = self.next_terminal_projection_attempt_id() else {
            self.freeze_degraded();
            return;
        };
        let attempt = match TerminalProjectionAttempt::try_new(
            task_id,
            operation_nonce,
            attempt_id,
            target,
            event_kind,
        ) {
            Ok(attempt) => attempt,
            Err(error) => {
                tracing::error!(%task_id, %error, "terminal projection identity is invalid");
                self.freeze_degraded();
                return;
            }
        };
        let Some(active) = self.active.get_mut(&task_id) else {
            return;
        };
        if active.operation_nonce != operation_nonce
            || active.phase != AdmissionPhase::TerminalWritePending
            || active.pending_terminal_write.is_some()
            || active.terminal_projection_barrier.is_some()
        {
            self.freeze_degraded();
            return;
        }
        active.terminal_event = Some((event_kind, event_id));
        active.terminal_task = Some(task.clone());
        active.terminal_projection_barrier = Some(TerminalProjectionBarrier::new(attempt));
        active.phase = AdmissionPhase::ProjectionPending;
        for waiter in std::mem::take(&mut active.terminal_cancel_waiters) {
            send_terminal_cancel_response(waiter, task.clone());
        }
        self.spawn_terminal_projection(attempt, false);
    }

    pub(super) fn spawn_terminal_projection(
        &self,
        attempt: TerminalProjectionAttempt,
        retry: bool,
    ) {
        let dispatcher = self.dispatcher.clone();
        let completion_sender = self.completion_sender.clone();
        #[cfg(test)]
        let claim_hooks = self.claim_hooks.clone();
        #[cfg(feature = "test-support")]
        let actor_pauses = self.actor_pauses.clone();
        tokio::spawn(async move {
            if retry {
                tokio::time::sleep(TERMINAL_PROJECTION_RETRY_INTERVAL).await;
            }
            let result = dispatcher.flush_to(attempt.target()).await;
            #[cfg(feature = "test-support")]
            if result.is_ok()
                && let Some(actor_pauses) = actor_pauses
            {
                actor_pauses
                    .pause(ActorPausePoint::TerminalAfterDispatchBeforeSchedulerPublish)
                    .await;
            }
            #[cfg(test)]
            if result.is_ok()
                && let Some(hooks) = claim_hooks
            {
                hooks.pause(ClaimPhase::TerminalDispatched).await;
            }
            let _ = completion_sender
                .send(TaskManagerCompletion::TerminalProjected {
                    completion: TerminalProjectionCompletion::new(attempt, result),
                })
                .await;
        });
    }

    pub(super) async fn handle_terminal_projected(
        &mut self,
        completion: TerminalProjectionCompletion,
    ) {
        let attempt = completion.attempt();
        let task_id = attempt.task_id();
        let Some(active) = self.active.get(&task_id) else {
            return;
        };
        if active.operation_nonce != attempt.operation_nonce()
            || active.phase != AdmissionPhase::ProjectionPending
        {
            self.freeze_degraded();
            return;
        }
        let Some(barrier) = active.terminal_projection_barrier.as_ref() else {
            self.freeze_degraded();
            return;
        };
        match barrier.classify_completion(&completion) {
            TerminalProjectionCompletionDisposition::IgnoreStale => {}
            TerminalProjectionCompletionDisposition::Conflict => {
                self.freeze_degraded();
            }
            TerminalProjectionCompletionDisposition::RetrySameTarget { .. } => {
                let Some(next_attempt_id) = self.next_terminal_projection_attempt_id() else {
                    self.freeze_degraded();
                    return;
                };
                let retry = self
                    .active
                    .get_mut(&task_id)
                    .and_then(|active| active.terminal_projection_barrier.as_mut())
                    .and_then(|barrier| barrier.advance_retry(next_attempt_id).ok());
                let Some(retry) = retry else {
                    self.freeze_degraded();
                    return;
                };
                self.spawn_terminal_projection(retry, true);
            }
            TerminalProjectionCompletionDisposition::FreezeRetainingBarrier => {
                self.freeze_degraded();
            }
            TerminalProjectionCompletionDisposition::Projected {
                target: projection,
                event_kind: projected_event_kind,
            } => {
                let scheduler_snapshot = match self.refresh_scheduler_projection().await {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        tracing::error!(
                            %task_id,
                            %error,
                            "terminal scheduler projection publication failed"
                        );
                        self.freeze_degraded();
                        return;
                    }
                };
                if !self.scheduler_snapshot_has_exact_terminal(
                    &scheduler_snapshot,
                    task_id,
                    projected_event_kind,
                    projection,
                ) {
                    tracing::error!(
                        %task_id,
                        "terminal scheduler projection does not contain the exact target receipt"
                    );
                    self.freeze_degraded();
                    return;
                }
                self.release_projected_terminal(
                    task_id,
                    attempt.operation_nonce(),
                    projected_event_kind,
                    projection,
                    &scheduler_snapshot,
                );
            }
        }
    }

    pub(super) fn release_projected_terminal(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        projected_event_kind: TaskEventKind,
        projection: EventCursor,
        scheduler_snapshot: &SchedulerBootstrapSnapshot,
    ) {
        let published_membership = scheduler_snapshot.membership_event_id;
        let (event_kind, event_id) = {
            let active = self
                .active
                .get(&task_id)
                .expect("the projected terminal remains actor-owned");
            let Some((event_kind, event_id)) = active.terminal_event else {
                self.freeze_degraded();
                return;
            };
            (event_kind, event_id)
        };
        if projected_event_kind != event_kind || projection.get() != event_id.get() {
            self.freeze_degraded();
            return;
        }
        if published_membership.get() < event_id.get() {
            self.freeze_degraded();
            return;
        }
        let Some(barrier) = self
            .active
            .get(&task_id)
            .and_then(|active| active.terminal_projection_barrier)
        else {
            self.freeze_degraded();
            return;
        };
        let permit_ledger = self.permit_ledger.clone();
        let launch_barrier = Arc::clone(&self.shutdown.launch_barrier);
        let launch_guard = launch_barrier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let safety_registry = self.safety_registry.clone();
        let mut safety_guard = safety_registry.lock();
        let release = commit_projected_terminal_release(
            &permit_ledger,
            &mut self.active,
            &mut safety_guard,
            ProjectedTerminalReleaseRequest::new(
                task_id,
                operation_nonce,
                barrier,
                published_membership,
            ),
        );
        drop(safety_guard);
        drop(launch_guard);
        let commit = match release {
            Ok(commit) => commit,
            Err(error) => {
                tracing::error!(%task_id, %error, "atomic terminal ownership release failed");
                self.freeze_degraded();
                return;
            }
        };
        debug_assert_eq!(commit.released_count(), 1);
        let shutdown_handles = self.finalize_terminal_release_commit(commit);
        debug_assert!(shutdown_handles.is_empty());
        if let Err(error) = self.publish_scheduler_snapshot(scheduler_snapshot) {
            tracing::error!(
                %task_id,
                %error,
                "released permit scheduler projection failed"
            );
            self.freeze_degraded();
            return;
        }
        if self.claims_allowed() {
            self.scan_requested = true;
        } else {
            self.try_start_quiesce_recovery();
            self.maybe_start_degraded_recovery();
        }
    }
}
