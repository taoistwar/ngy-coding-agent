use super::*;

impl TaskManager {
    pub(super) fn begin_cancel(&mut self, task_id: TaskId, response: CancelResponse) {
        if self.is_frozen() {
            let _ = response.send(Err(TaskManagerError::Frozen));
            return;
        }
        if self.degraded {
            let _ = response.send(Err(TaskManagerError::StoreDegraded));
            return;
        }
        if let Some(task) = self
            .active
            .get(&task_id)
            .and_then(|active| active.claimed_task.clone())
        {
            self.route_cancel_task(task, response);
            return;
        }
        self.start_cancel_task_lookup(
            task_id,
            CancelTaskLookupKind::MayPredateActiveRelease,
            response,
        );
    }

    fn start_cancel_task_lookup(
        &mut self,
        task_id: TaskId,
        lookup_kind: CancelTaskLookupKind,
        response: CancelResponse,
    ) {
        let Some(detached_cancel_completions) = self.detached_cancel_completions.checked_add(1)
        else {
            self.freeze_degraded();
            let _ = response.send(Err(TaskManagerError::StoreDegraded));
            return;
        };
        if !self.advance_exact_barrier_epoch() {
            let _ = response.send(Err(TaskManagerError::StoreDegraded));
            return;
        }
        self.detached_cancel_completions = detached_cancel_completions;
        let store = self.store.clone();
        let completion_sender = self.completion_sender.clone();
        tokio::spawn(async move {
            let result = store
                .task_detail(task_id)
                .await
                .map(|detail| detail.map(|detail| detail.task));
            let _ = completion_sender
                .send(TaskManagerCompletion::CancelTaskLoaded {
                    task_id,
                    lookup_kind,
                    result,
                    response,
                })
                .await;
        });
    }

    pub(super) fn handle_cancel_task_loaded(
        &mut self,
        task_id: TaskId,
        lookup_kind: CancelTaskLookupKind,
        result: Result<Option<Task>, StoreError>,
        response: CancelResponse,
    ) {
        let Some(detached_cancel_completions) = self.detached_cancel_completions.checked_sub(1)
        else {
            self.freeze_degraded();
            let _ = response.send(Err(TaskManagerError::Invariant(
                "cancel lookup completed without actor ownership",
            )));
            return;
        };
        self.detached_cancel_completions = detached_cancel_completions;
        match result {
            Ok(Some(task)) if task.id == task_id => match lookup_kind {
                CancelTaskLookupKind::MayPredateActiveRelease => {
                    self.route_cancel_task(task, response);
                }
                CancelTaskLookupKind::ReloadedAfterActiveRelease => {
                    self.route_revalidated_cancel_task(task, response);
                }
            },
            Ok(Some(_)) => {
                self.freeze_degraded();
                let _ = response.send(Err(TaskManagerError::Invariant(
                    "cancel task lookup returned a different task",
                )));
            }
            Ok(None) => {
                let _ = response.send(Err(TaskManagerError::TaskNotFound));
            }
            Err(error) => {
                let _ = response.send(Err(TaskManagerError::Store(error)));
            }
        }
        self.kick_exact_barrier_progress();
    }

    pub(super) fn route_cancel_task(&mut self, task: Task, response: CancelResponse) {
        self.route_cancel_task_from_snapshot(
            task,
            CancelTaskLookupKind::MayPredateActiveRelease,
            response,
        );
    }

    fn route_revalidated_cancel_task(&mut self, task: Task, response: CancelResponse) {
        self.route_cancel_task_from_snapshot(
            task,
            CancelTaskLookupKind::ReloadedAfterActiveRelease,
            response,
        );
    }

    fn route_cancel_task_from_snapshot(
        &mut self,
        task: Task,
        lookup_kind: CancelTaskLookupKind,
        response: CancelResponse,
    ) {
        if self.frozen {
            Self::respond_to_cancel_without_new_work(task, response);
            return;
        }
        match task.status {
            TaskStatus::Queued => {
                let Some(detached_cancel_completions) =
                    self.detached_cancel_completions.checked_add(1)
                else {
                    self.freeze_degraded();
                    let _ = response.send(Err(TaskManagerError::StoreDegraded));
                    return;
                };
                if !self.advance_exact_barrier_epoch() {
                    let _ = response.send(Err(TaskManagerError::StoreDegraded));
                    return;
                }
                self.detached_cancel_completions = detached_cancel_completions;
                let task_id = task.id;
                let writer = self.writer.clone();
                let completion_sender = self.completion_sender.clone();
                tokio::spawn(async move {
                    let result = writer
                        .cancel_task(task_id, TaskStatus::Queued, background_deadline())
                        .await;
                    let _ = completion_sender
                        .send(TaskManagerCompletion::QueuedCancelCompleted {
                            task_id,
                            result,
                            response,
                        })
                        .await;
                });
            }
            TaskStatus::Running
                if matches!(lookup_kind, CancelTaskLookupKind::MayPredateActiveRelease)
                    && !self.active.contains_key(&task.id) =>
            {
                self.start_cancel_task_lookup(
                    task.id,
                    CancelTaskLookupKind::ReloadedAfterActiveRelease,
                    response,
                );
            }
            TaskStatus::Running => self.accept_running_user_cancel(task, response),
            TaskStatus::Cancelled => {
                let _ = response.send(Ok(CancelOutcome::Cancelled { task }));
            }
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Interrupted => {
                let _ = response.send(Ok(CancelOutcome::Finished { task }));
            }
        }
    }

    pub(super) fn respond_to_cancel_without_new_work(task: Task, response: CancelResponse) {
        match task.status {
            TaskStatus::Cancelled => {
                let _ = response.send(Ok(CancelOutcome::Cancelled { task }));
            }
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Interrupted => {
                let _ = response.send(Ok(CancelOutcome::Finished { task }));
            }
            TaskStatus::Queued | TaskStatus::Running => {
                let _ = response.send(Err(TaskManagerError::Frozen));
            }
        }
    }

    pub(super) async fn handle_queued_cancel_completed(
        &mut self,
        task_id: TaskId,
        result: Result<crate::WriteReceipt<TransitionOutcome>, StoreWriterError>,
        response: CancelResponse,
    ) {
        let Some(detached_cancel_completions) = self.detached_cancel_completions.checked_sub(1)
        else {
            self.freeze_degraded();
            let _ = response.send(Err(TaskManagerError::Invariant(
                "queued cancel completed without actor ownership",
            )));
            return;
        };
        self.detached_cancel_completions = detached_cancel_completions;
        match result {
            Ok(receipt) => match receipt.value {
                TransitionOutcome::Applied { task, .. } if task.id == task_id => {
                    if self.publish_detached_terminal_before_response(&task).await {
                        let _ = response.send(Ok(CancelOutcome::Cancelled { task }));
                    } else {
                        let _ = response.send(Err(TaskManagerError::Invariant(
                            "queued cancel terminal projection failed",
                        )));
                    }
                }
                TransitionOutcome::Conflict { current } if current.id == task_id => {
                    if terminal_event_kind(current.status).is_some() {
                        if self
                            .publish_detached_terminal_before_response(&current)
                            .await
                        {
                            self.route_cancel_task(current, response);
                        } else {
                            let _ = response.send(Err(TaskManagerError::Invariant(
                                "queued cancel conflict projection failed",
                            )));
                        }
                    } else {
                        self.route_cancel_task(current, response);
                    }
                }
                _ => {
                    self.freeze_degraded();
                    let _ = response.send(Err(TaskManagerError::Invariant(
                        "queued cancel returned an inconsistent task",
                    )));
                }
            },
            Err(error) => {
                let _ = response.send(Err(TaskManagerError::StoreWriter(error)));
            }
        }
        self.kick_exact_barrier_progress();
    }

    pub(super) async fn publish_detached_terminal_before_response(&mut self, task: &Task) -> bool {
        let target = EventCursor::new(task.last_event_id.get())
            .expect("a terminal task event ID is a valid event cursor");
        if let Err(error) = self.dispatcher.flush_to(target).await {
            tracing::error!(
                task_id = %task.id,
                %error,
                "detached terminal dispatcher projection failed"
            );
            self.freeze_degraded();
            return false;
        }
        let snapshot = match self.refresh_scheduler_projection().await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::error!(
                    task_id = %task.id,
                    %error,
                    "detached terminal scheduler projection failed"
                );
                self.freeze_degraded();
                return false;
            }
        };
        let exact = snapshot.membership_event_id >= target
            && snapshot
                .tasks
                .iter()
                .any(|current| current == task && terminal_event_kind(current.status).is_some());
        if !exact {
            tracing::error!(
                task_id = %task.id,
                "detached terminal snapshot did not contain the exact task"
            );
            self.freeze_degraded();
        }
        exact
    }

    pub(super) fn accept_running_user_cancel(&mut self, task: Task, response: CancelResponse) {
        let task_id = task.id;
        let Some(active) = self.active.get_mut(&task_id) else {
            let _ = response.send(Err(TaskManagerError::Invariant(
                "running task has no active owner",
            )));
            return;
        };
        if active.repository_id != task.repository_id || active.attempt != task.attempt {
            self.freeze_degraded();
            let _ = response.send(Err(TaskManagerError::Invariant(
                "running cancel identity does not match active ownership",
            )));
            return;
        }
        match &active.stop_state {
            ActiveStopState::StopTerminal { task, .. } | ActiveStopState::TerminalWon { task } => {
                send_terminal_cancel_response(response, task.clone());
                return;
            }
            state if state.kind() == Some(StopIntentKind::DiskPressureCritical) => {
                let _ = response.send(Err(TaskManagerError::StopAlreadyRequested {
                    task,
                    existing: StopIntentKind::DiskPressureCritical,
                }));
                return;
            }
            state if state.kind() == Some(StopIntentKind::UserCancelled) => {
                if let Some(accepted) = active.accepted_stop_task.clone() {
                    let _ = response.send(Ok(CancelOutcome::Accepted { task: accepted }));
                } else {
                    active.user_cancel_waiters.push(response);
                }
                return;
            }
            ActiveStopState::NoWinner
                if matches!(
                    active.phase,
                    AdmissionPhase::TerminalWritePending | AdmissionPhase::ProjectionPending
                ) =>
            {
                active.terminal_cancel_waiters.push(response);
                return;
            }
            ActiveStopState::NoWinner => {
                active.user_cancel_waiters.push(response);
            }
            ActiveStopState::IntentSubmissionDeferred { .. }
            | ActiveStopState::IntentWritePending { .. }
            | ActiveStopState::IntentDurable { .. }
            | ActiveStopState::FinalStopWritePending { .. } => {
                unreachable!("stop kind guards handled every active winner")
            }
        }
        let Some(identity) =
            self.next_mutation_identity(task_id, DurableOperationKind::PersistStopIntent)
        else {
            self.fail_user_cancel_waiters(task_id, TaskManagerError::StoreDegraded);
            self.freeze_degraded();
            return;
        };
        let request = StopIntentRequest {
            task_id,
            expected_repository_id: task.repository_id,
            expected_attempt: task.attempt,
            kind: StopIntentKind::UserCancelled,
        };
        let deadline = self.current_persistence_deadline();
        let batch_identity = DurableOperationIdentity::stop_intent_batch(vec![identity])
            .expect("one exact user stop identity is a valid batch");
        let defer_submission = self
            .active
            .get(&task_id)
            .is_some_and(|active| active.durable_sequence_blocked)
            || self.stop_completion_has_pending_predecessor(&batch_identity);
        let Some(active) = self.active.get_mut(&task_id) else {
            self.freeze_degraded();
            return;
        };
        active.stop_state = if defer_submission {
            ActiveStopState::IntentSubmissionDeferred {
                kind: request.kind,
                identity,
                request,
                deadline,
                retries_remaining: STOP_WRITE_RETRY_LIMIT,
            }
        } else {
            ActiveStopState::IntentWritePending {
                kind: request.kind,
                identity,
                request,
                deadline,
                retries_remaining: STOP_WRITE_RETRY_LIMIT,
            }
        };
        if defer_submission {
            return;
        }
        match self
            .writer
            .submit_user_stop_intent(identity, request, deadline)
        {
            Ok(submission) => {
                let completion_sender = self.completion_sender.clone();
                tokio::spawn(async move {
                    let completion = submission.completion().await;
                    let identity = completion.identity.clone();
                    let _ = completion_sender
                        .send(TaskManagerCompletion::StopIntentPersisted {
                            identity,
                            completion,
                        })
                        .await;
                });
            }
            Err(error) => {
                tracing::error!(%task_id, %error, "user stop-intent submission failed");
                self.fail_user_cancel_waiters(task_id, TaskManagerError::StoreDegraded);
                self.freeze_degraded();
            }
        }
    }
}
