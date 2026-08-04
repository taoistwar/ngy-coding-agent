use super::*;

impl TaskManager {
    pub(super) fn apply_stop_intent_batch_receipt(
        &mut self,
        identity: &DurableOperationIdentity,
        requests: &[StopIntentRequest],
        receipt: StopIntentBatchReceipt,
    ) -> bool {
        enum Decision {
            NoOp,
            Durable {
                identity: TaskMutationIdentity,
                receipt: StopIntentReceipt,
            },
            Terminal {
                task: Task,
            },
            Conflict {
                existing: StopIntentReceipt,
            },
        }

        let DurableOperationIdentity::StopIntentBatch { items: identities } = identity else {
            return false;
        };
        if identities.len() != requests.len() || receipt.items.len() != requests.len() {
            return false;
        }
        let mut decisions = Vec::with_capacity(requests.len());
        for ((expected_identity, request), item) in
            identities.iter().zip(requests).zip(receipt.items)
        {
            let Some(active) = self.active.get(&request.task_id) else {
                return false;
            };
            if expected_identity.task_id != request.task_id
                || expected_identity.kind != DurableOperationKind::PersistStopIntent
                || item.request != *request
            {
                return false;
            }
            if let Some(lineage) = &active.stop_intent_lineage {
                if lineage.identity != *expected_identity
                    || !stop_intent_lineage_matches_state(active, lineage)
                    || !stop_intent_outcome_matches_lineage(lineage, *request, &item.outcome)
                {
                    return false;
                }
                decisions.push((*request, Decision::NoOp));
                continue;
            }
            if !matches!(
                &active.stop_state,
                ActiveStopState::IntentWritePending {
                    identity,
                    request: active_request,
                    ..
                } if identity == expected_identity && active_request == request
            ) {
                return false;
            }
            let decision = match item.outcome {
                PersistStopIntentOutcome::Applied(stop_receipt)
                | PersistStopIntentOutcome::Existing(stop_receipt)
                    if stop_receipt_matches_request(stop_receipt, *request) =>
                {
                    Decision::Durable {
                        identity: *expected_identity,
                        receipt: stop_receipt,
                    }
                }
                PersistStopIntentOutcome::TerminalWon { current }
                    if current.id == request.task_id
                        && current.repository_id == request.expected_repository_id
                        && current.attempt == request.expected_attempt
                        && task_status_is_terminal(current.status)
                        && terminal_event_kind(current.status).is_some_and(|event_kind| {
                            terminal_receipt_is_exact(
                                Some(active),
                                &current,
                                event_kind,
                                current.last_event_id,
                            )
                        }) =>
                {
                    Decision::Terminal { task: current }
                }
                PersistStopIntentOutcome::IntentConflict { existing }
                    if existing.task_id == request.task_id
                        && existing.repository_id == request.expected_repository_id
                        && existing.attempt == request.expected_attempt
                        && existing.kind != request.kind =>
                {
                    Decision::Conflict { existing }
                }
                _ => return false,
            };
            decisions.push((*request, decision));
        }

        let mut advance = Vec::with_capacity(decisions.len());
        let mut conflicted = false;
        for (request, decision) in decisions {
            let Some(active) = self.active.get_mut(&request.task_id) else {
                return false;
            };
            let operation_nonce = active.operation_nonce;
            match decision {
                Decision::NoOp => {}
                Decision::Durable { identity, receipt } => {
                    active.stop_intent_lineage = Some(StopIntentLineage {
                        identity,
                        request,
                        decision: StopIntentLineageDecision::Durable(receipt),
                    });
                    active.stop_state = ActiveStopState::IntentDurable { identity, receipt };
                    advance.push((request.task_id, operation_nonce, true));
                }
                Decision::Terminal { task } => {
                    active.stop_intent_lineage = Some(StopIntentLineage {
                        identity: identities
                            .iter()
                            .find(|identity| identity.task_id == request.task_id)
                            .copied()
                            .expect("preflighted stop identity remains in its batch"),
                        request,
                        decision: StopIntentLineageDecision::TerminalWon(task.clone()),
                    });
                    active.stop_state = ActiveStopState::TerminalWon { task: task.clone() };
                    let user_waiters = std::mem::take(&mut active.user_cancel_waiters);
                    let terminal_waiters = std::mem::take(&mut active.terminal_cancel_waiters);
                    for waiter in user_waiters.into_iter().chain(terminal_waiters) {
                        send_terminal_cancel_response(waiter, task.clone());
                    }
                    advance.push((request.task_id, operation_nonce, false));
                }
                Decision::Conflict { existing } => {
                    conflicted = true;
                    active.stop_intent_lineage = Some(StopIntentLineage {
                        identity: identities
                            .iter()
                            .find(|identity| identity.task_id == request.task_id)
                            .copied()
                            .expect("preflighted stop identity remains in its batch"),
                        request,
                        decision: StopIntentLineageDecision::IntentConflict(existing),
                    });
                    if request.kind == StopIntentKind::UserCancelled {
                        let Some(task) = active.claimed_task.clone() else {
                            return false;
                        };
                        for waiter in std::mem::take(&mut active.user_cancel_waiters) {
                            let _ = waiter.send(Err(TaskManagerError::StopAlreadyRequested {
                                task: task.clone(),
                                existing: existing.kind,
                            }));
                        }
                    }
                }
            }
        }
        for (task_id, operation_nonce, durable) in advance {
            if durable {
                self.on_stop_intent_durable(task_id);
            } else {
                self.advance_stop_after_barriers(task_id, operation_nonce);
            }
        }
        !conflicted
    }

    pub(super) fn on_stop_intent_durable(&mut self, task_id: TaskId) {
        let Some(active) = self.active.get_mut(&task_id) else {
            return;
        };
        active.cancellation.cancel();
        if self.frozen {
            return;
        }
        let needs_task_load = active.stop_state.kind() == Some(StopIntentKind::UserCancelled)
            && !active.user_cancel_waiters.is_empty()
            && active.accepted_stop_task.is_none()
            && !active.accepted_stop_task_load_in_flight;
        let operation_nonce = active.operation_nonce;
        if needs_task_load {
            active.accepted_stop_task_load_in_flight = true;
            let store = self.store.clone();
            let completion_sender = self.completion_sender.clone();
            tokio::spawn(async move {
                let result = store
                    .task_detail(task_id)
                    .await
                    .map(|detail| detail.map(|detail| detail.task));
                let _ = completion_sender
                    .send(TaskManagerCompletion::StopAcceptedTaskLoaded {
                        task_id,
                        operation_nonce,
                        result,
                    })
                    .await;
            });
        }
        self.advance_stop_after_barriers(task_id, operation_nonce);
    }

    pub(super) fn handle_stop_accepted_task_loaded(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
        result: Result<Option<Task>, StoreError>,
    ) {
        let Some(active) = self.active.get_mut(&task_id) else {
            return;
        };
        if active.operation_nonce != operation_nonce
            || active.stop_state.kind() != Some(StopIntentKind::UserCancelled)
            || !active.accepted_stop_task_load_in_flight
        {
            self.freeze_degraded();
            return;
        }
        active.accepted_stop_task_load_in_flight = false;
        match result {
            Ok(Some(task))
                if task.id == task_id
                    && task.repository_id == active.repository_id
                    && task.attempt == active.attempt
                    && task.status == TaskStatus::Running =>
            {
                active.accepted_stop_task = Some(task.clone());
                for waiter in std::mem::take(&mut active.user_cancel_waiters) {
                    let _ = waiter.send(Ok(CancelOutcome::Accepted { task: task.clone() }));
                }
            }
            Ok(Some(task))
                if task.id == task_id
                    && active.stop_intent_lineage.as_ref().is_some_and(|lineage| {
                        matches!(
                            &lineage.decision,
                            StopIntentLineageDecision::Durable(receipt)
                                if stopped_terminal_matches_active_intent(active, &task, *receipt)
                        )
                    }) =>
            {
                active.stop_state = ActiveStopState::TerminalWon { task: task.clone() };
                for waiter in std::mem::take(&mut active.user_cancel_waiters) {
                    send_terminal_cancel_response(waiter, task.clone());
                }
            }
            Ok(_) | Err(_) => {
                for waiter in std::mem::take(&mut active.user_cancel_waiters) {
                    let _ = waiter.send(Err(TaskManagerError::StoreDegraded));
                }
                self.enter_degraded(None);
                return;
            }
        }
        self.advance_stop_after_barriers(task_id, operation_nonce);
    }

    pub(super) fn fail_stop_submission(&mut self, pending: PendingDurableResult) {
        let identity = pending.identity();
        let task_ids = match &identity {
            DurableOperationIdentity::StopIntentBatch { items } => {
                items.iter().map(|item| item.task_id).collect::<Vec<_>>()
            }
            DurableOperationIdentity::TaskMutation(identity) => vec![identity.task_id],
            DurableOperationIdentity::CreateTask { .. }
            | DurableOperationIdentity::RetryTask { .. } => Vec::new(),
        };
        for task_id in &task_ids {
            if let Some(active) = self.active.get_mut(task_id) {
                active.durable_sequence_blocked = true;
                for waiter in std::mem::take(&mut active.user_cancel_waiters) {
                    let _ = waiter.send(Err(TaskManagerError::StoreDegraded));
                }
            }
        }
        self.enter_degraded(Some(pending));
        for task_id in task_ids {
            let operation_nonce = self
                .active
                .get(&task_id)
                .map(|active| active.operation_nonce);
            if let Some(operation_nonce) = operation_nonce {
                self.advance_stop_after_barriers(task_id, operation_nonce);
            }
        }
    }

    pub(super) fn fail_user_cancel_waiters(&mut self, task_id: TaskId, _error: TaskManagerError) {
        if let Some(active) = self.active.get_mut(&task_id) {
            for waiter in std::mem::take(&mut active.user_cancel_waiters) {
                let _ = waiter.send(Err(TaskManagerError::StoreDegraded));
            }
        }
    }

    pub(super) fn advance_stop_after_barriers(&mut self, task_id: TaskId, operation_nonce: u64) {
        let Some(active) = self.active.get(&task_id) else {
            return;
        };
        if active.operation_nonce != operation_nonce
            || active.cleanup_confirmation.is_none()
            || active.in_flight_mutations != 0
        {
            return;
        }
        if active.phase == AdmissionPhase::RunnerReturned
            && let ActiveStopState::TerminalWon { task } = &active.stop_state
        {
            let task = task.clone();
            let Some(event_kind) = terminal_event_kind(task.status) else {
                self.freeze_degraded();
                return;
            };
            let event_id = task.last_event_id;
            if !terminal_receipt_is_exact(Some(active), &task, event_kind, event_id) {
                self.freeze_degraded();
                return;
            }
            let Some(active) = self.active.get_mut(&task_id) else {
                return;
            };
            active.phase = AdmissionPhase::TerminalWritePending;
            self.start_terminal_projection(task_id, operation_nonce, task, event_kind, event_id);
            return;
        }
        if self.frozen {
            return;
        }
        match &active.stop_state {
            ActiveStopState::IntentDurable { receipt, .. }
                if active.phase == AdmissionPhase::RunnerReturned
                    && (receipt.kind != StopIntentKind::UserCancelled
                        || (!active.accepted_stop_task_load_in_flight
                            && active.user_cancel_waiters.is_empty())) =>
            {
                self.start_stop_finalization(task_id, operation_nonce);
            }
            ActiveStopState::NoWinner
            | ActiveStopState::IntentSubmissionDeferred { .. }
            | ActiveStopState::IntentWritePending { .. }
            | ActiveStopState::IntentDurable { .. }
            | ActiveStopState::FinalStopWritePending { .. }
            | ActiveStopState::StopTerminal { .. }
            | ActiveStopState::TerminalWon { .. } => {}
        }
    }
}
