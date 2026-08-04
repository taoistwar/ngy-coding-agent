use super::*;

impl TaskManager {
    pub(super) fn enter_degraded(&mut self, pending: Option<PendingDurableResult>) {
        if let Some(pending) = pending
            && !self.pending_durable_results.contains(&pending)
        {
            if !self.advance_exact_barrier_epoch() {
                return;
            }
            self.pending_durable_results.push(pending);
        }
        if !self.degraded {
            let _ = self.service_state.set(ServiceState::StoreDegraded);
            self.degraded = true;
            self.scan_requested = false;
            for active in self.active.values() {
                active.cancellation.cancel();
            }
        }
        self.maybe_start_degraded_recovery();
    }

    pub(super) fn canonical_pending_state(
        &self,
        pending: &PendingDurableResult,
    ) -> Option<CanonicalPendingState> {
        let identity = pending.identity();
        if self
            .pending_durable_results
            .iter()
            .any(|owned| owned.identity() == identity && owned != pending)
        {
            return None;
        }
        let Some(index) = self
            .pending_durable_results
            .iter()
            .position(|owned| owned == pending)
        else {
            return Some(CanonicalPendingState::Absent);
        };
        let task_ids = match &identity {
            DurableOperationIdentity::TaskMutation(identity) => vec![identity.task_id],
            DurableOperationIdentity::StopIntentBatch { items } => {
                items.iter().map(|identity| identity.task_id).collect()
            }
            DurableOperationIdentity::CreateTask { .. }
            | DurableOperationIdentity::RetryTask { .. } => Vec::new(),
        };
        Some(
            if self.pending_durable_results[..index]
                .iter()
                .any(|predecessor| {
                    task_ids.iter().any(|task_id| {
                        durable_identity_contains_task(&predecessor.identity(), *task_id)
                    })
                })
            {
                CanonicalPendingState::Blocked
            } else {
                CanonicalPendingState::Ready
            },
        )
    }

    pub(super) fn resolve_canonical_pending_from_original(
        &mut self,
        pending: PendingDurableResult,
        receipt: PendingReplayReceipt,
    ) -> bool {
        if !pending_replay_receipt_matches(&pending, &receipt) {
            return false;
        }
        let identity = pending.identity();
        if self
            .pending_durable_results
            .iter()
            .any(|owned| owned.identity() == identity && owned != &pending)
        {
            return false;
        }
        let Some(index) = self
            .pending_durable_results
            .iter()
            .position(|owned| owned == &pending)
        else {
            return true;
        };
        if let Some(event_id) = receipt.event_id() {
            self.degraded_replay_high_watermark = Some(
                self.degraded_replay_high_watermark
                    .map_or(event_id, |current| current.max(event_id)),
            );
        }
        let task_ids = match &identity {
            DurableOperationIdentity::TaskMutation(identity) => vec![identity.task_id],
            DurableOperationIdentity::StopIntentBatch { items } => {
                items.iter().map(|identity| identity.task_id).collect()
            }
            DurableOperationIdentity::CreateTask { .. }
            | DurableOperationIdentity::RetryTask { .. } => Vec::new(),
        };
        if self.pending_durable_results[..index]
            .iter()
            .any(|predecessor| {
                task_ids.iter().any(|task_id| {
                    durable_identity_contains_task(&predecessor.identity(), *task_id)
                })
            })
        {
            return false;
        }
        self.pending_durable_results.remove(index);
        if self
            .pending_replay_in_flight
            .as_ref()
            .is_some_and(|attempt| attempt.pending == pending)
        {
            match self.resolved_pending_replays.entry(identity) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(ResolvedPendingReplay {
                        pending: pending.clone(),
                        receipt,
                    });
                }
                std::collections::hash_map::Entry::Occupied(_) => return false,
            }
        }
        for task_id in task_ids {
            let remains_blocked = self
                .pending_durable_results
                .iter()
                .any(|pending| durable_identity_contains_task(&pending.identity(), task_id));
            if let Some(active) = self.active.get_mut(&task_id) {
                active.durable_sequence_blocked = remains_blocked;
            }
        }
        if self.drain_staged_stop_intent_completions() == StopCompletionDrain::Stop {
            return false;
        }
        if !self.frozen {
            self.drain_deferred_stop_submissions();
        }
        true
    }
}
