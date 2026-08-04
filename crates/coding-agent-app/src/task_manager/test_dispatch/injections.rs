use super::super::*;

impl TaskManager {
    pub(super) fn inject_record_review_completion_for_test(
        &mut self,
        identity: TaskMutationIdentity,
        request: RecordReviewRequest,
        completion: DurableCompletion<RecordReviewOutcome>,
        response: oneshot::Sender<Result<EventId, RunnerEventError>>,
    ) {
        let Some((operation_nonce, lineage_id, attempt_id)) =
            self.active.get(&identity.task_id).map(|active| {
                let attempt = active
                    .pending_record_review_writes
                    .iter()
                    .find_map(|(lineage_id, pending)| match pending.stage {
                        RecordReviewWriteStage::Submitted {
                            attempt_id,
                            identity: pending_identity,
                            ..
                        } if pending_identity == identity && pending.request == request => {
                            Some((*lineage_id, attempt_id))
                        }
                        RecordReviewWriteStage::Deferred
                        | RecordReviewWriteStage::Submitted { .. } => None,
                    })
                    .or_else(|| {
                        active
                            .pending_record_review_replays
                            .get(&identity)
                            .filter(|pending| pending.request == request)
                            .map(|pending| (pending.lineage_id, pending.attempt_id))
                    })
                    .unwrap_or((0, 0));
                (active.operation_nonce, attempt.0, attempt.1)
            })
        else {
            let _ = response.send(Err(RunnerEventError::StoreDegraded));
            return;
        };
        self.handle_runner_review_persisted(
            identity.task_id,
            operation_nonce,
            lineage_id,
            attempt_id,
            identity,
            completion,
            Some(response),
        );
    }

    pub(super) fn inject_final_stop_completion_for_test(
        &mut self,
        identity: TaskMutationIdentity,
        request: FinalizeStoppedTaskRequest,
        completion: DurableCompletion<FinalizeStoppedTaskOutcome>,
        response: oneshot::Sender<()>,
    ) {
        let Some((operation_nonce, request_is_exact)) =
            self.active.get(&identity.task_id).map(|active| {
                let request_is_exact = active.applied_final_stop.as_ref().is_some_and(|applied| {
                    applied.identity == identity && applied.request == request
                }) || matches!(
                    &active.stop_state,
                    ActiveStopState::FinalStopWritePending {
                        identity: active_identity,
                        request: active_request,
                        ..
                    } if *active_identity == identity && *active_request == request
                );
                (active.operation_nonce, request_is_exact)
            })
        else {
            self.freeze_degraded();
            let _ = response.send(());
            return;
        };
        if !request_is_exact {
            self.freeze_degraded();
            let _ = response.send(());
            return;
        }
        self.handle_final_stop_persisted(identity.task_id, operation_nonce, identity, completion);
        let _ = response.send(());
    }
}
