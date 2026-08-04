use super::*;

impl TaskManager {
    pub(super) async fn project_and_handle_stop_intent_persisted(
        &mut self,
        identity: DurableOperationIdentity,
        completion: DurableCompletion<StopIntentBatchReceipt>,
    ) -> StopCompletionDrain {
        if let Err(error) = self.refresh_scheduler_projection().await {
            tracing::error!(%error, "durable stop-intent scheduler projection failed");
            self.freeze_degraded();
            return StopCompletionDrain::Stop;
        }
        self.handle_stop_intent_persisted(identity, completion)
    }
}
