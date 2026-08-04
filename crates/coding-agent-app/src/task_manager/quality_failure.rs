use super::*;

impl TaskManager {
    pub(super) fn apply_quality_write_failure(
        &mut self,
        task_id: TaskId,
        message: &'static str,
        failure: QualityWriteFailure,
    ) {
        tracing::error!(task_id = %task_id, "{message}");
        match failure {
            QualityWriteFailure::RetryNextSequence => {
                unreachable!("retry actions are handled by the submission loop")
            }
            QualityWriteFailure::Replay(pending) => {
                if let Some(active) = self.active.get_mut(&task_id) {
                    active.durable_sequence_blocked = true;
                }
                self.enter_degraded(Some(pending));
            }
            QualityWriteFailure::ColdRecovery => self.enter_degraded(None),
            QualityWriteFailure::Freeze => {
                self.enter_degraded(None);
                self.freeze_degraded();
            }
        }
    }
}
