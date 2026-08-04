use super::*;

#[derive(Debug)]
pub(super) struct ActiveRepositoryControlRecovery {
    task_id: TaskId,
    repository_id: RepositoryId,
    attempt: u32,
    operation_nonce: u64,
    process_owner_id: u64,
    coordination_key: RepositoryCoordinationKey,
    witness: RepositoryControlRecoveryWitness,
}

impl ActiveRepositoryControlRecovery {
    pub(super) fn new(
        task_id: TaskId,
        repository_id: RepositoryId,
        attempt: u32,
        operation_nonce: u64,
        process_owner_id: u64,
        coordination_key: RepositoryCoordinationKey,
        witness: RepositoryControlRecoveryWitness,
    ) -> Self {
        Self {
            task_id,
            repository_id,
            attempt,
            operation_nonce,
            process_owner_id,
            coordination_key,
            witness,
        }
    }

    fn is_exact(
        &self,
        task_id: TaskId,
        repository_id: RepositoryId,
        attempt: u32,
        operation_nonce: u64,
        process_owner_id: u64,
        coordination_key: RepositoryCoordinationKey,
    ) -> bool {
        self.task_id == task_id
            && self.repository_id == repository_id
            && self.attempt == attempt
            && self.operation_nonce == operation_nonce
            && self.process_owner_id == process_owner_id
            && self.coordination_key == coordination_key
    }

    fn state(&self) -> RepositoryControlRecoveryState {
        self.witness.state()
    }

    fn settle_after_process_cleanup(self) -> bool {
        self.witness.settle_after_process_cleanup().is_ok()
    }
}

impl TaskManager {
    /// The runner publishes cleanup retention through the witness before its
    /// completion reaches the actor. This closes the final admission gate in
    /// that short window; the actor-owned RunnerReturned phase remains the
    /// source for exact scheduler projection transitions.
    pub(super) fn repository_control_recovery_pauses_admission(&self) -> bool {
        self.active.values().any(|active| {
            active
                .control_recovery
                .as_ref()
                .is_some_and(|recovery| recovery.state().pauses_for_process_cleanup())
        })
    }

    pub(super) fn exact_repository_control_recovery_pauses_admission(
        &self,
        task_id: TaskId,
        operation_nonce: u64,
    ) -> bool {
        self.active.get(&task_id).is_some_and(|active| {
            active.operation_nonce == operation_nonce
                && active
                    .control_recovery
                    .as_ref()
                    .is_some_and(|recovery| recovery.state().pauses_for_process_cleanup())
        })
    }

    pub(super) fn runner_return_matches_repository_control_recovery(
        &self,
        task_id: TaskId,
        operation_nonce: u64,
        outcome: &RunnerOutcome,
    ) -> bool {
        let Some(active) = self.active.get(&task_id) else {
            return false;
        };
        let Some(recovery) = active.control_recovery.as_ref() else {
            return false;
        };
        if active.control_lease.is_some() {
            return false;
        }
        if !recovery.is_exact(
            task_id,
            active.repository_id,
            active.attempt,
            operation_nonce,
            active.permit.process_owner_id(),
            active.permit.coordination_key(),
        ) {
            return false;
        }
        match recovery.state() {
            RepositoryControlRecoveryState::Released => true,
            RepositoryControlRecoveryState::CleanupRetained => {
                active.phase == AdmissionPhase::Preparing
                    && !active.preparation_complete
                    && matches!(outcome, RunnerOutcome::ProcessCleanupUnproven)
            }
            RepositoryControlRecoveryState::AbnormalRetained => {
                active.phase == AdmissionPhase::Preparing
                    && !active.preparation_complete
                    && matches!(outcome, RunnerOutcome::Failed(_) | RunnerOutcome::Cancelled)
            }
            RepositoryControlRecoveryState::ReconciliationRetained => {
                active.phase == AdmissionPhase::Preparing
                    && !active.preparation_complete
                    && matches!(outcome, RunnerOutcome::Failed(_))
            }
            RepositoryControlRecoveryState::Live | RepositoryControlRecoveryState::Invalid => false,
        }
    }

    pub(super) fn settle_repository_control_after_process_cleanup(
        &mut self,
        task_id: TaskId,
        operation_nonce: u64,
    ) -> bool {
        let Some(active) = self.active.get_mut(&task_id) else {
            return false;
        };
        if active.control_lease.is_some() {
            return false;
        }
        let Some(recovery) = active.control_recovery.take() else {
            return false;
        };
        recovery.is_exact(
            task_id,
            active.repository_id,
            active.attempt,
            operation_nonce,
            active.permit.process_owner_id(),
            active.permit.coordination_key(),
        ) && recovery.settle_after_process_cleanup()
    }
}
