use coding_agent_domain::{Repository, Task};
use coding_agent_runtime::{ProcessCleanupProof, ProcessLivenessScope};
use coding_agent_store::AttemptArtifactIdentity;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::repository_control::RepositoryControlLease;
use crate::task_manager::TaskProcessScopeOwnership;
use crate::{PermitOwnershipState, PermitOwnershipWitness, RepositoryCoordinationKey};

#[derive(Debug)]
pub(crate) struct PreparationCompleted {
    task_id: coding_agent_domain::TaskId,
    coordination_key: RepositoryCoordinationKey,
    operation_nonce: u64,
    acknowledgement: oneshot::Sender<()>,
}

impl PreparationCompleted {
    pub(crate) const fn task_id(&self) -> coding_agent_domain::TaskId {
        self.task_id
    }

    pub(crate) const fn coordination_key(&self) -> RepositoryCoordinationKey {
        self.coordination_key
    }

    pub(crate) const fn operation_nonce(&self) -> u64 {
        self.operation_nonce
    }

    pub(crate) fn acknowledge(self) {
        let _ = self.acknowledgement.send(());
    }
}

/// Move-only ownership passed from the claim boundary into one task run.
///
/// Task 11 establishes the preparation ownership slots. Task 15 adds concrete
/// process ownership, and Task 17 installs scheduler permits before adoption.
/// A context dropped while it still owns a repository lease inherits the
/// lease's fail-closed poison-on-drop behavior.
#[derive(Debug)]
pub struct RunContext {
    pub task: Task,
    pub repository: Repository,
    pub cancellation: CancellationToken,
    ownership: RunOwnership,
    #[cfg(feature = "test-support")]
    launch_ordinal: u64,
}

#[derive(Debug)]
struct RunOwnership {
    control_lease: Option<RepositoryControlLease>,
    artifact_identity: Option<AttemptArtifactIdentity>,
    process_scope: TaskProcessScopeOwnership,
    permit: PermitOwnershipWitness,
    preparation_completion: Option<mpsc::Sender<PreparationCompleted>>,
    preparation_complete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum RunContextOwnershipError {
    #[error("the run context does not own a repository control lease")]
    ControlLeaseNotOwned,
    #[error("the run context artifact identity does not match the claimed task")]
    ArtifactIdentityMismatch,
    #[error("the run context artifact identity was already installed")]
    ArtifactIdentityAlreadyInstalled,
    #[error("the run context task and repository do not match")]
    TaskRepositoryMismatch,
    #[error("the run context permit does not belong to the claimed task")]
    PermitTaskMismatch,
    #[error("the run context lease and permit coordination keys do not match")]
    CoordinationKeyMismatch,
    #[error("the run context process-liveness scope does not belong to the claimed task")]
    ProcessLivenessScopeMismatch,
    #[error("the preparation completion owner is closed")]
    PreparationCompletionClosed,
    #[error("attempt preparation has already completed")]
    PreparationAlreadyComplete,
    #[error("attempt preparation cannot complete while the control lease is owned")]
    ControlLeaseStillOwned,
}

impl RunContext {
    #[cfg(feature = "test-support")]
    // The test-only adoption seam mirrors the ownership witnesses required by
    // `adopt` and adds only a deterministic launch ordinal. Grouping them would
    // obscure the one-to-one invariant and churn every test call site.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn adopt_with_launch_ordinal(
        task: Task,
        repository: Repository,
        cancellation: CancellationToken,
        control_lease: RepositoryControlLease,
        process_scope: TaskProcessScopeOwnership,
        permit: PermitOwnershipWitness,
        preparation_completion: mpsc::Sender<PreparationCompleted>,
        launch_ordinal: u64,
    ) -> Result<Self, RunContextOwnershipError> {
        let mut context = Self::adopt(
            task,
            repository,
            cancellation,
            control_lease,
            process_scope,
            permit,
            preparation_completion,
        )?;
        context.launch_ordinal = launch_ordinal;
        Ok(context)
    }

    pub(crate) fn adopt(
        task: Task,
        repository: Repository,
        cancellation: CancellationToken,
        control_lease: RepositoryControlLease,
        process_scope: TaskProcessScopeOwnership,
        permit: PermitOwnershipWitness,
        preparation_completion: mpsc::Sender<PreparationCompleted>,
    ) -> Result<Self, RunContextOwnershipError> {
        if task.repository_id != repository.id {
            return Err(RunContextOwnershipError::TaskRepositoryMismatch);
        }
        if permit.task_id() != task.id {
            return Err(RunContextOwnershipError::PermitTaskMismatch);
        }
        if permit.state() != Ok(PermitOwnershipState::Active) {
            return Err(RunContextOwnershipError::PermitTaskMismatch);
        }
        if permit.coordination_key() != control_lease.coordination_key() {
            return Err(RunContextOwnershipError::CoordinationKeyMismatch);
        }
        if process_scope.task_id() != task.id
            || process_scope.operation_nonce() != permit.admission_nonce()
            || process_scope.owner_id() != permit.process_owner_id()
        {
            return Err(RunContextOwnershipError::ProcessLivenessScopeMismatch);
        }
        if !matches!(
            process_scope
                .scope()
                .cleanup_proof_for_task(*task.id.as_uuid().as_bytes()),
            Ok(ProcessCleanupProof::Confirmed)
        ) {
            return Err(RunContextOwnershipError::ProcessLivenessScopeMismatch);
        }
        Ok(Self {
            task,
            repository,
            cancellation,
            ownership: RunOwnership {
                control_lease: Some(control_lease),
                artifact_identity: None,
                process_scope,
                permit,
                preparation_completion: Some(preparation_completion),
                preparation_complete: false,
            },
            #[cfg(feature = "test-support")]
            launch_ordinal: 0,
        })
    }

    pub fn process_liveness_scope(&self) -> &ProcessLivenessScope {
        self.ownership.process_scope.scope()
    }

    pub(crate) fn take_control_lease(
        &mut self,
    ) -> Result<RepositoryControlLease, RunContextOwnershipError> {
        self.ownership
            .control_lease
            .take()
            .ok_or(RunContextOwnershipError::ControlLeaseNotOwned)
    }

    pub(crate) fn record_artifact_identity(
        &mut self,
        identity: AttemptArtifactIdentity,
    ) -> Result<(), RunContextOwnershipError> {
        if identity.task_id != self.task.id
            || identity.repository_id != self.repository.id
            || identity.attempt != self.task.attempt
        {
            return Err(RunContextOwnershipError::ArtifactIdentityMismatch);
        }
        if self.ownership.artifact_identity.replace(identity).is_some() {
            return Err(RunContextOwnershipError::ArtifactIdentityAlreadyInstalled);
        }
        Ok(())
    }

    pub(crate) async fn mark_preparation_complete(
        &mut self,
    ) -> Result<(), RunContextOwnershipError> {
        if self.ownership.preparation_complete {
            return Err(RunContextOwnershipError::PreparationAlreadyComplete);
        }
        if self.ownership.control_lease.is_some() {
            return Err(RunContextOwnershipError::ControlLeaseStillOwned);
        }
        if self.ownership.artifact_identity.is_none() {
            return Err(RunContextOwnershipError::ArtifactIdentityMismatch);
        }
        let completion = self
            .ownership
            .preparation_completion
            .take()
            .ok_or(RunContextOwnershipError::PreparationAlreadyComplete)?;
        let (acknowledgement, acknowledged) = oneshot::channel();
        completion
            .send(PreparationCompleted {
                task_id: self.task.id,
                coordination_key: self.ownership.permit.coordination_key(),
                operation_nonce: self.ownership.permit.admission_nonce(),
                acknowledgement,
            })
            .await
            .map_err(|_| RunContextOwnershipError::PreparationCompletionClosed)?;
        acknowledged
            .await
            .map_err(|_| RunContextOwnershipError::PreparationCompletionClosed)?;
        self.ownership.preparation_complete = true;
        Ok(())
    }

    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub async fn complete_preparation_for_test(&mut self) {
        self.record_artifact_identity(AttemptArtifactIdentity {
            task_id: self.task.id,
            repository_id: self.repository.id,
            attempt: self.task.attempt,
        })
        .expect("test RunContext installs the matching synthetic artifact identity");
        let lease = self
            .take_control_lease()
            .expect("test RunContext owns its pre-acquired repository lease");
        lease
            .clean_release()
            .expect("test RunContext releases its pre-acquired repository lease");
        self.mark_preparation_complete()
            .await
            .expect("test RunContext uses the production preparation completion path");
    }

    pub(crate) const fn artifact_identity(&self) -> Option<AttemptArtifactIdentity> {
        self.ownership.artifact_identity
    }

    #[cfg(feature = "test-support")]
    pub(crate) const fn launch_ordinal(&self) -> u64 {
        self.launch_ordinal
    }
}
