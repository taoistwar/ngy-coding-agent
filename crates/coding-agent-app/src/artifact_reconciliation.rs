use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use coding_agent_domain::RepositoryId;
use coding_agent_runtime::{WorktreeIdentity, WorktreeObservation, WorktreeProvisioner};
use coding_agent_store::{AttemptArtifactState, Store, StoreError, TaskAttemptArtifact};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{StoreWriterError, StoreWriterHandle};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartArtifactObservation {
    Absent,
    Ready,
    Partial,
    Inconsistent,
}

#[async_trait::async_trait]
pub trait AttemptArtifactObserver: Send + Sync {
    async fn observe(&self, artifact: &TaskAttemptArtifact) -> RestartArtifactObservation;
}

/// Runtime-backed observer for restart reconciliation. Each provisioner is
/// already bound to one registered repository and its retained Git/artifact
/// capabilities; persisted rows provide identity values, never authority.
pub struct WorktreeArtifactObserver {
    provisioners: HashMap<RepositoryId, Arc<WorktreeProvisioner>>,
}

impl WorktreeArtifactObserver {
    pub fn new(
        provisioners: impl IntoIterator<Item = (RepositoryId, Arc<WorktreeProvisioner>)>,
    ) -> Self {
        Self {
            provisioners: provisioners.into_iter().collect(),
        }
    }
}

#[async_trait::async_trait]
impl AttemptArtifactObserver for WorktreeArtifactObserver {
    async fn observe(&self, artifact: &TaskAttemptArtifact) -> RestartArtifactObservation {
        let Some(provisioner) = self.provisioners.get(&artifact.identity.repository_id) else {
            return RestartArtifactObservation::Inconsistent;
        };
        let identity = match WorktreeIdentity::try_new(
            artifact.identity.repository_id.to_string(),
            artifact.identity.task_id.to_string(),
            artifact.identity.attempt,
        ) {
            Ok(identity) => identity,
            Err(_) => return RestartArtifactObservation::Inconsistent,
        };
        let reservation = match provisioner.restore_reservation(
            identity,
            artifact.base_commit.clone(),
            artifact.branch_name.clone(),
            artifact.worktree_path.as_path().to_owned(),
        ) {
            Ok(reservation) => reservation,
            Err(_) => return RestartArtifactObservation::Inconsistent,
        };
        match provisioner
            .observe(&reservation, CancellationToken::new())
            .await
        {
            WorktreeObservation::Absent => RestartArtifactObservation::Absent,
            WorktreeObservation::Ready => RestartArtifactObservation::Ready,
            WorktreeObservation::BranchOnly
            | WorktreeObservation::AdministrativeCreated
            | WorktreeObservation::CheckoutPartial => RestartArtifactObservation::Partial,
            WorktreeObservation::Inconsistent => RestartArtifactObservation::Inconsistent,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArtifactReconciliationSummary {
    pub examined: usize,
    pub marked_ready: usize,
    pub marked_inconsistent: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum ArtifactReconciliationError {
    #[error("artifact reconciliation write timeout must be non-zero")]
    InvalidTimeout,
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Writer(#[from] StoreWriterError),
}

/// Reconciles only restart-abandoned `reserved` rows. Same-run reentry does not
/// call this function and may continue provisioning an identical reservation.
/// All mutations remain serialized through `StoreWriterHandle`.
pub async fn reconcile_restart_artifacts(
    store: &Store,
    writer: &StoreWriterHandle,
    observer: &dyn AttemptArtifactObserver,
    write_timeout: Duration,
) -> Result<ArtifactReconciliationSummary, ArtifactReconciliationError> {
    if write_timeout.is_zero() {
        return Err(ArtifactReconciliationError::InvalidTimeout);
    }
    let artifacts = store.list_reserved_attempt_artifacts().await?;
    let mut summary = ArtifactReconciliationSummary::default();
    for artifact in artifacts {
        debug_assert_eq!(artifact.state, AttemptArtifactState::Reserved);
        summary.examined += 1;
        let observation = observer.observe(&artifact).await;
        let deadline = Instant::now() + write_timeout;
        match observation {
            RestartArtifactObservation::Ready => {
                writer
                    .mark_attempt_artifact_ready(artifact.identity, deadline)
                    .await?;
                summary.marked_ready += 1;
            }
            RestartArtifactObservation::Absent => {
                writer
                    .mark_attempt_artifact_inconsistent(
                        artifact.identity,
                        "WORKTREE_RESERVATION_ABANDONED",
                        deadline,
                    )
                    .await?;
                summary.marked_inconsistent += 1;
            }
            RestartArtifactObservation::Partial | RestartArtifactObservation::Inconsistent => {
                writer
                    .mark_attempt_artifact_inconsistent(
                        artifact.identity,
                        "WORKTREE_STATE_INCONSISTENT",
                        deadline,
                    )
                    .await?;
                summary.marked_inconsistent += 1;
            }
        }
    }
    Ok(summary)
}
