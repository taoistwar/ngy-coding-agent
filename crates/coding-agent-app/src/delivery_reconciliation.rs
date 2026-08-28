use std::collections::HashMap;

use coding_agent_domain::TaskId;
use coding_agent_store::{
    AttemptArtifactState, DeliveryOperationId, DeliveryRecoveryAction, DeliveryRecoveryDisposition,
    DeliveryRecoveryQuery, DirectoryIdentity, StartupDeliveryOwnership, Store, StoreError,
    TaskAttemptArtifact,
};

use crate::{
    DeliveryManagerError, DeliveryManagerHandle, DeliveryOperationRecoveryOutcome,
    RepositoryControlCoordinator, RepositoryControlError, RepositoryControlPoisonReason,
};

const MAX_PENDING_RECOVERY_PASSES: usize = 16;

/// Startup routing decision for an attempt artifact.
///
/// P4-A may observe only artifacts for which the audited delivery ownership
/// join is absent. Delivery-owned artifacts are interpreted exclusively by
/// the P4-B recovery pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StartupArtifactRoute {
    BaseLifecycle,
    DeliveryOwned,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DeliveryOwnershipRoutingError {
    #[error("the delivery startup ownership snapshot is inconsistent")]
    Inconsistent,
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// One audited, immutable ownership overlay loaded before either reconciler
/// runs. Construction delegates the complete task/attempt/artifact/delivery
/// graph audit to Store's single-read startup query.
#[derive(Clone)]
pub(crate) struct DeliveryArtifactOwnershipRouter {
    ownership: Vec<StartupDeliveryOwnership>,
    by_task: HashMap<TaskId, usize>,
}

impl DeliveryArtifactOwnershipRouter {
    pub(crate) async fn load(store: &Store) -> Result<Self, DeliveryOwnershipRoutingError> {
        Self::from_audited(store.startup_delivery_ownership().await?)
    }

    fn from_audited(
        ownership: Vec<StartupDeliveryOwnership>,
    ) -> Result<Self, DeliveryOwnershipRoutingError> {
        let mut by_task = HashMap::with_capacity(ownership.len());
        for (index, item) in ownership.iter().enumerate() {
            if by_task.insert(item.identity.task_id(), index).is_some() {
                return Err(DeliveryOwnershipRoutingError::Inconsistent);
            }
        }
        Ok(Self { ownership, by_task })
    }

    pub(crate) fn route(
        &self,
        artifact: &TaskAttemptArtifact,
    ) -> Result<StartupArtifactRoute, DeliveryOwnershipRoutingError> {
        let Some(index) = self.by_task.get(&artifact.identity.task_id).copied() else {
            return Ok(StartupArtifactRoute::BaseLifecycle);
        };
        let owner = self
            .ownership
            .get(index)
            .ok_or(DeliveryOwnershipRoutingError::Inconsistent)?;
        if owner.identity.repository_id() != artifact.identity.repository_id
            || owner.identity.attempt() != artifact.identity.attempt
            || artifact.state != AttemptArtifactState::Ready
        {
            return Err(DeliveryOwnershipRoutingError::Inconsistent);
        }
        Ok(StartupArtifactRoute::DeliveryOwned)
    }

    pub(crate) fn require_base_lifecycle(
        &self,
        artifact: &TaskAttemptArtifact,
    ) -> Result<(), DeliveryOwnershipRoutingError> {
        match self.route(artifact)? {
            StartupArtifactRoute::BaseLifecycle => Ok(()),
            StartupArtifactRoute::DeliveryOwned => Err(DeliveryOwnershipRoutingError::Inconsistent),
        }
    }

    pub(crate) fn ownership(&self) -> &[StartupDeliveryOwnership] {
        &self.ownership
    }
}

impl std::fmt::Debug for DeliveryArtifactOwnershipRouter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeliveryArtifactOwnershipRouter")
            .field("owned_task_count", &self.ownership.len())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DeliveryStartupRecoverySummary {
    pub(crate) owned_tasks: usize,
    pub(crate) recovered_operations: usize,
    pub(crate) reconciliation_required: usize,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DeliveryStartupRecoveryError {
    #[error("delivery startup recovery ownership is inconsistent")]
    Inconsistent,
    #[error("delivery startup recovery did not settle")]
    Pending,
    #[error("delivery startup recovery runtime is unavailable")]
    Unavailable,
    #[error("delivery startup recovery retained fail-closed ownership")]
    RetainedFailClosed,
    #[error(transparent)]
    Manager(#[from] DeliveryManagerError),
    #[error(transparent)]
    RepositoryControl(#[from] RepositoryControlError),
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Drives only operations already present in Store's audited recovery query.
/// It never creates a preflight, acceptance, or cleanup receipt. Pagination is
/// identity-bound and every operation is awaited before later work for that
/// common Git identity can begin.
pub(crate) struct DeliveryStartupRecoveryCoordinator<'a> {
    store: &'a Store,
    manager: &'a DeliveryManagerHandle,
    repository_control: &'a RepositoryControlCoordinator,
}

impl<'a> DeliveryStartupRecoveryCoordinator<'a> {
    pub(crate) fn new(
        store: &'a Store,
        manager: &'a DeliveryManagerHandle,
        repository_control: &'a RepositoryControlCoordinator,
    ) -> Self {
        Self {
            store,
            manager,
            repository_control,
        }
    }

    pub(crate) async fn recover(
        &self,
        ownership: &[StartupDeliveryOwnership],
    ) -> Result<DeliveryStartupRecoverySummary, DeliveryStartupRecoveryError> {
        let groups = group_ownership(ownership, self.repository_control)?;
        let mut summary = DeliveryStartupRecoverySummary {
            owned_tasks: ownership.len(),
            ..DeliveryStartupRecoverySummary::default()
        };
        for group in groups {
            self.recover_group(group, &mut summary).await?;
        }
        Ok(summary)
    }

    async fn recover_group(
        &self,
        group: DeliveryRecoveryIdentityGroup,
        summary: &mut DeliveryStartupRecoverySummary,
    ) -> Result<(), DeliveryStartupRecoveryError> {
        let mut query = DeliveryRecoveryQuery::first(group.identity.clone());
        loop {
            let batch = self.store.delivery_recovery_batch(&query).await?;
            for entry in batch.entries {
                if entry.expected_common_git_identity != group.identity
                    || !group
                        .repository_ids
                        .contains(&entry.identity.repository_id())
                {
                    return Err(DeliveryStartupRecoveryError::Inconsistent);
                }
                match entry.disposition {
                    DeliveryRecoveryDisposition::ReconciliationRequired => {
                        // The first uncertain fact is a mutation barrier for this
                        // common Git identity. Later durable operations stay pending;
                        // the outer loop still recovers independent identities.
                        self.mark_reconciliation_required(&group, summary)?;
                        return Ok(());
                    }
                    DeliveryRecoveryDisposition::Recover(action) => {
                        let operation_id = recovery_operation_id(&action);
                        match self.recover_operation(operation_id).await? {
                            SettledRecovery::Converged => {
                                summary.recovered_operations = summary
                                    .recovered_operations
                                    .checked_add(1)
                                    .ok_or(DeliveryStartupRecoveryError::Inconsistent)?;
                            }
                            SettledRecovery::ReconciliationRequired => {
                                // The manager has already classified this identity as
                                // unsafe. Do not bypass its sticky poison for later work.
                                self.mark_reconciliation_required(&group, summary)?;
                                return Ok(());
                            }
                        }
                    }
                }
            }
            let Some(cursor) = batch.next_cursor else {
                break;
            };
            query = DeliveryRecoveryQuery::try_after(group.identity.clone(), cursor)
                .map_err(|_| DeliveryStartupRecoveryError::Inconsistent)?;
        }
        Ok(())
    }

    fn mark_reconciliation_required(
        &self,
        group: &DeliveryRecoveryIdentityGroup,
        summary: &mut DeliveryStartupRecoverySummary,
    ) -> Result<(), DeliveryStartupRecoveryError> {
        self.repository_control.require_reconciliation(
            group.coordination_key,
            RepositoryControlPoisonReason::DeliveryReconciliationRequired,
        )?;
        summary.reconciliation_required = summary
            .reconciliation_required
            .checked_add(1)
            .ok_or(DeliveryStartupRecoveryError::Inconsistent)?;
        Ok(())
    }

    async fn recover_operation(
        &self,
        operation_id: DeliveryOperationId,
    ) -> Result<SettledRecovery, DeliveryStartupRecoveryError> {
        for _ in 0..MAX_PENDING_RECOVERY_PASSES {
            match self.manager.recover_operation(operation_id).await? {
                DeliveryOperationRecoveryOutcome::Converged => {
                    return Ok(SettledRecovery::Converged);
                }
                DeliveryOperationRecoveryOutcome::ReconciliationRequired => {
                    return Ok(SettledRecovery::ReconciliationRequired);
                }
                DeliveryOperationRecoveryOutcome::Pending => tokio::task::yield_now().await,
                DeliveryOperationRecoveryOutcome::NotFound => {
                    return Err(DeliveryStartupRecoveryError::Inconsistent);
                }
                DeliveryOperationRecoveryOutcome::RetainedFailClosed => {
                    return Err(DeliveryStartupRecoveryError::RetainedFailClosed);
                }
                DeliveryOperationRecoveryOutcome::Unavailable => {
                    return Err(DeliveryStartupRecoveryError::Unavailable);
                }
            }
        }
        Err(DeliveryStartupRecoveryError::Pending)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettledRecovery {
    Converged,
    ReconciliationRequired,
}

struct DeliveryRecoveryIdentityGroup {
    identity: DirectoryIdentity,
    coordination_key: crate::RepositoryCoordinationKey,
    repository_ids: Vec<coding_agent_domain::RepositoryId>,
}

fn group_ownership(
    ownership: &[StartupDeliveryOwnership],
    repository_control: &RepositoryControlCoordinator,
) -> Result<Vec<DeliveryRecoveryIdentityGroup>, DeliveryStartupRecoveryError> {
    let mut groups = Vec::<DeliveryRecoveryIdentityGroup>::new();
    for owner in ownership {
        let repository_id = owner.identity.repository_id();
        let coordination_key = repository_control.delivery_coordination_key(repository_id)?;
        if let Some(group) = groups
            .iter_mut()
            .find(|group| group.identity == owner.expected_common_git_identity)
        {
            if group.coordination_key != coordination_key {
                return Err(DeliveryStartupRecoveryError::Inconsistent);
            }
            if !group.repository_ids.contains(&repository_id) {
                group.repository_ids.push(repository_id);
            }
        } else {
            if groups
                .iter()
                .any(|group| group.coordination_key == coordination_key)
            {
                return Err(DeliveryStartupRecoveryError::Inconsistent);
            }
            groups.push(DeliveryRecoveryIdentityGroup {
                identity: owner.expected_common_git_identity.clone(),
                coordination_key,
                repository_ids: vec![repository_id],
            });
        }
    }
    Ok(groups)
}

const fn recovery_operation_id(action: &DeliveryRecoveryAction) -> DeliveryOperationId {
    match action {
        DeliveryRecoveryAction::PreflightPending { operation_id, .. }
        | DeliveryRecoveryAction::Accepted { operation_id, .. }
        | DeliveryRecoveryAction::MergePending { operation_id, .. }
        | DeliveryRecoveryAction::AbortPending { operation_id, .. }
        | DeliveryRecoveryAction::UnlockPending { operation_id, .. }
        | DeliveryRecoveryAction::UnlockedPendingRemove { operation_id, .. }
        | DeliveryRecoveryAction::RemovePending { operation_id, .. }
        | DeliveryRecoveryAction::DeletePending { operation_id, .. } => *operation_id,
    }
}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub async fn startup_artifact_is_delivery_owned_for_test(
    store: &Store,
    artifact: &TaskAttemptArtifact,
) -> Result<bool, &'static str> {
    let router = DeliveryArtifactOwnershipRouter::load(store)
        .await
        .map_err(|_| "DELIVERY_STARTUP_OWNERSHIP_INVALID")?;
    router
        .route(artifact)
        .map(|route| route == StartupArtifactRoute::DeliveryOwned)
        .map_err(|_| "DELIVERY_STARTUP_OWNERSHIP_INVALID")
}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub async fn recover_delivery_startup_for_test(
    store: &Store,
    manager: &DeliveryManagerHandle,
    repository_control: &RepositoryControlCoordinator,
) -> Result<(usize, usize, usize), &'static str> {
    let router = DeliveryArtifactOwnershipRouter::load(store)
        .await
        .map_err(|_| "DELIVERY_STARTUP_OWNERSHIP_INVALID")?;
    let summary = DeliveryStartupRecoveryCoordinator::new(store, manager, repository_control)
        .recover(router.ownership())
        .await
        .map_err(|_| "DELIVERY_STARTUP_RECOVERY_FAILED")?;
    Ok((
        summary.owned_tasks,
        summary.recovered_operations,
        summary.reconciliation_required,
    ))
}
