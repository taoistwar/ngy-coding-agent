use std::fmt;

use coding_agent_domain::{
    DeliveryReadiness, ReviewEvidence, ReviewVerdict, Task, TaskId, TaskStatus,
};

use crate::reviews::{load_stored_reviews_for_task, validate_task_review_aggregate};
use crate::tasks::load_task;
use crate::{Store, StoreError};

use super::evidence::derive_evidence_identity;
use super::ownership::load_delivery_ownership;
use super::{DeliveryIdentity, DeliveryOwnershipSnapshot, EvidenceIdentityV1};
use crate::AttemptArtifactState;

const ELIGIBILITY_INVARIANT: &str = "delivery eligibility snapshot is inconsistent";
const OWNERSHIP_INVARIANT: &str = "delivery ownership snapshot is inconsistent";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistentEligibilityBlocker {
    TaskNotCompleted,
    ReviewNotApproved,
    ApprovedEvidenceMissing,
    AttemptArtifactMissing,
    AttemptArtifactNotReady,
    DeliveryOwned,
    AlreadyMerged,
    ReconciliationRequired,
}

#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryEligibilitySnapshot {
    pub task: Task,
    pub final_review: Option<ReviewEvidence>,
    pub evidence_identity: Option<EvidenceIdentityV1>,
    pub ownership: DeliveryOwnershipSnapshot,
    pub persistent_blockers: Vec<PersistentEligibilityBlocker>,
}

impl fmt::Debug for DeliveryEligibilitySnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let final_review = self
            .final_review
            .as_ref()
            .map(|review| (review.round(), review.verdict()));
        formatter
            .debug_struct("DeliveryEligibilitySnapshot")
            .field("task_id", &self.task.id)
            .field("repository_id", &self.task.repository_id)
            .field("attempt", &self.task.attempt)
            .field("status", &self.task.status)
            .field("delivery_readiness", &self.task.delivery_readiness)
            .field("final_review", &final_review)
            .field("evidence_identity", &self.evidence_identity)
            .field("ownership", &self.ownership)
            .field("persistent_blockers", &self.persistent_blockers)
            .finish()
    }
}

impl Store {
    pub async fn delivery_eligibility_snapshot(
        &self,
        task_id: TaskId,
    ) -> Result<Option<DeliveryEligibilitySnapshot>, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let snapshot = load_snapshot(&mut transaction, task_id)
            .await
            .map_err(|error| snapshot_read_error(error, ELIGIBILITY_INVARIANT))?;
        transaction.commit().await?;
        Ok(snapshot)
    }

    pub async fn delivery_ownership_snapshot(
        &self,
        task_id: TaskId,
    ) -> Result<Option<DeliveryOwnershipSnapshot>, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let snapshot = load_snapshot(&mut transaction, task_id)
            .await
            .map_err(|error| snapshot_read_error(error, OWNERSHIP_INVARIANT))?
            .map(|snapshot| snapshot.ownership);
        transaction.commit().await?;
        Ok(snapshot)
    }
}

fn snapshot_read_error(error: StoreError, invariant: &'static str) -> StoreError {
    match error {
        StoreError::Database(_) => error,
        _ => StoreError::InvariantViolation(invariant),
    }
}

pub(super) async fn load_snapshot(
    connection: &mut sqlx::SqliteConnection,
    task_id: TaskId,
) -> Result<Option<DeliveryEligibilitySnapshot>, StoreError> {
    let Some(task) = load_task(&mut *connection, task_id).await? else {
        return Ok(None);
    };
    let stored_reviews = load_stored_reviews_for_task(&mut *connection, task_id).await?;
    let reviews = stored_reviews
        .iter()
        .map(|stored| stored.review.clone())
        .collect::<Vec<_>>();
    validate_task_review_aggregate(&mut *connection, &task, &reviews).await?;
    let final_stored = stored_reviews.last();
    let final_review = final_stored.map(|stored| stored.review.clone());
    let approved_tuple = task.status == TaskStatus::Completed
        && task.delivery_readiness == DeliveryReadiness::ReviewApproved
        && final_stored.is_some_and(|stored| stored.review.verdict() == ReviewVerdict::Approved);
    let evidence_identity = if approved_tuple {
        let identity = DeliveryIdentity::try_new(task.id, task.repository_id, task.attempt)?;
        Some(derive_evidence_identity(
            identity,
            final_stored.expect("approved tuple has a final review"),
        )?)
    } else {
        None
    };
    let ownership = load_delivery_ownership(
        &mut *connection,
        &task,
        evidence_identity.as_ref(),
        approved_tuple,
    )
    .await?;
    let persistent_blockers = persistent_blockers(&task, evidence_identity.as_ref(), &ownership);
    Ok(Some(DeliveryEligibilitySnapshot {
        task,
        final_review,
        evidence_identity,
        ownership,
        persistent_blockers,
    }))
}

fn persistent_blockers(
    task: &Task,
    evidence: Option<&EvidenceIdentityV1>,
    ownership: &DeliveryOwnershipSnapshot,
) -> Vec<PersistentEligibilityBlocker> {
    let mut blockers = Vec::new();
    if task.status != TaskStatus::Completed {
        blockers.push(PersistentEligibilityBlocker::TaskNotCompleted);
    }
    if task.delivery_readiness != DeliveryReadiness::ReviewApproved {
        blockers.push(PersistentEligibilityBlocker::ReviewNotApproved);
    }
    if evidence.is_none() {
        blockers.push(PersistentEligibilityBlocker::ApprovedEvidenceMissing);
    }
    match ownership.artifact.as_ref().map(|artifact| artifact.state) {
        None => blockers.push(PersistentEligibilityBlocker::AttemptArtifactMissing),
        Some(AttemptArtifactState::Ready) => {}
        Some(AttemptArtifactState::Reserved | AttemptArtifactState::Inconsistent) => {
            blockers.push(PersistentEligibilityBlocker::AttemptArtifactNotReady);
        }
    }
    if ownership.has_blocking_owned_state() {
        blockers.push(PersistentEligibilityBlocker::DeliveryOwned);
    }
    if ownership.has_merged_facts() {
        blockers.push(PersistentEligibilityBlocker::AlreadyMerged);
    }
    if ownership.requires_reconciliation() {
        blockers.push(PersistentEligibilityBlocker::ReconciliationRequired);
    }
    blockers
}
