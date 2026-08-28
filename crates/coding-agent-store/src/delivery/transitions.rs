use coding_agent_domain::TaskId;

use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::merges::{
    OperationLookup, TransitionLookup, load_operation_for_caller, lookup_transition,
};
use super::mutation::{
    DeliveryMutationEntity, DeliveryMutationEntityKind, DeliveryMutationKey, DeliveryMutationKind,
    impl_delivery_mutation_request,
};
use super::ownership::load_merge_operation_exact;
use super::{
    DeliveryIdentity, DeliveryOperationId, DeliveryTimestamp, DeliveryVersion, MergeOperationState,
};

const TRANSITION_INVARIANT: &str = "delivery preflight transition is inconsistent";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreflightStaleReason {
    EvidenceStale,
    TargetBranchChanged,
    TargetHeadChanged,
    SourceChanged,
}

impl PreflightStaleReason {
    pub const fn as_failure_code(self) -> &'static str {
        match self {
            Self::EvidenceStale => "DELIVERY_EVIDENCE_STALE",
            Self::TargetBranchChanged => "TARGET_BRANCH_MISMATCH",
            Self::TargetHeadChanged => "TARGET_HEAD_CHANGED",
            Self::SourceChanged => "DELIVERY_SOURCE_CHANGED",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarkPreflightStaleRequest {
    task_id: TaskId,
    operation_id: DeliveryOperationId,
    expected_version: DeliveryVersion,
    reason: PreflightStaleReason,
}

impl MarkPreflightStaleRequest {
    pub fn try_new(
        task_id: TaskId,
        operation_id: DeliveryOperationId,
        expected_version: DeliveryVersion,
        reason: PreflightStaleReason,
    ) -> Result<Self, super::DeliveryError> {
        if task_id.as_uuid().is_nil() || operation_id.as_uuid().is_nil() {
            return Err(super::DeliveryError::InvalidCommandRequest);
        }
        expected_version.next()?;
        Ok(Self {
            task_id,
            operation_id,
            expected_version,
            reason,
        })
    }
}

impl_delivery_mutation_request!(MarkPreflightStaleRequest, |request| {
    DeliveryMutationKey::new(
        DeliveryMutationKind::MarkMergePreflightStale,
        request.task_id,
        vec![DeliveryMutationEntity::operation(
            DeliveryMutationEntityKind::MergeOperation,
            request.operation_id,
            request.expected_version,
        )],
        None,
    )
});

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkPreflightStaleOutcome {
    Applied {
        operation_id: DeliveryOperationId,
        version: DeliveryVersion,
        state: MergeOperationState,
        reason: PreflightStaleReason,
    },
    Existing {
        operation_id: DeliveryOperationId,
        version: DeliveryVersion,
        state: MergeOperationState,
        reason: PreflightStaleReason,
    },
    Conflict,
}

pub(super) enum ReadyPreflightTransition {
    Superseded,
    Stale(PreflightStaleReason),
}

impl ReadyPreflightTransition {
    const fn state(&self) -> MergeOperationState {
        match self {
            Self::Superseded => MergeOperationState::Superseded,
            Self::Stale(_) => MergeOperationState::Stale,
        }
    }

    const fn failure_code(&self) -> Option<&'static str> {
        match self {
            Self::Superseded => None,
            Self::Stale(reason) => Some(reason.as_failure_code()),
        }
    }
}

impl Store {
    pub async fn mark_merge_preflight_stale(
        &self,
        request: MarkPreflightStaleRequest,
    ) -> Result<MarkPreflightStaleOutcome, StoreError> {
        let target_version = request.expected_version.next()?;
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let operation_lookup =
            load_operation_for_caller(&mut transaction, request.operation_id, request.task_id)
                .await?;
        let task_exists: i64 =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE id = ?)")
                .bind(request.task_id.to_string())
                .fetch_one(&mut *transaction)
                .await?;
        if task_exists != 1 {
            return Err(StoreError::TaskNotFound);
        }
        let current = match operation_lookup {
            OperationLookup::Exact(operation) => operation,
            OperationLookup::WrongTask | OperationLookup::Missing => {
                transaction.commit().await?;
                return Ok(MarkPreflightStaleOutcome::Conflict);
            }
        };
        validate_stale_reason_binding(&current, request.task_id, request.reason)?;
        if current.state == MergeOperationState::Stale && current.version == target_version {
            if current.failure_code.as_ref().map(|code| code.as_str())
                != Some(request.reason.as_failure_code())
            {
                transaction.commit().await?;
                return Ok(MarkPreflightStaleOutcome::Conflict);
            }
            match lookup_transition(
                &mut transaction,
                request.operation_id,
                target_version,
                MergeOperationState::PreflightReady,
                MergeOperationState::Stale,
                Some(request.reason.as_failure_code()),
            )
            .await?
            {
                TransitionLookup::Exact(receipt)
                    if receipt.transitioned_at == current.updated_at =>
                {
                    transaction.commit().await?;
                    return Ok(MarkPreflightStaleOutcome::Existing {
                        operation_id: request.operation_id,
                        version: target_version,
                        state: MergeOperationState::Stale,
                        reason: request.reason,
                    });
                }
                TransitionLookup::Exact(_)
                | TransitionLookup::Missing
                | TransitionLookup::Conflict => return Err(transition_invariant()),
            }
        }
        if current.state != MergeOperationState::PreflightReady
            || current.version != request.expected_version
        {
            transaction.commit().await?;
            return Ok(MarkPreflightStaleOutcome::Conflict);
        }
        let timestamp: DeliveryTimestamp = current_timestamp()?.to_string().parse()?;
        let transition = ReadyPreflightTransition::Stale(request.reason);
        let Some(version) = transition_ready_preflight(
            &mut transaction,
            request.operation_id,
            current.provenance.identity,
            request.expected_version,
            &transition,
            timestamp,
        )
        .await?
        else {
            return Err(transition_invariant());
        };
        let updated = load_merge_operation_exact(&mut transaction, request.operation_id).await?;
        let expected_failure = request.reason.as_failure_code();
        if updated.state != MergeOperationState::Stale
            || updated.version != version
            || updated.failure_code.as_ref().map(|code| code.as_str()) != Some(expected_failure)
        {
            return Err(transition_invariant());
        }
        transaction.commit().await?;
        Ok(MarkPreflightStaleOutcome::Applied {
            operation_id: request.operation_id,
            version,
            state: MergeOperationState::Stale,
            reason: request.reason,
        })
    }
}

pub(super) async fn transition_ready_preflight(
    connection: &mut sqlx::SqliteConnection,
    operation_id: DeliveryOperationId,
    identity: DeliveryIdentity,
    expected_version: DeliveryVersion,
    transition: &ReadyPreflightTransition,
    timestamp: DeliveryTimestamp,
) -> Result<Option<DeliveryVersion>, StoreError> {
    let next_version = expected_version.next()?;
    let state = transition.state();
    let failure_code = transition.failure_code();
    let updated = sqlx::query(
        "UPDATE task_merge_operations \
         SET state = ?, failure_code = ?, version = ?, updated_at = ? \
         WHERE operation_id = ? AND task_id = ? AND repository_id = ? AND attempt = ? \
           AND state = 'preflight_ready' AND version = ?",
    )
    .bind(state.as_str())
    .bind(failure_code)
    .bind(i64::try_from(next_version.get()).map_err(|_| transition_invariant())?)
    .bind(timestamp.to_string())
    .bind(operation_id.to_string())
    .bind(identity.task_id().to_string())
    .bind(identity.repository_id().to_string())
    .bind(i64::from(identity.attempt()))
    .bind(i64::try_from(expected_version.get()).map_err(|_| transition_invariant())?)
    .execute(&mut *connection)
    .await?;
    if updated.rows_affected() == 0 {
        return Ok(None);
    }
    if updated.rows_affected() != 1 {
        return Err(transition_invariant());
    }

    let exact: i64 = sqlx::query_scalar(
        "SELECT EXISTS( \
             SELECT 1 FROM task_merge_operations m \
             JOIN task_delivery_operation_transitions t \
               ON t.entity_kind = 'merge_operation' AND t.entity_id = m.operation_id \
              AND t.entity_version = m.version AND t.to_state = m.state \
             WHERE m.operation_id = ? AND m.task_id = ? AND m.repository_id = ? \
               AND m.attempt = ? AND m.state = ? AND m.failure_code IS ? \
               AND m.version = ? AND m.updated_at = ? \
               AND t.from_state = 'preflight_ready' AND t.failure_code IS ? \
               AND t.transitioned_at = ? \
         )",
    )
    .bind(operation_id.to_string())
    .bind(identity.task_id().to_string())
    .bind(identity.repository_id().to_string())
    .bind(i64::from(identity.attempt()))
    .bind(state.as_str())
    .bind(failure_code)
    .bind(i64::try_from(next_version.get()).map_err(|_| transition_invariant())?)
    .bind(timestamp.to_string())
    .bind(failure_code)
    .bind(timestamp.to_string())
    .fetch_one(&mut *connection)
    .await?;
    if exact != 1 {
        return Err(transition_invariant());
    }
    Ok(Some(next_version))
}

fn transition_invariant() -> StoreError {
    StoreError::InvariantViolation(TRANSITION_INVARIANT)
}

fn validate_stale_reason_binding(
    operation: &super::MergeOperationRecord,
    task_id: TaskId,
    reason: PreflightStaleReason,
) -> Result<(), StoreError> {
    if operation.provenance.identity.task_id() != task_id {
        return Err(transition_invariant());
    }
    let bound = match reason {
        PreflightStaleReason::EvidenceStale => {
            operation.provenance.evidence.identity() == operation.provenance.identity
        }
        PreflightStaleReason::TargetBranchChanged => {
            operation.target_branch != operation.provenance.source_branch
        }
        PreflightStaleReason::TargetHeadChanged => {
            operation.target_branch != operation.provenance.source_branch
                && operation.expected_target_head.algorithm()
                    == operation.provenance.base_commit.algorithm()
        }
        PreflightStaleReason::SourceChanged => {
            operation.preflight_inputs.as_ref().is_some_and(|inputs| {
                inputs.preflight_source_commit.algorithm()
                    == operation.provenance.base_commit.algorithm()
                    && inputs.preflight_source_commit != operation.provenance.base_commit
                    && inputs.candidate_tree.algorithm()
                        == operation.provenance.base_commit.algorithm()
            })
        }
    };
    if bound {
        Ok(())
    } else {
        Err(transition_invariant())
    }
}
