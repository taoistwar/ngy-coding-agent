use std::fmt;

use coding_agent_domain::TaskId;

use crate::delivery::mutation::{
    DeliveryMutationEntity, DeliveryMutationEntityKind, DeliveryMutationKey, DeliveryMutationKind,
    impl_delivery_mutation_request,
};
use crate::delivery::{
    DeliveryError, DeliveryOperationId, DeliveryTimestamp, DeliveryVersion, MergeOperationRecord,
    MergeOperationState, PreflightStaleReason,
};
use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::model::{MergeReconciliationReason, MergeTransitionOutcome, PreflightRejectedReason};
use super::replay::{
    OperationLookup, TransitionLookup, load_operation_for_caller, lookup_transition, version_i64,
};

/// A terminal outcome discovered before repository object inputs can be prepared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnboundMergePreflightFailure {
    Rejected(PreflightRejectedReason),
    Stale(PreflightStaleReason),
    ReconciliationRequired(MergeReconciliationReason),
}

impl UnboundMergePreflightFailure {
    const fn state(self) -> MergeOperationState {
        match self {
            Self::Rejected(_) => MergeOperationState::Rejected,
            Self::Stale(_) => MergeOperationState::Stale,
            Self::ReconciliationRequired(_) => MergeOperationState::ReconciliationRequired,
        }
    }

    const fn failure_code(self) -> &'static str {
        match self {
            Self::Rejected(reason) => reason.as_failure_code(),
            Self::Stale(reason) => reason.as_failure_code(),
            Self::ReconciliationRequired(reason) => reason.as_failure_code(),
        }
    }
}

/// Outcome of terminalizing an unbound durable preflight intent.
pub type FailUnboundMergePreflightOutcome = MergeTransitionOutcome;

#[derive(Clone, PartialEq, Eq)]
pub struct FailUnboundMergePreflightRequest {
    pub(in crate::delivery::merges) task_id: TaskId,
    pub(in crate::delivery::merges) operation_id: DeliveryOperationId,
    pub(in crate::delivery::merges) expected_version: DeliveryVersion,
    pub(in crate::delivery::merges) failure: UnboundMergePreflightFailure,
}

impl fmt::Debug for FailUnboundMergePreflightRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FailUnboundMergePreflightRequest")
            .field("task_id", &self.task_id)
            .field("operation_id", &self.operation_id)
            .field("expected_version", &self.expected_version)
            .field("failure", &self.failure)
            .finish()
    }
}

impl FailUnboundMergePreflightRequest {
    pub fn try_new(
        task_id: TaskId,
        operation_id: DeliveryOperationId,
        expected_version: DeliveryVersion,
        failure: UnboundMergePreflightFailure,
    ) -> Result<Self, DeliveryError> {
        if task_id.as_uuid().is_nil()
            || operation_id.as_uuid().is_nil()
            || expected_version != DeliveryVersion::initial()
        {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        expected_version.next()?;
        Ok(Self {
            task_id,
            operation_id,
            expected_version,
            failure,
        })
    }

    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub const fn operation_id(&self) -> DeliveryOperationId {
        self.operation_id
    }

    pub const fn expected_version(&self) -> DeliveryVersion {
        self.expected_version
    }

    pub const fn failure(&self) -> UnboundMergePreflightFailure {
        self.failure
    }
}

impl_delivery_mutation_request!(FailUnboundMergePreflightRequest, |request| {
    DeliveryMutationKey::new(
        DeliveryMutationKind::FailUnboundMergePreflight,
        request.task_id,
        vec![DeliveryMutationEntity::operation(
            DeliveryMutationEntityKind::MergeOperation,
            request.operation_id,
            request.expected_version,
        )],
        None,
    )
});

impl Store {
    pub async fn fail_unbound_merge_preflight(
        &self,
        request: FailUnboundMergePreflightRequest,
    ) -> Result<FailUnboundMergePreflightOutcome, StoreError> {
        let target_version = request.expected_version.next()?;
        let state = request.failure.state();
        let failure_code = request.failure.failure_code();
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let operation = match load_operation_for_caller(
            &mut transaction,
            request.operation_id,
            request.task_id,
        )
        .await?
        {
            OperationLookup::Exact(operation) => operation,
            OperationLookup::WrongTask | OperationLookup::Missing => {
                transaction.commit().await?;
                return Ok(MergeTransitionOutcome::Conflict);
            }
        };

        match lookup_transition(
            &mut transaction,
            request.operation_id,
            target_version,
            MergeOperationState::PreflightPending,
            state,
            Some(failure_code),
        )
        .await?
        {
            TransitionLookup::Exact(receipt) => {
                validate_persisted_failure(&operation, state, failure_code, target_version)?;
                transaction.commit().await?;
                return Ok(MergeTransitionOutcome::Existing(receipt));
            }
            TransitionLookup::Conflict => {
                transaction.commit().await?;
                return Ok(MergeTransitionOutcome::Conflict);
            }
            TransitionLookup::Missing => {}
        }

        if operation.state != MergeOperationState::PreflightPending
            || operation.version != request.expected_version
            || operation.failure_code.is_some()
            || operation.preflight_inputs.is_some()
        {
            transaction.commit().await?;
            return Ok(MergeTransitionOutcome::Conflict);
        }

        let timestamp: DeliveryTimestamp = current_timestamp()?.to_string().parse()?;
        let updated = sqlx::query(
            "UPDATE task_merge_operations \
             SET state = ?, failure_code = ?, version = ?, updated_at = ? \
             WHERE operation_id = ? AND task_id = ? \
               AND state = 'preflight_pending' AND failure_code IS NULL AND version = 1 \
               AND candidate_tree_oid IS NULL AND preflight_source_commit_oid IS NULL \
               AND merge_base_oid IS NULL AND candidate_merge_tree_oid IS NULL",
        )
        .bind(state.as_str())
        .bind(failure_code)
        .bind(version_i64(target_version)?)
        .bind(timestamp.to_string())
        .bind(request.operation_id.to_string())
        .bind(request.task_id.to_string())
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(super::merge_invariant());
        }

        let receipt = match lookup_transition(
            &mut transaction,
            request.operation_id,
            target_version,
            MergeOperationState::PreflightPending,
            state,
            Some(failure_code),
        )
        .await?
        {
            TransitionLookup::Exact(receipt) => receipt,
            TransitionLookup::Missing | TransitionLookup::Conflict => {
                return Err(super::merge_invariant());
            }
        };
        let persisted = match load_operation_for_caller(
            &mut transaction,
            request.operation_id,
            request.task_id,
        )
        .await?
        {
            OperationLookup::Exact(operation) => operation,
            OperationLookup::WrongTask | OperationLookup::Missing => {
                return Err(super::merge_invariant());
            }
        };
        validate_persisted_failure(&persisted, state, failure_code, target_version)?;
        transaction.commit().await?;
        Ok(MergeTransitionOutcome::Applied(receipt))
    }
}

fn validate_persisted_failure(
    operation: &MergeOperationRecord,
    state: MergeOperationState,
    failure_code: &str,
    version: DeliveryVersion,
) -> Result<(), StoreError> {
    let exact = operation.state == state
        && operation.failure_code.as_ref().map(|code| code.as_str()) == Some(failure_code)
        && operation.version == version
        && operation.preflight_inputs.is_none()
        && operation.merge_base.is_none()
        && operation.candidate_merge_tree.is_none()
        && operation.conflict_path_count.is_none()
        && operation.conflicts.is_empty();
    if exact {
        Ok(())
    } else {
        Err(super::merge_invariant())
    }
}
