use std::fmt;

use coding_agent_domain::TaskId;

use crate::delivery::mutation::{
    DeliveryMutationEntity, DeliveryMutationEntityKind, DeliveryMutationKey, DeliveryMutationKind,
    impl_delivery_mutation_request,
};
use crate::delivery::{
    DeliveryError, DeliveryOperationId, DeliveryTimestamp, DeliveryVersion, GitCommitOid,
    GitTreeOid, MergeOperationState, PreparedMergePreflightInputs,
};
use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::model::MergeTransitionOutcome;
use super::replay::{
    OperationLookup, TransitionLookup, load_operation_for_caller, lookup_transition, version_i64,
};

/// Outcome of sealing the repository object inputs for a durable preflight intent.
pub type BindMergePreflightInputsOutcome = MergeTransitionOutcome;

#[derive(Clone, PartialEq, Eq)]
pub struct BindMergePreflightInputsRequest {
    pub(in crate::delivery::merges) task_id: TaskId,
    pub(in crate::delivery::merges) operation_id: DeliveryOperationId,
    pub(in crate::delivery::merges) expected_version: DeliveryVersion,
    pub(in crate::delivery::merges) inputs: PreparedMergePreflightInputs,
}

impl fmt::Debug for BindMergePreflightInputsRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BindMergePreflightInputsRequest")
            .field("task_id", &self.task_id)
            .field("operation_id", &self.operation_id)
            .field("expected_version", &self.expected_version)
            .field("inputs", &self.inputs)
            .finish()
    }
}

impl BindMergePreflightInputsRequest {
    pub fn try_new(
        task_id: TaskId,
        operation_id: DeliveryOperationId,
        expected_version: DeliveryVersion,
        candidate_tree: GitTreeOid,
        preflight_source_commit: GitCommitOid,
    ) -> Result<Self, DeliveryError> {
        if task_id.as_uuid().is_nil()
            || operation_id.as_uuid().is_nil()
            || expected_version != DeliveryVersion::initial()
            || candidate_tree.algorithm() != preflight_source_commit.algorithm()
        {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        expected_version.next()?;
        Ok(Self {
            task_id,
            operation_id,
            expected_version,
            inputs: PreparedMergePreflightInputs {
                candidate_tree,
                preflight_source_commit,
            },
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

    pub const fn inputs(&self) -> &PreparedMergePreflightInputs {
        &self.inputs
    }
}

impl_delivery_mutation_request!(BindMergePreflightInputsRequest, |request| {
    DeliveryMutationKey::new(
        DeliveryMutationKind::BindMergePreflightInputs,
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
    pub async fn bind_merge_preflight_inputs(
        &self,
        request: BindMergePreflightInputsRequest,
    ) -> Result<BindMergePreflightInputsOutcome, StoreError> {
        let target_version = request.expected_version.next()?;
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
        validate_input_binding(&mut transaction, &operation, &request.inputs).await?;

        match lookup_transition(
            &mut transaction,
            request.operation_id,
            target_version,
            MergeOperationState::PreflightPending,
            MergeOperationState::PreflightPending,
            None,
        )
        .await?
        {
            TransitionLookup::Exact(receipt) => {
                if operation.preflight_inputs.as_ref() != Some(&request.inputs) {
                    transaction.commit().await?;
                    return Ok(MergeTransitionOutcome::Conflict);
                }
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
             SET candidate_tree_oid = ?, preflight_source_commit_oid = ?, \
                 version = ?, updated_at = ? \
             WHERE operation_id = ? AND task_id = ? \
               AND state = 'preflight_pending' AND failure_code IS NULL AND version = ? \
               AND candidate_tree_oid IS NULL AND preflight_source_commit_oid IS NULL",
        )
        .bind(request.inputs.candidate_tree.as_str())
        .bind(request.inputs.preflight_source_commit.as_str())
        .bind(version_i64(target_version)?)
        .bind(timestamp.to_string())
        .bind(request.operation_id.to_string())
        .bind(request.task_id.to_string())
        .bind(version_i64(request.expected_version)?)
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
            MergeOperationState::PreflightPending,
            None,
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
        validate_persisted_inputs(&persisted, &request.inputs)?;
        transaction.commit().await?;
        Ok(MergeTransitionOutcome::Applied(receipt))
    }
}

async fn validate_input_binding(
    connection: &mut sqlx::SqliteConnection,
    operation: &crate::delivery::MergeOperationRecord,
    inputs: &PreparedMergePreflightInputs,
) -> Result<(), StoreError> {
    let expected = operation.provenance.base_commit.algorithm();
    if inputs.candidate_tree.algorithm() != expected
        || inputs.preflight_source_commit.algorithm() != expected
        || inputs.preflight_source_commit == operation.provenance.base_commit
    {
        return Err(StoreError::Delivery(DeliveryError::InvalidCommandRequest));
    }
    let snapshot = crate::delivery::eligibility::load_snapshot(
        connection,
        operation.provenance.identity.task_id(),
    )
    .await?
    .ok_or_else(super::merge_invariant)?;
    if let Some(source) = snapshot.ownership.source.as_ref()
        && (source.provenance != operation.provenance
            || source.candidate_tree != inputs.candidate_tree
            || source.expected_source_commit.as_ref() != Some(&inputs.preflight_source_commit))
    {
        return Err(StoreError::Delivery(DeliveryError::InvalidCommandRequest));
    }
    Ok(())
}

fn validate_persisted_inputs(
    operation: &crate::delivery::MergeOperationRecord,
    inputs: &PreparedMergePreflightInputs,
) -> Result<(), StoreError> {
    if operation.preflight_inputs.as_ref() == Some(inputs) {
        Ok(())
    } else {
        Err(super::merge_invariant())
    }
}
