use crate::StoreError;
use crate::delivery::ownership::load_source_exact;
use crate::delivery::{
    DeliveryOperationId, DeliverySourceRecord, DeliverySourceState, DeliveryVersion,
    MergeOperationRecord, MergeOperationState,
};

use super::super::merge_invariant;
use super::super::model::{MergeReconciliationReason, MergeTransitionReceipt};
use super::super::replay::{TransitionLookup, lookup_transition};

pub(super) async fn audit_failed_source_origin(
    connection: &mut sqlx::SqliteConnection,
    operation: &MergeOperationRecord,
    from_state: MergeOperationState,
) -> Result<(), StoreError> {
    let source = load_source_exact(connection, operation.provenance.identity.task_id())
        .await?
        .ok_or_else(merge_invariant)?;
    let expected_oid_shape = match from_state {
        MergeOperationState::Accepted => operation.expected_merge_commit.is_none(),
        MergeOperationState::MergePending => operation.expected_merge_commit.is_some(),
        _ => false,
    };
    if operation.state == MergeOperationState::Failed
        && source_identity_matches(&source, operation)
        && source_link_matches(&source, operation)
        && expected_oid_shape
    {
        Ok(())
    } else {
        Err(merge_invariant())
    }
}

pub(super) async fn merge_only_reconciliation_is_blocked(
    connection: &mut sqlx::SqliteConnection,
    operation: &MergeOperationRecord,
    from_state: MergeOperationState,
    reason: MergeReconciliationReason,
) -> Result<bool, StoreError> {
    let source = load_source_exact(connection, operation.provenance.identity.task_id()).await?;
    match source {
        Some(source)
            if from_state == MergeOperationState::Accepted
                && matches!(
                    source.state,
                    DeliverySourceState::ObjectPending | DeliverySourceState::CommitPending
                ) =>
        {
            Ok(true)
        }
        Some(source)
            if from_state == MergeOperationState::Accepted
                && source.state == DeliverySourceState::Committed
                && reason == MergeReconciliationReason::SourceInconsistent =>
        {
            Ok(true)
        }
        Some(source) if source.state == DeliverySourceState::ReconciliationRequired => {
            Err(merge_invariant())
        }
        Some(source)
            if matches!(
                from_state,
                MergeOperationState::MergePending | MergeOperationState::AbortPending
            ) && (!committed_source_matches(&source, operation)
                || !source_link_matches(&source, operation)) =>
        {
            Err(merge_invariant())
        }
        None if matches!(
            from_state,
            MergeOperationState::MergePending | MergeOperationState::AbortPending
        ) =>
        {
            Err(merge_invariant())
        }
        _ => Ok(false),
    }
}

pub(super) async fn audit_reconciliation_source_origin(
    _connection: &mut sqlx::SqliteConnection,
    operation: &MergeOperationRecord,
    _from_state: MergeOperationState,
) -> Result<(), StoreError> {
    if operation.state == MergeOperationState::ReconciliationRequired {
        Ok(())
    } else {
        Err(merge_invariant())
    }
}

pub(super) fn committed_source_matches(
    source: &DeliverySourceRecord,
    operation: &MergeOperationRecord,
) -> bool {
    source.state == DeliverySourceState::Committed
        && source.failure_code.is_none()
        && source_identity_matches(source, operation)
}

fn source_identity_matches(
    source: &DeliverySourceRecord,
    operation: &MergeOperationRecord,
) -> bool {
    source.provenance == operation.provenance
        && operation
            .preflight_inputs
            .as_ref()
            .is_some_and(|inputs| source.candidate_tree == inputs.candidate_tree)
        && source.expected_source_commit.is_some()
}

fn source_link_matches(source: &DeliverySourceRecord, operation: &MergeOperationRecord) -> bool {
    operation.delivery_source_task_id == Some(operation.provenance.identity.task_id())
        && operation.source_commit.as_ref() == source.expected_source_commit.as_ref()
}

pub(super) async fn require_transition(
    connection: &mut sqlx::SqliteConnection,
    operation_id: DeliveryOperationId,
    version: DeliveryVersion,
    from: MergeOperationState,
    to: MergeOperationState,
    failure: &str,
) -> Result<MergeTransitionReceipt, StoreError> {
    match lookup_transition(connection, operation_id, version, from, to, Some(failure)).await? {
        TransitionLookup::Exact(receipt) => Ok(receipt),
        TransitionLookup::Missing | TransitionLookup::Conflict => Err(merge_invariant()),
    }
}
