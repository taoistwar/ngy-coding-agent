use crate::StoreError;
use crate::delivery::ownership::{
    load_merge_operation_exact, validate_source_merge_reconciliation_pair,
};
use crate::delivery::{DeliverySourceState, DeliveryVersion};

use super::super::load::{
    MergeTransitionReceipt, TransitionLookup, load_source_context,
    lookup_merge_reconciliation_transition, lookup_source_transition,
};
use super::super::model::{ReconcileDeliverySourceReceipt, ReconcileDeliverySourceRequest};
use super::super::source_invariant;
use super::super::validate::validate_mutation_owner;

pub(super) async fn verify_reconciliation_pair(
    connection: &mut sqlx::SqliteConnection,
    request: &ReconcileDeliverySourceRequest,
    source_version: DeliveryVersion,
    merge_version: DeliveryVersion,
    failure_code: &str,
) -> Result<ReconcileDeliverySourceReceipt, StoreError> {
    let source = load_source_context(&mut *connection, request.anchor.task_id)
        .await?
        .ok_or_else(source_invariant)?;
    let operation =
        load_merge_operation_exact(&mut *connection, request.anchor.accepted_operation_id).await?;
    validate_mutation_owner(&source, &operation, request.anchor)?;
    validate_source_merge_reconciliation_pair(&mut *connection, &source, &operation).await?;
    let source_receipt = match lookup_source_transition(
        &mut *connection,
        request.anchor.task_id,
        source_version,
        request.expected_state,
        DeliverySourceState::ReconciliationRequired,
        Some(failure_code),
    )
    .await?
    {
        TransitionLookup::Exact(receipt) => receipt,
        TransitionLookup::Missing | TransitionLookup::Conflict => return Err(source_invariant()),
    };
    let merge_receipt = match lookup_merge_reconciliation_transition(
        connection,
        request.anchor.accepted_operation_id,
        merge_version,
        failure_code,
    )
    .await?
    {
        TransitionLookup::Exact(receipt) => receipt,
        TransitionLookup::Missing | TransitionLookup::Conflict => return Err(source_invariant()),
    };
    paired_receipt(source_receipt, merge_receipt)
}

pub(super) fn paired_receipt(
    source: super::super::model::DeliverySourceTransitionReceipt,
    merge: MergeTransitionReceipt,
) -> Result<ReconcileDeliverySourceReceipt, StoreError> {
    if source.failure_code.as_ref() != Some(&merge.failure_code)
        || source.transitioned_at != merge.transitioned_at
    {
        return Err(source_invariant());
    }
    Ok(ReconcileDeliverySourceReceipt {
        source,
        merge_operation_id: merge.operation_id,
        merge_version: merge.version,
        failure_code: merge.failure_code,
        transitioned_at: merge.transitioned_at,
    })
}
