use crate::StoreError;
use crate::delivery::{DeliverySourceState, DeliveryVersion, MergeOperationRecord};

use super::super::load::{
    AnchorLookup, TransitionLookup, load_accepted_anchor, load_source_context,
    lookup_source_transition,
};
use super::super::model::{DeliverySourceAnchor, DeliverySourceTransitionReceipt};
use super::super::source_invariant;
use super::super::validate::{
    validate_anchor_compatibility, validate_current_source_reconciliation, validate_mutation_owner,
};

pub(super) async fn verify_source_transition(
    connection: &mut sqlx::SqliteConnection,
    anchor: DeliverySourceAnchor,
    version: DeliveryVersion,
    from: DeliverySourceState,
    to: DeliverySourceState,
    failure_code: Option<&str>,
) -> Result<DeliverySourceTransitionReceipt, StoreError> {
    let source = load_source_context(connection, anchor.task_id)
        .await?
        .ok_or_else(source_invariant)?;
    let operation = match load_accepted_anchor(connection, anchor).await? {
        AnchorLookup::Exact(operation) => operation,
        AnchorLookup::Conflict => return Err(source_invariant()),
    };
    validate_mutation_owner(&source, &operation, anchor)?;
    match lookup_source_transition(connection, anchor.task_id, version, from, to, failure_code)
        .await?
    {
        TransitionLookup::Exact(receipt) => Ok(receipt),
        TransitionLookup::Missing | TransitionLookup::Conflict => Err(source_invariant()),
    }
}

pub(super) fn version_i64(version: DeliveryVersion) -> Result<i64, StoreError> {
    i64::try_from(version.get()).map_err(|_| source_invariant())
}

pub(super) async fn exact_accepted(
    connection: &mut sqlx::SqliteConnection,
    anchor: DeliverySourceAnchor,
) -> Result<Option<MergeOperationRecord>, StoreError> {
    Ok(match load_accepted_anchor(connection, anchor).await? {
        AnchorLookup::Exact(operation) => Some(*operation),
        AnchorLookup::Conflict => None,
    })
}

pub(super) async fn audit_conflicting_source_transition(
    connection: &mut sqlx::SqliteConnection,
    anchor: DeliverySourceAnchor,
) -> Result<(), StoreError> {
    let source = load_source_context(&mut *connection, anchor.task_id)
        .await?
        .ok_or_else(source_invariant)?;
    validate_current_source_reconciliation(&mut *connection, &source).await?;
    if let AnchorLookup::Exact(operation) = load_accepted_anchor(connection, anchor).await? {
        validate_anchor_compatibility(&source, &operation, anchor)?;
    }
    Ok(())
}
