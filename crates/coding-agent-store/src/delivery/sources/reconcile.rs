mod apply;
mod verify;

use crate::delivery::{
    DeliverySourceRecord, DeliverySourceState, MergeOperationRecord, MergeOperationState,
};
use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::load::{
    AnchorLookup, MergeTransitionReceipt, TransitionLookup, load_accepted_anchor,
    load_source_context, lookup_merge_reconciliation_transition, lookup_source_transition,
};
use super::model::{
    DeliverySourceTransitionReceipt, ReconcileDeliverySourceOutcome, ReconcileDeliverySourceRequest,
};
use super::source_invariant;
use super::validate::{
    validate_anchor_compatibility, validate_current_source_reconciliation, validate_mutation_owner,
};
use apply::{ReconciliationPair, apply_reconciliation_pair};
use verify::{paired_receipt, verify_reconciliation_pair};

struct ReconciliationTargets {
    source: TransitionLookup<DeliverySourceTransitionReceipt>,
    merge: TransitionLookup<MergeTransitionReceipt>,
}

enum CurrentClassification {
    Proceed(ReconciliationTargets),
    Finish(ReconcileDeliverySourceOutcome),
}

impl Store {
    pub async fn reconcile_delivery_source(
        &self,
        request: ReconcileDeliverySourceRequest,
    ) -> Result<ReconcileDeliverySourceOutcome, StoreError> {
        let source_version = request.expected_source_version.next()?;
        let merge_version = request.expected_current_merge_version.next()?;
        let failure_code = request.reason.as_failure_code();
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        let targets = lookup_reconciliation_targets(
            &mut transaction,
            &request,
            source_version,
            merge_version,
            failure_code,
        )
        .await?;
        let Some((source, operation)) =
            load_reconciliation_context(&mut transaction, &request).await?
        else {
            transaction.commit().await?;
            return Ok(ReconcileDeliverySourceOutcome::Conflict);
        };
        validate_anchor_compatibility(&source, &operation, request.anchor)?;
        validate_current_source_reconciliation(&mut transaction, &source).await?;

        let targets = match classify_current(&request, &source, &operation, targets)? {
            CurrentClassification::Proceed(targets) => targets,
            CurrentClassification::Finish(outcome) => {
                transaction.commit().await?;
                return Ok(outcome);
            }
        };
        if !fresh_targets_are_compatible(targets)?
            || !request_matches_current(&request, &source, &operation)
        {
            transaction.commit().await?;
            return Ok(ReconcileDeliverySourceOutcome::Conflict);
        }
        validate_mutation_owner(&source, &operation, request.anchor)?;

        let timestamp = current_timestamp()?.to_string().parse()?;
        apply_reconciliation_pair(
            &mut transaction,
            ReconciliationPair::new(
                &request,
                &source,
                &operation,
                source_version,
                merge_version,
                failure_code,
                timestamp,
            ),
        )
        .await?;
        let receipt = verify_reconciliation_pair(
            &mut transaction,
            &request,
            source_version,
            merge_version,
            failure_code,
        )
        .await?;
        transaction.commit().await?;
        Ok(ReconcileDeliverySourceOutcome::Applied(receipt))
    }
}

async fn lookup_reconciliation_targets(
    connection: &mut sqlx::SqliteConnection,
    request: &ReconcileDeliverySourceRequest,
    source_version: crate::delivery::DeliveryVersion,
    merge_version: crate::delivery::DeliveryVersion,
    failure_code: &str,
) -> Result<ReconciliationTargets, StoreError> {
    let source = lookup_source_transition(
        &mut *connection,
        request.anchor.task_id,
        source_version,
        request.expected_state,
        DeliverySourceState::ReconciliationRequired,
        Some(failure_code),
    )
    .await?;
    let merge = lookup_merge_reconciliation_transition(
        connection,
        request.anchor.accepted_operation_id,
        merge_version,
        failure_code,
    )
    .await?;
    Ok(ReconciliationTargets { source, merge })
}

async fn load_reconciliation_context(
    connection: &mut sqlx::SqliteConnection,
    request: &ReconcileDeliverySourceRequest,
) -> Result<Option<(DeliverySourceRecord, MergeOperationRecord)>, StoreError> {
    let Some(source) = load_source_context(&mut *connection, request.anchor.task_id).await? else {
        return Ok(None);
    };
    let operation = match load_accepted_anchor(connection, request.anchor).await? {
        AnchorLookup::Exact(operation) => *operation,
        AnchorLookup::Conflict => return Ok(None),
    };
    Ok(Some((source, operation)))
}

fn classify_current(
    request: &ReconcileDeliverySourceRequest,
    source: &DeliverySourceRecord,
    operation: &MergeOperationRecord,
    targets: ReconciliationTargets,
) -> Result<CurrentClassification, StoreError> {
    if source.state == DeliverySourceState::ReconciliationRequired {
        if operation.state != MergeOperationState::ReconciliationRequired {
            return Ok(CurrentClassification::Finish(
                ReconcileDeliverySourceOutcome::Conflict,
            ));
        }
        validate_mutation_owner(source, operation, request.anchor)?;
        let outcome = match (targets.source, targets.merge) {
            (TransitionLookup::Exact(source), TransitionLookup::Exact(merge)) => {
                ReconcileDeliverySourceOutcome::Existing(paired_receipt(source, merge)?)
            }
            _ => ReconcileDeliverySourceOutcome::Conflict,
        };
        return Ok(CurrentClassification::Finish(outcome));
    }
    if operation.state == MergeOperationState::ReconciliationRequired {
        if source.state != DeliverySourceState::Committed {
            return Err(source_invariant());
        }
        return Ok(CurrentClassification::Finish(
            ReconcileDeliverySourceOutcome::Conflict,
        ));
    }
    Ok(CurrentClassification::Proceed(targets))
}

fn fresh_targets_are_compatible(targets: ReconciliationTargets) -> Result<bool, StoreError> {
    match (targets.source, targets.merge) {
        (TransitionLookup::Exact(_), _) | (_, TransitionLookup::Exact(_)) => {
            Err(source_invariant())
        }
        (TransitionLookup::Conflict, _) | (_, TransitionLookup::Conflict) => Ok(false),
        (TransitionLookup::Missing, TransitionLookup::Missing) => Ok(true),
    }
}

fn request_matches_current(
    request: &ReconcileDeliverySourceRequest,
    source: &DeliverySourceRecord,
    operation: &MergeOperationRecord,
) -> bool {
    source.state == request.expected_state
        && source.version == request.expected_source_version
        && operation.state == MergeOperationState::Accepted
        && operation.version == request.expected_current_merge_version
        && operation.version == request.anchor.accepted_receipt_version
}
