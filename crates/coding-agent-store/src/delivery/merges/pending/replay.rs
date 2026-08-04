use crate::StoreError;
use crate::delivery::{
    DeliverySourceRecord, DeliveryVersion, MergeOperationRecord, MergeOperationState,
};

use super::super::model::{EnterMergePendingRequest, MergeTransitionOutcome};
use super::super::replay::{TransitionLookup, lookup_transition};
use super::validate::pending_facts_match;

pub(super) async fn classify_pending_replay(
    connection: &mut sqlx::SqliteConnection,
    operation: &MergeOperationRecord,
    source: &DeliverySourceRecord,
    request: &EnterMergePendingRequest,
    target_version: DeliveryVersion,
) -> Result<Option<MergeTransitionOutcome>, StoreError> {
    match lookup_transition(
        connection,
        request.operation_id,
        target_version,
        MergeOperationState::Accepted,
        MergeOperationState::MergePending,
        None,
    )
    .await?
    {
        TransitionLookup::Exact(receipt)
            if pending_facts_match(operation, source, request, target_version) =>
        {
            Ok(Some(MergeTransitionOutcome::Existing(receipt)))
        }
        TransitionLookup::Exact(_) | TransitionLookup::Conflict => {
            Ok(Some(MergeTransitionOutcome::Conflict))
        }
        TransitionLookup::Missing => Ok(None),
    }
}
