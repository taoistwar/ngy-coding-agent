use crate::StoreError;
use crate::delivery::ownership::validate_merged_disposition_origin;
use crate::delivery::{DeliveryVersion, MergeOperationRecord, MergeOperationState};

use super::super::model::{CompleteMergeRequest, MergeTransitionOutcome};
use super::super::replay::{TransitionLookup, lookup_transition};
use super::validate::{
    applied_proof_matches, require_committed_source, require_merged_disposition,
};

pub(super) async fn classify_merge_replay(
    connection: &mut sqlx::SqliteConnection,
    operation: &MergeOperationRecord,
    request: &CompleteMergeRequest,
    target_version: DeliveryVersion,
) -> Result<Option<MergeTransitionOutcome>, StoreError> {
    match lookup_transition(
        &mut *connection,
        request.operation_id,
        target_version,
        MergeOperationState::MergePending,
        MergeOperationState::Merged,
        None,
    )
    .await?
    {
        TransitionLookup::Exact(receipt) => {
            let source = require_committed_source(&mut *connection, operation).await?;
            let disposition = require_merged_disposition(&mut *connection, operation).await?;
            validate_merged_disposition_origin(&mut *connection, operation, &source, &disposition)
                .await?;
            if applied_proof_matches(operation, &source, request) {
                Ok(Some(MergeTransitionOutcome::Existing(receipt)))
            } else {
                Ok(Some(MergeTransitionOutcome::Conflict))
            }
        }
        TransitionLookup::Conflict => Ok(Some(MergeTransitionOutcome::Conflict)),
        TransitionLookup::Missing => Ok(None),
    }
}
