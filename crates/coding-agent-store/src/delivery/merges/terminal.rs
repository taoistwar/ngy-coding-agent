use crate::{Store, StoreError};

use super::model::{MergeTransitionOutcome, ReconcileMergeRequest, RecordMergeKnownFailureRequest};

mod failure;
mod reconcile;
mod validate;

impl Store {
    pub async fn record_merge_known_failure(
        &self,
        request: RecordMergeKnownFailureRequest,
    ) -> Result<MergeTransitionOutcome, StoreError> {
        failure::record(self, request).await
    }

    pub async fn reconcile_merge(
        &self,
        request: ReconcileMergeRequest,
    ) -> Result<MergeTransitionOutcome, StoreError> {
        reconcile::record(self, request).await
    }
}
