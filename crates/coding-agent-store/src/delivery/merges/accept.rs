mod apply;
mod replay;

use crate::delivery::AcceptMergeCommandRequest;
use crate::{Store, StoreError};

use super::model::AcceptMergeOutcome;
use apply::apply_fresh_accept;
use replay::{load_ready_operation, try_existing_accept};

impl Store {
    pub async fn accept_merge(
        &self,
        request: AcceptMergeCommandRequest,
    ) -> Result<AcceptMergeOutcome, StoreError> {
        let mut first = self.pool.begin().await?;
        if let Some(outcome) = try_existing_accept(&mut first, &request).await? {
            first.commit().await?;
            return Ok(outcome);
        }
        first.commit().await?;

        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(outcome) = try_existing_accept(&mut transaction, &request).await? {
            transaction.commit().await?;
            return Ok(outcome);
        }
        let Some(operation) = load_ready_operation(&mut transaction, &request).await? else {
            transaction.commit().await?;
            return Ok(AcceptMergeOutcome::Conflict);
        };
        let receipt = apply_fresh_accept(&mut transaction, &request, &operation).await?;
        transaction.commit().await?;
        Ok(AcceptMergeOutcome::Accepted(receipt))
    }
}
