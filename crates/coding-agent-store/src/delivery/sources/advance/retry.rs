use crate::delivery::DeliveryTimestamp;
use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::super::load::{TransitionLookup, load_source_context, lookup_source_transition};
use super::super::model::{DeliverySourceTransitionOutcome, RecordDeliverySourceRetryRequest};
use super::super::source_invariant;
use super::super::validate::{
    validate_anchor_compatibility, validate_current_source_reconciliation, validate_pending_source,
    validate_replay_anchor,
};
use super::verify::{
    audit_conflicting_source_transition, exact_accepted, verify_source_transition, version_i64,
};

impl Store {
    pub async fn record_delivery_source_retry(
        &self,
        request: RecordDeliverySourceRetryRequest,
    ) -> Result<DeliverySourceTransitionOutcome, StoreError> {
        let target_version = request.expected_source_version.next()?;
        let failure_code = request.reason.as_failure_code();
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        match lookup_source_transition(
            &mut transaction,
            request.anchor.task_id,
            target_version,
            request.expected_state,
            request.expected_state,
            Some(failure_code),
        )
        .await?
        {
            TransitionLookup::Exact(receipt) => {
                let source = load_source_context(&mut transaction, request.anchor.task_id)
                    .await?
                    .ok_or_else(source_invariant)?;
                let Some(operation) = exact_accepted(&mut transaction, request.anchor).await?
                else {
                    transaction.commit().await?;
                    return Ok(DeliverySourceTransitionOutcome::Conflict);
                };
                validate_replay_anchor(&source, &operation, request.anchor)?;
                validate_current_source_reconciliation(&mut transaction, &source).await?;
                transaction.commit().await?;
                return Ok(DeliverySourceTransitionOutcome::Existing(receipt));
            }
            TransitionLookup::Conflict => {
                audit_conflicting_source_transition(&mut transaction, request.anchor).await?;
                transaction.commit().await?;
                return Ok(DeliverySourceTransitionOutcome::Conflict);
            }
            TransitionLookup::Missing => {}
        }

        let Some(source) = load_source_context(&mut transaction, request.anchor.task_id).await?
        else {
            transaction.commit().await?;
            return Ok(DeliverySourceTransitionOutcome::Conflict);
        };
        let Some(operation) = exact_accepted(&mut transaction, request.anchor).await? else {
            transaction.commit().await?;
            return Ok(DeliverySourceTransitionOutcome::Conflict);
        };
        validate_anchor_compatibility(&source, &operation, request.anchor)?;
        validate_current_source_reconciliation(&mut transaction, &source).await?;
        if source.state != request.expected_state
            || source.version != request.expected_source_version
        {
            transaction.commit().await?;
            return Ok(DeliverySourceTransitionOutcome::Conflict);
        }
        validate_pending_source(
            &source,
            &operation,
            request.anchor,
            request.expected_state,
            request.expected_source_version,
        )?;

        let timestamp: DeliveryTimestamp = current_timestamp()?.to_string().parse()?;
        let updated = sqlx::query(
            "UPDATE task_delivery_sources \
             SET failure_code = ?, version = ?, updated_at = ? \
             WHERE task_id = ? AND repository_id = ? AND attempt = ? \
               AND state = ? AND version = ?",
        )
        .bind(failure_code)
        .bind(version_i64(target_version)?)
        .bind(timestamp.to_string())
        .bind(request.anchor.task_id.to_string())
        .bind(source.provenance.identity.repository_id().to_string())
        .bind(i64::from(source.provenance.identity.attempt()))
        .bind(request.expected_state.as_str())
        .bind(version_i64(request.expected_source_version)?)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(source_invariant());
        }
        let receipt = verify_source_transition(
            &mut transaction,
            request.anchor,
            target_version,
            request.expected_state,
            request.expected_state,
            Some(failure_code),
        )
        .await?;
        transaction.commit().await?;
        Ok(DeliverySourceTransitionOutcome::Applied(receipt))
    }
}
