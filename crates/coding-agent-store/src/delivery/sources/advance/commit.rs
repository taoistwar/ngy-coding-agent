use crate::delivery::{DeliverySourceState, DeliveryTimestamp};
use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::super::load::{TransitionLookup, load_source_context, lookup_source_transition};
use super::super::model::{CommitDeliverySourceRequest, DeliverySourceTransitionOutcome};
use super::super::source_invariant;
use super::super::validate::{
    validate_anchor_compatibility, validate_applied_proof, validate_current_source_reconciliation,
    validate_mutation_owner, validate_replay_anchor,
};
use super::verify::{
    audit_conflicting_source_transition, exact_accepted, verify_source_transition, version_i64,
};

impl Store {
    pub async fn commit_delivery_source(
        &self,
        request: CommitDeliverySourceRequest,
    ) -> Result<DeliverySourceTransitionOutcome, StoreError> {
        let target_version = request.expected_source_version.next()?;
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        match lookup_source_transition(
            &mut transaction,
            request.anchor.task_id,
            target_version,
            DeliverySourceState::CommitPending,
            DeliverySourceState::Committed,
            None,
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
                if validate_applied_proof(&source, &request.proof).is_err() {
                    transaction.commit().await?;
                    return Ok(DeliverySourceTransitionOutcome::Conflict);
                }
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
        if source.state != DeliverySourceState::CommitPending
            || source.version != request.expected_source_version
        {
            transaction.commit().await?;
            return Ok(DeliverySourceTransitionOutcome::Conflict);
        }
        validate_mutation_owner(&source, &operation, request.anchor)?;
        if validate_applied_proof(&source, &request.proof).is_err() {
            transaction.commit().await?;
            return Ok(DeliverySourceTransitionOutcome::Conflict);
        }

        let expected_commit = source
            .expected_source_commit
            .as_ref()
            .ok_or_else(source_invariant)?;
        let timestamp: DeliveryTimestamp = current_timestamp()?.to_string().parse()?;
        let updated = sqlx::query(
            "UPDATE task_delivery_sources \
             SET state = 'committed', failure_code = NULL, version = ?, updated_at = ? \
             WHERE task_id = ? AND repository_id = ? AND attempt = ? \
               AND state = 'commit_pending' AND version = ? \
               AND expected_source_commit_oid = ?",
        )
        .bind(version_i64(target_version)?)
        .bind(timestamp.to_string())
        .bind(request.anchor.task_id.to_string())
        .bind(source.provenance.identity.repository_id().to_string())
        .bind(i64::from(source.provenance.identity.attempt()))
        .bind(version_i64(request.expected_source_version)?)
        .bind(expected_commit.as_str())
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(source_invariant());
        }
        let receipt = verify_source_transition(
            &mut transaction,
            request.anchor,
            target_version,
            DeliverySourceState::CommitPending,
            DeliverySourceState::Committed,
            None,
        )
        .await?;
        transaction.commit().await?;
        Ok(DeliverySourceTransitionOutcome::Applied(receipt))
    }
}
