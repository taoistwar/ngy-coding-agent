use crate::delivery::{
    CleanupOperationState, CleanupTransitionOutcome, DeliveryTimestamp,
    RefreshBranchCleanupTargetRequest,
};
use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::super::cleanup_invariant;
use super::super::replay::{
    CleanupTransitionLookup, lookup_transition, require_transition, version_i64,
};
use super::common::{
    BranchContext, load_context, operation_is_current, retained_branch_facts_are_exact,
};

impl Store {
    pub async fn refresh_branch_cleanup_target(
        &self,
        request: RefreshBranchCleanupTargetRequest,
    ) -> Result<CleanupTransitionOutcome, StoreError> {
        let target_version = request.anchor.expected_version.next()?;
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let Some(context) = load_context(&mut transaction, request.anchor).await? else {
            transaction.commit().await?;
            return Ok(CleanupTransitionOutcome::Conflict);
        };
        if let Some(outcome) =
            classify_replay(&mut transaction, &context, &request, target_version).await?
        {
            transaction.commit().await?;
            return Ok(outcome);
        }
        if !fresh_request_is_current(&context, &request) {
            transaction.commit().await?;
            return Ok(CleanupTransitionOutcome::Conflict);
        }
        let timestamp: DeliveryTimestamp = current_timestamp()?.to_string().parse()?;
        apply_refresh(
            &mut transaction,
            &context,
            &request,
            target_version,
            timestamp,
        )
        .await?;
        let receipt = verify_refresh(&mut transaction, &context, &request, target_version).await?;
        transaction.commit().await?;
        Ok(CleanupTransitionOutcome::Applied(receipt))
    }
}

async fn classify_replay(
    connection: &mut sqlx::SqliteConnection,
    context: &BranchContext,
    request: &RefreshBranchCleanupTargetRequest,
    target_version: crate::delivery::DeliveryVersion,
) -> Result<Option<CleanupTransitionOutcome>, StoreError> {
    let lookup = lookup_transition(
        connection,
        request.anchor.operation_id,
        target_version,
        CleanupOperationState::DeletePending,
        CleanupOperationState::DeletePending,
        None,
    )
    .await?;
    Ok(match lookup {
        CleanupTransitionLookup::Exact(receipt) => {
            let exact_payload = context
                .operation
                .target_head_at(request.anchor.expected_version)
                == Some(&request.expected_target_head)
                && context.operation.target_head_at(target_version)
                    == Some(&request.fresh_target_head);
            Some(if exact_payload {
                CleanupTransitionOutcome::Existing(receipt)
            } else {
                CleanupTransitionOutcome::Conflict
            })
        }
        CleanupTransitionLookup::Conflict => Some(CleanupTransitionOutcome::Conflict),
        CleanupTransitionLookup::Missing => None,
    })
}

fn fresh_request_is_current(
    context: &BranchContext,
    request: &RefreshBranchCleanupTargetRequest,
) -> bool {
    let current_target_is_exact = context.operation.expected_target_head.as_ref()
        == Some(&request.expected_target_head)
        && context
            .operation
            .target_head_at(request.anchor.expected_version)
            == Some(&request.expected_target_head)
        && context.operation.expected_source_oid.algorithm()
            == request.fresh_target_head.algorithm();
    operation_is_current(context, request.anchor)
        && retained_branch_facts_are_exact(context)
        && current_target_is_exact
}

async fn apply_refresh(
    connection: &mut sqlx::SqliteConnection,
    context: &BranchContext,
    request: &RefreshBranchCleanupTargetRequest,
    target_version: crate::delivery::DeliveryVersion,
    timestamp: DeliveryTimestamp,
) -> Result<(), StoreError> {
    let updated = sqlx::query(
        "UPDATE task_cleanup_operations \
         SET expected_target_head = ?, version = ?, updated_at = ? \
         WHERE operation_id = ? AND task_id = ? AND kind = 'delete_branch' \
           AND state = 'delete_pending' AND version = ? AND failure_code IS NULL \
           AND expected_disposition_version = ? AND expected_target_head = ?",
    )
    .bind(request.fresh_target_head.as_str())
    .bind(version_i64(target_version)?)
    .bind(timestamp.to_string())
    .bind(request.anchor.operation_id.to_string())
    .bind(request.anchor.task_id.to_string())
    .bind(version_i64(request.anchor.expected_version)?)
    .bind(version_i64(context.operation.expected_disposition_version)?)
    .bind(request.expected_target_head.as_str())
    .execute(connection)
    .await?;
    if updated.rows_affected() == 1 {
        Ok(())
    } else {
        Err(cleanup_invariant())
    }
}

async fn verify_refresh(
    connection: &mut sqlx::SqliteConnection,
    context: &BranchContext,
    request: &RefreshBranchCleanupTargetRequest,
    target_version: crate::delivery::DeliveryVersion,
) -> Result<super::super::model::CleanupTransitionReceipt, StoreError> {
    let receipt = require_transition(
        &mut *connection,
        request.anchor.operation_id,
        target_version,
        CleanupOperationState::DeletePending,
        CleanupOperationState::DeletePending,
        None,
    )
    .await?;
    let updated = load_context(&mut *connection, request.anchor)
        .await?
        .ok_or_else(cleanup_invariant)?;
    let exact = updated.operation.version == target_version
        && updated.operation.state == CleanupOperationState::DeletePending
        && updated.operation.failure_code.is_none()
        && updated.operation.expected_target_head.as_ref() == Some(&request.fresh_target_head)
        && updated.operation.target_head_at(target_version) == Some(&request.fresh_target_head)
        && updated.operation.origin_target_head == context.operation.origin_target_head
        && updated.disposition == context.disposition;
    if exact {
        Ok(receipt)
    } else {
        Err(cleanup_invariant())
    }
}
