use sqlx::SqliteConnection;

use crate::delivery::eligibility;
use crate::delivery::ownership::load_cleanup_operation_exact;
use crate::delivery::receipts::{ReceiptWrite, insert_receipt, lookup_receipt};
use crate::delivery::{
    BranchDisposition, CleanupKind, CleanupOperationState, DeliveryAcceptedOperationState,
    DeliveryCommandReceipt, DeliveryOperationId, DeliverySourceState, DeliveryTimestamp,
    DeliveryVersion, RemoveWorktreeCommandRequest, WorktreeDisposition,
};
use crate::tasks::current_timestamp;
use crate::{Store, StoreError};

use super::super::{cleanup_invariant, model::CleanupAcceptanceOutcome};

impl Store {
    pub async fn accept_worktree_cleanup(
        &self,
        request: RemoveWorktreeCommandRequest,
    ) -> Result<CleanupAcceptanceOutcome, StoreError> {
        let mut first = self.pool.begin().await?;
        if let Some(outcome) = try_existing(&mut first, &request).await? {
            first.commit().await?;
            return Ok(outcome);
        }
        first.commit().await?;

        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(outcome) = try_existing(&mut transaction, &request).await? {
            transaction.commit().await?;
            return Ok(outcome);
        }
        let Some(prepared) = prepare_fresh(&mut transaction, &request).await? else {
            transaction.commit().await?;
            return Ok(CleanupAcceptanceOutcome::Conflict);
        };
        let receipt = insert_fresh(&mut transaction, &request, prepared).await?;
        transaction.commit().await?;
        Ok(CleanupAcceptanceOutcome::Accepted(receipt))
    }
}

async fn try_existing(
    connection: &mut SqliteConnection,
    request: &RemoveWorktreeCommandRequest,
) -> Result<Option<CleanupAcceptanceOutcome>, StoreError> {
    let Some(receipt) = lookup_receipt(&mut *connection, request).await? else {
        return Ok(None);
    };
    let operation = load_cleanup_operation_exact(&mut *connection, receipt.operation_id).await?;
    let exact = operation.operation_id == receipt.operation_id
        && operation.identity == receipt.identity
        && operation.identity.task_id() == request.task_id()
        && operation.kind == CleanupKind::RemoveWorktree
        && operation.origin_receipt_id == request.client_request_id()
        && operation.expected_source_ref == *request.expected_source_ref()
        && operation.expected_source_oid == *request.expected_source_oid()
        && operation.origin_target_head.is_none();
    if !exact {
        return Err(cleanup_invariant());
    }
    Ok(Some(CleanupAcceptanceOutcome::Existing(receipt)))
}

struct PreparedWorktreeAcceptance {
    identity: crate::delivery::DeliveryIdentity,
    state: CleanupOperationState,
    source: crate::delivery::DeliverySourceRecord,
}

async fn prepare_fresh(
    connection: &mut SqliteConnection,
    request: &RemoveWorktreeCommandRequest,
) -> Result<Option<PreparedWorktreeAcceptance>, StoreError> {
    let Some(snapshot) = eligibility::load_snapshot(&mut *connection, request.task_id()).await?
    else {
        return Err(StoreError::TaskNotFound);
    };
    let ownership = snapshot.ownership;
    let Some(source) = ownership.source else {
        return Ok(None);
    };
    let Some(disposition) = ownership.disposition else {
        return Ok(None);
    };
    let merged_is_exact = ownership.merge_operations.iter().any(|operation| {
        operation.operation_id == disposition.merged_operation_id
            && operation.state == crate::delivery::MergeOperationState::Merged
    });
    let active_or_reconciliation = ownership.cleanup_operations.iter().any(|operation| {
        operation.state.is_side_effect_active() || operation.state.is_reconciliation()
    });
    let anchors_are_exact = source.state == DeliverySourceState::Committed
        && source.expected_source_commit.as_ref() == Some(request.expected_source_oid())
        && source.provenance.source_branch == *request.expected_source_ref()
        && disposition.merged_operation_id == request.expected_merge_operation_id()
        && disposition.source_commit == *request.expected_source_oid()
        && disposition.worktree_version == request.expected_disposition_version()
        && disposition.branch_state == BranchDisposition::Retained;
    if !merged_is_exact || active_or_reconciliation || !anchors_are_exact {
        return Ok(None);
    }

    let state = match disposition.worktree_state {
        WorktreeDisposition::RetainedLocked => CleanupOperationState::UnlockPending,
        WorktreeDisposition::RetainedUnlocked => {
            let failed_at_current_fact = ownership.cleanup_operations.iter().any(|operation| {
                operation.kind == CleanupKind::RemoveWorktree
                    && operation.state == CleanupOperationState::Failed
                    && operation.expected_disposition_version == disposition.worktree_version
            });
            if !failed_at_current_fact {
                return Ok(None);
            }
            CleanupOperationState::RemovePending
        }
        WorktreeDisposition::Removed | WorktreeDisposition::ReconciliationRequired => {
            return Ok(None);
        }
    };
    Ok(Some(PreparedWorktreeAcceptance {
        identity: disposition.identity,
        state,
        source,
    }))
}

async fn insert_fresh(
    connection: &mut SqliteConnection,
    request: &RemoveWorktreeCommandRequest,
    prepared: PreparedWorktreeAcceptance,
) -> Result<DeliveryCommandReceipt, StoreError> {
    let operation_id = DeliveryOperationId::new();
    let version = DeliveryVersion::initial();
    let timestamp: DeliveryTimestamp = current_timestamp()?.to_string().parse()?;
    let source_commit = prepared
        .source
        .expected_source_commit
        .as_ref()
        .ok_or_else(cleanup_invariant)?;
    let inserted = sqlx::query(
        "INSERT INTO task_cleanup_operations ( \
             operation_id, task_id, repository_id, attempt, kind, origin_receipt_id, \
             disposition_task_id, expected_worktree_path, expected_admin_identity_algorithm, \
             expected_admin_identity_digest, expected_common_git_identity_algorithm, \
             expected_common_git_identity_digest, expected_source_ref, expected_source_oid, \
             expected_disposition_version, origin_target_head, expected_target_ref, \
             expected_target_head, state, failure_code, version, created_at, updated_at \
         ) VALUES (?, ?, ?, ?, 'remove_worktree', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                   NULL, NULL, NULL, ?, NULL, ?, ?, ?)",
    )
    .bind(operation_id.to_string())
    .bind(prepared.identity.task_id().to_string())
    .bind(prepared.identity.repository_id().to_string())
    .bind(i64::from(prepared.identity.attempt()))
    .bind(request.client_request_id().to_string())
    .bind(prepared.identity.task_id().to_string())
    .bind(prepared.source.provenance.worktree_path.to_string())
    .bind(
        prepared
            .source
            .provenance
            .worktree_admin_identity
            .algorithm(),
    )
    .bind(
        prepared
            .source
            .provenance
            .worktree_admin_identity
            .digest
            .as_str(),
    )
    .bind(prepared.source.provenance.common_git_identity.algorithm())
    .bind(
        prepared
            .source
            .provenance
            .common_git_identity
            .digest
            .as_str(),
    )
    .bind(prepared.source.provenance.source_branch.as_str())
    .bind(source_commit.as_str())
    .bind(version_i64(request.expected_disposition_version())?)
    .bind(prepared.state.as_str())
    .bind(version_i64(version)?)
    .bind(timestamp.to_string())
    .bind(timestamp.to_string())
    .execute(&mut *connection)
    .await?;
    if inserted.rows_affected() != 1 {
        return Err(cleanup_invariant());
    }
    let accepted_state = match prepared.state {
        CleanupOperationState::UnlockPending => DeliveryAcceptedOperationState::UnlockPending,
        CleanupOperationState::RemovePending => DeliveryAcceptedOperationState::RemovePending,
        _ => return Err(cleanup_invariant()),
    };
    let receipt_write = ReceiptWrite::try_new(
        request,
        prepared.identity,
        operation_id,
        version,
        accepted_state,
        timestamp,
    )?;
    insert_receipt(&mut *connection, &receipt_write).await?;
    let receipt = lookup_receipt(&mut *connection, request)
        .await?
        .ok_or_else(cleanup_invariant)?;
    let stored = load_cleanup_operation_exact(&mut *connection, operation_id).await?;
    if stored.state != prepared.state || stored.version != version {
        return Err(cleanup_invariant());
    }
    Ok(receipt)
}

fn version_i64(version: DeliveryVersion) -> Result<i64, StoreError> {
    i64::try_from(version.get()).map_err(|_| cleanup_invariant())
}
