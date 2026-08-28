use sqlx::SqliteConnection;

use super::receipt_invariant;
use crate::StoreError;
use crate::delivery::receipts::model::{
    CanonicalCommandRequest, CommandActionAnchor, CommandRequestKey, DeliveryOperationKind,
};
use crate::delivery::receipts::{
    DELIVERY_COMMAND_REQUEST_HASH_ALGORITHM, DELIVERY_COMMAND_REQUEST_HASH_DOMAIN,
    DELIVERY_COMMAND_REQUEST_HASH_VERSION, DeliveryAcceptedOperationState,
};
use crate::delivery::{DeliveryIdentity, DeliveryOperationId, DeliveryTimestamp, DeliveryVersion};

pub(crate) struct ReceiptWrite {
    key: CommandRequestKey,
    identity: DeliveryIdentity,
    operation_id: DeliveryOperationId,
    accepted_operation_version: DeliveryVersion,
    accepted_operation_state: DeliveryAcceptedOperationState,
    created_at: DeliveryTimestamp,
}

impl ReceiptWrite {
    pub(crate) fn try_new(
        request: &impl CanonicalCommandRequest,
        identity: DeliveryIdentity,
        operation_id: DeliveryOperationId,
        accepted_operation_version: DeliveryVersion,
        accepted_operation_state: DeliveryAcceptedOperationState,
        created_at: DeliveryTimestamp,
    ) -> Result<Self, StoreError> {
        let key = request.command_request_key();
        if identity.task_id() != key.task_id
            || accepted_operation_version != key.expected_accepted_version
            || matches!(
                key.action_anchor,
                CommandActionAnchor::ExistingOperation(expected) if expected != operation_id
            )
            || !accepted_operation_state.accepts(key.command_kind)
        {
            return Err(receipt_invariant());
        }
        Ok(Self {
            key,
            identity,
            operation_id,
            accepted_operation_version,
            accepted_operation_state,
            created_at,
        })
    }
}

pub(crate) async fn insert_receipt(
    connection: &mut SqliteConnection,
    write: &ReceiptWrite,
) -> Result<(), StoreError> {
    validate_action_anchor(connection, write).await?;
    reject_duplicate_operation_receipt(connection, write).await?;

    let operation_kind = write.key.command_kind.operation_kind();
    let merge_operation_id = (operation_kind == DeliveryOperationKind::MergeOperation)
        .then(|| write.operation_id.to_string());
    let cleanup_operation_id = (operation_kind == DeliveryOperationKind::CleanupOperation)
        .then(|| write.operation_id.to_string());
    let cleanup_merged_operation_id = match write.key.action_anchor {
        CommandActionAnchor::CleanupFromMerge(operation_id) => Some(operation_id.to_string()),
        CommandActionAnchor::NewOperation | CommandActionAnchor::ExistingOperation(_) => None,
    };
    sqlx::query(
        "INSERT INTO task_delivery_command_receipts ( \
             client_request_id, command_kind, task_id, repository_id, attempt, \
             request_hash_domain, request_hash_version, request_hash_algorithm, \
             canonical_request_hash, operation_kind, operation_id, merge_operation_id, \
             cleanup_operation_id, cleanup_merged_operation_id, accepted_operation_version, \
             accepted_operation_state, response_discriminator, created_at \
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(write.key.client_request_id.to_string())
    .bind(write.key.command_kind.as_str())
    .bind(write.identity.task_id().to_string())
    .bind(write.identity.repository_id().to_string())
    .bind(i64::from(write.identity.attempt()))
    .bind(DELIVERY_COMMAND_REQUEST_HASH_DOMAIN)
    .bind(i64::from(DELIVERY_COMMAND_REQUEST_HASH_VERSION))
    .bind(DELIVERY_COMMAND_REQUEST_HASH_ALGORITHM)
    .bind(write.key.canonical_request_hash.as_str())
    .bind(operation_kind.as_str())
    .bind(write.operation_id.to_string())
    .bind(merge_operation_id)
    .bind(cleanup_operation_id)
    .bind(cleanup_merged_operation_id)
    .bind(i64::try_from(write.accepted_operation_version.get()).map_err(|_| receipt_invariant())?)
    .bind(write.accepted_operation_state.as_str())
    .bind(write.key.command_kind.response_discriminator().as_str())
    .bind(write.created_at.to_string())
    .execute(&mut *connection)
    .await?;
    Ok(())
}

async fn reject_duplicate_operation_receipt(
    connection: &mut SqliteConnection,
    write: &ReceiptWrite,
) -> Result<(), StoreError> {
    let duplicate: i64 = sqlx::query_scalar(
        "SELECT EXISTS( \
             SELECT 1 FROM task_delivery_command_receipts \
             WHERE command_kind = ? AND operation_id = ? \
         )",
    )
    .bind(write.key.command_kind.as_str())
    .bind(write.operation_id.to_string())
    .fetch_one(&mut *connection)
    .await?;
    if duplicate == 0 {
        Ok(())
    } else {
        Err(StoreError::IdempotencyConflict)
    }
}

async fn validate_action_anchor(
    connection: &mut SqliteConnection,
    write: &ReceiptWrite,
) -> Result<(), StoreError> {
    match write.key.action_anchor {
        CommandActionAnchor::NewOperation => Ok(()),
        CommandActionAnchor::ExistingOperation(expected) => {
            if expected == write.operation_id {
                Ok(())
            } else {
                Err(receipt_invariant())
            }
        }
        CommandActionAnchor::CleanupFromMerge(expected) => {
            validate_cleanup_anchor(connection, write, expected).await
        }
    }
}

async fn validate_cleanup_anchor(
    connection: &mut SqliteConnection,
    write: &ReceiptWrite,
    expected_merge_operation_id: DeliveryOperationId,
) -> Result<(), StoreError> {
    let matches: i64 = sqlx::query_scalar(
        "SELECT EXISTS( \
             SELECT 1 FROM task_cleanup_operations c \
             JOIN task_artifact_dispositions d ON d.task_id = c.disposition_task_id \
             WHERE c.operation_id = ? AND c.task_id = ? \
               AND d.merged_operation_id = ? \
         )",
    )
    .bind(write.operation_id.to_string())
    .bind(write.identity.task_id().to_string())
    .bind(expected_merge_operation_id.to_string())
    .fetch_one(&mut *connection)
    .await?;
    if matches == 1 {
        Ok(())
    } else {
        Err(receipt_invariant())
    }
}
