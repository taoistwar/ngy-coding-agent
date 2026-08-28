use std::str::FromStr;

use sqlx::Row;

use super::receipt_invariant;
use crate::StoreError;
use crate::delivery::receipts::model::{
    CommandActionAnchor, CommandRequestKey, DeliveryOperationKind,
};
use crate::delivery::receipts::{
    DELIVERY_COMMAND_REQUEST_HASH_ALGORITHM, DELIVERY_COMMAND_REQUEST_HASH_DOMAIN,
    DELIVERY_COMMAND_REQUEST_HASH_VERSION, DeliveryAcceptedOperationState, DeliveryCommandKind,
    DeliveryCommandReceipt, DeliveryResponseDiscriminator,
};
use crate::delivery::{
    DeliveryCommandId, DeliveryIdentity, DeliveryOperationId, DeliveryTimestamp, DeliveryVersion,
    Sha256Digest,
};

pub(super) fn decode_exact_receipt(
    row: &sqlx::sqlite::SqliteRow,
    key: &CommandRequestKey,
) -> Result<DeliveryCommandReceipt, StoreError> {
    let command_kind =
        DeliveryCommandKind::parse(&text(row, "command_kind")?).map_err(|_| receipt_invariant())?;
    let request_hash: Sha256Digest = text(row, "canonical_request_hash")?
        .parse()
        .map_err(|_| receipt_invariant())?;
    if command_kind != key.command_kind || request_hash != key.canonical_request_hash {
        return Err(StoreError::IdempotencyConflict);
    }

    let client_request_id = DeliveryCommandId::from_str(&text(row, "client_request_id")?)
        .map_err(|_| receipt_invariant())?;
    if client_request_id != key.client_request_id {
        return Err(receipt_invariant());
    }
    require_equal_text(
        row,
        "request_hash_domain",
        DELIVERY_COMMAND_REQUEST_HASH_DOMAIN,
    )?;
    require_equal_integer(
        row,
        "request_hash_version",
        i64::from(DELIVERY_COMMAND_REQUEST_HASH_VERSION),
    )?;
    require_equal_text(
        row,
        "request_hash_algorithm",
        DELIVERY_COMMAND_REQUEST_HASH_ALGORITHM,
    )?;

    let attempt = positive_u32(integer(row, "attempt")?)?;
    let identity = DeliveryIdentity::try_from_text(
        &text(row, "task_id")?,
        &text(row, "repository_id")?,
        attempt,
    )
    .map_err(|_| receipt_invariant())?;
    if identity.task_id() != key.task_id {
        return Err(receipt_invariant());
    }
    let operation_kind = DeliveryOperationKind::parse(&text(row, "operation_kind")?)
        .map_err(|_| receipt_invariant())?;
    if operation_kind != command_kind.operation_kind() {
        return Err(receipt_invariant());
    }
    let operation_id = DeliveryOperationId::from_str(&text(row, "operation_id")?)
        .map_err(|_| receipt_invariant())?;
    validate_decoded_action_anchor(row, key, operation_id)?;
    validate_pointer_union(row, operation_kind, operation_id)?;

    let version = delivery_version(integer(row, "accepted_operation_version")?)?;
    if version != key.expected_accepted_version {
        return Err(receipt_invariant());
    }
    let state = DeliveryAcceptedOperationState::parse(&text(row, "accepted_operation_state")?)
        .map_err(|_| receipt_invariant())?;
    if !state.accepts(command_kind) {
        return Err(receipt_invariant());
    }
    let response = DeliveryResponseDiscriminator::parse(&text(row, "response_discriminator")?)
        .map_err(|_| receipt_invariant())?;
    if response != command_kind.response_discriminator() {
        return Err(receipt_invariant());
    }
    let created_at =
        DeliveryTimestamp::from_str(&text(row, "created_at")?).map_err(|_| receipt_invariant())?;
    validate_historical_acceptance(row, command_kind)?;

    Ok(DeliveryCommandReceipt {
        client_request_id,
        command_kind,
        identity,
        canonical_request_hash: request_hash,
        operation_id,
        accepted_operation_version: version,
        accepted_operation_state: state,
        response_discriminator: response,
        created_at,
    })
}

fn validate_pointer_union(
    row: &sqlx::sqlite::SqliteRow,
    operation_kind: DeliveryOperationKind,
    operation_id: DeliveryOperationId,
) -> Result<(), StoreError> {
    let merge_operation_id = optional_text(row, "merge_operation_id")?;
    let cleanup_operation_id = optional_text(row, "cleanup_operation_id")?;
    let cleanup_merged_operation_id = optional_text(row, "cleanup_merged_operation_id")?;
    let operation_id = operation_id.to_string();
    let matches = match operation_kind {
        DeliveryOperationKind::MergeOperation => {
            merge_operation_id.as_deref() == Some(operation_id.as_str())
                && cleanup_operation_id.is_none()
                && cleanup_merged_operation_id.is_none()
        }
        DeliveryOperationKind::CleanupOperation => {
            cleanup_operation_id.as_deref() == Some(operation_id.as_str())
                && merge_operation_id.is_none()
                && cleanup_merged_operation_id.is_some()
        }
    };
    if matches {
        Ok(())
    } else {
        Err(receipt_invariant())
    }
}

fn validate_historical_acceptance(
    row: &sqlx::sqlite::SqliteRow,
    command_kind: DeliveryCommandKind,
) -> Result<(), StoreError> {
    let expected_from_state = match command_kind {
        DeliveryCommandKind::AcceptMerge => "preflight_ready",
        DeliveryCommandKind::Preflight
        | DeliveryCommandKind::RemoveWorktree
        | DeliveryCommandKind::DeleteBranch => "absent",
    };
    let transition_id = optional_integer(row, "historical_transition_id")?;
    if transition_id.is_none()
        || transition_id.is_some_and(|id| id <= 0)
        || optional_text(row, "historical_from_state")?.as_deref() != Some(expected_from_state)
        || optional_text(row, "historical_failure_code")?.is_some()
        || optional_text(row, "historical_transitioned_at")? != optional_text(row, "created_at")?
        || integer(row, "immutable_pointer_matches")? != 1
    {
        Err(receipt_invariant())
    } else {
        Ok(())
    }
}

fn validate_decoded_action_anchor(
    row: &sqlx::sqlite::SqliteRow,
    key: &CommandRequestKey,
    receipt_operation_id: DeliveryOperationId,
) -> Result<(), StoreError> {
    let cleanup_merge = optional_text(row, "cleanup_merged_operation_id")?
        .map(|value| DeliveryOperationId::from_str(&value).map_err(|_| receipt_invariant()))
        .transpose()?;
    let matches = match key.action_anchor {
        CommandActionAnchor::NewOperation => cleanup_merge.is_none(),
        CommandActionAnchor::ExistingOperation(expected) => {
            expected == receipt_operation_id && cleanup_merge.is_none()
        }
        CommandActionAnchor::CleanupFromMerge(expected) => cleanup_merge == Some(expected),
    };
    if matches {
        Ok(())
    } else {
        Err(receipt_invariant())
    }
}

fn text(row: &sqlx::sqlite::SqliteRow, column: &str) -> Result<String, StoreError> {
    row.try_get(column).map_err(|_| receipt_invariant())
}

fn integer(row: &sqlx::sqlite::SqliteRow, column: &str) -> Result<i64, StoreError> {
    row.try_get(column).map_err(|_| receipt_invariant())
}

fn optional_text(
    row: &sqlx::sqlite::SqliteRow,
    column: &str,
) -> Result<Option<String>, StoreError> {
    row.try_get(column).map_err(|_| receipt_invariant())
}

fn optional_integer(
    row: &sqlx::sqlite::SqliteRow,
    column: &str,
) -> Result<Option<i64>, StoreError> {
    row.try_get(column).map_err(|_| receipt_invariant())
}

fn require_equal_text(
    row: &sqlx::sqlite::SqliteRow,
    column: &str,
    expected: &str,
) -> Result<(), StoreError> {
    if text(row, column)? == expected {
        Ok(())
    } else {
        Err(receipt_invariant())
    }
}

fn require_equal_integer(
    row: &sqlx::sqlite::SqliteRow,
    column: &str,
    expected: i64,
) -> Result<(), StoreError> {
    if integer(row, column)? == expected {
        Ok(())
    } else {
        Err(receipt_invariant())
    }
}

fn positive_u32(value: i64) -> Result<u32, StoreError> {
    let value = u32::try_from(value).map_err(|_| receipt_invariant())?;
    if value == 0 {
        Err(receipt_invariant())
    } else {
        Ok(value)
    }
}

fn delivery_version(value: i64) -> Result<DeliveryVersion, StoreError> {
    let value = u64::try_from(value).map_err(|_| receipt_invariant())?;
    DeliveryVersion::try_new(value).map_err(|_| receipt_invariant())
}
