use std::cmp::Ordering;

use crate::StoreError;
use crate::delivery::{
    CleanupOperationState, DeliveryOperationId, DeliverySourceState, MergeOperationState,
};

use super::audit::AuditedDeliveryOwnership;
use super::model::{
    AcceptedDeliverySourceState, DeliveryRecoveryAction, DeliveryRecoveryBatch,
    DeliveryRecoveryCursor, DeliveryRecoveryDisposition, DeliveryRecoveryEntry,
    DeliveryRecoveryQuery, MAX_DELIVERY_RECOVERY_BATCH,
};

#[derive(Clone, Eq, PartialEq)]
struct RecoveryOrder {
    initial_transition_id: i64,
    entity_rank: u8,
    canonical_id: String,
}

impl Ord for RecoveryOrder {
    fn cmp(&self, other: &Self) -> Ordering {
        (
            self.initial_transition_id,
            self.entity_rank,
            self.canonical_id.as_str(),
        )
            .cmp(&(
                other.initial_transition_id,
                other.entity_rank,
                other.canonical_id.as_str(),
            ))
    }
}

impl PartialOrd for RecoveryOrder {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct OrderedEntry {
    order: RecoveryOrder,
    entry: DeliveryRecoveryEntry,
}

pub(super) fn bounded_batch(
    audited: Vec<AuditedDeliveryOwnership>,
    query: &DeliveryRecoveryQuery,
) -> Result<DeliveryRecoveryBatch, StoreError> {
    if query
        .after
        .as_ref()
        .is_some_and(|cursor| cursor.authenticated_identity != query.authenticated_identity)
    {
        return Err(recovery_invariant());
    }
    let after = query.after.as_ref().map(order_from_cursor);
    let mut ordered = audited
        .into_iter()
        .filter(|item| item.expected_common_git_identity == query.authenticated_identity)
        .map(classify)
        .filter_map(Result::transpose)
        .collect::<Result<Vec<_>, _>>()?;
    ordered.sort_by(|left, right| left.order.cmp(&right.order));
    if let Some(after) = after {
        ordered.retain(|candidate| candidate.order > after);
    }

    let has_more = ordered.len() > MAX_DELIVERY_RECOVERY_BATCH;
    ordered.truncate(MAX_DELIVERY_RECOVERY_BATCH);
    let next_cursor =
        has_more
            .then(|| ordered.last())
            .flatten()
            .map(|last| DeliveryRecoveryCursor {
                authenticated_identity: query.authenticated_identity.clone(),
                initial_transition_id: last.order.initial_transition_id,
                entity_rank: last.order.entity_rank,
                canonical_id: last.order.canonical_id.clone(),
            });
    Ok(DeliveryRecoveryBatch {
        entries: ordered.into_iter().map(|item| item.entry).collect(),
        next_cursor,
    })
}

fn classify(item: AuditedDeliveryOwnership) -> Result<Option<OrderedEntry>, StoreError> {
    let classification = if item.ownership.requires_reconciliation() {
        Some((
            reconciliation_order(&item)?,
            DeliveryRecoveryDisposition::ReconciliationRequired,
        ))
    } else if let Some(cleanup) = item
        .ownership
        .cleanup_operations
        .iter()
        .find(|operation| operation.state.is_side_effect_active())
    {
        let action = cleanup_action(cleanup.operation_id, cleanup.version, cleanup.state)?;
        Some((
            RecoveryOrder {
                initial_transition_id: cleanup.initial_transition_id,
                entity_rank: 2,
                canonical_id: cleanup.operation_id.to_string(),
            },
            DeliveryRecoveryDisposition::Recover(action),
        ))
    } else if let Some(operation) = item.ownership.merge_operations.iter().find(|operation| {
        matches!(
            operation.state,
            MergeOperationState::PreflightPending
                | MergeOperationState::Accepted
                | MergeOperationState::MergePending
                | MergeOperationState::AbortPending
        )
    }) {
        let (action, initial_transition_id) = merge_action(&item, operation)?;
        Some((
            RecoveryOrder {
                initial_transition_id,
                entity_rank: 1,
                canonical_id: operation.operation_id.to_string(),
            },
            DeliveryRecoveryDisposition::Recover(action),
        ))
    } else {
        None
    };
    Ok(classification.map(|(order, disposition)| OrderedEntry {
        order,
        entry: DeliveryRecoveryEntry {
            identity: item.identity,
            expected_common_git_identity: item.expected_common_git_identity,
            disposition,
            ownership: item.ownership,
        },
    }))
}

fn merge_action(
    item: &AuditedDeliveryOwnership,
    operation: &crate::delivery::MergeOperationRecord,
) -> Result<(DeliveryRecoveryAction, i64), StoreError> {
    let action = match operation.state {
        MergeOperationState::PreflightPending => DeliveryRecoveryAction::PreflightPending {
            operation_id: operation.operation_id,
            version: operation.version,
            inputs: operation.preflight_inputs.clone(),
            target_config_attributes_digest: operation.target_config_attributes_digest.clone(),
            target_security_digest: operation.target_security_digest.clone(),
        },
        MergeOperationState::Accepted => {
            let source = match item.ownership.source.as_ref() {
                None => AcceptedDeliverySourceState::Missing,
                Some(source) if source.state == DeliverySourceState::ObjectPending => {
                    AcceptedDeliverySourceState::ObjectPending {
                        version: source.version,
                    }
                }
                Some(source) if source.state == DeliverySourceState::CommitPending => {
                    AcceptedDeliverySourceState::CommitPending {
                        version: source.version,
                    }
                }
                Some(source) if source.state == DeliverySourceState::Committed => {
                    AcceptedDeliverySourceState::Committed {
                        version: source.version,
                    }
                }
                Some(_) => return Err(recovery_invariant()),
            };
            return Ok((
                DeliveryRecoveryAction::Accepted {
                    operation_id: operation.operation_id,
                    version: operation.version,
                    source,
                    target_config_attributes_digest: operation
                        .target_config_attributes_digest
                        .clone(),
                    target_security_digest: operation.target_security_digest.clone(),
                },
                operation.initial_transition_id,
            ));
        }
        MergeOperationState::MergePending => DeliveryRecoveryAction::MergePending {
            operation_id: operation.operation_id,
            version: operation.version,
            target_config_attributes_digest: operation.target_config_attributes_digest.clone(),
            target_security_digest: operation.target_security_digest.clone(),
        },
        MergeOperationState::AbortPending => DeliveryRecoveryAction::AbortPending {
            operation_id: operation.operation_id,
            version: operation.version,
            target_config_attributes_digest: operation.target_config_attributes_digest.clone(),
            target_security_digest: operation.target_security_digest.clone(),
        },
        _ => return Err(recovery_invariant()),
    };
    Ok((action, operation.initial_transition_id))
}

fn cleanup_action(
    operation_id: DeliveryOperationId,
    version: crate::delivery::DeliveryVersion,
    state: CleanupOperationState,
) -> Result<DeliveryRecoveryAction, StoreError> {
    match state {
        CleanupOperationState::UnlockPending => Ok(DeliveryRecoveryAction::UnlockPending {
            operation_id,
            version,
        }),
        CleanupOperationState::UnlockedPendingRemove => {
            Ok(DeliveryRecoveryAction::UnlockedPendingRemove {
                operation_id,
                version,
            })
        }
        CleanupOperationState::RemovePending => Ok(DeliveryRecoveryAction::RemovePending {
            operation_id,
            version,
        }),
        CleanupOperationState::DeletePending => Ok(DeliveryRecoveryAction::DeletePending {
            operation_id,
            version,
        }),
        _ => Err(recovery_invariant()),
    }
}

fn reconciliation_order(item: &AuditedDeliveryOwnership) -> Result<RecoveryOrder, StoreError> {
    if let Some(cleanup) = item
        .ownership
        .cleanup_operations
        .iter()
        .find(|operation| operation.state.is_reconciliation())
    {
        return Ok(RecoveryOrder {
            initial_transition_id: cleanup.initial_transition_id,
            entity_rank: 2,
            canonical_id: cleanup.operation_id.to_string(),
        });
    }
    if let Some(source) = item
        .ownership
        .source
        .as_ref()
        .filter(|source| source.state.is_reconciliation())
    {
        let operation = item
            .ownership
            .merge_operations
            .iter()
            .find(|operation| {
                operation.operation_id == source.origin_accepted_operation_id
                    && operation.state.is_reconciliation()
            })
            .ok_or_else(recovery_invariant)?;
        return Ok(RecoveryOrder {
            initial_transition_id: operation.initial_transition_id,
            entity_rank: 1,
            canonical_id: operation.operation_id.to_string(),
        });
    }
    item.ownership
        .merge_operations
        .iter()
        .find(|operation| operation.state.is_reconciliation())
        .map(|operation| RecoveryOrder {
            initial_transition_id: operation.initial_transition_id,
            entity_rank: 1,
            canonical_id: operation.operation_id.to_string(),
        })
        .ok_or_else(recovery_invariant)
}

fn order_from_cursor(cursor: &DeliveryRecoveryCursor) -> RecoveryOrder {
    RecoveryOrder {
        initial_transition_id: cursor.initial_transition_id,
        entity_rank: cursor.entity_rank,
        canonical_id: cursor.canonical_id.clone(),
    }
}

fn recovery_invariant() -> StoreError {
    StoreError::InvariantViolation("delivery recovery snapshot is inconsistent")
}
