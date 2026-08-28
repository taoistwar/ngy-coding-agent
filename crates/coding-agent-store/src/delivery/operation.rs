use std::fmt;

use coding_agent_domain::TaskId;

use crate::{Store, StoreError};

use super::ownership::{load_cleanup_operation_exact, load_merge_operation_exact};
use super::{CleanupOperationRecord, DeliveryOperationId, MergeOperationRecord};

const OPERATION_SNAPSHOT_INVARIANT: &str = "delivery operation snapshot is inconsistent";

#[derive(Clone, PartialEq, Eq)]
pub enum DeliveryOperationSnapshot {
    Merge(Box<MergeOperationRecord>),
    Cleanup(Box<CleanupOperationRecord>),
}

impl DeliveryOperationSnapshot {
    pub const fn operation_id(&self) -> DeliveryOperationId {
        match self {
            Self::Merge(operation) => operation.operation_id,
            Self::Cleanup(operation) => operation.operation_id,
        }
    }

    pub const fn task_id(&self) -> TaskId {
        match self {
            Self::Merge(operation) => operation.provenance.identity.task_id(),
            Self::Cleanup(operation) => operation.identity.task_id(),
        }
    }
}

impl fmt::Debug for DeliveryOperationSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Merge(operation) => formatter
                .debug_tuple("DeliveryOperationSnapshot::Merge")
                .field(operation)
                .finish(),
            Self::Cleanup(operation) => formatter
                .debug_tuple("DeliveryOperationSnapshot::Cleanup")
                .field(operation)
                .finish(),
        }
    }
}

impl Store {
    /// Loads one merge or cleanup operation after auditing its complete task-owned graph.
    ///
    /// Operation identifiers are required to identify exactly one operation kind. A corrupt
    /// cross-table duplicate fails closed instead of selecting either row by precedence.
    pub async fn delivery_operation_snapshot(
        &self,
        operation_id: DeliveryOperationId,
    ) -> Result<Option<DeliveryOperationSnapshot>, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let presence: (i64, i64) = sqlx::query_as(
            "SELECT EXISTS(SELECT 1 FROM task_merge_operations WHERE operation_id = ?), \
                    EXISTS(SELECT 1 FROM task_cleanup_operations WHERE operation_id = ?)",
        )
        .bind(operation_id.to_string())
        .bind(operation_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        let snapshot = match presence {
            (0, 0) => None,
            (1, 0) => Some(DeliveryOperationSnapshot::Merge(Box::new(
                load_merge_operation_exact(&mut transaction, operation_id)
                    .await
                    .map_err(operation_snapshot_error)?,
            ))),
            (0, 1) => Some(DeliveryOperationSnapshot::Cleanup(Box::new(
                load_cleanup_operation_exact(&mut transaction, operation_id)
                    .await
                    .map_err(operation_snapshot_error)?,
            ))),
            _ => return Err(operation_snapshot_invariant()),
        };
        transaction.commit().await?;
        Ok(snapshot)
    }
}

fn operation_snapshot_error(error: StoreError) -> StoreError {
    match error {
        StoreError::Database(_) => error,
        _ => operation_snapshot_invariant(),
    }
}

fn operation_snapshot_invariant() -> StoreError {
    StoreError::InvariantViolation(OPERATION_SNAPSHOT_INVARIANT)
}
