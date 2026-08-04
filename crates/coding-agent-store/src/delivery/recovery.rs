//! Read-only startup discovery and bounded delivery-operation recovery selection.

mod audit;
mod model;
mod select;

pub use model::{
    AcceptedDeliverySourceState, DeliveryRecoveryAction, DeliveryRecoveryBatch,
    DeliveryRecoveryCursor, DeliveryRecoveryDisposition, DeliveryRecoveryEntry,
    DeliveryRecoveryQuery, DeliveryRecoveryQueryError, MAX_DELIVERY_RECOVERY_BATCH,
    StartupDeliveryOwnership,
};

use crate::{Store, StoreError};

impl Store {
    /// Audits every delivery-owned graph in one consistent read and returns the
    /// persisted common Git identity expected for each owned task.
    ///
    /// This method does not authenticate an external checkout identity and does
    /// not execute Git. The caller remains responsible for authentication before
    /// constructing a recovery query.
    pub async fn startup_delivery_ownership(
        &self,
    ) -> Result<Vec<StartupDeliveryOwnership>, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let audited = audit::load_all(&mut transaction)
            .await
            .map_err(recovery_read_error)?;
        let ownership = audited
            .iter()
            .map(StartupDeliveryOwnership::from_audited)
            .collect();
        transaction.commit().await?;
        Ok(ownership)
    }

    /// Returns at most [`MAX_DELIVERY_RECOVERY_BATCH`] durable recovery entries
    /// for one identity that the caller has already authenticated.
    ///
    /// Every call audits all delivery-owned graphs before selecting the requested
    /// identity, so corruption in another graph cannot be hidden by filtering or
    /// pagination.
    pub async fn delivery_recovery_batch(
        &self,
        query: &DeliveryRecoveryQuery,
    ) -> Result<DeliveryRecoveryBatch, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let audited = audit::load_all(&mut transaction)
            .await
            .map_err(recovery_read_error)?;
        let batch = select::bounded_batch(audited, query).map_err(recovery_read_error)?;
        transaction.commit().await?;
        Ok(batch)
    }
}

fn recovery_read_error(error: StoreError) -> StoreError {
    match error {
        StoreError::Database(_) => error,
        _ => StoreError::InvariantViolation("delivery recovery snapshot is inconsistent"),
    }
}
