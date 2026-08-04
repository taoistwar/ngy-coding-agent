#[path = "corruption/graphs.rs"]
mod graphs;
#[path = "corruption/observations.rs"]
mod observations;
#[path = "corruption/orphans.rs"]
mod orphans;

use coding_agent_store::{DeliveryRecoveryQuery, Store, StoreError};

use crate::authenticated_identity;

async fn assert_recovery_invariant(store: &Store) {
    let startup_error = store.startup_delivery_ownership().await.unwrap_err();
    assert_invariant(startup_error);
    let batch_error = store
        .delivery_recovery_batch(&DeliveryRecoveryQuery::first(authenticated_identity()))
        .await
        .unwrap_err();
    assert_invariant(batch_error);
}

fn assert_invariant(error: StoreError) {
    assert!(matches!(error, StoreError::InvariantViolation(_)));
    assert_eq!(
        error.to_string(),
        "store invariant failed: delivery recovery snapshot is inconsistent"
    );
    for secret in [
        "approved delivery prompt secret",
        crate::support::delivery::eligibility::COMMON_IDENTITY,
        crate::support::delivery::eligibility::ADMIN_IDENTITY,
    ] {
        assert!(!error.to_string().contains(secret));
    }
}
