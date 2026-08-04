mod support;

#[path = "delivery_recovery/corruption.rs"]
mod corruption_cases;
#[path = "delivery_recovery/faults.rs"]
mod fault_cases;
#[path = "delivery_recovery/pagination.rs"]
mod pagination_cases;
#[path = "delivery_recovery/reconciliation.rs"]
mod reconciliation_cases;
#[path = "delivery_recovery/fixtures.rs"]
mod recovery_fixtures;
#[path = "delivery_recovery/selection.rs"]
mod selection_cases;

use coding_agent_store::{
    DIRECTORY_IDENTITY_ALGORITHM_V1, DeliveryRecoveryQuery, DirectoryIdentity,
    MAX_DELIVERY_RECOVERY_BATCH,
};

pub(crate) fn authenticated_identity() -> DirectoryIdentity {
    DirectoryIdentity::try_new(
        DIRECTORY_IDENTITY_ALGORITHM_V1,
        support::delivery::eligibility::COMMON_IDENTITY,
    )
    .unwrap()
}

#[tokio::test]
async fn empty_store_has_no_delivery_startup_ownership_or_recovery_work() {
    let store = support::seeded_store().await;
    let authenticated_identity = authenticated_identity();

    let ownership = store.startup_delivery_ownership().await.unwrap();
    assert!(ownership.is_empty());

    let query = DeliveryRecoveryQuery::first(authenticated_identity);
    let batch = store.delivery_recovery_batch(&query).await.unwrap();
    assert!(batch.entries.is_empty());
    assert!(batch.next_cursor.is_none());
    assert_eq!(MAX_DELIVERY_RECOVERY_BATCH, 64);
}
