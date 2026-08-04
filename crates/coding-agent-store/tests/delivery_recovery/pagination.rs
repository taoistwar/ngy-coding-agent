use coding_agent_store::{
    CreateDeliverySourceOutcome, CreateDeliverySourceRequest, DIRECTORY_IDENTITY_ALGORITHM_V1,
    DeliveryRecoveryQuery, DeliveryRecoveryQueryError, DirectoryIdentity,
    MAX_DELIVERY_RECOVERY_BATCH,
};

use crate::authenticated_identity;
use crate::recovery_fixtures::{accept_existing, pending_preflight};
use crate::support::delivery::eligibility::COMMON_IDENTITY;

const ALTERNATE_IDENTITY: &str = "abababababababababababababababababababababababababababababababab";

#[tokio::test]
async fn bounded_batches_page_in_immutable_operation_creation_order() {
    let store = crate::support::seeded_store().await;
    let mut expected = Vec::with_capacity(MAX_DELIVERY_RECOVERY_BATCH + 1);
    for index in 0..=MAX_DELIVERY_RECOVERY_BATCH {
        let branch = format!("codex/recovery-page-{index:02}");
        let (task, operation_id) = pending_preflight(&store, &branch, COMMON_IDENTITY).await;
        expected.push((task, operation_id));
    }

    let identity = authenticated_identity();
    let first = store
        .delivery_recovery_batch(&DeliveryRecoveryQuery::first(identity.clone()))
        .await
        .unwrap();
    assert_eq!(first.entries.len(), MAX_DELIVERY_RECOVERY_BATCH);
    let cursor = first.next_cursor.expect("one later entry remains");
    assert!(!format!("{cursor:?}").contains(COMMON_IDENTITY));

    let different_identity =
        DirectoryIdentity::try_new(DIRECTORY_IDENTITY_ALGORITHM_V1, ALTERNATE_IDENTITY).unwrap();
    assert_eq!(
        DeliveryRecoveryQuery::try_after(different_identity, cursor.clone()).unwrap_err(),
        DeliveryRecoveryQueryError::CursorIdentityMismatch
    );

    // Moving the operation at the cursor from PreflightPending through Accepted
    // to source ObjectPending must not move its immutable pagination key.
    let (cursor_task, cursor_operation) = &expected[MAX_DELIVERY_RECOVERY_BATCH - 1];
    let accept_command = accept_existing(&store, cursor_task, *cursor_operation).await;
    assert!(matches!(
        store
            .create_delivery_source(CreateDeliverySourceRequest::try_new(accept_command).unwrap())
            .await
            .unwrap(),
        CreateDeliverySourceOutcome::Created(_)
    ));

    let second_query = DeliveryRecoveryQuery::try_after(identity, cursor).unwrap();
    assert!(!format!("{second_query:?}").contains(COMMON_IDENTITY));
    let second = store.delivery_recovery_batch(&second_query).await.unwrap();
    assert_eq!(second.entries.len(), 1);
    assert!(second.next_cursor.is_none());

    let recovered = first
        .entries
        .into_iter()
        .chain(second.entries)
        .map(|entry| entry.identity.task_id())
        .collect::<Vec<_>>();
    assert_eq!(
        recovered,
        expected
            .into_iter()
            .map(|(task, _)| task.id)
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn caller_authenticated_identity_filters_without_store_side_authentication() {
    let store = crate::support::seeded_store().await;
    let (common_task, _) =
        pending_preflight(&store, "codex/recovery-common", COMMON_IDENTITY).await;
    let (alternate_task, _) =
        pending_preflight(&store, "codex/recovery-alternate", ALTERNATE_IDENTITY).await;

    let common = store
        .delivery_recovery_batch(&DeliveryRecoveryQuery::first(authenticated_identity()))
        .await
        .unwrap();
    assert_eq!(common.entries.len(), 1);
    assert_eq!(common.entries[0].identity.task_id(), common_task.id);

    let alternate_identity =
        DirectoryIdentity::try_new(DIRECTORY_IDENTITY_ALGORITHM_V1, ALTERNATE_IDENTITY).unwrap();
    let alternate = store
        .delivery_recovery_batch(&DeliveryRecoveryQuery::first(alternate_identity.clone()))
        .await
        .unwrap();
    assert_eq!(alternate.entries.len(), 1);
    assert_eq!(alternate.entries[0].identity.task_id(), alternate_task.id);
    assert_eq!(
        alternate.entries[0].expected_common_git_identity,
        alternate_identity
    );

    let startup = store.startup_delivery_ownership().await.unwrap();
    assert_eq!(startup.len(), 2);
}
