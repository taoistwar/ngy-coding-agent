use std::str::FromStr;

use coding_agent_domain::ClientRequestId;
use coding_agent_store::{
    BindMergePreflightInputsRequest, CreatePreflightOutcome, DeliveryAcceptedOperationState,
    DeliveryCommand, DeliveryCommandKind, DeliveryCommandLookup, DeliveryError, DeliveryVersion,
    GitCommitOid, GitTreeOid, MarkPreflightStaleOutcome, MarkPreflightStaleRequest,
    MergeOperationState, MergeTransitionOutcome, PreflightStaleReason, StoreError,
};

#[tokio::test]
async fn typed_read_only_lookup_is_missing_then_returns_the_exact_durable_receipt() {
    let (store, task) = eligible_fixture().await;
    let request = preflight_request(&task, ClientRequestId::new());
    let command = DeliveryCommand::Preflight(request.command().clone());
    assert_eq!(
        store.lookup_delivery_command(&command).await.unwrap(),
        DeliveryCommandLookup::Missing
    );

    let created = receipt(store.create_merge_preflight(request).await.unwrap());
    let receipt_debug = format!("{created:?}");
    assert!(receipt_debug.contains("<redacted>"));
    assert!(!receipt_debug.contains(created.canonical_request_hash.as_str()));
    assert_eq!(
        store.lookup_delivery_command(&command).await.unwrap(),
        DeliveryCommandLookup::Existing(created)
    );
}

#[tokio::test]
async fn merge_receipt_replay_ignores_a_valid_cleanup_operation_with_the_same_uuid() {
    let (store, task) = eligible_fixture().await;
    let request = preflight_request(&task, ClientRequestId::new());
    let created = receipt(store.create_merge_preflight(request.clone()).await.unwrap());
    mark_preflight_ready(&store, created.operation_id).await;
    accept_merge(&store, &task, created.operation_id).await;
    create_committed_source(&store, &task, created.operation_id).await;
    finish_merged_delivery(&store, &task, created.operation_id).await;
    create_worktree_cleanup_with_operation_id(&store, &task, created.operation_id).await;

    assert_eq!(
        store.create_merge_preflight(request).await.unwrap(),
        CreatePreflightOutcome::Existing(created)
    );
}

use crate::receipt_fixtures::{eligible_fixture, preflight_request, receipt, row_counts};
use crate::support;
use crate::support::delivery::eligibility::{
    CANDIDATE_TREE, PREFLIGHT_SOURCE, SOURCE_COMMIT, TARGET_CONFIG_DIGEST, TARGET_SECURITY_DIGEST,
    accept_merge, create_committed_source, create_merged_delivery,
    create_worktree_cleanup_with_operation_id, fail_accepted_merge, finish_merged_delivery,
    finish_preflight_terminal, insert_preflight, mark_preflight_ready, try_accept_merge_ready,
};

#[tokio::test]
async fn first_preflight_and_receipt_are_atomic_and_exact_replay_uses_historical_tuple() {
    let (store, task) = eligible_fixture().await;
    let request = preflight_request(&task, ClientRequestId::new());

    let created = match store.create_merge_preflight(request.clone()).await.unwrap() {
        CreatePreflightOutcome::Created(receipt) => receipt,
        other => panic!("expected created preflight, got {other:?}"),
    };
    assert_eq!(created.command_kind, DeliveryCommandKind::Preflight);
    assert_eq!(
        created.accepted_operation_version,
        DeliveryVersion::initial()
    );
    assert_eq!(
        created.accepted_operation_state,
        DeliveryAcceptedOperationState::PreflightPending
    );
    assert_eq!(row_counts(&store).await, (1, 1, 1));

    mark_preflight_ready(&store, created.operation_id).await;
    assert_eq!(row_counts(&store).await, (1, 3, 1));
    let replayed = match store.create_merge_preflight(request).await.unwrap() {
        CreatePreflightOutcome::Existing(receipt) => receipt,
        other => panic!("expected historical replay, got {other:?}"),
    };
    assert_eq!(replayed, created);
    assert_eq!(row_counts(&store).await, (1, 3, 1));
    let current: (String, i64) =
        sqlx::query_as("SELECT state, version FROM task_merge_operations WHERE operation_id = ?")
            .bind(created.operation_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(current, ("preflight_ready".to_owned(), 3));
}

#[tokio::test]
async fn concurrent_same_uuid_is_one_created_one_existing_with_one_durable_aggregate() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    support::register_repository(&fixture.store, "delivery-receipt-race").await;
    let (store, task) = crate::support::delivery::eligibility::approved_task_on_store(
        fixture.store.clone(),
        "codex/task-receipt-race",
        0,
    )
    .await;
    let request = preflight_request(&task, ClientRequestId::new());
    let first_store = store.clone();
    let second_store = store.clone();
    let first_request = request.clone();
    let (first, second) = tokio::join!(
        async move {
            first_store
                .create_merge_preflight(first_request)
                .await
                .unwrap()
        },
        async move { second_store.create_merge_preflight(request).await.unwrap() }
    );
    let outcomes = [first, second];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, CreatePreflightOutcome::Created(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, CreatePreflightOutcome::Existing(_)))
            .count(),
        1
    );
    assert_eq!(receipt(outcomes[0].clone()), receipt(outcomes[1].clone()));
    assert_eq!(row_counts(&store).await, (1, 1, 1));
}

#[tokio::test]
async fn concurrent_distinct_uuids_create_one_preflight_and_reject_the_other_as_in_progress() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    support::register_repository(&fixture.store, "delivery-distinct-request-race").await;
    let (store, task) = crate::support::delivery::eligibility::approved_task_on_store(
        fixture.store.clone(),
        "codex/task-distinct-request-race",
        0,
    )
    .await;
    let first_store = store.clone();
    let second_store = store.clone();
    let first_request = preflight_request(&task, ClientRequestId::new());
    let second_request = preflight_request(&task, ClientRequestId::new());
    let (first, second) = tokio::join!(
        async move { first_store.create_merge_preflight(first_request).await },
        async move { second_store.create_merge_preflight(second_request).await }
    );
    let outcomes = [first, second];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Ok(CreatePreflightOutcome::Created(_))))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Err(StoreError::DeliveryOperationInProgress)))
            .count(),
        1
    );
    assert_eq!(row_counts(&store).await, (1, 1, 1));
}

#[tokio::test]
async fn concurrent_same_uuid_with_different_hash_creates_once_and_conflicts_once() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    support::register_repository(&fixture.store, "delivery-request-hash-race").await;
    let (store, task) = crate::support::delivery::eligibility::approved_task_on_store(
        fixture.store.clone(),
        "codex/task-request-hash-race",
        0,
    )
    .await;
    let client_request_id = ClientRequestId::new();
    let first_request = preflight_request(&task, client_request_id);
    let second_request = coding_agent_store::CreatePreflightRequest::try_new(
        coding_agent_store::PreflightCommandRequest::try_new(
            client_request_id,
            task.id,
            "refs/heads/release".parse().unwrap(),
            "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".parse().unwrap(),
        )
        .unwrap(),
        coding_agent_store::DirectoryIdentity::try_new(
            "directory_identity_v1",
            "c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1",
        )
        .unwrap(),
        coding_agent_store::DirectoryIdentity::try_new(
            "directory_identity_v1",
            "d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2",
        )
        .unwrap(),
        "e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3"
            .parse()
            .unwrap(),
        TARGET_CONFIG_DIGEST.parse().unwrap(),
        TARGET_SECURITY_DIGEST.parse().unwrap(),
    )
    .unwrap();
    let first_store = store.clone();
    let second_store = store.clone();
    let (first, second) = tokio::join!(
        async move { first_store.create_merge_preflight(first_request).await },
        async move { second_store.create_merge_preflight(second_request).await }
    );
    let outcomes = [first, second];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Ok(CreatePreflightOutcome::Created(_))))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Err(StoreError::IdempotencyConflict)))
            .count(),
        1
    );
    assert_eq!(row_counts(&store).await, (1, 1, 1));
}

#[tokio::test]
async fn ready_supersede_and_future_accept_cas_have_exactly_one_version_winner() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    support::register_repository(&fixture.store, "delivery-ready-cas-race").await;
    let (store, task) = crate::support::delivery::eligibility::approved_task_on_store(
        fixture.store.clone(),
        "codex/task-ready-cas-race",
        0,
    )
    .await;
    let first = receipt(
        store
            .create_merge_preflight(preflight_request(&task, ClientRequestId::new()))
            .await
            .unwrap(),
    );
    mark_preflight_ready(&store, first.operation_id).await;
    let create_store = store.clone();
    let accept_store = store.clone();
    let create_task = task.clone();
    let accept_task = task.clone();
    let (replacement, accepted) = tokio::join!(
        async move {
            create_store
                .create_merge_preflight(preflight_request(&create_task, ClientRequestId::new()))
                .await
        },
        async move { try_accept_merge_ready(&accept_store, &accept_task, first.operation_id).await }
    );
    let old: (String, i64) =
        sqlx::query_as("SELECT state, version FROM task_merge_operations WHERE operation_id = ?")
            .bind(first.operation_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    match (replacement, accepted) {
        (Ok(CreatePreflightOutcome::Created(_)), false) => {
            assert_eq!(old, ("superseded".to_owned(), 4));
            assert_eq!(row_counts(&store).await, (2, 5, 2));
        }
        (Err(StoreError::DeliveryOperationInProgress), true) => {
            assert_eq!(old, ("accepted".to_owned(), 4));
            assert_eq!(row_counts(&store).await, (1, 4, 2));
        }
        other => panic!("expected exactly one Ready-version CAS winner, got {other:?}"),
    }
}

#[tokio::test]
async fn global_uuid_conflict_wins_before_task_or_current_state_classification() {
    let (store, task) = eligible_fixture().await;
    let client_request_id = ClientRequestId::new();
    let original = preflight_request(&task, client_request_id);
    let created = receipt(store.create_merge_preflight(original).await.unwrap());
    let changed = coding_agent_store::CreatePreflightRequest::try_new(
        coding_agent_store::PreflightCommandRequest::try_new(
            client_request_id,
            task.id,
            "refs/heads/release".parse().unwrap(),
            "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".parse().unwrap(),
        )
        .unwrap(),
        coding_agent_store::DirectoryIdentity::try_new(
            "directory_identity_v1",
            "c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1",
        )
        .unwrap(),
        coding_agent_store::DirectoryIdentity::try_new(
            "directory_identity_v1",
            "d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2",
        )
        .unwrap(),
        "e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3"
            .parse()
            .unwrap(),
        TARGET_CONFIG_DIGEST.parse().unwrap(),
        TARGET_SECURITY_DIGEST.parse().unwrap(),
    )
    .unwrap();
    assert!(matches!(
        store.create_merge_preflight(changed).await.unwrap_err(),
        StoreError::IdempotencyConflict
    ));
    assert_eq!(row_counts(&store).await, (1, 1, 1));

    mark_preflight_ready(&store, created.operation_id).await;
    accept_merge(&store, &task, created.operation_id).await;
    let accept_id: String = sqlx::query_scalar(
        "SELECT accept_receipt_id FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(created.operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    let cross_action = preflight_request(&task, ClientRequestId::from_str(&accept_id).unwrap());
    assert!(matches!(
        store
            .create_merge_preflight(cross_action)
            .await
            .unwrap_err(),
        StoreError::IdempotencyConflict
    ));
    assert_eq!(row_counts(&store).await, (1, 4, 2));
}

#[tokio::test]
async fn a_new_uuid_cannot_replace_pending_but_ready_is_superseded_atomically() {
    let (store, task) = eligible_fixture().await;
    let first = receipt(
        store
            .create_merge_preflight(preflight_request(&task, ClientRequestId::new()))
            .await
            .unwrap(),
    );
    assert!(matches!(
        store
            .create_merge_preflight(preflight_request(&task, ClientRequestId::new()))
            .await
            .unwrap_err(),
        StoreError::DeliveryOperationInProgress
    ));
    assert_eq!(row_counts(&store).await, (1, 1, 1));

    mark_preflight_ready(&store, first.operation_id).await;
    sqlx::query(
        "CREATE TRIGGER reject_replacement_preflight BEFORE INSERT ON task_merge_operations \
         BEGIN SELECT RAISE(ABORT, 'fault after supersede'); END",
    )
    .execute(store.pool())
    .await
    .unwrap();
    let failed = store
        .create_merge_preflight(preflight_request(&task, ClientRequestId::new()))
        .await;
    assert!(matches!(failed, Err(StoreError::Database(_))));
    let rolled_back: (String, i64) =
        sqlx::query_as("SELECT state, version FROM task_merge_operations WHERE operation_id = ?")
            .bind(first.operation_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(rolled_back, ("preflight_ready".to_owned(), 3));
    assert_eq!(row_counts(&store).await, (1, 3, 1));
    sqlx::query("DROP TRIGGER reject_replacement_preflight")
        .execute(store.pool())
        .await
        .unwrap();

    let replacement = receipt(
        store
            .create_merge_preflight(preflight_request(&task, ClientRequestId::new()))
            .await
            .unwrap(),
    );
    assert_ne!(replacement.operation_id, first.operation_id);
    let old: (String, i64) =
        sqlx::query_as("SELECT state, version FROM task_merge_operations WHERE operation_id = ?")
            .bind(first.operation_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(old, ("superseded".to_owned(), 4));
    assert_eq!(row_counts(&store).await, (2, 5, 2));
}

#[tokio::test]
async fn receipt_insert_failure_rolls_back_new_pending_and_ready_supersede() {
    let (store, task) = eligible_fixture().await;
    let first = receipt(
        store
            .create_merge_preflight(preflight_request(&task, ClientRequestId::new()))
            .await
            .unwrap(),
    );
    mark_preflight_ready(&store, first.operation_id).await;
    sqlx::query(
        "CREATE TRIGGER reject_delivery_receipt BEFORE INSERT ON task_delivery_command_receipts \
         BEGIN SELECT RAISE(ABORT, 'fault before receipt'); END",
    )
    .execute(store.pool())
    .await
    .unwrap();
    let outcome = store
        .create_merge_preflight(preflight_request(&task, ClientRequestId::new()))
        .await;
    assert!(matches!(outcome, Err(StoreError::Database(_))));
    assert_eq!(row_counts(&store).await, (1, 3, 1));
    let old: (String, i64) =
        sqlx::query_as("SELECT state, version FROM task_merge_operations WHERE operation_id = ?")
            .bind(first.operation_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(old, ("preflight_ready".to_owned(), 3));
}

#[tokio::test]
async fn exact_replay_fails_closed_when_its_historical_journal_tuple_is_missing() {
    let (store, task) = eligible_fixture().await;
    let request = preflight_request(&task, ClientRequestId::new());
    let created = receipt(store.create_merge_preflight(request.clone()).await.unwrap());
    mark_preflight_ready(&store, created.operation_id).await;
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER task_delivery_operation_transitions_no_delete")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "DELETE FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 1",
    )
    .bind(created.operation_id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);
    assert!(matches!(
        store.create_merge_preflight(request).await.unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
}

#[tokio::test]
async fn exact_replay_audits_hidden_mutually_exclusive_task_merge_slots() {
    let (store, task) = eligible_fixture().await;
    let request = preflight_request(&task, ClientRequestId::new());
    let first = receipt(store.create_merge_preflight(request.clone()).await.unwrap());
    finish_preflight_terminal(&store, first.operation_id, MergeOperationState::Rejected).await;
    let second = receipt(
        store
            .create_merge_preflight(preflight_request(&task, ClientRequestId::new()))
            .await
            .unwrap(),
    );
    assert_ne!(first.operation_id, second.operation_id);

    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::raw_sql(
        "DROP TRIGGER task_merge_operations_immutable_on_update; \
         DROP TRIGGER task_merge_operations_transition_on_update; \
         DROP TRIGGER task_merge_operations_source_consistency_on_update; \
         DROP TRIGGER task_merge_operations_source_reconciliation_on_update; \
         DROP TRIGGER task_merge_operations_journal_on_update; \
         DROP TRIGGER task_delivery_operation_transitions_no_update;",
    )
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_merge_operations \
         SET state = 'reconciliation_required', \
             failure_code = 'DELIVERY_RECONCILIATION_REQUIRED' \
         WHERE operation_id = ?",
    )
    .bind(first.operation_id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions \
         SET to_state = 'reconciliation_required', \
             failure_code = 'DELIVERY_RECONCILIATION_REQUIRED' \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 3",
    )
    .bind(first.operation_id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);

    assert!(matches!(
        store.create_merge_preflight(request).await,
        Err(StoreError::InvariantViolation(_))
    ));
}

#[tokio::test]
async fn exact_replay_rejects_a_historical_tuple_with_an_action_invalid_accepted_version() {
    let (store, task) = eligible_fixture().await;
    let request = preflight_request(&task, ClientRequestId::new());
    let created = receipt(store.create_merge_preflight(request.clone()).await.unwrap());
    mark_preflight_ready(&store, created.operation_id).await;
    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER task_delivery_operation_transitions_no_update")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER task_delivery_command_receipts_no_update")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions \
         SET from_state = 'absent', to_state = 'preflight_pending' \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 3",
    )
    .bind(created.operation_id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_delivery_command_receipts SET accepted_operation_version = 3 \
         WHERE client_request_id = ?",
    )
    .bind(created.client_request_id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);

    assert!(matches!(
        store.create_merge_preflight(request).await.unwrap_err(),
        StoreError::InvariantViolation(_)
    ));
}

#[tokio::test]
async fn stale_is_ready_only_exact_version_cas_and_releases_the_open_slot() {
    let (store, task) = eligible_fixture().await;
    let created = receipt(
        store
            .create_merge_preflight(preflight_request(&task, ClientRequestId::new()))
            .await
            .unwrap(),
    );
    let pending_conflict = store
        .mark_merge_preflight_stale(
            MarkPreflightStaleRequest::try_new(
                task.id,
                created.operation_id,
                DeliveryVersion::initial(),
                PreflightStaleReason::TargetHeadChanged,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(pending_conflict, MarkPreflightStaleOutcome::Conflict);

    mark_preflight_ready(&store, created.operation_id).await;
    let applied = store
        .mark_merge_preflight_stale(
            MarkPreflightStaleRequest::try_new(
                task.id,
                created.operation_id,
                DeliveryVersion::try_new(3).unwrap(),
                PreflightStaleReason::TargetHeadChanged,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        applied,
        MarkPreflightStaleOutcome::Applied {
            operation_id: created.operation_id,
            version: DeliveryVersion::try_new(4).unwrap(),
            state: MergeOperationState::Stale,
            reason: PreflightStaleReason::TargetHeadChanged,
        }
    );
    let stored: (String, Option<String>, i64) = sqlx::query_as(
        "SELECT state, failure_code, version FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(created.operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(
        stored,
        (
            "stale".to_owned(),
            Some("TARGET_HEAD_CHANGED".to_owned()),
            4
        )
    );
    assert_eq!(
        store
            .mark_merge_preflight_stale(
                MarkPreflightStaleRequest::try_new(
                    task.id,
                    created.operation_id,
                    DeliveryVersion::try_new(3).unwrap(),
                    PreflightStaleReason::TargetHeadChanged,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        MarkPreflightStaleOutcome::Existing {
            operation_id: created.operation_id,
            version: DeliveryVersion::try_new(4).unwrap(),
            state: MergeOperationState::Stale,
            reason: PreflightStaleReason::TargetHeadChanged,
        }
    );

    let replacement = store
        .create_merge_preflight(preflight_request(&task, ClientRequestId::new()))
        .await
        .unwrap();
    assert!(matches!(replacement, CreatePreflightOutcome::Created(_)));
}

#[tokio::test]
async fn stale_cas_is_bound_to_the_exact_task_and_all_typed_reason_codes() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    support::register_repository(&fixture.store, "delivery-stale-task-binding").await;
    let (store, task) = crate::support::delivery::eligibility::approved_task_on_store(
        fixture.store.clone(),
        "codex/task-stale-binding",
        0,
    )
    .await;
    let (_, other_task) = crate::support::delivery::eligibility::approved_task_on_store(
        store.clone(),
        "codex/task-stale-binding-other",
        0,
    )
    .await;
    let created = receipt(
        store
            .create_merge_preflight(preflight_request(&task, ClientRequestId::new()))
            .await
            .unwrap(),
    );
    mark_preflight_ready(&store, created.operation_id).await;
    let before_wrong_task = row_counts(&store).await;
    assert_eq!(
        store
            .mark_merge_preflight_stale(
                MarkPreflightStaleRequest::try_new(
                    other_task.id,
                    created.operation_id,
                    DeliveryVersion::try_new(3).unwrap(),
                    PreflightStaleReason::EvidenceStale,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        MarkPreflightStaleOutcome::Conflict
    );
    assert_eq!(row_counts(&store).await, before_wrong_task);
    let unchanged: (String, i64) =
        sqlx::query_as("SELECT state, version FROM task_merge_operations WHERE operation_id = ?")
            .bind(created.operation_id.to_string())
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(unchanged, ("preflight_ready".to_owned(), 3));
    assert!(matches!(
        store
            .mark_merge_preflight_stale(
                MarkPreflightStaleRequest::try_new(
                    task.id,
                    created.operation_id,
                    DeliveryVersion::try_new(3).unwrap(),
                    PreflightStaleReason::TargetHeadChanged,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        MarkPreflightStaleOutcome::Applied { .. }
    ));
    let before_wrong_task_replay = row_counts(&store).await;
    assert_eq!(
        store
            .mark_merge_preflight_stale(
                MarkPreflightStaleRequest::try_new(
                    other_task.id,
                    created.operation_id,
                    DeliveryVersion::try_new(3).unwrap(),
                    PreflightStaleReason::TargetHeadChanged,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        MarkPreflightStaleOutcome::Conflict
    );
    assert_eq!(row_counts(&store).await, before_wrong_task_replay);

    for (reason, failure_code) in [
        (
            PreflightStaleReason::TargetHeadChanged,
            "TARGET_HEAD_CHANGED",
        ),
        (
            PreflightStaleReason::EvidenceStale,
            "DELIVERY_EVIDENCE_STALE",
        ),
        (
            PreflightStaleReason::SourceChanged,
            "DELIVERY_SOURCE_CHANGED",
        ),
    ] {
        let (store, task) = eligible_fixture().await;
        let created = receipt(
            store
                .create_merge_preflight(preflight_request(&task, ClientRequestId::new()))
                .await
                .unwrap(),
        );
        mark_preflight_ready(&store, created.operation_id).await;
        let applied = store
            .mark_merge_preflight_stale(
                MarkPreflightStaleRequest::try_new(
                    task.id,
                    created.operation_id,
                    DeliveryVersion::try_new(3).unwrap(),
                    reason,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            applied,
            MarkPreflightStaleOutcome::Applied {
                operation_id: created.operation_id,
                version: DeliveryVersion::try_new(4).unwrap(),
                state: MergeOperationState::Stale,
                reason,
            }
        );
        let stored: (String, Option<String>, i64) = sqlx::query_as(
            "SELECT state, failure_code, version \
             FROM task_merge_operations WHERE operation_id = ?",
        )
        .bind(created.operation_id.to_string())
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(
            stored,
            ("stale".to_owned(), Some(failure_code.to_owned()), 4)
        );
        let journal: (String, String, Option<String>, i64) = sqlx::query_as(
            "SELECT from_state, to_state, failure_code, entity_version \
             FROM task_delivery_operation_transitions \
             WHERE entity_kind = 'merge_operation' AND entity_id = ? \
               AND entity_version = 4",
        )
        .bind(created.operation_id.to_string())
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(
            journal,
            (
                "preflight_ready".to_owned(),
                "stale".to_owned(),
                Some(failure_code.to_owned()),
                4,
            )
        );

        let before_repeat = row_counts(&store).await;
        assert_eq!(
            store
                .mark_merge_preflight_stale(
                    MarkPreflightStaleRequest::try_new(
                        task.id,
                        created.operation_id,
                        DeliveryVersion::try_new(3).unwrap(),
                        reason,
                    )
                    .unwrap(),
                )
                .await
                .unwrap(),
            MarkPreflightStaleOutcome::Existing {
                operation_id: created.operation_id,
                version: DeliveryVersion::try_new(4).unwrap(),
                state: MergeOperationState::Stale,
                reason,
            }
        );
        assert_eq!(row_counts(&store).await, before_repeat);
    }
}

#[tokio::test]
async fn stale_replay_rejects_wrong_reason_and_version_without_writes() {
    let (store, task) = eligible_fixture().await;
    let created = receipt(
        store
            .create_merge_preflight(preflight_request(&task, ClientRequestId::new()))
            .await
            .unwrap(),
    );
    mark_preflight_ready(&store, created.operation_id).await;
    let applied = store
        .mark_merge_preflight_stale(
            MarkPreflightStaleRequest::try_new(
                task.id,
                created.operation_id,
                DeliveryVersion::try_new(3).unwrap(),
                PreflightStaleReason::TargetHeadChanged,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(applied, MarkPreflightStaleOutcome::Applied { .. }));
    let before = row_counts(&store).await;

    for request in [
        MarkPreflightStaleRequest::try_new(
            task.id,
            created.operation_id,
            DeliveryVersion::try_new(3).unwrap(),
            PreflightStaleReason::EvidenceStale,
        )
        .unwrap(),
        MarkPreflightStaleRequest::try_new(
            task.id,
            created.operation_id,
            DeliveryVersion::initial(),
            PreflightStaleReason::TargetHeadChanged,
        )
        .unwrap(),
    ] {
        assert_eq!(
            store.mark_merge_preflight_stale(request).await.unwrap(),
            MarkPreflightStaleOutcome::Conflict
        );
        assert_eq!(row_counts(&store).await, before);
    }
}

#[tokio::test]
async fn stale_exact_replay_fails_closed_when_the_current_journal_tuple_is_corrupt() {
    let (store, task) = eligible_fixture().await;
    let created = receipt(
        store
            .create_merge_preflight(preflight_request(&task, ClientRequestId::new()))
            .await
            .unwrap(),
    );
    mark_preflight_ready(&store, created.operation_id).await;
    let request = MarkPreflightStaleRequest::try_new(
        task.id,
        created.operation_id,
        DeliveryVersion::try_new(3).unwrap(),
        PreflightStaleReason::TargetHeadChanged,
    )
    .unwrap();
    assert!(matches!(
        store.mark_merge_preflight_stale(request).await.unwrap(),
        MarkPreflightStaleOutcome::Applied { .. }
    ));

    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("DROP TRIGGER task_delivery_operation_transitions_no_update")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_delivery_operation_transitions \
         SET failure_code = 'DELIVERY_EVIDENCE_STALE' \
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 4",
    )
    .bind(created.operation_id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);

    assert!(matches!(
        store.mark_merge_preflight_stale(request).await,
        Err(StoreError::InvariantViolation(_))
    ));
}

#[tokio::test]
async fn a_committed_source_requires_the_new_preflight_to_bind_its_exact_commit_and_provenance() {
    let (store, task) = eligible_fixture().await;
    let first = receipt(
        store
            .create_merge_preflight(preflight_request(&task, ClientRequestId::new()))
            .await
            .unwrap(),
    );
    mark_preflight_ready(&store, first.operation_id).await;
    accept_merge(&store, &task, first.operation_id).await;
    create_committed_source(&store, &task, first.operation_id).await;
    fail_accepted_merge(&store, &task, first.operation_id).await;
    let second = receipt(
        store
            .create_merge_preflight(preflight_request(&task, ClientRequestId::new()))
            .await
            .unwrap(),
    );
    let before_bind = row_counts(&store).await;
    let wrong_source = BindMergePreflightInputsRequest::try_new(
        task.id,
        second.operation_id,
        DeliveryVersion::initial(),
        GitTreeOid::from_str(CANDIDATE_TREE).unwrap(),
        GitCommitOid::from_str(PREFLIGHT_SOURCE).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        store.bind_merge_preflight_inputs(wrong_source).await,
        Err(StoreError::Delivery(DeliveryError::InvalidCommandRequest))
    ));
    assert_eq!(row_counts(&store).await, before_bind);

    let exact_source = BindMergePreflightInputsRequest::try_new(
        task.id,
        second.operation_id,
        DeliveryVersion::initial(),
        GitTreeOid::from_str(CANDIDATE_TREE).unwrap(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        store
            .bind_merge_preflight_inputs(exact_source)
            .await
            .unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));
}

#[tokio::test]
async fn merged_side_effect_and_reconciliation_states_reject_without_writes() {
    let (store, task) = eligible_fixture().await;
    let snapshot = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let operation_id = coding_agent_store::DeliveryOperationId::new();
    insert_preflight(
        &store,
        &task,
        snapshot.evidence_identity.as_ref().unwrap(),
        operation_id,
    )
    .await;
    mark_preflight_ready(&store, operation_id).await;
    accept_merge(&store, &task, operation_id).await;
    let before = row_counts(&store).await;
    assert!(matches!(
        store
            .create_merge_preflight(preflight_request(&task, ClientRequestId::new()))
            .await
            .unwrap_err(),
        StoreError::DeliveryOperationInProgress
    ));
    assert_eq!(row_counts(&store).await, before);

    let (store, task) = eligible_fixture().await;
    let snapshot = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    create_merged_delivery(&store, &task, snapshot.evidence_identity.as_ref().unwrap()).await;
    let before = row_counts(&store).await;
    assert!(matches!(
        store
            .create_merge_preflight(preflight_request(&task, ClientRequestId::new()))
            .await
            .unwrap_err(),
        StoreError::TaskNotMergeEligible
    ));
    assert_eq!(row_counts(&store).await, before);

    let (store, task) = eligible_fixture().await;
    let snapshot = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    let operation_id = coding_agent_store::DeliveryOperationId::new();
    insert_preflight(
        &store,
        &task,
        snapshot.evidence_identity.as_ref().unwrap(),
        operation_id,
    )
    .await;
    sqlx::query(
        "UPDATE task_merge_operations SET state = 'reconciliation_required', \
             failure_code = 'DELIVERY_RECONCILIATION_REQUIRED', version = 3, updated_at = ? \
         WHERE operation_id = ?",
    )
    .bind(crate::support::delivery::eligibility::DELIVERY_TIMESTAMP)
    .bind(operation_id.to_string())
    .execute(store.pool())
    .await
    .unwrap();
    let before = row_counts(&store).await;
    assert!(matches!(
        store
            .create_merge_preflight(preflight_request(&task, ClientRequestId::new()))
            .await
            .unwrap_err(),
        StoreError::DeliveryReconciliationRequired
    ));
    assert_eq!(row_counts(&store).await, before);
}
