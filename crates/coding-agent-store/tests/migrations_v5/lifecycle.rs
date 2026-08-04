use super::support;

#[tokio::test]
async fn valid_delivery_lifecycle_closes_receipts_journal_and_disposition() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();

    support::delivery::merge::seed_merged_delivery(fixture.store.pool())
        .await
        .unwrap();

    let merge: (String, i64, Option<String>) = sqlx::query_as(
        "SELECT state, version, merged_disposition_task_id
         FROM task_merge_operations WHERE operation_id = ?",
    )
    .bind(support::delivery::MERGE_OPERATION_ID)
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(
        merge,
        (
            "merged".to_owned(),
            5,
            Some(support::delivery::TASK_ID.to_owned())
        )
    );

    let counts: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT COUNT(*) FROM task_delivery_sources),
             (SELECT COUNT(*) FROM task_artifact_dispositions),
             (SELECT COUNT(*) FROM task_delivery_command_receipts),
             (SELECT COUNT(*) FROM task_delivery_operation_transitions)",
    )
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(counts, (1, 1, 2, 10));
}

#[tokio::test]
async fn merged_state_and_initial_disposition_commit_as_one_closed_transaction() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let parents =
        support::delivery::parents::seed_eligible_delivery_parents(fixture.store.pool()).await;
    support::delivery::merge::create_preflight(
        fixture.store.pool(),
        parents.final_review_event_id,
        support::delivery::MERGE_OPERATION_ID,
        support::delivery::PREFLIGHT_RECEIPT_ID,
    )
    .await
    .unwrap();
    support::delivery::merge::mark_preflight_ready(
        fixture.store.pool(),
        support::delivery::MERGE_OPERATION_ID,
    )
    .await
    .unwrap();
    support::delivery::merge::accept_merge(
        fixture.store.pool(),
        support::delivery::MERGE_OPERATION_ID,
    )
    .await
    .unwrap();
    support::delivery::merge::create_committed_source(fixture.store.pool())
        .await
        .unwrap();
    support::delivery::merge::mark_merge_pending(fixture.store.pool())
        .await
        .unwrap();

    let error = sqlx::query(
        "UPDATE task_merge_operations
         SET state = 'merged', merged_disposition_task_id = ?,
             version = 5, updated_at = ?
         WHERE operation_id = ?",
    )
    .bind(support::delivery::TASK_ID)
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(fixture.store.pool())
    .await
    .unwrap_err();
    assert!(error.to_string().contains("FOREIGN KEY constraint failed"));

    let state: (String, i64) =
        sqlx::query_as("SELECT state, version FROM task_merge_operations WHERE operation_id = ?")
            .bind(support::delivery::MERGE_OPERATION_ID)
            .fetch_one(fixture.store.pool())
            .await
            .unwrap();
    assert_eq!(state, ("merge_pending".to_owned(), 4));
    let disposition_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_artifact_dispositions")
            .fetch_one(fixture.store.pool())
            .await
            .unwrap();
    assert_eq!(disposition_count, 0);
    let merged_journal_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions
         WHERE entity_kind = 'merge_operation' AND entity_id = ? AND entity_version = 5",
    )
    .bind(support::delivery::MERGE_OPERATION_ID)
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(merged_journal_count, 0);

    support::delivery::merge::complete_merge_with_disposition(fixture.store.pool())
        .await
        .unwrap();
}

#[tokio::test]
async fn initial_transition_id_is_the_only_creation_order_key() {
    const EARLIER_UUID: &str = "00000000-0000-4000-8000-000000000001";
    const SECOND_RECEIPT: &str = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let parents =
        support::delivery::parents::seed_eligible_delivery_parents(fixture.store.pool()).await;
    support::delivery::merge::create_preflight(
        fixture.store.pool(),
        parents.final_review_event_id,
        support::delivery::MERGE_OPERATION_ID,
        support::delivery::PREFLIGHT_RECEIPT_ID,
    )
    .await
    .unwrap();
    sqlx::query(
        "UPDATE task_merge_operations
         SET state = 'conflict', failure_code = 'MERGE_CONFLICT', version = 2,
             merge_base_oid = ?, candidate_merge_tree_oid = ?,
             conflict_path_count = 0, updated_at = ?
         WHERE operation_id = ?",
    )
    .bind(support::delivery::MERGE_BASE_OID)
    .bind(support::delivery::MERGE_TREE_OID)
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::MERGE_OPERATION_ID)
    .execute(fixture.store.pool())
    .await
    .unwrap();
    support::delivery::merge::create_preflight(
        fixture.store.pool(),
        parents.final_review_event_id,
        EARLIER_UUID,
        SECOND_RECEIPT,
    )
    .await
    .unwrap();

    let creation_order: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT transition_id, entity_id, transitioned_at
         FROM task_delivery_operation_transitions
         WHERE entity_kind = 'merge_operation' AND entity_version = 1
         ORDER BY transition_id",
    )
    .fetch_all(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(creation_order.len(), 2);
    assert_eq!(creation_order[0].1, support::delivery::MERGE_OPERATION_ID);
    assert_eq!(creation_order[1].1, EARLIER_UUID);
    assert_eq!(creation_order[0].2, creation_order[1].2);
    assert!(creation_order[0].0 < creation_order[1].0);
    let sequence_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_sequence
         WHERE name = 'task_delivery_operation_transitions'",
    )
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(sequence_count, 0);
}

#[tokio::test]
async fn concurrent_preflight_origins_leave_exactly_one_closed_transaction() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    let parents =
        support::delivery::parents::seed_eligible_delivery_parents(fixture.store.pool()).await;
    let pool_a = fixture.store.pool().clone();
    let pool_b = fixture.store.pool().clone();
    let event_id = parents.final_review_event_id;
    let (first, second) = tokio::join!(
        support::delivery::merge::create_preflight(
            &pool_a,
            event_id,
            support::delivery::MERGE_OPERATION_ID,
            support::delivery::PREFLIGHT_RECEIPT_ID,
        ),
        support::delivery::merge::create_preflight(
            &pool_b,
            event_id,
            support::delivery::MERGE_OPERATION_ID,
            "55555555-5555-4555-8555-555555555556",
        )
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT
             (SELECT COUNT(*) FROM task_merge_operations),
             (SELECT COUNT(*) FROM task_delivery_command_receipts),
             (SELECT COUNT(*) FROM task_delivery_operation_transitions)",
    )
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(counts, (1, 1, 1));

    let unique_columns: Vec<String> = sqlx::query_scalar(
        "SELECT group_concat(ii.name, ',')
         FROM pragma_index_list('task_delivery_command_receipts') il
         JOIN pragma_index_info(il.name) ii
         WHERE il.\"unique\" = 1
         GROUP BY il.name ORDER BY il.name",
    )
    .fetch_all(fixture.store.pool())
    .await
    .unwrap();
    assert!(
        unique_columns
            .iter()
            .any(|columns| columns == "command_kind,operation_id")
    );
}
