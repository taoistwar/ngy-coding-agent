use super::support;

#[tokio::test]
async fn cleanup_active_slot_is_global_across_kinds() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    support::delivery::merge::seed_merged_delivery(fixture.store.pool())
        .await
        .unwrap();
    support::delivery::cleanup::create_remove_cleanup(
        fixture.store.pool(),
        support::delivery::CLEANUP_OPERATION_ID,
        support::delivery::CLEANUP_RECEIPT_ID,
    )
    .await
    .unwrap();
    sqlx::query("DROP TRIGGER task_cleanup_operations_ownership_on_insert")
        .execute(fixture.store.pool())
        .await
        .unwrap();

    let competing = support::delivery::cleanup::create_delete_cleanup(fixture.store.pool()).await;
    assert!(
        competing.is_err(),
        "remove and delete cleanup occupied the same active disposition slot"
    );
    let index = super::helpers::normalized_schema_sql(
        fixture.store.pool(),
        "task_cleanup_operations_one_active_disposition",
    )
    .await;
    assert!(index.contains("ON task_cleanup_operations (disposition_task_id)"));
    assert!(!index.contains("disposition_task_id, kind"));
}

#[tokio::test]
async fn cleanup_origin_target_head_is_initial_kind_exact_and_immutable() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    support::delivery::merge::seed_merged_delivery(fixture.store.pool())
        .await
        .unwrap();
    support::delivery::cleanup::create_remove_cleanup(
        fixture.store.pool(),
        support::delivery::CLEANUP_OPERATION_ID,
        support::delivery::CLEANUP_RECEIPT_ID,
    )
    .await
    .unwrap();
    let remove_origin: Option<String> = sqlx::query_scalar(
        "SELECT origin_target_head FROM task_cleanup_operations WHERE operation_id = ?",
    )
    .bind(support::delivery::CLEANUP_OPERATION_ID)
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(remove_origin, None);
    let remove_observations: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_cleanup_target_head_observations \
         WHERE cleanup_operation_id = ?",
    )
    .bind(support::delivery::CLEANUP_OPERATION_ID)
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(remove_observations, 0);

    let delete_fixture = support::file_store().await;
    delete_fixture.store.migrate().await.unwrap();
    support::delivery::merge::seed_merged_delivery(delete_fixture.store.pool())
        .await
        .unwrap();
    support::delivery::cleanup::advance_disposition_to_removed(delete_fixture.store.pool())
        .await
        .unwrap();
    support::delivery::cleanup::create_delete_cleanup(delete_fixture.store.pool())
        .await
        .unwrap();
    let initial: (String, String) = sqlx::query_as(
        "SELECT origin_target_head, expected_target_head FROM task_cleanup_operations \
         WHERE operation_id = ?",
    )
    .bind(support::delivery::DELETE_CLEANUP_OPERATION_ID)
    .fetch_one(delete_fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(initial.0, support::delivery::MERGE_COMMIT_OID);
    assert_eq!(initial.1, support::delivery::MERGE_COMMIT_OID);
    let initial_observation: (i64, String, String) = sqlx::query_as(
        "SELECT operation_version, target_head, observed_at \
         FROM task_cleanup_target_head_observations WHERE cleanup_operation_id = ?",
    )
    .bind(support::delivery::DELETE_CLEANUP_OPERATION_ID)
    .fetch_one(delete_fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(initial_observation.0, 1);
    assert_eq!(initial_observation.1, support::delivery::MERGE_COMMIT_OID);
    assert_eq!(initial_observation.2, support::delivery::TIMESTAMP);

    let unchanged_refresh = sqlx::query(
        "UPDATE task_cleanup_operations SET expected_target_head = ?, version = 2, \
             updated_at = ? WHERE operation_id = ?",
    )
    .bind(support::delivery::MERGE_COMMIT_OID)
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::DELETE_CLEANUP_OPERATION_ID)
    .execute(delete_fixture.store.pool())
    .await;
    assert!(unchanged_refresh.is_err());

    let refreshed_head = "8".repeat(40);
    sqlx::query(
        "UPDATE task_cleanup_operations SET expected_target_head = ?, version = 2, \
             updated_at = ? WHERE operation_id = ?",
    )
    .bind(&refreshed_head)
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::DELETE_CLEANUP_OPERATION_ID)
    .execute(delete_fixture.store.pool())
    .await
    .unwrap();
    let refreshed: (String, String) = sqlx::query_as(
        "SELECT origin_target_head, expected_target_head FROM task_cleanup_operations \
         WHERE operation_id = ?",
    )
    .bind(support::delivery::DELETE_CLEANUP_OPERATION_ID)
    .fetch_one(delete_fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(refreshed.0, support::delivery::MERGE_COMMIT_OID);
    assert_eq!(refreshed.1, refreshed_head);
    let observations: Vec<(i64, String)> = sqlx::query_as(
        "SELECT operation_version, target_head \
         FROM task_cleanup_target_head_observations \
         WHERE cleanup_operation_id = ? ORDER BY operation_version",
    )
    .bind(support::delivery::DELETE_CLEANUP_OPERATION_ID)
    .fetch_all(delete_fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(
        observations,
        vec![
            (1, support::delivery::MERGE_COMMIT_OID.to_owned()),
            (2, refreshed_head.clone()),
        ]
    );

    let duplicate_version = sqlx::query(
        "INSERT INTO task_cleanup_target_head_observations (cleanup_operation_id, \
             operation_version, target_head, observed_at) VALUES (?, 2, ?, ?)",
    )
    .bind(support::delivery::DELETE_CLEANUP_OPERATION_ID)
    .bind("9".repeat(40))
    .bind(support::delivery::TIMESTAMP)
    .execute(delete_fixture.store.pool())
    .await;
    assert!(duplicate_version.is_err());
    let mutate_observation = sqlx::query(
        "UPDATE task_cleanup_target_head_observations SET target_head = ? \
         WHERE cleanup_operation_id = ? AND operation_version = 1",
    )
    .bind("9".repeat(40))
    .bind(support::delivery::DELETE_CLEANUP_OPERATION_ID)
    .execute(delete_fixture.store.pool())
    .await;
    assert!(mutate_observation.is_err());

    let origin_change = sqlx::query(
        "UPDATE task_cleanup_operations SET origin_target_head = ?, version = 3, \
             updated_at = ? WHERE operation_id = ?",
    )
    .bind("9".repeat(40))
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::DELETE_CLEANUP_OPERATION_ID)
    .execute(delete_fixture.store.pool())
    .await;
    assert!(origin_change.is_err());
}

#[tokio::test]
async fn receipt_fields_must_match_the_current_row_and_historical_journal() {
    let cases = [
        ("preflight", 2, "preflight_pending", "preflight_created"),
        ("preflight", 1, "accepted", "preflight_created"),
        ("preflight", 1, "preflight_pending", "merge_accepted"),
        ("accept_merge", 1, "accepted", "merge_accepted"),
    ];
    for (command, version, state, discriminator) in cases {
        let fixture = support::file_store().await;
        fixture.store.migrate().await.unwrap();
        let parents =
            support::delivery::parents::seed_eligible_delivery_parents(fixture.store.pool()).await;

        let mut preflight = support::delivery::PreflightFixture::valid(
            support::delivery::MERGE_OPERATION_ID,
            support::delivery::PREFLIGHT_RECEIPT_ID,
        );
        preflight.command_kind = command;
        preflight.accepted_version = version;
        preflight.accepted_state = state;
        preflight.response_discriminator = discriminator;
        let result = support::delivery::merge::create_preflight_with_fixture(
            fixture.store.pool(),
            parents.final_review_event_id,
            preflight,
        )
        .await;

        assert!(
            result.is_err(),
            "accepted forged receipt case {command}/{version}/{state}/{discriminator}"
        );
        let counts: (i64, i64, i64) = sqlx::query_as(
            "SELECT
                 (SELECT COUNT(*) FROM task_merge_operations),
                 (SELECT COUNT(*) FROM task_delivery_command_receipts),
                 (SELECT COUNT(*) FROM task_delivery_operation_transitions)",
        )
        .fetch_one(fixture.store.pool())
        .await
        .unwrap();
        assert_eq!(counts, (0, 0, 0));
    }
}

#[tokio::test]
async fn each_cleanup_kind_has_one_atomic_origin_receipt() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    support::delivery::merge::seed_merged_delivery(fixture.store.pool())
        .await
        .unwrap();
    support::delivery::cleanup::create_remove_cleanup(
        fixture.store.pool(),
        support::delivery::CLEANUP_OPERATION_ID,
        support::delivery::CLEANUP_RECEIPT_ID,
    )
    .await
    .unwrap();
    let competing = support::delivery::cleanup::create_remove_cleanup(
        fixture.store.pool(),
        support::delivery::SECOND_CLEANUP_OPERATION_ID,
        support::delivery::SECOND_CLEANUP_RECEIPT_ID,
    )
    .await;
    assert!(competing.is_err());

    let cleanup: (String, String, i64, String) = sqlx::query_as(
        "SELECT c.kind, c.state, c.version, r.response_discriminator
         FROM task_cleanup_operations c
         JOIN task_delivery_command_receipts r
           ON r.cleanup_operation_id = c.operation_id
          AND r.client_request_id = c.origin_receipt_id
         WHERE c.operation_id = ?",
    )
    .bind(support::delivery::CLEANUP_OPERATION_ID)
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(
        cleanup,
        (
            "remove_worktree".to_owned(),
            "unlock_pending".to_owned(),
            1,
            "worktree_cleanup_accepted".to_owned()
        )
    );
    let second_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_cleanup_operations WHERE operation_id = ?")
            .bind(support::delivery::SECOND_CLEANUP_OPERATION_ID)
            .fetch_one(fixture.store.pool())
            .await
            .unwrap();
    assert_eq!(second_count, 0);

    let delete_fixture = support::file_store().await;
    delete_fixture.store.migrate().await.unwrap();
    support::delivery::merge::seed_merged_delivery(delete_fixture.store.pool())
        .await
        .unwrap();
    support::delivery::cleanup::advance_disposition_to_removed(delete_fixture.store.pool())
        .await
        .unwrap();
    support::delivery::cleanup::create_delete_cleanup(delete_fixture.store.pool())
        .await
        .unwrap();
    let delete_receipt: (String, String, String) = sqlx::query_as(
        "SELECT command_kind, accepted_operation_state, response_discriminator
         FROM task_delivery_command_receipts WHERE client_request_id = ?",
    )
    .bind(support::delivery::DELETE_CLEANUP_RECEIPT_ID)
    .fetch_one(delete_fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(
        delete_receipt,
        (
            "delete_branch".to_owned(),
            "delete_pending".to_owned(),
            "branch_cleanup_accepted".to_owned()
        )
    );
}

#[tokio::test]
async fn disposition_facts_require_the_matching_cleanup_transition_and_receipt() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    support::delivery::merge::seed_merged_delivery(fixture.store.pool())
        .await
        .unwrap();

    let direct = sqlx::query(
        "UPDATE task_artifact_dispositions
         SET worktree_state = 'retained_unlocked', worktree_version = 2,
             worktree_updated_at = ? WHERE task_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::TASK_ID)
    .execute(fixture.store.pool())
    .await;
    assert!(direct.is_err());

    support::delivery::cleanup::create_remove_cleanup(
        fixture.store.pool(),
        support::delivery::CLEANUP_OPERATION_ID,
        support::delivery::CLEANUP_RECEIPT_ID,
    )
    .await
    .unwrap();
    let disposition_only = sqlx::query(
        "UPDATE task_artifact_dispositions
         SET worktree_state = 'retained_unlocked', worktree_version = 2,
             worktree_cleanup_operation_id = ?,
             worktree_cleanup_operation_version = 2,
             worktree_cleanup_operation_state = 'unlocked_pending_remove',
             worktree_updated_at = ? WHERE task_id = ?",
    )
    .bind(support::delivery::CLEANUP_OPERATION_ID)
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::TASK_ID)
    .execute(fixture.store.pool())
    .await;
    assert!(disposition_only.is_err());

    support::delivery::cleanup::complete_remove_cleanup(fixture.store.pool())
        .await
        .unwrap();
    let wrong_kind_proof = sqlx::query(
        "UPDATE task_artifact_dispositions
         SET branch_state = 'deleted', branch_version = 2,
             branch_cleanup_operation_id = ?,
             branch_cleanup_operation_version = 4,
             branch_cleanup_operation_state = 'completed',
             branch_updated_at = ? WHERE task_id = ?",
    )
    .bind(support::delivery::CLEANUP_OPERATION_ID)
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::TASK_ID)
    .execute(fixture.store.pool())
    .await;
    assert!(wrong_kind_proof.is_err());
    support::delivery::cleanup::create_delete_cleanup(fixture.store.pool())
        .await
        .unwrap();
    let branch_only = sqlx::query(
        "UPDATE task_artifact_dispositions
         SET branch_state = 'deleted', branch_version = 2,
             branch_updated_at = ? WHERE task_id = ?",
    )
    .bind(support::delivery::TIMESTAMP)
    .bind(support::delivery::TASK_ID)
    .execute(fixture.store.pool())
    .await;
    assert!(branch_only.is_err());
    support::delivery::cleanup::complete_delete_cleanup(fixture.store.pool())
        .await
        .unwrap();

    let facts: (String, String) = sqlx::query_as(
        "SELECT worktree_state, branch_state
         FROM task_artifact_dispositions WHERE task_id = ?",
    )
    .bind(support::delivery::TASK_ID)
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(facts, ("removed".to_owned(), "deleted".to_owned()));
}

#[tokio::test]
async fn delete_cleanup_target_ref_is_the_merged_target_but_head_may_be_fresh() {
    let fixture = support::file_store().await;
    fixture.store.migrate().await.unwrap();
    support::delivery::merge::seed_merged_delivery(fixture.store.pool())
        .await
        .unwrap();
    support::delivery::cleanup::advance_disposition_to_removed(fixture.store.pool())
        .await
        .unwrap();

    let wrong_target = support::delivery::cleanup::create_delete_cleanup_for_target(
        fixture.store.pool(),
        "refs/heads/release",
        support::delivery::MERGE_COMMIT_OID,
    )
    .await;
    assert!(wrong_target.is_err());
    support::delivery::cleanup::create_delete_cleanup_for_target(
        fixture.store.pool(),
        support::delivery::TARGET_BRANCH,
        support::delivery::MERGE_COMMIT_OID,
    )
    .await
    .unwrap();
}
