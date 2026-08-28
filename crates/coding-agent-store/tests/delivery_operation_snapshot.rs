mod support;

use coding_agent_store::{
    CleanupKind, DeliveryOperationId, DeliveryOperationSnapshot, MergeOperationState, StoreError,
};

use support::delivery::eligibility::{
    approved_task_with_ready_artifact, create_merged_delivery, create_worktree_cleanup,
    create_worktree_cleanup_with_operation_id,
};

#[tokio::test]
async fn exact_merge_and_cleanup_operations_are_loaded_from_audited_task_graphs() {
    let (store, task) = approved_task_with_ready_artifact("codex/operation-snapshot").await;
    let evidence = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .evidence_identity
        .unwrap();
    let merge_id = create_merged_delivery(&store, &task, &evidence).await;
    let merge = store
        .delivery_operation_snapshot(merge_id)
        .await
        .unwrap()
        .expect("merge operation");
    assert_eq!(merge.operation_id(), merge_id);
    assert_eq!(merge.task_id(), task.id);
    match merge {
        DeliveryOperationSnapshot::Merge(operation) => {
            assert_eq!(operation.state, MergeOperationState::Merged)
        }
        DeliveryOperationSnapshot::Cleanup(_) => panic!("merge ID resolved as cleanup"),
    }

    let cleanup_id = create_worktree_cleanup(&store, &task).await;
    let cleanup = store
        .delivery_operation_snapshot(cleanup_id)
        .await
        .unwrap()
        .expect("cleanup operation");
    assert_eq!(cleanup.operation_id(), cleanup_id);
    assert_eq!(cleanup.task_id(), task.id);
    match cleanup {
        DeliveryOperationSnapshot::Cleanup(operation) => {
            assert_eq!(operation.kind, CleanupKind::RemoveWorktree);
            assert_eq!(operation.expected_merge_operation_id, merge_id);
        }
        DeliveryOperationSnapshot::Merge(_) => panic!("cleanup ID resolved as merge"),
    }
}

#[tokio::test]
async fn cleanup_origin_merge_mismatch_fails_the_audited_operation_snapshot() {
    let (store, task) = approved_task_with_ready_artifact("codex/operation-origin-mismatch").await;
    let evidence = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .evidence_identity
        .unwrap();
    create_merged_delivery(&store, &task, &evidence).await;
    let cleanup_id = create_worktree_cleanup(&store, &task).await;

    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER task_artifact_dispositions_immutable_on_update")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::raw_sql(
        "DROP TRIGGER task_artifact_dispositions_transition_on_update;
         DROP TRIGGER task_artifact_dispositions_worktree_journal_on_update;
         DROP TRIGGER task_artifact_dispositions_branch_journal_on_update;",
    )
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query("UPDATE task_artifact_dispositions SET merged_operation_id = ? WHERE task_id = ?")
        .bind(DeliveryOperationId::new().to_string())
        .bind(task.id.to_string())
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);

    assert!(matches!(
        store.delivery_operation_snapshot(cleanup_id).await,
        Err(StoreError::InvariantViolation(
            "delivery operation snapshot is inconsistent"
        ))
    ));
}

#[tokio::test]
async fn cleanup_origin_receipt_merge_pointer_corruption_fails_the_operation_snapshot() {
    let (store, task) = approved_task_with_ready_artifact("codex/operation-receipt-anchor").await;
    let evidence = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .evidence_identity
        .unwrap();
    create_merged_delivery(&store, &task, &evidence).await;
    let cleanup_id = create_worktree_cleanup(&store, &task).await;

    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER task_delivery_command_receipts_no_update")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE task_delivery_command_receipts SET cleanup_merged_operation_id = ? \
         WHERE cleanup_operation_id = ?",
    )
    .bind(DeliveryOperationId::new().to_string())
    .bind(cleanup_id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);

    assert!(matches!(
        store.delivery_operation_snapshot(cleanup_id).await,
        Err(StoreError::InvariantViolation(
            "delivery operation snapshot is inconsistent"
        ))
    ));
}

#[tokio::test]
async fn cleanup_without_its_immutable_origin_receipt_fails_the_operation_snapshot() {
    let (store, task) = approved_task_with_ready_artifact("codex/operation-origin-receipt").await;
    let evidence = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .evidence_identity
        .unwrap();
    create_merged_delivery(&store, &task, &evidence).await;
    let cleanup_id = create_worktree_cleanup(&store, &task).await;

    let mut connection = store.pool().acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER task_delivery_command_receipts_no_delete")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(
        "DELETE FROM task_delivery_command_receipts WHERE operation_id = ? \
         AND operation_kind = 'cleanup_operation'",
    )
    .bind(cleanup_id.to_string())
    .execute(&mut *connection)
    .await
    .unwrap();
    drop(connection);

    assert!(matches!(
        store.delivery_operation_snapshot(cleanup_id).await,
        Err(StoreError::InvariantViolation(
            "delivery operation snapshot is inconsistent"
        ))
    ));
}

#[tokio::test]
async fn missing_is_none_and_a_cross_table_duplicate_fails_closed() {
    let (store, task) = approved_task_with_ready_artifact("codex/operation-collision").await;
    assert!(
        store
            .delivery_operation_snapshot(DeliveryOperationId::new())
            .await
            .unwrap()
            .is_none()
    );

    let evidence = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .evidence_identity
        .unwrap();
    let operation_id = create_merged_delivery(&store, &task, &evidence).await;
    create_worktree_cleanup_with_operation_id(&store, &task, operation_id).await;

    assert!(matches!(
        store.delivery_operation_snapshot(operation_id).await,
        Err(StoreError::InvariantViolation(
            "delivery operation snapshot is inconsistent"
        ))
    ));
}
