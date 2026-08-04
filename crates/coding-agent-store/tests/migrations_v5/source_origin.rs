use super::{helpers, support};

#[tokio::test]
async fn delivery_source_origin_is_exact_required_and_immutable() {
    let fixture = helpers::object_pending_source_fixture().await;
    let origin: (String, String, i64) = sqlx::query_as(
        "SELECT origin_accepted_operation_id, origin_accept_receipt_id, \
                origin_accepted_version \
         FROM task_delivery_sources WHERE task_id = ?",
    )
    .bind(support::delivery::TASK_ID)
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(origin.0, support::delivery::MERGE_OPERATION_ID);
    assert_eq!(origin.1, support::delivery::ACCEPT_RECEIPT_ID);
    assert_eq!(origin.2, 3);

    for update in [
        "UPDATE task_delivery_sources \
         SET origin_accepted_operation_id = '44444444-4444-4444-8444-444444444445', \
             failure_code = 'COMMAND_TIMED_OUT', version = 2 \
         WHERE task_id = ?",
        "UPDATE task_delivery_sources \
         SET origin_accept_receipt_id = '66666666-6666-4666-8666-666666666667', \
             failure_code = 'COMMAND_TIMED_OUT', version = 2 \
         WHERE task_id = ?",
        "UPDATE task_delivery_sources \
         SET origin_accepted_version = 4, failure_code = 'COMMAND_TIMED_OUT', version = 2 \
         WHERE task_id = ?",
    ] {
        let error = sqlx::query(update)
            .bind(support::delivery::TASK_ID)
            .execute(fixture.store.pool())
            .await
            .expect_err("source origin must be immutable");
        assert!(
            error
                .to_string()
                .contains("delivery source provenance is immutable"),
            "unexpected immutable-origin error: {error}"
        );
    }

    let table = helpers::normalized_schema_sql(fixture.store.pool(), "task_delivery_sources").await;
    for required in [
        "origin_accepted_operation_id TEXT NOT NULL",
        "origin_accept_receipt_id TEXT NOT NULL",
        "origin_accepted_version INTEGER NOT NULL",
        "FOREIGN KEY (origin_accepted_operation_id) REFERENCES task_merge_operations (operation_id)",
        "FOREIGN KEY (origin_accept_receipt_id) REFERENCES task_delivery_command_receipts (client_request_id)",
    ] {
        assert!(
            table.contains(required),
            "missing source origin schema: {required}"
        );
    }
    let trigger = helpers::normalized_schema_sql(
        fixture.store.pool(),
        "task_delivery_sources_ownership_on_insert",
    )
    .await;
    for required in [
        "m.operation_id = NEW.origin_accepted_operation_id",
        "m.accept_receipt_id = NEW.origin_accept_receipt_id",
        "m.version = NEW.origin_accepted_version",
        "receipt.accepted_operation_version = NEW.origin_accepted_version",
        "transition.entity_version = NEW.origin_accepted_version",
        "transition.from_state = 'preflight_ready'",
        "transition.to_state = 'accepted'",
    ] {
        assert!(
            trigger.contains(required),
            "missing exact origin trigger: {required}"
        );
    }
}
