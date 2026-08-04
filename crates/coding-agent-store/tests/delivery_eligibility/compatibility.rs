use coding_agent_domain::{TaskEventKind, TaskId};
use coding_agent_store::DeliveryOperationId;

use crate::support;
use crate::support::delivery::eligibility::{approved_task_with_ready_artifact, insert_preflight};

#[tokio::test]
async fn eligibility_and_ownership_reads_leave_all_p4a_rows_byte_exact() {
    let (store, task) = approved_task_with_ready_artifact("codex/task-read-only").await;
    let eligible = store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap();
    insert_preflight(
        &store,
        &task,
        eligible.evidence_identity.as_ref().unwrap(),
        DeliveryOperationId::new(),
    )
    .await;
    let before = support::durable_task_event_snapshot(&store).await;

    for _ in 0..3 {
        assert!(
            store
                .delivery_eligibility_snapshot(task.id)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .delivery_ownership_snapshot(task.id)
                .await
                .unwrap()
                .is_some()
        );
    }

    assert_eq!(support::durable_task_event_snapshot(&store).await, before);
    let non_v1_events: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM task_events WHERE schema_version != 1")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(non_v1_events, 0);
}

#[tokio::test]
async fn absent_tasks_remain_absent_and_the_event_kind_contract_stays_at_eleven() {
    let store = support::seeded_store().await;
    let missing = TaskId::new();
    assert!(
        store
            .delivery_eligibility_snapshot(missing)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .delivery_ownership_snapshot(missing)
            .await
            .unwrap()
            .is_none()
    );

    let kinds = [
        TaskEventKind::TaskQueued,
        TaskEventKind::TaskStarted,
        TaskEventKind::PlanUpdated,
        TaskEventKind::ActivityAppended,
        TaskEventKind::DiffUpdated,
        TaskEventKind::TestUpdated,
        TaskEventKind::ReviewUpdated,
        TaskEventKind::TaskCompleted,
        TaskEventKind::TaskFailed,
        TaskEventKind::TaskCancelled,
        TaskEventKind::TaskInterrupted,
    ];
    assert_eq!(kinds.len(), 11);
    assert_eq!(
        kinds
            .iter()
            .map(|kind| serde_json::to_value(kind).unwrap())
            .collect::<Vec<_>>(),
        [
            "task.queued",
            "task.started",
            "plan.updated",
            "activity.appended",
            "diff.updated",
            "test.updated",
            "review.updated",
            "task.completed",
            "task.failed",
            "task.cancelled",
            "task.interrupted",
        ]
        .map(serde_json::Value::from)
    );
}
