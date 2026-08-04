use coding_agent_domain::{
    ActivityEntry, ActivityLevel, DiffFile, DiffFileStatus, DiffSnapshot, Task, TaskEventPayload,
    TaskStatus, TestCase, TestSnapshot, TestStatus,
};
use coding_agent_store::{
    AppendEventOutcome, PersistStopIntentOutcome, StopIntentKind, StopIntentRequest, Store,
};

use crate::support;
use crate::support::delivery::eligibility::approved_task_on_store;

pub struct CompatibilityFixture {
    pub store: Store,
    pub delivery_task: Task,
    pub panel_task: Task,
    pub queued_task: Task,
    pub stopped_task: Task,
}

impl CompatibilityFixture {
    pub async fn new() -> Self {
        let store = support::seeded_store().await;
        let (_, delivery_task) =
            approved_task_on_store(store.clone(), "codex/task8-delivery", 0).await;
        let panel_task = support::running_task(&store).await;
        append_panel_events(&store, &panel_task).await;
        let panel_task = store
            .task_detail(panel_task.id)
            .await
            .unwrap()
            .unwrap()
            .task;
        let queued_task = support::queued_task(&store).await;
        let _failed = support::terminal_task(&store, TaskStatus::Failed).await;
        let _cancelled = support::terminal_task(&store, TaskStatus::Cancelled).await;
        let _interrupted = support::terminal_task(&store, TaskStatus::Interrupted).await;
        let stopped_task = support::running_task(&store).await;
        persist_stop_intent(&store, &stopped_task).await;

        Self {
            store,
            delivery_task,
            panel_task,
            queued_task,
            stopped_task,
        }
    }

    pub async fn install_event_coupling_trigger(&self) {
        let sql = format!(
            "CREATE TRIGGER task8_forbidden_delivery_event_coupling \
             AFTER INSERT ON task_merge_operations \
             BEGIN \
                 INSERT INTO task_events (schema_version, task_id, kind, payload_json, created_at) \
                 SELECT schema_version, task_id, kind, payload_json, created_at \
                 FROM task_events \
                 WHERE task_id = '{}' AND kind = 'activity.appended' \
                 ORDER BY id DESC LIMIT 1; \
                 UPDATE tasks SET last_event_id = last_insert_rowid() WHERE id = '{}'; \
             END;",
            self.panel_task.id, self.panel_task.id
        );
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
            .execute(self.store.pool())
            .await
            .unwrap();
    }

    pub async fn remove_event_coupling_trigger(&self) {
        sqlx::query("DROP TRIGGER task8_forbidden_delivery_event_coupling")
            .execute(self.store.pool())
            .await
            .unwrap();
    }
}

async fn append_panel_events(store: &Store, task: &Task) {
    append(
        store,
        task,
        TaskEventPayload::ActivityAppended {
            entry: ActivityEntry::legacy(
                "task8-compatibility",
                ActivityLevel::Info,
                "exercise the complete P4-A projection",
                support::current_timestamp(),
            ),
        },
    )
    .await;
    append(
        store,
        task,
        TaskEventPayload::DiffUpdated {
            diff: DiffSnapshot {
                revision: 1,
                files: vec![DiffFile {
                    path: "src/task8.rs".to_owned(),
                    status: DiffFileStatus::Modified,
                    patch: "compatibility fixture".to_owned(),
                    additions: 1,
                    deletions: 0,
                    truncated: false,
                }],
            },
        },
    )
    .await;
    append(
        store,
        task,
        TaskEventPayload::TestUpdated {
            tests: TestSnapshot {
                revision: 1,
                status: TestStatus::Passed,
                cases: vec![TestCase {
                    id: "task8-compatibility".to_owned(),
                    name: "delivery remains event-inert".to_owned(),
                    status: TestStatus::Passed,
                    duration_ms: 1,
                    summary: "passed".to_owned(),
                }],
            },
        },
    )
    .await;
}

async fn append(store: &Store, task: &Task, payload: TaskEventPayload) {
    assert!(matches!(
        store.append_running_event(task.id, payload).await.unwrap(),
        AppendEventOutcome::Applied { .. }
    ));
}

async fn persist_stop_intent(store: &Store, task: &Task) {
    let request = StopIntentRequest {
        task_id: task.id,
        expected_repository_id: task.repository_id,
        expected_attempt: task.attempt,
        kind: StopIntentKind::UserCancelled,
    };
    assert!(matches!(
        store.persist_stop_intent(request).await.unwrap(),
        PersistStopIntentOutcome::Applied(_)
    ));
}
