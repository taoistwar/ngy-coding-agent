use std::collections::BTreeMap;

use coding_agent_domain::{EventCursor, TaskEventKind};
use coding_agent_store::{
    BootstrapSnapshot, EventPage, SchedulerBootstrapSnapshot, Store, TaskDetail,
};

use crate::support::{self, DurableTaskEventSnapshot};

const DELIVERY_TABLES: [&str; 8] = [
    "task_delivery_sources",
    "task_merge_operations",
    "task_merge_conflicts",
    "task_artifact_dispositions",
    "task_cleanup_operations",
    "task_cleanup_target_head_observations",
    "task_delivery_command_receipts",
    "task_delivery_operation_transitions",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilitySnapshot {
    pub durable: DurableTaskEventSnapshot,
    pub event_page: EventPage,
    pub task_event_pages: Vec<EventPage>,
    pub task_details: Vec<TaskDetail>,
    pub bootstrap: BootstrapSnapshot,
    pub scheduler: SchedulerBootstrapSnapshot,
    pub latest_event_id: EventCursor,
}

impl CompatibilitySnapshot {
    pub async fn capture(store: &Store) -> Self {
        let mut durable = support::durable_task_event_snapshot(store).await;
        durable.sequences.retain(|(name, _)| name == "task_events");
        let event_page = store
            .events_after(EventCursor::ZERO, usize::MAX)
            .await
            .unwrap();
        let bootstrap = store.bootstrap_snapshot().await.unwrap();
        let scheduler = store.scheduler_bootstrap_snapshot().await.unwrap();
        let latest_event_id = store.latest_event_id().await.unwrap();
        let mut task_event_pages = Vec::with_capacity(bootstrap.tasks.len());
        let mut task_details = Vec::with_capacity(bootstrap.tasks.len());
        for task in &bootstrap.tasks {
            task_event_pages.push(
                store
                    .task_events_after(task.id, EventCursor::ZERO, usize::MAX)
                    .await
                    .unwrap(),
            );
            task_details.push(store.task_detail(task.id).await.unwrap().unwrap());
        }
        Self {
            durable,
            event_page,
            task_event_pages,
            task_details,
            bootstrap,
            scheduler,
            latest_event_id,
        }
    }

    pub async fn assert_unchanged(&self, store: &Store, transition: &str) {
        assert_eq!(
            Self::capture(store).await,
            *self,
            "delivery transition changed P4-A state or projection: {transition}"
        );
    }

    pub fn assert_all_event_kinds(&self) {
        let mut seen = [false; 11];
        for event in &self.event_page.events {
            seen[kind_index(event.payload.kind())] = true;
        }
        assert_eq!(
            seen, [true; 11],
            "fixture must decode every P4-A event kind"
        );
        assert_eq!(self.event_page.high_watermark, self.latest_event_id);
        assert_eq!(
            self.durable.high_watermark,
            self.latest_event_id.get(),
            "raw and typed event watermarks must agree"
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryRowsSnapshot {
    rows: BTreeMap<String, Vec<String>>,
}

impl DeliveryRowsSnapshot {
    pub async fn capture(store: &Store) -> Self {
        let mut rows = BTreeMap::new();
        for table in DELIVERY_TABLES {
            rows.insert(table.to_owned(), exact_table_rows(store, table).await);
        }
        Self { rows }
    }

    pub fn is_empty(&self) -> bool {
        self.rows.values().all(Vec::is_empty)
    }
}

async fn exact_table_rows(store: &Store, table: &str) -> Vec<String> {
    let pragma = format!("PRAGMA table_info({})", quoted_identifier(table));
    let columns: Vec<(i64, String, String, i64, Option<String>, i64)> =
        sqlx::query_as(sqlx::AssertSqlSafe(pragma))
            .fetch_all(store.pool())
            .await
            .unwrap();
    assert!(!columns.is_empty(), "missing delivery table {table}");

    let values = columns
        .iter()
        .flat_map(|(_, name, _, _, _, _)| {
            let column = quoted_identifier(name);
            [format!("typeof({column})"), format!("quote({column})")]
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut primary_key = columns
        .iter()
        .filter(|(_, _, _, _, _, ordinal)| *ordinal > 0)
        .map(|(_, name, _, _, _, ordinal)| (*ordinal, quoted_identifier(name)))
        .collect::<Vec<_>>();
    primary_key.sort_by_key(|(ordinal, _)| *ordinal);
    let order = if primary_key.is_empty() {
        columns
            .iter()
            .map(|(_, name, _, _, _, _)| quoted_identifier(name))
            .collect::<Vec<_>>()
            .join(", ")
    } else {
        primary_key
            .into_iter()
            .map(|(_, column)| column)
            .collect::<Vec<_>>()
            .join(", ")
    };
    let sql = format!(
        "SELECT json_array({values}) FROM {} ORDER BY {order}",
        quoted_identifier(table)
    );
    sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
        .fetch_all(store.pool())
        .await
        .unwrap()
}

fn quoted_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

const fn kind_index(kind: TaskEventKind) -> usize {
    match kind {
        TaskEventKind::TaskQueued => 0,
        TaskEventKind::TaskStarted => 1,
        TaskEventKind::PlanUpdated => 2,
        TaskEventKind::ActivityAppended => 3,
        TaskEventKind::DiffUpdated => 4,
        TaskEventKind::TestUpdated => 5,
        TaskEventKind::ReviewUpdated => 6,
        TaskEventKind::TaskCompleted => 7,
        TaskEventKind::TaskFailed => 8,
        TaskEventKind::TaskCancelled => 9,
        TaskEventKind::TaskInterrupted => 10,
    }
}
