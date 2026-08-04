mod support;

use std::collections::HashSet;
use std::sync::Arc;

use coding_agent_domain::{ClientRequestId, NewTask, TaskId, TaskStatus};
use coding_agent_store::{
    QueueLimitedCreateTaskOutcome, QueueLimitedRetryTaskOutcome, Store, StoreError, TaskTransition,
    TransitionOutcome,
};
use tokio::sync::Barrier;

#[tokio::test]
async fn create_prioritizes_existing_and_conflict_before_queue_full_without_writes() {
    let store = support::seeded_store().await;
    let repositories = store.list_repositories().await.unwrap();
    let repository = &repositories[0];
    let other_repository = support::register_repository(&store, "other").await;
    let limit = support::queue_limit(1);
    let request_id = ClientRequestId::new();
    let input = NewTask::try_new(request_id, repository.id, "same input").unwrap();

    let created = store
        .create_task_with_queue_limit(input.clone(), limit)
        .await
        .unwrap();
    let created_task = match created {
        QueueLimitedCreateTaskOutcome::Created { task, event_id } => {
            assert_eq!(task.last_event_id, event_id);
            task
        }
        other => panic!("first request must create, got {other:?}"),
    };
    let full_snapshot = support::durable_task_event_snapshot(&store).await;

    let existing = store
        .create_task_with_queue_limit(input, limit)
        .await
        .unwrap();
    assert!(matches!(
        existing,
        QueueLimitedCreateTaskOutcome::Existing { ref task } if task.id == created_task.id
    ));

    let changed_prompt = NewTask::try_new(request_id, repository.id, "different input").unwrap();
    assert!(matches!(
        store
            .create_task_with_queue_limit(changed_prompt, limit)
            .await
            .unwrap_err(),
        StoreError::IdempotencyConflict
    ));
    let changed_repository =
        NewTask::try_new(request_id, other_repository.id, "same input").unwrap();
    assert!(matches!(
        store
            .create_task_with_queue_limit(changed_repository, limit)
            .await
            .unwrap_err(),
        StoreError::IdempotencyConflict
    ));

    let queue_full = store
        .create_task_with_queue_limit(support::new_task(other_repository.id, "new request"), limit)
        .await
        .unwrap();
    assert_queue_full_create(queue_full, 1, limit);
    assert_eq!(
        support::durable_task_event_snapshot(&store).await,
        full_snapshot
    );
}

#[tokio::test]
async fn retry_prioritizes_source_validation_and_existing_child_before_queue_full() {
    let store = support::seeded_store().await;
    let limit = support::queue_limit(1);
    let source = support::terminal_task(&store, TaskStatus::Failed).await;

    let created = store
        .retry_task_with_queue_limit(source.id, limit)
        .await
        .unwrap();
    let child = match created {
        QueueLimitedRetryTaskOutcome::Created { task, event_id } => {
            assert_eq!(task.last_event_id, event_id);
            assert_eq!(task.retry_of, Some(source.id));
            task
        }
        other => panic!("first retry must create, got {other:?}"),
    };

    let full_snapshot = support::durable_task_event_snapshot(&store).await;
    let existing = store
        .retry_task_with_queue_limit(source.id, limit)
        .await
        .unwrap();
    assert!(matches!(
        existing,
        QueueLimitedRetryTaskOutcome::Existing { ref task } if task.id == child.id
    ));
    assert!(matches!(
        store
            .retry_task_with_queue_limit(child.id, limit)
            .await
            .unwrap_err(),
        StoreError::TaskNotRetryable
    ));
    assert!(matches!(
        store
            .retry_task_with_queue_limit(TaskId::new(), limit)
            .await
            .unwrap_err(),
        StoreError::TaskNotFound
    ));
    assert_eq!(
        support::durable_task_event_snapshot(&store).await,
        full_snapshot
    );

    let overflow_source = support::terminal_task(&store, TaskStatus::Interrupted).await;
    sqlx::query("UPDATE tasks SET attempt = ? WHERE id = ?")
        .bind(i64::from(u32::MAX))
        .bind(overflow_source.id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(
        store
            .retry_task_with_queue_limit(overflow_source.id, limit)
            .await
            .unwrap_err(),
        StoreError::TaskAttemptOverflow
    ));

    let new_source = support::terminal_task(&store, TaskStatus::Cancelled).await;
    let before_full_retry = support::durable_task_event_snapshot(&store).await;
    let queue_full = store
        .retry_task_with_queue_limit(new_source.id, limit)
        .await
        .unwrap();
    assert_queue_full_retry(queue_full, 1, limit);
    assert_eq!(
        support::durable_task_event_snapshot(&store).await,
        before_full_retry
    );
}

#[tokio::test]
async fn capacity_counts_only_global_queued_tasks() {
    let store = support::seeded_store().await;
    let first_repository = store.list_repositories().await.unwrap().remove(0);
    let second_repository = support::register_repository(&store, "second").await;
    let limit = support::queue_limit(1);

    let empty = store.queue_capacity(limit).await.unwrap();
    assert_eq!(empty.queued_tasks, 0);
    assert_eq!(empty.max_queued_tasks, limit);
    assert_eq!(empty.available_tasks(), 1);

    let queued = created_create(
        store
            .create_task_with_queue_limit(support::new_task(first_repository.id, "queued"), limit)
            .await
            .unwrap(),
    );
    assert_eq!(
        store.queue_capacity(limit).await.unwrap().available_tasks(),
        0
    );
    assert_queue_full_create(
        store
            .create_task_with_queue_limit(
                support::new_task(second_repository.id, "global queue"),
                limit,
            )
            .await
            .unwrap(),
        1,
        limit,
    );

    let running = applied_task(
        store
            .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
            .await
            .unwrap(),
    );
    assert_eq!(store.queue_capacity(limit).await.unwrap().queued_tasks, 0);
    let next = created_create(
        store
            .create_task_with_queue_limit(
                support::new_task(second_repository.id, "after running"),
                limit,
            )
            .await
            .unwrap(),
    );
    applied_task(
        store
            .transition_with_event(next.id, TaskStatus::Queued, TaskTransition::Cancelled)
            .await
            .unwrap(),
    );
    assert_eq!(store.queue_capacity(limit).await.unwrap().queued_tasks, 0);
    assert!(matches!(running.status, TaskStatus::Running));
    let final_queued = created_create(
        store
            .create_task_with_queue_limit(
                support::new_task(first_repository.id, "after terminal"),
                limit,
            )
            .await
            .unwrap(),
    );
    applied_task(
        store
            .transition_with_event(
                final_queued.id,
                TaskStatus::Queued,
                TaskTransition::Cancelled,
            )
            .await
            .unwrap(),
    );
    for status in [
        TaskStatus::Completed,
        TaskStatus::Failed,
        TaskStatus::Interrupted,
    ] {
        let terminal = support::terminal_task(&store, status).await;
        assert_eq!(terminal.status, status);
        assert_eq!(store.queue_capacity(limit).await.unwrap().queued_tasks, 0);
    }
}

#[tokio::test]
async fn legacy_over_capacity_saturates_without_rewriting_and_drains_below_limit() {
    let store = support::seeded_store().await;
    let repository = store.list_repositories().await.unwrap().remove(0);
    let mut queued = Vec::new();
    for prompt in ["legacy one", "legacy two", "legacy three"] {
        queued.push(
            store
                .create_task(support::new_task(repository.id, prompt))
                .await
                .unwrap()
                .task()
                .clone(),
        );
    }
    let limit = support::queue_limit(2);
    let legacy_snapshot = support::durable_task_event_snapshot(&store).await;

    let capacity = store.queue_capacity(limit).await.unwrap();
    assert_eq!(capacity.queued_tasks, 3);
    assert_eq!(capacity.max_queued_tasks, limit);
    assert_eq!(capacity.available_tasks(), 0);
    assert_queue_full_create(
        store
            .create_task_with_queue_limit(
                support::new_task(repository.id, "blocked above maximum"),
                limit,
            )
            .await
            .unwrap(),
        3,
        limit,
    );
    assert_eq!(
        support::durable_task_event_snapshot(&store).await,
        legacy_snapshot
    );

    applied_task(
        store
            .transition_with_event(queued[0].id, TaskStatus::Queued, TaskTransition::Running)
            .await
            .unwrap(),
    );
    assert_eq!(store.queue_capacity(limit).await.unwrap().queued_tasks, 2);
    assert_queue_full_create(
        store
            .create_task_with_queue_limit(
                support::new_task(repository.id, "blocked at maximum"),
                limit,
            )
            .await
            .unwrap(),
        2,
        limit,
    );

    applied_task(
        store
            .transition_with_event(
                queued[1].id,
                TaskStatus::Queued,
                TaskTransition::Interrupted(support::failure("DRAINED")),
            )
            .await
            .unwrap(),
    );
    assert_eq!(store.queue_capacity(limit).await.unwrap().queued_tasks, 1);
    created_create(
        store
            .create_task_with_queue_limit(
                support::new_task(repository.id, "admitted below maximum"),
                limit,
            )
            .await
            .unwrap(),
    );
    let final_capacity = store.queue_capacity(limit).await.unwrap();
    assert_eq!(final_capacity.queued_tasks, 2);
    assert_eq!(final_capacity.available_tasks(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_stores_serialize_create_and_retry_for_the_last_slot() {
    let fixture = support::store_fixture().await;
    let repository = support::register_repository(&fixture.store, "mixed-race").await;
    let limit = support::queue_limit(3);
    for prompt in ["seed one", "seed two"] {
        fixture
            .store
            .create_task(support::new_task(repository.id, prompt))
            .await
            .unwrap();
    }
    let source = support::terminal_task(&fixture.store, TaskStatus::Interrupted).await;
    let before = support::durable_task_event_snapshot(&fixture.store).await;

    let create_store = Store::open(&fixture.database_path).await.unwrap();
    let retry_store = Store::open(&fixture.database_path).await.unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let create_barrier = barrier.clone();
    let create_input = support::new_task(repository.id, "racing create");
    let create = tokio::spawn(async move {
        create_barrier.wait().await;
        create_store
            .create_task_with_queue_limit(create_input, limit)
            .await
    });
    let retry_barrier = barrier.clone();
    let retry = tokio::spawn(async move {
        retry_barrier.wait().await;
        retry_store
            .retry_task_with_queue_limit(source.id, limit)
            .await
    });

    barrier.wait().await;
    let create = create
        .await
        .unwrap()
        .expect("create must not return SQLITE_BUSY");
    let retry = retry
        .await
        .unwrap()
        .expect("retry must not return SQLITE_BUSY");
    let created_count = usize::from(matches!(
        create,
        QueueLimitedCreateTaskOutcome::Created { .. }
    )) + usize::from(matches!(
        retry,
        QueueLimitedRetryTaskOutcome::Created { .. }
    ));
    let full_count = usize::from(matches!(
        create,
        QueueLimitedCreateTaskOutcome::QueueFull { .. }
    )) + usize::from(matches!(
        retry,
        QueueLimitedRetryTaskOutcome::QueueFull { .. }
    ));
    assert_eq!(created_count, 1);
    assert_eq!(full_count, 1);
    match &create {
        QueueLimitedCreateTaskOutcome::QueueFull {
            queued_tasks,
            max_queued_tasks,
        } => {
            assert_eq!(*queued_tasks, 3);
            assert_eq!(*max_queued_tasks, limit);
        }
        QueueLimitedCreateTaskOutcome::Created { .. } => {}
        QueueLimitedCreateTaskOutcome::Existing { .. } => {
            panic!("unique concurrent create cannot be existing")
        }
    }
    match &retry {
        QueueLimitedRetryTaskOutcome::QueueFull {
            queued_tasks,
            max_queued_tasks,
        } => {
            assert_eq!(*queued_tasks, 3);
            assert_eq!(*max_queued_tasks, limit);
        }
        QueueLimitedRetryTaskOutcome::Created { .. } => {}
        QueueLimitedRetryTaskOutcome::Existing { .. } => {
            panic!("first concurrent retry cannot be existing")
        }
    }

    let capacity = fixture.store.queue_capacity(limit).await.unwrap();
    assert_eq!(capacity.queued_tasks, 3);
    assert_eq!(capacity.available_tasks(), 0);
    let after = support::durable_task_event_snapshot(&fixture.store).await;
    assert_eq!(after.tasks.len(), before.tasks.len() + 1);
    assert_eq!(after.events.len(), before.events.len() + 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_same_create_returns_existing_after_the_first_commit_even_when_full() {
    const CALLS: usize = 8;

    let fixture = support::store_fixture().await;
    let repository = support::register_repository(&fixture.store, "same-create-race").await;
    let limit = support::queue_limit(1);
    let first = Store::open(&fixture.database_path).await.unwrap();
    let second = Store::open(&fixture.database_path).await.unwrap();
    let request_id = ClientRequestId::new();
    let input = NewTask::try_new(request_id, repository.id, "same concurrent input").unwrap();
    let barrier = Arc::new(Barrier::new(CALLS + 1));
    let mut calls = Vec::new();

    for index in 0..CALLS {
        let store = if index % 2 == 0 {
            first.clone()
        } else {
            second.clone()
        };
        let barrier = barrier.clone();
        let input = input.clone();
        calls.push(tokio::spawn(async move {
            barrier.wait().await;
            store.create_task_with_queue_limit(input, limit).await
        }));
    }

    barrier.wait().await;
    let mut outcomes = Vec::new();
    for call in calls {
        outcomes.push(
            call.await
                .unwrap()
                .expect("idempotent create must not return SQLITE_BUSY"),
        );
    }
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, QueueLimitedCreateTaskOutcome::Created { .. }))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, QueueLimitedCreateTaskOutcome::Existing { .. }))
            .count(),
        CALLS - 1
    );
    assert!(
        outcomes
            .iter()
            .all(|outcome| !matches!(outcome, QueueLimitedCreateTaskOutcome::QueueFull { .. }))
    );
    let task_ids: HashSet<_> = outcomes
        .iter()
        .map(|outcome| match outcome {
            QueueLimitedCreateTaskOutcome::Created { task, .. }
            | QueueLimitedCreateTaskOutcome::Existing { task } => task.id,
            QueueLimitedCreateTaskOutcome::QueueFull { .. } => {
                panic!("same input must resolve before capacity")
            }
        })
        .collect();
    assert_eq!(task_ids.len(), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tasks")
            .fetch_one(fixture.store.pool())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_events")
            .fetch_one(fixture.store.pool())
            .await
            .unwrap(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn independent_stores_fill_but_never_oversell_queue_capacity() {
    const CALLS: usize = 12;

    let fixture = support::store_fixture().await;
    let repository = support::register_repository(&fixture.store, "create-race").await;
    let limit = support::queue_limit(4);
    let first = Store::open(&fixture.database_path).await.unwrap();
    let second = Store::open(&fixture.database_path).await.unwrap();
    let barrier = Arc::new(Barrier::new(CALLS + 1));
    let mut calls = Vec::new();

    for index in 0..CALLS {
        let store = if index % 2 == 0 {
            first.clone()
        } else {
            second.clone()
        };
        let barrier = barrier.clone();
        let input = support::new_task(repository.id, &format!("request {index}"));
        calls.push(tokio::spawn(async move {
            barrier.wait().await;
            store.create_task_with_queue_limit(input, limit).await
        }));
    }

    barrier.wait().await;
    let mut outcomes = Vec::new();
    for call in calls {
        outcomes.push(
            call.await
                .unwrap()
                .expect("admission must not return SQLITE_BUSY"),
        );
    }
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, QueueLimitedCreateTaskOutcome::Created { .. }))
            .count(),
        usize::try_from(limit.get()).unwrap()
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, QueueLimitedCreateTaskOutcome::QueueFull { .. }))
            .count(),
        CALLS - usize::try_from(limit.get()).unwrap()
    );
    assert_eq!(
        fixture
            .store
            .queue_capacity(limit)
            .await
            .unwrap()
            .queued_tasks,
        u64::from(limit.get())
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_events")
            .fetch_one(fixture.store.pool())
            .await
            .unwrap(),
        i64::from(limit.get())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_limited_retries_create_one_child_then_return_existing_while_full() {
    const CALLS: usize = 8;

    let fixture = support::store_fixture().await;
    support::register_repository(&fixture.store, "retry-race").await;
    let source = support::terminal_task(&fixture.store, TaskStatus::Failed).await;
    let limit = support::queue_limit(1);
    let first = Store::open(&fixture.database_path).await.unwrap();
    let second = Store::open(&fixture.database_path).await.unwrap();
    let barrier = Arc::new(Barrier::new(CALLS + 1));
    let mut calls = Vec::new();

    for index in 0..CALLS {
        let store = if index % 2 == 0 {
            first.clone()
        } else {
            second.clone()
        };
        let barrier = barrier.clone();
        calls.push(tokio::spawn(async move {
            barrier.wait().await;
            store.retry_task_with_queue_limit(source.id, limit).await
        }));
    }

    barrier.wait().await;
    let mut outcomes = Vec::new();
    for call in calls {
        outcomes.push(
            call.await
                .unwrap()
                .expect("retry must not return SQLITE_BUSY"),
        );
    }
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, QueueLimitedRetryTaskOutcome::Created { .. }))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, QueueLimitedRetryTaskOutcome::Existing { .. }))
            .count(),
        CALLS - 1
    );
    assert!(
        outcomes
            .iter()
            .all(|outcome| !matches!(outcome, QueueLimitedRetryTaskOutcome::QueueFull { .. }))
    );
    let child_ids: HashSet<_> = outcomes
        .iter()
        .map(|outcome| match outcome {
            QueueLimitedRetryTaskOutcome::Created { task, .. }
            | QueueLimitedRetryTaskOutcome::Existing { task } => task.id,
            QueueLimitedRetryTaskOutcome::QueueFull { .. } => {
                panic!("existing retry child must win over capacity")
            }
        })
        .collect();
    assert_eq!(child_ids.len(), 1);
    assert_eq!(
        fixture
            .store
            .queue_capacity(limit)
            .await
            .unwrap()
            .queued_tasks,
        1
    );
}

fn created_create(outcome: QueueLimitedCreateTaskOutcome) -> coding_agent_domain::Task {
    match outcome {
        QueueLimitedCreateTaskOutcome::Created { task, .. } => task,
        other => panic!("request must create, got {other:?}"),
    }
}

fn applied_task(outcome: TransitionOutcome) -> coding_agent_domain::Task {
    match outcome {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("fixture transition must apply"),
    }
}

fn assert_queue_full_create(
    outcome: QueueLimitedCreateTaskOutcome,
    queued_tasks: u64,
    max_queued_tasks: std::num::NonZeroU32,
) {
    assert!(matches!(
        outcome,
        QueueLimitedCreateTaskOutcome::QueueFull {
            queued_tasks: actual,
            max_queued_tasks: maximum,
        } if actual == queued_tasks && maximum == max_queued_tasks
    ));
}

fn assert_queue_full_retry(
    outcome: QueueLimitedRetryTaskOutcome,
    queued_tasks: u64,
    max_queued_tasks: std::num::NonZeroU32,
) {
    assert!(matches!(
        outcome,
        QueueLimitedRetryTaskOutcome::QueueFull {
            queued_tasks: actual,
            max_queued_tasks: maximum,
        } if actual == queued_tasks && maximum == max_queued_tasks
    ));
}
