#![cfg(feature = "test-support")]

mod support;

use std::io::{self, Write};
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use coding_agent_api::{
    ApiBackend, AuthContext, CancelResult, CreateResult, CreateTaskRequest, LiveEventItem,
    RequestSecurity, SchedulerStateFrames, SessionExchange, SseBackend, build_api_router,
};
use coding_agent_app::{
    ApplicationBackend, EventDispatcherHandle, MutationGate, RepositoryDiscovery,
    SchedulerRepositoryStorageState, SchedulerStorageNotification, ServiceState,
    ServiceStateController, StorageState, StoreWriterHandle, TaskManagerHandle,
};
use coding_agent_domain::{
    ClientRequestId, DiffFile, DiffFileStatus, DiffSnapshot, EventCursor, TaskEventPayload, TaskId,
    TaskStatus, TestCase, TestSnapshot, TestStatus,
};
use coding_agent_store::{
    AppendEventOutcome, CreateTaskOutcome, FinalizeReviewedTaskOutcome, RecordReviewOutcome,
    TaskTransition, TransitionOutcome,
};
use futures_util::StreamExt as _;
use http::header::{CONTENT_TYPE, COOKIE, HOST, ORIGIN};
use http::{Method, Request, StatusCode};
use http_body_util::BodyExt as _;
use tower::ServiceExt as _;

fn application_backend(
    fixture: &support::TaskManagerFixture,
    security: &support::SecurityFixture,
    gate: MutationGate,
    write_budget: Duration,
    quit_signal: Arc<AtomicBool>,
) -> Arc<ApplicationBackend> {
    application_backend_with_queue_limit(
        fixture,
        security,
        gate,
        write_budget,
        NonZeroU32::new(256).unwrap(),
        quit_signal,
    )
}

fn application_backend_with_queue_limit(
    fixture: &support::TaskManagerFixture,
    security: &support::SecurityFixture,
    gate: MutationGate,
    write_budget: Duration,
    max_queued_tasks: NonZeroU32,
    quit_signal: Arc<AtomicBool>,
) -> Arc<ApplicationBackend> {
    Arc::new(application_backend_value(
        fixture,
        security,
        gate,
        write_budget,
        max_queued_tasks,
        quit_signal,
    ))
}

fn application_backend_value(
    fixture: &support::TaskManagerFixture,
    security: &support::SecurityFixture,
    gate: MutationGate,
    write_budget: Duration,
    max_queued_tasks: NonZeroU32,
    quit_signal: Arc<AtomicBool>,
) -> ApplicationBackend {
    ApplicationBackend::new_without_repository_runtime_for_test(
        fixture.store.clone(),
        fixture.writer.clone(),
        fixture.dispatcher.clone(),
        fixture.manager.clone(),
        RepositoryDiscovery::new_without_commands_for_test(std::env::temp_dir()),
        None,
        security.manager.clone(),
        fixture.state.clone(),
        gate,
        support::timestamp(),
        1,
        max_queued_tasks,
        write_budget,
        Arc::new(move || quit_signal.store(true, Ordering::SeqCst)),
    )
}

#[tokio::test]
async fn bootstrap_projects_the_exact_joined_store_and_service_fields() {
    let fixture = support::task_manager_fixture(1).await;
    fixture
        .manager
        .notify_scheduler_storage_for_test(SchedulerStorageNotification::new(
            StorageState::Normal,
            StorageState::Normal,
            StorageState::Normal,
            vec![SchedulerRepositoryStorageState::new(
                fixture.repository.id,
                StorageState::Normal,
            )],
        ));
    let expected = fixture
        .store
        .scheduler_bootstrap_snapshot()
        .await
        .expect("load exact expected Store snapshot");
    let security = support::SecurityFixture::production();
    let backend = application_backend(
        &fixture,
        &security,
        MutationGate::new(fixture.state.clone()),
        Duration::from_secs(2),
        Arc::new(AtomicBool::new(false)),
    );

    let bootstrap = backend
        .bootstrap(&establish_session(&security).await.auth)
        .await
        .expect("join exact Bootstrap snapshot");

    assert_eq!(bootstrap.latest_event_id, expected.latest_event_id.get());
    assert_eq!(
        bootstrap.service_state,
        coding_agent_api::ServiceStateDto::Ready
    );
    assert_eq!(bootstrap.service_state_generation, 0);
    assert_eq!(bootstrap.max_concurrent_tasks, 1);
    assert_eq!(bootstrap.scheduler.limits.global, 1);
    assert_eq!(bootstrap.scheduler.limits.per_repository, 1);
    assert_eq!(bootstrap.scheduler.limits.queued, 256);
    assert_eq!(bootstrap.scheduler.limits.cargo_jobs_per_task, 1);
    assert_eq!(
        bootstrap.scheduler.server_started_at,
        bootstrap.server_started_at
    );
    assert_eq!(
        bootstrap.scheduler.service_state_generation,
        bootstrap.service_state_generation
    );
    assert_eq!(
        bootstrap.scheduler.as_of_event_id,
        u64::try_from(expected.membership_event_id.get()).unwrap()
    );
    assert_eq!(
        bootstrap.scheduler.queued_task_count as usize,
        bootstrap.scheduler.queued_tasks.len()
    );
    assert_eq!(
        bootstrap.scheduler.storage.repositories.len(),
        bootstrap.repositories.len()
    );
    assert_eq!(
        bootstrap.scheduler.admission_state,
        coding_agent_api::SchedulerAdmissionStateDto::Running
    );
    assert_eq!(bootstrap.scheduler.active_task_count, 0);
    assert_eq!(
        bootstrap.scheduler.storage.state,
        coding_agent_api::SchedulerStorageStateDto::Normal
    );
    assert_eq!(bootstrap.repositories.len(), expected.repositories.len());
    assert_eq!(bootstrap.tasks.len(), expected.tasks.len());
    assert_eq!(
        backend.scheduler_state_for_test(),
        bootstrap.scheduler,
        "Bootstrap and the future live Scheduler source share one exact DTO projection",
    );
    let event_high_before_wire = fixture.store.latest_event_id().await.unwrap();
    let wire = SchedulerStateFrames::try_from_snapshot(&bootstrap.scheduler)
        .expect("the exact joined Scheduler projection is segmentable");
    assert_eq!(
        wire.manifest().item_count,
        bootstrap.scheduler.queued_tasks.len() as u32
            + bootstrap.scheduler.stopping_tasks.len() as u32
            + bootstrap.scheduler.storage.repositories.len() as u32,
    );
    assert_eq!(
        fixture.store.latest_event_id().await.unwrap(),
        event_high_before_wire,
        "Scheduler control construction is non-persistent",
    );
    assert_eq!(
        bootstrap
            .repositories
            .iter()
            .map(|repository| repository.id)
            .collect::<Vec<_>>(),
        expected
            .repositories
            .iter()
            .map(|repository| repository.id.as_uuid())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn bootstrap_scheduler_projects_service_pause_with_authoritative_queue_order() {
    let fixture = support::task_manager_fixture(1).await;
    fixture
        .state
        .set(ServiceState::StoreDegraded)
        .expect("pause Scheduler admission");
    let queued = match fixture
        .store
        .create_task(support::new_task(
            fixture.repository.id,
            "service-paused scheduler queue",
        ))
        .await
        .expect("create a durable queued task while paused")
    {
        CreateTaskOutcome::Created { task, .. } | CreateTaskOutcome::Existing { task } => task,
    };
    fixture
        .manager
        .notify_scheduler_storage_for_test(SchedulerStorageNotification::new(
            StorageState::Normal,
            StorageState::Normal,
            StorageState::Normal,
            vec![SchedulerRepositoryStorageState::new(
                fixture.repository.id,
                StorageState::Normal,
            )],
        ));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let projection = fixture.manager.scheduler_projection_for_test();
            if projection.as_of_event_id.get() == queued.last_event_id.get()
                && projection.tasks == vec![(queued.id, TaskStatus::Queued)]
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("TaskManager publishes the paused queue before the API join");
    let security = support::SecurityFixture::production();
    let backend = application_backend(
        &fixture,
        &security,
        MutationGate::new(fixture.state.clone()),
        Duration::from_secs(2),
        Arc::new(AtomicBool::new(false)),
    );

    let bootstrap = backend
        .bootstrap(&establish_session(&security).await.auth)
        .await
        .expect("join paused Scheduler projection");

    assert_eq!(
        bootstrap.scheduler.admission_state,
        coding_agent_api::SchedulerAdmissionStateDto::Paused
    );
    assert_eq!(bootstrap.scheduler.queued_task_count, 1);
    assert_eq!(
        bootstrap.scheduler.queued_tasks[0].task_id,
        queued.id.as_uuid()
    );
    assert_eq!(
        bootstrap.scheduler.queued_tasks[0].reason,
        coding_agent_api::SchedulerQueueReasonDto::ServicePaused
    );
    assert_eq!(
        bootstrap.scheduler.as_of_event_id,
        u64::try_from(bootstrap.latest_event_id).unwrap()
    );
}

#[tokio::test]
async fn bootstrap_fails_closed_when_scheduler_and_http_queue_limits_diverge() {
    let fixture = support::task_manager_fixture(1).await;
    let security = support::SecurityFixture::production();
    let backend = application_backend_with_queue_limit(
        &fixture,
        &security,
        MutationGate::new(fixture.state.clone()),
        Duration::from_secs(2),
        NonZeroU32::MIN,
        Arc::new(AtomicBool::new(false)),
    );

    let error = backend
        .bootstrap(&establish_session(&security).await.auth)
        .await
        .expect_err("mismatched immutable queue limits must fail closed");

    assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error.code, "BOOTSTRAP_SNAPSHOT_UNAVAILABLE");
    assert!(error.retryable);
}

#[tokio::test]
async fn bootstrap_join_budget_exhaustion_maps_to_exact_retryable_503() {
    let fixture = support::task_manager_fixture(1).await;
    let security = support::SecurityFixture::production();
    let backend = Arc::new(
        application_backend_value(
            &fixture,
            &security,
            MutationGate::new(fixture.state.clone()),
            Duration::from_secs(2),
            NonZeroU32::new(256).unwrap(),
            Arc::new(AtomicBool::new(false)),
        )
        .with_bootstrap_join_budget_for_test(Duration::ZERO),
    );

    let error = backend
        .bootstrap(&establish_session(&security).await.auth)
        .await
        .expect_err("an exhausted join budget must fail closed");

    assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error.code, "BOOTSTRAP_SNAPSHOT_UNAVAILABLE");
    assert!(error.retryable);
}

#[tokio::test]
async fn application_backend_adapts_dispatcher_and_service_watch_to_sse_ports() {
    let fixture = support::task_manager_fixture(1).await;
    let security = support::SecurityFixture::production();
    let backend = application_backend(
        &fixture,
        &security,
        MutationGate::new(fixture.state.clone()),
        Duration::from_secs(2),
        Arc::new(AtomicBool::new(false)),
    );
    let mut live = backend.subscribe_live();
    let mut service = backend.subscribe_service_state();

    fixture.state.set(ServiceState::StoreDegraded).unwrap();
    let control = tokio::time::timeout(Duration::from_secs(1), service.next())
        .await
        .expect("service update")
        .expect("open service stream");
    assert_eq!(
        control.state,
        coding_agent_api::ServiceStateDto::StoreDegraded
    );
    assert_eq!(control.generation, 1);
    fixture.state.set(ServiceState::Ready).unwrap();

    let task = fixture.enqueue_tasks(1, true).await.remove(0);
    fixture
        .dispatcher
        .flush_to(coding_agent_domain::EventCursor::new(task.last_event_id.get()).unwrap())
        .await
        .unwrap();
    let event = tokio::time::timeout(Duration::from_secs(1), live.next())
        .await
        .expect("live event")
        .expect("open live stream");
    match event {
        LiveEventItem::Event(event) => {
            assert_eq!(
                serde_json::to_value(event).unwrap()["id"],
                task.last_event_id.get()
            );
        }
        LiveEventItem::Lagged => panic!("fresh subscriber must not lag"),
    }
}

#[tokio::test]
async fn application_backend_adapts_scheduler_watch_and_exact_membership_watermark() {
    let fixture = support::task_manager_fixture(1).await;
    let security = support::SecurityFixture::production();
    let backend = application_backend(
        &fixture,
        &security,
        MutationGate::new(fixture.state.clone()),
        Duration::from_secs(2),
        Arc::new(AtomicBool::new(false)),
    );
    let initial = backend
        .current_scheduler_state()
        .await
        .expect("project current Scheduler state");
    assert_eq!(initial.as_ref(), &backend.scheduler_state_for_test());
    assert_eq!(backend.membership_watermark_through(0).await.unwrap(), 0);

    let mut scheduler = backend.subscribe_scheduler_state();
    fixture
        .state
        .set(ServiceState::StoreDegraded)
        .expect("pause Scheduler admission");
    let paused = tokio::time::timeout(Duration::from_secs(2), scheduler.next())
        .await
        .expect("Scheduler watch publishes the service transition")
        .expect("Scheduler watch remains open")
        .expect("Scheduler projection remains valid");
    assert_eq!(
        paused.admission_state,
        coding_agent_api::SchedulerAdmissionStateDto::Paused
    );
    assert_eq!(paused.service_state_generation, 1);
    assert!(paused.generation > initial.generation);

    let queued = match fixture
        .store
        .create_task(support::new_task(
            fixture.repository.id,
            "bounded membership watermark",
        ))
        .await
        .expect("create queued lifecycle event")
    {
        CreateTaskOutcome::Created { task, .. } | CreateTaskOutcome::Existing { task } => task,
    };
    let running = match fixture
        .store
        .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .expect("create started lifecycle event")
    {
        TransitionOutcome::Applied { task, .. } => task,
        other => panic!("fixture transition must apply, got {other:?}"),
    };
    let panel_event_id = match fixture
        .store
        .append_running_event(
            running.id,
            TaskEventPayload::PlanUpdated {
                plan: coding_agent_domain::PlanSnapshot::legacy(1, Vec::new()),
            },
        )
        .await
        .expect("create non-membership event")
    {
        AppendEventOutcome::Applied { event_id } => event_id,
        AppendEventOutcome::NotRunning { .. } => panic!("fixture task must remain running"),
    };

    assert_eq!(
        backend
            .membership_watermark_through(queued.last_event_id.get())
            .await
            .unwrap(),
        queued.last_event_id.get()
    );
    assert_eq!(
        backend
            .membership_watermark_through(running.last_event_id.get())
            .await
            .unwrap(),
        running.last_event_id.get()
    );
    assert_eq!(
        backend
            .membership_watermark_through(panel_event_id.get())
            .await
            .unwrap(),
        running.last_event_id.get()
    );
}

#[tokio::test]
async fn both_sse_sources_close_after_publishing_quiescing() {
    let fixture = support::task_manager_fixture(1).await;
    let security = support::SecurityFixture::production();
    let backend = application_backend(
        &fixture,
        &security,
        MutationGate::new(fixture.state.clone()),
        Duration::from_secs(2),
        Arc::new(AtomicBool::new(false)),
    );
    let mut live = backend.subscribe_live();
    let mut service = backend.subscribe_service_state();

    fixture.state.set(ServiceState::Quiescing).unwrap();

    let control = service.next().await.expect("publish Quiescing once");
    assert_eq!(control.state, coding_agent_api::ServiceStateDto::Quiescing);
    assert!(service.next().await.is_none());
    assert!(live.next().await.is_none());

    let mut late_live = backend.subscribe_live();
    let mut late_service = backend.subscribe_service_state();
    let late_control = tokio::time::timeout(Duration::from_secs(1), late_service.next())
        .await
        .expect("a late service subscriber must not wait for another transition")
        .expect("a late service subscriber receives Quiescing once");
    assert_eq!(
        late_control.state,
        coding_agent_api::ServiceStateDto::Quiescing
    );
    assert!(late_service.next().await.is_none());
    assert!(late_live.next().await.is_none());
}

#[tokio::test]
async fn production_sse_router_recovers_dispatcher_lag_from_sqlite_with_exact_frames() {
    let fixture = support::store_fixture().await;
    let dispatcher = EventDispatcherHandle::spawn(fixture.store.clone(), 2)
        .await
        .expect("spawn deliberately small SSE dispatcher");
    let writer = StoreWriterHandle::spawn(fixture.store.clone(), Arc::new(dispatcher.clone()), 16);
    // Keep the TaskManager admission loop paused so this test isolates dispatcher lag
    // recovery from independently valid scheduler-generation preemption/reset behavior.
    let state = ServiceStateController::new(ServiceState::StoreDegraded);
    let launch_resources = support::task_manager_launch_resources(&fixture, 1, 1).await;
    let manager = TaskManagerHandle::spawn(
        fixture.store.clone(),
        writer.clone(),
        dispatcher.clone(),
        state.clone(),
        Arc::new(support::ControlledRunner::default()),
        launch_resources,
        16,
    );
    let security = support::SecurityFixture::production();
    let session = establish_session(&security).await;
    let backend = Arc::new(ApplicationBackend::new_without_repository_runtime_for_test(
        fixture.store.clone(),
        writer.clone(),
        dispatcher.clone(),
        manager,
        RepositoryDiscovery::new_without_commands_for_test(std::env::temp_dir()),
        None,
        security.manager.clone(),
        state.clone(),
        MutationGate::new(state.clone()),
        support::timestamp(),
        1,
        NonZeroU32::new(256).unwrap(),
        Duration::from_secs(2),
        Arc::new(|| {}),
    ));
    let router = build_api_router(backend.clone(), Arc::new(security.manager.clone()), backend);
    let response = router
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/events?after=0")
                .header(HOST, &security.expected_host)
                .header(COOKIE, &session.cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[CONTENT_TYPE], "text/event-stream");

    let mut stream = response.into_body().into_data_stream();
    let service = stream
        .next()
        .await
        .expect("initial service frame")
        .expect("read initial service frame");
    assert!(String::from_utf8_lossy(&service).contains("event: service.state\n"));

    // Drive the connection through its empty initial snapshot and leave it
    // waiting on the production dispatcher. Four durable commits then overflow
    // the two-event live buffer, forcing the real SSE path to refill the gap
    // from SQLite rather than serializing or dropping a lag marker.
    let waiting = stream.next();
    futures_util::pin_mut!(waiting);
    assert!(futures_util::poll!(waiting.as_mut()).is_pending());

    let mut expected_ids = Vec::new();
    for prompt in ["lag one", "lag two", "lag three", "lag four"] {
        let receipt = writer
            .create_task(
                support::new_task(fixture.repository.id, prompt),
                support::deadline(),
            )
            .await
            .expect("persist lag-recovery event");
        expected_ids.push(receipt.event_id.expect("create emits a queued event").get());
    }
    dispatcher
        .flush_to(EventCursor::new(*expected_ids.last().unwrap()).unwrap())
        .await
        .expect("publish every lag-recovery event");

    let first = tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .expect("lag recovery produced its first persisted frame")
        .expect("SSE stream remains open")
        .expect("read first recovered frame");
    let mut wire = String::from_utf8(first.to_vec()).expect("SSE frame is UTF-8");
    while persisted_sse_ids(&wire).len() < expected_ids.len() {
        let frame = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("lag recovery produced the complete persisted range")
            .expect("SSE stream remains open during recovery")
            .expect("read recovered frame");
        wire.push_str(std::str::from_utf8(&frame).expect("SSE frame is UTF-8"));
    }

    state.set(ServiceState::Quiescing).unwrap();
    while let Some(frame) = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("quiescing closes the production SSE stream")
    {
        let frame = frame.expect("read quiescing frame");
        wire.push_str(std::str::from_utf8(&frame).expect("SSE frame is UTF-8"));
    }

    assert_eq!(persisted_sse_ids(&wire), expected_ids);
    assert!(!wire.contains("lagged"));
    for id in expected_ids {
        let frame = wire
            .split("\n\n")
            .find(|frame| frame.lines().any(|line| line == format!("id: {id}")))
            .expect("recovered event has an SSE frame");
        assert!(frame.contains("event: task.queued\n"));
        assert!(frame.contains(&format!("\"id\":{id}")));
    }
}

fn persisted_sse_ids(wire: &str) -> Vec<i64> {
    wire.split("\n\n")
        .filter_map(|frame| {
            frame
                .lines()
                .find_map(|line| line.strip_prefix("id: "))
                .and_then(|id| id.parse().ok())
        })
        .collect()
}

#[tokio::test]
async fn task_surfaces_keep_unreviewed_readiness_across_create_list_get_cancel_and_retry() {
    let fixture = occupied_fixture().await;
    let security = support::SecurityFixture::production();
    let backend = application_backend(
        &fixture,
        &security,
        MutationGate::new(fixture.state.clone()),
        Duration::from_secs(2),
        Arc::new(AtomicBool::new(false)),
    );
    let auth = establish_session(&security).await.auth;
    let created = match backend
        .create_task(
            &auth,
            CreateTaskRequest {
                client_request_id: ClientRequestId::new(),
                repository_id: fixture.repository.id,
                prompt: "exercise every task surface".to_owned(),
            },
        )
        .await
        .expect("create unreviewed task")
    {
        CreateResult::Created(task) => task,
        CreateResult::Existing(_) => panic!("fresh request must create a task"),
    };
    assert_task_readiness(&created, "queued", "unreviewed");

    let bootstrap_task = backend
        .bootstrap(&auth)
        .await
        .expect("load bootstrap")
        .tasks
        .into_iter()
        .find(|task| task.id == created.id)
        .expect("bootstrap contains created task");
    assert_task_readiness(&bootstrap_task, "queued", "unreviewed");
    let listed_task = backend
        .list_tasks(&auth, Some(fixture.repository.id))
        .await
        .expect("list repository tasks")
        .into_iter()
        .find(|task| task.id == created.id)
        .expect("list contains created task");
    assert_task_readiness(&listed_task, "queued", "unreviewed");
    let created_id = dto_task_id(&created);
    let created_detail = backend
        .task_detail(&auth, created_id)
        .await
        .expect("get created task");
    assert_task_readiness(&created_detail.task, "queued", "unreviewed");
    assert!(
        serde_json::to_value(&created_detail).unwrap()["reviews"]
            .as_array()
            .expect("task detail reviews is an array")
            .is_empty()
    );

    let cancelled = match backend
        .cancel_task(&auth, created_id)
        .await
        .expect("cancel queued task")
    {
        CancelResult::Finished(task) => task,
        CancelResult::Accepted { .. } => panic!("queued cancellation must finish synchronously"),
    };
    assert_task_readiness(&cancelled, "cancelled", "unreviewed");
    let cancelled_detail = backend
        .task_detail(&auth, created_id)
        .await
        .expect("get cancelled task");
    assert_task_readiness(&cancelled_detail.task, "cancelled", "unreviewed");
    assert!(
        serde_json::to_value(&cancelled_detail).unwrap()["reviews"]
            .as_array()
            .expect("cancelled detail reviews is an array")
            .is_empty()
    );

    let retried = match backend
        .retry_task(&auth, created_id)
        .await
        .expect("retry cancelled task")
    {
        CreateResult::Created(task) => task,
        CreateResult::Existing(_) => panic!("first retry must create a direct child"),
    };
    assert_task_readiness(&retried, "queued", "unreviewed");
    assert_eq!(retried.retry_of, Some(created.id));
    let retried_id = dto_task_id(&retried);
    let retried_detail = backend
        .task_detail(&auth, retried_id)
        .await
        .expect("get retried task");
    assert_task_readiness(&retried_detail.task, "queued", "unreviewed");
    assert!(
        serde_json::to_value(&retried_detail).unwrap()["reviews"]
            .as_array()
            .expect("retry detail reviews is an array")
            .is_empty(),
        "retry must not inherit review evidence"
    );
}

#[tokio::test]
async fn real_review_projection_is_identical_for_detail_rest_recovery_and_live_sse() {
    let fixture = support::task_manager_fixture(1).await;
    let security = support::SecurityFixture::production();
    let backend = application_backend(
        &fixture,
        &security,
        MutationGate::new(fixture.state.clone()),
        Duration::from_secs(2),
        Arc::new(AtomicBool::new(false)),
    );
    let auth = establish_session(&security).await.auth;
    fixture
        .dispatcher
        .flush_to(
            fixture
                .store
                .latest_event_id()
                .await
                .expect("load dispatcher baseline"),
        )
        .await
        .expect("flush dispatcher baseline");
    let mut live = backend.subscribe_live();

    let queued = match fixture
        .store
        .create_task(support::new_task(
            fixture.repository.id,
            "persist a fully reviewed task",
        ))
        .await
        .expect("create reviewed task")
    {
        CreateTaskOutcome::Created { task, .. } | CreateTaskOutcome::Existing { task } => task,
    };
    let running = match fixture
        .store
        .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .expect("start reviewed task")
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("reviewed task must start"),
    };
    match fixture
        .store
        .append_running_event(
            running.id,
            TaskEventPayload::PlanUpdated {
                plan: support::fixture_review_plan(),
            },
        )
        .await
        .expect("persist structured plan")
    {
        AppendEventOutcome::Applied { .. } => {}
        AppendEventOutcome::NotRunning { .. } => panic!("reviewed task must remain running"),
    }
    match fixture
        .store
        .record_review(
            running.id,
            running.repository_id,
            running.attempt,
            support::changes_requested_review(1),
        )
        .await
        .expect("persist nonterminal review")
    {
        RecordReviewOutcome::Applied { .. } => {}
        RecordReviewOutcome::Existing { .. } => panic!("first review write must apply"),
    }

    let intermediate = backend
        .task_detail(&auth, running.id)
        .await
        .expect("load intermediate reviewed detail");
    assert_task_readiness(&intermediate.task, "running", "unreviewed");
    let intermediate_json = serde_json::to_value(&intermediate).unwrap();
    assert_eq!(
        intermediate_json["reviews"]
            .as_array()
            .expect("intermediate reviews array")
            .len(),
        1
    );
    assert_eq!(intermediate_json["reviews"][0]["round"], 1);
    assert_eq!(
        intermediate_json["reviews"][0]["verdict"],
        "changes_requested"
    );

    let diff = DiffSnapshot {
        revision: 2,
        files: vec![DiffFile {
            path: "src/lib.rs".to_owned(),
            status: DiffFileStatus::Modified,
            patch: "@@ final reviewed patch @@".to_owned(),
            additions: 1,
            deletions: 0,
            truncated: false,
        }],
    };
    match fixture
        .store
        .append_running_event(running.id, TaskEventPayload::DiffUpdated { diff })
        .await
        .expect("persist final diff")
    {
        AppendEventOutcome::Applied { .. } => {}
        AppendEventOutcome::NotRunning { .. } => panic!("final diff must precede finalization"),
    }
    let tests = TestSnapshot {
        revision: 2,
        status: TestStatus::Passed,
        cases: vec![TestCase {
            id: "fixture-cargo-test".to_owned(),
            name: "cargo test".to_owned(),
            status: TestStatus::Passed,
            duration_ms: 10,
            summary: "fixture check passed".to_owned(),
        }],
    };
    match fixture
        .store
        .append_running_event(running.id, TaskEventPayload::TestUpdated { tests })
        .await
        .expect("persist final tests")
    {
        AppendEventOutcome::Applied { .. } => {}
        AppendEventOutcome::NotRunning { .. } => panic!("final tests must precede finalization"),
    }
    let terminal_event_id = match fixture
        .store
        .finalize_reviewed_task(
            running.id,
            running.repository_id,
            running.attempt,
            support::approved_review_round(2),
        )
        .await
        .expect("persist final approved review")
    {
        FinalizeReviewedTaskOutcome::Applied {
            terminal_event_id, ..
        } => terminal_event_id,
        FinalizeReviewedTaskOutcome::Existing { .. } => {
            panic!("first final review write must apply")
        }
    };

    fixture.dispatcher.wake();
    fixture
        .dispatcher
        .flush_to(EventCursor::new(terminal_event_id.get()).unwrap())
        .await
        .expect("publish reviewed task events");
    let mut live_events = Vec::new();
    for _ in 0..8 {
        match tokio::time::timeout(Duration::from_secs(2), live.next())
            .await
            .expect("receive every reviewed live event")
            .expect("live stream remains open")
        {
            LiveEventItem::Event(event) => live_events.push(event),
            LiveEventItem::Lagged => panic!("reviewed fixture must not lag"),
        }
    }
    let rest_events = backend
        .task_events(&auth, running.id, 0)
        .await
        .expect("load reviewed task REST events");
    let recovery_events = backend
        .events_between(0, terminal_event_id.get(), usize::MAX)
        .await
        .expect("load reviewed SSE recovery range");
    let live_json = live_events
        .iter()
        .map(|event| serde_json::to_value(event).unwrap())
        .collect::<Vec<_>>();
    let rest_json = rest_events
        .iter()
        .map(|event| serde_json::to_value(event).unwrap())
        .collect::<Vec<_>>();
    let recovery_json = recovery_events
        .iter()
        .map(|event| serde_json::to_value(event).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(live_json, rest_json);
    assert_eq!(recovery_json, rest_json);
    assert_eq!(
        rest_json
            .iter()
            .map(|event| event["kind"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "task.queued",
            "task.started",
            "plan.updated",
            "review.updated",
            "diff.updated",
            "test.updated",
            "review.updated",
            "task.completed",
        ]
    );
    assert_eq!(rest_json[6]["payload"]["review"]["round"], 2);
    assert_eq!(rest_json[6]["payload"]["review"]["verdict"], "approved");
    assert_eq!(
        rest_json[6]["payload"]["review"]["summary"],
        "fixture round 2 approved"
    );
    assert_eq!(
        rest_json[7]["payload"]["task"]["delivery_readiness"],
        "review_approved"
    );

    let detail = backend
        .task_detail(&auth, running.id)
        .await
        .expect("load final reviewed detail");
    assert_task_readiness(&detail.task, "completed", "review_approved");
    let detail_json = serde_json::to_value(&detail).unwrap();
    assert_eq!(
        detail_json["reviews"]
            .as_array()
            .expect("final reviews array")
            .iter()
            .map(|review| review["round"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(
        detail_json["reviews"][1], rest_json[6]["payload"]["review"],
        "TaskDetail and event projection must use the same typed review DTO"
    );
    let bootstrap_task = backend
        .bootstrap(&auth)
        .await
        .expect("load reviewed bootstrap")
        .tasks
        .into_iter()
        .find(|task| task.id == detail.task.id)
        .expect("bootstrap contains reviewed task");
    assert_task_readiness(&bootstrap_task, "completed", "review_approved");
    let listed_task = backend
        .list_tasks(&auth, Some(fixture.repository.id))
        .await
        .expect("list reviewed repository tasks")
        .into_iter()
        .find(|task| task.id == detail.task.id)
        .expect("list contains reviewed task");
    assert_task_readiness(&listed_task, "completed", "review_approved");
}

#[tokio::test]
async fn completed_task_cancel_maps_to_the_not_cancellable_api_contract() {
    let fixture = support::task_manager_fixture(1).await;
    let completion = fixture.runner.push_completion_gate();
    let task = fixture.enqueue_tasks(1, true).await.remove(0);
    fixture.wait_for_status(task.id, TaskStatus::Running).await;
    completion.release.notify_one();
    fixture
        .wait_for_status(task.id, TaskStatus::Completed)
        .await;

    let security = support::SecurityFixture::production();
    let backend = application_backend(
        &fixture,
        &security,
        MutationGate::new(fixture.state.clone()),
        Duration::from_secs(2),
        Arc::new(AtomicBool::new(false)),
    );
    let error = backend
        .cancel_task(&establish_session(&security).await.auth, task.id)
        .await
        .expect_err("a completed task cannot be cancelled");

    assert_eq!(error.status, StatusCode::CONFLICT);
    assert_eq!(error.code, "TASK_NOT_CANCELLABLE");
    assert!(!error.retryable);
}

fn assert_task_readiness(
    task: &coding_agent_api::TaskDto,
    expected_status: &str,
    expected_readiness: &str,
) {
    let task = serde_json::to_value(task).expect("serialize task DTO");
    assert_eq!(task["status"], expected_status);
    assert_eq!(task["delivery_readiness"], expected_readiness);
}

fn dto_task_id(task: &coding_agent_api::TaskDto) -> TaskId {
    task.id
        .to_string()
        .parse()
        .expect("TaskDto carries a valid task UUID")
}

async fn occupied_fixture() -> support::TaskManagerFixture {
    let fixture = support::task_manager_fixture(1).await;
    fixture.runner.push_blocking(1);
    let occupied = fixture.enqueue_tasks(1, true).await.remove(0);
    fixture
        .wait_for_status(occupied.id, TaskStatus::Running)
        .await;
    fixture.wait_for_runner_start(occupied.id).await;
    fixture
}

async fn occupied_retry_fixture(
    failure_code: &str,
) -> (support::TaskManagerFixture, coding_agent_domain::Task) {
    let fixture = support::task_manager_fixture(1).await;
    fixture.runner.push_failure(support::failure(failure_code));
    let source = fixture.enqueue_tasks(1, true).await.remove(0);
    fixture.wait_for_status(source.id, TaskStatus::Failed).await;
    let terminal = fixture.load(source.id).await;

    fixture.runner.push_blocking(1);
    let occupied = fixture.enqueue_tasks(1, true).await.remove(0);
    fixture
        .wait_for_status(occupied.id, TaskStatus::Running)
        .await;
    fixture.wait_for_runner_start(occupied.id).await;
    (fixture, terminal)
}

#[tokio::test]
async fn concurrent_same_request_id_commits_one_task_and_one_queued_event() {
    let fixture = occupied_fixture().await;
    let security = support::SecurityFixture::production();
    let backend = application_backend(
        &fixture,
        &security,
        MutationGate::new(fixture.state.clone()),
        Duration::from_secs(2),
        Arc::new(AtomicBool::new(false)),
    );
    let auth = AuthContext {
        session_id: "already-authorized".to_owned(),
    };
    let client_request_id = ClientRequestId::new();
    let request = CreateTaskRequest {
        client_request_id,
        repository_id: fixture.repository.id,
        prompt: "  one durable prompt  ".to_owned(),
    };

    let mut calls = Vec::new();
    for _ in 0..16 {
        let backend = backend.clone();
        let auth = auth.clone();
        let request = request.clone();
        calls.push(tokio::spawn(async move {
            backend.create_task(&auth, request).await
        }));
    }

    let mut created = 0;
    let mut existing = 0;
    let mut task_ids = Vec::new();
    for call in calls {
        match call.await.expect("join create call").expect("create task") {
            CreateResult::Created(task) => {
                created += 1;
                task_ids.push(task.id);
                assert_eq!(task.prompt, "one durable prompt");
            }
            CreateResult::Existing(task) => {
                existing += 1;
                task_ids.push(task.id);
                assert_eq!(task.prompt, "one durable prompt");
            }
        }
    }
    assert_eq!((created, existing), (1, 15));
    assert!(task_ids.iter().all(|id| *id == task_ids[0]));

    let snapshot = fixture.store.bootstrap_snapshot().await.unwrap();
    let stored = snapshot
        .tasks
        .iter()
        .filter(|task| task.client_request_id == client_request_id)
        .collect::<Vec<_>>();
    assert_eq!(stored.len(), 1);
    let page = fixture
        .store
        .task_events_after(stored[0].id, coding_agent_domain::EventCursor::ZERO, 100)
        .await
        .unwrap();
    assert_eq!(page.events.len(), 1);
}

#[tokio::test]
async fn production_create_path_enforces_queue_cap_without_hiding_idempotency_results() {
    let fixture = occupied_fixture().await;
    let security = support::SecurityFixture::production();
    let backend = application_backend_with_queue_limit(
        &fixture,
        &security,
        MutationGate::new(fixture.state.clone()),
        Duration::from_secs(2),
        NonZeroU32::new(1).unwrap(),
        Arc::new(AtomicBool::new(false)),
    );
    let auth = AuthContext {
        session_id: "already-authorized".to_owned(),
    };
    let client_request_id = ClientRequestId::new();
    let canonical = CreateTaskRequest {
        client_request_id,
        repository_id: fixture.repository.id,
        prompt: "the only queued request".to_owned(),
    };

    assert!(matches!(
        backend.create_task(&auth, canonical.clone()).await.unwrap(),
        CreateResult::Created(_)
    ));
    assert!(matches!(
        backend.create_task(&auth, canonical).await.unwrap(),
        CreateResult::Existing(_)
    ));

    let conflict = backend
        .create_task(
            &auth,
            CreateTaskRequest {
                client_request_id,
                repository_id: fixture.repository.id,
                prompt: "different input for the same request id".to_owned(),
            },
        )
        .await
        .expect_err("idempotency conflict wins before queue capacity");
    assert_eq!(conflict.status, StatusCode::CONFLICT);
    assert_eq!(conflict.code, "IDEMPOTENCY_CONFLICT");

    let full = backend
        .create_task(
            &auth,
            CreateTaskRequest {
                client_request_id: ClientRequestId::new(),
                repository_id: fixture.repository.id,
                prompt: "new request beyond queue capacity".to_owned(),
            },
        )
        .await
        .expect_err("production create must not bypass the queue cap");
    assert_eq!(full.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(full.code, "TASK_QUEUE_FULL");
    assert!(full.retryable);
    assert_eq!(full.details["queued_tasks"], serde_json::json!(1));
    assert_eq!(full.details["max_queued_tasks"], serde_json::json!(1));
}

#[tokio::test]
async fn concurrent_retry_returns_one_direct_child_with_one_created_response() {
    let (fixture, terminal) = occupied_retry_fixture("RETRY_SOURCE_FAILED").await;

    let security = support::SecurityFixture::production();
    let backend = application_backend(
        &fixture,
        &security,
        MutationGate::new(fixture.state.clone()),
        Duration::from_secs(2),
        Arc::new(AtomicBool::new(false)),
    );
    let auth = AuthContext {
        session_id: "already-authorized".to_owned(),
    };
    let mut calls = Vec::new();
    for _ in 0..16 {
        let backend = backend.clone();
        let auth = auth.clone();
        calls.push(tokio::spawn(async move {
            backend.retry_task(&auth, terminal.id).await
        }));
    }

    let mut created = 0;
    let mut existing = 0;
    let mut child_ids = Vec::new();
    for call in calls {
        match call.await.expect("join retry").expect("retry task") {
            CreateResult::Created(task) => {
                created += 1;
                child_ids.push(task.id);
                assert_eq!(task.retry_of, Some(terminal.id.as_uuid()));
            }
            CreateResult::Existing(task) => {
                existing += 1;
                child_ids.push(task.id);
                assert_eq!(task.retry_of, Some(terminal.id.as_uuid()));
            }
        }
    }
    assert_eq!((created, existing), (1, 15));
    assert!(child_ids.iter().all(|id| *id == child_ids[0]));
    let children = fixture
        .store
        .bootstrap_snapshot()
        .await
        .unwrap()
        .tasks
        .into_iter()
        .filter(|task| task.retry_of == Some(terminal.id))
        .collect::<Vec<_>>();
    assert_eq!(children.len(), 1);
}

#[tokio::test]
async fn production_retry_path_enforces_queue_cap() {
    let (fixture, terminal) = occupied_retry_fixture("QUEUE_LIMITED_RETRY_SOURCE").await;
    fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "fill the only queue slot"),
            support::deadline(),
        )
        .await
        .expect("fill queue through the sole writer");

    let security = support::SecurityFixture::production();
    let backend = application_backend_with_queue_limit(
        &fixture,
        &security,
        MutationGate::new(fixture.state.clone()),
        Duration::from_secs(2),
        NonZeroU32::new(1).unwrap(),
        Arc::new(AtomicBool::new(false)),
    );
    let error = backend
        .retry_task(
            &AuthContext {
                session_id: "already-authorized".to_owned(),
            },
            terminal.id,
        )
        .await
        .expect_err("production retry must not bypass the queue cap");
    assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(error.code, "TASK_QUEUE_FULL");
    assert!(error.retryable);
    assert_eq!(error.details["queued_tasks"], serde_json::json!(1));
    assert_eq!(error.details["max_queued_tasks"], serde_json::json!(1));
    assert!(
        fixture
            .store
            .bootstrap_snapshot()
            .await
            .unwrap()
            .tasks
            .iter()
            .all(|task| task.retry_of != Some(terminal.id))
    );
}

#[tokio::test]
async fn prompt_validation_is_trimmed_and_counts_unicode_scalars() {
    let fixture = occupied_fixture().await;
    let security = support::SecurityFixture::production();
    let backend = application_backend(
        &fixture,
        &security,
        MutationGate::new(fixture.state.clone()),
        Duration::from_secs(2),
        Arc::new(AtomicBool::new(false)),
    );
    let auth = AuthContext {
        session_id: "already-authorized".to_owned(),
    };
    for prompt in ["   ".to_owned(), "界".repeat(50_001)] {
        let error = backend
            .create_task(
                &auth,
                CreateTaskRequest {
                    client_request_id: ClientRequestId::new(),
                    repository_id: fixture.repository.id,
                    prompt,
                },
            )
            .await
            .expect_err("invalid prompt");
        assert_eq!(error.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(error.code, "INVALID_PROMPT");
    }
}

#[tokio::test]
async fn bounded_busy_exhaustion_is_503_and_commits_nothing() {
    let fixture = occupied_fixture().await;
    let security = support::SecurityFixture::production();
    let backend = application_backend(
        &fixture,
        &security,
        MutationGate::new(fixture.state.clone()),
        Duration::from_millis(75),
        Arc::new(AtomicBool::new(false)),
    );
    let before = fixture
        .store
        .bootstrap_snapshot()
        .await
        .unwrap()
        .tasks
        .len();
    fixture.force_claim_busy(true).await;
    let error = backend
        .create_task(
            &AuthContext {
                session_id: "already-authorized".to_owned(),
            },
            CreateTaskRequest {
                client_request_id: ClientRequestId::new(),
                repository_id: fixture.repository.id,
                prompt: "must not commit".to_owned(),
            },
        )
        .await
        .expect_err("busy writer is bounded");
    assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error.code, "STORE_BUSY");
    assert!(error.retryable);
    fixture.force_claim_busy(false).await;
    assert_eq!(
        fixture
            .store
            .bootstrap_snapshot()
            .await
            .unwrap()
            .tasks
            .len(),
        before
    );
}

#[tokio::test]
async fn degraded_gate_blocks_data_mutations_but_quit_fires_only_after_body_eof() {
    let fixture = occupied_fixture().await;
    fixture.state.set(ServiceState::StoreDegraded).unwrap();
    let security = support::SecurityFixture::production();
    let session = establish_session(&security).await;
    let gate = MutationGate::new(fixture.state.clone());
    let quit_signal = Arc::new(AtomicBool::new(false));
    let backend = application_backend(
        &fixture,
        &security,
        gate,
        Duration::from_secs(2),
        quit_signal.clone(),
    );
    let auth = session.auth.clone();

    let create = backend
        .create_task(
            &auth,
            CreateTaskRequest {
                client_request_id: ClientRequestId::new(),
                repository_id: fixture.repository.id,
                prompt: "blocked while degraded".to_owned(),
            },
        )
        .await
        .expect_err("degraded create is blocked");
    assert_eq!(create.code, "STORE_DEGRADED");
    let repository = backend
        .add_repository(
            &auth,
            coding_agent_api::AddRepositoryRequest {
                path: PathBuf::from("secret-path-that-must-not-be-probed"),
            },
        )
        .await
        .expect_err("degraded repository add is blocked");
    assert_eq!(repository.code, "STORE_DEGRADED");

    let router = build_api_router(
        backend.clone(),
        Arc::new(security.manager.clone()),
        backend.clone(),
    );
    let response = router
        .oneshot(mutation_request(
            "/api/app/quit",
            &security.expected_host,
            &security.public_origin,
            &session.cookie,
            &session.csrf,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert!(!quit_signal.load(Ordering::SeqCst));
    assert_eq!(fixture.state.current().state, ServiceState::StoreDegraded);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
        serde_json::json!({"status":"shutting_down"})
    );
    assert!(quit_signal.load(Ordering::SeqCst));
    assert_eq!(fixture.state.current().state, ServiceState::Quiescing);
}

#[tokio::test]
async fn dropping_quit_response_before_eof_does_not_close_gate_or_signal() {
    let fixture = occupied_fixture().await;
    let security = support::SecurityFixture::production();
    let session = establish_session(&security).await;
    let gate = MutationGate::new(fixture.state.clone());
    let quit_signal = Arc::new(AtomicBool::new(false));
    let backend = application_backend(
        &fixture,
        &security,
        gate.clone(),
        Duration::from_secs(2),
        quit_signal.clone(),
    );
    let router = build_api_router(backend.clone(), Arc::new(security.manager.clone()), backend);
    let response = router
        .oneshot(mutation_request(
            "/api/app/quit",
            &security.expected_host,
            &security.public_origin,
            &session.cookie,
            &session.csrf,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    drop(response);
    tokio::task::yield_now().await;
    assert!(!quit_signal.load(Ordering::SeqCst));
    assert_eq!(fixture.state.current().state, ServiceState::Ready);
    drop(gate.enter_data_mutation().expect("gate remains open"));
}

#[tokio::test]
async fn wait_for_idle_observes_the_final_concurrent_guard_drop() {
    let state = coding_agent_app::ServiceStateController::new(ServiceState::Ready);
    let gate = MutationGate::new(state);
    let first = gate.enter_data_mutation().unwrap();
    let second = gate.enter_data_mutation().unwrap();
    let waiting_gate = gate.clone();
    let waiter = tokio::spawn(async move {
        waiting_gate.wait_for_idle().await;
    });

    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    drop(first);
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    drop(second);
    tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("final guard drop cannot be missed")
        .expect("join idle waiter");
}

#[test]
fn independently_quiescing_service_rejects_new_data_mutations() {
    let state = coding_agent_app::ServiceStateController::new(ServiceState::Ready);
    let gate = MutationGate::new(state.clone());

    state.set(ServiceState::Quiescing).unwrap();

    let error = match gate.enter_data_mutation() {
        Ok(_) => {
            panic!("quiescing must reject a mutation even before the gate is explicitly closed")
        }
        Err(error) => error,
    };
    assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error.code, "APP_SHUTTING_DOWN");
}

#[tokio::test(flavor = "current_thread")]
async fn info_logs_keep_stable_ids_and_codes_but_redact_prompt_path_and_secrets() {
    let fixture = occupied_fixture().await;
    let security = support::SecurityFixture::production();
    let backend = application_backend(
        &fixture,
        &security,
        MutationGate::new(fixture.state.clone()),
        Duration::from_secs(2),
        Arc::new(AtomicBool::new(false)),
    );
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let writer = TraceWriter(bytes.clone());
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer(move || writer.clone())
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    tracing::dispatcher::set_global_default(dispatch)
        .expect("server log test owns the process-global tracing subscriber");
    let prompt = "known-ultra-secret-prompt";
    let task = backend
        .create_task(
            &AuthContext {
                session_id: "known-session-secret".to_owned(),
            },
            CreateTaskRequest {
                client_request_id: ClientRequestId::new(),
                repository_id: fixture.repository.id,
                prompt: prompt.to_owned(),
            },
        )
        .await
        .expect("create logged task");
    let task_id = match task {
        CreateResult::Created(task) | CreateResult::Existing(task) => task.id,
    };
    let full_path = if cfg!(windows) {
        PathBuf::from(r"C:\known\private\repository")
    } else {
        PathBuf::from("/known/private/repository")
    };
    let error = backend
        .add_repository(
            &AuthContext {
                session_id: "known-session-secret".to_owned(),
            },
            coding_agent_api::AddRepositoryRequest {
                path: full_path.clone(),
            },
        )
        .await
        .expect_err("missing path is a stable discovery error");
    let output = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
    assert!(output.contains(&task_id.to_string()));
    assert!(output.contains(&fixture.repository.id.to_string()));
    assert!(output.contains(&error.code));
    for secret in [
        prompt,
        full_path.to_string_lossy().as_ref(),
        "known-session-secret",
        security.initial_launch_token.as_str(),
    ] {
        assert!(!output.contains(secret), "info log leaked {secret:?}");
    }
}

struct BrowserSession {
    auth: AuthContext,
    cookie: String,
    csrf: String,
}

async fn establish_session(fixture: &support::SecurityFixture) -> BrowserSession {
    let parts = Request::builder()
        .method(Method::POST)
        .uri("/api/session/exchange")
        .header(HOST, &fixture.expected_host)
        .header(ORIGIN, &fixture.public_origin)
        .body(())
        .unwrap()
        .into_parts()
        .0;
    let exchange = RequestSecurity::exchange(
        &fixture.manager,
        &parts,
        fixture.initial_launch_token.as_str(),
    )
    .await
    .expect("exchange initial token");
    session_from_exchange(fixture, exchange)
}

fn session_from_exchange(
    fixture: &support::SecurityFixture,
    exchange: SessionExchange,
) -> BrowserSession {
    let cookie = exchange
        .set_cookie
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let parts = Request::builder()
        .uri("/api/bootstrap")
        .header(HOST, &fixture.expected_host)
        .header(COOKIE, &cookie)
        .body(())
        .unwrap()
        .into_parts()
        .0;
    let auth = RequestSecurity::authorize_read(&fixture.manager, &parts).unwrap();
    let csrf = fixture.manager.csrf_for_auth(&auth).unwrap();
    BrowserSession { auth, cookie, csrf }
}

fn mutation_request(
    uri: &str,
    host: &str,
    origin: &str,
    cookie: &str,
    csrf: &str,
) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(HOST, host)
        .header(ORIGIN, origin)
        .header(COOKIE, cookie)
        .header("x-csrf-token", csrf)
        .body(Body::empty())
        .unwrap()
}

#[derive(Clone)]
struct TraceWriter(Arc<Mutex<Vec<u8>>>);

impl Write for TraceWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
