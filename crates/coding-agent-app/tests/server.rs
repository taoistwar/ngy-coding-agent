mod support;

use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use coding_agent_api::{
    ApiBackend, AuthContext, CreateResult, CreateTaskRequest, LiveEventItem, RequestSecurity,
    SessionExchange, SseBackend, build_api_router,
};
use coding_agent_app::{ApplicationBackend, MutationGate, RepositoryDiscovery, ServiceState};
use coding_agent_domain::{ClientRequestId, NewTask, TaskStatus};
use coding_agent_store::{CreateTaskOutcome, TaskTransition, TransitionOutcome};
use futures_util::StreamExt as _;
use http::header::{COOKIE, HOST, ORIGIN};
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
    Arc::new(ApplicationBackend::new(
        fixture.store.clone(),
        fixture.writer.clone(),
        fixture.dispatcher.clone(),
        fixture.manager.clone(),
        RepositoryDiscovery::new(std::env::temp_dir()),
        None,
        security.manager.clone(),
        fixture.state.clone(),
        gate,
        support::timestamp(),
        4,
        write_budget,
        Arc::new(move || quit_signal.store(true, Ordering::SeqCst)),
    ))
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

async fn occupied_fixture() -> support::TaskManagerFixture {
    let fixture = support::task_manager_fixture(1).await;
    fixture.runner.push_blocking(1);
    let occupied = fixture.enqueue_tasks(1, true).await.remove(0);
    fixture
        .wait_for_status(occupied.id, TaskStatus::Running)
        .await;
    fixture
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
async fn concurrent_retry_returns_one_direct_child_with_one_created_response() {
    let fixture = occupied_fixture().await;
    let source = match fixture
        .store
        .create_task(
            NewTask::try_new(
                ClientRequestId::new(),
                fixture.repository.id,
                "retry source",
            )
            .unwrap(),
        )
        .await
        .unwrap()
    {
        CreateTaskOutcome::Created { task, .. } | CreateTaskOutcome::Existing { task } => task,
    };
    let running = match fixture
        .store
        .transition_with_event(source.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .unwrap()
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("source must start"),
    };
    let terminal = match fixture
        .store
        .transition_with_event(running.id, TaskStatus::Running, TaskTransition::Completed)
        .await
        .unwrap()
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("source must complete"),
    };

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
