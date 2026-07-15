mod support;

use std::num::{NonZeroU16, NonZeroU32};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use coding_agent_app::{
    InstanceLock, RuntimeDescriptor, RuntimeDescriptorError, SecurityManager, SecuritySeed,
    StartupError, StartupOutcome, StartupPhase, StartupPhaseController, SystemSecurityClock,
    build_runtime_router, launch,
};
use coding_agent_domain::{
    CanonicalPath, ClientRequestId, NewRepository, NewTask, TaskEventKind, TaskStatus, UtcTimestamp,
};
use coding_agent_store::{CreateTaskOutcome, RegisterRepositoryOutcome, Store};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio_util::sync::CancellationToken;

const DEVELOPMENT_PUBLIC_ORIGIN: &str = "http://127.0.0.1:5173";

#[test]
fn permanent_lock_selects_exactly_one_primary_without_replacing_the_lock_file() {
    let temp = tempfile::tempdir().expect("create lock fixture");
    let lock_path = temp.path().join("instance.lock");

    let primary = InstanceLock::try_acquire(&lock_path)
        .expect("open permanent lock")
        .expect("first process owns the lock");
    let original = std::fs::metadata(&lock_path).expect("lock path remains published");

    assert!(
        InstanceLock::try_acquire(&lock_path)
            .expect("contended lock is not an I/O error")
            .is_none(),
        "the second process must become a secondary"
    );
    assert_eq!(
        std::fs::metadata(&lock_path)
            .expect("contended lock path remains published")
            .len(),
        original.len()
    );

    drop(primary);
    assert!(
        InstanceLock::try_acquire(&lock_path)
            .expect("reopen permanent lock")
            .is_some(),
        "dropping the owner releases the existing lock file"
    );
}

#[test]
fn runtime_descriptor_is_atomically_reopenable_private_and_secret_redacted() {
    let temp = tempfile::tempdir().expect("create descriptor fixture");
    let path = temp.path().join("instance.json");
    let seed = SecuritySeed::generate().expect("generate launcher secret");
    let descriptor = RuntimeDescriptor::new(
        uuid::Uuid::new_v4(),
        NonZeroU32::new(std::process::id()).expect("test process ID is nonzero"),
        NonZeroU16::new(43_121).expect("test port is nonzero"),
        UtcTimestamp::parse_rfc3339("2026-07-15T00:00:00Z").expect("fixed timestamp"),
        seed.launcher_secret().clone(),
    )
    .expect("construct valid descriptor");

    descriptor.publish(&path).expect("publish descriptor");
    let reopened = RuntimeDescriptor::read(&path).expect("reopen descriptor");

    assert_eq!(reopened, descriptor);
    assert!(!format!("{reopened:?}").contains(seed.launcher_secret().as_str()));
    let temporary_descriptors = std::fs::read_dir(temp.path())
        .expect("list descriptor directory")
        .map(|entry| entry.expect("read descriptor directory entry").file_name())
        .filter(|name| {
            let name = name.to_string_lossy();
            name.starts_with("instance.json.") && name.ends_with(".tmp")
        })
        .collect::<Vec<_>>();
    assert!(temporary_descriptors.is_empty());
    assert_private_file(&path);
}

#[test]
fn atomic_descriptor_replacement_never_exposes_a_partial_document() {
    let temp = tempfile::tempdir().expect("create replacement fixture");
    let path = temp.path().join("instance.json");
    let first = test_descriptor(43_121);
    let second = test_descriptor(43_122);
    first.publish(&path).expect("publish initial descriptor");

    let stop = Arc::new(AtomicBool::new(false));
    let observations = Arc::new(AtomicUsize::new(0));
    let reader_path = path.clone();
    let reader_stop = stop.clone();
    let reader_observations = observations.clone();
    let (first_observation, observed_once) = std::sync::mpsc::sync_channel(1);
    let expected_first = first.clone();
    let expected_second = second.clone();
    let reader = std::thread::spawn(move || {
        let mut announced = false;
        while !reader_stop.load(Ordering::Acquire) {
            match RuntimeDescriptor::read(&reader_path) {
                Ok(observed) => {
                    assert!(observed == expected_first || observed == expected_second);
                    reader_observations.fetch_add(1, Ordering::Relaxed);
                    if !announced {
                        first_observation
                            .send(())
                            .expect("announce first complete descriptor observation");
                        announced = true;
                    }
                }
                Err(RuntimeDescriptorError::Io(error)) if transient_replacement_error(&error) => {
                    std::thread::yield_now();
                }
                Err(error) => panic!("descriptor reader observed a partial document: {error}"),
            }
        }
    });

    observed_once
        .recv_timeout(Duration::from_secs(2))
        .expect("reader observes the initial complete descriptor");

    for index in 0..20 {
        let descriptor = if index % 2 == 0 { &second } else { &first };
        descriptor
            .publish(&path)
            .expect("atomically replace descriptor");
    }
    stop.store(true, Ordering::Release);
    reader.join().expect("join descriptor reader");
    assert!(observations.load(Ordering::Relaxed) > 0);
    assert_eq!(
        RuntimeDescriptor::read(&path).expect("read final descriptor"),
        first
    );
}

#[test]
fn malformed_runtime_descriptor_is_rejected_before_network_contact() {
    let temp = tempfile::tempdir().expect("create malformed descriptor fixture");
    let path = temp.path().join("instance.json");
    let mut file = coding_agent_app::PrivateFile::create_new(&path)
        .expect("create owner-only malformed descriptor");
    std::io::Write::write_all(&mut file, br#"{"port":43121}"#).expect("write malformed descriptor");
    std::io::Write::flush(&mut file).expect("flush malformed descriptor");

    assert!(RuntimeDescriptor::read(&path).is_err());
}

#[tokio::test(start_paused = true)]
async fn malformed_descriptor_times_out_without_contacting_its_embedded_port() {
    let fixture = support::StartupFixture::new();
    fixture.prepare();
    let lock = InstanceLock::try_acquire(&fixture.paths.instance_lock)
        .expect("acquire malformed-descriptor lock")
        .expect("own malformed-descriptor lock");
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind network-contact sentinel");
    let port = listener.local_addr().expect("sentinel address").port();
    let mut file = coding_agent_app::PrivateFile::create_new(&fixture.paths.instance_descriptor)
        .expect("create private malformed descriptor");
    std::io::Write::write_all(&mut file, format!(r#"{{"port":{port}}}"#).as_bytes())
        .expect("write malformed descriptor with a live port");
    std::io::Write::flush(&mut file).expect("flush malformed descriptor");
    drop(file);
    let contact = tokio::spawn(async move { listener.accept().await });

    let secondary = tokio::spawn(launch(fixture.dependencies(support::StartupBehavior {
        panic_on_store_open: true,
        ..support::StartupBehavior::default()
    })));
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    let result = secondary.await.expect("join malformed secondary");

    assert!(matches!(result, Err(StartupError::PrimaryUnverified)));
    assert!(
        !contact.is_finished(),
        "malformed data must not select a port"
    );
    contact.abort();
    assert_eq!(fixture.calls.store_opens(), 0);
    assert_eq!(fixture.calls.listener_binds(), 0);
    assert_eq!(fixture.calls.messages().len(), 1);
    drop(lock);
}

#[test]
fn startup_phase_is_separate_and_transitions_once_to_ready() {
    let phase = StartupPhaseController::new();
    assert_eq!(phase.current(), StartupPhase::Starting);
    assert!(phase.mark_ready());
    assert_eq!(phase.current(), StartupPhase::Ready);
    assert!(!phase.mark_ready(), "Ready is an idempotent terminal phase");
}

#[tokio::test]
async fn path_preparation_failure_reports_once_before_lock_store_or_listener() {
    let fixture = support::StartupFixture::new();
    let result = launch(fixture.dependencies(support::StartupBehavior {
        prepare_error: Some(std::io::ErrorKind::PermissionDenied),
        ..support::StartupBehavior::default()
    }))
    .await;

    assert!(matches!(result, Err(StartupError::Paths(_))));
    assert_eq!(fixture.calls.store_opens(), 0);
    assert_eq!(fixture.calls.listener_binds(), 0);
    assert_eq!(fixture.calls.browser_urls(), Vec::<String>::new());
    assert_eq!(fixture.calls.messages().len(), 1);
    assert!(!fixture.paths.instance_lock.exists());
    assert!(!fixture.paths.database_path.exists());
    assert!(!fixture.paths.instance_descriptor.exists());
}

#[tokio::test]
async fn database_open_or_migration_failure_stops_before_listener_publication() {
    let fixture = support::StartupFixture::new();
    fixture.prepare();
    let invalid_database = b"not a SQLite database";
    let shutdown_marker = b"{\"error_code\":\"SHUTDOWN_PERSISTENCE_FAILED\"}";
    std::fs::write(&fixture.paths.database_path, invalid_database)
        .expect("install invalid database");
    std::fs::write(&fixture.paths.unclean_shutdown, shutdown_marker)
        .expect("install existing shutdown marker");

    let result = launch(fixture.dependencies(support::StartupBehavior::default())).await;

    assert!(matches!(result, Err(StartupError::Store(_))));
    assert_eq!(fixture.calls.store_opens(), 1);
    assert_eq!(fixture.calls.listener_binds(), 0);
    assert_eq!(fixture.calls.browser_urls(), Vec::<String>::new());
    assert_eq!(fixture.calls.messages().len(), 1);
    assert!(!fixture.paths.instance_descriptor.exists());
    assert_eq!(
        std::fs::read(&fixture.paths.database_path).expect("read preserved invalid database"),
        invalid_database
    );
    assert_eq!(
        std::fs::read(&fixture.paths.unclean_shutdown).expect("read preserved shutdown marker"),
        shutdown_marker
    );
    assert!(
        InstanceLock::try_acquire(&fixture.paths.instance_lock)
            .expect("database failure releases lock")
            .is_some()
    );
}

#[tokio::test]
async fn primary_recovers_incomplete_tasks_before_publishing_ready_descriptor() {
    let fixture = support::StartupFixture::new();
    fixture.prepare();
    let store = Store::open(&fixture.paths.database_path)
        .await
        .expect("open seed store");
    store.migrate().await.expect("migrate seed store");
    let repository_root = fixture.paths.data_dir.join("seed-repository");
    std::fs::create_dir_all(&repository_root).expect("create seed repository paths");
    let canonical =
        CanonicalPath::try_from_canonical(repository_root).expect("construct canonical seed path");
    let repository = match store
        .register_repository(NewRepository {
            selected_path: canonical.clone(),
            display_name: "seed".to_owned(),
            git_root: canonical.clone(),
            cargo_workspace_root: canonical,
        })
        .await
        .expect("register seed repository")
    {
        RegisterRepositoryOutcome::Created(repository) => repository,
        RegisterRepositoryOutcome::Existing(_) => panic!("seed repository must be new"),
    };
    let queued = match store
        .create_task(
            NewTask::try_new(ClientRequestId::new(), repository.id, "recover this task")
                .expect("construct queued task"),
        )
        .await
        .expect("create queued seed task")
    {
        CreateTaskOutcome::Created { task, .. } => task,
        CreateTaskOutcome::Existing { .. } => panic!("seed task must be new"),
    };
    drop(store);

    let outcome = launch(fixture.dependencies(support::StartupBehavior::default()))
        .await
        .expect("start recovered primary");
    let StartupOutcome::Primary(primary) = outcome else {
        panic!("seed database must start as primary");
    };
    let observer = Store::open(&fixture.paths.database_path)
        .await
        .expect("open recovery observer");
    observer.migrate().await.expect("migrate recovery observer");
    let detail = observer
        .task_detail(queued.id)
        .await
        .expect("read recovered task")
        .expect("recovered task exists");

    assert_eq!(detail.task.status, TaskStatus::Interrupted);
    assert_eq!(
        detail
            .task
            .failure
            .as_ref()
            .map(|failure| failure.code.as_str()),
        Some("APP_RESTARTED")
    );
    assert_eq!(
        detail.timeline.last().map(|event| event.kind),
        Some(TaskEventKind::TaskInterrupted)
    );
    assert!(fixture.paths.instance_descriptor.exists());
    drop(primary);
}

#[tokio::test]
async fn primary_composes_real_listener_and_stays_alive_when_browser_open_fails() {
    let fixture = support::StartupFixture::new();
    let outcome = launch(fixture.dependencies(support::StartupBehavior {
        browser_fails: true,
        ..support::StartupBehavior::default()
    }))
    .await
    .expect("primary startup succeeds despite browser failure");
    let StartupOutcome::Primary(primary) = outcome else {
        panic!("the first process must become primary");
    };

    assert_eq!(primary.startup_phase(), StartupPhase::Ready);
    assert!(!primary.browser_opened());
    assert_eq!(fixture.calls.store_opens(), 1);
    assert_eq!(fixture.calls.listener_binds(), 1);
    let descriptor = RuntimeDescriptor::read(&fixture.paths.instance_descriptor)
        .expect("primary atomically publishes its descriptor");
    assert_eq!(descriptor.instance_id(), primary.instance_id());
    assert_eq!(descriptor.port().get(), primary.port());
    let urls = fixture.calls.browser_urls();
    assert_eq!(urls.len(), 1);
    assert!(urls[0].starts_with(&format!("http://127.0.0.1:{}/#token=", primary.port())));
    let messages = fixture.calls.messages();
    assert_eq!(messages.len(), 1);
    assert!(messages[0].1.contains(&urls[0]));

    drop(primary);
    assert!(!fixture.paths.instance_descriptor.exists());
    let reacquired = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Some(lock) = InstanceLock::try_acquire(&fixture.paths.instance_lock)
                .expect("reopen primary lock")
            {
                return lock;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server task releases its lock keepalive after abort");
    drop(reacquired);
}

#[tokio::test]
async fn development_primary_uses_vite_origin_listener_proxy_host_and_vite_browser_target() {
    let fixture = support::StartupFixture::new();
    let dependencies = fixture
        .dependencies(support::StartupBehavior::default())
        .with_development_public_origin(DEVELOPMENT_PUBLIC_ORIGIN);
    let outcome = launch(dependencies)
        .await
        .expect("start development primary");
    let StartupOutcome::Primary(primary) = outcome else {
        panic!("the first development process must become primary");
    };

    let urls = fixture.calls.browser_urls();
    assert_eq!(urls.len(), 1);
    assert!(
        urls[0].starts_with(&format!("{DEVELOPMENT_PUBLIC_ORIGIN}/#token=")),
        "development must open the Vite public origin"
    );
    let token = fragment_token(&urls[0]);
    let listener_host = format!("127.0.0.1:{}", primary.port());

    assert_eq!(
        exchange_session_status(
            primary.port(),
            "127.0.0.1:5173",
            DEVELOPMENT_PUBLIC_ORIGIN,
            token,
        )
        .await,
        403,
        "Vite must rewrite Host to the exact Axum listener authority"
    );
    assert_eq!(
        exchange_session_status(
            primary.port(),
            &listener_host,
            &format!("http://{listener_host}"),
            token,
        )
        .await,
        403,
        "development must reject the listener origin"
    );
    assert_eq!(
        exchange_session_status(
            primary.port(),
            &listener_host,
            DEVELOPMENT_PUBLIC_ORIGIN,
            token,
        )
        .await,
        204,
        "the one Vite public origin and listener proxy Host must be accepted"
    );

    let secondary = launch(
        fixture
            .dependencies(support::StartupBehavior::default())
            .with_development_public_origin(DEVELOPMENT_PUBLIC_ORIGIN),
    )
    .await
    .expect("start development secondary");
    let StartupOutcome::Secondary(secondary) = secondary else {
        panic!("the contending development process must become secondary");
    };
    assert_eq!(secondary.instance_id(), primary.instance_id());
    assert!(secondary.browser_opened());
    let urls = fixture.calls.browser_urls();
    assert_eq!(urls.len(), 2);
    assert!(
        urls.iter()
            .all(|url| url.starts_with(&format!("{DEVELOPMENT_PUBLIC_ORIGIN}/#token="))),
        "both primary and reopen must target Vite"
    );

    drop(primary);
}

#[tokio::test]
async fn invalid_development_public_origin_fails_before_descriptor_or_browser_publication() {
    let fixture = support::StartupFixture::new();
    let result = launch(
        fixture
            .dependencies(support::StartupBehavior::default())
            .with_development_public_origin("http://localhost:5173"),
    )
    .await;

    assert!(matches!(result, Err(StartupError::Security(_))));
    assert!(!fixture.paths.instance_descriptor.exists());
    assert!(fixture.calls.browser_urls().is_empty());
}

#[tokio::test(start_paused = true)]
async fn secondary_waits_for_descriptor_and_ready_without_opening_store_or_listener() {
    let fixture = support::StartupFixture::new();
    fixture.prepare();
    let primary = FakePrimary::start(&fixture).await;
    let secondary = tokio::spawn(launch(fixture.dependencies(support::StartupBehavior {
        panic_on_store_open: true,
        ..support::StartupBehavior::default()
    })));

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    primary.publish();
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(
        !secondary.is_finished(),
        "a published Starting descriptor must not issue a token"
    );

    assert!(primary.phase.mark_ready());
    drive_until_finished(&secondary).await;
    let outcome = secondary
        .await
        .expect("join secondary")
        .expect("verified secondary reopens primary");
    let StartupOutcome::Secondary(secondary) = outcome else {
        panic!("the contending process must remain secondary");
    };
    assert_eq!(secondary.instance_id(), primary.descriptor.instance_id());
    assert!(secondary.browser_opened());
    assert_eq!(fixture.calls.store_opens(), 0);
    assert_eq!(fixture.calls.listener_binds(), 0);
    assert_eq!(fixture.calls.browser_urls().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn dead_primary_descriptor_times_out_at_ten_seconds_without_mutation() {
    let fixture = support::StartupFixture::new();
    fixture.prepare();
    let lock = InstanceLock::try_acquire(&fixture.paths.instance_lock)
        .expect("acquire fake live lock")
        .expect("own fake live lock");
    let seed = SecuritySeed::generate().expect("generate dead descriptor secret");
    let descriptor = RuntimeDescriptor::new(
        uuid::Uuid::new_v4(),
        NonZeroU32::new(i32::MAX as u32).expect("nonzero dead PID"),
        NonZeroU16::new(43_121).expect("nonzero dead port"),
        fixed_timestamp(),
        seed.launcher_secret().clone(),
    )
    .expect("construct dead descriptor");
    descriptor
        .publish(&fixture.paths.instance_descriptor)
        .expect("publish dead descriptor");

    let started = tokio::time::Instant::now();
    let secondary = tokio::spawn(launch(fixture.dependencies(support::StartupBehavior {
        panic_on_store_open: true,
        ..support::StartupBehavior::default()
    })));
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    let result = secondary.await.expect("join timed-out secondary");

    assert!(matches!(result, Err(StartupError::PrimaryUnverified)));
    assert_eq!(
        tokio::time::Instant::now().duration_since(started),
        Duration::from_secs(10)
    );
    assert_eq!(fixture.calls.store_opens(), 0);
    assert_eq!(fixture.calls.listener_binds(), 0);
    assert_eq!(fixture.calls.browser_urls(), Vec::<String>::new());
    assert_eq!(fixture.calls.messages().len(), 1);
    assert_eq!(
        RuntimeDescriptor::read(&fixture.paths.instance_descriptor)
            .expect("secondary leaves descriptor untouched"),
        descriptor
    );
    assert!(
        InstanceLock::try_acquire(&fixture.paths.instance_lock)
            .expect("probe live lock")
            .is_none()
    );
    drop(lock);
}

#[tokio::test(start_paused = true)]
async fn wrong_launcher_secret_never_reopens_or_mutates_the_locked_primary() {
    let fixture = support::StartupFixture::new();
    fixture.prepare();
    let primary = FakePrimary::start(&fixture).await;
    assert!(primary.phase.mark_ready());
    let wrong = primary.publish_with_secret(
        SecuritySeed::generate()
            .expect("generate wrong launcher secret")
            .launcher_secret()
            .clone(),
    );

    let secondary = tokio::spawn(launch(fixture.dependencies(support::StartupBehavior {
        panic_on_store_open: true,
        ..support::StartupBehavior::default()
    })));
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    let result = secondary.await.expect("join wrong-secret secondary");

    assert!(matches!(result, Err(StartupError::PrimaryUnverified)));
    assert_eq!(fixture.calls.store_opens(), 0);
    assert_eq!(fixture.calls.listener_binds(), 0);
    assert_eq!(fixture.calls.browser_urls(), Vec::<String>::new());
    assert_eq!(fixture.calls.messages().len(), 1);
    assert_eq!(
        RuntimeDescriptor::read(&fixture.paths.instance_descriptor)
            .expect("wrong-secret descriptor remains untouched"),
        wrong
    );
}

#[tokio::test]
async fn listener_binding_retries_are_finite_and_never_publish_a_descriptor() {
    let fixture = support::StartupFixture::new();
    let result = launch(fixture.dependencies(support::StartupBehavior {
        listener_failures: 3,
        ..support::StartupBehavior::default()
    }))
    .await;

    assert!(matches!(result, Err(StartupError::Listener(_))));
    assert_eq!(fixture.calls.store_opens(), 1);
    assert_eq!(fixture.calls.listener_binds(), 3);
    assert_eq!(fixture.calls.browser_urls(), Vec::<String>::new());
    assert_eq!(fixture.calls.messages().len(), 1);
    assert!(!fixture.paths.instance_descriptor.exists());
    assert!(
        InstanceLock::try_acquire(&fixture.paths.instance_lock)
            .expect("startup error releases lock")
            .is_some()
    );
}

async fn drive_until_finished<T>(task: &tokio::task::JoinHandle<T>) {
    for _ in 0..80 {
        if task.is_finished() {
            return;
        }
        tokio::time::advance(Duration::from_millis(25)).await;
        tokio::task::yield_now().await;
    }
    assert!(task.is_finished(), "task did not finish under virtual time");
}

fn fragment_token(url: &str) -> &str {
    url.split_once("/#token=")
        .map(|(_, token)| token)
        .expect("browser URL carries a fragment token")
}

async fn exchange_session_status(port: u16, host: &str, origin: &str, token: &str) -> u16 {
    let body = serde_json::to_string(&serde_json::json!({ "token": token }))
        .expect("encode exchange body");
    let request = format!(
        "POST /api/session/exchange HTTP/1.1\r\nHost: {host}\r\nOrigin: {origin}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut stream = tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port))
        .await
        .expect("connect to development primary");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write exchange request");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("read exchange response");
    let response = String::from_utf8(response).expect("HTTP response is UTF-8");
    response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse().ok())
        .expect("response has a numeric HTTP status")
}

struct FakePrimary {
    _lock: InstanceLock,
    descriptor_path: std::path::PathBuf,
    descriptor: RuntimeDescriptor,
    phase: StartupPhaseController,
    shutdown: CancellationToken,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl FakePrimary {
    async fn start(fixture: &support::StartupFixture) -> Self {
        let lock = InstanceLock::try_acquire(&fixture.paths.instance_lock)
            .expect("acquire fake primary lock")
            .expect("fake primary owns lock");
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind fake primary");
        let port = NonZeroU16::new(listener.local_addr().unwrap().port()).unwrap();
        let seed = SecuritySeed::generate().expect("generate fake primary security");
        let launcher_secret = seed.launcher_secret().clone();
        let security = SecurityManager::from_seed(
            seed,
            format!("http://127.0.0.1:{port}"),
            Arc::new(SystemSecurityClock),
        )
        .expect("construct fake primary security");
        let phase = StartupPhaseController::new();
        let instance_id = uuid::Uuid::new_v4();
        let router = build_runtime_router(
            Router::new(),
            instance_id,
            phase.clone(),
            security,
            Arc::new(support::FixedWallClock),
        );
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(server_shutdown.cancelled_owned())
                .await
        });
        let descriptor = RuntimeDescriptor::new(
            instance_id,
            NonZeroU32::new(std::process::id()).unwrap(),
            port,
            fixed_timestamp(),
            launcher_secret,
        )
        .expect("construct fake primary descriptor");
        Self {
            _lock: lock,
            descriptor_path: fixture.paths.instance_descriptor.clone(),
            descriptor,
            phase,
            shutdown,
            server,
        }
    }

    fn publish(&self) {
        self.descriptor
            .publish(&self.descriptor_path)
            .expect("publish fake primary descriptor");
    }

    fn publish_with_secret(
        &self,
        launcher_secret: coding_agent_app::LauncherSecret,
    ) -> RuntimeDescriptor {
        let descriptor = RuntimeDescriptor::new(
            self.descriptor.instance_id(),
            self.descriptor.pid(),
            self.descriptor.port(),
            self.descriptor.started_at(),
            launcher_secret,
        )
        .expect("construct alternate descriptor");
        descriptor
            .publish(&self.descriptor_path)
            .expect("publish alternate descriptor");
        descriptor
    }
}

impl Drop for FakePrimary {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.server.abort();
        let _ = std::fs::remove_file(&self.descriptor_path);
    }
}

fn fixed_timestamp() -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339("2026-07-15T00:00:00Z").expect("fixed timestamp")
}

fn test_descriptor(port: u16) -> RuntimeDescriptor {
    let seed = SecuritySeed::generate().expect("generate descriptor secret");
    RuntimeDescriptor::new(
        uuid::Uuid::new_v4(),
        NonZeroU32::new(std::process::id()).expect("nonzero test PID"),
        NonZeroU16::new(port).expect("nonzero test port"),
        fixed_timestamp(),
        seed.launcher_secret().clone(),
    )
    .expect("construct test descriptor")
}

fn transient_replacement_error(error: &std::io::Error) -> bool {
    if error.kind() == std::io::ErrorKind::NotFound {
        return true;
    }
    #[cfg(windows)]
    {
        error.raw_os_error() == Some(windows_sys::Win32::Foundation::ERROR_SHARING_VIOLATION as i32)
    }
    #[cfg(not(windows))]
    {
        false
    }
}

#[cfg(unix)]
fn assert_private_file(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt as _;

    assert_eq!(
        std::fs::metadata(path)
            .expect("read descriptor metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}

#[cfg(windows)]
fn assert_private_file(path: &std::path::Path) {
    assert!(path.is_file(), "descriptor publication produced a file");
}
