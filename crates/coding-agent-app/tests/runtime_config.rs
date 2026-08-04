#![cfg(feature = "test-support")]

mod support;

use std::any::Any;
use std::io::Write as _;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use coding_agent_app::{
    AvailableParallelismProbe, FakeTaskRunner, FixedStartupRunnerFactory, MAX_RUNTIME_CONFIG_BYTES,
    PlatformPaths, PreActorStartupRunnerContext, PrivateFile, RUNTIME_CONFIG_INVALID,
    RuntimeConfig, RuntimeConfigLoadErrorKind, StartupError, StartupOutcome, StartupRunnerContext,
    StartupRunnerFactory, StartupRunnerFactoryError, StartupRunnerSelection,
    derive_cargo_jobs_per_task_for_test, launch, load_runtime_config_for_test,
};

fn fixture() -> (tempfile::TempDir, PlatformPaths) {
    let temp = tempfile::tempdir().expect("create runtime configuration fixture");
    let paths = PlatformPaths::new(temp.path().join("data"), temp.path().join("runtime"));
    paths.prepare().expect("prepare private application paths");
    (temp, paths)
}

fn valid_json(
    global: u32,
    per_repository: u32,
    queued: u32,
    control_reserve: u64,
    task_reservation: u64,
) -> Vec<u8> {
    format!(
        concat!(
            "{{",
            "\"schema_version\":1,",
            "\"max_concurrent_tasks\":{global},",
            "\"max_concurrent_tasks_per_repository\":{per_repository},",
            "\"max_queued_tasks\":{queued},",
            "\"storage\":{{",
            "\"data_control_reserve_bytes\":{control_reserve},",
            "\"data_task_reservation_bytes\":{task_reservation}",
            "}}",
            "}}"
        ),
        global = global,
        per_repository = per_repository,
        queued = queued,
        control_reserve = control_reserve,
        task_reservation = task_reservation,
    )
    .into_bytes()
}

fn write_private_runtime_config(paths: &PlatformPaths, bytes: &[u8]) {
    let mut file =
        PrivateFile::create_new(&paths.runtime_config).expect("create private runtime config");
    file.write_all(bytes).expect("write runtime config");
    file.flush().expect("flush runtime config");
}

fn load_with_cpu(
    paths: &PlatformPaths,
    available_parallelism: Option<usize>,
) -> Result<RuntimeConfig, coding_agent_app::RuntimeConfigLoadError> {
    load_runtime_config_for_test(paths, available_parallelism.and_then(NonZeroUsize::new))
}

#[test]
fn missing_runtime_json_uses_the_recorded_defaults() {
    let (_temp, paths) = fixture();

    let config = load_with_cpu(&paths, Some(8)).expect("load missing-file defaults");

    assert_eq!(config.max_concurrent_tasks().get(), 2);
    assert_eq!(config.max_concurrent_tasks_per_repository().get(), 2);
    assert_eq!(config.max_queued_tasks().get(), 32);
    assert_eq!(
        config.storage().data_control_reserve_bytes().get(),
        2 * 1024 * 1024 * 1024
    );
    assert_eq!(
        config.storage().data_task_reservation_bytes().get(),
        2 * 1024 * 1024 * 1024
    );
    assert_eq!(config.cargo_jobs_per_task().get(), 4);
}

#[test]
fn exact_private_runtime_json_loads_validated_limits() {
    let (_temp, paths) = fixture();
    let mut document = valid_json(4, 3, 256, 1, 2);
    document.extend_from_slice(b" \r\n\t");
    write_private_runtime_config(&paths, &document);

    let config = load_with_cpu(&paths, Some(64)).expect("load exact runtime config");

    assert_eq!(config.max_concurrent_tasks().get(), 4);
    assert_eq!(config.max_concurrent_tasks_per_repository().get(), 3);
    assert_eq!(config.max_queued_tasks().get(), 256);
    assert_eq!(config.storage().data_control_reserve_bytes().get(), 1);
    assert_eq!(config.storage().data_task_reservation_bytes().get(), 2);
    assert_eq!(config.cargo_jobs_per_task().get(), 8);
}

#[test]
fn runtime_json_rejects_non_exact_or_ambiguous_json() {
    let invalid_documents: &[(&str, &[u8])] = &[
        (
            "missing top-level field",
            br#"{"schema_version":1,"max_concurrent_tasks":2,"max_concurrent_tasks_per_repository":2,"storage":{"data_control_reserve_bytes":1,"data_task_reservation_bytes":1}}"#,
        ),
        (
            "unknown top-level field",
            br#"{"schema_version":1,"max_concurrent_tasks":2,"max_concurrent_tasks_per_repository":2,"max_queued_tasks":32,"storage":{"data_control_reserve_bytes":1,"data_task_reservation_bytes":1},"unknown":true}"#,
        ),
        (
            "duplicate top-level field",
            br#"{"schema_version":1,"schema_version":1,"max_concurrent_tasks":2,"max_concurrent_tasks_per_repository":2,"max_queued_tasks":32,"storage":{"data_control_reserve_bytes":1,"data_task_reservation_bytes":1}}"#,
        ),
        (
            "duplicate global field",
            br#"{"schema_version":1,"max_concurrent_tasks":2,"max_concurrent_tasks":2,"max_concurrent_tasks_per_repository":2,"max_queued_tasks":32,"storage":{"data_control_reserve_bytes":1,"data_task_reservation_bytes":1}}"#,
        ),
        (
            "duplicate repository field",
            br#"{"schema_version":1,"max_concurrent_tasks":2,"max_concurrent_tasks_per_repository":2,"max_concurrent_tasks_per_repository":2,"max_queued_tasks":32,"storage":{"data_control_reserve_bytes":1,"data_task_reservation_bytes":1}}"#,
        ),
        (
            "duplicate queue field",
            br#"{"schema_version":1,"max_concurrent_tasks":2,"max_concurrent_tasks_per_repository":2,"max_queued_tasks":32,"max_queued_tasks":32,"storage":{"data_control_reserve_bytes":1,"data_task_reservation_bytes":1}}"#,
        ),
        (
            "duplicate storage field",
            br#"{"schema_version":1,"max_concurrent_tasks":2,"max_concurrent_tasks_per_repository":2,"max_queued_tasks":32,"storage":{"data_control_reserve_bytes":1,"data_task_reservation_bytes":1},"storage":{"data_control_reserve_bytes":1,"data_task_reservation_bytes":1}}"#,
        ),
        (
            "missing nested field",
            br#"{"schema_version":1,"max_concurrent_tasks":2,"max_concurrent_tasks_per_repository":2,"max_queued_tasks":32,"storage":{"data_control_reserve_bytes":1}}"#,
        ),
        (
            "unknown nested field",
            br#"{"schema_version":1,"max_concurrent_tasks":2,"max_concurrent_tasks_per_repository":2,"max_queued_tasks":32,"storage":{"data_control_reserve_bytes":1,"data_task_reservation_bytes":1,"unknown":true}}"#,
        ),
        (
            "duplicate nested field",
            br#"{"schema_version":1,"max_concurrent_tasks":2,"max_concurrent_tasks_per_repository":2,"max_queued_tasks":32,"storage":{"data_control_reserve_bytes":1,"data_control_reserve_bytes":1,"data_task_reservation_bytes":1}}"#,
        ),
        (
            "duplicate nested task reservation field",
            br#"{"schema_version":1,"max_concurrent_tasks":2,"max_concurrent_tasks_per_repository":2,"max_queued_tasks":32,"storage":{"data_control_reserve_bytes":1,"data_task_reservation_bytes":1,"data_task_reservation_bytes":1}}"#,
        ),
        (
            "unknown schema version",
            br#"{"schema_version":2,"max_concurrent_tasks":2,"max_concurrent_tasks_per_repository":2,"max_queued_tasks":32,"storage":{"data_control_reserve_bytes":1,"data_task_reservation_bytes":1}}"#,
        ),
        (
            "wrong scalar type",
            br#"{"schema_version":1,"max_concurrent_tasks":"2","max_concurrent_tasks_per_repository":2,"max_queued_tasks":32,"storage":{"data_control_reserve_bytes":1,"data_task_reservation_bytes":1}}"#,
        ),
        (
            "negative unsigned scalar",
            br#"{"schema_version":1,"max_concurrent_tasks":2,"max_concurrent_tasks_per_repository":2,"max_queued_tasks":-1,"storage":{"data_control_reserve_bytes":1,"data_task_reservation_bytes":1}}"#,
        ),
        (
            "fractional storage scalar",
            br#"{"schema_version":1,"max_concurrent_tasks":2,"max_concurrent_tasks_per_repository":2,"max_queued_tasks":32,"storage":{"data_control_reserve_bytes":1.5,"data_task_reservation_bytes":1}}"#,
        ),
        (
            "null storage object",
            br#"{"schema_version":1,"max_concurrent_tasks":2,"max_concurrent_tasks_per_repository":2,"max_queued_tasks":32,"storage":null}"#,
        ),
        (
            "trailing content",
            br#"{"schema_version":1,"max_concurrent_tasks":2,"max_concurrent_tasks_per_repository":2,"max_queued_tasks":32,"storage":{"data_control_reserve_bytes":1,"data_task_reservation_bytes":1}} true"#,
        ),
    ];

    for (case, document) in invalid_documents {
        let (_temp, paths) = fixture();
        write_private_runtime_config(&paths, document);

        let error = load_with_cpu(&paths, Some(8)).expect_err(case);

        assert_eq!(error.kind(), RuntimeConfigLoadErrorKind::Invalid, "{case}");
        assert_eq!(error.code(), RUNTIME_CONFIG_INVALID, "{case}");
        assert!(!error.retryable(), "{case}");
    }
}

#[test]
fn runtime_json_rejects_bounds_relationships_and_checked_arithmetic_overflow() {
    let invalid_values = [
        ("global below range", valid_json(0, 1, 32, 1, 1)),
        ("global above range", valid_json(5, 1, 32, 1, 1)),
        ("repository below range", valid_json(2, 0, 32, 1, 1)),
        ("repository above range", valid_json(4, 5, 32, 1, 1)),
        ("repository above global", valid_json(1, 2, 32, 1, 1)),
        ("queue below range", valid_json(2, 2, 0, 1, 1)),
        ("queue above range", valid_json(2, 2, 257, 1, 1)),
        ("zero control reserve", valid_json(2, 2, 32, 0, 1)),
        ("zero task reservation", valid_json(2, 2, 32, 1, 0)),
        (
            "reservation multiplication overflow",
            valid_json(4, 2, 32, 1, u64::MAX),
        ),
        (
            "reservation addition overflow",
            valid_json(1, 1, 32, u64::MAX, 1),
        ),
    ];

    for (case, document) in invalid_values {
        let (_temp, paths) = fixture();
        write_private_runtime_config(&paths, &document);

        let error = load_with_cpu(&paths, Some(8)).expect_err(case);

        assert_eq!(error.kind(), RuntimeConfigLoadErrorKind::Invalid, "{case}");
        assert_eq!(error.code(), RUNTIME_CONFIG_INVALID, "{case}");
    }

    let (_boundary_temp, boundary_paths) = fixture();
    let exact_task_reservation = (u64::MAX - 3) / 4;
    write_private_runtime_config(
        &boundary_paths,
        &valid_json(4, 4, 256, 3, exact_task_reservation),
    );
    load_with_cpu(&boundary_paths, Some(8))
        .expect("the largest exactly non-overflowing reservation is valid");
}

#[test]
fn cargo_jobs_derivation_is_stable_bounded_and_uses_the_configured_global_limit() {
    let cases = [
        (1, Some(1), 1),
        (1, Some(2), 2),
        (1, Some(4), 4),
        (1, Some(8), 8),
        (1, Some(64), 8),
        (1, None, 1),
        (2, Some(1), 1),
        (2, Some(2), 1),
        (2, Some(4), 2),
        (2, Some(8), 4),
        (2, Some(64), 8),
        (2, None, 1),
        (3, Some(1), 1),
        (3, Some(2), 1),
        (3, Some(4), 1),
        (3, Some(8), 2),
        (3, Some(64), 8),
        (3, None, 1),
        (4, Some(1), 1),
        (4, Some(2), 1),
        (4, Some(4), 1),
        (4, Some(8), 2),
        (4, Some(64), 8),
        (4, None, 1),
    ];

    for (global, available, expected) in cases {
        let actual = derive_cargo_jobs_per_task_for_test(
            available.and_then(NonZeroUsize::new),
            NonZeroU32::new(global).expect("table global limit is nonzero"),
        );
        assert_eq!(
            actual.get(),
            expected,
            "global={global}, available={available:?}"
        );
    }
}

#[test]
fn runtime_json_must_be_private_regular_and_bounded() {
    let (_oversized_temp, oversized_paths) = fixture();
    let mut oversized = vec![b' '; MAX_RUNTIME_CONFIG_BYTES + 1];
    oversized[..20].copy_from_slice(b"known-runtime-secret");
    write_private_runtime_config(&oversized_paths, &oversized);
    let oversized_error =
        load_with_cpu(&oversized_paths, Some(8)).expect_err("reject oversized runtime config");
    assert_eq!(oversized_error.kind(), RuntimeConfigLoadErrorKind::TooLarge);
    assert!(!format!("{oversized_error:?}").contains("known-runtime-secret"));
    assert!(!format!("{oversized_error}").contains("known-runtime-secret"));

    let (_permissions_temp, permissions_paths) = fixture();
    std::fs::write(
        &permissions_paths.runtime_config,
        valid_json(2, 2, 32, 1, 1),
    )
    .expect("write inherited-permission runtime config");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        std::fs::set_permissions(
            &permissions_paths.runtime_config,
            std::fs::Permissions::from_mode(0o644),
        )
        .expect("make runtime config non-private");
    }
    let permissions_error =
        load_with_cpu(&permissions_paths, Some(8)).expect_err("reject non-private runtime config");
    assert_eq!(permissions_error.code(), RUNTIME_CONFIG_INVALID);

    let (_directory_temp, directory_paths) = fixture();
    std::fs::create_dir(&directory_paths.runtime_config)
        .expect("create non-regular runtime config path");
    let directory_error =
        load_with_cpu(&directory_paths, Some(8)).expect_err("reject non-regular runtime config");
    assert_eq!(directory_error.code(), RUNTIME_CONFIG_INVALID);
}

#[cfg(unix)]
#[test]
fn runtime_json_fifo_is_rejected_without_waiting_for_a_writer() {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt as _;
    use std::sync::mpsc;
    use std::time::Duration;

    let (_temp, paths) = fixture();
    let encoded_path = CString::new(paths.runtime_config.as_os_str().as_bytes())
        .expect("runtime config path contains no NUL");
    let result = unsafe { libc::mkfifo(encoded_path.as_ptr(), 0o600) };
    assert_eq!(
        result,
        0,
        "create FIFO fixture: {}",
        std::io::Error::last_os_error()
    );

    let worker_paths = paths.clone();
    let (sender, receiver) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _ = sender.send(load_with_cpu(&worker_paths, Some(8)));
    });
    let error = receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("O_NONBLOCK must prevent waiting for a FIFO writer")
        .expect_err("a FIFO is not a private regular configuration file");
    worker.join().expect("join FIFO loader");

    assert_eq!(error.code(), RUNTIME_CONFIG_INVALID);
}

#[cfg(windows)]
#[test]
fn existing_but_unreadable_runtime_json_never_falls_back_to_defaults() {
    use std::os::windows::fs::OpenOptionsExt as _;

    let (_temp, paths) = fixture();
    write_private_runtime_config(&paths, &valid_json(2, 2, 32, 1, 1));
    let _exclusive_handle = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(&paths.runtime_config)
        .expect("hold an exclusive runtime config handle");

    let error = load_with_cpu(&paths, Some(8))
        .expect_err("an existing unreadable runtime config must fail closed");

    assert_eq!(error.code(), RUNTIME_CONFIG_INVALID);
    assert!(!error.retryable());
}

#[test]
fn runtime_json_rejects_a_final_path_symlink_or_reparse_point() {
    let (_temp, paths) = fixture();
    let target = paths.data_dir.join("runtime-target.json");
    let mut file = PrivateFile::create_new(&target).expect("create private symlink target");
    file.write_all(&valid_json(2, 2, 32, 1, 1))
        .expect("write symlink target");
    file.flush().expect("flush symlink target");
    create_file_symlink(&target, &paths.runtime_config).expect("create runtime config symlink");

    let error = load_with_cpu(&paths, Some(8)).expect_err("reject runtime config symlink");

    assert_eq!(error.code(), RUNTIME_CONFIG_INVALID);
}

#[tokio::test]
async fn invalid_runtime_json_fails_before_store_open_and_uses_a_secret_safe_message() {
    let fixture = support::StartupFixture::new();
    fixture.prepare();
    write_private_runtime_config(
        &fixture.paths,
        br#"{"known-runtime-secret":"E:\\private\\runtime.json"}"#,
    );

    let result = launch(fixture.dependencies(support::StartupBehavior {
        panic_on_store_open: true,
        ..support::StartupBehavior::default()
    }))
    .await;

    match result {
        Err(StartupError::RuntimeConfig(error)) => {
            assert_eq!(error.code(), RUNTIME_CONFIG_INVALID);
        }
        Err(error) => panic!("unexpected startup error: {error:?}"),
        Ok(_) => panic!("invalid runtime config must fail startup"),
    }
    assert_eq!(fixture.calls.store_opens(), 0);
    assert_eq!(fixture.calls.listener_binds(), 0);
    let messages = fixture.calls.messages();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].0, "Coding Agent could not start");
    assert_eq!(
        messages[0].1,
        "The runtime configuration is invalid.\n\nError code: RUNTIME_CONFIG_INVALID"
    );
    assert!(!messages[0].1.contains("known-runtime-secret"));
    assert!(!messages[0].1.contains("runtime.json"));
}

struct CapturingRunnerFactory {
    observed: Arc<Mutex<Option<RuntimeConfig>>>,
    delegate: FixedStartupRunnerFactory,
}

struct FailingAvailableParallelismProbe {
    calls: Arc<AtomicUsize>,
}

impl AvailableParallelismProbe for FailingAvailableParallelismProbe {
    fn available_parallelism(&self) -> Option<NonZeroUsize> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        None
    }
}

#[async_trait::async_trait]
impl StartupRunnerFactory for CapturingRunnerFactory {
    async fn prepare_before_actors(
        &self,
        context: &PreActorStartupRunnerContext,
    ) -> Result<Arc<dyn Any + Send + Sync>, StartupRunnerFactoryError> {
        self.delegate.prepare_before_actors(context).await
    }

    async fn create(
        &self,
        context: StartupRunnerContext,
    ) -> Result<StartupRunnerSelection, StartupRunnerFactoryError> {
        *self.observed.lock().expect("lock observed runtime config") =
            Some(context.runtime_config().clone());
        self.delegate.create(context).await
    }
}

#[tokio::test]
async fn locked_primary_passes_the_immutable_runtime_config_to_runner_startup() {
    let fixture = support::StartupFixture::new();
    fixture.prepare();
    write_private_runtime_config(&fixture.paths, &valid_json(3, 2, 17, 11, 13));
    let observed = Arc::new(Mutex::new(None));
    let parallelism_probes = Arc::new(AtomicUsize::new(0));
    let mut dependencies = fixture.dependencies(support::StartupBehavior::default());
    dependencies.available_parallelism = Arc::new(FailingAvailableParallelismProbe {
        calls: Arc::clone(&parallelism_probes),
    });
    dependencies.runner_factory = Arc::new(CapturingRunnerFactory {
        observed: Arc::clone(&observed),
        delegate: FixedStartupRunnerFactory::new(
            Arc::new(FakeTaskRunner::default()),
            NonZeroU32::new(1).expect("test concurrency is nonzero"),
        ),
    });

    let outcome = launch(dependencies)
        .await
        .expect("start primary with runtime config");
    let StartupOutcome::Primary(primary) = outcome else {
        panic!("fixture must become primary");
    };

    let config = observed
        .lock()
        .expect("lock observed runtime config")
        .clone()
        .expect("runner startup received runtime config");
    assert_eq!(config.max_concurrent_tasks().get(), 3);
    assert_eq!(config.max_concurrent_tasks_per_repository().get(), 2);
    assert_eq!(config.max_queued_tasks().get(), 17);
    assert_eq!(config.storage().data_control_reserve_bytes().get(), 11);
    assert_eq!(config.storage().data_task_reservation_bytes().get(), 13);
    assert_eq!(config.cargo_jobs_per_task().get(), 1);
    assert_eq!(parallelism_probes.load(Ordering::SeqCst), 1);
    let cargo_jobs_per_task = config.cargo_jobs_per_task();

    std::fs::remove_file(&fixture.paths.runtime_config)
        .expect("remove the consumed runtime configuration");
    write_private_runtime_config(&fixture.paths, &valid_json(1, 1, 1, 1, 1));

    assert_eq!(config.max_concurrent_tasks().get(), 3);
    assert_eq!(config.max_queued_tasks().get(), 17);
    assert_eq!(config.cargo_jobs_per_task(), cargo_jobs_per_task);

    drop(primary);
}

#[cfg(unix)]
fn create_file_symlink(target: &std::path::Path, link: &std::path::Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_file_symlink(target: &std::path::Path, link: &std::path::Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_file(target, link)
}
