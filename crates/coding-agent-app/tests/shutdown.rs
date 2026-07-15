mod support;

use std::io::Write as _;

#[cfg(feature = "test-support")]
use coding_agent_app::FakeScenario;
use coding_agent_app::{InstanceLock, PrivateFile, ShutdownOutcome, StartupOutcome, launch};
#[cfg(feature = "test-support")]
use coding_agent_domain::{TaskEventKind, TaskStatus};
#[cfg(feature = "test-support")]
use tokio::io::AsyncWriteExt as _;
#[cfg(feature = "test-support")]
use tokio::task::JoinHandle;
#[cfg(feature = "test-support")]
use tokio::time::{Duration, Instant};

#[tokio::test]
async fn clean_shutdown_closes_listener_removes_descriptor_and_releases_lock() {
    let fixture = support::StartupFixture::new();
    let primary = match launch(fixture.dependencies(Default::default()))
        .await
        .expect("launch primary")
    {
        StartupOutcome::Primary(primary) => primary,
        StartupOutcome::Secondary(_) => panic!("fixture must own the primary lock"),
    };
    let port = primary.port();
    assert!(fixture.paths.instance_descriptor.is_file());

    assert_eq!(primary.shutdown().await, ShutdownOutcome::Clean);

    assert!(!fixture.paths.instance_descriptor.exists());
    assert!(
        InstanceLock::try_acquire(&fixture.paths.instance_lock)
            .expect("reopen instance lock")
            .is_some(),
        "the completed coordinator must release the lock before PrimaryRuntime is dropped"
    );
    assert!(
        tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port))
            .await
            .is_err(),
        "the loopback listener must be closed"
    );
}

#[tokio::test]
async fn concurrent_shutdown_requests_share_one_completed_outcome() {
    let fixture = support::StartupFixture::new();
    let primary = match launch(fixture.dependencies(Default::default()))
        .await
        .expect("launch primary")
    {
        StartupOutcome::Primary(primary) => primary,
        StartupOutcome::Secondary(_) => panic!("fixture must own the primary lock"),
    };

    let (first, second) = tokio::join!(primary.shutdown(), primary.shutdown());

    assert_eq!(first, ShutdownOutcome::Clean);
    assert_eq!(second, ShutdownOutcome::Clean);
    assert!(!fixture.paths.instance_descriptor.exists());
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn dropping_an_unstarted_primary_closes_the_mutation_gate_before_lock_release() {
    let fixture = support::StartupFixture::new();
    let primary = match launch(fixture.dependencies(Default::default()))
        .await
        .expect("launch primary")
    {
        StartupOutcome::Primary(primary) => primary,
        StartupOutcome::Secondary(_) => panic!("fixture must own the primary lock"),
    };
    let handles = primary.test_handles();

    drop(primary);

    assert!(
        handles.mutation_gate.enter_data_mutation().is_err(),
        "Drop must close the write gate before another primary can take the lock"
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if InstanceLock::try_acquire(&fixture.paths.instance_lock)
                .expect("reopen instance lock")
                .is_some()
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the stopped server releases its lock keepalive");
}

#[tokio::test]
async fn dropping_primary_after_shutdown_acceptance_does_not_preempt_the_worker() {
    let fixture = support::StartupFixture::new();
    let primary = match launch(fixture.dependencies(Default::default()))
        .await
        .expect("launch primary")
    {
        StartupOutcome::Primary(primary) => primary,
        StartupOutcome::Secondary(_) => panic!("fixture must own the primary lock"),
    };
    let coordinator = primary.shutdown_coordinator();
    let shutdown = coordinator.shutdown();
    tokio::pin!(shutdown);
    assert!(futures_util::poll!(&mut shutdown).is_pending());

    drop(primary);

    assert_eq!(shutdown.await, ShutdownOutcome::Clean);
    assert!(
        InstanceLock::try_acquire(&fixture.paths.instance_lock)
            .expect("reopen instance lock")
            .is_some()
    );
    assert!(!fixture.paths.instance_descriptor.exists());
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn ignored_cancellation_is_durably_interrupted_before_bounded_clean_exit() {
    let fixture = support::shutdown_fixture([FakeScenario::IgnoresCancellation]).await;
    let task = fixture
        .start_task("runner ignores cancellation during shutdown")
        .await;
    let started = Instant::now();
    let deadline = started + Duration::from_secs(10);
    let coordinator = fixture.primary.shutdown_coordinator();
    let shutdown = tokio::spawn(async move { coordinator.shutdown().await });

    fixture
        .wait_for_status(task.id, TaskStatus::Interrupted)
        .await;
    let event_kinds = fixture.event_kinds(task.id).await;
    assert_eq!(event_kinds.last(), Some(&TaskEventKind::TaskInterrupted));
    assert!(!event_kinds.contains(&TaskEventKind::TaskCancelled));
    tokio::time::pause();
    assert!(
        !shutdown.is_finished(),
        "the coordinator must still be bounding the live runner after the durable barrier"
    );

    let runner_boundary = deadline - Duration::from_secs(2);
    if Instant::now() < runner_boundary {
        tokio::time::advance(runner_boundary - Instant::now()).await;
    }
    wait_for_listener_to_stop_accepting(fixture.primary.port()).await;
    settle_by(deadline, &shutdown).await;
    assert_eq!(shutdown.await.unwrap(), ShutdownOutcome::Clean);
    fixture.assert_runtime_cleanup().await;
    tokio::time::resume();

    assert!(
        fixture.runner.release(task.id),
        "the fake runner must still be alive after ignoring cancellation"
    );
    settle_tasks().await;
    assert_eq!(
        fixture.reopen_task(task.id).await.status,
        TaskStatus::Interrupted,
        "a late successful runner result must not overwrite the shutdown barrier"
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn permanent_store_and_marker_failure_still_cleans_runtime_within_ten_seconds() {
    const SECRET_PROMPT: &str = "shutdown prompt that must not enter the native warning";
    let fixture = support::shutdown_fixture([FakeScenario::Blocking]).await;
    let task = fixture.start_task(SECRET_PROMPT).await;
    fixture.install_interrupted_event_failure().await;
    let failed_marker_path = fixture.make_marker_creation_fail();
    tokio::time::pause();
    let started = Instant::now();
    let deadline = started + Duration::from_secs(10);
    let coordinator = fixture.primary.shutdown_coordinator();
    let shutdown = tokio::spawn(async move { coordinator.shutdown().await });

    settle_by(deadline, &shutdown).await;
    assert_eq!(shutdown.await.unwrap(), ShutdownOutcome::Degraded);
    fixture.assert_runtime_cleanup().await;
    assert!(
        failed_marker_path.is_dir(),
        "the occupied marker path proves best-effort marker creation failed"
    );
    tokio::time::resume();
    assert_eq!(
        fixture.reopen_task(task.id).await.status,
        TaskStatus::Running,
        "a failed shutdown transaction must not persist a misleading cancellation"
    );

    let messages = fixture.startup.calls.messages();
    assert_eq!(
        messages,
        vec![(
            "Coding Agent did not shut down cleanly".to_owned(),
            "Some terminal task states could not be persisted. They will be recovered the next time Coding Agent starts."
                .to_owned(),
        )]
    );
    assert!(!messages[0].1.contains(SECRET_PROMPT));
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn a_live_http_connection_cannot_push_cleanup_past_the_total_deadline() {
    let fixture = support::StartupFixture::new();
    let primary = match launch(fixture.dependencies(Default::default()))
        .await
        .expect("launch primary")
    {
        StartupOutcome::Primary(primary) => primary,
        StartupOutcome::Secondary(_) => panic!("fixture must own the primary lock"),
    };
    let mut connection =
        tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, primary.port()))
            .await
            .expect("open a live loopback connection");
    connection
        .write_all(b"GET /api/bootstrap HTTP/1.1\r\nHost: 127.0.0.1")
        .await
        .expect("leave an HTTP request deliberately incomplete");
    tokio::task::yield_now().await;

    tokio::time::pause();
    let deadline = Instant::now() + Duration::from_secs(10);
    let coordinator = primary.shutdown_coordinator();
    let shutdown = tokio::spawn(async move { coordinator.shutdown().await });
    wait_for_listener_to_stop_accepting(primary.port()).await;

    settle_by(deadline, &shutdown).await;
    assert_eq!(shutdown.await.unwrap(), ShutdownOutcome::Clean);
    assert!(
        InstanceLock::try_acquire(&fixture.paths.instance_lock)
            .expect("reopen instance lock")
            .is_some()
    );
    assert!(!fixture.paths.instance_descriptor.exists());
    tokio::time::resume();
    drop(connection);
}

#[tokio::test]
async fn successful_startup_recovery_removes_an_existing_unclean_marker() {
    let fixture = support::StartupFixture::new();
    fixture.prepare();
    let mut marker = PrivateFile::create_new(&fixture.paths.unclean_shutdown)
        .expect("create preexisting unclean shutdown marker");
    marker
        .write_all(b"{\"error_code\":\"SHUTDOWN_PERSISTENCE_FAILED\"}")
        .expect("write preexisting unclean shutdown marker");
    marker.flush().expect("flush preexisting shutdown marker");
    marker
        .as_file()
        .sync_all()
        .expect("sync preexisting shutdown marker");
    drop(marker);
    let staging_marker = fixture
        .paths
        .unclean_shutdown
        .with_file_name("unclean-shutdown.json.old-instance.pending");
    let mut staging = PrivateFile::create_new(&staging_marker)
        .expect("create preexisting staged shutdown marker");
    staging
        .write_all(b"staged marker")
        .expect("write staged shutdown marker");
    drop(staging);
    let instance_marker = fixture
        .paths
        .unclean_shutdown
        .with_file_name("unclean-shutdown.json.old-instance.marker");
    let mut instance =
        PrivateFile::create_new(&instance_marker).expect("create immutable shutdown marker");
    instance
        .write_all(b"immutable marker")
        .expect("write immutable shutdown marker");
    drop(instance);

    let primary = match launch(fixture.dependencies(Default::default()))
        .await
        .expect("launch primary with recovered marker")
    {
        StartupOutcome::Primary(primary) => primary,
        StartupOutcome::Secondary(_) => panic!("fixture must own the primary lock"),
    };

    assert!(
        !fixture.paths.unclean_shutdown.exists(),
        "the marker is removed only after startup recovery commits"
    );
    assert!(
        !staging_marker.exists(),
        "staged marker debris is removed only after startup recovery commits"
    );
    assert!(
        !instance_marker.exists(),
        "immutable markers are removed only after startup recovery commits"
    );
    assert_eq!(primary.shutdown().await, ShutdownOutcome::Clean);
}

#[cfg(feature = "test-support")]
async fn settle_by(deadline: Instant, shutdown: &JoinHandle<ShutdownOutcome>) {
    let now = Instant::now();
    if now < deadline {
        tokio::time::advance(deadline - now).await;
    }
    for _ in 0..20_000 {
        if shutdown.is_finished() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("shutdown did not reach an exit decision within ten seconds");
}

#[cfg(feature = "test-support")]
async fn settle_tasks() {
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
}

#[cfg(feature = "test-support")]
async fn wait_for_listener_to_stop_accepting(port: u16) {
    for _ in 0..20_000 {
        match tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port)).await {
            Ok(connection) => drop(connection),
            Err(_) => return,
        }
        tokio::task::yield_now().await;
    }
    panic!("loopback listener did not begin shutdown");
}
