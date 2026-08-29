#![cfg(feature = "test-support")]

mod support;

use std::io::Write as _;

#[cfg(feature = "test-support")]
use coding_agent_app::FakeScenario;
use coding_agent_app::{
    CancelOutcome, InstanceLock, PrivateFile, ShutdownOutcome, StartupOutcome, launch,
};
#[cfg(feature = "test-support")]
use coding_agent_domain::{TaskEventKind, TaskStatus};
#[cfg(feature = "test-support")]
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
#[cfg(feature = "test-support")]
use tokio::time::Duration;

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
async fn non_cooperative_mutation_reaches_a_finite_degraded_outcome_without_releasing_the_lock() {
    let fixture = support::shutdown_fixture([]).await;
    let guard = fixture
        .handles
        .mutation_gate
        .enter_data_mutation()
        .expect("enter a non-cooperative mutation");
    let marker_path = fixture.instance_shutdown_marker_path();

    tokio::time::pause();
    let coordinator = fixture.primary.shutdown_coordinator();
    let shutdown = tokio::spawn(async move { coordinator.shutdown().await });
    settle_tasks().await;
    tokio::time::advance(Duration::from_secs(10)).await;
    settle_until_finished(&shutdown).await;
    assert!(
        shutdown.is_finished(),
        "a process-clean non-cooperative mutation must reach its degraded outcome within the total budget"
    );
    tokio::time::resume();

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), shutdown)
            .await
            .expect("a process-clean shutdown remains finite")
            .expect("join non-cooperative mutation shutdown"),
        ShutdownOutcome::Degraded
    );
    assert!(marker_path.is_file());
    assert!(!fixture.startup.paths.instance_descriptor.exists());
    assert!(
        InstanceLock::try_acquire(&fixture.startup.paths.instance_lock)
            .expect("probe mutation-unknown lock")
            .is_none(),
        "an unproven mutation requires the OS lock fence until process exit"
    );
    drop(guard);
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

#[cfg(feature = "test-support")]
#[tokio::test]
async fn dropping_primary_with_an_unknown_tree_stops_http_but_does_not_release_the_lock() {
    let fixture = support::shutdown_fixture([FakeScenario::Blocking]).await;
    let task = fixture
        .start_task("drop must retain ownership while cleanup is unknown")
        .await;
    let held_tree = fixture.hold_task_process_tree(task.id);
    let lock_path = fixture.startup.paths.instance_lock.clone();
    let descriptor_path = fixture.startup.paths.instance_descriptor.clone();
    let marker_path = fixture.instance_shutdown_marker_path();
    let port = fixture.primary.port();
    let mutation_gate = fixture.handles.mutation_gate.clone();

    drop(fixture.primary);
    wait_for_listener_to_stop_accepting(port).await;
    assert!(mutation_gate.enter_data_mutation().is_err());
    assert!(!descriptor_path.exists());
    assert!(!marker_path.exists());
    assert!(
        InstanceLock::try_acquire(&lock_path)
            .expect("probe dropped-primary lock before cleanup proof")
            .is_none(),
        "Drop must retain the lock while an exact process tree is unknown"
    );

    drop(held_tree);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if InstanceLock::try_acquire(&lock_path)
                .expect("probe dropped-primary lock after cleanup proof")
                .is_some()
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the fail-safe worker releases the lock only after cleanup proof");
    assert!(!marker_path.exists());
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
async fn shutdown_waits_for_running_cleanup_then_interrupts_remaining_running_and_queued_tasks() {
    let fixture = support::shutdown_fixture([FakeScenario::IgnoresCancellation]).await;
    let task = fixture
        .start_task("runner ignores cancellation during shutdown")
        .await;
    let queued = fixture
        .handles
        .writer
        .create_task(
            support::new_task(
                fixture.repository.id,
                "queued task is interrupted only after running cleanup",
            ),
            support::deadline(),
        )
        .await
        .expect("create queued shutdown fixture task")
        .value
        .task()
        .clone();
    let coordinator = fixture.primary.shutdown_coordinator();
    let shutdown = tokio::spawn(async move { coordinator.shutdown().await });

    tokio::time::timeout(Duration::from_secs(5), async {
        while !fixture.handles.task_manager.shutdown_latched_for_test() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown closes task admission");
    settle_tasks().await;
    assert_eq!(
        fixture.reopen_task(task.id).await.status,
        TaskStatus::Running,
        "a live process tree cannot be terminalized before cleanup is proven"
    );
    assert_eq!(
        fixture.reopen_task(queued.id).await.status,
        TaskStatus::Queued,
        "generic interruption cannot overtake running process cleanup"
    );
    let live_event_kinds = fixture.event_kinds(task.id).await;
    assert!(!live_event_kinds.contains(&TaskEventKind::TaskInterrupted));
    assert!(!live_event_kinds.contains(&TaskEventKind::TaskCancelled));
    assert!(
        !shutdown.is_finished(),
        "shutdown cannot complete while the runner still owns a live process tree"
    );
    assert!(
        InstanceLock::try_acquire(&fixture.startup.paths.instance_lock)
            .expect("probe the live shutdown instance lock")
            .is_none(),
        "the primary lock remains held until process cleanup and terminal durability"
    );

    assert!(
        fixture.runner.release(task.id),
        "release the still-live fake runner within the shutdown budget"
    );
    fixture
        .wait_for_status(task.id, TaskStatus::Interrupted)
        .await;
    fixture
        .wait_for_status(queued.id, TaskStatus::Interrupted)
        .await;
    assert!(
        !fixture.runner.started_task_ids().contains(&queued.id),
        "queued shutdown work must not be started before generic interruption"
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), shutdown)
            .await
            .expect("shutdown completes after process cleanup")
            .expect("join shutdown worker"),
        ShutdownOutcome::Clean
    );
    fixture.assert_runtime_cleanup().await;
    let event_kinds = fixture.reopen_event_kinds(task.id).await;
    assert_eq!(event_kinds.last(), Some(&TaskEventKind::TaskInterrupted));
    assert!(!event_kinds.contains(&TaskEventKind::TaskCancelled));
    assert_eq!(
        fixture.reopen_event_kinds(queued.id).await.last(),
        Some(&TaskEventKind::TaskInterrupted)
    );
    assert_eq!(
        fixture.reopen_task(task.id).await.status,
        TaskStatus::Interrupted,
        "the released runner's successful outcome cannot overwrite the shutdown barrier"
    );
    assert_eq!(
        fixture.reopen_task(queued.id).await.status,
        TaskStatus::Interrupted
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn shutdown_drains_an_entered_user_cancel_before_freezing_the_task_manager() {
    let fixture = support::shutdown_fixture([FakeScenario::Blocking]).await;
    let task = fixture
        .start_task("entered user cancel must win before shutdown freeze")
        .await;
    let mutation_guard = fixture
        .handles
        .mutation_gate
        .enter_data_mutation()
        .expect("user cancel enters the mutation gate");
    let manager = fixture.handles.task_manager.clone();
    let cancel = tokio::spawn(async move { manager.cancel(task.id).await });
    let coordinator = fixture.primary.shutdown_coordinator();
    let shutdown = tokio::spawn(async move { coordinator.shutdown().await });

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), cancel)
            .await
            .expect("entered user cancel reaches a durable decision")
            .expect("join entered user cancel")
            .expect("persist entered user cancel"),
        CancelOutcome::Accepted { .. }
            | CancelOutcome::Cancelled { .. }
            | CancelOutcome::Finished { .. }
    ));
    drop(mutation_guard);

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), shutdown)
            .await
            .expect("shutdown completes after the entered cancel drains")
            .expect("join cancel-order shutdown"),
        ShutdownOutcome::Clean
    );
    assert_eq!(
        fixture.reopen_task(task.id).await.status,
        TaskStatus::Cancelled
    );
    let reopened = coding_agent_store::Store::open(&fixture.startup.paths.database_path)
        .await
        .expect("reopen cancel-order store");
    let events = reopened
        .task_events_after(task.id, coding_agent_domain::EventCursor::ZERO, usize::MAX)
        .await
        .expect("load cancel-order events")
        .events
        .into_iter()
        .map(|event| event.payload.kind())
        .collect::<Vec<_>>();
    reopened.pool().close().await;
    assert!(events.contains(&TaskEventKind::TaskCancelled));
    assert!(!events.contains(&TaskEventKind::TaskInterrupted));
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn unknown_process_tree_past_total_budget_stops_http_but_retains_lock_and_marker() {
    let fixture = support::shutdown_fixture([FakeScenario::Blocking]).await;
    let task = fixture
        .start_task("held process tree outlives the total shutdown budget")
        .await;
    let held_tree = fixture.hold_task_process_tree(task.id);
    let marker_path = fixture.instance_shutdown_marker_path();
    let port = fixture.primary.port();

    tokio::time::pause();
    let coordinator = fixture.primary.shutdown_coordinator();
    let shutdown = tokio::spawn(async move { coordinator.shutdown().await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !fixture.handles.task_manager.shutdown_latched_for_test() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown freezes the launch set");

    tokio::time::advance(Duration::from_secs(10)).await;
    settle_tasks().await;
    wait_for_listener_to_stop_accepting(port).await;
    assert!(
        !shutdown.is_finished(),
        "an unknown process tree forbids a finite shutdown outcome"
    );
    assert!(
        !fixture.startup.paths.instance_descriptor.exists(),
        "the dead HTTP endpoint must no longer be advertised"
    );
    assert!(
        InstanceLock::try_acquire(&fixture.startup.paths.instance_lock)
            .expect("probe the proof-gated shutdown lock")
            .is_none(),
        "the primary lock remains held past the total budget"
    );
    assert!(
        !marker_path.exists() && !fixture.startup.paths.unclean_shutdown.exists(),
        "degraded markers are forbidden before all process trees are proven clean"
    );
    // A fresh SQLx SQLite pool waits on a worker thread, so paused Tokio time
    // could auto-advance to the acquire timeout before that thread replies.
    tokio::time::resume();
    assert_eq!(
        fixture.reopen_task(task.id).await.status,
        TaskStatus::Running,
        "generic interruption cannot overtake process cleanup proof"
    );
    assert!(fixture.startup.calls.messages().is_empty());

    drop(held_tree);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), shutdown)
            .await
            .expect("shutdown completes after process cleanup proof")
            .expect("join proof-gated shutdown"),
        ShutdownOutcome::Degraded
    );
    assert!(marker_path.exists());
    assert!(
        InstanceLock::try_acquire(&fixture.startup.paths.instance_lock)
            .expect("probe post-budget retained lock")
            .is_none(),
        "a post-budget exit retains the lease until the process actually exits"
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn instance_scoped_repository_discovery_tree_is_included_in_shutdown_cleanup_proof() {
    let fixture = support::shutdown_fixture([]).await;
    let held_tree = fixture
        .handles
        .hold_instance_process_tree_for_test()
        .expect("hold repository-discovery instance process tree");
    let marker_path = fixture.instance_shutdown_marker_path();
    let port = fixture.primary.port();

    tokio::time::pause();
    let coordinator = fixture.primary.shutdown_coordinator();
    let shutdown = tokio::spawn(async move { coordinator.shutdown().await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !fixture.handles.task_manager.shutdown_latched_for_test() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown freezes after draining mutations");
    tokio::time::advance(Duration::from_secs(10)).await;
    settle_tasks().await;
    wait_for_listener_to_stop_accepting(port).await;

    assert!(!shutdown.is_finished());
    assert!(!fixture.startup.paths.instance_descriptor.exists());
    assert!(!marker_path.exists());
    assert!(
        InstanceLock::try_acquire(&fixture.startup.paths.instance_lock)
            .expect("probe instance-scoped cleanup lock")
            .is_none(),
        "a non-task controlled tree must retain the primary lock"
    );

    drop(held_tree);
    tokio::time::resume();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), shutdown)
            .await
            .expect("instance-scoped cleanup reaches a finite post-proof outcome")
            .expect("join instance-scoped shutdown"),
        ShutdownOutcome::Degraded
    );
    assert!(marker_path.is_file());
    assert!(
        InstanceLock::try_acquire(&fixture.startup.paths.instance_lock)
            .expect("probe retained post-budget instance lock")
            .is_none(),
        "post-budget shutdown retains the lock until process exit"
    );
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn store_failure_and_unknown_tree_defers_marker_and_degraded_exit_until_proof() {
    let fixture = support::shutdown_fixture([FakeScenario::Blocking]).await;
    let task = fixture
        .start_task("store failure waits behind exact process cleanup")
        .await;
    let held_tree = fixture.hold_task_process_tree(task.id);
    fixture.install_interrupted_event_failure().await;
    let marker_path = fixture.instance_shutdown_marker_path();

    tokio::time::pause();
    let coordinator = fixture.primary.shutdown_coordinator();
    let shutdown = tokio::spawn(async move { coordinator.shutdown().await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !fixture.handles.task_manager.shutdown_latched_for_test() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown freezes the failed-store launch set");
    tokio::time::advance(Duration::from_secs(10)).await;
    settle_tasks().await;

    assert!(!shutdown.is_finished());
    assert!(!marker_path.exists());
    assert!(fixture.startup.calls.messages().is_empty());
    assert!(
        InstanceLock::try_acquire(&fixture.startup.paths.instance_lock)
            .expect("probe failed-store shutdown lock")
            .is_none()
    );

    drop(held_tree);
    tokio::time::resume();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), shutdown)
            .await
            .expect("process-clean undurable shutdown reaches a finite outcome")
            .expect("join process-clean undurable shutdown"),
        ShutdownOutcome::Degraded
    );
    assert!(!fixture.startup.paths.instance_descriptor.exists());
    assert!(
        InstanceLock::try_acquire(&fixture.startup.paths.instance_lock)
            .expect("probe post-budget failed-store lock")
            .is_none(),
        "post-budget degraded shutdown retains the lease until process exit"
    );
    assert!(marker_path.is_file());
    assert_eq!(fixture.startup.calls.messages().len(), 1);
    assert_eq!(
        fixture.reopen_task(task.id).await.status,
        TaskStatus::Running
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
    let coordinator = fixture.primary.shutdown_coordinator();
    let shutdown = tokio::spawn(async move { coordinator.shutdown().await });

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), shutdown)
            .await
            .expect("process-clean Store failure reaches a finite shutdown outcome")
            .expect("join failed-store shutdown"),
        ShutdownOutcome::Degraded
    );
    fixture.assert_runtime_cleanup().await;
    assert!(
        failed_marker_path.is_dir(),
        "the occupied marker path proves best-effort marker creation failed"
    );
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

    let coordinator = primary.shutdown_coordinator();
    let shutdown = tokio::spawn(async move { coordinator.shutdown().await });
    wait_for_listener_to_stop_accepting(primary.port()).await;

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), shutdown)
            .await
            .expect("the incomplete HTTP request cannot exceed the total shutdown budget")
            .expect("join live-connection shutdown"),
        ShutdownOutcome::Clean
    );
    assert!(
        InstanceLock::try_acquire(&fixture.paths.instance_lock)
            .expect("reopen instance lock")
            .is_some()
    );
    assert!(!fixture.paths.instance_descriptor.exists());
    drop(connection);
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn clean_shutdown_seals_actor_store_readers_before_the_wal_checkpoint() {
    let fixture = support::StartupFixture::new();
    let primary = match launch(fixture.dependencies(Default::default()))
        .await
        .expect("launch primary")
    {
        StartupOutcome::Primary(primary) => primary,
        StartupOutcome::Secondary(_) => panic!("fixture must own the primary lock"),
    };
    let port = primary.port();
    let launch_url = fixture
        .calls
        .browser_urls()
        .into_iter()
        .next()
        .expect("primary publishes one browser launch URL");
    let launch_token = launch_url
        .split_once("/#token=")
        .map(|(_, token)| token)
        .expect("browser launch URL carries a fragment token");
    let session_cookie = exchange_session(port, launch_token).await;
    let sse = open_authenticated_sse(port, &session_cookie).await;

    sqlx::query("CREATE TABLE shutdown_sse_reader_probe (value INTEGER NOT NULL PRIMARY KEY)")
        .execute(primary.test_handles().store.pool())
        .await
        .expect("create the shutdown SSE reader probe");
    let mut reader = primary
        .test_handles()
        .store
        .pool()
        .acquire()
        .await
        .expect("acquire the SSE-owned SQLite reader");
    sqlx::query("BEGIN")
        .execute(&mut *reader)
        .await
        .expect("begin the SSE-owned read transaction");
    let observed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM shutdown_sse_reader_probe")
        .fetch_one(&mut *reader)
        .await
        .expect("establish the SSE-owned SQLite snapshot");
    assert_eq!(observed, 0);
    sqlx::query("INSERT INTO shutdown_sse_reader_probe (value) VALUES (1)")
        .execute(primary.test_handles().store.pool())
        .await
        .expect("append a WAL frame newer than the SSE reader snapshot");

    let reader_pool = primary.test_handles().store.pool().clone();
    let reader_lifetime = tokio::spawn(async move {
        let mut sse = sse;
        let mut remaining = Vec::new();
        sse.read_to_end(&mut remaining)
            .await
            .expect("SSE connection closes during server shutdown");
        reader_pool.close_event().await;
        sqlx::query("ROLLBACK")
            .execute(&mut *reader)
            .await
            .expect("release the actor-owned SQLite reader after the pool is sealed");
    });

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), primary.shutdown())
            .await
            .expect("actor cleanup and WAL checkpoint stay inside the total budget"),
        ShutdownOutcome::Clean
    );
    reader_lifetime
        .await
        .expect("join the actor-owned reader lifetime");
    assert!(
        InstanceLock::try_acquire(&fixture.paths.instance_lock)
            .expect("reopen instance lock")
            .is_some()
    );
    assert!(!fixture.paths.instance_descriptor.exists());
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
async fn settle_tasks() {
    for _ in 0..200 {
        tokio::task::yield_now().await;
    }
}

async fn settle_until_finished<T>(task: &tokio::task::JoinHandle<T>) {
    for _ in 0..20_000 {
        if task.is_finished() {
            return;
        }
        tokio::task::yield_now().await;
    }
}

#[cfg(feature = "test-support")]
async fn exchange_session(port: u16, launch_token: &str) -> String {
    let body = serde_json::to_vec(&serde_json::json!({ "token": launch_token }))
        .expect("encode launch-token exchange body");
    let authority = format!("127.0.0.1:{port}");
    let request = format!(
        "POST /api/session/exchange HTTP/1.1\r\nHost: {authority}\r\nOrigin: http://{authority}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut connection = tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port))
        .await
        .expect("connect to the session exchange");
    connection
        .write_all(request.as_bytes())
        .await
        .expect("write session exchange headers");
    connection
        .write_all(&body)
        .await
        .expect("write session exchange body");
    let mut response = Vec::new();
    connection
        .read_to_end(&mut response)
        .await
        .expect("read session exchange response");
    let response = String::from_utf8(response).expect("session exchange response is UTF-8");
    assert!(
        response.starts_with("HTTP/1.1 204 "),
        "launch-token exchange must succeed: {response}"
    );
    response
        .split("\r\n")
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("set-cookie").then(|| {
                value
                    .trim()
                    .split(';')
                    .next()
                    .unwrap_or_default()
                    .to_owned()
            })
        })
        .filter(|cookie| !cookie.is_empty())
        .expect("session exchange sets the process session cookie")
}

#[cfg(feature = "test-support")]
async fn open_authenticated_sse(port: u16, session_cookie: &str) -> tokio::net::TcpStream {
    let authority = format!("127.0.0.1:{port}");
    let request = format!(
        "GET /api/events?after=0 HTTP/1.1\r\nHost: {authority}\r\nAccept: text/event-stream\r\nCookie: {session_cookie}\r\n\r\n"
    );
    let mut connection = tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port))
        .await
        .expect("connect authenticated SSE reader");
    connection
        .write_all(request.as_bytes())
        .await
        .expect("write authenticated SSE request");

    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut buffer = [0_u8; 4 * 1024];
        loop {
            let count = connection
                .read(&mut buffer)
                .await
                .expect("read authenticated SSE response");
            assert_ne!(
                count, 0,
                "SSE response closed before its initial control frame"
            );
            response.extend_from_slice(&buffer[..count]);
            if response
                .windows(b"event: service.state".len())
                .any(|window| window == b"event: service.state")
            {
                return;
            }
        }
    })
    .await
    .expect("authenticated SSE response publishes its initial control frame");
    let response = String::from_utf8_lossy(&response);
    assert!(response.starts_with("HTTP/1.1 200 "));
    assert!(
        response
            .to_ascii_lowercase()
            .contains("content-type: text/event-stream")
    );
    connection
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
