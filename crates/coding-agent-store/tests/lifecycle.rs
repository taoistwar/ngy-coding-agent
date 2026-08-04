mod support;

use std::time::Duration;

use coding_agent_store::{
    FinalizeStoppedTaskOutcome, FinalizeStoppedTaskRequest, PersistStopIntentOutcome,
    StopIntentKind, StopIntentRequest, Store, StoreError,
};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection as _, SqliteConnection};

#[tokio::test]
async fn stop_intent_and_final_receipts_survive_checkpoint_and_reopen() {
    let fixture = support::store_fixture().await;
    support::register_repository(&fixture.store, "lifecycle-stop").await;
    let running = support::running_task(&fixture.store).await;
    let intent_request = StopIntentRequest {
        task_id: running.id,
        expected_repository_id: running.repository_id,
        expected_attempt: running.attempt,
        kind: StopIntentKind::DiskPressureCritical,
    };
    let intent = match fixture
        .store
        .persist_stop_intent(intent_request)
        .await
        .unwrap()
    {
        PersistStopIntentOutcome::Applied(intent) => intent,
        other => panic!("fixture intent must apply, got {other:?}"),
    };
    let final_request = FinalizeStoppedTaskRequest {
        task_id: running.id,
        expected_repository_id: running.repository_id,
        expected_attempt: running.attempt,
        expected_intent: StopIntentKind::DiskPressureCritical,
    };
    let terminal = match fixture
        .store
        .finalize_stopped_task(final_request)
        .await
        .unwrap()
    {
        FinalizeStoppedTaskOutcome::Applied(receipt) => receipt,
        other => panic!("fixture final stop must apply, got {other:?}"),
    };

    fixture.store.checkpoint_and_close().await.unwrap();
    let reopened = Store::open(&fixture.database_path).await.unwrap();
    assert!(matches!(
        reopened.persist_stop_intent(intent_request).await.unwrap(),
        PersistStopIntentOutcome::Existing(existing) if existing == intent
    ));
    assert!(matches!(
        reopened.finalize_stopped_task(final_request).await.unwrap(),
        FinalizeStoppedTaskOutcome::Existing(existing) if existing == terminal
    ));
}

#[tokio::test]
async fn checkpoint_and_close_preserves_committed_data_for_reopen() {
    let temp_dir = tempfile::tempdir().expect("create lifecycle temp directory");
    let database_path = temp_dir.path().join("lifecycle.sqlite3");
    let store = Store::open(&database_path).await.expect("open store");

    sqlx::query("CREATE TABLE lifecycle_probe (value TEXT NOT NULL)")
        .execute(store.pool())
        .await
        .expect("create lifecycle probe table");
    sqlx::query("INSERT INTO lifecycle_probe (value) VALUES ('durable')")
        .execute(store.pool())
        .await
        .expect("insert lifecycle probe row");

    store
        .checkpoint_and_close()
        .await
        .expect("checkpoint and close store");
    assert!(store.pool().is_closed());

    let reopened = Store::open(&database_path).await.expect("reopen store");
    let value: String = sqlx::query_scalar("SELECT value FROM lifecycle_probe")
        .fetch_one(reopened.pool())
        .await
        .expect("read checkpointed probe row");
    assert_eq!(value, "durable");
    reopened.pool().close().await;
}

#[tokio::test]
async fn checkpoint_seals_a_saturated_pool_before_waiting_for_borrowers() {
    let temp_dir = tempfile::tempdir().expect("create lifecycle temp directory");
    let database_path = temp_dir.path().join("saturated-pool.sqlite3");
    let store = Store::open(&database_path).await.expect("open store");
    sqlx::query("CREATE TABLE lifecycle_probe (value TEXT NOT NULL)")
        .execute(store.pool())
        .await
        .expect("create lifecycle probe table");
    sqlx::query("INSERT INTO lifecycle_probe (value) VALUES ('durable')")
        .execute(store.pool())
        .await
        .expect("insert lifecycle probe row");

    let connection_count = store.pool().options().get_max_connections() as usize;
    let mut borrowers = Vec::with_capacity(connection_count);
    for _ in 0..connection_count {
        borrowers.push(
            store
                .pool()
                .acquire()
                .await
                .expect("saturate the shared pool"),
        );
    }

    let shutdown_store = store.clone();
    let shutdown = tokio::spawn(async move { shutdown_store.checkpoint_and_close().await });
    tokio::time::timeout(Duration::from_secs(1), store.pool().close_event())
        .await
        .expect("checkpoint shutdown seals the pool before waiting for borrowers");
    drop(borrowers);

    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .expect("checkpoint completes after saturated borrowers return")
        .expect("join checkpoint shutdown")
        .expect("checkpoint saturated pool");
    assert!(store.pool().is_closed());
}

#[tokio::test]
async fn checkpoint_failure_still_closes_every_shared_pool_handle() {
    let temp_dir = tempfile::tempdir().expect("create lifecycle temp directory");
    let database_path = temp_dir.path().join("checkpoint-failure.sqlite3");
    let store = Store::open(&database_path).await.expect("open store");
    sqlx::query("CREATE TABLE lifecycle_probe (value TEXT NOT NULL)")
        .execute(store.pool())
        .await
        .expect("create lifecycle probe table");
    sqlx::query("INSERT INTO lifecycle_probe (value) VALUES ('before-reader')")
        .execute(store.pool())
        .await
        .expect("insert lifecycle probe row");

    // Hold an old WAL snapshot on a connection outside the Store's pool, then append
    // a newer frame. A truncating checkpoint cannot complete while that reader lives.
    let options = SqliteConnectOptions::new().filename(&database_path);
    let mut reader = SqliteConnection::connect_with(&options)
        .await
        .expect("open independent WAL reader");
    sqlx::query("BEGIN")
        .execute(&mut reader)
        .await
        .expect("begin reader transaction");
    let _: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM lifecycle_probe")
        .fetch_one(&mut reader)
        .await
        .expect("establish reader snapshot");
    sqlx::query("INSERT INTO lifecycle_probe (value) VALUES ('after-reader')")
        .execute(store.pool())
        .await
        .expect("append WAL frame after reader snapshot");

    // Configure the dedicated shutdown connection to report checkpoint contention
    // immediately instead of consuming SQLite's normal five-second busy timeout.
    let checkpoint_options = store
        .pool()
        .connect_options()
        .as_ref()
        .clone()
        .busy_timeout(Duration::ZERO);
    store.pool().set_connect_options(checkpoint_options);

    let shared = store.clone();
    let result = store.checkpoint_and_close().await;

    match result {
        Err(StoreError::WalCheckpointIncomplete {
            busy,
            log_frames,
            checkpointed_frames,
        }) => {
            assert_eq!(busy, 1);
            assert!(log_frames > checkpointed_frames);
        }
        other => panic!("blocked checkpoint returned an unexpected result: {other:?}"),
    }
    assert!(store.pool().is_closed());
    assert!(shared.pool().is_closed());
    assert!(matches!(
        shared.pool().acquire().await,
        Err(sqlx::Error::PoolClosed)
    ));
    sqlx::query("ROLLBACK")
        .execute(&mut reader)
        .await
        .expect("release independent WAL reader");
}
