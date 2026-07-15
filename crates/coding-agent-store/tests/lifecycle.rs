use coding_agent_store::{Store, StoreError};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection as _, SqliteConnection};

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

    // Configure every existing pooled connection to report checkpoint contention
    // immediately instead of consuming SQLite's normal five-second busy timeout.
    let connection_count = store.pool().options().get_max_connections() as usize;
    let mut connections = Vec::with_capacity(connection_count);
    for _ in 0..connection_count {
        let mut connection = store.pool().acquire().await.expect("acquire connection");
        sqlx::query("PRAGMA busy_timeout = 0")
            .execute(&mut *connection)
            .await
            .expect("disable checkpoint busy wait");
        connections.push(connection);
    }
    for mut connection in connections {
        connection.return_to_pool().await;
    }

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
