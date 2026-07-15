#![allow(dead_code)]

use std::path::{Path, PathBuf};

use coding_agent_domain::{CanonicalPath, NewRepository};
use coding_agent_store::Store;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, SqliteConnection};
use tempfile::TempDir;

pub struct FileStoreFixture {
    pub store: Store,
    pub database_path: PathBuf,
    _temp_dir: TempDir,
}

pub async fn memory_store() -> Store {
    let store = Store::open(Path::new(":memory:"))
        .await
        .expect("open in-memory store");
    store.migrate().await.expect("migrate in-memory store");
    store
}

pub async fn file_store() -> FileStoreFixture {
    let temp_dir = tempfile::tempdir().expect("create store temp directory");
    let database_path = temp_dir.path().join("store.sqlite3");
    let store = Store::open(&database_path)
        .await
        .expect("open file-backed store");

    FileStoreFixture {
        store,
        database_path,
        _temp_dir: temp_dir,
    }
}

pub async fn conflicting_file_store() -> FileStoreFixture {
    let temp_dir = tempfile::tempdir().expect("create store temp directory");
    let database_path = temp_dir.path().join("store.sqlite3");
    seed_conflicting_repository_schema(&database_path).await;
    let store = Store::open(&database_path)
        .await
        .expect("open conflicting file-backed store");

    FileStoreFixture {
        store,
        database_path,
        _temp_dir: temp_dir,
    }
}

async fn seed_conflicting_repository_schema(database_path: &Path) {
    let options = SqliteConnectOptions::new()
        .filename(database_path)
        .create_if_missing(true);
    let mut connection = SqliteConnection::connect_with(&options)
        .await
        .expect("open database for fault seeding");

    sqlx::raw_sql(
        "CREATE TABLE migration_marker (value TEXT NOT NULL);\
         INSERT INTO migration_marker (value) VALUES ('preserve-me');\
         CREATE TABLE repositories (broken INTEGER NOT NULL);",
    )
    .execute(&mut connection)
    .await
    .expect("seed conflicting schema");
}

pub struct StoreFixture {
    pub store: Store,
    pub database_path: PathBuf,
    _temp_dir: TempDir,
}

pub async fn store_fixture() -> StoreFixture {
    let temp_dir = tempfile::tempdir().expect("create repository fixture directory");
    let database_path = temp_dir.path().join("store.sqlite3");
    let store = Store::open(&database_path)
        .await
        .expect("open repository fixture store");
    store
        .migrate()
        .await
        .expect("migrate repository fixture store");

    StoreFixture {
        store,
        database_path,
        _temp_dir: temp_dir,
    }
}

impl StoreFixture {
    pub async fn canonical_repository_input(&self, name: &str) -> NewRepository {
        let git_root = self.canonical_path(format!("repositories/{name}")).await;
        let cargo_workspace_root = self
            .canonical_path(format!("repositories/{name}/workspace"))
            .await;
        let selected_path = self
            .canonical_path(format!("repositories/{name}/selected"))
            .await;

        NewRepository {
            selected_path,
            display_name: name.to_owned(),
            git_root,
            cargo_workspace_root,
        }
    }

    pub async fn canonical_path(&self, relative: impl AsRef<Path>) -> CanonicalPath {
        let path = self._temp_dir.path().join(relative);
        tokio::fs::create_dir_all(&path)
            .await
            .expect("create canonical fixture path");
        let path = tokio::fs::canonicalize(path)
            .await
            .expect("canonicalize fixture path");
        CanonicalPath::try_from_canonical(path).expect("construct canonical fixture path")
    }
}
