#![allow(dead_code)]

use std::path::{Path, PathBuf};

use coding_agent_domain::{
    CanonicalPath, ClientRequestId, NewRepository, NewTask, Repository, RepositoryId, Task,
    TaskFailure, TaskStatus, UtcTimestamp,
};
use coding_agent_store::{RegisterRepositoryOutcome, Store, TaskTransition, TransitionOutcome};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, SqliteConnection};
use tempfile::TempDir;
use time::OffsetDateTime;

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

pub async fn seeded_store() -> Store {
    let store = memory_store().await;
    register_repository(&store, "repo").await;
    store
}

pub async fn register_repository(store: &Store, name: &str) -> Repository {
    let root = std::env::temp_dir().join(format!(
        "coding-agent-store-{name}-{}",
        uuid::Uuid::new_v4()
    ));
    let input = NewRepository {
        selected_path: CanonicalPath::try_from_canonical(root.join("selected"))
            .expect("construct selected path"),
        display_name: name.to_owned(),
        git_root: CanonicalPath::try_from_canonical(root.join("git")).expect("construct git root"),
        cargo_workspace_root: CanonicalPath::try_from_canonical(root.join("workspace"))
            .expect("construct workspace root"),
    };

    match store
        .register_repository(input)
        .await
        .expect("register fixture repository")
    {
        RegisterRepositoryOutcome::Created(repository)
        | RegisterRepositoryOutcome::Existing(repository) => repository,
    }
}

pub fn new_task(repository_id: RepositoryId, prompt: &str) -> NewTask {
    NewTask::try_new(ClientRequestId::new(), repository_id, prompt).expect("construct fixture task")
}

pub async fn queued_task(store: &Store) -> Task {
    let repository = store
        .list_repositories()
        .await
        .expect("list fixture repositories")
        .into_iter()
        .next()
        .expect("fixture repository");
    store
        .create_task(new_task(repository.id, "fixture prompt"))
        .await
        .expect("create queued fixture task")
        .task()
        .clone()
}

pub async fn running_task(store: &Store) -> Task {
    let queued = queued_task(store).await;
    match store
        .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .expect("start fixture task")
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("fixture start must apply"),
    }
}

pub async fn terminal_task(store: &Store, status: TaskStatus) -> Task {
    let (task, expected, transition) = match status {
        TaskStatus::Completed => (
            running_task(store).await,
            TaskStatus::Running,
            TaskTransition::Completed,
        ),
        TaskStatus::Failed => (
            running_task(store).await,
            TaskStatus::Running,
            TaskTransition::Failed(failure("FIXTURE_FAILED")),
        ),
        TaskStatus::Cancelled => (
            queued_task(store).await,
            TaskStatus::Queued,
            TaskTransition::Cancelled,
        ),
        TaskStatus::Interrupted => (
            queued_task(store).await,
            TaskStatus::Queued,
            TaskTransition::Interrupted(failure("FIXTURE_INTERRUPTED")),
        ),
        TaskStatus::Queued | TaskStatus::Running => panic!("fixture status must be terminal"),
    };

    match store
        .transition_with_event(task.id, expected, transition)
        .await
        .expect("finish fixture task")
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("fixture terminal transition must apply"),
    }
}

pub fn failure(code: &str) -> TaskFailure {
    TaskFailure {
        code: code.to_owned(),
        message: format!("safe message for {code}"),
        retryable: true,
    }
}

pub fn current_timestamp() -> UtcTimestamp {
    UtcTimestamp::new(OffsetDateTime::now_utc()).expect("construct current timestamp")
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
