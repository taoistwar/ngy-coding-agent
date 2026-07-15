#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use coding_agent_app::{EventDispatcherHandle, EventWake, StoreWriterHandle};
use coding_agent_domain::{
    ActivityEntry, ActivityLevel, CanonicalPath, ClientRequestId, EventCursor, EventId,
    NewRepository, NewTask, Repository, RepositoryId, Task, TaskEventPayload, TaskFailure,
    TaskStatus, UtcTimestamp,
};
use coding_agent_store::{
    AppendEventOutcome, CreateTaskOutcome, RegisterRepositoryOutcome, Store, TaskTransition,
    TransitionOutcome,
};
use tempfile::TempDir;
use tokio::time::{Duration, Instant};

pub struct StoreFixture {
    pub store: Store,
    pub repository: Repository,
    root: PathBuf,
    _temp_dir: TempDir,
}

pub struct WriterFixture {
    pub store: Store,
    pub repository: Repository,
    pub writer: StoreWriterHandle,
    pub wake: Arc<CountingWake>,
    root: PathBuf,
    _temp_dir: TempDir,
}

pub struct DispatcherFixture {
    pub store: Store,
    pub dispatcher: EventDispatcherHandle,
    pub startup_cursor: EventCursor,
    running_task: Task,
    _temp_dir: TempDir,
}

pub async fn store_fixture() -> StoreFixture {
    let temp_dir = tempfile::tempdir().expect("create app fixture directory");
    let database_path = temp_dir.path().join("store.sqlite3");
    let store = Store::open(database_path)
        .await
        .expect("open fixture store");
    store.migrate().await.expect("migrate fixture store");
    let root = temp_dir.path().to_path_buf();
    let repository = match store
        .register_repository(repository_input_at(&root, "seed"))
        .await
        .expect("register seed repository")
    {
        RegisterRepositoryOutcome::Created(repository)
        | RegisterRepositoryOutcome::Existing(repository) => repository,
    };

    StoreFixture {
        store,
        repository,
        root,
        _temp_dir: temp_dir,
    }
}

pub async fn writer_fixture() -> WriterFixture {
    let fixture = store_fixture().await;
    let wake = Arc::new(CountingWake::default());
    let writer = StoreWriterHandle::spawn(fixture.store.clone(), wake.clone(), 8);
    WriterFixture {
        store: fixture.store,
        repository: fixture.repository,
        writer,
        wake,
        root: fixture.root,
        _temp_dir: fixture._temp_dir,
    }
}

pub async fn dispatcher_fixture() -> DispatcherFixture {
    let fixture = store_fixture().await;
    let queued = match fixture
        .store
        .create_task(new_task(fixture.repository.id, "dispatcher fixture"))
        .await
        .expect("create dispatcher fixture task")
    {
        CreateTaskOutcome::Created { task, .. } | CreateTaskOutcome::Existing { task } => task,
    };
    let running_task = match fixture
        .store
        .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .expect("start dispatcher fixture task")
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("dispatcher fixture task must start"),
    };
    let startup_cursor = fixture
        .store
        .latest_event_id()
        .await
        .expect("read dispatcher startup cursor");
    let dispatcher = EventDispatcherHandle::spawn(fixture.store.clone(), 1_024)
        .await
        .expect("spawn event dispatcher");

    DispatcherFixture {
        store: fixture.store,
        dispatcher,
        startup_cursor,
        running_task,
        _temp_dir: fixture._temp_dir,
    }
}

impl WriterFixture {
    pub fn repository_input(&self, name: &str) -> NewRepository {
        repository_input_at(&self.root, name)
    }
}

impl DispatcherFixture {
    pub async fn commit_events_without_wake(&self, messages: &[String]) -> Vec<EventId> {
        let mut event_ids = Vec::with_capacity(messages.len());
        for (index, message) in messages.iter().enumerate() {
            let outcome = self
                .store
                .append_running_event(
                    self.running_task.id,
                    TaskEventPayload::ActivityAppended {
                        entry: ActivityEntry {
                            id: format!("activity-{index}-{message}"),
                            level: ActivityLevel::Info,
                            message: message.clone(),
                            created_at: timestamp(),
                        },
                    },
                )
                .await
                .expect("append dispatcher fixture event");
            let event_id = match outcome {
                AppendEventOutcome::Applied { event_id } => event_id,
                AppendEventOutcome::NotRunning { .. } => {
                    panic!("dispatcher fixture task must remain running")
                }
            };
            event_ids.push(event_id);
        }
        event_ids
    }
}

pub fn new_task(repository_id: RepositoryId, prompt: &str) -> NewTask {
    NewTask::try_new(ClientRequestId::new(), repository_id, prompt).expect("construct fixture task")
}

pub fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

pub fn timestamp() -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339("2026-07-15T00:00:00Z").expect("construct fixture timestamp")
}

pub fn failure(code: &str) -> TaskFailure {
    TaskFailure {
        code: code.to_owned(),
        message: format!("safe message for {code}"),
        retryable: true,
    }
}

fn repository_input_at(root: &std::path::Path, name: &str) -> NewRepository {
    NewRepository {
        selected_path: canonical(root.join(format!("{name}-selected"))),
        display_name: name.to_owned(),
        git_root: canonical(root.join(format!("{name}-git"))),
        cargo_workspace_root: canonical(root.join(format!("{name}-workspace"))),
    }
}

fn canonical(path: PathBuf) -> CanonicalPath {
    CanonicalPath::try_from_canonical(path).expect("construct canonical fixture path")
}

#[derive(Default)]
pub struct CountingWake {
    count: AtomicUsize,
}

impl CountingWake {
    pub fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

impl EventWake for CountingWake {
    fn wake(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }
}

pub struct PanickingWake;

impl EventWake for PanickingWake {
    fn wake(&self) {
        panic!("injected wake panic");
    }
}
