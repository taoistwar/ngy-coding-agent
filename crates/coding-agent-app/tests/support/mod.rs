#![allow(dead_code)]

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use coding_agent_app::{
    EventWake, StoreWriterBackend, StoreWriterBackendFuture, StoreWriterError, StoreWriterHandle,
    StoreWriterOperation,
};
use coding_agent_domain::{
    CanonicalPath, ClientRequestId, NewRepository, NewTask, Repository, RepositoryId, TaskFailure,
    UtcTimestamp,
};
use coding_agent_store::{RegisterRepositoryOutcome, Store};
use tempfile::TempDir;
use tokio::sync::Notify;
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

impl WriterFixture {
    pub fn repository_input(&self, name: &str) -> NewRepository {
        repository_input_at(&self.root, name)
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

#[derive(Debug, Clone, Copy)]
pub enum InjectedFault {
    Busy,
    Terminal,
}

pub struct FaultControlledStore {
    inner: Store,
    faults: Mutex<VecDeque<InjectedFault>>,
    attempts: AtomicUsize,
    pause: Option<Arc<PausePoint>>,
}

impl FaultControlledStore {
    pub fn new(inner: Store, faults: impl IntoIterator<Item = InjectedFault>) -> Self {
        Self {
            inner,
            faults: Mutex::new(faults.into_iter().collect()),
            attempts: AtomicUsize::new(0),
            pause: None,
        }
    }

    pub fn paused(inner: Store, pause: Arc<PausePoint>) -> Self {
        Self {
            inner,
            faults: Mutex::new(VecDeque::new()),
            attempts: AtomicUsize::new(0),
            pause: Some(pause),
        }
    }

    pub fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
}

impl StoreWriterBackend for FaultControlledStore {
    fn execute(&self, operation: StoreWriterOperation) -> StoreWriterBackendFuture<'_> {
        Box::pin(async move {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if let Some(pause) = &self.pause {
                pause.started.notify_one();
                pause.release.notified().await;
            }
            let fault = self
                .faults
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front();
            match fault {
                Some(InjectedFault::Busy) => Err(StoreWriterError::Busy),
                Some(InjectedFault::Terminal) => Err(StoreWriterError::Store(
                    coding_agent_store::StoreError::InvariantViolation("injected rollback"),
                )),
                None => StoreWriterBackend::execute(&self.inner, operation).await,
            }
        })
    }
}

#[derive(Default)]
pub struct PausePoint {
    pub started: Notify,
    pub release: Notify,
}
