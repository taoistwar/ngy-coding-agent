#[derive(Default)]
struct CancellingRunner {
    starts: AtomicUsize,
    cancelled: tokio::sync::Notify,
}

impl CancellingRunner {
    async fn wait_for_starts(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.starts.load(Ordering::SeqCst) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("runner did not observe {expected} starts"));
    }
}

#[derive(Default)]
struct ReleaseRunner {
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[derive(Default)]
struct FailingReleaseRunner {
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(feature = "test-support")]
struct RunningHardFreezeFixture {
    _temp_dir: tempfile::TempDir,
    store: Store,
    repository: Repository,
    manager: TaskManagerHandle,
    runner: Arc<FailingReleaseRunner>,
    task: Task,
}

#[cfg(feature = "test-support")]
struct TwoTaskHardFreezeFixture {
    _temp_dir: tempfile::TempDir,
    store: Store,
    repository: Repository,
    manager: TaskManagerHandle,
    runner: Arc<FailingReleaseRunner>,
    tasks: [Task; 2],
}

#[cfg(feature = "test-support")]
struct PausedTerminalProjectionFixture {
    _temp_dir: tempfile::TempDir,
    store: Store,
    repository: Repository,
    manager: TaskManagerHandle,
    hooks: Arc<ClaimTestHooks>,
    task: Task,
}

#[cfg(feature = "test-support")]
struct PausedFinalStopFixture {
    _temp_dir: tempfile::TempDir,
    store: Store,
    manager: TaskManagerHandle,
    controller: Arc<StoreWriterTestController>,
    task: Task,
    pending: PendingDurableResult,
    cancel: tokio::task::JoinHandle<Result<CancelOutcome, TaskManagerError>>,
}

#[cfg(feature = "test-support")]
struct PausedQueuedCancelFixture {
    _temp_dir: tempfile::TempDir,
    store: Store,
    manager: TaskManagerHandle,
    controller: Arc<StoreWriterTestController>,
    task: Task,
    cancel: tokio::task::JoinHandle<Result<CancelOutcome, TaskManagerError>>,
}

#[cfg(feature = "test-support")]
#[derive(Default)]
struct DelayedCancellationRunner {
    started: tokio::sync::Notify,
    cancelled: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(feature = "test-support")]
#[derive(Default)]
struct StagedReviewStopRunner {
    started: tokio::sync::Notify,
    review_release: tokio::sync::Notify,
    review_applied: tokio::sync::Notify,
    finish_release: tokio::sync::Notify,
    review_result: std::sync::Mutex<Option<Result<EventId, RunnerEventError>>>,
}

#[cfg(feature = "test-support")]
type PairedRunnerEventResults = (
    Result<EventId, RunnerEventError>,
    Result<EventId, RunnerEventError>,
);

#[cfg(feature = "test-support")]
#[derive(Default)]
struct ConcurrentReviewRunner {
    started: tokio::sync::Notify,
    review_release: tokio::sync::Notify,
    reviews_applied: tokio::sync::Notify,
    finish_release: tokio::sync::Notify,
    review_results: std::sync::Mutex<Option<PairedRunnerEventResults>>,
}

#[cfg(feature = "test-support")]
struct BidirectionalFifoRunner {
    review_first: bool,
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
    completed: tokio::sync::Notify,
    finish_release: tokio::sync::Notify,
    results: std::sync::Mutex<Option<PairedRunnerEventResults>>,
}

#[cfg(feature = "test-support")]
impl BidirectionalFifoRunner {
    fn new(review_first: bool) -> Self {
        Self {
            review_first,
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            completed: tokio::sync::Notify::new(),
            finish_release: tokio::sync::Notify::new(),
            results: std::sync::Mutex::new(None),
        }
    }
}

#[cfg(feature = "test-support")]
#[derive(Default)]
struct GenericRecoveryLeaseRunner {
    started: tokio::sync::Notify,
    event_release: tokio::sync::Notify,
    event_completed: tokio::sync::Notify,
    event_result: std::sync::Mutex<Option<Result<EventId, RunnerEventError>>>,
}

#[cfg(feature = "test-support")]
#[derive(Default)]
struct HeldCleanupRunner {
    held: std::sync::Mutex<Option<coding_agent_runtime::HeldProcessLivenessTreeForTest>>,
    returned: tokio::sync::Notify,
    starts: AtomicUsize,
}

#[derive(Default)]
struct EarlyCancelledRunner;

#[async_trait::async_trait]
impl TaskRunner for EarlyCancelledRunner {
    async fn run(&self, _context: RunContext, _sink: RunnerEventSink) -> RunnerOutcome {
        RunnerOutcome::Cancelled
    }
}

#[cfg(feature = "test-support")]
#[async_trait::async_trait]
impl TaskRunner for HeldCleanupRunner {
    async fn run(&self, mut context: RunContext, _sink: RunnerEventSink) -> RunnerOutcome {
        context.complete_preparation_for_test().await;
        let start = self.starts.fetch_add(1, Ordering::SeqCst);
        if start == 0 {
            let held = context
                .process_liveness_scope()
                .hold_tree_for_test()
                .expect("hold exact runner process tree");
            *self
                .held
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(held);
            self.returned.notify_one();
        }
        RunnerOutcome::Cancelled
    }
}

#[cfg(feature = "test-support")]
impl HeldCleanupRunner {
    fn start_count(&self) -> usize {
        self.starts.load(Ordering::SeqCst)
    }

    fn release_cleanup(&self) {
        self.held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("held runner process tree remains owned");
    }
}

#[async_trait::async_trait]
impl TaskRunner for CancellingRunner {
    async fn run(&self, mut context: RunContext, _sink: RunnerEventSink) -> RunnerOutcome {
        context.complete_preparation_for_test().await;
        self.starts.fetch_add(1, Ordering::SeqCst);
        context.cancellation.cancelled().await;
        self.cancelled.notify_one();
        RunnerOutcome::Cancelled
    }
}

#[async_trait::async_trait]
impl TaskRunner for ReleaseRunner {
    async fn run(&self, mut context: RunContext, sink: RunnerEventSink) -> RunnerOutcome {
        context.complete_preparation_for_test().await;
        sink.append(RunnerEvent::PlanUpdated(crate::fake_runner::fake_plan()))
            .await
            .expect("release runner persists matching review plan");
        self.started.notify_one();
        self.release.notified().await;
        RunnerOutcome::Approved(crate::fake_runner::approved_evidence())
    }
}

#[async_trait::async_trait]
impl TaskRunner for FailingReleaseRunner {
    async fn run(&self, mut context: RunContext, _sink: RunnerEventSink) -> RunnerOutcome {
        context.complete_preparation_for_test().await;
        self.started.notify_one();
        self.release.notified().await;
        RunnerOutcome::Failed(TaskFailure {
            code: "PUBLISH_GATE_FAILURE".to_owned(),
            message: "synthetic terminal for scheduler publication gating".to_owned(),
            retryable: false,
        })
    }
}
