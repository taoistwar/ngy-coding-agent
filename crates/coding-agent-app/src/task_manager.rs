use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use coding_agent_domain::{
    ActivityEntry, DiffSnapshot, EventId, PlanSnapshot, Repository, Task, TaskEventPayload,
    TaskFailure, TaskId, TaskStatus, TestSnapshot, UtcTimestamp,
};
use coding_agent_store::{
    AppendEventOutcome, Store, StoreError, TaskTransition, TransitionOutcome,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::{ServiceState, ServiceStateController, StoreWriterError, StoreWriterHandle};

const RECONCILE_INTERVAL: Duration = Duration::from_millis(100);
const BACKGROUND_WRITE_BUDGET: Duration = Duration::from_secs(5);

#[async_trait::async_trait]
pub trait TaskRunner: Send + Sync + 'static {
    async fn run(&self, context: RunContext, sink: RunnerEventSink) -> RunnerOutcome;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunnerOutcome {
    Succeeded,
    Cancelled,
    Failed(TaskFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelOutcome {
    Cancelled { task: Task },
    Accepted { task: Task },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunnerEvent {
    PlanUpdated(PlanSnapshot),
    ActivityAppended(ActivityEntry),
    DiffUpdated(DiffSnapshot),
    TestUpdated(TestSnapshot),
}

impl RunnerEvent {
    fn into_payload(self) -> TaskEventPayload {
        match self {
            Self::PlanUpdated(plan) => TaskEventPayload::PlanUpdated { plan },
            Self::ActivityAppended(entry) => TaskEventPayload::ActivityAppended { entry },
            Self::DiffUpdated(diff) => TaskEventPayload::DiffUpdated { diff },
            Self::TestUpdated(tests) => TaskEventPayload::TestUpdated { tests },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RunnerEventError {
    #[error("task is not running")]
    TaskNotRunning,
    #[error("the store is degraded")]
    StoreDegraded,
    #[error("the task manager is closed")]
    ManagerClosed,
}

pub struct RunnerEventSink {
    task_id: TaskId,
    sender: mpsc::Sender<TaskManagerMessage>,
}

impl RunnerEventSink {
    pub async fn append(&self, event: RunnerEvent) -> Result<EventId, RunnerEventError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(TaskManagerMessage::RunnerEvent {
                task_id: self.task_id,
                event,
                response,
            })
            .await
            .map_err(|_| RunnerEventError::ManagerClosed)?;
        receiver
            .await
            .map_err(|_| RunnerEventError::ManagerClosed)?
    }
}

pub struct RunnerShutdownHandle {
    pub task_id: TaskId,
    pub cancellation: CancellationToken,
    pub done: oneshot::Receiver<()>,
}

pub enum QuiesceResult {
    Durable {
        recovery: coding_agent_store::RecoveryOutcome,
        active: Vec<RunnerShutdownHandle>,
    },
    Frozen {
        active: Vec<RunnerShutdownHandle>,
        error: StoreWriterError,
    },
}

#[derive(Debug, Clone)]
pub struct RunContext {
    pub task: Task,
    pub repository: Repository,
    pub cancellation: CancellationToken,
}

#[derive(Debug, thiserror::Error)]
pub enum TaskManagerError {
    #[error("task manager is closed")]
    Closed,
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    StoreWriter(#[from] StoreWriterError),
    #[error("task was not found")]
    TaskNotFound,
    #[error("task is not cancellable in state {task:?}")]
    TaskNotCancellable { task: Task },
    #[error("task manager is frozen")]
    Frozen,
    #[error("the store is degraded")]
    StoreDegraded,
    #[error("task manager invariant failed: {0}")]
    Invariant(&'static str),
}

#[derive(Clone)]
pub struct TaskManagerHandle {
    sender: mpsc::Sender<TaskManagerMessage>,
}

impl TaskManagerHandle {
    pub fn spawn(
        store: Store,
        writer: StoreWriterHandle,
        service_state: ServiceStateController,
        runner: Arc<dyn TaskRunner>,
        concurrency: usize,
        capacity: usize,
    ) -> Self {
        assert!(concurrency > 0, "task-manager concurrency must be positive");
        assert!(
            capacity > 0,
            "task-manager channel capacity must be positive"
        );
        let (sender, receiver) = mpsc::channel(capacity);
        let actor = TaskManager {
            store,
            writer,
            service_state,
            runner,
            semaphore: Arc::new(Semaphore::new(concurrency)),
            sender: sender.clone(),
            receiver,
            active: HashMap::new(),
            scan_requested: false,
            degraded: false,
            frozen: false,
            #[cfg(test)]
            claim_hooks: None,
        };
        tokio::spawn(actor.run());
        Self { sender }
    }

    #[cfg(test)]
    fn spawn_with_claim_hooks(
        store: Store,
        writer: StoreWriterHandle,
        service_state: ServiceStateController,
        runner: Arc<dyn TaskRunner>,
        concurrency: usize,
        capacity: usize,
        claim_hooks: Arc<ClaimTestHooks>,
    ) -> Self {
        assert!(concurrency > 0, "task-manager concurrency must be positive");
        assert!(
            capacity > 0,
            "task-manager channel capacity must be positive"
        );
        let (sender, receiver) = mpsc::channel(capacity);
        let actor = TaskManager {
            store,
            writer,
            service_state,
            runner,
            semaphore: Arc::new(Semaphore::new(concurrency)),
            sender: sender.clone(),
            receiver,
            active: HashMap::new(),
            scan_requested: false,
            degraded: false,
            frozen: false,
            claim_hooks: Some(claim_hooks),
        };
        tokio::spawn(actor.run());
        Self { sender }
    }

    pub async fn notify_queued(&self, task_id: TaskId) -> Result<(), TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::NotifyQueued {
            _task_id: task_id,
            response,
        })
        .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)?
    }

    pub async fn cancel(&self, task_id: TaskId) -> Result<CancelOutcome, TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::Cancel { task_id, response })
            .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)?
    }

    pub async fn quiesce_and_interrupt(
        &self,
        deadline: Instant,
    ) -> Result<QuiesceResult, TaskManagerError> {
        let (response, receiver) = oneshot::channel();
        self.send(TaskManagerMessage::Quiesce { deadline, response })
            .await?;
        receiver.await.map_err(|_| TaskManagerError::Closed)?
    }

    async fn send(&self, message: TaskManagerMessage) -> Result<(), TaskManagerError> {
        self.sender
            .send(message)
            .await
            .map_err(|_| TaskManagerError::Closed)
    }
}

enum TaskManagerMessage {
    NotifyQueued {
        _task_id: TaskId,
        response: oneshot::Sender<Result<(), TaskManagerError>>,
    },
    Cancel {
        task_id: TaskId,
        response: oneshot::Sender<Result<CancelOutcome, TaskManagerError>>,
    },
    RunnerEvent {
        task_id: TaskId,
        event: RunnerEvent,
        response: oneshot::Sender<Result<EventId, RunnerEventError>>,
    },
    RunnerFinished {
        task_id: TaskId,
        outcome: RunnerOutcome,
    },
    Quiesce {
        deadline: Instant,
        response: oneshot::Sender<Result<QuiesceResult, TaskManagerError>>,
    },
}

struct ActiveRunner {
    cancellation: CancellationToken,
    _permit: OwnedSemaphorePermit,
    done_sender: Option<oneshot::Sender<()>>,
    done_receiver: Option<oneshot::Receiver<()>>,
}

struct TaskManager {
    store: Store,
    writer: StoreWriterHandle,
    service_state: ServiceStateController,
    runner: Arc<dyn TaskRunner>,
    semaphore: Arc<Semaphore>,
    sender: mpsc::Sender<TaskManagerMessage>,
    receiver: mpsc::Receiver<TaskManagerMessage>,
    active: HashMap<TaskId, ActiveRunner>,
    scan_requested: bool,
    degraded: bool,
    frozen: bool,
    #[cfg(test)]
    claim_hooks: Option<Arc<ClaimTestHooks>>,
}

impl TaskManager {
    async fn run(mut self) {
        let mut reconcile = tokio::time::interval(RECONCILE_INTERVAL);
        reconcile.set_missed_tick_behavior(MissedTickBehavior::Skip);
        reconcile.tick().await;

        loop {
            tokio::select! {
                message = self.receiver.recv() => {
                    let Some(message) = message else { break };
                    self.handle_message(message).await;
                }
                _ = reconcile.tick() => {
                    if self.claims_allowed() {
                        self.scan_requested = true;
                    }
                }
            }

            if self.scan_requested && self.claims_allowed() {
                self.scan_requested = false;
                if let Err(error) = self.claim_queued().await {
                    tracing::warn!(error = %error, "task reconciliation scan failed");
                }
            }
        }
    }

    async fn handle_message(&mut self, message: TaskManagerMessage) {
        match message {
            TaskManagerMessage::NotifyQueued { response, .. } => {
                let result = if self.frozen {
                    Err(TaskManagerError::Frozen)
                } else if self.degraded || self.service_state.current().state != ServiceState::Ready
                {
                    Err(TaskManagerError::StoreDegraded)
                } else {
                    self.scan_requested = true;
                    Ok(())
                };
                let _ = response.send(result);
            }
            TaskManagerMessage::Cancel { task_id, response } => {
                let result = self.cancel_task(task_id).await;
                let _ = response.send(result);
            }
            TaskManagerMessage::RunnerEvent {
                task_id,
                event,
                response,
            } => {
                let result = self.append_runner_event(task_id, event).await;
                let _ = response.send(result);
            }
            TaskManagerMessage::RunnerFinished { task_id, outcome } => {
                self.finish_runner(task_id, outcome).await;
            }
            TaskManagerMessage::Quiesce { deadline, response } => {
                let result = self.quiesce(deadline).await;
                let _ = response.send(result);
            }
        }
    }

    async fn claim_queued(&mut self) -> Result<(), TaskManagerError> {
        let snapshot = self.store.bootstrap_snapshot().await?;
        let repositories = snapshot
            .repositories
            .into_iter()
            .map(|repository| (repository.id, repository))
            .collect::<HashMap<_, _>>();
        let mut queued = snapshot
            .tasks
            .into_iter()
            .filter(|task| task.status == TaskStatus::Queued)
            .collect::<Vec<_>>();
        queued.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.to_string().cmp(&right.id.to_string()))
        });

        for task in queued {
            if self.active.contains_key(&task.id) {
                continue;
            }
            let Ok(permit) = self.semaphore.clone().try_acquire_owned() else {
                break;
            };
            #[cfg(test)]
            self.pause_claim(ClaimPhase::PermitAcquired).await;
            let Some(repository) = repositories.get(&task.repository_id).cloned() else {
                tracing::error!(task_id = %task.id, "queued task references a missing repository");
                drop(permit);
                continue;
            };

            let cancellation = CancellationToken::new();
            let (done_sender, done_receiver) = oneshot::channel();
            self.active.insert(
                task.id,
                ActiveRunner {
                    cancellation: cancellation.clone(),
                    _permit: permit,
                    done_sender: Some(done_sender),
                    done_receiver: Some(done_receiver),
                },
            );
            #[cfg(test)]
            if let Some(hooks) = &self.claim_hooks {
                hooks
                    .active_count
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            #[cfg(test)]
            self.pause_claim(ClaimPhase::HandleRegistered).await;

            let claim = self
                .writer
                .transition_with_event(
                    task.id,
                    TaskStatus::Queued,
                    TaskTransition::Running,
                    background_deadline(),
                )
                .await;
            match claim {
                Ok(receipt) => match receipt.value {
                    TransitionOutcome::Applied { task, .. } => {
                        #[cfg(test)]
                        self.pause_claim(ClaimPhase::RunningCommitted).await;
                        self.spawn_runner(task, repository, cancellation);
                    }
                    TransitionOutcome::Conflict { .. } => {
                        self.remove_active(task.id);
                    }
                },
                Err(error) => {
                    tracing::warn!(task_id = %task.id, error = %error, "task claim failed");
                    self.remove_active(task.id);
                    break;
                }
            }
        }
        Ok(())
    }

    fn spawn_runner(&self, task: Task, repository: Repository, cancellation: CancellationToken) {
        let task_id = task.id;
        let runner = self.runner.clone();
        let sender = self.sender.clone();
        let sink = RunnerEventSink {
            task_id,
            sender: sender.clone(),
        };
        let context = RunContext {
            task,
            repository,
            cancellation,
        };
        let join = tokio::spawn(async move { runner.run(context, sink).await });
        tokio::spawn(async move {
            let outcome = match join.await {
                Ok(outcome) => outcome,
                Err(error) => {
                    tracing::error!(task_id = %task_id, error = %error, "task runner panicked");
                    RunnerOutcome::Failed(runner_panicked_failure())
                }
            };
            let _ = sender
                .send(TaskManagerMessage::RunnerFinished { task_id, outcome })
                .await;
        });
    }

    async fn cancel_task(&mut self, task_id: TaskId) -> Result<CancelOutcome, TaskManagerError> {
        if self.frozen {
            return Err(TaskManagerError::Frozen);
        }
        if self.degraded {
            return Err(TaskManagerError::StoreDegraded);
        }
        let task = self.load_task(task_id).await?;
        match task.status {
            TaskStatus::Queued => {
                let receipt = self
                    .writer
                    .transition_with_event(
                        task_id,
                        TaskStatus::Queued,
                        TaskTransition::Cancelled,
                        background_deadline(),
                    )
                    .await?;
                match receipt.value {
                    TransitionOutcome::Applied { task, .. } => {
                        Ok(CancelOutcome::Cancelled { task })
                    }
                    TransitionOutcome::Conflict { current } => self.cancel_current(current).await,
                }
            }
            current => self.cancel_current_with_status(task, current).await,
        }
    }

    async fn cancel_current(&mut self, task: Task) -> Result<CancelOutcome, TaskManagerError> {
        let status = task.status;
        self.cancel_current_with_status(task, status).await
    }

    async fn cancel_current_with_status(
        &mut self,
        task: Task,
        status: TaskStatus,
    ) -> Result<CancelOutcome, TaskManagerError> {
        match status {
            TaskStatus::Running => {
                let active = self
                    .active
                    .get(&task.id)
                    .ok_or(TaskManagerError::Invariant(
                        "running task has no active cancellation token",
                    ))?;
                active.cancellation.cancel();
                let task = self.load_task(task.id).await?;
                Ok(CancelOutcome::Accepted { task })
            }
            TaskStatus::Cancelled => Ok(CancelOutcome::Cancelled { task }),
            TaskStatus::Queued => Err(TaskManagerError::Invariant(
                "queued conflict was not retried by the cancel owner",
            )),
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Interrupted => {
                Err(TaskManagerError::TaskNotCancellable { task })
            }
        }
    }

    async fn append_runner_event(
        &mut self,
        task_id: TaskId,
        event: RunnerEvent,
    ) -> Result<EventId, RunnerEventError> {
        if self.frozen || self.degraded || self.service_state.current().state != ServiceState::Ready
        {
            return Err(RunnerEventError::StoreDegraded);
        }
        if !self.active.contains_key(&task_id) {
            return Err(RunnerEventError::TaskNotRunning);
        }
        let receipt = self
            .writer
            .append_running_event(task_id, event.into_payload(), background_deadline())
            .await;
        match receipt {
            Ok(receipt) => match receipt.value {
                AppendEventOutcome::Applied { event_id } => Ok(event_id),
                AppendEventOutcome::NotRunning { .. } => Err(RunnerEventError::TaskNotRunning),
            },
            Err(error) => {
                tracing::error!(task_id = %task_id, error = %error, "runner event persistence failed");
                self.enter_degraded();
                Err(RunnerEventError::StoreDegraded)
            }
        }
    }

    async fn finish_runner(&mut self, task_id: TaskId, outcome: RunnerOutcome) {
        if !self.active.contains_key(&task_id) {
            return;
        }
        if self.frozen {
            self.remove_active(task_id);
            return;
        }
        let transition = match outcome {
            RunnerOutcome::Succeeded => TaskTransition::Completed,
            RunnerOutcome::Cancelled => TaskTransition::Cancelled,
            RunnerOutcome::Failed(failure) => TaskTransition::Failed(failure),
        };
        let result = self
            .writer
            .transition_with_event(
                task_id,
                TaskStatus::Running,
                transition,
                background_deadline(),
            )
            .await;
        match result {
            Ok(receipt) => {
                if let TransitionOutcome::Conflict { current } = receipt.value {
                    tracing::debug!(task_id = %task_id, status = ?current.status, "late runner result rejected");
                }
            }
            Err(error) => {
                tracing::error!(task_id = %task_id, error = %error, "runner result persistence failed");
                self.enter_degraded();
            }
        }
        self.remove_active(task_id);
        if self.claims_allowed() {
            self.scan_requested = true;
        }
    }

    async fn quiesce(&mut self, deadline: Instant) -> Result<QuiesceResult, TaskManagerError> {
        if self.frozen {
            return Err(TaskManagerError::Frozen);
        }
        self.frozen = true;
        self.scan_requested = false;
        let _ = self.service_state.set(ServiceState::Quiescing);
        let now = current_timestamp().map_err(TaskManagerError::Invariant)?;
        let recovery = self
            .writer
            .recover_incomplete(now, shutdown_failure(), deadline)
            .await;
        let active = self.take_shutdown_handles();
        Ok(match recovery {
            Ok(receipt) => QuiesceResult::Durable {
                recovery: receipt.value,
                active,
            },
            Err(error) => QuiesceResult::Frozen { active, error },
        })
    }

    fn take_shutdown_handles(&mut self) -> Vec<RunnerShutdownHandle> {
        let mut task_ids = self.active.keys().copied().collect::<Vec<_>>();
        task_ids.sort_by_key(ToString::to_string);
        task_ids
            .into_iter()
            .filter_map(|task_id| {
                let active = self.active.get_mut(&task_id)?;
                Some(RunnerShutdownHandle {
                    task_id,
                    cancellation: active.cancellation.clone(),
                    done: active.done_receiver.take()?,
                })
            })
            .collect()
    }

    fn remove_active(&mut self, task_id: TaskId) {
        if let Some(mut active) = self.active.remove(&task_id) {
            #[cfg(test)]
            if let Some(hooks) = &self.claim_hooks {
                hooks
                    .active_count
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }
            if let Some(done) = active.done_sender.take() {
                let _ = done.send(());
            }
        }
    }

    #[cfg(test)]
    async fn pause_claim(&self, phase: ClaimPhase) {
        if let Some(hooks) = &self.claim_hooks {
            hooks.pause(phase).await;
        }
    }

    fn enter_degraded(&mut self) {
        self.degraded = true;
        self.scan_requested = false;
        let _ = self.service_state.set(ServiceState::StoreDegraded);
        for active in self.active.values() {
            active.cancellation.cancel();
        }
    }

    fn claims_allowed(&self) -> bool {
        !self.degraded && !self.frozen && self.service_state.current().state == ServiceState::Ready
    }

    async fn load_task(&self, task_id: TaskId) -> Result<Task, TaskManagerError> {
        self.store
            .task_detail(task_id)
            .await?
            .map(|detail| detail.task)
            .ok_or(TaskManagerError::TaskNotFound)
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimPhase {
    PermitAcquired,
    HandleRegistered,
    RunningCommitted,
}

#[cfg(test)]
struct ClaimTestHooks {
    phase: ClaimPhase,
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
    active_count: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl ClaimTestHooks {
    fn new(phase: ClaimPhase) -> Self {
        Self {
            phase,
            reached: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            active_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    async fn pause(&self, phase: ClaimPhase) {
        if self.phase == phase {
            self.reached.notify_one();
            self.release.notified().await;
        }
    }

    async fn wait_until_reached(&self) {
        self.reached.notified().await;
    }

    fn resume(&self) {
        self.release.notify_one();
    }

    fn active_count(&self) -> usize {
        self.active_count.load(std::sync::atomic::Ordering::SeqCst)
    }
}

fn background_deadline() -> Instant {
    Instant::now() + BACKGROUND_WRITE_BUDGET
}

fn runner_panicked_failure() -> TaskFailure {
    TaskFailure {
        code: "RUNNER_PANICKED".to_owned(),
        message: "task runner panicked".to_owned(),
        retryable: false,
    }
}

fn shutdown_failure() -> TaskFailure {
    TaskFailure {
        code: "APP_SHUTDOWN".to_owned(),
        message: "application shut down before the task finished".to_owned(),
        retryable: true,
    }
}

fn current_timestamp() -> Result<UtcTimestamp, &'static str> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "system clock predates the Unix epoch")?;
    let seconds = i64::try_from(duration.as_secs())
        .map_err(|_| "system clock exceeds supported timestamp")?;
    let days = seconds.div_euclid(86_400);
    let seconds_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = seconds_of_day % 3_600 / 60;
    let second = seconds_of_day % 60;
    let value = format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{:09}Z",
        duration.subsec_nanos()
    );
    UtcTimestamp::parse_rfc3339(&value)
        .map_err(|_| "system clock produced an invalid UTC timestamp")
}

fn civil_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let days = days_since_epoch + 719_468;
    let era = if days >= 0 {
        days / 146_097
    } else {
        (days - 146_096) / 146_097
    };
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use coding_agent_domain::{CanonicalPath, ClientRequestId, NewRepository, NewTask};
    use coding_agent_store::{CreateTaskOutcome, RegisterRepositoryOutcome};

    use super::*;

    struct NoopWake;

    impl crate::EventWake for NoopWake {
        fn wake(&self) {}
    }

    #[derive(Default)]
    struct CancellingRunner {
        starts: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl TaskRunner for CancellingRunner {
        async fn run(&self, context: RunContext, _sink: RunnerEventSink) -> RunnerOutcome {
            self.starts.fetch_add(1, Ordering::SeqCst);
            context.cancellation.cancelled().await;
            RunnerOutcome::Cancelled
        }
    }

    #[test]
    fn unix_day_conversion_covers_epoch_and_leap_day() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(19_782), (2024, 2, 29));
    }

    #[tokio::test]
    async fn all_claim_pause_phases_preserve_running_token_invariant() {
        for phase in [
            ClaimPhase::PermitAcquired,
            ClaimPhase::HandleRegistered,
            ClaimPhase::RunningCommitted,
        ] {
            let temp_dir = tempfile::tempdir().expect("create claim-pause fixture directory");
            let store = Store::open(temp_dir.path().join("store.sqlite3"))
                .await
                .expect("open claim-pause store");
            store.migrate().await.expect("migrate claim-pause store");
            let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
            let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(NoopWake), 8);
            let runner = Arc::new(CancellingRunner::default());
            let hooks = Arc::new(ClaimTestHooks::new(phase));
            let manager = TaskManagerHandle::spawn_with_claim_hooks(
                store.clone(),
                writer.clone(),
                ServiceStateController::new(ServiceState::Ready),
                runner.clone(),
                1,
                8,
                hooks.clone(),
            );
            let created = writer
                .create_task(
                    NewTask::try_new(ClientRequestId::new(), repository.id, "claim pause")
                        .expect("construct claim-pause task"),
                    background_deadline(),
                )
                .await
                .expect("create claim-pause task");
            let task = match created.value {
                CreateTaskOutcome::Created { task, .. } | CreateTaskOutcome::Existing { task } => {
                    task
                }
            };

            manager.notify_queued(task.id).await.expect("notify actor");
            hooks.wait_until_reached().await;
            let paused = store
                .task_detail(task.id)
                .await
                .expect("read paused task")
                .expect("paused task exists")
                .task;
            match phase {
                ClaimPhase::PermitAcquired => {
                    assert_eq!(paused.status, TaskStatus::Queued);
                    assert_eq!(hooks.active_count(), 0);
                }
                ClaimPhase::HandleRegistered => {
                    assert_eq!(paused.status, TaskStatus::Queued);
                    assert_eq!(hooks.active_count(), 1);
                }
                ClaimPhase::RunningCommitted => {
                    assert_eq!(paused.status, TaskStatus::Running);
                    assert_eq!(hooks.active_count(), 1);
                }
            }

            let cancel = tokio::spawn({
                let manager = manager.clone();
                async move { manager.cancel(task.id).await }
            });
            tokio::task::yield_now().await;
            hooks.resume();
            assert!(matches!(
                cancel.await.expect("join cancel").expect("cancel task"),
                CancelOutcome::Accepted { .. }
            ));
            wait_for_status(&store, task.id, TaskStatus::Cancelled).await;
            assert_eq!(runner.starts.load(Ordering::SeqCst), 1);
            assert_eq!(hooks.active_count(), 0);
        }
    }

    async fn register_repository(store: &Store, root: PathBuf) -> Repository {
        let input = NewRepository {
            selected_path: canonical(root.join("selected")),
            display_name: "claim pause".to_owned(),
            git_root: canonical(root.join("git")),
            cargo_workspace_root: canonical(root.join("workspace")),
        };
        match store
            .register_repository(input)
            .await
            .expect("register claim-pause repository")
        {
            RegisterRepositoryOutcome::Created(repository)
            | RegisterRepositoryOutcome::Existing(repository) => repository,
        }
    }

    fn canonical(path: PathBuf) -> CanonicalPath {
        CanonicalPath::try_from_canonical(path).expect("construct claim-pause canonical path")
    }

    async fn wait_for_status(store: &Store, task_id: TaskId, expected: TaskStatus) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let task = store
                    .task_detail(task_id)
                    .await
                    .expect("load claim-pause task")
                    .expect("claim-pause task exists")
                    .task;
                if task.status == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("claim-pause task reaches expected status");
    }
}
