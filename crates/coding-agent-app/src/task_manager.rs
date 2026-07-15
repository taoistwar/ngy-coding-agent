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
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast, mpsc, oneshot};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::{
    DegradedCoordinator, DegradedCoordinatorError, DegradedRecoveryResult, EventDispatcherHandle,
    PendingDurableResult, ServiceState, ServiceStateController, StoreWriterError,
    StoreWriterHandle,
};

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
    #[cfg(feature = "test-support")]
    launch_ordinal: u64,
}

#[cfg(feature = "test-support")]
impl RunContext {
    pub(crate) const fn launch_ordinal(&self) -> u64 {
        self.launch_ordinal
    }
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
    degraded_recoveries: broadcast::Sender<DegradedRecoveryResult>,
}

impl TaskManagerHandle {
    pub fn spawn(
        store: Store,
        writer: StoreWriterHandle,
        dispatcher: EventDispatcherHandle,
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
        let coordinator = DegradedCoordinator::new(
            writer.clone(),
            dispatcher,
            service_state.clone(),
            sender.downgrade(),
        );
        let (degraded_recoveries, _) = broadcast::channel(16);
        let actor = TaskManager {
            store,
            writer,
            service_state,
            runner,
            semaphore: Arc::new(Semaphore::new(concurrency)),
            sender: sender.downgrade(),
            receiver,
            active: HashMap::new(),
            coordinator,
            degraded_recoveries: degraded_recoveries.clone(),
            pending_durable_results: Vec::new(),
            scan_requested: false,
            degraded: false,
            frozen: false,
            #[cfg(feature = "test-support")]
            next_launch_ordinal: 0,
            #[cfg(test)]
            claim_hooks: None,
            #[cfg(test)]
            exit_probe: None,
        };
        tokio::spawn(actor.run());
        Self {
            sender,
            degraded_recoveries,
        }
    }

    #[cfg(test)]
    fn spawn_with_claim_hooks(
        runtime: (
            Store,
            StoreWriterHandle,
            EventDispatcherHandle,
            ServiceStateController,
        ),
        runner: Arc<dyn TaskRunner>,
        concurrency: usize,
        capacity: usize,
        claim_hooks: Arc<ClaimTestHooks>,
    ) -> Self {
        let (store, writer, dispatcher, service_state) = runtime;
        assert!(concurrency > 0, "task-manager concurrency must be positive");
        assert!(
            capacity > 0,
            "task-manager channel capacity must be positive"
        );
        let (sender, receiver) = mpsc::channel(capacity);
        let coordinator = DegradedCoordinator::new(
            writer.clone(),
            dispatcher,
            service_state.clone(),
            sender.downgrade(),
        );
        let (degraded_recoveries, _) = broadcast::channel(16);
        let actor = TaskManager {
            store,
            writer,
            service_state,
            runner,
            semaphore: Arc::new(Semaphore::new(concurrency)),
            sender: sender.downgrade(),
            receiver,
            active: HashMap::new(),
            coordinator,
            degraded_recoveries: degraded_recoveries.clone(),
            pending_durable_results: Vec::new(),
            scan_requested: false,
            degraded: false,
            frozen: false,
            #[cfg(feature = "test-support")]
            next_launch_ordinal: 0,
            claim_hooks: Some(claim_hooks),
            exit_probe: None,
        };
        tokio::spawn(actor.run());
        Self {
            sender,
            degraded_recoveries,
        }
    }

    pub fn subscribe_degraded_recovery(&self) -> broadcast::Receiver<DegradedRecoveryResult> {
        self.degraded_recoveries.subscribe()
    }

    #[cfg(test)]
    async fn install_exit_probe(&self) -> oneshot::Receiver<()> {
        let (exited, receiver) = oneshot::channel();
        let (installed, installed_receiver) = oneshot::channel();
        self.send(TaskManagerMessage::InstallExitProbe { exited, installed })
            .await
            .expect("install task-manager exit probe");
        installed_receiver
            .await
            .expect("task manager acknowledges exit probe");
        receiver
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

pub(crate) enum TaskManagerMessage {
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
    FinalizeDegraded {
        recovery: coding_agent_store::RecoveryOutcome,
        response: oneshot::Sender<Result<DegradedRecoveryResult, DegradedCoordinatorError>>,
    },
    Quiesce {
        deadline: Instant,
        response: oneshot::Sender<Result<QuiesceResult, TaskManagerError>>,
    },
    #[cfg(test)]
    InstallExitProbe {
        exited: oneshot::Sender<()>,
        installed: oneshot::Sender<()>,
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
    sender: mpsc::WeakSender<TaskManagerMessage>,
    receiver: mpsc::Receiver<TaskManagerMessage>,
    active: HashMap<TaskId, ActiveRunner>,
    coordinator: DegradedCoordinator,
    degraded_recoveries: broadcast::Sender<DegradedRecoveryResult>,
    pending_durable_results: Vec<PendingDurableResult>,
    scan_requested: bool,
    degraded: bool,
    frozen: bool,
    #[cfg(feature = "test-support")]
    next_launch_ordinal: u64,
    #[cfg(test)]
    claim_hooks: Option<Arc<ClaimTestHooks>>,
    #[cfg(test)]
    exit_probe: Option<oneshot::Sender<()>>,
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

        #[cfg(test)]
        if let Some(exited) = self.exit_probe.take() {
            let _ = exited.send(());
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
            TaskManagerMessage::FinalizeDegraded { recovery, response } => {
                let result = self.finalize_degraded(recovery);
                let _ = response.send(result);
            }
            TaskManagerMessage::Quiesce { deadline, response } => {
                let result = self.quiesce(deadline).await;
                let _ = response.send(result);
            }
            #[cfg(test)]
            TaskManagerMessage::InstallExitProbe { exited, installed } => {
                self.exit_probe = Some(exited);
                let _ = installed.send(());
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

            let Some(sender) = self.sender.upgrade() else {
                self.remove_active(task.id);
                break;
            };
            if sender.is_closed() {
                self.remove_active(task.id);
                break;
            }

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
                        self.spawn_runner(task, repository, cancellation, sender);
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

    fn spawn_runner(
        &mut self,
        task: Task,
        repository: Repository,
        cancellation: CancellationToken,
        sender: mpsc::Sender<TaskManagerMessage>,
    ) {
        let task_id = task.id;
        #[cfg(feature = "test-support")]
        let launch_ordinal = {
            let launch_ordinal = self.next_launch_ordinal;
            self.next_launch_ordinal = self
                .next_launch_ordinal
                .checked_add(1)
                .expect("task launch ordinal overflow");
            launch_ordinal
        };
        let runner = self.runner.clone();
        let sink = RunnerEventSink {
            task_id,
            sender: sender.clone(),
        };
        let context = RunContext {
            task,
            repository,
            cancellation,
            #[cfg(feature = "test-support")]
            launch_ordinal,
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
        if self.frozen {
            return Err(RunnerEventError::StoreDegraded);
        }
        if self.degraded {
            self.pending_durable_results
                .push(PendingDurableResult::RunnerEvent { task_id, event });
            return Err(RunnerEventError::StoreDegraded);
        }
        if self.service_state.current().state != ServiceState::Ready {
            return Err(RunnerEventError::StoreDegraded);
        }
        if !self.active.contains_key(&task_id) {
            return Err(RunnerEventError::TaskNotRunning);
        }
        let pending = PendingDurableResult::RunnerEvent {
            task_id,
            event: event.clone(),
        };
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
                self.enter_degraded(pending);
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
        if self.degraded {
            self.pending_durable_results
                .push(PendingDurableResult::RunnerTerminal { task_id, outcome });
            self.remove_active(task_id);
            return;
        }
        let pending = PendingDurableResult::RunnerTerminal {
            task_id,
            outcome: outcome.clone(),
        };
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
                self.enter_degraded(pending);
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

    fn enter_degraded(&mut self, pending: PendingDurableResult) {
        self.pending_durable_results.push(pending);
        if self.degraded {
            return;
        }
        self.degraded = true;
        self.scan_requested = false;
        let _ = self.service_state.set(ServiceState::StoreDegraded);
        for active in self.active.values() {
            active.cancellation.cancel();
        }
        let coordinator = self.coordinator.clone();
        tokio::spawn(async move {
            if let Err(error) = coordinator.run().await
                && error != DegradedCoordinatorError::Quiescing
            {
                tracing::error!(error = %error, "degraded recovery coordinator stopped");
            }
        });
    }

    fn finalize_degraded(
        &mut self,
        recovery: coding_agent_store::RecoveryOutcome,
    ) -> Result<DegradedRecoveryResult, DegradedCoordinatorError> {
        if self.frozen || self.service_state.current().state == ServiceState::Quiescing {
            return Err(DegradedCoordinatorError::Quiescing);
        }
        if !self.degraded {
            return Err(DegradedCoordinatorError::ManagerClosed);
        }
        let pending = std::mem::take(&mut self.pending_durable_results);
        self.degraded = false;
        self.scan_requested = true;
        let ready = match self.service_state.set(ServiceState::Ready) {
            Ok(ready) => ready,
            Err(_) => {
                self.pending_durable_results = pending;
                self.degraded = true;
                self.scan_requested = false;
                return Err(DegradedCoordinatorError::Quiescing);
            }
        };
        let result = DegradedRecoveryResult {
            recovery,
            discarded_pending_count: pending.len(),
            ready_generation: ready.generation,
        };
        let _ = self.degraded_recoveries.send(result.clone());
        Ok(result)
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

pub(crate) fn current_timestamp() -> Result<UtcTimestamp, &'static str> {
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
    use tokio::sync::mpsc::error::TryRecvError;

    use super::*;
    use crate::{FakeRunnerConfig, FakeTaskRunner};

    #[derive(Default)]
    struct CancellingRunner {
        starts: AtomicUsize,
    }

    #[derive(Default)]
    struct ReleaseRunner {
        started: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl TaskRunner for CancellingRunner {
        async fn run(&self, context: RunContext, _sink: RunnerEventSink) -> RunnerOutcome {
            self.starts.fetch_add(1, Ordering::SeqCst);
            context.cancellation.cancelled().await;
            RunnerOutcome::Cancelled
        }
    }

    #[async_trait::async_trait]
    impl TaskRunner for ReleaseRunner {
        async fn run(&self, _context: RunContext, _sink: RunnerEventSink) -> RunnerOutcome {
            self.started.notify_one();
            self.release.notified().await;
            RunnerOutcome::Succeeded
        }
    }

    #[test]
    fn unix_day_conversion_covers_epoch_and_leap_day() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(19_782), (2024, 2, 29));
    }

    #[tokio::test]
    async fn dropping_the_last_idle_handle_releases_the_manager_actor() {
        let temp_dir = tempfile::tempdir().expect("create manager-exit fixture directory");
        let store = Store::open(temp_dir.path().join("store.sqlite3"))
            .await
            .expect("open manager-exit store");
        store.migrate().await.expect("migrate manager-exit store");
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
            .await
            .expect("spawn manager-exit dispatcher");
        let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
        let manager = TaskManagerHandle::spawn(
            store,
            writer,
            dispatcher,
            ServiceStateController::new(ServiceState::Ready),
            Arc::new(CancellingRunner::default()),
            1,
            8,
        );
        let mut exited = manager.install_exit_probe().await;

        drop(manager);
        match tokio::time::timeout(Duration::from_secs(2), &mut exited).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => panic!("manager dropped its exit probe without a clean actor exit"),
            Err(_) => {
                panic!("idle task-manager actor stayed alive after its final handle was dropped")
            }
        }
    }

    #[tokio::test]
    async fn active_runner_sender_keeps_actor_alive_until_terminal_cleanup() {
        let temp_dir = tempfile::tempdir().expect("create active-exit fixture directory");
        let store = Store::open(temp_dir.path().join("store.sqlite3"))
            .await
            .expect("open active-exit store");
        store.migrate().await.expect("migrate active-exit store");
        let repository = register_repository(&store, temp_dir.path().to_path_buf()).await;
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
            .await
            .expect("spawn active-exit dispatcher");
        let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
        let runner = Arc::new(ReleaseRunner::default());
        let manager = TaskManagerHandle::spawn(
            store.clone(),
            writer.clone(),
            dispatcher,
            ServiceStateController::new(ServiceState::Ready),
            runner.clone(),
            1,
            8,
        );
        let task = writer
            .create_task(
                NewTask::try_new(ClientRequestId::new(), repository.id, "active exit")
                    .expect("construct active-exit task"),
                background_deadline(),
            )
            .await
            .expect("create active-exit task")
            .value
            .task()
            .clone();
        manager.notify_queued(task.id).await.expect("notify actor");
        runner.started.notified().await;
        wait_for_status(&store, task.id, TaskStatus::Running).await;
        let mut exited = manager.install_exit_probe().await;
        let manager_sender = manager.sender.downgrade();

        drop(manager);
        assert!(matches!(
            exited.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert!(manager_sender.strong_count() > 0);

        runner.release.notify_one();
        wait_for_status(&store, task.id, TaskStatus::Completed).await;
        match tokio::time::timeout(Duration::from_secs(2), &mut exited).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => panic!("active manager dropped its exit probe without cleanup"),
            Err(_) => panic!(
                "manager actor stayed alive after active terminal cleanup; strong_senders={}",
                manager_sender.strong_count()
            ),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn fake_runner_uses_exact_deadlines_and_cancels_before_the_next_boundary() {
        assert_eq!(
            FakeRunnerConfig::default().emission_interval(),
            Duration::from_millis(200)
        );
        let cancellation = CancellationToken::new();
        let context = fake_run_context(cancellation.clone());
        let task_id = context.task.id;
        let (sender, mut receiver) = mpsc::channel(8);
        let sink = RunnerEventSink { task_id, sender };
        let run = tokio::spawn(async move { FakeTaskRunner::default().run(context, sink).await });

        assert!(matches!(
            acknowledge_runner_event(&mut receiver, 1).await,
            RunnerEvent::PlanUpdated(_)
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(199)).await;
        tokio::task::yield_now().await;
        assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(matches!(
            acknowledge_runner_event(&mut receiver, 2).await,
            RunnerEvent::ActivityAppended(ActivityEntry { id, .. }) if id == "fake-plan-ready"
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(199)).await;
        cancellation.cancel();
        tokio::time::advance(Duration::from_millis(1)).await;

        assert_eq!(
            run.await.expect("join fake runner"),
            RunnerOutcome::Cancelled
        );
        assert!(matches!(
            receiver.try_recv(),
            Err(TryRecvError::Empty | TryRecvError::Disconnected)
        ));
    }

    async fn acknowledge_runner_event(
        receiver: &mut mpsc::Receiver<TaskManagerMessage>,
        event_id: i64,
    ) -> RunnerEvent {
        let message = receiver.recv().await.expect("fake runner sends an event");
        let TaskManagerMessage::RunnerEvent {
            event, response, ..
        } = message
        else {
            panic!("fake runner sink sends only runner events");
        };
        response
            .send(Ok(EventId::new(event_id).expect("positive event ID")))
            .expect("fake runner awaits event acknowledgement");
        event
    }

    fn fake_run_context(cancellation: CancellationToken) -> RunContext {
        let repository_id = coding_agent_domain::RepositoryId::new();
        let timestamp = UtcTimestamp::parse_rfc3339("2026-07-15T00:00:00Z")
            .expect("construct fake runner timestamp");
        let root = std::env::current_dir().expect("read test current directory");
        let task = Task::try_from_stored(Task {
            id: TaskId::new(),
            client_request_id: ClientRequestId::new(),
            repository_id,
            prompt: "direct fake runner test".to_owned(),
            status: TaskStatus::Running,
            attempt: 1,
            retry_of: None,
            created_at: timestamp,
            started_at: Some(timestamp),
            finished_at: None,
            last_event_id: EventId::new(1).expect("positive event ID"),
            failure: None,
        })
        .expect("construct valid running task");
        RunContext {
            task,
            repository: Repository {
                id: repository_id,
                selected_path: canonical(root.join("fake-selected")),
                display_name: "fake runner".to_owned(),
                git_root: canonical(root.join("fake-git")),
                cargo_workspace_root: canonical(root.join("fake-workspace")),
                created_at: timestamp,
                last_opened_at: timestamp,
            },
            cancellation,
            #[cfg(feature = "test-support")]
            launch_ordinal: 0,
        }
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
            let dispatcher = EventDispatcherHandle::spawn(store.clone(), 64)
                .await
                .expect("spawn claim-pause dispatcher");
            let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 8);
            let runner = Arc::new(CancellingRunner::default());
            let hooks = Arc::new(ClaimTestHooks::new(phase));
            let manager = TaskManagerHandle::spawn_with_claim_hooks(
                (
                    store.clone(),
                    writer.clone(),
                    dispatcher,
                    ServiceStateController::new(ServiceState::Ready),
                ),
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
