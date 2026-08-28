use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use coding_agent_api::{
    AddRepositoryRequest, ApiBackend, ApiError, ApiResult, AuthContext, BootstrapResponse,
    CancelResult, CreateResult, CreateTaskRequest, LiveEventStream, QuitAcceptance, RepositoryDto,
    SchedulerStateStream, ServiceStateControl, ServiceStateDto, ServiceStateStream, SseBackend,
    TaskDetailDto, TaskDto, TaskEventDto,
};
use coding_agent_domain::{
    CanonicalPath, DomainError, EventCursor, NewRepository, NewTask, RepositoryId, TaskId,
    UtcTimestamp,
};
use coding_agent_store::{
    QueueLimitedCreateTaskOutcome, QueueLimitedRetryTaskOutcome, RegisterRepositoryOutcome, Store,
    StoreError,
};
use http::StatusCode;
use tokio::time::{Instant, timeout_at};

use crate::bootstrap_join::{
    BOOTSTRAP_SNAPSHOT_UNAVAILABLE, BootstrapJoin, BootstrapJoinError, JoinedBootstrapSnapshot,
};
use crate::repository_service::RepositoryRuntimeRegistrar;
use crate::scheduler::{SchedulerStateReader, SchedulerStoreState};
use crate::scheduler_api_projection::project_scheduler_snapshot;
use crate::scheduler_api_projection::{SchedulerApiProjectionError, project_joined_scheduler};
use crate::store_writer::sqlite_code_is_retryable;
#[cfg(feature = "test-support")]
use crate::test_support::{ActorPauseController, ActorPausePoint};
use crate::{
    CancelOutcome, DurableDisposition, EventDispatcherHandle, KnownNotAppliedError,
    KnownNotAppliedReason, NativeDialogService, PickerError, RepositoryDiscovery,
    RepositoryDiscoveryError, SecurityManager, ServiceState, ServiceStateController,
    StoreWriterError, StoreWriterHandle, StoreWriterSubmitError, TaskManagerError,
    TaskManagerHandle,
};

mod runtime_router;

pub use runtime_router::build_runtime_router;
#[cfg(test)]
use runtime_router::{LOCAL_READY_PATH, LOCAL_REOPEN_PATH, REQUEST_ID_HEADER};

mod mutation_gate;

mod delivery;

pub use delivery::{ApplicationDeliveryBackend, build_application_api_router_with_delivery};
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use delivery::{
    map_delivery_busy_for_test, map_delivery_cleanup_eligibility_for_test,
    map_delivery_command_conflict_for_test, map_delivery_eligibility_for_test,
    map_delivery_unavailable_for_test,
};

use mutation_gate::DurableMutationIdentity;
pub(crate) use mutation_gate::MutationDrainOutcome;
pub use mutation_gate::{MutationGate, MutationGuard};

pub struct ApplicationBackend {
    store: Store,
    bootstrap_join: BootstrapJoin,
    scheduler_state_reader: SchedulerStateReader<SchedulerStoreState>,
    writer: StoreWriterHandle,
    dispatcher: EventDispatcherHandle,
    task_manager: TaskManagerHandle,
    repository_registrar: Option<RepositoryRuntimeRegistrar>,
    discovery: RepositoryDiscovery,
    dialog: Option<NativeDialogService>,
    security: SecurityManager,
    service_state: ServiceStateController,
    mutation_gate: MutationGate,
    server_started_at: UtcTimestamp,
    max_concurrent_tasks: u32,
    max_queued_tasks: NonZeroU32,
    write_budget: Duration,
    quit_signal: Arc<dyn Fn() + Send + Sync + 'static>,
    #[cfg(feature = "test-support")]
    actor_pauses: Option<Arc<ActorPauseController>>,
}

impl ApplicationBackend {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_repository_runtime(
        store: Store,
        writer: StoreWriterHandle,
        dispatcher: EventDispatcherHandle,
        task_manager: TaskManagerHandle,
        repository_registrar: RepositoryRuntimeRegistrar,
        discovery: RepositoryDiscovery,
        dialog: Option<NativeDialogService>,
        security: SecurityManager,
        service_state: ServiceStateController,
        mutation_gate: MutationGate,
        server_started_at: UtcTimestamp,
        max_concurrent_tasks: u32,
        max_queued_tasks: NonZeroU32,
        write_budget: Duration,
        quit_signal: Arc<dyn Fn() + Send + Sync + 'static>,
    ) -> Self {
        Self::from_parts(
            store,
            writer,
            dispatcher,
            task_manager,
            Some(repository_registrar),
            discovery,
            dialog,
            security,
            service_state,
            mutation_gate,
            server_started_at,
            max_concurrent_tasks,
            max_queued_tasks,
            write_budget,
            quit_signal,
        )
    }

    /// Explicit fail-closed constructor for API-focused fixtures that do not
    /// exercise successful repository admission.
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn new_without_repository_runtime_for_test(
        store: Store,
        writer: StoreWriterHandle,
        dispatcher: EventDispatcherHandle,
        task_manager: TaskManagerHandle,
        discovery: RepositoryDiscovery,
        dialog: Option<NativeDialogService>,
        security: SecurityManager,
        service_state: ServiceStateController,
        mutation_gate: MutationGate,
        server_started_at: UtcTimestamp,
        max_concurrent_tasks: u32,
        max_queued_tasks: NonZeroU32,
        write_budget: Duration,
        quit_signal: Arc<dyn Fn() + Send + Sync + 'static>,
    ) -> Self {
        Self::from_parts(
            store,
            writer,
            dispatcher,
            task_manager,
            None,
            discovery,
            dialog,
            security,
            service_state,
            mutation_gate,
            server_started_at,
            max_concurrent_tasks,
            max_queued_tasks,
            write_budget,
            quit_signal,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_parts(
        store: Store,
        writer: StoreWriterHandle,
        dispatcher: EventDispatcherHandle,
        task_manager: TaskManagerHandle,
        repository_registrar: Option<RepositoryRuntimeRegistrar>,
        discovery: RepositoryDiscovery,
        dialog: Option<NativeDialogService>,
        security: SecurityManager,
        service_state: ServiceStateController,
        mutation_gate: MutationGate,
        server_started_at: UtcTimestamp,
        max_concurrent_tasks: u32,
        max_queued_tasks: NonZeroU32,
        write_budget: Duration,
        quit_signal: Arc<dyn Fn() + Send + Sync + 'static>,
    ) -> Self {
        assert!(
            max_concurrent_tasks > 0,
            "task concurrency must be positive"
        );
        assert!(!write_budget.is_zero(), "write budget must be positive");
        let scheduler_state_reader = task_manager.scheduler_state_reader();
        let bootstrap_join = BootstrapJoin::new(
            store.clone(),
            service_state.clone(),
            scheduler_state_reader.clone(),
        );
        Self {
            store,
            bootstrap_join,
            scheduler_state_reader,
            writer,
            dispatcher,
            task_manager,
            repository_registrar,
            discovery,
            dialog,
            security,
            service_state,
            mutation_gate,
            server_started_at,
            max_concurrent_tasks,
            max_queued_tasks,
            write_budget,
            quit_signal,
            #[cfg(feature = "test-support")]
            actor_pauses: None,
        }
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn with_bootstrap_join_budget_for_test(mut self, budget: Duration) -> Self {
        self.bootstrap_join.set_budget_for_test(budget);
        self
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn scheduler_state_for_test(&self) -> coding_agent_api::SchedulerStateDto {
        project_scheduler_snapshot(
            self.scheduler_state_reader.current().as_ref(),
            self.server_started_at,
        )
        .expect("published scheduler state remains wire-projectable")
    }

    #[cfg(feature = "test-support")]
    pub(crate) fn with_process_test_pauses(
        mut self,
        actor_pauses: Arc<ActorPauseController>,
    ) -> Self {
        self.actor_pauses = Some(actor_pauses);
        self
    }

    pub fn mutation_gate(&self) -> &MutationGate {
        &self.mutation_gate
    }

    fn deadline(&self) -> Instant {
        Instant::now() + self.write_budget
    }

    #[cfg(feature = "test-support")]
    async fn pause_actor(&self, point: ActorPausePoint) -> bool {
        if let Some(pauses) = &self.actor_pauses {
            return pauses.pause(point).await;
        }
        false
    }

    async fn register_path(&self, path: &Path) -> ApiResult<CreateResult<RepositoryDto>> {
        let deadline = self.deadline();
        let discovered = self
            .discovery
            .discover(path, deadline)
            .await
            .map_err(map_discovery_error)?;
        let input = NewRepository {
            selected_path: canonical(discovered.selected_path)?,
            display_name: discovered.display_name,
            git_root: canonical(discovered.git_root)?,
            cargo_workspace_root: canonical(discovered.cargo_workspace_root)?,
        };
        let delegation = self.mutation_gate.mark_delegated()?;
        let receipt = match self.writer.register_repository(input, deadline).await {
            Ok(receipt) => {
                delegation.confirm_known();
                receipt
            }
            Err(error) => return Err(map_writer_error(error)),
        };
        let repository = match &receipt.value {
            RegisterRepositoryOutcome::Created(repository)
            | RegisterRepositoryOutcome::Existing(repository) => repository,
        };
        self.repository_registrar
            .as_ref()
            .ok_or_else(internal_error)?
            .attach(repository, deadline)
            .await
            .map_err(|_| internal_error())?;
        timeout_at(deadline, self.task_manager.notify_admission_changed())
            .await
            .map_err(|_| internal_error())?
            .map_err(map_manager_error)?;
        let result = match receipt.value {
            RegisterRepositoryOutcome::Created(repository) => {
                tracing::info!(repository_id = %repository.id, "repository registered");
                CreateResult::Created(repository.into())
            }
            RegisterRepositoryOutcome::Existing(repository) => {
                tracing::info!(repository_id = %repository.id, "repository reopened");
                CreateResult::Existing(repository.into())
            }
        };
        Ok(result)
    }
}

#[async_trait::async_trait]
impl ApiBackend for ApplicationBackend {
    async fn bootstrap(&self, auth: &AuthContext) -> ApiResult<BootstrapResponse> {
        let joined = self
            .bootstrap_join
            .snapshot()
            .await
            .map_err(map_bootstrap_join_error)?;
        let scheduler = project_joined_scheduler(&joined, self.server_started_at)
            .map_err(map_scheduler_api_projection_error)?;
        if scheduler.limits.global != self.max_concurrent_tasks
            || scheduler.limits.queued != self.max_queued_tasks.get()
        {
            return Err(map_scheduler_api_projection_error(
                SchedulerApiProjectionError::InconsistentSnapshot,
            ));
        }
        let JoinedBootstrapSnapshot {
            store: snapshot,
            scheduler: _,
            service_state: state,
            server_instance_id: _,
        } = joined;
        #[cfg(feature = "test-support")]
        self.pause_actor(ActorPausePoint::BootstrapBeforeSse).await;
        #[cfg(feature = "test-support")]
        let latest_event_id = if self
            .pause_actor(ActorPausePoint::BootstrapCursorAhead)
            .await
        {
            snapshot.latest_event_id.get().saturating_add(1)
        } else {
            snapshot.latest_event_id.get()
        };
        #[cfg(not(feature = "test-support"))]
        let latest_event_id = snapshot.latest_event_id.get();
        Ok(BootstrapResponse {
            csrf_token: self.security.csrf_for_auth(auth)?,
            repositories: snapshot.repositories.into_iter().map(Into::into).collect(),
            tasks: snapshot.tasks.into_iter().map(Into::into).collect(),
            latest_event_id,
            server_started_at: self.server_started_at.into(),
            service_state: service_state(state.state),
            service_state_generation: state.generation,
            max_concurrent_tasks: self.max_concurrent_tasks,
            scheduler,
        })
    }

    async fn list_repositories(&self, _: &AuthContext) -> ApiResult<Vec<RepositoryDto>> {
        Ok(self
            .store
            .list_repositories()
            .await
            .map_err(map_store_error)?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    async fn add_repository(
        &self,
        _: &AuthContext,
        request: AddRepositoryRequest,
    ) -> ApiResult<CreateResult<RepositoryDto>> {
        self.mutation_gate
            .run_data_mutation(async {
                let result = self.register_path(&request.path).await;
                if let Err(error) = &result {
                    log_failure("repository.add", error);
                }
                result
            })
            .await
    }

    async fn pick_repository(
        &self,
        _: &AuthContext,
    ) -> ApiResult<Option<CreateResult<RepositoryDto>>> {
        self.mutation_gate
            .run_data_mutation(async {
                let dialog = self.dialog.as_ref().ok_or_else(picker_unavailable)?;
                let selected = dialog.pick_repository().await.map_err(map_picker_error)?;
                match selected {
                    Some(path) => self.register_path(&path).await.map(Some),
                    None => Ok(None),
                }
            })
            .await
    }

    async fn list_tasks(
        &self,
        _: &AuthContext,
        repository_id: Option<RepositoryId>,
    ) -> ApiResult<Vec<TaskDto>> {
        let snapshot = self
            .store
            .bootstrap_snapshot()
            .await
            .map_err(map_store_error)?;
        Ok(snapshot
            .tasks
            .into_iter()
            .filter(|task| repository_id.is_none_or(|id| task.repository_id == id))
            .map(Into::into)
            .collect())
    }

    async fn create_task(
        &self,
        _: &AuthContext,
        request: CreateTaskRequest,
    ) -> ApiResult<CreateResult<TaskDto>> {
        self.mutation_gate
            .run_data_mutation(async {
                let input = NewTask::try_new(
                    request.client_request_id,
                    request.repository_id,
                    request.prompt,
                )
                .map_err(map_domain_error)?;
                let identity = DurableMutationIdentity::CreateTask {
                    client_request_id: input.client_request_id,
                    repository_id: input.repository_id,
                    prompt: Arc::from(input.prompt.as_str()),
                };
                #[cfg(feature = "test-support")]
                self.pause_actor(ActorPausePoint::CreateBeforeWrite).await;
                let delegation = self.mutation_gate.mark_identified_delegated(identity)?;
                let submission = self.writer.submit_queue_limited_create(
                    input,
                    self.max_queued_tasks,
                    self.deadline(),
                );
                let completion = match submission {
                    Ok(submission) => submission.completion().await,
                    Err(error) => {
                        delegation.confirm_known();
                        let error = map_submit_error(error);
                        log_failure("task.create", &error);
                        return Err(error);
                    }
                };
                match &completion.disposition {
                    DurableDisposition::Confirmed(_) => delegation.confirm_exact_resolution(),
                    DurableDisposition::KnownNotApplied {
                        reason: KnownNotAppliedReason::ExactReconciliation,
                        outcome: Some(QueueLimitedCreateTaskOutcome::QueueFull { .. }),
                        ..
                    } => delegation.confirm_exact_resolution(),
                    DurableDisposition::KnownNotApplied { .. } => {
                        delegation.confirm_known();
                    }
                    DurableDisposition::OutcomeUnknown { .. }
                    | DurableDisposition::InvariantConflict { .. } => drop(delegation),
                }
                match completion.disposition {
                    DurableDisposition::Confirmed(QueueLimitedCreateTaskOutcome::Created {
                        task,
                        ..
                    }) => {
                        notify_after_commit(&self.task_manager, task.id).await;
                        tracing::info!(
                            task_id = %task.id,
                            repository_id = %task.repository_id,
                            disposition = "created",
                            "task mutation committed"
                        );
                        Ok(CreateResult::Created(task.into()))
                    }
                    DurableDisposition::Confirmed(QueueLimitedCreateTaskOutcome::Existing {
                        task,
                    }) => {
                        tracing::info!(
                            task_id = %task.id,
                            repository_id = %task.repository_id,
                            disposition = "existing",
                            "task mutation replayed"
                        );
                        Ok(CreateResult::Existing(task.into()))
                    }
                    DurableDisposition::Confirmed(QueueLimitedCreateTaskOutcome::QueueFull {
                        queued_tasks,
                        max_queued_tasks,
                    })
                    | DurableDisposition::KnownNotApplied {
                        outcome:
                            Some(QueueLimitedCreateTaskOutcome::QueueFull {
                                queued_tasks,
                                max_queued_tasks,
                            }),
                        ..
                    } => Err(map_task_queue_full(queued_tasks, max_queued_tasks)),
                    DurableDisposition::KnownNotApplied {
                        error: Some(error), ..
                    } => Err(map_known_not_applied_error(error)),
                    DurableDisposition::KnownNotApplied { .. } => Err(store_busy()),
                    DurableDisposition::OutcomeUnknown { .. } => Err(app_shutting_down()),
                    DurableDisposition::InvariantConflict { .. } => Err(internal_error()),
                }
            })
            .await
    }

    async fn task_detail(&self, _: &AuthContext, id: TaskId) -> ApiResult<TaskDetailDto> {
        let detail = self
            .store
            .task_detail(id)
            .await
            .map_err(map_store_error)?
            .ok_or_else(task_not_found)?;
        #[cfg(feature = "test-support")]
        self.pause_actor(ActorPausePoint::TaskDetailAfterSnapshot)
            .await;
        Ok(TaskDetailDto {
            task: detail.task.into(),
            plan: detail.plan.map(Into::into),
            activity: detail.activity.into_iter().map(Into::into).collect(),
            diff: detail.diff.map(Into::into),
            tests: detail.tests.map(Into::into),
            reviews: detail.reviews.into_iter().map(Into::into).collect(),
            timeline: detail.timeline.into_iter().map(Into::into).collect(),
            event_cursor: detail.event_cursor.get(),
        })
    }

    async fn cancel_task(&self, _: &AuthContext, id: TaskId) -> ApiResult<CancelResult> {
        self.mutation_gate
            .run_data_mutation(async {
                let delegation = self.mutation_gate.mark_delegated()?;
                let outcome = self.task_manager.cancel(id).await;
                if matches!(
                    &outcome,
                    Err(TaskManagerError::Closed | TaskManagerError::DeadlineElapsed)
                ) {
                    drop(delegation);
                } else {
                    delegation.confirm_known();
                }
                match outcome.map_err(map_manager_error)? {
                    CancelOutcome::Cancelled { task } => Ok(CancelResult::Finished(task.into())),
                    CancelOutcome::Finished { task } => {
                        Err(map_manager_error(TaskManagerError::TaskNotCancellable {
                            task,
                        }))
                    }
                    CancelOutcome::Accepted { task } => {
                        Ok(CancelResult::Accepted { task: task.into() })
                    }
                }
            })
            .await
    }

    async fn retry_task(&self, _: &AuthContext, id: TaskId) -> ApiResult<CreateResult<TaskDto>> {
        self.mutation_gate
            .run_data_mutation(async {
                #[cfg(feature = "test-support")]
                self.pause_actor(ActorPausePoint::RetryBeforeWrite).await;
                let delegation = self
                    .mutation_gate
                    .mark_identified_delegated(DurableMutationIdentity::RetryTask(id))?;
                let submission = self.writer.submit_queue_limited_retry(
                    id,
                    self.max_queued_tasks,
                    self.deadline(),
                );
                let completion = match submission {
                    Ok(submission) => submission.completion().await,
                    Err(error) => {
                        delegation.confirm_known();
                        let error = map_submit_error(error);
                        log_failure("task.retry", &error);
                        return Err(error);
                    }
                };
                match &completion.disposition {
                    DurableDisposition::Confirmed(_) => delegation.confirm_exact_resolution(),
                    DurableDisposition::KnownNotApplied {
                        reason: KnownNotAppliedReason::ExactReconciliation,
                        outcome: Some(QueueLimitedRetryTaskOutcome::QueueFull { .. }),
                        ..
                    } => delegation.confirm_exact_resolution(),
                    DurableDisposition::KnownNotApplied { .. } => {
                        delegation.confirm_known();
                    }
                    DurableDisposition::OutcomeUnknown { .. }
                    | DurableDisposition::InvariantConflict { .. } => drop(delegation),
                }
                match completion.disposition {
                    DurableDisposition::Confirmed(QueueLimitedRetryTaskOutcome::Created {
                        task,
                        ..
                    }) => {
                        notify_after_commit(&self.task_manager, task.id).await;
                        tracing::info!(
                            task_id = %task.id,
                            repository_id = %task.repository_id,
                            disposition = "created",
                            "task retry committed"
                        );
                        Ok(CreateResult::Created(task.into()))
                    }
                    DurableDisposition::Confirmed(QueueLimitedRetryTaskOutcome::Existing {
                        task,
                    }) => {
                        tracing::info!(
                            task_id = %task.id,
                            repository_id = %task.repository_id,
                            disposition = "existing",
                            "task retry replayed"
                        );
                        Ok(CreateResult::Existing(task.into()))
                    }
                    DurableDisposition::Confirmed(QueueLimitedRetryTaskOutcome::QueueFull {
                        queued_tasks,
                        max_queued_tasks,
                    })
                    | DurableDisposition::KnownNotApplied {
                        outcome:
                            Some(QueueLimitedRetryTaskOutcome::QueueFull {
                                queued_tasks,
                                max_queued_tasks,
                            }),
                        ..
                    } => Err(map_task_queue_full(queued_tasks, max_queued_tasks)),
                    DurableDisposition::KnownNotApplied {
                        error: Some(error), ..
                    } => Err(map_known_not_applied_error(error)),
                    DurableDisposition::KnownNotApplied { .. } => Err(store_busy()),
                    DurableDisposition::OutcomeUnknown { .. } => Err(app_shutting_down()),
                    DurableDisposition::InvariantConflict { .. } => Err(internal_error()),
                }
            })
            .await
    }

    async fn task_events(
        &self,
        _: &AuthContext,
        id: TaskId,
        after: i64,
    ) -> ApiResult<Vec<TaskEventDto>> {
        if self
            .store
            .task_detail(id)
            .await
            .map_err(map_store_error)?
            .is_none()
        {
            return Err(task_not_found());
        }
        let after = EventCursor::new(after).map_err(map_domain_error)?;
        Ok(self
            .store
            .task_events_after(id, after, usize::MAX)
            .await
            .map_err(map_store_error)?
            .events
            .into_iter()
            .map(Into::into)
            .collect())
    }

    async fn request_quit(&self, _: &AuthContext) -> ApiResult<QuitAcceptance> {
        self.mutation_gate.prepare_quit()?;
        let gate = self.mutation_gate.clone();
        let signal = self.quit_signal.clone();
        Ok(QuitAcceptance::new(move || {
            if gate.begin_quiescing() {
                signal();
            }
        }))
    }
}

#[async_trait::async_trait]
impl SseBackend for ApplicationBackend {
    fn subscribe_live(&self) -> LiveEventStream {
        let mut receiver = self.dispatcher.subscribe();
        let mut service = self.service_state.subscribe();
        Box::pin(async_stream::stream! {
            loop {
                if service.borrow().state == ServiceState::Quiescing {
                    return;
                }
                tokio::select! {
                    event = receiver.recv() => match event {
                        Ok(event) => yield coding_agent_api::LiveEventItem::Event(event.into()),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            yield coding_agent_api::LiveEventItem::Lagged;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                    },
                    changed = service.changed() => {
                        if changed.is_err()
                            || service.borrow_and_update().state == ServiceState::Quiescing
                        {
                            return;
                        }
                    }
                }
            }
        })
    }

    fn subscribe_service_state(&self) -> ServiceStateStream {
        let mut receiver = self.service_state.subscribe();
        Box::pin(async_stream::stream! {
            let current = *receiver.borrow();
            if current.state == ServiceState::Quiescing {
                yield ServiceStateControl::new(
                    service_state(current.state),
                    current.generation,
                );
                return;
            }
            loop {
                if receiver.changed().await.is_err() {
                    return;
                }
                let snapshot = *receiver.borrow_and_update();
                yield ServiceStateControl::new(
                    service_state(snapshot.state),
                    snapshot.generation,
                );
                if snapshot.state == ServiceState::Quiescing {
                    return;
                }
            }
        })
    }

    fn subscribe_scheduler_state(&self) -> SchedulerStateStream {
        let mut receiver = self.scheduler_state_reader.watch();
        let server_started_at = self.server_started_at;
        Box::pin(async_stream::stream! {
            loop {
                if receiver.changed().await.is_err() {
                    yield Err(map_scheduler_api_projection_error(
                        SchedulerApiProjectionError::InconsistentSnapshot,
                    ));
                    return;
                }
                let snapshot = receiver.current();
                yield project_scheduler_snapshot(snapshot.as_ref(), server_started_at)
                    .map(Arc::new)
                    .map_err(map_scheduler_api_projection_error);
            }
        })
    }

    async fn current_service_state(&self) -> ApiResult<ServiceStateControl> {
        let state = self.service_state.current();
        Ok(ServiceStateControl::new(
            service_state(state.state),
            state.generation,
        ))
    }

    async fn current_scheduler_state(&self) -> ApiResult<Arc<coding_agent_api::SchedulerStateDto>> {
        project_scheduler_snapshot(
            self.scheduler_state_reader.current().as_ref(),
            self.server_started_at,
        )
        .map(Arc::new)
        .map_err(map_scheduler_api_projection_error)
    }

    async fn membership_watermark_through(&self, after_cursor: i64) -> ApiResult<i64> {
        let through = EventCursor::new(after_cursor).map_err(map_domain_error)?;
        let watermark = self
            .store
            .membership_watermark_through(through)
            .await
            .map_err(map_store_error)?;
        if watermark > through {
            return Err(internal_error());
        }
        Ok(watermark.get())
    }

    async fn latest_event_id(&self) -> ApiResult<i64> {
        Ok(self
            .store
            .latest_event_id()
            .await
            .map_err(map_store_error)?
            .get())
    }

    async fn events_between(
        &self,
        after: i64,
        through: i64,
        limit: usize,
    ) -> ApiResult<Vec<TaskEventDto>> {
        let after = EventCursor::new(after).map_err(map_domain_error)?;
        Ok(self
            .store
            .events_after(after, limit)
            .await
            .map_err(map_store_error)?
            .events
            .into_iter()
            .filter(|event| event.id.get() <= through)
            .map(Into::into)
            .collect())
    }
}

async fn notify_after_commit(manager: &TaskManagerHandle, task_id: TaskId) {
    if manager.notify_queued(task_id).await.is_err() {
        tracing::warn!(
            task_id = %task_id,
            "queued-task notification was lost after durable commit"
        );
    }
}

fn service_state(state: ServiceState) -> ServiceStateDto {
    match state {
        ServiceState::Ready => ServiceStateDto::Ready,
        ServiceState::StoreDegraded => ServiceStateDto::StoreDegraded,
        ServiceState::Quiescing => ServiceStateDto::Quiescing,
    }
}

fn canonical(path: impl Into<std::path::PathBuf>) -> ApiResult<CanonicalPath> {
    CanonicalPath::try_from_canonical(path).map_err(|_| {
        api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_REPOSITORY_PATH",
            "the repository path is invalid",
            false,
        )
    })
}

fn map_domain_error(error: DomainError) -> ApiError {
    match error {
        DomainError::InvalidPrompt => api_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_PROMPT",
            "prompt must contain between 1 and 50,000 Unicode scalar values",
            false,
        ),
        DomainError::InvalidEventCursor => api_error(
            StatusCode::BAD_REQUEST,
            "INVALID_QUERY",
            "the event cursor must be nonnegative",
            false,
        ),
        _ => internal_error(),
    }
}

fn map_store_error(error: StoreError) -> ApiError {
    if let StoreError::Database(database) = &error {
        let code = database.as_database_error().and_then(|error| error.code());
        if let Some(error) = map_database_code(code.as_deref()) {
            return error;
        }
    }
    match error {
        StoreError::Domain(error) => map_domain_error(error),
        StoreError::IdempotencyConflict => api_error(
            StatusCode::CONFLICT,
            "IDEMPOTENCY_CONFLICT",
            "the client request ID belongs to different task input",
            false,
        ),
        StoreError::TaskNotFound => task_not_found(),
        StoreError::TaskNotRetryable => api_error(
            StatusCode::CONFLICT,
            "TASK_NOT_RETRYABLE",
            "the task is not terminal and cannot be retried",
            false,
        ),
        _ => internal_error(),
    }
}

fn map_bootstrap_join_error(error: BootstrapJoinError) -> ApiError {
    match error {
        BootstrapJoinError::Store(error) => map_store_error(error),
        BootstrapJoinError::SnapshotUnavailable => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            BOOTSTRAP_SNAPSHOT_UNAVAILABLE,
            "a consistent bootstrap snapshot is temporarily unavailable",
            true,
        ),
    }
}

fn map_scheduler_api_projection_error(error: SchedulerApiProjectionError) -> ApiError {
    match error {
        SchedulerApiProjectionError::DatabaseProjectionLimitExceeded => {
            database_projection_limit_exceeded()
        }
        SchedulerApiProjectionError::InconsistentSnapshot => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            BOOTSTRAP_SNAPSHOT_UNAVAILABLE,
            "a consistent bootstrap snapshot is temporarily unavailable",
            true,
        ),
    }
}

fn map_task_queue_full(queued_tasks: u64, max_queued_tasks: NonZeroU32) -> ApiError {
    u32::try_from(queued_tasks).map_or_else(
        |_| database_projection_limit_exceeded(),
        |queued_tasks| ApiError::task_queue_full(queued_tasks, max_queued_tasks.get()),
    )
}

fn database_projection_limit_exceeded() -> ApiError {
    api_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "DATABASE_PROJECTION_LIMIT_EXCEEDED",
        "database state exceeds the supported public projection range",
        false,
    )
}

fn map_writer_error(error: StoreWriterError) -> ApiError {
    match error {
        StoreWriterError::Busy => store_busy(),
        StoreWriterError::DeadlineElapsed => store_busy(),
        StoreWriterError::Store(error) => map_store_error(error),
        StoreWriterError::Closed => app_shutting_down(),
    }
}

fn map_submit_error(error: StoreWriterSubmitError) -> ApiError {
    match error {
        StoreWriterSubmitError::Full => store_busy(),
        StoreWriterSubmitError::Closed => app_shutting_down(),
        StoreWriterSubmitError::InvalidIdentity
        | StoreWriterSubmitError::SequenceGap
        | StoreWriterSubmitError::SequenceReversed => internal_error(),
    }
}

fn map_known_not_applied_error(error: KnownNotAppliedError) -> ApiError {
    match error {
        KnownNotAppliedError::Domain(error) => map_domain_error(error),
        KnownNotAppliedError::IdempotencyConflict => {
            map_store_error(StoreError::IdempotencyConflict)
        }
        KnownNotAppliedError::TaskNotFound => map_store_error(StoreError::TaskNotFound),
        KnownNotAppliedError::TaskNotRetryable => map_store_error(StoreError::TaskNotRetryable),
        _ => internal_error(),
    }
}

fn map_database_code(code: Option<&str>) -> Option<ApiError> {
    code.filter(|code| sqlite_code_is_retryable(code))
        .map(|_| store_busy())
}

fn map_manager_error(error: TaskManagerError) -> ApiError {
    match error {
        TaskManagerError::Closed | TaskManagerError::DeadlineElapsed => app_shutting_down(),
        TaskManagerError::Store(error) => map_store_error(error),
        TaskManagerError::StoreWriter(error) => map_writer_error(error),
        TaskManagerError::TaskNotFound => task_not_found(),
        TaskManagerError::TaskNotCancellable { .. } => api_error(
            StatusCode::CONFLICT,
            "TASK_NOT_CANCELLABLE",
            "the task cannot be cancelled in its current state",
            false,
        ),
        TaskManagerError::StopAlreadyRequested { .. } => ApiError::task_stop_already_requested(),
        TaskManagerError::Frozen | TaskManagerError::StoreDegraded => store_degraded(),
        TaskManagerError::Invariant(_) => internal_error(),
    }
}

fn map_discovery_error(error: RepositoryDiscoveryError) -> ApiError {
    api_error(
        StatusCode::UNPROCESSABLE_ENTITY,
        error.code(),
        "the selected repository could not be validated",
        false,
    )
}

fn map_picker_error(error: PickerError) -> ApiError {
    match error {
        PickerError::AlreadyOpen => api_error(
            StatusCode::CONFLICT,
            error.code(),
            "a repository picker is already open",
            false,
        ),
        PickerError::Unavailable => picker_unavailable(),
        PickerError::MainThreadRequired => internal_error(),
    }
}

fn picker_unavailable() -> ApiError {
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "PICKER_UNAVAILABLE",
        "the repository picker is unavailable",
        true,
    )
}

fn task_not_found() -> ApiError {
    api_error(
        StatusCode::NOT_FOUND,
        "TASK_NOT_FOUND",
        "the task was not found",
        false,
    )
}

fn store_degraded() -> ApiError {
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "STORE_DEGRADED",
        "the local store is degraded; data mutations are temporarily disabled",
        true,
    )
}

fn store_busy() -> ApiError {
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "STORE_BUSY",
        "the local store is busy; retry the request",
        true,
    )
}

fn app_shutting_down() -> ApiError {
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "APP_SHUTTING_DOWN",
        "the application is shutting down",
        true,
    )
}

fn app_starting() -> ApiError {
    api_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "APP_STARTING",
        "the application is starting",
        true,
    )
}

fn internal_error() -> ApiError {
    api_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "INTERNAL_ERROR",
        "the request could not be completed",
        false,
    )
}

fn api_error(status: StatusCode, code: &str, message: &str, retryable: bool) -> ApiError {
    ApiError {
        status,
        code: code.to_owned(),
        message: message.to_owned(),
        retryable,
        details: BTreeMap::new(),
    }
}

fn log_failure(operation: &'static str, error: &ApiError) {
    tracing::info!(operation, error_code = %error.code, "application mutation rejected");
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::Arc;
    #[cfg(feature = "test-support")]
    use std::{
        ffi::OsString,
        io,
        num::NonZeroU32,
        path::{Path, PathBuf},
        sync::Mutex,
    };

    use axum::response::{Html, IntoResponse, Response};
    use axum::routing::get;
    use axum::{Router, body::Body};
    use http::{HeaderValue, Method};
    use http_body_util::BodyExt as _;
    use serde_json::{Value, json};
    use tokio::sync::{Notify, oneshot};
    use tower::ServiceExt as _;
    use uuid::Uuid;

    #[cfg(feature = "test-support")]
    use crate::runner_factory::ValidatedStartupInputs;
    #[cfg(feature = "test-support")]
    use crate::{
        CommandRunner, FixedStartupRunnerFactory, LegacyV2Seed, PlatformPaths,
        PreActorStartupRunnerContext, ProcessRunnerMode, ProcessStorageSample, ProcessTestConfig,
        ProcessTestEnvironment, RepositoryControlError, RunContext, RunnerEventSink, RunnerOutcome,
        StartupDependencies, StartupRunnerFactory, StoreWriterFaultPoint, StoreWriterFaultSpec,
        StoreWriterOperationKind, StoreWriterTestController, TaskRunner, VirtualReleaseSignal,
        VirtualReleaseTarget, load_runtime_config,
    };
    use crate::{SecuritySeed, StartupPhaseController, SystemSecurityClock, SystemWallClock};
    #[cfg(feature = "test-support")]
    use coding_agent_runtime::ProcessLivenessDirectory;

    use super::*;

    struct RuntimeFixture {
        router: Router,
        phase: StartupPhaseController,
        security: SecurityManager,
        instance_id: Uuid,
        host: String,
        launcher_secret: String,
    }

    #[tokio::test]
    async fn forced_shutdown_cancels_a_pre_handoff_mutation_and_observes_the_guard_drop() {
        let state = ServiceStateController::new(ServiceState::Ready);
        let gate = MutationGate::new(state);
        let task_gate = gate.clone();
        let (entered, entered_receiver) = oneshot::channel();
        let mutation = tokio::spawn(async move {
            task_gate
                .run_data_mutation(async move {
                    let _ = entered.send(());
                    pending::<ApiResult<()>>().await
                })
                .await
        });
        entered_receiver.await.expect("mutation enters the gate");

        assert!(gate.begin_quiescing());
        assert!(gate.force_cancel_in_flight());
        let error = mutation
            .await
            .expect("join cancelled mutation")
            .expect_err("forced shutdown rejects the request");
        assert_eq!(error.code, "APP_SHUTTING_DOWN");
        assert_eq!(
            gate.drain_until(Instant::now() + Duration::from_secs(1))
                .await,
            MutationDrainOutcome::Drained
        );
    }

    #[tokio::test]
    async fn forced_shutdown_latches_an_abandoned_delegated_mutation_as_unproven() {
        let state = ServiceStateController::new(ServiceState::Ready);
        let gate = MutationGate::new(state);
        let task_gate = gate.clone();
        let operation_gate = gate.clone();
        let (entered, entered_receiver) = oneshot::channel();
        let mutation = tokio::spawn(async move {
            task_gate
                .run_data_mutation(async move {
                    let _delegation = operation_gate.mark_delegated()?;
                    let _ = entered.send(());
                    pending::<ApiResult<()>>().await
                })
                .await
        });
        entered_receiver
            .await
            .expect("delegated mutation enters the gate");

        assert!(gate.begin_quiescing());
        assert!(gate.force_cancel_in_flight());
        let error = mutation
            .await
            .expect("join cancelled delegated mutation")
            .expect_err("forced shutdown rejects the delegated request");
        assert_eq!(error.code, "APP_SHUTTING_DOWN");
        assert_eq!(
            gate.drain_until(Instant::now() + Duration::from_secs(1))
                .await,
            MutationDrainOutcome::Unproven
        );
    }

    #[tokio::test]
    async fn a_non_cooperative_guard_returns_unproven_at_the_drain_deadline() {
        let state = ServiceStateController::new(ServiceState::Ready);
        let gate = MutationGate::new(state);
        let guard = gate.enter_data_mutation().expect("enter mutation gate");
        assert!(gate.begin_quiescing());
        assert!(gate.force_cancel_in_flight());

        assert_eq!(
            gate.drain_until(Instant::now() + Duration::from_millis(10))
                .await,
            MutationDrainOutcome::Unproven
        );
        drop(guard);
    }

    impl RuntimeFixture {
        fn new() -> Self {
            let instance_id = Uuid::new_v4();
            let host = "127.0.0.1:43121".to_owned();
            let seed = SecuritySeed::generate().expect("generate runtime secrets");
            let launcher_secret = seed.launcher_secret().as_str().to_owned();
            let security = SecurityManager::from_seed(
                seed,
                format!("http://{host}"),
                Arc::new(SystemSecurityClock),
            )
            .expect("construct runtime security");
            let phase = StartupPhaseController::new();
            let api_router = Router::new()
                .route("/api/ping", get(|| async { StatusCode::NO_CONTENT }))
                .route(
                    "/page",
                    get(|| async {
                        let mut response = Html("<!doctype html>").into_response();
                        response.headers_mut().insert(
                            http::header::ACCESS_CONTROL_ALLOW_ORIGIN,
                            HeaderValue::from_static("*"),
                        );
                        response
                    }),
                );
            let router = build_runtime_router(
                api_router,
                instance_id,
                phase.clone(),
                security.clone(),
                Arc::new(SystemWallClock),
            );
            Self {
                router,
                phase,
                security,
                instance_id,
                host,
                launcher_secret,
            }
        }

        fn request(&self, method: Method, path: &str) -> http::Request<Body> {
            http::Request::builder()
                .method(method)
                .uri(path)
                .header(http::header::HOST, &self.host)
                .header("x-launcher-secret", &self.launcher_secret)
                .body(Body::empty())
                .expect("build runtime request")
        }
    }

    async fn json_body(response: Response) -> Value {
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("collect response body")
            .to_bytes();
        serde_json::from_slice(&bytes).expect("decode response JSON")
    }

    async fn error_code(response: Response) -> String {
        json_body(response).await["code"]
            .as_str()
            .expect("error code is a string")
            .to_owned()
    }

    #[test]
    fn sqlite_busy_codes_map_to_the_retryable_store_busy_contract() {
        for code in ["5", "6", "261", "262"] {
            let error = map_database_code(Some(code)).expect("busy code must map");
            assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE, "code={code}");
            assert_eq!(error.code, "STORE_BUSY", "code={code}");
            assert!(error.retryable, "code={code}");
        }

        for code in [None, Some("4"), Some("7"), Some("260"), Some("not-a-code")] {
            assert!(map_database_code(code).is_none(), "code={code:?}");
        }

        let fallback = map_store_error(StoreError::InvariantViolation("test fallback"));
        assert_eq!(fallback.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(fallback.code, "INTERNAL_ERROR");
        assert!(!fallback.retryable);

        let pool_timeout = map_store_error(StoreError::Database(sqlx::Error::PoolTimedOut));
        assert_eq!(pool_timeout.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(pool_timeout.code, "INTERNAL_ERROR");
        assert!(!pool_timeout.retryable);
    }

    #[cfg(feature = "test-support")]
    struct RegistrationDiscoveryRunner {
        git_root: PathBuf,
        manifest: PathBuf,
        calls: Mutex<u8>,
        delay: Duration,
    }

    #[cfg(feature = "test-support")]
    #[derive(Default)]
    struct RegistrationTaskRunner {
        started: Mutex<Vec<TaskId>>,
        changed: Notify,
    }

    #[cfg(feature = "test-support")]
    impl RegistrationTaskRunner {
        async fn wait_for(&self, task_id: TaskId) {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if self.started.lock().unwrap().contains(&task_id) {
                        return;
                    }
                    self.changed.notified().await;
                }
            })
            .await
            .expect("dynamically registered task reaches the runner");
        }
    }

    #[cfg(feature = "test-support")]
    #[async_trait::async_trait]
    impl TaskRunner for RegistrationTaskRunner {
        async fn run(&self, mut context: RunContext, _sink: RunnerEventSink) -> RunnerOutcome {
            context.complete_preparation_for_test().await;
            self.started.lock().unwrap().push(context.task.id);
            self.changed.notify_one();
            RunnerOutcome::Cancelled
        }
    }

    #[cfg(feature = "test-support")]
    #[async_trait::async_trait]
    impl CommandRunner for RegistrationDiscoveryRunner {
        async fn run(
            &self,
            _program: &str,
            _args: &[OsString],
            _current_dir: &Path,
            _deadline: Instant,
        ) -> io::Result<Vec<u8>> {
            let output = {
                let mut calls = self.calls.lock().expect("lock discovery calls");
                let output = match *calls % 2 {
                    0 => self.git_root.clone(),
                    1 => self.manifest.clone(),
                    _ => unreachable!("modulo two has only two values"),
                };
                *calls += 1;
                output
            };
            tokio::time::sleep(self.delay).await;
            Ok(format!("{}\n", output.display()).into_bytes())
        }
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn forced_shutdown_cancels_create_handler_before_store_writer_handoff() {
        let temporary = tempfile::tempdir().expect("create forced-shutdown fixture");
        let data_dir = temporary.path().join("data");
        let runtime_dir = temporary.path().join("runtime");
        let signal_dir = runtime_dir.join("signals");
        std::fs::create_dir_all(&data_dir).expect("create test data directory");
        std::fs::create_dir_all(&signal_dir).expect("create test signal directory");
        let signal_dir = signal_dir
            .canonicalize()
            .expect("canonicalize test signal directory");
        let release_signal = signal_dir.join("create-before-write.release");
        let reached_signal = signal_dir.join("create-before-write.release.reached");
        let scenario = data_dir.join("create-before-write.json");
        std::fs::write(
            &scenario,
            serde_json::to_vec(&ProcessTestConfig {
                runner_mode: ProcessRunnerMode::ScriptedFake {},
                runtime_config: None,
                fake_scenarios: Vec::new(),
                storage_samples: vec![ProcessStorageSample::Native],
                store_writer_faults: Vec::new(),
                actor_pauses: vec![ActorPausePoint::CreateBeforeWrite],
                virtual_release_signals: vec![VirtualReleaseSignal {
                    name: "create-before-write".to_owned(),
                    path: release_signal,
                    target: VirtualReleaseTarget::ActorCreateBeforeWrite,
                }],
                legacy_v2_seed: LegacyV2Seed::None,
                marker_write_failure: false,
            })
            .expect("serialize process-test scenario"),
        )
        .expect("write process-test scenario");
        let environment = ProcessTestEnvironment::load(&data_dir, &runtime_dir, &scenario)
            .expect("load process-test environment");
        let dependencies = environment
            .apply(StartupDependencies::production(None))
            .expect("apply process-test environment");
        let actor_pauses = dependencies
            .process_test_support
            .as_ref()
            .expect("process-test runtime is installed")
            .actor_pauses
            .clone();

        let paths = PlatformPaths::new(&data_dir, &runtime_dir);
        let repository_root = data_dir.join("repository");
        std::fs::create_dir_all(&repository_root).expect("create repository directory");
        let repository_root = repository_root
            .canonicalize()
            .expect("canonicalize repository directory");
        let store = Store::open(&paths.database_path)
            .await
            .expect("open forced-shutdown store");
        store
            .migrate()
            .await
            .expect("migrate forced-shutdown store");
        let repository = match store
            .register_repository(NewRepository {
                selected_path: canonical(repository_root.clone()).unwrap(),
                display_name: "forced shutdown repository".to_owned(),
                git_root: canonical(repository_root.clone()).unwrap(),
                cargo_workspace_root: canonical(repository_root).unwrap(),
            })
            .await
            .expect("register forced-shutdown repository")
        {
            RegisterRepositoryOutcome::Created(repository)
            | RegisterRepositoryOutcome::Existing(repository) => repository,
        };
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
            .await
            .expect("spawn forced-shutdown dispatcher");
        let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 16);
        let state = ServiceStateController::new(ServiceState::Ready);
        let manager = TaskManagerHandle::spawn(
            store.clone(),
            writer.clone(),
            dispatcher.clone(),
            state.clone(),
            Arc::new(RegistrationTaskRunner::default()),
            crate::task_manager::test_task_manager_launch_resources(1, 1),
            16,
        );
        let mutation_gate = MutationGate::new(state.clone());
        let backend = Arc::new(
            ApplicationBackend::new_without_repository_runtime_for_test(
                store.clone(),
                writer,
                dispatcher,
                manager,
                RepositoryDiscovery::new_without_commands_for_test(&runtime_dir),
                None,
                SecurityManager::from_seed(
                    SecuritySeed::generate().expect("generate forced-shutdown security seed"),
                    "http://127.0.0.1:43124",
                    Arc::new(SystemSecurityClock),
                )
                .expect("construct forced-shutdown security"),
                state,
                mutation_gate.clone(),
                UtcTimestamp::new(time::OffsetDateTime::now_utc())
                    .expect("construct forced-shutdown timestamp"),
                1,
                NonZeroU32::new(16).unwrap(),
                Duration::from_secs(1),
                Arc::new(|| {}),
            )
            .with_process_test_pauses(actor_pauses),
        );
        let request_backend = backend.clone();
        let request = tokio::spawn(async move {
            request_backend
                .create_task(
                    &AuthContext {
                        session_id: "already-authorized".to_owned(),
                    },
                    CreateTaskRequest {
                        client_request_id: coding_agent_domain::ClientRequestId::new(),
                        repository_id: repository.id,
                        prompt: "cancel before durable handoff".to_owned(),
                    },
                )
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while !reached_signal.is_file() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("create handler reaches its pre-handoff pause");
        assert!(mutation_gate.begin_quiescing());
        assert!(mutation_gate.force_cancel_in_flight());

        let error = tokio::time::timeout(Duration::from_secs(1), request)
            .await
            .expect("forced create handler returns before its deadline")
            .expect("join forced create handler")
            .expect_err("forced shutdown rejects the create request");
        assert_eq!(error.code, "APP_SHUTTING_DOWN");
        assert!(
            store
                .bootstrap_snapshot()
                .await
                .expect("read tasks after forced cancellation")
                .tasks
                .is_empty(),
            "a pre-handoff cancellation must not create a durable task"
        );
        assert_eq!(
            mutation_gate
                .drain_until(Instant::now() + Duration::from_secs(1))
                .await,
            MutationDrainOutcome::Drained
        );
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn repository_registration_deadline_starts_before_discovery() {
        let temporary = tempfile::tempdir().expect("create deadline fixture");
        let paths = PlatformPaths::new(
            temporary.path().join("data"),
            temporary.path().join("runtime"),
        );
        std::fs::create_dir_all(&paths.data_dir).unwrap();
        std::fs::create_dir_all(&paths.runtime_dir).unwrap();
        let repository_root = temporary.path().join("repository");
        std::fs::create_dir_all(repository_root.join(".git")).unwrap();
        std::fs::write(repository_root.join("Cargo.toml"), b"[workspace]\n").unwrap();
        let repository_root = repository_root.canonicalize().unwrap();
        let manifest = repository_root.join("Cargo.toml").canonicalize().unwrap();
        let store = Store::open(&paths.database_path).await.unwrap();
        store.migrate().await.unwrap();
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
            .await
            .unwrap();
        let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 16);
        let state = ServiceStateController::new(ServiceState::Ready);
        let manager = TaskManagerHandle::spawn(
            store.clone(),
            writer.clone(),
            dispatcher.clone(),
            state.clone(),
            Arc::new(RegistrationTaskRunner::default()),
            crate::task_manager::test_task_manager_launch_resources(1, 1),
            16,
        );
        let seed = SecuritySeed::generate().unwrap();
        let backend = ApplicationBackend::new_without_repository_runtime_for_test(
            store.clone(),
            writer,
            dispatcher,
            manager,
            RepositoryDiscovery::with_runner(
                paths.runtime_dir,
                Arc::new(RegistrationDiscoveryRunner {
                    git_root: repository_root.clone(),
                    manifest,
                    calls: Mutex::new(0),
                    delay: Duration::from_millis(30),
                }),
            ),
            None,
            SecurityManager::from_seed(
                seed,
                "http://127.0.0.1:43123",
                Arc::new(SystemSecurityClock),
            )
            .unwrap(),
            state.clone(),
            MutationGate::new(state),
            UtcTimestamp::new(time::OffsetDateTime::now_utc()).unwrap(),
            1,
            NonZeroU32::new(16).unwrap(),
            Duration::from_millis(10),
            Arc::new(|| {}),
        );

        let error = backend
            .add_repository(
                &AuthContext {
                    session_id: "already-authorized".to_owned(),
                },
                AddRepositoryRequest {
                    path: repository_root,
                },
            )
            .await
            .expect_err("discovery consumes the one end-to-end write budget");

        assert_eq!(error.code, "REPOSITORY_COMMAND_FAILED");
        assert!(
            store.list_repositories().await.unwrap().is_empty(),
            "an expired discovery deadline reaches no durable write"
        );
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn commit_before_reply_does_not_guess_runtime_attachment_and_existing_retry_converges() {
        let temporary = tempfile::tempdir().expect("create registration fixture");
        let paths = PlatformPaths::new(
            temporary.path().join("data"),
            temporary.path().join("runtime"),
        );
        std::fs::create_dir_all(&paths.data_dir).unwrap();
        std::fs::create_dir_all(&paths.runtime_dir).unwrap();
        let repository_root = temporary.path().join("repository");
        std::fs::create_dir_all(repository_root.join(".git")).unwrap();
        std::fs::write(repository_root.join("Cargo.toml"), b"[workspace]\n").unwrap();
        let repository_root = repository_root.canonicalize().unwrap();
        let manifest = repository_root.join("Cargo.toml").canonicalize().unwrap();

        let store = Store::open(&paths.database_path).await.unwrap();
        store.migrate().await.unwrap();
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
            .await
            .unwrap();
        let controller = Arc::new(
            StoreWriterTestController::try_new([StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
                operation: Some(StoreWriterOperationKind::RegisterRepository),
                count: 1,
            }])
            .unwrap(),
        );
        let writer = StoreWriterHandle::spawn_with_test_controller(
            store.clone(),
            Arc::new(dispatcher.clone()),
            16,
            controller,
        );
        let runtime_config = load_runtime_config(&paths).unwrap();
        let instance_id = Uuid::new_v4();
        let process_scope = ProcessLivenessDirectory::open(&paths.runtime_dir)
            .unwrap()
            .instance_scope(*instance_id.as_bytes())
            .unwrap();
        let task_runner = Arc::new(RegistrationTaskRunner::default());
        let factory =
            FixedStartupRunnerFactory::new(task_runner.clone(), NonZeroU32::new(2).unwrap());
        let context = PreActorStartupRunnerContext::new(
            paths,
            store.clone(),
            Arc::new(SystemWallClock),
            ValidatedStartupInputs::new(runtime_config, Arc::new(())),
            instance_id,
            process_scope,
        );
        let prepared = factory.prepare_before_actors(&context).await.unwrap();
        let selection = factory
            .create(context.into_live(writer.clone(), prepared))
            .await
            .unwrap();
        let repository_control = selection.launch_resources().repository_control();
        let registrar = selection
            .repository_registrar()
            .expect("fixed factory installs registrar");
        let storage_ack_pause = registrar.pause_next_storage_ack_for_test();
        let state = ServiceStateController::new(ServiceState::Ready);
        let manager = TaskManagerHandle::spawn(
            store.clone(),
            writer.clone(),
            dispatcher.clone(),
            state.clone(),
            selection.runner(),
            selection.launch_resources(),
            16,
        );
        let seed = SecuritySeed::generate().unwrap();
        let backend = ApplicationBackend::new_with_repository_runtime(
            store.clone(),
            writer,
            dispatcher,
            manager,
            registrar,
            RepositoryDiscovery::with_runner(
                temporary.path().join("runtime"),
                Arc::new(RegistrationDiscoveryRunner {
                    git_root: repository_root.clone(),
                    manifest,
                    calls: Mutex::new(0),
                    delay: Duration::ZERO,
                }),
            ),
            None,
            SecurityManager::from_seed(
                seed,
                "http://127.0.0.1:43122",
                Arc::new(SystemSecurityClock),
            )
            .unwrap(),
            state.clone(),
            MutationGate::new(state),
            UtcTimestamp::new(time::OffsetDateTime::now_utc()).unwrap(),
            2,
            NonZeroU32::new(16).unwrap(),
            Duration::from_millis(500),
            Arc::new(|| {}),
        );
        let auth = AuthContext {
            session_id: "already-authorized".to_owned(),
        };

        let first = backend
            .add_repository(
                &auth,
                AddRepositoryRequest {
                    path: repository_root.clone(),
                },
            )
            .await
            .expect_err("first reply is deliberately lost after commit");
        assert_eq!(first.code, "APP_SHUTTING_DOWN");
        let durable = store.list_repositories().await.unwrap();
        assert_eq!(durable.len(), 1, "the durable row remains committed");
        assert_eq!(
            repository_control.coordination_key(durable[0].id),
            Err(RepositoryControlError::UnknownRepository),
            "an ambiguous first reply must not guess and attach by request path"
        );

        let timed_out = backend
            .add_repository(
                &auth,
                AddRepositoryRequest {
                    path: repository_root.clone(),
                },
            )
            .await
            .expect_err("monitor applies but deliberately withholds its acknowledgement");
        assert_eq!(timed_out.code, "INTERNAL_ERROR");
        assert!(
            repository_control.coordination_key(durable[0].id).is_ok(),
            "the timed-out request retains monotonic coordinator progress"
        );
        tokio::time::timeout(
            Duration::from_millis(50),
            backend.mutation_gate().wait_for_idle(),
        )
        .await
        .expect("request timeout releases its mutation guard");

        storage_ack_pause.release();
        let converged = backend
            .add_repository(
                &auth,
                AddRepositoryRequest {
                    path: repository_root.clone(),
                },
            )
            .await
            .expect("Existing retry converges the apply-before-reply timeout");
        assert!(matches!(converged, CreateResult::Existing(_)));

        let created = backend
            .create_task(
                &auth,
                CreateTaskRequest {
                    client_request_id: coding_agent_domain::ClientRequestId::new(),
                    repository_id: durable[0].id,
                    prompt: "dynamic repository starts".to_owned(),
                },
            )
            .await
            .expect("queue task for dynamically registered repository");
        let task_id = match created {
            CreateResult::Created(task) | CreateResult::Existing(task) => task.id,
        };
        task_runner
            .wait_for(task_id.to_string().parse().unwrap())
            .await;
    }

    #[tokio::test]
    async fn starting_ready_probe_returns_only_instance_and_phase() {
        let fixture = RuntimeFixture::new();
        let response = fixture
            .router
            .clone()
            .oneshot(fixture.request(Method::GET, LOCAL_READY_PATH))
            .await
            .expect("call ready probe");

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key(REQUEST_ID_HEADER));
        assert_eq!(
            response.headers().get(http::header::CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store"))
        );
        assert!(
            !response
                .headers()
                .contains_key(http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
        );
        let body = json_body(response).await;
        assert_eq!(body.as_object().expect("ready object").len(), 2);
        assert_eq!(body["instance_id"], json!(fixture.instance_id));
        assert_eq!(body["state"], "starting");
    }

    #[tokio::test]
    async fn outer_response_policy_covers_success_errors_html_and_api_without_cors() {
        const EXPECTED_CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'";

        fn assert_common_policy(response: &Response) {
            assert_eq!(
                response.headers().get("x-content-type-options"),
                Some(&HeaderValue::from_static("nosniff"))
            );
            assert_eq!(
                response.headers().get("referrer-policy"),
                Some(&HeaderValue::from_static("no-referrer"))
            );
            assert_eq!(
                response.headers().get("content-security-policy"),
                Some(&HeaderValue::from_static(EXPECTED_CSP))
            );
            assert!(
                !response
                    .headers()
                    .contains_key(http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
            );
        }

        let fixture = RuntimeFixture::new();
        assert!(fixture.phase.mark_ready());

        let api = fixture
            .router
            .clone()
            .oneshot(fixture.request(Method::GET, "/api/ping"))
            .await
            .expect("call API");
        assert_common_policy(&api);
        assert_eq!(
            api.headers().get(http::header::CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store"))
        );

        let missing_api = fixture
            .router
            .clone()
            .oneshot(fixture.request(Method::GET, "/api/not-a-route"))
            .await
            .expect("call missing API route");
        assert_eq!(missing_api.status(), StatusCode::NOT_FOUND);
        assert_common_policy(&missing_api);
        assert_eq!(
            missing_api.headers().get(http::header::CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store"))
        );

        let html = fixture
            .router
            .clone()
            .oneshot(fixture.request(Method::GET, "/page"))
            .await
            .expect("call HTML route");
        assert_common_policy(&html);
        assert_eq!(
            html.headers().get(http::header::CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store"))
        );

        let wrong_host = http::Request::builder()
            .uri("/api/ping")
            .header(http::header::HOST, "localhost:43121")
            .body(Body::empty())
            .expect("build wrong-host request");
        let error = fixture
            .router
            .clone()
            .oneshot(wrong_host)
            .await
            .expect("call API with wrong Host");
        assert_eq!(error.status(), StatusCode::FORBIDDEN);
        assert_common_policy(&error);
        assert_eq!(
            error.headers().get(http::header::CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store"))
        );
    }

    #[tokio::test]
    async fn starting_blocks_reopen_and_api_after_exact_host_validation() {
        let fixture = RuntimeFixture::new();
        let reopen = fixture
            .router
            .clone()
            .oneshot(fixture.request(Method::POST, LOCAL_REOPEN_PATH))
            .await
            .expect("call reopen while starting");
        assert_eq!(reopen.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error_code(reopen).await, "APP_STARTING");

        let api = fixture
            .router
            .clone()
            .oneshot(fixture.request(Method::GET, "/api/ping"))
            .await
            .expect("call API while starting");
        assert_eq!(api.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error_code(api).await, "APP_STARTING");

        let wrong_host = http::Request::builder()
            .uri("/api/ping")
            .header(http::header::HOST, "localhost:43121")
            .body(Body::empty())
            .expect("build wrong-host request");
        let response = fixture
            .router
            .clone()
            .oneshot(wrong_host)
            .await
            .expect("call API with wrong Host");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(error_code(response).await, "SECURITY_INVALID_HOST");
    }

    #[tokio::test]
    async fn launcher_endpoints_reject_wrong_or_duplicated_security_headers() {
        let fixture = RuntimeFixture::new();
        let wrong_secret = http::Request::builder()
            .uri(LOCAL_READY_PATH)
            .header(http::header::HOST, &fixture.host)
            .header("x-launcher-secret", "not-a-valid-secret")
            .body(Body::empty())
            .expect("build wrong-secret request");
        let response = fixture
            .router
            .clone()
            .oneshot(wrong_secret)
            .await
            .expect("call with wrong launcher secret");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            error_code(response).await,
            "SECURITY_INVALID_LAUNCHER_SECRET"
        );

        let mut duplicated = fixture.request(Method::GET, LOCAL_READY_PATH);
        duplicated.headers_mut().append(
            "x-launcher-secret",
            HeaderValue::from_str(&fixture.launcher_secret).expect("launcher header value"),
        );
        let response = fixture
            .router
            .clone()
            .oneshot(duplicated)
            .await
            .expect("call with duplicate launcher secret");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error_code(response).await, "SECURITY_DUPLICATE_HEADER");
    }

    #[tokio::test]
    async fn ready_reopen_issues_a_fresh_two_minute_fragment_url_and_opens_api_gate() {
        let fixture = RuntimeFixture::new();
        assert!(fixture.phase.mark_ready());
        let requested_at = time::OffsetDateTime::now_utc();

        let response = fixture
            .router
            .clone()
            .oneshot(fixture.request(Method::POST, LOCAL_REOPEN_PATH))
            .await
            .expect("call ready reopen");
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body.as_object().expect("reopen object").len(), 2);
        let url = body["url"].as_str().expect("fragment URL");
        let expected_prefix = format!("{}/#token=", fixture.security.public_origin());
        let token = url
            .strip_prefix(&expected_prefix)
            .expect("URL uses exact public origin and token fragment");
        assert!(!token.is_empty());
        let expires_at = UtcTimestamp::parse_rfc3339(
            body["expires_at"]
                .as_str()
                .expect("RFC3339 expiration string"),
        )
        .expect("parse RFC3339 expiration");
        let observed_at = time::OffsetDateTime::now_utc();
        assert!(
            expires_at.as_offset_date_time()
                >= requested_at.saturating_add(time::Duration::seconds(119))
        );
        assert!(
            expires_at.as_offset_date_time()
                <= observed_at.saturating_add(time::Duration::seconds(121))
        );

        let api = fixture
            .router
            .clone()
            .oneshot(fixture.request(Method::GET, "/api/ping"))
            .await
            .expect("call API after ready");
        assert_eq!(api.status(), StatusCode::NO_CONTENT);
    }
}
