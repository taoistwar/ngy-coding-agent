use std::str::FromStr;
use std::sync::{Arc, Weak};

use coding_agent_app::{
    DeliveryAcceptRequest, DeliveryManagerHandle, DeliveryManagerLiveDependencies,
    DeliveryMergeAcceptance, DeliveryMergeAcceptanceOutcome, DeliveryPreflightOutcome,
    DeliveryPreflightRequest, DeliveryPreflightState, EventDispatcherHandle,
    RepositoryControlCoordinator, RepositoryControlState, SchedulerConcurrencyLimits, ServiceState,
    ServiceStateController, StoreWriterHandle, StoreWriterTestController, TaskManagerHandle,
    TaskManagerLaunchResources,
};
use coding_agent_domain::{
    CanonicalPath, ClientRequestId, Task, TaskEventPayload, TaskId, TaskStatus,
};
use coding_agent_store::{
    AcceptMergeCommandRequest, AttemptArtifactIdentity, DeliveryOperationId, DeliverySourceRecord,
    DeliverySourceState, FinalizeReviewedTaskOutcome, GitBranchRef, GitCommitOid,
    MergeOperationRecord, MergeOperationState, PreflightCommandRequest, ReserveAttemptArtifact,
    TaskTransition, TransitionOutcome,
};
use tokio::time::{Duration, timeout};

use crate::support;

use super::live::{ControlledProcessProofs, LiveRuntimeControl, assert_merge_snapshot};
use super::preflight::PreflightRuntime;
use super::teardown::{
    TeardownFailures, close_dispatcher, stop_delivery_manager, stop_store_writer,
    stop_task_manager, tracked_dispatcher_wake, tracked_process_proofs,
};
use super::{BASE_COMMIT, TARGET_HEAD};

pub struct DeliveryMergeFixture {
    pub base: support::StoreFixture,
    pub coordinator: Arc<RepositoryControlCoordinator>,
    pub live_runtime: Arc<LiveRuntimeControl>,
    pub process_proofs: Arc<ControlledProcessProofs>,
    dispatcher: EventDispatcherHandle,
    writer: StoreWriterHandle,
    writer_actor_lifetime: Weak<()>,
    task_manager: TaskManagerHandle,
    task_manager_actor_lifetime: Weak<support::ControlledRunner>,
    preflight_runtime: Arc<PreflightRuntime>,
    manager: Option<DeliveryManagerHandle>,
    manager_actor_lifetimes: Vec<Weak<()>>,
}

pub struct PreparedAccept {
    pub task: Task,
    pub operation_id: DeliveryOperationId,
    pub command: AcceptMergeCommandRequest,
}

impl PreparedAccept {
    pub fn request(&self) -> DeliveryAcceptRequest {
        DeliveryAcceptRequest::new(self.command.clone())
    }
}

impl DeliveryMergeFixture {
    pub async fn new(writer_controller: Option<Arc<StoreWriterTestController>>) -> Self {
        let mut base = support::store_fixture().await;
        base.arm_delivery_root_for_explicit_close();
        let (coordinator, _) = support::repository_control_fixture(&base.store).await;
        let dispatcher = EventDispatcherHandle::spawn(base.store.clone(), 128)
            .await
            .expect("spawn delivery-merge dispatcher");
        let (writer_wake, writer_actor_lifetime) = tracked_dispatcher_wake(dispatcher.clone());
        let writer = match writer_controller.as_ref() {
            Some(controller) => StoreWriterHandle::spawn_with_test_controller(
                base.store.clone(),
                writer_wake,
                32,
                controller.clone(),
            ),
            None => StoreWriterHandle::spawn(base.store.clone(), writer_wake, 32),
        };
        let task_manager_state = ServiceStateController::new(ServiceState::StoreDegraded);
        let launch_resources = TaskManagerLaunchResources::new_for_test(
            SchedulerConcurrencyLimits::try_new(4, 4).expect("valid fixture limits"),
            coordinator.clone(),
            base.instance_process_scope(),
        );
        let runner = Arc::new(support::ControlledRunner::default());
        let task_manager_actor_lifetime = Arc::downgrade(&runner);
        let task_manager = TaskManagerHandle::spawn(
            base.store.clone(),
            writer.clone(),
            dispatcher.clone(),
            task_manager_state,
            runner,
            launch_resources,
            32,
        );
        let preflight_runtime = PreflightRuntime::new(coordinator.clone());
        let live_runtime = LiveRuntimeControl::new(coordinator.clone());
        let process_proofs = Arc::new(ControlledProcessProofs::default());
        let mut fixture = Self {
            base,
            coordinator,
            live_runtime,
            process_proofs,
            dispatcher,
            writer,
            writer_actor_lifetime,
            task_manager,
            task_manager_actor_lifetime,
            preflight_runtime,
            manager: None,
            manager_actor_lifetimes: Vec::new(),
        };
        let manager = fixture.spawn_manager();
        fixture.manager = Some(manager);
        fixture
    }

    pub fn manager(&self) -> &DeliveryManagerHandle {
        self.manager.as_ref().expect("fixture manager is running")
    }

    pub async fn restart_manager(&mut self) {
        drop(self.manager.take());
        tokio::task::yield_now().await;
        let manager = self.spawn_manager();
        self.manager = Some(manager);
    }

    // This shared fixture module is compiled into integration-test binaries
    // whose individual scenarios do not all exercise cold-start recovery.
    #[allow(dead_code)]
    pub async fn restart_manager_with_fresh_repository_control(&mut self) {
        drop(self.manager.take());
        tokio::task::yield_now().await;
        let (coordinator, _) = support::repository_control_fixture(&self.base.store).await;
        self.coordinator = coordinator;
        self.preflight_runtime = PreflightRuntime::new(self.coordinator.clone());
        self.live_runtime = LiveRuntimeControl::new(self.coordinator.clone());
        let manager = self.spawn_manager();
        self.manager = Some(manager);
    }

    fn spawn_manager(&mut self) -> DeliveryManagerHandle {
        let (process_proofs, actor_lifetime) = tracked_process_proofs(self.process_proofs.clone());
        self.manager_actor_lifetimes.push(actor_lifetime);
        let dependencies =
            DeliveryManagerLiveDependencies::new_with_store_operation_query_for_test(
                self.base.store.clone(),
                self.writer.clone(),
                self.task_manager.clone(),
                self.coordinator.clone(),
                self.preflight_runtime.clone(),
                process_proofs,
            )
            .with_live_runtime_registry_for_test(self.live_runtime.clone());
        DeliveryManagerHandle::spawn_live_for_test(
            dependencies,
            ServiceStateController::new(ServiceState::Ready),
            16,
        )
    }

    pub async fn finish(self) {
        if let Err(failure) = self.finish_result().await {
            panic!("{failure}");
        }
    }

    pub(crate) async fn finish_result(mut self) -> Result<(), String> {
        let mut failures = TeardownFailures::new("delivery-merge fixture");
        if let Some(manager) = self.manager.take() {
            stop_delivery_manager(manager, &self.manager_actor_lifetimes, &mut failures).await;
        }
        stop_task_manager(
            self.task_manager,
            &self.task_manager_actor_lifetime,
            &mut failures,
        )
        .await;
        stop_store_writer(self.writer, &self.writer_actor_lifetime, &mut failures).await;
        close_dispatcher(self.dispatcher, &mut failures).await;

        drop(self.preflight_runtime);
        drop(self.process_proofs);
        drop(self.live_runtime);
        drop(self.coordinator);
        if let Err(error) = self.base.close().await {
            failures.push(error);
        }
        failures.into_result()
    }

    pub async fn prepare_accept(&self) -> PreparedAccept {
        let task = approved_task(&self.base).await;
        let preflight = self
            .manager()
            .preflight(DeliveryPreflightRequest::new(
                PreflightCommandRequest::try_new(
                    ClientRequestId::new(),
                    task.id,
                    GitBranchRef::from_str("refs/heads/main").expect("valid target branch"),
                    GitCommitOid::from_str(TARGET_HEAD).expect("valid target head"),
                )
                .expect("valid preflight request"),
            ))
            .await
            .expect("preflight manager remains open");
        let operation_id = match preflight {
            DeliveryPreflightOutcome::Durable(operation) => {
                assert_eq!(operation.state(), DeliveryPreflightState::PreflightReady);
                operation.operation_id()
            }
            other => panic!("expected durable ready preflight, got {other:?}"),
        };
        let operation = self.operation(operation_id).await;
        let snapshot = self
            .base
            .store
            .delivery_eligibility_snapshot(task.id)
            .await
            .expect("load accept snapshot")
            .expect("accept task exists");
        let evidence = snapshot
            .evidence_identity
            .expect("approved task has evidence identity");
        let command = AcceptMergeCommandRequest::try_new(
            ClientRequestId::new(),
            task.id,
            operation_id,
            operation.version,
            evidence.workspace_generation(),
            evidence.workspace_fingerprint().clone(),
            operation.target_branch,
            operation.expected_target_head,
        )
        .expect("valid accept request");
        PreparedAccept {
            task,
            operation_id,
            command,
        }
    }

    pub async fn accept(&self, prepared: &PreparedAccept) -> DeliveryMergeAcceptance {
        match self
            .manager()
            .accept_merge(prepared.request())
            .await
            .expect("accept manager remains open")
        {
            DeliveryMergeAcceptanceOutcome::Durable(acceptance) => acceptance,
            other => panic!("expected durable acceptance, got {other:?}"),
        }
    }

    pub async fn operation(&self, operation_id: DeliveryOperationId) -> MergeOperationRecord {
        assert_merge_snapshot(
            self.base
                .store
                .delivery_operation_snapshot(operation_id)
                .await
                .expect("load delivery operation")
                .expect("delivery operation exists"),
        )
    }

    pub async fn source(&self, task_id: TaskId) -> Option<DeliverySourceRecord> {
        self.base
            .store
            .delivery_eligibility_snapshot(task_id)
            .await
            .expect("load delivery source snapshot")
            .expect("delivery task exists")
            .ownership
            .source
    }

    pub async fn wait_operation_state(
        &self,
        operation_id: DeliveryOperationId,
        expected: MergeOperationState,
    ) -> MergeOperationRecord {
        timeout(Duration::from_secs(10), async {
            loop {
                let operation = self.operation(operation_id).await;
                if operation.state == expected {
                    return operation;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("operation {operation_id} did not reach {expected:?}"))
    }

    pub async fn wait_source_state(
        &self,
        task_id: TaskId,
        expected: DeliverySourceState,
    ) -> DeliverySourceRecord {
        timeout(Duration::from_secs(10), async {
            loop {
                if let Some(source) = self.source(task_id).await
                    && source.state == expected
                {
                    return source;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("source for {task_id} did not reach {expected:?}"))
    }

    pub async fn wait_repository_state(&self, expected: RepositoryControlState) {
        timeout(Duration::from_secs(10), async {
            loop {
                if self
                    .coordinator
                    .control_state(self.base.repository.id)
                    .expect("fixture repository remains registered")
                    == expected
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("repository did not reach {expected:?}"));
    }
}

async fn approved_task(fixture: &support::StoreFixture) -> Task {
    let repository = &fixture.repository;
    let queued = fixture
        .store
        .create_task(support::new_task(
            repository.id,
            "delivery merge approved task",
        ))
        .await
        .expect("create fixture task")
        .task()
        .clone();
    let running = match fixture
        .store
        .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .expect("start fixture task")
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("fixture task must start"),
    };
    fixture
        .store
        .append_running_event(
            running.id,
            TaskEventPayload::PlanUpdated {
                plan: support::fixture_review_plan(),
            },
        )
        .await
        .expect("persist fixture plan");
    let running = fixture
        .store
        .task_detail(running.id)
        .await
        .expect("read fixture task")
        .expect("fixture task exists")
        .task;
    let identity = AttemptArtifactIdentity {
        task_id: running.id,
        repository_id: running.repository_id,
        attempt: running.attempt,
    };
    let artifact_namespace = format!("delivery-merge-{}-{}", repository.id, running.id);
    fixture
        .store
        .reserve_attempt_artifact(ReserveAttemptArtifact {
            identity,
            base_commit: BASE_COMMIT.to_owned(),
            branch_name: format!("codex/{artifact_namespace}"),
            worktree_path: CanonicalPath::try_from_canonical(
                repository
                    .git_root
                    .as_path()
                    .join("artifacts")
                    .join(artifact_namespace),
            )
            .expect("valid artifact path"),
        })
        .await
        .expect("reserve fixture artifact");
    fixture
        .store
        .mark_attempt_artifact_ready(identity)
        .await
        .expect("mark fixture artifact ready");
    match fixture
        .store
        .finalize_reviewed_task(
            running.id,
            running.repository_id,
            running.attempt,
            support::approved_review(),
        )
        .await
        .expect("finalize approved fixture task")
    {
        FinalizeReviewedTaskOutcome::Applied { task, .. }
        | FinalizeReviewedTaskOutcome::Existing { task, .. } => task,
    }
}
