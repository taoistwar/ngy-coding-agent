use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use coding_agent_app::{
    DeliveryBranchCleanupBinding, DeliveryCleanupRuntimeRegistry,
    DeliveryCleanupRuntimeRegistryTestSeam, DeliveryCleanupRuntimeSession,
    DeliveryCleanupRuntimeSessionTestSeam, DeliveryDeleteBranchRequest,
    DeliveryLiveBranchCleanupIntent, DeliveryLiveBranchCleanupRefreshProof,
    DeliveryLiveCleanupRuntimeError, DeliveryLiveDeletePendingCapability,
    DeliveryLiveDeletePendingDisposition, DeliveryLiveRemovePendingCapability,
    DeliveryLiveUnlockPendingCapability, DeliveryLiveUnlockedPendingRemoveCapability,
    DeliveryLiveWorktreeCleanupIntent, DeliveryManagerHandle, DeliveryManagerLiveDependencies,
    DeliveryProcessProof, DeliveryProcessProofError, DeliveryProcessProofProvider,
    DeliveryProcessProofProviderTestSeam, DeliveryRemoveWorktreeRequest, DeliveryRuntimeFailure,
    DeliveryRuntimeRegistry, DeliveryRuntimeRegistryTestSeam, DeliveryWorktreeCleanupBinding,
    EventDispatcherHandle, RepositoryControlCoordinator, RepositoryControlState,
    SchedulerConcurrencyLimits, ServiceState, ServiceStateController, StoreWriterHandle,
    StoreWriterTestController, TaskManagerHandle, TaskManagerLaunchResources,
};
use coding_agent_domain::{ClientRequestId, Task};
use coding_agent_runtime::{
    DeliveryRemovePendingDisposition, DeliveryUnlockPendingDisposition,
    DeliveryUnlockedPendingRemoveDisposition,
};
use coding_agent_store::{
    CleanupOperationRecord, CleanupOperationState, CleanupReconciliationReason,
    DeleteBranchCommandRequest, DeliveryEligibilitySnapshot, DeliveryOperationId,
    DeliveryOperationSnapshot, GitCommitOid, RemoveWorktreeCommandRequest,
};
use tokio::time::{Duration, timeout};

use crate::delivery_merge_support::teardown::{
    TeardownFailures, close_dispatcher, stop_delivery_manager, stop_store_writer,
    stop_task_manager, tracked_dispatcher_wake, tracked_process_proofs,
};
use crate::delivery_merge_support::{DeliveryMergeFixture, EXPECTED_MERGE_COMMIT};
use crate::support;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupStage {
    BindWorktree,
    Unlock,
    EnterRemove,
    Remove,
    BindBranch,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupFault {
    Unavailable,
    TargetWorktreeDirty,
    ProcessCleanupUnproven,
    Reconcile(CleanupReconciliationReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CleanupCall {
    BindWorktreeAcceptance,
    BindWorktreePersisted(CleanupOperationState),
    Unlock(u64),
    EnterRemove(u64),
    Remove(u64),
    BindBranchAcceptance,
    BindBranchPersisted(CleanupOperationState),
    Delete(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchStep {
    Deleted,
    Refresh(GitCommitOid),
    SourceNotMerged,
    CommandTimedOut,
    ReconciliationRequired,
    RetryExact,
}

struct CleanupRuntimeState {
    calls: Mutex<Vec<CleanupCall>>,
    faults: Mutex<VecDeque<(CleanupStage, CleanupFault)>>,
    branch_steps: Mutex<VecDeque<BranchStep>>,
    enter_remove_steps: Mutex<VecDeque<DeliveryUnlockedPendingRemoveDisposition>>,
    remove_steps: Mutex<VecDeque<DeliveryRemovePendingDisposition>>,
    next_identity: AtomicU64,
}

#[derive(Clone)]
pub struct CleanupRuntimeControl {
    state: Arc<CleanupRuntimeState>,
}

impl Default for CleanupRuntimeControl {
    fn default() -> Self {
        Self {
            state: Arc::new(CleanupRuntimeState {
                calls: Mutex::new(Vec::new()),
                faults: Mutex::new(VecDeque::new()),
                branch_steps: Mutex::new(VecDeque::new()),
                enter_remove_steps: Mutex::new(VecDeque::new()),
                remove_steps: Mutex::new(VecDeque::new()),
                next_identity: AtomicU64::new(100),
            }),
        }
    }
}

impl CleanupRuntimeControl {
    pub fn fail_once(&self, stage: CleanupStage, fault: CleanupFault) {
        self.state
            .faults
            .lock()
            .expect("lock cleanup faults")
            .push_back((stage, fault));
    }

    pub fn push_branch_step(&self, step: BranchStep) {
        self.state
            .branch_steps
            .lock()
            .expect("lock branch script")
            .push_back(step);
    }

    pub fn push_remove_step(&self, step: DeliveryRemovePendingDisposition) {
        self.state
            .remove_steps
            .lock()
            .expect("lock remove script")
            .push_back(step);
    }

    pub fn push_enter_remove_step(&self, step: DeliveryUnlockedPendingRemoveDisposition) {
        self.state
            .enter_remove_steps
            .lock()
            .expect("lock enter-remove script")
            .push_back(step);
    }

    pub fn calls(&self) -> Vec<CleanupCall> {
        self.state.calls.lock().expect("lock cleanup calls").clone()
    }

    fn next_identity(&self) -> u64 {
        self.state.next_identity.fetch_add(10, Ordering::SeqCst)
    }

    fn record(&self, call: CleanupCall) {
        self.state
            .calls
            .lock()
            .expect("lock cleanup calls")
            .push(call);
    }

    fn take_fault(&self, stage: CleanupStage) -> Result<(), DeliveryLiveCleanupRuntimeError> {
        let mut faults = self.state.faults.lock().expect("lock cleanup faults");
        if faults
            .front()
            .is_some_and(|(candidate, _)| *candidate == stage)
        {
            let (_, fault) = faults.pop_front().expect("front cleanup fault exists");
            return Err(match fault {
                CleanupFault::Unavailable => DeliveryLiveCleanupRuntimeError::Unavailable,
                CleanupFault::TargetWorktreeDirty => {
                    DeliveryLiveCleanupRuntimeError::TargetWorktreeDirty
                }
                CleanupFault::ProcessCleanupUnproven => {
                    DeliveryLiveCleanupRuntimeError::ProcessCleanupUnproven
                }
                CleanupFault::Reconcile(reason) => {
                    DeliveryLiveCleanupRuntimeError::ReconciliationRequired(reason)
                }
            });
        }
        Ok(())
    }
}

impl DeliveryCleanupRuntimeRegistryTestSeam for CleanupRuntimeControl {}

#[async_trait::async_trait]
impl DeliveryCleanupRuntimeRegistry for CleanupRuntimeControl {
    async fn open_cleanup_session(
        &self,
        _snapshot: &DeliveryEligibilitySnapshot,
    ) -> Result<Arc<dyn DeliveryCleanupRuntimeSession>, DeliveryLiveCleanupRuntimeError> {
        Ok(Arc::new(CleanupRuntimeSession {
            control: self.clone(),
        }))
    }
}

struct CleanupRuntimeSession {
    control: CleanupRuntimeControl,
}

impl DeliveryCleanupRuntimeSessionTestSeam for CleanupRuntimeSession {}

#[async_trait::async_trait]
impl DeliveryCleanupRuntimeSession for CleanupRuntimeSession {
    async fn bind_worktree_cleanup(
        &self,
        _snapshot: &DeliveryEligibilitySnapshot,
        binding: DeliveryWorktreeCleanupBinding<'_>,
    ) -> Result<DeliveryLiveWorktreeCleanupIntent, DeliveryLiveCleanupRuntimeError> {
        self.control.take_fault(CleanupStage::BindWorktree)?;
        self.control.record(match binding {
            DeliveryWorktreeCleanupBinding::Acceptance(_) => CleanupCall::BindWorktreeAcceptance,
            DeliveryWorktreeCleanupBinding::Persisted(operation) => {
                CleanupCall::BindWorktreePersisted(operation.state)
            }
        });
        Ok(DeliveryLiveWorktreeCleanupIntent::new_for_test(
            self.control.next_identity(),
        ))
    }

    async fn bind_branch_cleanup(
        &self,
        _snapshot: &DeliveryEligibilitySnapshot,
        binding: DeliveryBranchCleanupBinding<'_>,
    ) -> Result<DeliveryLiveBranchCleanupIntent, DeliveryLiveCleanupRuntimeError> {
        self.control.take_fault(CleanupStage::BindBranch)?;
        self.control.record(match binding {
            DeliveryBranchCleanupBinding::Acceptance(_) => CleanupCall::BindBranchAcceptance,
            DeliveryBranchCleanupBinding::Persisted(operation) => {
                CleanupCall::BindBranchPersisted(operation.state)
            }
        });
        Ok(DeliveryLiveBranchCleanupIntent::new_for_test(
            self.control.next_identity(),
        ))
    }

    async fn drive_unlock_pending(
        &self,
        capability: DeliveryLiveUnlockPendingCapability,
    ) -> Result<DeliveryUnlockPendingDisposition, DeliveryLiveCleanupRuntimeError> {
        self.control.take_fault(CleanupStage::Unlock)?;
        self.control.record(CleanupCall::Unlock(
            capability
                .identity_for_test()
                .expect("controlled unlock capability"),
        ));
        Ok(DeliveryUnlockPendingDisposition::UnlockApplied)
    }

    async fn drive_unlocked_pending_remove(
        &self,
        capability: DeliveryLiveUnlockedPendingRemoveCapability,
    ) -> Result<DeliveryUnlockedPendingRemoveDisposition, DeliveryLiveCleanupRuntimeError> {
        self.control.take_fault(CleanupStage::EnterRemove)?;
        self.control.record(CleanupCall::EnterRemove(
            capability
                .identity_for_test()
                .expect("controlled enter-remove capability"),
        ));
        Ok(self
            .control
            .state
            .enter_remove_steps
            .lock()
            .expect("lock enter-remove script")
            .pop_front()
            .unwrap_or(DeliveryUnlockedPendingRemoveDisposition::EnterRemovePending))
    }

    async fn drive_remove_pending(
        &self,
        capability: DeliveryLiveRemovePendingCapability,
    ) -> Result<DeliveryRemovePendingDisposition, DeliveryLiveCleanupRuntimeError> {
        self.control.take_fault(CleanupStage::Remove)?;
        self.control.record(CleanupCall::Remove(
            capability
                .identity_for_test()
                .expect("controlled remove capability"),
        ));
        Ok(self
            .control
            .state
            .remove_steps
            .lock()
            .expect("lock remove script")
            .pop_front()
            .unwrap_or(DeliveryRemovePendingDisposition::Removed))
    }

    async fn drive_delete_pending(
        &self,
        capability: DeliveryLiveDeletePendingCapability,
    ) -> Result<DeliveryLiveDeletePendingDisposition, DeliveryLiveCleanupRuntimeError> {
        self.control.take_fault(CleanupStage::Delete)?;
        let identity = capability
            .identity_for_test()
            .expect("controlled delete capability");
        self.control.record(CleanupCall::Delete(identity));
        Ok(
            match self
                .control
                .state
                .branch_steps
                .lock()
                .expect("lock branch script")
                .pop_front()
                .unwrap_or(BranchStep::Deleted)
            {
                BranchStep::Deleted => DeliveryLiveDeletePendingDisposition::Deleted,
                BranchStep::Refresh(head) => {
                    DeliveryLiveDeletePendingDisposition::RefreshExpectedTarget(
                        DeliveryLiveBranchCleanupRefreshProof::new_for_test(identity, head),
                    )
                }
                BranchStep::SourceNotMerged => {
                    DeliveryLiveDeletePendingDisposition::KnownNotAppliedSourceNotMerged
                }
                BranchStep::CommandTimedOut => {
                    DeliveryLiveDeletePendingDisposition::KnownNotAppliedCommandTimedOut
                }
                BranchStep::ReconciliationRequired => {
                    DeliveryLiveDeletePendingDisposition::ReconciliationRequired
                }
                BranchStep::RetryExact => DeliveryLiveDeletePendingDisposition::RetryExactDelete,
            },
        )
    }
}

#[derive(Default)]
pub struct CleanupProcessProofs {
    next: Mutex<VecDeque<DeliveryProcessProof>>,
}

impl CleanupProcessProofs {
    pub fn push(&self, proof: DeliveryProcessProof) {
        self.next
            .lock()
            .expect("lock cleanup process proofs")
            .push_back(proof);
    }
}

impl DeliveryProcessProofProviderTestSeam for CleanupProcessProofs {}

#[async_trait::async_trait]
impl DeliveryProcessProofProvider for CleanupProcessProofs {
    async fn observe(
        &self,
        _task_id: coding_agent_domain::TaskId,
    ) -> Result<DeliveryProcessProof, DeliveryProcessProofError> {
        Ok(self
            .next
            .lock()
            .expect("lock cleanup process proofs")
            .pop_front()
            .unwrap_or(DeliveryProcessProof::Clean))
    }
}

struct UnusedPreflightRuntime;

impl DeliveryRuntimeRegistryTestSeam for UnusedPreflightRuntime {}

#[async_trait::async_trait]
impl DeliveryRuntimeRegistry for UnusedPreflightRuntime {
    async fn open_session(
        &self,
        _snapshot: &DeliveryEligibilitySnapshot,
    ) -> Result<Arc<dyn coding_agent_app::DeliveryRuntimeSession>, DeliveryRuntimeFailure> {
        Err(DeliveryRuntimeFailure::Unavailable)
    }
}

pub struct DeliveryCleanupFixture {
    pub merge: DeliveryMergeFixture,
    pub task: Task,
    pub merge_operation_id: DeliveryOperationId,
    pub coordinator: Arc<RepositoryControlCoordinator>,
    pub runtime: Arc<CleanupRuntimeControl>,
    pub process_proofs: Arc<CleanupProcessProofs>,
    dispatcher: EventDispatcherHandle,
    writer: StoreWriterHandle,
    writer_actor_lifetime: Weak<()>,
    task_manager: TaskManagerHandle,
    task_manager_actor_lifetime: Weak<support::ControlledRunner>,
    manager: Option<DeliveryManagerHandle>,
    manager_actor_lifetimes: Vec<Weak<()>>,
}

impl DeliveryCleanupFixture {
    pub async fn new(writer_controller: Option<Arc<StoreWriterTestController>>) -> Self {
        let merge = DeliveryMergeFixture::new(None).await;
        let prepared = merge.prepare_accept().await;
        merge.accept(&prepared).await;
        merge
            .wait_operation_state(
                prepared.operation_id,
                coding_agent_store::MergeOperationState::Merged,
            )
            .await;
        merge
            .wait_repository_state(RepositoryControlState::Available)
            .await;

        let dispatcher = EventDispatcherHandle::spawn(merge.base.store.clone(), 128)
            .await
            .expect("spawn delivery-cleanup dispatcher");
        let (writer_wake, writer_actor_lifetime) = tracked_dispatcher_wake(dispatcher.clone());
        let writer = match writer_controller {
            Some(controller) => StoreWriterHandle::spawn_with_test_controller(
                merge.base.store.clone(),
                writer_wake,
                32,
                controller,
            ),
            None => StoreWriterHandle::spawn(merge.base.store.clone(), writer_wake, 32),
        };
        let task_manager_state = ServiceStateController::new(ServiceState::StoreDegraded);
        let launch_resources = TaskManagerLaunchResources::new_for_test(
            SchedulerConcurrencyLimits::try_new(4, 4).expect("valid cleanup fixture limits"),
            merge.coordinator.clone(),
            merge.base.instance_process_scope(),
        );
        let runner = Arc::new(support::ControlledRunner::default());
        let task_manager_actor_lifetime = Arc::downgrade(&runner);
        let task_manager = TaskManagerHandle::spawn(
            merge.base.store.clone(),
            writer.clone(),
            dispatcher.clone(),
            task_manager_state,
            runner,
            launch_resources,
            32,
        );
        let runtime = Arc::new(CleanupRuntimeControl::default());
        let process_proofs = Arc::new(CleanupProcessProofs::default());
        let coordinator = merge.coordinator.clone();
        let mut fixture = Self {
            merge,
            task: prepared.task,
            merge_operation_id: prepared.operation_id,
            coordinator,
            runtime,
            process_proofs,
            dispatcher,
            writer,
            writer_actor_lifetime,
            task_manager,
            task_manager_actor_lifetime,
            manager: None,
            manager_actor_lifetimes: Vec::new(),
        };
        let manager = fixture.spawn_manager();
        fixture.manager = Some(manager);
        fixture
    }

    pub fn manager(&self) -> &DeliveryManagerHandle {
        self.manager.as_ref().expect("cleanup manager is running")
    }

    pub async fn restart_manager(&mut self) {
        drop(self.manager.take());
        tokio::task::yield_now().await;
        let manager = self.spawn_manager();
        self.manager = Some(manager);
    }

    fn spawn_manager(&mut self) -> DeliveryManagerHandle {
        let (process_proofs, actor_lifetime) = tracked_process_proofs(self.process_proofs.clone());
        self.manager_actor_lifetimes.push(actor_lifetime);
        let dependencies =
            DeliveryManagerLiveDependencies::new_with_store_operation_query_for_test(
                self.merge.base.store.clone(),
                self.writer.clone(),
                self.task_manager.clone(),
                self.coordinator.clone(),
                Arc::new(UnusedPreflightRuntime),
                process_proofs,
            )
            .with_cleanup_runtime_registry_for_test(self.runtime.clone());
        DeliveryManagerHandle::spawn_live_for_test(
            dependencies,
            ServiceStateController::new(ServiceState::Ready),
            16,
        )
    }

    pub async fn finish(mut self) {
        let mut failures = TeardownFailures::new("delivery-cleanup fixture");
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

        drop(self.process_proofs);
        drop(self.runtime);
        drop(self.coordinator);
        if let Err(error) = self.merge.finish_result().await {
            failures.push(error);
        }
        if let Err(failure) = failures.into_result() {
            panic!("{failure}");
        }
    }

    pub async fn remove_request(&self) -> RemoveWorktreeCommandRequest {
        let snapshot = self
            .merge
            .base
            .store
            .delivery_ownership_snapshot(self.task.id)
            .await
            .expect("load cleanup ownership")
            .expect("cleanup task exists");
        let source = snapshot.source.as_ref().expect("merged source exists");
        let disposition = snapshot
            .disposition
            .as_ref()
            .expect("merged disposition exists");
        RemoveWorktreeCommandRequest::try_new(
            ClientRequestId::new(),
            self.task.id,
            disposition.worktree_version,
            disposition.merged_operation_id,
            source.provenance.source_branch.clone(),
            source
                .expected_source_commit
                .clone()
                .expect("committed source oid"),
        )
        .expect("valid worktree cleanup request")
    }

    pub async fn delete_request(&self, target_head: &str) -> DeleteBranchCommandRequest {
        let snapshot = self
            .merge
            .base
            .store
            .delivery_ownership_snapshot(self.task.id)
            .await
            .expect("load branch cleanup ownership")
            .expect("cleanup task exists");
        let source = snapshot.source.as_ref().expect("merged source exists");
        let disposition = snapshot
            .disposition
            .as_ref()
            .expect("merged disposition exists");
        let merge = snapshot
            .merge_operations
            .iter()
            .find(|operation| operation.operation_id == disposition.merged_operation_id)
            .expect("merged operation exists");
        DeleteBranchCommandRequest::try_new(
            ClientRequestId::new(),
            self.task.id,
            disposition.branch_version,
            disposition.merged_operation_id,
            source.provenance.source_branch.clone(),
            source
                .expected_source_commit
                .clone()
                .expect("committed source oid"),
            merge.target_branch.clone(),
            GitCommitOid::from_str(target_head).expect("valid target head"),
        )
        .expect("valid branch cleanup request")
    }

    pub async fn remove(&self) -> coding_agent_app::DeliveryCleanupAcceptance {
        let request = self.remove_request().await;
        match self
            .manager()
            .remove_worktree(DeliveryRemoveWorktreeRequest::new(request))
            .await
            .expect("cleanup manager remains open")
        {
            coding_agent_app::DeliveryCleanupAcceptanceOutcome::Durable(acceptance) => acceptance,
            other => panic!("expected durable worktree acceptance, got {other:?}"),
        }
    }

    pub async fn delete(&self) -> coding_agent_app::DeliveryCleanupAcceptance {
        let request = self.delete_request(EXPECTED_MERGE_COMMIT).await;
        match self
            .manager()
            .delete_branch(DeliveryDeleteBranchRequest::new(request))
            .await
            .expect("cleanup manager remains open")
        {
            coding_agent_app::DeliveryCleanupAcceptanceOutcome::Durable(acceptance) => acceptance,
            other => panic!("expected durable branch acceptance, got {other:?}"),
        }
    }

    pub async fn operation(&self, operation_id: DeliveryOperationId) -> CleanupOperationRecord {
        match self
            .merge
            .base
            .store
            .delivery_operation_snapshot(operation_id)
            .await
            .expect("load cleanup operation")
            .expect("cleanup operation exists")
        {
            DeliveryOperationSnapshot::Cleanup(operation) => *operation,
            DeliveryOperationSnapshot::Merge(_) => panic!("expected cleanup operation"),
        }
    }

    pub async fn wait_operation_state(
        &self,
        operation_id: DeliveryOperationId,
        expected: CleanupOperationState,
    ) -> CleanupOperationRecord {
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
        .unwrap_or_else(|_| panic!("cleanup operation {operation_id} did not reach {expected:?}"))
    }

    pub async fn wait_repository_state(&self, expected: RepositoryControlState) {
        self.merge.wait_repository_state(expected).await;
    }
}
