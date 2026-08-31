use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;

use coding_agent_app::{
    DeliveryAcceptAuthenticationError, DeliveryLiveAbortAppliedProof, DeliveryLiveAbortDisposition,
    DeliveryLiveAbortProof, DeliveryLiveExpectedMergeProof, DeliveryLiveMergeAppliedProof,
    DeliveryLiveMergeDisposition, DeliveryLiveRuntimeError, DeliveryLiveRuntimeRegistry,
    DeliveryLiveRuntimeRegistryTestSeam, DeliveryLiveRuntimeSession,
    DeliveryLiveRuntimeSessionTestSeam, DeliveryLiveSourceAppliedProof,
    DeliveryLiveSourceObjectProof, DeliveryLiveSourceResult, DeliveryProcessProof,
    DeliveryProcessProofError, DeliveryProcessProofProvider, DeliveryProcessProofProviderTestSeam,
    DeliveryRuntimeAuthentication, RepositoryControlCoordinator,
};
use coding_agent_domain::TaskId;
use coding_agent_store::{
    AcceptMergeCommandRequest, DeliveryEligibilitySnapshot, DeliveryOperationSnapshot,
    DeliverySourceAppliedProof, DeliverySourceObjectProof, DeliverySourceRecord,
    DeliverySourceRetryReason, DirectoryIdentity, GitCommitOid, MergeAbortAppliedProof,
    MergeAbortProof, MergeAppliedProof, MergeAutostashObservation, MergeCommitObjectProof,
    MergeConflictPaths, MergeOperationRecord, MergeReconciliationReason,
    OtherGitOperationObservation, Sha256Digest, SourceWorktreeProof,
};
use tokio::sync::Semaphore;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

use super::{ABORT_INDEX_DIGEST, ABORT_WORKTREE_DIGEST, EXPECTED_MERGE_COMMIT, SOURCE_COMMIT};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveStage {
    OpenSession,
    AuthenticateAccept,
    SourceObject,
    SourceCommit,
    ExpectedMerge,
    ActualMerge,
    Abort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveCall {
    AuthenticateAccept,
    SourceObject,
    SourceCommit,
    ExpectedMerge,
    ActualMerge,
    Abort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveFault {
    Unavailable,
    ReconciliationRequired(MergeReconciliationReason),
    ProcessCleanupUnproven,
    Accept(DeliveryAcceptAuthenticationError),
}

pub struct StageGate {
    reached: Semaphore,
    release: Semaphore,
    exited: Semaphore,
}

struct StageGateExitProof<'a>(&'a Semaphore);

impl Drop for StageGateExitProof<'_> {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}

type StageGateSlot = Arc<Mutex<Option<(LiveStage, Arc<StageGate>)>>>;

/// One-shot test delay with a start barrier. Tests can leave Store and retry
/// timers on real time, then freeze the clock only after this stage timer is
/// registered.
pub struct StageDelay {
    started: Semaphore,
}

impl StageGate {
    fn new() -> Self {
        Self {
            reached: Semaphore::new(0),
            release: Semaphore::new(0),
            exited: Semaphore::new(0),
        }
    }

    pub async fn wait_until_reached(&self) {
        self.reached
            .acquire()
            .await
            .expect("live stage gate remains open")
            .forget();
    }

    pub fn release(&self) {
        self.release.add_permits(1);
    }

    pub async fn wait_until_exited(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.exited.acquire())
            .await
            .expect("live stage exits after release or cancellation")
            .expect("live stage exit proof remains open")
            .forget();
    }

    async fn enter(&self) {
        self.reached.add_permits(1);
        let _exit_proof = StageGateExitProof(&self.exited);
        self.release
            .acquire()
            .await
            .expect("live stage gate remains open")
            .forget();
    }
}

impl StageDelay {
    fn new() -> Self {
        Self {
            started: Semaphore::new(0),
        }
    }

    pub async fn wait_until_started(&self) {
        self.started
            .acquire()
            .await
            .expect("live stage delay remains open")
            .forget();
    }

    fn mark_started(&self) {
        self.started.add_permits(1);
    }
}

#[derive(Default)]
pub struct ControlledProcessProofs {
    next: Mutex<VecDeque<DeliveryProcessProof>>,
    observations: AtomicUsize,
}

impl ControlledProcessProofs {
    pub fn push(&self, proof: DeliveryProcessProof) {
        self.next
            .lock()
            .expect("lock process-proof script")
            .push_back(proof);
    }
}

impl DeliveryProcessProofProviderTestSeam for ControlledProcessProofs {}

#[async_trait::async_trait]
impl DeliveryProcessProofProvider for ControlledProcessProofs {
    async fn observe(
        &self,
        _task_id: TaskId,
    ) -> Result<DeliveryProcessProof, DeliveryProcessProofError> {
        self.observations.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .next
            .lock()
            .expect("lock process-proof script")
            .pop_front()
            .unwrap_or(DeliveryProcessProof::Clean))
    }
}

pub struct LiveRuntimeControl {
    coordinator: Arc<RepositoryControlCoordinator>,
    calls: Arc<Mutex<Vec<LiveCall>>>,
    fault: Arc<Mutex<Option<(LiveStage, LiveFault)>>>,
    gate: StageGateSlot,
    delays: Arc<Mutex<StageDelayQueue>>,
    conflict: Arc<AtomicBool>,
    source_known_not_applied: Arc<AtomicUsize>,
    abort_observed_persisted_proof: Arc<AtomicBool>,
}

type StageDelayQueue = VecDeque<(LiveStage, Duration, Arc<StageDelay>)>;

impl LiveRuntimeControl {
    pub fn new(coordinator: Arc<RepositoryControlCoordinator>) -> Arc<Self> {
        Arc::new(Self {
            coordinator,
            calls: Arc::new(Mutex::new(Vec::new())),
            fault: Arc::new(Mutex::new(None)),
            gate: Arc::new(Mutex::new(None)),
            delays: Arc::new(Mutex::new(VecDeque::new())),
            conflict: Arc::new(AtomicBool::new(false)),
            source_known_not_applied: Arc::new(AtomicUsize::new(0)),
            abort_observed_persisted_proof: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn fail_once(&self, stage: LiveStage, fault: LiveFault) {
        *self.fault.lock().expect("lock live fault") = Some((stage, fault));
    }

    pub fn install_gate(&self, stage: LiveStage) -> Arc<StageGate> {
        let gate = Arc::new(StageGate::new());
        *self.gate.lock().expect("lock live gate") = Some((stage, gate.clone()));
        gate
    }

    pub fn delay_once(&self, stage: LiveStage, delay: Duration) -> Arc<StageDelay> {
        let control = Arc::new(StageDelay::new());
        self.delays.lock().expect("lock live delays").push_back((
            stage,
            delay,
            Arc::clone(&control),
        ));
        control
    }

    pub fn use_conflict(&self) {
        self.conflict.store(true, Ordering::SeqCst);
    }

    pub fn source_known_not_applied_times(&self, count: usize) {
        self.source_known_not_applied.store(count, Ordering::SeqCst);
    }

    pub fn calls(&self) -> Vec<LiveCall> {
        self.calls.lock().expect("lock live calls").clone()
    }

    pub fn abort_observed_persisted_proof(&self) -> bool {
        self.abort_observed_persisted_proof.load(Ordering::SeqCst)
    }

    fn record(&self, call: LiveCall) {
        self.calls.lock().expect("lock live calls").push(call);
    }

    fn take_fault(&self, stage: LiveStage) -> Option<LiveFault> {
        let mut fault = self.fault.lock().expect("lock live fault");
        match *fault {
            Some((expected, value)) if expected == stage => {
                *fault = None;
                Some(value)
            }
            _ => None,
        }
    }

    async fn enter_gate(&self, stage: LiveStage) {
        let gate = self
            .gate
            .lock()
            .expect("lock live gate")
            .as_ref()
            .and_then(|(expected, gate)| (*expected == stage).then(|| gate.clone()));
        if let Some(gate) = gate {
            gate.enter().await;
        }
    }

    async fn enter_delay(&self, stage: LiveStage) {
        let delay = {
            let mut delays = self.delays.lock().expect("lock live delays");
            delays
                .iter()
                .position(|(expected, _, _)| *expected == stage)
                .and_then(|index| delays.remove(index))
                .map(|(_, delay, control)| (delay, control))
        };
        if let Some((delay, control)) = delay {
            let mut timer = Box::pin(sleep(delay));
            let already_elapsed =
                poll_fn(|context| Poll::Ready(timer.as_mut().poll(context).is_ready())).await;
            control.mark_started();
            if !already_elapsed {
                timer.await;
            }
        }
    }

    fn fault_result<T>(&self, stage: LiveStage) -> Option<Result<T, DeliveryLiveRuntimeError>> {
        self.take_fault(stage).map(|fault| {
            Err(match fault {
                LiveFault::Unavailable => DeliveryLiveRuntimeError::Unavailable,
                LiveFault::ReconciliationRequired(reason) => {
                    DeliveryLiveRuntimeError::ReconciliationRequired(reason)
                }
                LiveFault::ProcessCleanupUnproven => {
                    DeliveryLiveRuntimeError::ProcessCleanupUnproven
                }
                LiveFault::Accept(_) => DeliveryLiveRuntimeError::Unavailable,
            })
        })
    }

    fn accept_fault_result(
        &self,
    ) -> Option<Result<DeliveryRuntimeAuthentication, DeliveryAcceptAuthenticationError>> {
        self.take_fault(LiveStage::AuthenticateAccept).map(|fault| {
            Err(match fault {
                LiveFault::Accept(error) => error,
                LiveFault::Unavailable => DeliveryAcceptAuthenticationError::Unavailable,
                LiveFault::ReconciliationRequired(reason) => {
                    DeliveryAcceptAuthenticationError::ReconciliationRequired(reason)
                }
                LiveFault::ProcessCleanupUnproven => {
                    DeliveryAcceptAuthenticationError::ProcessCleanupUnproven
                }
            })
        })
    }
}

impl DeliveryLiveRuntimeRegistryTestSeam for LiveRuntimeControl {}

#[async_trait::async_trait]
impl DeliveryLiveRuntimeRegistry for LiveRuntimeControl {
    async fn open_live_session(
        &self,
        snapshot: &DeliveryEligibilitySnapshot,
    ) -> Result<Arc<dyn DeliveryLiveRuntimeSession>, DeliveryLiveRuntimeError> {
        self.enter_delay(LiveStage::OpenSession).await;
        let evidence = snapshot
            .evidence_identity
            .as_ref()
            .ok_or(DeliveryLiveRuntimeError::Unavailable)?;
        let artifact = snapshot
            .ownership
            .artifact
            .as_ref()
            .ok_or(DeliveryLiveRuntimeError::Unavailable)?;
        let authentication = DeliveryRuntimeAuthentication::new_for_test(
            self.coordinator
                .coordination_key(snapshot.task.repository_id)
                .map_err(|_| DeliveryLiveRuntimeError::Unavailable)?,
            evidence.identity(),
            GitCommitOid::from_str(&artifact.base_commit)
                .map_err(|_| DeliveryLiveRuntimeError::Unavailable)?,
            format!("refs/heads/{}", artifact.branch_name)
                .parse()
                .map_err(|_| DeliveryLiveRuntimeError::Unavailable)?,
            evidence.workspace_fingerprint().clone(),
            coding_agent_store::GitObjectAlgorithm::Sha1,
            snapshot_common_identity(snapshot)?,
            snapshot_admin_identity(snapshot)?,
            snapshot_source_config(snapshot)?,
            snapshot_target_branch(snapshot)?,
            snapshot_target_head(snapshot)?,
            snapshot_target_config(snapshot)?,
            snapshot_target_security(snapshot)?,
        )
        .map_err(|_| DeliveryLiveRuntimeError::Unavailable)?;
        Ok(Arc::new(ControlledLiveSession {
            control: Arc::new(self.clone_for_session()),
            authentication,
        }))
    }
}

impl LiveRuntimeControl {
    fn clone_for_session(&self) -> Self {
        Self {
            coordinator: self.coordinator.clone(),
            calls: self.calls.clone(),
            fault: self.fault.clone(),
            gate: self.gate.clone(),
            delays: self.delays.clone(),
            conflict: self.conflict.clone(),
            source_known_not_applied: self.source_known_not_applied.clone(),
            abort_observed_persisted_proof: self.abort_observed_persisted_proof.clone(),
        }
    }
}

struct ControlledLiveSession {
    control: Arc<LiveRuntimeControl>,
    authentication: DeliveryRuntimeAuthentication,
}

impl DeliveryLiveRuntimeSessionTestSeam for ControlledLiveSession {}

#[async_trait::async_trait]
impl DeliveryLiveRuntimeSession for ControlledLiveSession {
    async fn authenticate_accept(
        &self,
        _command: &AcceptMergeCommandRequest,
    ) -> Result<DeliveryRuntimeAuthentication, DeliveryAcceptAuthenticationError> {
        self.control.record(LiveCall::AuthenticateAccept);
        self.control
            .enter_delay(LiveStage::AuthenticateAccept)
            .await;
        self.control.enter_gate(LiveStage::AuthenticateAccept).await;
        if let Some(result) = self.control.accept_fault_result() {
            return result;
        }
        Ok(self.authentication.clone())
    }

    async fn build_source_object(
        &self,
        source: &DeliverySourceRecord,
    ) -> Result<DeliveryLiveSourceObjectProof, DeliveryLiveRuntimeError> {
        self.control.record(LiveCall::SourceObject);
        self.control.enter_delay(LiveStage::SourceObject).await;
        self.control.enter_gate(LiveStage::SourceObject).await;
        if let Some(result) = self.control.fault_result(LiveStage::SourceObject) {
            return result;
        }
        Ok(DeliveryLiveSourceObjectProof::from_store_proof_for_test(
            source_object_proof(source),
        ))
    }

    async fn apply_source_commit(
        &self,
        source: &DeliverySourceRecord,
    ) -> Result<DeliveryLiveSourceResult, DeliveryLiveRuntimeError> {
        self.control.record(LiveCall::SourceCommit);
        self.control.enter_delay(LiveStage::SourceCommit).await;
        self.control.enter_gate(LiveStage::SourceCommit).await;
        if let Some(result) = self.control.fault_result(LiveStage::SourceCommit) {
            return result;
        }
        if self
            .control
            .source_known_not_applied
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Ok(DeliveryLiveSourceResult::known_not_applied(
                DeliverySourceRetryReason::CommandTimedOut,
            ));
        }
        Ok(DeliveryLiveSourceResult::applied(
            DeliveryLiveSourceAppliedProof::from_store_proof_for_test(source_applied_proof(source)),
        ))
    }

    async fn build_expected_merge(
        &self,
        operation: &MergeOperationRecord,
        source: &DeliverySourceRecord,
    ) -> Result<DeliveryLiveExpectedMergeProof, DeliveryLiveRuntimeError> {
        self.control.record(LiveCall::ExpectedMerge);
        self.control.enter_delay(LiveStage::ExpectedMerge).await;
        self.control.enter_gate(LiveStage::ExpectedMerge).await;
        if let Some(result) = self.control.fault_result(LiveStage::ExpectedMerge) {
            return result;
        }
        Ok(DeliveryLiveExpectedMergeProof::from_store_proof_for_test(
            expected_merge_proof(operation, source),
        ))
    }

    async fn drive_merge_pending(
        &self,
        operation: &MergeOperationRecord,
        source: &DeliverySourceRecord,
    ) -> Result<DeliveryLiveMergeDisposition, DeliveryLiveRuntimeError> {
        self.control.record(LiveCall::ActualMerge);
        self.control.enter_delay(LiveStage::ActualMerge).await;
        self.control.enter_gate(LiveStage::ActualMerge).await;
        if let Some(result) = self.control.fault_result(LiveStage::ActualMerge) {
            return result;
        }
        if self.control.conflict.load(Ordering::SeqCst) {
            Ok(DeliveryLiveMergeDisposition::Conflict(Box::new(
                DeliveryLiveAbortProof::from_store_proof_for_test(abort_proof(operation, source)),
            )))
        } else {
            Ok(DeliveryLiveMergeDisposition::Applied(Box::new(
                DeliveryLiveMergeAppliedProof::from_store_proof_for_test(merge_applied_proof(
                    operation, source,
                )),
            )))
        }
    }

    async fn drive_abort_pending(
        &self,
        operation: &MergeOperationRecord,
        source: &DeliverySourceRecord,
    ) -> Result<DeliveryLiveAbortDisposition, DeliveryLiveRuntimeError> {
        self.control.record(LiveCall::Abort);
        assert_eq!(
            operation.state,
            coding_agent_store::MergeOperationState::AbortPending,
            "runtime abort authority requires a reloaded durable AbortPending"
        );
        assert!(operation.abort_child_receipt_id.is_some());
        assert!(operation.abort_merge_head.is_some());
        assert!(operation.abort_index_stages_digest.is_some());
        assert!(operation.abort_worktree_digest.is_some());
        assert_eq!(
            operation.abort_merge_autostash_proof.as_deref(),
            Some("absent")
        );
        assert!(!operation.conflicts.is_empty());
        self.control
            .abort_observed_persisted_proof
            .store(true, Ordering::SeqCst);
        self.control.enter_delay(LiveStage::Abort).await;
        self.control.enter_gate(LiveStage::Abort).await;
        if let Some(result) = self.control.fault_result(LiveStage::Abort) {
            return result;
        }
        Ok(DeliveryLiveAbortDisposition::Applied(
            DeliveryLiveAbortAppliedProof::from_store_proof_for_test(abort_applied_proof(
                operation, source,
            )),
        ))
    }
}

fn source_object_proof(source: &DeliverySourceRecord) -> DeliverySourceObjectProof {
    DeliverySourceObjectProof::try_new(
        GitCommitOid::from_str(SOURCE_COMMIT).expect("valid source commit"),
        source.candidate_tree.clone(),
        vec![source.expected_parent.clone()],
        source.commit_metadata.clone(),
    )
    .expect("valid source object proof")
}

fn source_applied_proof(source: &DeliverySourceRecord) -> DeliverySourceAppliedProof {
    let commit = source
        .expected_source_commit
        .clone()
        .expect("CommitPending stores the expected source commit");
    DeliverySourceAppliedProof::try_new(
        source_object_proof(source),
        source.provenance.source_branch.clone(),
        commit.clone(),
        commit,
        SourceWorktreeProof::try_new(
            source.candidate_tree.clone(),
            source.candidate_tree.clone(),
            0,
            0,
            0,
            0,
        )
        .expect("valid clean source worktree proof"),
        source.provenance.common_git_identity.clone(),
        source.provenance.worktree_admin_identity.clone(),
        source.provenance.fixed_lock_reason.clone(),
        source.provenance.config_attributes_digest.clone(),
    )
    .expect("valid source applied proof")
}

fn expected_merge_proof(
    operation: &MergeOperationRecord,
    source: &DeliverySourceRecord,
) -> MergeCommitObjectProof {
    MergeCommitObjectProof::try_new(
        GitCommitOid::from_str(EXPECTED_MERGE_COMMIT).expect("valid expected merge commit"),
        operation
            .candidate_merge_tree
            .clone()
            .expect("ready operation has candidate merge tree"),
        vec![
            operation.expected_target_head.clone(),
            source
                .expected_source_commit
                .clone()
                .expect("committed source has a commit"),
        ],
        operation
            .merge_metadata
            .clone()
            .expect("accepted operation has merge metadata"),
    )
    .expect("valid expected merge proof")
}

fn merge_applied_proof(
    operation: &MergeOperationRecord,
    source: &DeliverySourceRecord,
) -> MergeAppliedProof {
    let object = expected_merge_proof(operation, source);
    let merge_commit =
        GitCommitOid::from_str(EXPECTED_MERGE_COMMIT).expect("valid expected merge commit");
    let source_commit = source
        .expected_source_commit
        .clone()
        .expect("committed source has a commit");
    let tree = operation
        .candidate_merge_tree
        .clone()
        .expect("pending merge has candidate tree");
    MergeAppliedProof::try_new(
        object,
        operation.target_branch.clone(),
        merge_commit,
        source.provenance.source_branch.clone(),
        source_commit,
        source.provenance.common_git_identity.clone(),
        source.provenance.worktree_admin_identity.clone(),
        source.provenance.fixed_lock_reason.clone(),
        source.provenance.config_attributes_digest.clone(),
        tree.clone(),
        tree,
        0,
        0,
        0,
        0,
        None,
        MergeAutostashObservation::Absent,
        OtherGitOperationObservation::Clear,
    )
    .expect("valid merge applied proof")
}

fn abort_proof(operation: &MergeOperationRecord, source: &DeliverySourceRecord) -> MergeAbortProof {
    let source_commit = source
        .expected_source_commit
        .clone()
        .expect("committed source has a commit");
    MergeAbortProof::try_new(
        Uuid::from_u128(0x1234),
        operation.target_branch.clone(),
        operation.expected_target_head.clone(),
        source.provenance.source_branch.clone(),
        source_commit.clone(),
        source_commit,
        source.provenance.common_git_identity.clone(),
        source.provenance.worktree_admin_identity.clone(),
        source.provenance.fixed_lock_reason.clone(),
        source.provenance.config_attributes_digest.clone(),
        Sha256Digest::from_str(ABORT_INDEX_DIGEST).expect("valid abort index digest"),
        Sha256Digest::from_str(ABORT_WORKTREE_DIGEST).expect("valid abort worktree digest"),
        MergeAutostashObservation::Absent,
        OtherGitOperationObservation::Clear,
        MergeConflictPaths::try_from_raw(vec![b"src/conflict.rs".to_vec()])
            .expect("valid conflict path"),
    )
    .expect("valid merge abort proof")
}

fn abort_applied_proof(
    operation: &MergeOperationRecord,
    source: &DeliverySourceRecord,
) -> MergeAbortAppliedProof {
    MergeAbortAppliedProof::try_new(
        operation.target_branch.clone(),
        operation.expected_target_head.clone(),
        source.provenance.source_branch.clone(),
        source
            .expected_source_commit
            .clone()
            .expect("committed source has a commit"),
        source.provenance.common_git_identity.clone(),
        source.provenance.worktree_admin_identity.clone(),
        source.provenance.fixed_lock_reason.clone(),
        source.provenance.config_attributes_digest.clone(),
        0,
        0,
        0,
        0,
        None,
        MergeAutostashObservation::Absent,
        OtherGitOperationObservation::Clear,
    )
    .expect("valid abort applied proof")
}

fn snapshot_operation(
    snapshot: &DeliveryEligibilitySnapshot,
) -> Result<&MergeOperationRecord, DeliveryLiveRuntimeError> {
    snapshot
        .ownership
        .merge_operations
        .iter()
        .max_by_key(|operation| operation.version)
        .ok_or(DeliveryLiveRuntimeError::Unavailable)
}

fn snapshot_common_identity(
    snapshot: &DeliveryEligibilitySnapshot,
) -> Result<DirectoryIdentity, DeliveryLiveRuntimeError> {
    Ok(snapshot_operation(snapshot)?
        .provenance
        .common_git_identity
        .clone())
}

fn snapshot_admin_identity(
    snapshot: &DeliveryEligibilitySnapshot,
) -> Result<DirectoryIdentity, DeliveryLiveRuntimeError> {
    Ok(snapshot_operation(snapshot)?
        .provenance
        .worktree_admin_identity
        .clone())
}

fn snapshot_source_config(
    snapshot: &DeliveryEligibilitySnapshot,
) -> Result<Sha256Digest, DeliveryLiveRuntimeError> {
    Ok(snapshot_operation(snapshot)?
        .provenance
        .config_attributes_digest
        .clone())
}

fn snapshot_target_branch(
    snapshot: &DeliveryEligibilitySnapshot,
) -> Result<coding_agent_store::GitBranchRef, DeliveryLiveRuntimeError> {
    Ok(snapshot_operation(snapshot)?.target_branch.clone())
}

fn snapshot_target_head(
    snapshot: &DeliveryEligibilitySnapshot,
) -> Result<GitCommitOid, DeliveryLiveRuntimeError> {
    Ok(snapshot_operation(snapshot)?.expected_target_head.clone())
}

fn snapshot_target_config(
    snapshot: &DeliveryEligibilitySnapshot,
) -> Result<Sha256Digest, DeliveryLiveRuntimeError> {
    Ok(snapshot_operation(snapshot)?
        .target_config_attributes_digest
        .clone())
}

fn snapshot_target_security(
    snapshot: &DeliveryEligibilitySnapshot,
) -> Result<Sha256Digest, DeliveryLiveRuntimeError> {
    Ok(snapshot_operation(snapshot)?.target_security_digest.clone())
}

pub fn assert_merge_snapshot(snapshot: DeliveryOperationSnapshot) -> MergeOperationRecord {
    match snapshot {
        DeliveryOperationSnapshot::Merge(operation) => *operation,
        DeliveryOperationSnapshot::Cleanup(_) => panic!("merge operation resolved as cleanup"),
    }
}
