#![cfg(feature = "test-support")]

mod support;

use std::str::FromStr;
use std::sync::Arc;

use coding_agent_app::{
    DeliveryCleanupWriteCommand, DeliveryCleanupWriteOutcome, DeliveryDisposition,
    DeliveryMergeWriteCommand, DeliveryMergeWriteOutcome, DeliverySourceWriteCommand,
    DeliverySourceWriteOutcome, DeliveryWriteCommand, DeliveryWriteOutcome, KnownNotAppliedReason,
    OutcomeUnknownReason, StoreWriterFaultPoint, StoreWriterFaultSpec, StoreWriterHandle,
    StoreWriterOperationKind, StoreWriterTestController,
};
use coding_agent_domain::{CanonicalPath, ClientRequestId, Task, TaskEventPayload, TaskStatus};
use coding_agent_store::{
    AcceptMergeCommandRequest, AcceptMergeOutcome, AdvanceDeliverySourceObjectRequest,
    AttemptArtifactIdentity, BeginMergeAbortRequest, BindMergePreflightInputsRequest,
    BranchCleanupKnownNotAppliedReason, CleanupAcceptanceOutcome, CleanupOperationAnchor,
    CleanupOperationState, CleanupReconciliationReason, CleanupTransitionOutcome,
    CommitDeliverySourceRequest, CompleteBranchCleanupRequest, CompleteMergeAbortRequest,
    CompleteMergeRequest, CompleteWorktreeCleanupRequest, CreateDeliverySourceOutcome,
    CreateDeliverySourceRequest, CreatePreflightOutcome, CreatePreflightRequest,
    DeleteBranchCommandRequest, DeliveryCommandReceipt, DeliveryOperationId, DeliverySourceAnchor,
    DeliverySourceAppliedProof, DeliverySourceObjectProof, DeliverySourceReconciliationReason,
    DeliverySourceRecord, DeliverySourceRetryReason, DeliverySourceState,
    DeliverySourceTransitionOutcome, DeliveryVersion, DirectoryIdentity, EnterMergePendingRequest,
    EnterWorktreeRemovePendingRequest, FailUnboundMergePreflightRequest,
    FinalizeReviewedTaskOutcome, GitBranchRef, GitCommitOid, GitTreeOid, MarkPreflightStaleOutcome,
    MarkPreflightStaleRequest, MergeAbortAppliedProof, MergeAbortProof, MergeAppliedProof,
    MergeAutostashObservation, MergeCommitObjectProof, MergeConflictPaths,
    MergeKnownNotAppliedReason, MergeOperationRecord, MergeOperationState, MergePreflightResult,
    MergeReconciliationReason, MergeTransitionOutcome, OtherGitOperationObservation,
    PreflightCommandRequest, PreflightStaleReason, ReconcileBranchCleanupRequest,
    ReconcileDeliverySourceOutcome, ReconcileDeliverySourceRequest, ReconcileMergeRequest,
    ReconcileWorktreeCleanupRequest, RecordBranchCleanupFailureRequest,
    RecordDeliverySourceRetryRequest, RecordMergeKnownFailureRequest,
    RecordMergePreflightResultRequest, RecordWorktreeCleanupFailureRequest,
    RecordWorktreeUnlockedRequest, RefreshBranchCleanupTargetRequest, RemoveWorktreeCommandRequest,
    ReserveAttemptArtifact, Sha256Digest, SourceWorktreeProof, Store, TaskTransition,
    TransitionOutcome, UnboundMergePreflightFailure, WorktreeCleanupKnownNotAppliedReason,
};
use tokio::time::{Duration, Instant};

const BASE_COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
const CANDIDATE_TREE: &str = "123456789abcdef0123456789abcdef012345678";
const TARGET_HEAD: &str = "23456789abcdef0123456789abcdef0123456789";
const PREFLIGHT_SOURCE: &str = "3456789abcdef0123456789abcdef0123456789a";
const MERGE_BASE: &str = "456789abcdef0123456789abcdef0123456789ab";
const MERGE_TREE: &str = "56789abcdef0123456789abcdef0123456789abc";
const SOURCE_COMMIT: &str = "6789abcdef0123456789abcdef0123456789abcd";
const MERGE_COMMIT: &str = "789abcdef0123456789abcdef0123456789abcde";
const COMMON_IDENTITY: &str = "c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1";
const ADMIN_IDENTITY: &str = "d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2";
const CONFIG_DIGEST: &str = "e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3";
const TARGET_CONFIG_DIGEST: &str =
    "f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4";
const TARGET_SECURITY_DIGEST: &str =
    "a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5";
const INDEX_STAGES_DIGEST: &str =
    "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";
const WORKTREE_DIGEST: &str = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2";

struct EligibleMergeFixture {
    fixture: support::StoreFixture,
    task: Task,
}

impl EligibleMergeFixture {
    fn store(&self) -> &Store {
        &self.fixture.store
    }

    fn create_preflight_request(
        &self,
        client_request_id: ClientRequestId,
    ) -> CreatePreflightRequest {
        CreatePreflightRequest::try_new(
            PreflightCommandRequest::try_new(
                client_request_id,
                self.task.id,
                GitBranchRef::from_str("refs/heads/main").unwrap(),
                GitCommitOid::from_str(TARGET_HEAD).unwrap(),
            )
            .unwrap(),
            DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
            DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
            Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
            Sha256Digest::from_str(TARGET_CONFIG_DIGEST).unwrap(),
            Sha256Digest::from_str(TARGET_SECURITY_DIGEST).unwrap(),
        )
        .unwrap()
    }

    fn bind_preflight_inputs_request(
        &self,
        operation_id: DeliveryOperationId,
    ) -> BindMergePreflightInputsRequest {
        BindMergePreflightInputsRequest::try_new(
            self.task.id,
            operation_id,
            DeliveryVersion::initial(),
            GitTreeOid::from_str(CANDIDATE_TREE).unwrap(),
            GitCommitOid::from_str(PREFLIGHT_SOURCE).unwrap(),
        )
        .unwrap()
    }

    async fn accept_request(
        &self,
        operation_id: DeliveryOperationId,
        expected_version: DeliveryVersion,
        client_request_id: ClientRequestId,
    ) -> AcceptMergeCommandRequest {
        let evidence = self
            .store()
            .delivery_eligibility_snapshot(self.task.id)
            .await
            .unwrap()
            .unwrap()
            .evidence_identity
            .unwrap();
        AcceptMergeCommandRequest::try_new(
            client_request_id,
            self.task.id,
            operation_id,
            expected_version,
            evidence.workspace_generation(),
            evidence.workspace_fingerprint().clone(),
            GitBranchRef::from_str("refs/heads/main").unwrap(),
            GitCommitOid::from_str(TARGET_HEAD).unwrap(),
        )
        .unwrap()
    }
}

async fn eligible_merge_fixture() -> EligibleMergeFixture {
    let fixture = support::store_fixture().await;
    let task = approved_task(&fixture).await;
    EligibleMergeFixture { fixture, task }
}

struct AcceptedSourceFixture {
    fixture: support::StoreFixture,
    accept_command: AcceptMergeCommandRequest,
}

impl AcceptedSourceFixture {
    fn store(&self) -> &Store {
        &self.fixture.store
    }

    fn create_command(&self) -> DeliveryWriteCommand {
        DeliveryWriteCommand::Source(DeliverySourceWriteCommand::Create(
            CreateDeliverySourceRequest::try_new(self.accept_command.clone()).unwrap(),
        ))
    }

    fn source_anchor(&self) -> DeliverySourceAnchor {
        DeliverySourceAnchor::try_new(
            self.accept_command.task_id(),
            self.accept_command.preflight_operation_id(),
            DeliveryVersion::try_new(4).unwrap(),
        )
        .unwrap()
    }
}

async fn accepted_source_fixture() -> AcceptedSourceFixture {
    let fixture = support::store_fixture().await;
    let task = approved_task(&fixture).await;
    let evidence = fixture
        .store
        .delivery_eligibility_snapshot(task.id)
        .await
        .unwrap()
        .unwrap()
        .evidence_identity
        .unwrap();
    let preflight = CreatePreflightRequest::try_new(
        PreflightCommandRequest::try_new(
            ClientRequestId::new(),
            task.id,
            GitBranchRef::from_str("refs/heads/main").unwrap(),
            GitCommitOid::from_str(TARGET_HEAD).unwrap(),
        )
        .unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", COMMON_IDENTITY).unwrap(),
        DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY).unwrap(),
        Sha256Digest::from_str(CONFIG_DIGEST).unwrap(),
        Sha256Digest::from_str(TARGET_CONFIG_DIGEST).unwrap(),
        Sha256Digest::from_str(TARGET_SECURITY_DIGEST).unwrap(),
    )
    .unwrap();
    let operation_id = match fixture
        .store
        .create_merge_preflight(preflight)
        .await
        .unwrap()
    {
        CreatePreflightOutcome::Created(receipt) => receipt.operation_id,
        other => panic!("fixture preflight must be created, got {other:?}"),
    };
    assert!(matches!(
        fixture
            .store
            .bind_merge_preflight_inputs(
                BindMergePreflightInputsRequest::try_new(
                    task.id,
                    operation_id,
                    DeliveryVersion::initial(),
                    GitTreeOid::from_str(CANDIDATE_TREE).unwrap(),
                    GitCommitOid::from_str(PREFLIGHT_SOURCE).unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        MergeTransitionOutcome::Applied(_)
    ));
    let ready = MergePreflightResult::ready(
        GitCommitOid::from_str(MERGE_BASE).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
    )
    .unwrap();
    fixture
        .store
        .record_merge_preflight_result(
            RecordMergePreflightResultRequest::try_new(
                task.id,
                operation_id,
                DeliveryVersion::try_new(2).unwrap(),
                ready,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let accept_command = AcceptMergeCommandRequest::try_new(
        ClientRequestId::new(),
        task.id,
        operation_id,
        DeliveryVersion::try_new(3).unwrap(),
        evidence.workspace_generation(),
        evidence.workspace_fingerprint().clone(),
        GitBranchRef::from_str("refs/heads/main").unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        fixture
            .store
            .accept_merge(accept_command.clone())
            .await
            .unwrap(),
        AcceptMergeOutcome::Accepted(_)
    ));
    AcceptedSourceFixture {
        fixture,
        accept_command,
    }
}

async fn approved_task(fixture: &support::StoreFixture) -> Task {
    let queued = fixture
        .store
        .create_task(support::new_task(
            fixture.repository.id,
            "delivery StoreWriter source fixture",
        ))
        .await
        .unwrap()
        .task()
        .clone();
    let running = match fixture
        .store
        .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .unwrap()
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
        .unwrap();
    let running = fixture
        .store
        .task_detail(running.id)
        .await
        .unwrap()
        .unwrap()
        .task;
    let artifact_identity = AttemptArtifactIdentity {
        task_id: running.id,
        repository_id: running.repository_id,
        attempt: running.attempt,
    };
    fixture
        .store
        .reserve_attempt_artifact(ReserveAttemptArtifact {
            identity: artifact_identity,
            base_commit: BASE_COMMIT.to_owned(),
            branch_name: "codex/delivery-store-writer".to_owned(),
            worktree_path: CanonicalPath::try_from_canonical(
                fixture
                    .repository
                    .git_root
                    .as_path()
                    .join("artifacts")
                    .join(running.id.to_string()),
            )
            .unwrap(),
        })
        .await
        .unwrap();
    fixture
        .store
        .mark_attempt_artifact_ready(artifact_identity)
        .await
        .unwrap();
    match fixture
        .store
        .finalize_reviewed_task(
            running.id,
            running.repository_id,
            running.attempt,
            support::approved_review(),
        )
        .await
        .unwrap()
    {
        FinalizeReviewedTaskOutcome::Applied { task, .. }
        | FinalizeReviewedTaskOutcome::Existing { task, .. } => task,
    }
}

async fn confirmed(
    writer: &StoreWriterHandle,
    command: DeliveryWriteCommand,
) -> DeliveryWriteOutcome {
    let expected_key = command.mutation_key();
    let completion = writer
        .submit_delivery(command, support::deadline())
        .completion()
        .await;
    assert_eq!(completion.identity.mutation_key(), &expected_key);
    let outcome = match completion.disposition {
        DeliveryDisposition::Confirmed(outcome) => outcome,
        other => panic!("delivery write must be confirmed, got {other:?}"),
    };
    assert_eq!(outcome.kind(), expected_key.kind());
    outcome
}

fn object_proof(source: &coding_agent_store::DeliverySourceRecord) -> DeliverySourceObjectProof {
    DeliverySourceObjectProof::try_new(
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        source.candidate_tree.clone(),
        vec![source.expected_parent.clone()],
        source.commit_metadata.clone(),
    )
    .unwrap()
}

fn applied_source_proof(
    source: &DeliverySourceRecord,
    object: DeliverySourceObjectProof,
) -> DeliverySourceAppliedProof {
    let worktree = SourceWorktreeProof::try_new(
        source.candidate_tree.clone(),
        source.candidate_tree.clone(),
        0,
        0,
        0,
        0,
    )
    .unwrap();
    let source_commit = GitCommitOid::from_str(SOURCE_COMMIT).unwrap();
    DeliverySourceAppliedProof::try_new(
        object,
        source.provenance.source_branch.clone(),
        source_commit.clone(),
        source_commit,
        worktree,
        source.provenance.common_git_identity.clone(),
        source.provenance.worktree_admin_identity.clone(),
        source.provenance.fixed_lock_reason.clone(),
        source.provenance.config_attributes_digest.clone(),
    )
    .unwrap()
}

async fn committed_source_with_writer(
    writer: &StoreWriterHandle,
    fixture: &AcceptedSourceFixture,
) -> DeliverySourceRecord {
    let created = match confirmed(writer, fixture.create_command()).await {
        DeliveryWriteOutcome::Source(DeliverySourceWriteOutcome::Create(
            CreateDeliverySourceOutcome::Created(source),
        )) => source,
        other => panic!("source create must apply, got {other:?}"),
    };
    let object = object_proof(&created);
    let advance = AdvanceDeliverySourceObjectRequest::try_new(
        fixture.source_anchor(),
        created.version,
        object.clone(),
    )
    .unwrap();
    assert!(matches!(
        confirmed(
            writer,
            DeliveryWriteCommand::Source(DeliverySourceWriteCommand::AdvanceObject(advance)),
        )
        .await,
        DeliveryWriteOutcome::Source(DeliverySourceWriteOutcome::AdvanceObject(
            DeliverySourceTransitionOutcome::Applied(_)
        ))
    ));
    let pending = fixture
        .store()
        .delivery_ownership_snapshot(fixture.accept_command.task_id())
        .await
        .unwrap()
        .unwrap()
        .source
        .unwrap();
    let commit = CommitDeliverySourceRequest::try_new(
        fixture.source_anchor(),
        pending.version,
        applied_source_proof(&pending, object),
    )
    .unwrap();
    assert!(matches!(
        confirmed(
            writer,
            DeliveryWriteCommand::Source(DeliverySourceWriteCommand::Commit(commit)),
        )
        .await,
        DeliveryWriteOutcome::Source(DeliverySourceWriteOutcome::Commit(
            DeliverySourceTransitionOutcome::Applied(_)
        ))
    ));
    fixture
        .store()
        .delivery_ownership_snapshot(fixture.accept_command.task_id())
        .await
        .unwrap()
        .unwrap()
        .source
        .unwrap()
}

fn merge_command(command: DeliveryMergeWriteCommand) -> DeliveryWriteCommand {
    DeliveryWriteCommand::Merge(command)
}

async fn confirmed_merge(
    writer: &StoreWriterHandle,
    command: DeliveryMergeWriteCommand,
) -> DeliveryMergeWriteOutcome {
    match confirmed(writer, merge_command(command)).await {
        DeliveryWriteOutcome::Merge(outcome) => outcome,
        other => panic!("delivery merge route returned {other:?}"),
    }
}

fn ready_preflight_request(
    task_id: coding_agent_domain::TaskId,
    operation_id: DeliveryOperationId,
) -> RecordMergePreflightResultRequest {
    RecordMergePreflightResultRequest::try_new(
        task_id,
        operation_id,
        DeliveryVersion::try_new(2).unwrap(),
        MergePreflightResult::ready(
            GitCommitOid::from_str(MERGE_BASE).unwrap(),
            GitTreeOid::from_str(MERGE_TREE).unwrap(),
        )
        .unwrap(),
    )
    .unwrap()
}

async fn merge_operation(
    store: &Store,
    task_id: coding_agent_domain::TaskId,
    operation_id: DeliveryOperationId,
) -> MergeOperationRecord {
    store
        .delivery_ownership_snapshot(task_id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .unwrap()
}

fn merge_object_proof(operation: &MergeOperationRecord) -> MergeCommitObjectProof {
    MergeCommitObjectProof::try_new(
        GitCommitOid::from_str(MERGE_COMMIT).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
        vec![
            GitCommitOid::from_str(TARGET_HEAD).unwrap(),
            GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        ],
        operation.merge_metadata.clone().unwrap(),
    )
    .unwrap()
}

fn merge_applied_proof(
    operation: &MergeOperationRecord,
    source: &DeliverySourceRecord,
) -> MergeAppliedProof {
    MergeAppliedProof::try_new(
        merge_object_proof(operation),
        GitBranchRef::from_str("refs/heads/main").unwrap(),
        GitCommitOid::from_str(MERGE_COMMIT).unwrap(),
        source.provenance.source_branch.clone(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        source.provenance.common_git_identity.clone(),
        source.provenance.worktree_admin_identity.clone(),
        source.provenance.fixed_lock_reason.clone(),
        source.provenance.config_attributes_digest.clone(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
        GitTreeOid::from_str(MERGE_TREE).unwrap(),
        0,
        0,
        0,
        0,
        None,
        MergeAutostashObservation::Absent,
        OtherGitOperationObservation::Clear,
    )
    .unwrap()
}

fn abort_proof(source: &DeliverySourceRecord) -> MergeAbortProof {
    MergeAbortProof::try_new(
        uuid::Uuid::new_v4(),
        GitBranchRef::from_str("refs/heads/main").unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
        source.provenance.source_branch.clone(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        source.provenance.common_git_identity.clone(),
        source.provenance.worktree_admin_identity.clone(),
        source.provenance.fixed_lock_reason.clone(),
        source.provenance.config_attributes_digest.clone(),
        Sha256Digest::from_str(INDEX_STAGES_DIGEST).unwrap(),
        Sha256Digest::from_str(WORKTREE_DIGEST).unwrap(),
        MergeAutostashObservation::Absent,
        OtherGitOperationObservation::Clear,
        MergeConflictPaths::try_from_raw(vec![b"src/conflicted.rs".to_vec()]).unwrap(),
    )
    .unwrap()
}

fn abort_applied_proof(source: &DeliverySourceRecord) -> MergeAbortAppliedProof {
    MergeAbortAppliedProof::try_new(
        GitBranchRef::from_str("refs/heads/main").unwrap(),
        GitCommitOid::from_str(TARGET_HEAD).unwrap(),
        source.provenance.source_branch.clone(),
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
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
    .unwrap()
}

async fn source_journal_count(store: &Store, task_id: coding_agent_domain::TaskId) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'delivery_source' AND entity_id = ?",
    )
    .bind(task_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap()
}

async fn merge_journal_count(store: &Store, operation_id: DeliveryOperationId) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'merge_operation' AND entity_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap()
}

fn cleanup_command(command: DeliveryCleanupWriteCommand) -> DeliveryWriteCommand {
    DeliveryWriteCommand::Cleanup(command)
}

async fn confirmed_cleanup(
    writer: &StoreWriterHandle,
    command: DeliveryCleanupWriteCommand,
) -> DeliveryCleanupWriteOutcome {
    match confirmed(writer, cleanup_command(command)).await {
        DeliveryWriteOutcome::Cleanup(outcome) => outcome,
        other => panic!("delivery cleanup route returned {other:?}"),
    }
}

async fn merged_cleanup_with_writer(
    writer: &StoreWriterHandle,
    fixture: &AcceptedSourceFixture,
) -> DeliverySourceRecord {
    let source = committed_source_with_writer(writer, fixture).await;
    let task_id = fixture.accept_command.task_id();
    let operation_id = fixture.accept_command.preflight_operation_id();
    let accepted = merge_operation(fixture.store(), task_id, operation_id).await;
    let pending_version = match confirmed_merge(
        writer,
        DeliveryMergeWriteCommand::EnterPending(
            EnterMergePendingRequest::try_new(
                task_id,
                operation_id,
                accepted.version,
                merge_object_proof(&accepted),
            )
            .unwrap(),
        ),
    )
    .await
    {
        DeliveryMergeWriteOutcome::EnterPending(MergeTransitionOutcome::Applied(receipt)) => {
            receipt.version
        }
        other => panic!("cleanup fixture merge intent must apply, got {other:?}"),
    };
    let pending = merge_operation(fixture.store(), task_id, operation_id).await;
    assert!(matches!(
        confirmed_merge(
            writer,
            DeliveryMergeWriteCommand::Complete(
                CompleteMergeRequest::try_new(
                    task_id,
                    operation_id,
                    pending_version,
                    merge_applied_proof(&pending, &source),
                )
                .unwrap(),
            ),
        )
        .await,
        DeliveryMergeWriteOutcome::Complete(MergeTransitionOutcome::Applied(_))
    ));
    source
}

async fn remove_worktree_request(
    store: &Store,
    task_id: coding_agent_domain::TaskId,
    client_request_id: ClientRequestId,
) -> RemoveWorktreeCommandRequest {
    let snapshot = store
        .delivery_ownership_snapshot(task_id)
        .await
        .unwrap()
        .unwrap();
    let source = snapshot.source.as_ref().unwrap();
    let disposition = snapshot.disposition.as_ref().unwrap();
    RemoveWorktreeCommandRequest::try_new(
        client_request_id,
        task_id,
        disposition.worktree_version,
        disposition.merged_operation_id,
        source.provenance.source_branch.clone(),
        source.expected_source_commit.clone().unwrap(),
    )
    .unwrap()
}

async fn delete_branch_request(
    store: &Store,
    task_id: coding_agent_domain::TaskId,
    client_request_id: ClientRequestId,
    target_head: GitCommitOid,
) -> DeleteBranchCommandRequest {
    let snapshot = store
        .delivery_ownership_snapshot(task_id)
        .await
        .unwrap()
        .unwrap();
    let source = snapshot.source.as_ref().unwrap();
    let disposition = snapshot.disposition.as_ref().unwrap();
    let merged = snapshot
        .merge_operations
        .iter()
        .find(|operation| operation.operation_id == disposition.merged_operation_id)
        .unwrap();
    DeleteBranchCommandRequest::try_new(
        client_request_id,
        task_id,
        disposition.branch_version,
        disposition.merged_operation_id,
        source.provenance.source_branch.clone(),
        source.expected_source_commit.clone().unwrap(),
        merged.target_branch.clone(),
        target_head,
    )
    .unwrap()
}

async fn accepted_worktree_cleanup(
    writer: &StoreWriterHandle,
    request: RemoveWorktreeCommandRequest,
) -> DeliveryCommandReceipt {
    match confirmed_cleanup(writer, DeliveryCleanupWriteCommand::AcceptWorktree(request)).await {
        DeliveryCleanupWriteOutcome::AcceptWorktree(CleanupAcceptanceOutcome::Accepted(
            receipt,
        )) => receipt,
        other => panic!("worktree cleanup must be accepted, got {other:?}"),
    }
}

async fn finish_worktree_cleanup(
    writer: &StoreWriterHandle,
    task_id: coding_agent_domain::TaskId,
    accepted: &DeliveryCommandReceipt,
) {
    let unlocked = match confirmed_cleanup(
        writer,
        DeliveryCleanupWriteCommand::RecordWorktreeUnlocked(
            RecordWorktreeUnlockedRequest::try_new(
                CleanupOperationAnchor::try_new(
                    task_id,
                    accepted.operation_id,
                    accepted.accepted_operation_version,
                )
                .unwrap(),
            )
            .unwrap(),
        ),
    )
    .await
    {
        DeliveryCleanupWriteOutcome::RecordWorktreeUnlocked(CleanupTransitionOutcome::Applied(
            receipt,
        )) => receipt,
        other => panic!("worktree unlock must apply, got {other:?}"),
    };
    let pending = match confirmed_cleanup(
        writer,
        DeliveryCleanupWriteCommand::EnterWorktreeRemovePending(
            EnterWorktreeRemovePendingRequest::try_new(
                CleanupOperationAnchor::try_new(task_id, accepted.operation_id, unlocked.version)
                    .unwrap(),
            )
            .unwrap(),
        ),
    )
    .await
    {
        DeliveryCleanupWriteOutcome::EnterWorktreeRemovePending(
            CleanupTransitionOutcome::Applied(receipt),
        ) => receipt,
        other => panic!("worktree remove intent must apply, got {other:?}"),
    };
    assert!(matches!(
        confirmed_cleanup(
            writer,
            DeliveryCleanupWriteCommand::CompleteWorktree(
                CompleteWorktreeCleanupRequest::try_new(
                    CleanupOperationAnchor::try_new(
                        task_id,
                        accepted.operation_id,
                        pending.version,
                    )
                    .unwrap(),
                )
                .unwrap(),
            ),
        )
        .await,
        DeliveryCleanupWriteOutcome::CompleteWorktree(CleanupTransitionOutcome::Applied(_))
    ));
}

async fn accepted_branch_cleanup(
    writer: &StoreWriterHandle,
    request: DeleteBranchCommandRequest,
) -> DeliveryCommandReceipt {
    match confirmed_cleanup(writer, DeliveryCleanupWriteCommand::AcceptBranch(request)).await {
        DeliveryCleanupWriteOutcome::AcceptBranch(CleanupAcceptanceOutcome::Accepted(receipt)) => {
            receipt
        }
        other => panic!("branch cleanup must be accepted, got {other:?}"),
    }
}

async fn cleanup_journal_count(store: &Store, operation_id: DeliveryOperationId) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_delivery_operation_transitions \
         WHERE entity_kind = 'cleanup_operation' AND entity_id = ?",
    )
    .bind(operation_id.to_string())
    .fetch_one(store.pool())
    .await
    .unwrap()
}

async fn branch_cleanup_ready_request(
    writer: &StoreWriterHandle,
    fixture: &AcceptedSourceFixture,
    client_request_id: ClientRequestId,
    target_head: GitCommitOid,
) -> DeleteBranchCommandRequest {
    merged_cleanup_with_writer(writer, fixture).await;
    let task_id = fixture.accept_command.task_id();
    let remove = remove_worktree_request(fixture.store(), task_id, ClientRequestId::new()).await;
    let accepted = accepted_worktree_cleanup(writer, remove).await;
    finish_worktree_cleanup(writer, task_id, &accepted).await;
    delete_branch_request(fixture.store(), task_id, client_request_id, target_head).await
}

#[tokio::test]
async fn source_five_mutations_dispatch_exactly_and_never_wake_task_events() {
    let fixture = accepted_source_fixture().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::DropWakeAfterCommit,
            operation: Some(StoreWriterOperationKind::CreateDeliverySource),
            count: 1,
        }])
        .unwrap(),
    );
    let wake = Arc::new(support::CountingWake::default());
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store().clone(),
        wake.clone(),
        8,
        controller.clone(),
    );

    let created = match confirmed(&writer, fixture.create_command()).await {
        DeliveryWriteOutcome::Source(DeliverySourceWriteOutcome::Create(
            CreateDeliverySourceOutcome::Created(source),
        )) => source,
        other => panic!("source create must apply, got {other:?}"),
    };
    let anchor = fixture.source_anchor();
    let retry = RecordDeliverySourceRetryRequest::try_new(
        anchor,
        DeliverySourceState::ObjectPending,
        DeliveryVersion::initial(),
        DeliverySourceRetryReason::CommandTimedOut,
    )
    .unwrap();
    assert!(matches!(
        confirmed(
            &writer,
            DeliveryWriteCommand::Source(DeliverySourceWriteCommand::RecordRetry(retry)),
        )
        .await,
        DeliveryWriteOutcome::Source(DeliverySourceWriteOutcome::RecordRetry(
            DeliverySourceTransitionOutcome::Applied(_)
        ))
    ));

    let object = object_proof(&created);
    let advance = AdvanceDeliverySourceObjectRequest::try_new(
        anchor,
        DeliveryVersion::try_new(2).unwrap(),
        object.clone(),
    )
    .unwrap();
    assert!(matches!(
        confirmed(
            &writer,
            DeliveryWriteCommand::Source(DeliverySourceWriteCommand::AdvanceObject(advance)),
        )
        .await,
        DeliveryWriteOutcome::Source(DeliverySourceWriteOutcome::AdvanceObject(
            DeliverySourceTransitionOutcome::Applied(_)
        ))
    ));

    let commit_pending = fixture
        .store()
        .delivery_ownership_snapshot(fixture.accept_command.task_id())
        .await
        .unwrap()
        .unwrap()
        .source
        .unwrap();
    let worktree = SourceWorktreeProof::try_new(
        commit_pending.candidate_tree.clone(),
        commit_pending.candidate_tree.clone(),
        0,
        0,
        0,
        0,
    )
    .unwrap();
    let source_commit = GitCommitOid::from_str(SOURCE_COMMIT).unwrap();
    let applied = DeliverySourceAppliedProof::try_new(
        object,
        commit_pending.provenance.source_branch.clone(),
        source_commit.clone(),
        source_commit,
        worktree,
        commit_pending.provenance.common_git_identity.clone(),
        commit_pending.provenance.worktree_admin_identity.clone(),
        commit_pending.provenance.fixed_lock_reason.clone(),
        commit_pending.provenance.config_attributes_digest.clone(),
    )
    .unwrap();
    let commit =
        CommitDeliverySourceRequest::try_new(anchor, commit_pending.version, applied).unwrap();
    assert!(matches!(
        confirmed(
            &writer,
            DeliveryWriteCommand::Source(DeliverySourceWriteCommand::Commit(commit)),
        )
        .await,
        DeliveryWriteOutcome::Source(DeliverySourceWriteOutcome::Commit(
            DeliverySourceTransitionOutcome::Applied(_)
        ))
    ));

    let committed_ownership = fixture
        .store()
        .delivery_ownership_snapshot(fixture.accept_command.task_id())
        .await
        .unwrap()
        .unwrap();
    let committed = committed_ownership.source.unwrap();
    let current_merge_version = committed_ownership
        .merge_operations
        .iter()
        .find(|operation| operation.operation_id == fixture.accept_command.preflight_operation_id())
        .expect("accepted merge operation must remain current")
        .version;
    let reconcile = ReconcileDeliverySourceRequest::try_new(
        anchor,
        DeliverySourceState::Committed,
        committed.version,
        current_merge_version,
        DeliverySourceReconciliationReason::SourceInconsistent,
    )
    .unwrap();
    assert!(matches!(
        confirmed(
            &writer,
            DeliveryWriteCommand::Source(DeliverySourceWriteCommand::Reconcile(reconcile)),
        )
        .await,
        DeliveryWriteOutcome::Source(DeliverySourceWriteOutcome::Reconcile(
            ReconcileDeliverySourceOutcome::Applied(_)
        ))
    ));
    assert_eq!(wake.count(), 0);
    assert_eq!(
        controller.hit_count(
            StoreWriterFaultPoint::DropWakeAfterCommit,
            StoreWriterOperationKind::CreateDeliverySource,
        ),
        0
    );
}

#[tokio::test]
async fn expired_before_execution_is_known_not_applied_and_writes_nothing() {
    let fixture = accepted_source_fixture().await;
    let wake = Arc::new(support::CountingWake::default());
    let writer = StoreWriterHandle::spawn(fixture.store().clone(), wake.clone(), 4);
    let completion = writer
        .submit_delivery(fixture.create_command(), Instant::now())
        .completion()
        .await;
    assert!(matches!(
        completion.disposition,
        DeliveryDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::DeadlineBeforeStart,
            outcome: None,
            error: None,
        }
    ));
    assert!(
        fixture
            .store()
            .delivery_ownership_snapshot(fixture.accept_command.task_id())
            .await
            .unwrap()
            .unwrap()
            .source
            .is_none()
    );
    assert_eq!(wake.count(), 0);
}

#[tokio::test]
async fn closed_ingress_is_known_not_applied_before_enqueue() {
    let fixture = accepted_source_fixture().await;
    let completion = StoreWriterHandle::closed_for_test()
        .submit_delivery(fixture.create_command(), support::deadline())
        .completion()
        .await;
    assert!(matches!(
        completion.disposition,
        DeliveryDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::IngressClosed,
            outcome: None,
            error: None,
        }
    ));
    assert!(
        fixture
            .store()
            .delivery_ownership_snapshot(fixture.accept_command.task_id())
            .await
            .unwrap()
            .unwrap()
            .source
            .is_none()
    );
}

#[tokio::test]
async fn exhausted_busy_window_is_known_rollback_and_same_command_can_retry() {
    let fixture = accepted_source_fixture().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::BusyBeforeExecute,
            operation: Some(StoreWriterOperationKind::CreateDeliverySource),
            count: 6,
        }])
        .unwrap(),
    );
    let wake = Arc::new(support::CountingWake::default());
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store().clone(),
        wake.clone(),
        4,
        controller,
    );
    let command = fixture.create_command();
    let completion = writer
        .submit_delivery(command.clone(), support::deadline())
        .completion()
        .await;
    assert!(matches!(
        completion.disposition,
        DeliveryDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::BusyRolledBack,
            outcome: None,
            error: None,
        }
    ));
    assert!(
        fixture
            .store()
            .delivery_ownership_snapshot(fixture.accept_command.task_id())
            .await
            .unwrap()
            .unwrap()
            .source
            .is_none()
    );
    assert!(matches!(
        confirmed(&writer, command).await,
        DeliveryWriteOutcome::Source(DeliverySourceWriteOutcome::Create(
            CreateDeliverySourceOutcome::Created(_)
        ))
    ));
    assert_eq!(wake.count(), 0);
}

#[tokio::test]
async fn repeated_reply_loss_stays_unknown_then_query_first_reconciliation_is_existing() {
    let fixture = accepted_source_fixture().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            operation: Some(StoreWriterOperationKind::CreateDeliverySource),
            count: 2,
        }])
        .unwrap(),
    );
    let wake = Arc::new(support::CountingWake::default());
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store().clone(),
        wake.clone(),
        4,
        controller,
    );
    let unknown = writer
        .submit_delivery(fixture.create_command(), support::deadline())
        .completion()
        .await;
    let command = match unknown.disposition {
        DeliveryDisposition::OutcomeUnknown {
            reason: OutcomeUnknownReason::CommitStatusUnknown,
            command,
        } => command,
        other => panic!("two lost replies must remain unknown, got {other:?}"),
    };
    assert_eq!(
        source_journal_count(fixture.store(), fixture.accept_command.task_id()).await,
        1
    );

    let reconciled = writer
        .reconcile_delivery(command, support::deadline())
        .completion()
        .await;
    assert!(matches!(
        reconciled.disposition,
        DeliveryDisposition::Confirmed(DeliveryWriteOutcome::Source(
            DeliverySourceWriteOutcome::Create(CreateDeliverySourceOutcome::Existing(_))
        ))
    ));
    assert_eq!(
        source_journal_count(fixture.store(), fixture.accept_command.task_id()).await,
        1
    );
    assert_eq!(wake.count(), 0);
}

#[tokio::test]
async fn delivery_commit_pause_observes_durable_state_without_event_wake() {
    let fixture = accepted_source_fixture().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::PauseAfterCommitBeforeWake,
            operation: Some(StoreWriterOperationKind::CreateDeliverySource),
            count: 1,
        }])
        .unwrap(),
    );
    let wake = Arc::new(support::CountingWake::default());
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store().clone(),
        wake.clone(),
        4,
        controller.clone(),
    );
    let completion = tokio::spawn(
        writer
            .submit_delivery(fixture.create_command(), support::deadline())
            .completion(),
    );
    tokio::time::timeout(
        Duration::from_secs(2),
        controller.wait_until_reached(StoreWriterFaultPoint::PauseAfterCommitBeforeWake, 1),
    )
    .await
    .expect("delivery commit reaches the durable-state pause");
    assert_eq!(
        source_journal_count(fixture.store(), fixture.accept_command.task_id()).await,
        1
    );
    assert_eq!(wake.count(), 0);
    assert_eq!(
        controller.release(StoreWriterFaultPoint::PauseAfterCommitBeforeWake),
        1
    );
    assert!(matches!(
        completion.await.unwrap().disposition,
        DeliveryDisposition::Confirmed(DeliveryWriteOutcome::Source(
            DeliverySourceWriteOutcome::Create(CreateDeliverySourceOutcome::Created(_))
        ))
    ));
    assert_eq!(wake.count(), 0);
}

#[tokio::test]
async fn receipt_backed_create_and_accept_reply_loss_reconcile_without_duplicate_journal() {
    let fixture = eligible_merge_fixture().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
                operation: Some(StoreWriterOperationKind::CreateMergePreflight),
                count: 2,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
                operation: Some(StoreWriterOperationKind::BindMergePreflightInputs),
                count: 2,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
                operation: Some(StoreWriterOperationKind::AcceptMerge),
                count: 2,
            },
        ])
        .unwrap(),
    );
    let wake = Arc::new(support::CountingWake::default());
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store().clone(),
        wake.clone(),
        8,
        controller,
    );

    let create = merge_command(DeliveryMergeWriteCommand::CreatePreflight(
        fixture.create_preflight_request(ClientRequestId::new()),
    ));
    let unknown = writer
        .submit_delivery(create, support::deadline())
        .completion()
        .await;
    let create = match unknown.disposition {
        DeliveryDisposition::OutcomeUnknown {
            reason: OutcomeUnknownReason::CommitStatusUnknown,
            command,
        } => command,
        other => panic!("two create receipt losses must remain unknown, got {other:?}"),
    };
    let operations = fixture
        .store()
        .delivery_ownership_snapshot(fixture.task.id)
        .await
        .unwrap()
        .unwrap()
        .merge_operations;
    assert_eq!(operations.len(), 1);
    let operation_id = operations[0].operation_id;
    assert_eq!(merge_journal_count(fixture.store(), operation_id).await, 1);
    let replay = writer
        .reconcile_delivery(create, support::deadline())
        .completion()
        .await;
    let receipt = match replay.disposition {
        DeliveryDisposition::Confirmed(DeliveryWriteOutcome::Merge(
            DeliveryMergeWriteOutcome::CreatePreflight(CreatePreflightOutcome::Existing(receipt)),
        )) => receipt,
        other => panic!("create receipt replay must be existing, got {other:?}"),
    };
    assert_eq!(receipt.operation_id, operation_id);
    assert_eq!(merge_journal_count(fixture.store(), operation_id).await, 1);

    let bind = merge_command(DeliveryMergeWriteCommand::BindPreflightInputs(
        fixture.bind_preflight_inputs_request(operation_id),
    ));
    let unknown = writer
        .submit_delivery(bind, support::deadline())
        .completion()
        .await;
    let bind = match unknown.disposition {
        DeliveryDisposition::OutcomeUnknown {
            reason: OutcomeUnknownReason::CommitStatusUnknown,
            command,
        } => command,
        other => panic!("two input-binding reply losses must remain unknown, got {other:?}"),
    };
    assert_eq!(merge_journal_count(fixture.store(), operation_id).await, 2);
    let replay = writer
        .reconcile_delivery(bind, support::deadline())
        .completion()
        .await;
    assert!(matches!(
        replay.disposition,
        DeliveryDisposition::Confirmed(DeliveryWriteOutcome::Merge(
            DeliveryMergeWriteOutcome::BindPreflightInputs(MergeTransitionOutcome::Existing(_))
        ))
    ));
    assert_eq!(merge_journal_count(fixture.store(), operation_id).await, 2);

    assert!(matches!(
        confirmed_merge(
            &writer,
            DeliveryMergeWriteCommand::RecordPreflightResult(ready_preflight_request(
                fixture.task.id,
                operation_id,
            )),
        )
        .await,
        DeliveryMergeWriteOutcome::RecordPreflightResult(MergeTransitionOutcome::Applied(_))
    ));
    let accept = merge_command(DeliveryMergeWriteCommand::Accept(
        fixture
            .accept_request(
                operation_id,
                DeliveryVersion::try_new(3).unwrap(),
                ClientRequestId::new(),
            )
            .await,
    ));
    let unknown = writer
        .submit_delivery(accept, support::deadline())
        .completion()
        .await;
    let accept = match unknown.disposition {
        DeliveryDisposition::OutcomeUnknown {
            reason: OutcomeUnknownReason::CommitStatusUnknown,
            command,
        } => command,
        other => panic!("two accept receipt losses must remain unknown, got {other:?}"),
    };
    assert_eq!(merge_journal_count(fixture.store(), operation_id).await, 4);
    let replay = writer
        .reconcile_delivery(accept, support::deadline())
        .completion()
        .await;
    assert!(matches!(
        replay.disposition,
        DeliveryDisposition::Confirmed(DeliveryWriteOutcome::Merge(
            DeliveryMergeWriteOutcome::Accept(AcceptMergeOutcome::Existing(_))
        ))
    ));
    assert_eq!(merge_journal_count(fixture.store(), operation_id).await, 4);
    assert_eq!(wake.count(), 0);
}

#[tokio::test]
async fn unbound_preflight_failure_reply_loss_reconciles_existing_without_event_wake() {
    let fixture = eligible_merge_fixture().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([StoreWriterFaultSpec {
            point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
            operation: Some(StoreWriterOperationKind::FailUnboundMergePreflight),
            count: 2,
        }])
        .unwrap(),
    );
    let wake = Arc::new(support::CountingWake::default());
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store().clone(),
        wake.clone(),
        8,
        controller,
    );
    let operation_id = match confirmed_merge(
        &writer,
        DeliveryMergeWriteCommand::CreatePreflight(
            fixture.create_preflight_request(ClientRequestId::new()),
        ),
    )
    .await
    {
        DeliveryMergeWriteOutcome::CreatePreflight(CreatePreflightOutcome::Created(receipt)) => {
            receipt.operation_id
        }
        other => panic!("preflight intent must be created, got {other:?}"),
    };
    let fail = merge_command(DeliveryMergeWriteCommand::FailUnboundPreflight(
        FailUnboundMergePreflightRequest::try_new(
            fixture.task.id,
            operation_id,
            DeliveryVersion::initial(),
            UnboundMergePreflightFailure::Stale(PreflightStaleReason::TargetHeadChanged),
        )
        .unwrap(),
    ));
    let unknown = writer
        .submit_delivery(fail, support::deadline())
        .completion()
        .await;
    let fail = match unknown.disposition {
        DeliveryDisposition::OutcomeUnknown {
            reason: OutcomeUnknownReason::CommitStatusUnknown,
            command,
        } => command,
        other => panic!("two unbound failure reply losses must remain unknown, got {other:?}"),
    };
    assert_eq!(merge_journal_count(fixture.store(), operation_id).await, 2);
    let replay = writer
        .reconcile_delivery(fail, support::deadline())
        .completion()
        .await;
    assert!(matches!(
        replay.disposition,
        DeliveryDisposition::Confirmed(DeliveryWriteOutcome::Merge(
            DeliveryMergeWriteOutcome::FailUnboundPreflight(MergeTransitionOutcome::Existing(_))
        ))
    ));
    assert_eq!(merge_journal_count(fixture.store(), operation_id).await, 2);
    assert_eq!(wake.count(), 0);
}

#[tokio::test]
async fn preflight_ready_stale_exact_replay_and_conflict_are_typed_and_wakeless() {
    let fixture = eligible_merge_fixture().await;
    let wake = Arc::new(support::CountingWake::default());
    let writer = StoreWriterHandle::spawn(fixture.store().clone(), wake.clone(), 8);
    let operation_id = match confirmed_merge(
        &writer,
        DeliveryMergeWriteCommand::CreatePreflight(
            fixture.create_preflight_request(ClientRequestId::new()),
        ),
    )
    .await
    {
        DeliveryMergeWriteOutcome::CreatePreflight(CreatePreflightOutcome::Created(receipt)) => {
            receipt.operation_id
        }
        other => panic!("preflight must be created, got {other:?}"),
    };
    assert!(matches!(
        confirmed_merge(
            &writer,
            DeliveryMergeWriteCommand::BindPreflightInputs(
                fixture.bind_preflight_inputs_request(operation_id),
            ),
        )
        .await,
        DeliveryMergeWriteOutcome::BindPreflightInputs(MergeTransitionOutcome::Applied(_))
    ));
    assert!(matches!(
        confirmed_merge(
            &writer,
            DeliveryMergeWriteCommand::RecordPreflightResult(ready_preflight_request(
                fixture.task.id,
                operation_id,
            )),
        )
        .await,
        DeliveryMergeWriteOutcome::RecordPreflightResult(MergeTransitionOutcome::Applied(_))
    ));
    let stale = MarkPreflightStaleRequest::try_new(
        fixture.task.id,
        operation_id,
        DeliveryVersion::try_new(3).unwrap(),
        PreflightStaleReason::SourceChanged,
    )
    .unwrap();
    assert!(matches!(
        confirmed_merge(
            &writer,
            DeliveryMergeWriteCommand::MarkPreflightStale(stale),
        )
        .await,
        DeliveryMergeWriteOutcome::MarkPreflightStale(MarkPreflightStaleOutcome::Applied { .. })
    ));
    assert!(matches!(
        confirmed_merge(
            &writer,
            DeliveryMergeWriteCommand::MarkPreflightStale(stale),
        )
        .await,
        DeliveryMergeWriteOutcome::MarkPreflightStale(MarkPreflightStaleOutcome::Existing { .. })
    ));
    let conflict = writer
        .submit_delivery(
            merge_command(DeliveryMergeWriteCommand::MarkPreflightStale(
                MarkPreflightStaleRequest::try_new(
                    fixture.task.id,
                    operation_id,
                    DeliveryVersion::initial(),
                    PreflightStaleReason::SourceChanged,
                )
                .unwrap(),
            )),
            support::deadline(),
        )
        .completion()
        .await;
    assert!(matches!(
        conflict.disposition,
        DeliveryDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::ExactReconciliation,
            outcome: Some(DeliveryWriteOutcome::Merge(
                DeliveryMergeWriteOutcome::MarkPreflightStale(MarkPreflightStaleOutcome::Conflict)
            )),
            error: None,
        }
    ));
    assert_eq!(merge_journal_count(fixture.store(), operation_id).await, 4);
    assert_eq!(wake.count(), 0);
}

#[tokio::test]
async fn merge_pending_and_complete_dispatch_exact_proofs_without_event_wake() {
    let fixture = accepted_source_fixture().await;
    let wake = Arc::new(support::CountingWake::default());
    let writer = StoreWriterHandle::spawn(fixture.store().clone(), wake.clone(), 8);
    let source = committed_source_with_writer(&writer, &fixture).await;
    let task_id = fixture.accept_command.task_id();
    let operation_id = fixture.accept_command.preflight_operation_id();
    let accepted = merge_operation(fixture.store(), task_id, operation_id).await;
    assert_eq!(accepted.state, MergeOperationState::Accepted);
    let pending_version = match confirmed_merge(
        &writer,
        DeliveryMergeWriteCommand::EnterPending(
            EnterMergePendingRequest::try_new(
                task_id,
                operation_id,
                accepted.version,
                merge_object_proof(&accepted),
            )
            .unwrap(),
        ),
    )
    .await
    {
        DeliveryMergeWriteOutcome::EnterPending(MergeTransitionOutcome::Applied(receipt)) => {
            receipt.version
        }
        other => panic!("merge intent must apply, got {other:?}"),
    };
    let pending = merge_operation(fixture.store(), task_id, operation_id).await;
    assert_eq!(pending.state, MergeOperationState::MergePending);
    let complete = CompleteMergeRequest::try_new(
        task_id,
        operation_id,
        pending_version,
        merge_applied_proof(&pending, &source),
    )
    .unwrap();
    assert!(matches!(
        confirmed_merge(
            &writer,
            DeliveryMergeWriteCommand::Complete(complete.clone()),
        )
        .await,
        DeliveryMergeWriteOutcome::Complete(MergeTransitionOutcome::Applied(_))
    ));
    assert!(matches!(
        confirmed_merge(&writer, DeliveryMergeWriteCommand::Complete(complete)).await,
        DeliveryMergeWriteOutcome::Complete(MergeTransitionOutcome::Existing(_))
    ));
    assert_eq!(merge_journal_count(fixture.store(), operation_id).await, 6);
    assert_eq!(
        merge_operation(fixture.store(), task_id, operation_id)
            .await
            .state,
        MergeOperationState::Merged
    );
    assert!(
        fixture
            .store()
            .delivery_ownership_snapshot(task_id)
            .await
            .unwrap()
            .unwrap()
            .disposition
            .is_some()
    );
    assert_eq!(wake.count(), 0);
}

#[tokio::test]
async fn abort_pending_and_complete_abort_are_exact_replayable_and_wakeless() {
    let fixture = accepted_source_fixture().await;
    let wake = Arc::new(support::CountingWake::default());
    let writer = StoreWriterHandle::spawn(fixture.store().clone(), wake.clone(), 8);
    let source = committed_source_with_writer(&writer, &fixture).await;
    let task_id = fixture.accept_command.task_id();
    let operation_id = fixture.accept_command.preflight_operation_id();
    let accepted = merge_operation(fixture.store(), task_id, operation_id).await;
    let pending_version = match confirmed_merge(
        &writer,
        DeliveryMergeWriteCommand::EnterPending(
            EnterMergePendingRequest::try_new(
                task_id,
                operation_id,
                accepted.version,
                merge_object_proof(&accepted),
            )
            .unwrap(),
        ),
    )
    .await
    {
        DeliveryMergeWriteOutcome::EnterPending(MergeTransitionOutcome::Applied(receipt)) => {
            receipt.version
        }
        other => panic!("merge intent must apply, got {other:?}"),
    };
    let begin = BeginMergeAbortRequest::try_new(
        task_id,
        operation_id,
        pending_version,
        abort_proof(&source),
    )
    .unwrap();
    let abort_version = match confirmed_merge(
        &writer,
        DeliveryMergeWriteCommand::BeginAbort(begin.clone()),
    )
    .await
    {
        DeliveryMergeWriteOutcome::BeginAbort(MergeTransitionOutcome::Applied(receipt)) => {
            receipt.version
        }
        other => panic!("abort intent must apply, got {other:?}"),
    };
    assert!(matches!(
        confirmed_merge(&writer, DeliveryMergeWriteCommand::BeginAbort(begin)).await,
        DeliveryMergeWriteOutcome::BeginAbort(MergeTransitionOutcome::Existing(_))
    ));
    let complete = CompleteMergeAbortRequest::try_new(
        task_id,
        operation_id,
        abort_version,
        abort_applied_proof(&source),
    )
    .unwrap();
    assert!(matches!(
        confirmed_merge(
            &writer,
            DeliveryMergeWriteCommand::CompleteAbort(complete.clone()),
        )
        .await,
        DeliveryMergeWriteOutcome::CompleteAbort(MergeTransitionOutcome::Applied(_))
    ));
    assert!(matches!(
        confirmed_merge(&writer, DeliveryMergeWriteCommand::CompleteAbort(complete)).await,
        DeliveryMergeWriteOutcome::CompleteAbort(MergeTransitionOutcome::Existing(_))
    ));
    assert_eq!(merge_journal_count(fixture.store(), operation_id).await, 7);
    assert_eq!(
        merge_operation(fixture.store(), task_id, operation_id)
            .await
            .state,
        MergeOperationState::Conflict
    );
    assert_eq!(wake.count(), 0);
}

#[tokio::test]
async fn known_failure_and_reconciliation_terminal_routes_are_exact_and_wakeless() {
    let failure_fixture = accepted_source_fixture().await;
    let failure_wake = Arc::new(support::CountingWake::default());
    let failure_writer =
        StoreWriterHandle::spawn(failure_fixture.store().clone(), failure_wake.clone(), 8);
    committed_source_with_writer(&failure_writer, &failure_fixture).await;
    let failure_task = failure_fixture.accept_command.task_id();
    let failure_operation = failure_fixture.accept_command.preflight_operation_id();
    let accepted = merge_operation(failure_fixture.store(), failure_task, failure_operation).await;
    let failure = RecordMergeKnownFailureRequest::try_new(
        failure_task,
        failure_operation,
        MergeOperationState::Accepted,
        accepted.version,
        MergeKnownNotAppliedReason::TargetHeadChanged,
    )
    .unwrap();
    assert!(matches!(
        confirmed_merge(
            &failure_writer,
            DeliveryMergeWriteCommand::RecordKnownFailure(failure.clone()),
        )
        .await,
        DeliveryMergeWriteOutcome::RecordKnownFailure(MergeTransitionOutcome::Applied(_))
    ));
    assert!(matches!(
        confirmed_merge(
            &failure_writer,
            DeliveryMergeWriteCommand::RecordKnownFailure(failure),
        )
        .await,
        DeliveryMergeWriteOutcome::RecordKnownFailure(MergeTransitionOutcome::Existing(_))
    ));
    assert_eq!(
        merge_journal_count(failure_fixture.store(), failure_operation).await,
        5
    );
    assert_eq!(failure_wake.count(), 0);

    let reconcile_fixture = accepted_source_fixture().await;
    let reconcile_wake = Arc::new(support::CountingWake::default());
    let reconcile_writer =
        StoreWriterHandle::spawn(reconcile_fixture.store().clone(), reconcile_wake.clone(), 8);
    committed_source_with_writer(&reconcile_writer, &reconcile_fixture).await;
    let reconcile_task = reconcile_fixture.accept_command.task_id();
    let reconcile_operation = reconcile_fixture.accept_command.preflight_operation_id();
    let accepted = merge_operation(
        reconcile_fixture.store(),
        reconcile_task,
        reconcile_operation,
    )
    .await;
    let pending_version = match confirmed_merge(
        &reconcile_writer,
        DeliveryMergeWriteCommand::EnterPending(
            EnterMergePendingRequest::try_new(
                reconcile_task,
                reconcile_operation,
                accepted.version,
                merge_object_proof(&accepted),
            )
            .unwrap(),
        ),
    )
    .await
    {
        DeliveryMergeWriteOutcome::EnterPending(MergeTransitionOutcome::Applied(receipt)) => {
            receipt.version
        }
        other => panic!("merge intent must apply, got {other:?}"),
    };
    let reconcile = ReconcileMergeRequest::try_new(
        reconcile_task,
        reconcile_operation,
        MergeOperationState::MergePending,
        pending_version,
        MergeReconciliationReason::WorktreeIdentityMismatch,
    )
    .unwrap();
    assert!(matches!(
        confirmed_merge(
            &reconcile_writer,
            DeliveryMergeWriteCommand::Reconcile(reconcile.clone()),
        )
        .await,
        DeliveryMergeWriteOutcome::Reconcile(MergeTransitionOutcome::Applied(_))
    ));
    assert!(matches!(
        confirmed_merge(
            &reconcile_writer,
            DeliveryMergeWriteCommand::Reconcile(reconcile),
        )
        .await,
        DeliveryMergeWriteOutcome::Reconcile(MergeTransitionOutcome::Existing(_))
    ));
    assert_eq!(
        merge_journal_count(reconcile_fixture.store(), reconcile_operation).await,
        6
    );
    assert_eq!(reconcile_wake.count(), 0);
}

#[tokio::test]
async fn cleanup_accept_receipt_reply_loss_reconciles_exactly_without_duplicate_journal() {
    let fixture = accepted_source_fixture().await;
    let controller = Arc::new(
        StoreWriterTestController::try_new([
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
                operation: Some(StoreWriterOperationKind::AcceptWorktreeCleanup),
                count: 2,
            },
            StoreWriterFaultSpec {
                point: StoreWriterFaultPoint::FailAfterCommitBeforeReply,
                operation: Some(StoreWriterOperationKind::AcceptBranchCleanup),
                count: 2,
            },
        ])
        .unwrap(),
    );
    let wake = Arc::new(support::CountingWake::default());
    let writer = StoreWriterHandle::spawn_with_test_controller(
        fixture.store().clone(),
        wake.clone(),
        8,
        controller,
    );
    merged_cleanup_with_writer(&writer, &fixture).await;
    let task_id = fixture.accept_command.task_id();

    let remove_request =
        remove_worktree_request(fixture.store(), task_id, ClientRequestId::new()).await;
    let expected_remove = cleanup_command(DeliveryCleanupWriteCommand::AcceptWorktree(
        remove_request.clone(),
    ));
    let unknown = writer
        .submit_delivery(expected_remove.clone(), support::deadline())
        .completion()
        .await;
    let exact_remove = match unknown.disposition {
        DeliveryDisposition::OutcomeUnknown {
            reason: OutcomeUnknownReason::CommitStatusUnknown,
            command,
        } => {
            assert_eq!(command, expected_remove);
            command
        }
        other => panic!("two worktree receipt losses must remain unknown, got {other:?}"),
    };
    let worktree_operation = fixture
        .store()
        .delivery_ownership_snapshot(task_id)
        .await
        .unwrap()
        .unwrap()
        .cleanup_operations
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(
        cleanup_journal_count(fixture.store(), worktree_operation.operation_id).await,
        1
    );
    let replay = writer
        .reconcile_delivery(exact_remove, support::deadline())
        .completion()
        .await;
    let worktree_receipt = match replay.disposition {
        DeliveryDisposition::Confirmed(DeliveryWriteOutcome::Cleanup(
            DeliveryCleanupWriteOutcome::AcceptWorktree(CleanupAcceptanceOutcome::Existing(
                receipt,
            )),
        )) => receipt,
        other => panic!("worktree receipt replay must be existing, got {other:?}"),
    };
    assert_eq!(
        worktree_receipt.operation_id,
        worktree_operation.operation_id
    );
    assert_eq!(
        cleanup_journal_count(fixture.store(), worktree_receipt.operation_id).await,
        1
    );
    finish_worktree_cleanup(&writer, task_id, &worktree_receipt).await;

    let delete_request = delete_branch_request(
        fixture.store(),
        task_id,
        ClientRequestId::new(),
        GitCommitOid::from_str(MERGE_COMMIT).unwrap(),
    )
    .await;
    let expected_delete = cleanup_command(DeliveryCleanupWriteCommand::AcceptBranch(
        delete_request.clone(),
    ));
    let unknown = writer
        .submit_delivery(expected_delete.clone(), support::deadline())
        .completion()
        .await;
    let exact_delete = match unknown.disposition {
        DeliveryDisposition::OutcomeUnknown {
            reason: OutcomeUnknownReason::CommitStatusUnknown,
            command,
        } => {
            assert_eq!(command, expected_delete);
            command
        }
        other => panic!("two branch receipt losses must remain unknown, got {other:?}"),
    };
    let branch_operation = fixture
        .store()
        .delivery_ownership_snapshot(task_id)
        .await
        .unwrap()
        .unwrap()
        .cleanup_operations
        .into_iter()
        .find(|operation| operation.operation_id != worktree_receipt.operation_id)
        .unwrap();
    assert_eq!(
        cleanup_journal_count(fixture.store(), branch_operation.operation_id).await,
        1
    );
    let replay = writer
        .reconcile_delivery(exact_delete, support::deadline())
        .completion()
        .await;
    let branch_receipt = match replay.disposition {
        DeliveryDisposition::Confirmed(DeliveryWriteOutcome::Cleanup(
            DeliveryCleanupWriteOutcome::AcceptBranch(CleanupAcceptanceOutcome::Existing(receipt)),
        )) => receipt,
        other => panic!("branch receipt replay must be existing, got {other:?}"),
    };
    assert_eq!(branch_receipt.operation_id, branch_operation.operation_id);
    assert_eq!(
        cleanup_journal_count(fixture.store(), branch_receipt.operation_id).await,
        1
    );
    assert_eq!(wake.count(), 0);
}

#[tokio::test]
async fn worktree_cleanup_phases_and_replays_dispatch_exactly_without_event_wake() {
    let fixture = accepted_source_fixture().await;
    let wake = Arc::new(support::CountingWake::default());
    let writer = StoreWriterHandle::spawn(fixture.store().clone(), wake.clone(), 8);
    merged_cleanup_with_writer(&writer, &fixture).await;
    let task_id = fixture.accept_command.task_id();
    let remove = remove_worktree_request(fixture.store(), task_id, ClientRequestId::new()).await;
    let accepted = accepted_worktree_cleanup(&writer, remove).await;

    let unlock = RecordWorktreeUnlockedRequest::try_new(
        CleanupOperationAnchor::try_new(
            task_id,
            accepted.operation_id,
            accepted.accepted_operation_version,
        )
        .unwrap(),
    )
    .unwrap();
    let unlocked = match confirmed_cleanup(
        &writer,
        DeliveryCleanupWriteCommand::RecordWorktreeUnlocked(unlock.clone()),
    )
    .await
    {
        DeliveryCleanupWriteOutcome::RecordWorktreeUnlocked(CleanupTransitionOutcome::Applied(
            receipt,
        )) => receipt,
        other => panic!("worktree unlock must apply, got {other:?}"),
    };
    assert!(matches!(
        confirmed_cleanup(
            &writer,
            DeliveryCleanupWriteCommand::RecordWorktreeUnlocked(unlock),
        )
        .await,
        DeliveryCleanupWriteOutcome::RecordWorktreeUnlocked(CleanupTransitionOutcome::Existing(_))
    ));

    let enter = EnterWorktreeRemovePendingRequest::try_new(
        CleanupOperationAnchor::try_new(task_id, accepted.operation_id, unlocked.version).unwrap(),
    )
    .unwrap();
    let pending = match confirmed_cleanup(
        &writer,
        DeliveryCleanupWriteCommand::EnterWorktreeRemovePending(enter.clone()),
    )
    .await
    {
        DeliveryCleanupWriteOutcome::EnterWorktreeRemovePending(
            CleanupTransitionOutcome::Applied(receipt),
        ) => receipt,
        other => panic!("worktree remove intent must apply, got {other:?}"),
    };
    assert!(matches!(
        confirmed_cleanup(
            &writer,
            DeliveryCleanupWriteCommand::EnterWorktreeRemovePending(enter),
        )
        .await,
        DeliveryCleanupWriteOutcome::EnterWorktreeRemovePending(
            CleanupTransitionOutcome::Existing(_)
        )
    ));

    let complete = CompleteWorktreeCleanupRequest::try_new(
        CleanupOperationAnchor::try_new(task_id, accepted.operation_id, pending.version).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        confirmed_cleanup(
            &writer,
            DeliveryCleanupWriteCommand::CompleteWorktree(complete.clone()),
        )
        .await,
        DeliveryCleanupWriteOutcome::CompleteWorktree(CleanupTransitionOutcome::Applied(_))
    ));
    assert!(matches!(
        confirmed_cleanup(
            &writer,
            DeliveryCleanupWriteCommand::CompleteWorktree(complete),
        )
        .await,
        DeliveryCleanupWriteOutcome::CompleteWorktree(CleanupTransitionOutcome::Existing(_))
    ));
    let conflict = writer
        .submit_delivery(
            cleanup_command(DeliveryCleanupWriteCommand::CompleteWorktree(
                CompleteWorktreeCleanupRequest::try_new(
                    CleanupOperationAnchor::try_new(
                        task_id,
                        accepted.operation_id,
                        accepted.accepted_operation_version,
                    )
                    .unwrap(),
                )
                .unwrap(),
            )),
            support::deadline(),
        )
        .completion()
        .await;
    assert!(matches!(
        conflict.disposition,
        DeliveryDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::ExactReconciliation,
            outcome: Some(DeliveryWriteOutcome::Cleanup(
                DeliveryCleanupWriteOutcome::CompleteWorktree(CleanupTransitionOutcome::Conflict)
            )),
            error: None,
        }
    ));
    assert_eq!(
        cleanup_journal_count(fixture.store(), accepted.operation_id).await,
        4
    );
    assert_eq!(wake.count(), 0);
}

#[tokio::test]
async fn worktree_cleanup_failure_and_reconciliation_routes_are_exact_and_wakeless() {
    let failure_fixture = accepted_source_fixture().await;
    let failure_wake = Arc::new(support::CountingWake::default());
    let failure_writer =
        StoreWriterHandle::spawn(failure_fixture.store().clone(), failure_wake.clone(), 8);
    merged_cleanup_with_writer(&failure_writer, &failure_fixture).await;
    let failure_task = failure_fixture.accept_command.task_id();
    let accepted = accepted_worktree_cleanup(
        &failure_writer,
        remove_worktree_request(
            failure_fixture.store(),
            failure_task,
            ClientRequestId::new(),
        )
        .await,
    )
    .await;
    let failure = RecordWorktreeCleanupFailureRequest::try_new(
        CleanupOperationAnchor::try_new(
            failure_task,
            accepted.operation_id,
            accepted.accepted_operation_version,
        )
        .unwrap(),
        CleanupOperationState::UnlockPending,
        WorktreeCleanupKnownNotAppliedReason::CommandTimedOut,
    )
    .unwrap();
    assert!(matches!(
        confirmed_cleanup(
            &failure_writer,
            DeliveryCleanupWriteCommand::RecordWorktreeFailure(failure.clone()),
        )
        .await,
        DeliveryCleanupWriteOutcome::RecordWorktreeFailure(CleanupTransitionOutcome::Applied(_))
    ));
    assert!(matches!(
        confirmed_cleanup(
            &failure_writer,
            DeliveryCleanupWriteCommand::RecordWorktreeFailure(failure),
        )
        .await,
        DeliveryCleanupWriteOutcome::RecordWorktreeFailure(CleanupTransitionOutcome::Existing(_))
    ));
    assert_eq!(
        cleanup_journal_count(failure_fixture.store(), accepted.operation_id).await,
        2
    );
    assert_eq!(failure_wake.count(), 0);

    let reconcile_fixture = accepted_source_fixture().await;
    let reconcile_wake = Arc::new(support::CountingWake::default());
    let reconcile_writer =
        StoreWriterHandle::spawn(reconcile_fixture.store().clone(), reconcile_wake.clone(), 8);
    merged_cleanup_with_writer(&reconcile_writer, &reconcile_fixture).await;
    let reconcile_task = reconcile_fixture.accept_command.task_id();
    let accepted = accepted_worktree_cleanup(
        &reconcile_writer,
        remove_worktree_request(
            reconcile_fixture.store(),
            reconcile_task,
            ClientRequestId::new(),
        )
        .await,
    )
    .await;
    let reconcile = ReconcileWorktreeCleanupRequest::try_new(
        CleanupOperationAnchor::try_new(
            reconcile_task,
            accepted.operation_id,
            accepted.accepted_operation_version,
        )
        .unwrap(),
        CleanupOperationState::UnlockPending,
        CleanupReconciliationReason::WorktreeIdentityMismatch,
    )
    .unwrap();
    assert!(matches!(
        confirmed_cleanup(
            &reconcile_writer,
            DeliveryCleanupWriteCommand::ReconcileWorktree(reconcile.clone()),
        )
        .await,
        DeliveryCleanupWriteOutcome::ReconcileWorktree(CleanupTransitionOutcome::Applied(_))
    ));
    assert!(matches!(
        confirmed_cleanup(
            &reconcile_writer,
            DeliveryCleanupWriteCommand::ReconcileWorktree(reconcile),
        )
        .await,
        DeliveryCleanupWriteOutcome::ReconcileWorktree(CleanupTransitionOutcome::Existing(_))
    ));
    assert_eq!(
        cleanup_journal_count(reconcile_fixture.store(), accepted.operation_id).await,
        2
    );
    assert_eq!(reconcile_wake.count(), 0);
}

#[tokio::test]
async fn branch_cleanup_refresh_and_terminal_routes_are_exact_and_wakeless() {
    let refresh_fixture = accepted_source_fixture().await;
    let refresh_wake = Arc::new(support::CountingWake::default());
    let refresh_writer =
        StoreWriterHandle::spawn(refresh_fixture.store().clone(), refresh_wake.clone(), 8);
    let origin_head = GitCommitOid::from_str(MERGE_COMMIT).unwrap();
    let delete = branch_cleanup_ready_request(
        &refresh_writer,
        &refresh_fixture,
        ClientRequestId::new(),
        origin_head.clone(),
    )
    .await;
    let accepted = accepted_branch_cleanup(&refresh_writer, delete).await;
    let fresh_head = GitCommitOid::from_str(TARGET_HEAD).unwrap();
    let refresh = RefreshBranchCleanupTargetRequest::try_new(
        CleanupOperationAnchor::try_new(
            refresh_fixture.accept_command.task_id(),
            accepted.operation_id,
            accepted.accepted_operation_version,
        )
        .unwrap(),
        origin_head,
        fresh_head,
    )
    .unwrap();
    let refreshed = match confirmed_cleanup(
        &refresh_writer,
        DeliveryCleanupWriteCommand::RefreshBranchTarget(refresh.clone()),
    )
    .await
    {
        DeliveryCleanupWriteOutcome::RefreshBranchTarget(CleanupTransitionOutcome::Applied(
            receipt,
        )) => receipt,
        other => panic!("branch target refresh must apply, got {other:?}"),
    };
    assert!(matches!(
        confirmed_cleanup(
            &refresh_writer,
            DeliveryCleanupWriteCommand::RefreshBranchTarget(refresh),
        )
        .await,
        DeliveryCleanupWriteOutcome::RefreshBranchTarget(CleanupTransitionOutcome::Existing(_))
    ));
    let complete = CompleteBranchCleanupRequest::try_new(
        CleanupOperationAnchor::try_new(
            refresh_fixture.accept_command.task_id(),
            accepted.operation_id,
            refreshed.version,
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        confirmed_cleanup(
            &refresh_writer,
            DeliveryCleanupWriteCommand::CompleteBranch(complete.clone()),
        )
        .await,
        DeliveryCleanupWriteOutcome::CompleteBranch(CleanupTransitionOutcome::Applied(_))
    ));
    assert!(matches!(
        confirmed_cleanup(
            &refresh_writer,
            DeliveryCleanupWriteCommand::CompleteBranch(complete),
        )
        .await,
        DeliveryCleanupWriteOutcome::CompleteBranch(CleanupTransitionOutcome::Existing(_))
    ));
    assert_eq!(
        cleanup_journal_count(refresh_fixture.store(), accepted.operation_id).await,
        3
    );
    assert_eq!(refresh_wake.count(), 0);

    let failure_fixture = accepted_source_fixture().await;
    let failure_wake = Arc::new(support::CountingWake::default());
    let failure_writer =
        StoreWriterHandle::spawn(failure_fixture.store().clone(), failure_wake.clone(), 8);
    let delete = branch_cleanup_ready_request(
        &failure_writer,
        &failure_fixture,
        ClientRequestId::new(),
        GitCommitOid::from_str(MERGE_COMMIT).unwrap(),
    )
    .await;
    let accepted = accepted_branch_cleanup(&failure_writer, delete).await;
    let failure = RecordBranchCleanupFailureRequest::try_new(
        CleanupOperationAnchor::try_new(
            failure_fixture.accept_command.task_id(),
            accepted.operation_id,
            accepted.accepted_operation_version,
        )
        .unwrap(),
        BranchCleanupKnownNotAppliedReason::SourceBranchNotMerged,
    )
    .unwrap();
    assert!(matches!(
        confirmed_cleanup(
            &failure_writer,
            DeliveryCleanupWriteCommand::RecordBranchFailure(failure.clone()),
        )
        .await,
        DeliveryCleanupWriteOutcome::RecordBranchFailure(CleanupTransitionOutcome::Applied(_))
    ));
    assert!(matches!(
        confirmed_cleanup(
            &failure_writer,
            DeliveryCleanupWriteCommand::RecordBranchFailure(failure),
        )
        .await,
        DeliveryCleanupWriteOutcome::RecordBranchFailure(CleanupTransitionOutcome::Existing(_))
    ));
    assert_eq!(
        cleanup_journal_count(failure_fixture.store(), accepted.operation_id).await,
        2
    );
    assert_eq!(failure_wake.count(), 0);

    let reconcile_fixture = accepted_source_fixture().await;
    let reconcile_wake = Arc::new(support::CountingWake::default());
    let reconcile_writer =
        StoreWriterHandle::spawn(reconcile_fixture.store().clone(), reconcile_wake.clone(), 8);
    let delete = branch_cleanup_ready_request(
        &reconcile_writer,
        &reconcile_fixture,
        ClientRequestId::new(),
        GitCommitOid::from_str(MERGE_COMMIT).unwrap(),
    )
    .await;
    let accepted = accepted_branch_cleanup(&reconcile_writer, delete).await;
    let reconcile = ReconcileBranchCleanupRequest::try_new(
        CleanupOperationAnchor::try_new(
            reconcile_fixture.accept_command.task_id(),
            accepted.operation_id,
            accepted.accepted_operation_version,
        )
        .unwrap(),
        CleanupReconciliationReason::SourceInconsistent,
    )
    .unwrap();
    assert!(matches!(
        confirmed_cleanup(
            &reconcile_writer,
            DeliveryCleanupWriteCommand::ReconcileBranch(reconcile.clone()),
        )
        .await,
        DeliveryCleanupWriteOutcome::ReconcileBranch(CleanupTransitionOutcome::Applied(_))
    ));
    assert!(matches!(
        confirmed_cleanup(
            &reconcile_writer,
            DeliveryCleanupWriteCommand::ReconcileBranch(reconcile),
        )
        .await,
        DeliveryCleanupWriteOutcome::ReconcileBranch(CleanupTransitionOutcome::Existing(_))
    ));
    assert_eq!(
        cleanup_journal_count(reconcile_fixture.store(), accepted.operation_id).await,
        2
    );
    assert_eq!(reconcile_wake.count(), 0);
}
