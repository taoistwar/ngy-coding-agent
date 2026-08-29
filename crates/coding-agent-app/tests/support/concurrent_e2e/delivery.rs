use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use coding_agent_app::{
    DeliveryAcceptRequest, DeliveryCleanupAcceptanceOutcome, DeliveryManagerHandle,
    DeliveryManagerLiveDependencies, DeliveryMergeAcceptanceOutcome, DeliveryPreflightOutcome,
    DeliveryPreflightRequest, DeliveryPreflightState, DeliveryProcessProof,
    DeliveryProcessProofError, DeliveryProcessProofProvider, DeliveryProcessProofProviderTestSeam,
    DeliveryRemoveWorktreeRequest, RepositoryControlCoordinator, ServiceStateController,
    StoreWriterHandle, TaskManagerHandle, production_delivery_dynamic_registries_for_test,
};
use coding_agent_domain::{ClientRequestId, TaskId};
use coding_agent_runtime::{
    ExecutionDirectory, ProcessCleanupProof, ProcessLimits, ProcessLivenessScope, ToolchainPaths,
    probe_delivery_git,
};
use coding_agent_store::{
    AcceptMergeCommandRequest, DeliveryOperationId, DeliveryOperationSnapshot,
    DeliveryOwnershipSnapshot, GitBranchRef, GitCommitOid, MergeOperationRecord,
    PreflightCommandRequest, RemoveWorktreeCommandRequest, Store,
};
use tokio_util::sync::CancellationToken;

use super::{PROCESS_CLEANUP_TIMEOUT, PROCESS_COMMAND_TIMEOUT, repository};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverySideEffectSnapshot {
    target_head: String,
    repository_refs: Vec<u8>,
    target_status: Vec<u8>,
    target_source: Vec<u8>,
    source_ref_oid: Option<String>,
    source_worktree_status: Vec<u8>,
    source_worktree_bytes: Vec<u8>,
    ownership: DeliveryOwnershipSnapshot,
    accept_receipts: i64,
    remove_worktree_receipts: i64,
}

impl DeliverySideEffectSnapshot {
    pub fn assert_no_command_receipts(&self) {
        assert_eq!(
            self.accept_receipts, 0,
            "accept receipt exists before ownership"
        );
        assert_eq!(
            self.remove_worktree_receipts, 0,
            "cleanup receipt exists before ownership"
        );
    }

    pub fn assert_receipts(&self, accept_merge: i64, remove_worktree: i64) {
        assert_eq!(self.accept_receipts, accept_merge);
        assert_eq!(self.remove_worktree_receipts, remove_worktree);
    }
}

pub(super) struct ConcurrentDelivery {
    manager: DeliveryManagerHandle,
}

impl ConcurrentDelivery {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn start(
        store: Store,
        writer: StoreWriterHandle,
        task_manager: TaskManagerHandle,
        repository_control: Arc<RepositoryControlCoordinator>,
        instance_process_scope: ProcessLivenessScope,
        toolchain: ToolchainPaths,
        artifact_root: std::path::PathBuf,
        runtime_directory: std::path::PathBuf,
        service_state: ServiceStateController,
    ) -> Self {
        let private_runtime = Arc::new(
            ExecutionDirectory::open(&runtime_directory)
                .expect("open concurrent delivery private runtime"),
        );
        let process_limits = ProcessLimits::try_new(
            512 * 1024,
            256 * 1024,
            PROCESS_COMMAND_TIMEOUT,
            PROCESS_CLEANUP_TIMEOUT,
        )
        .expect("valid concurrent delivery process limits");
        let probe = probe_delivery_git(
            toolchain.git(),
            private_runtime,
            instance_process_scope.clone(),
            process_limits,
            Duration::from_secs(30),
            CancellationToken::new(),
        )
        .await
        .expect("probe production delivery Git for concurrent E2E");
        assert_eq!(
            instance_process_scope.active_tree_count(),
            0,
            "delivery Git probe must prove and release every child tree"
        );
        let (runtime, live_runtime, cleanup_runtime) =
            production_delivery_dynamic_registries_for_test(
                store.clone(),
                Arc::new(probe),
                toolchain,
                artifact_root,
                runtime_directory,
                Arc::clone(&repository_control),
                instance_process_scope.clone(),
            );
        let process_proofs = Arc::new(InstanceProcessProofs {
            scope: instance_process_scope,
        });
        let dependencies =
            DeliveryManagerLiveDependencies::new_with_store_operation_query_for_test(
                store,
                writer,
                task_manager,
                repository_control,
                runtime,
                process_proofs,
            )
            .with_live_runtime_registry_for_test(live_runtime)
            .with_cleanup_runtime_registry_for_test(cleanup_runtime);
        Self {
            manager: DeliveryManagerHandle::spawn_live_for_test(dependencies, service_state, 32),
        }
    }

    pub(super) async fn prepare_accept(
        &self,
        store: &Store,
        repository_path: &std::path::Path,
        task_id: TaskId,
    ) -> (DeliveryOperationId, AcceptMergeCommandRequest) {
        let target_branch_name =
            repository::git_line(repository_path, &["symbolic-ref", "--short", "HEAD"]);
        let target_branch = GitBranchRef::from_str(&format!("refs/heads/{target_branch_name}"))
            .expect("valid concurrent delivery target branch");
        let target_head = GitCommitOid::from_str(&repository::git_line(
            repository_path,
            &["rev-parse", target_branch.as_str()],
        ))
        .expect("valid concurrent delivery target head");
        let outcome = self
            .manager
            .preflight(DeliveryPreflightRequest::new(
                PreflightCommandRequest::try_new(
                    ClientRequestId::new(),
                    task_id,
                    target_branch,
                    target_head,
                )
                .expect("valid concurrent delivery preflight command"),
            ))
            .await
            .expect("concurrent delivery manager remains open");
        let operation_id = match outcome {
            DeliveryPreflightOutcome::Durable(operation)
                if operation.state() == DeliveryPreflightState::PreflightReady =>
            {
                operation.operation_id()
            }
            other => panic!("expected ready concurrent delivery preflight, got {other:?}"),
        };
        let operation = merge_operation(store, operation_id).await;
        let evidence = store
            .delivery_eligibility_snapshot(task_id)
            .await
            .expect("load concurrent delivery eligibility")
            .expect("concurrent delivery task exists")
            .evidence_identity
            .expect("concurrent delivery approval evidence exists");
        let command = AcceptMergeCommandRequest::try_new(
            ClientRequestId::new(),
            task_id,
            operation_id,
            operation.version,
            evidence.workspace_generation(),
            evidence.workspace_fingerprint().clone(),
            operation.target_branch,
            operation.expected_target_head,
        )
        .expect("valid concurrent delivery accept command");
        (operation_id, command)
    }

    pub(super) async fn accept_merge(
        &self,
        command: AcceptMergeCommandRequest,
    ) -> DeliveryMergeAcceptanceOutcome {
        self.manager
            .accept_merge(DeliveryAcceptRequest::new(command))
            .await
            .expect("concurrent delivery manager remains open")
    }

    pub(super) async fn remove_worktree(
        &self,
        request: RemoveWorktreeCommandRequest,
    ) -> DeliveryCleanupAcceptanceOutcome {
        self.manager
            .remove_worktree(DeliveryRemoveWorktreeRequest::new(request))
            .await
            .expect("concurrent delivery manager remains open")
    }

    pub(super) async fn shutdown_and_join(
        &self,
    ) -> Result<
        coding_agent_app::DeliveryManagerShutdownProof,
        coding_agent_app::DeliveryManagerError,
    > {
        self.manager.shutdown_and_join().await
    }
}

pub(super) async fn remove_request(store: &Store, task_id: TaskId) -> RemoveWorktreeCommandRequest {
    let snapshot = store
        .delivery_ownership_snapshot(task_id)
        .await
        .expect("load concurrent delivery cleanup ownership")
        .expect("concurrent delivery cleanup task exists");
    let source = snapshot.source.as_ref().expect("merged source exists");
    let disposition = snapshot
        .disposition
        .as_ref()
        .expect("merged disposition exists");
    RemoveWorktreeCommandRequest::try_new(
        ClientRequestId::new(),
        task_id,
        disposition.worktree_version,
        disposition.merged_operation_id,
        source.provenance.source_branch.clone(),
        source
            .expected_source_commit
            .clone()
            .expect("committed source oid"),
    )
    .expect("valid concurrent worktree cleanup request")
}

pub(super) async fn snapshot(
    store: &Store,
    repository_path: &std::path::Path,
    source_worktree: &std::path::Path,
    source_ref: &str,
    task_id: TaskId,
) -> DeliverySideEffectSnapshot {
    let (accept_receipts, remove_worktree_receipts) = receipt_counts(store, task_id).await;
    DeliverySideEffectSnapshot {
        target_head: repository::git_line(repository_path, &["rev-parse", "HEAD"]),
        repository_refs: repository::git_bytes(repository_path, &["show-ref"]),
        target_status: repository::git_bytes(
            repository_path,
            &[
                "--no-optional-locks",
                "status",
                "--porcelain=v2",
                "--untracked-files=all",
                "-z",
            ],
        ),
        target_source: std::fs::read(repository_path.join("src/lib.rs"))
            .expect("read concurrent delivery target source"),
        source_ref_oid: repository::git_optional_ref(repository_path, source_ref),
        source_worktree_status: repository::git_bytes(
            source_worktree,
            &[
                "--no-optional-locks",
                "status",
                "--porcelain=v2",
                "--ignored=matching",
                "--untracked-files=all",
                "-z",
            ],
        ),
        source_worktree_bytes: std::fs::read(source_worktree.join("src/lib.rs"))
            .expect("read concurrent delivery source bytes"),
        ownership: store
            .delivery_ownership_snapshot(task_id)
            .await
            .expect("load concurrent delivery ownership")
            .expect("concurrent delivery ownership exists"),
        accept_receipts,
        remove_worktree_receipts,
    }
}

pub(super) async fn receipt_counts(store: &Store, task_id: TaskId) -> (i64, i64) {
    sqlx::query_as::<_, (i64, i64)>(
        "SELECT (SELECT COUNT(*) FROM task_delivery_command_receipts \
         WHERE task_id = ? AND command_kind = 'accept_merge'), \
         (SELECT COUNT(*) FROM task_delivery_command_receipts \
         WHERE task_id = ? AND command_kind = 'remove_worktree')",
    )
    .bind(task_id.to_string())
    .bind(task_id.to_string())
    .fetch_one(store.pool())
    .await
    .expect("count concurrent delivery receipts")
}

async fn merge_operation(store: &Store, operation_id: DeliveryOperationId) -> MergeOperationRecord {
    match store
        .delivery_operation_snapshot(operation_id)
        .await
        .expect("load concurrent delivery merge operation")
        .expect("concurrent delivery merge operation exists")
    {
        DeliveryOperationSnapshot::Merge(operation) => *operation,
        DeliveryOperationSnapshot::Cleanup(_) => panic!("expected concurrent merge operation"),
    }
}

struct InstanceProcessProofs {
    scope: ProcessLivenessScope,
}

impl DeliveryProcessProofProviderTestSeam for InstanceProcessProofs {}

#[async_trait::async_trait]
impl DeliveryProcessProofProvider for InstanceProcessProofs {
    async fn observe(
        &self,
        _task_id: TaskId,
    ) -> Result<DeliveryProcessProof, DeliveryProcessProofError> {
        match self
            .scope
            .cleanup_proof()
            .map_err(|_| DeliveryProcessProofError::Unavailable)?
        {
            ProcessCleanupProof::Confirmed => Ok(DeliveryProcessProof::Clean),
            ProcessCleanupProof::Held => Ok(DeliveryProcessProof::Active),
            ProcessCleanupProof::Unknown => Ok(DeliveryProcessProof::CleanupUnproven),
        }
    }
}
