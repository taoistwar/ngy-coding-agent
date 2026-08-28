#![allow(dead_code)]

use std::str::FromStr;
use std::sync::Arc;

use coding_agent_app::{
    DeliveryAcceptRequest, DeliveryManagerHandle, DeliveryManagerLiveDependencies,
    DeliveryPreflightOutcome, DeliveryPreflightRequest, DeliveryPreflightState,
    DeliveryProcessProof, DeliveryProcessProofError, DeliveryProcessProofProvider,
    DeliveryProcessProofProviderTestSeam, EventDispatcherHandle, RepositoryControlCoordinator,
    SchedulerConcurrencyLimits, ServiceState, ServiceStateController, StoreWriterHandle,
    TaskManagerHandle, TaskManagerLaunchResources, production_delivery_dynamic_registries_for_test,
};
use coding_agent_core::WorkspaceFingerprint;
use coding_agent_domain::{
    CanonicalPath, CheckActor, CheckEvidence, CheckEvidenceStatus, ClientRequestId, NewRepository,
    NewReviewEvidence, RequiredCheck, ReviewCoverageEvidence, ReviewDecisionSource, ReviewVerdict,
    Task, TaskEventPayload, TaskStatus, WorkspaceDigest,
};
use coding_agent_runtime::WorktreeIdentity;
use coding_agent_store::{
    AcceptMergeCommandRequest, AttemptArtifactIdentity, CreateTaskOutcome, DeliveryOperationId,
    FinalizeReviewedTaskOutcome, GitBranchRef, GitCommitOid, MergeOperationRecord,
    PreflightCommandRequest, RegisterRepositoryOutcome, ReserveAttemptArtifact, Store,
    TaskTransition, TransitionOutcome,
};
use tokio_util::sync::CancellationToken;

use crate::support;

#[path = "../../../coding-agent-runtime/tests/delivery_source_support/mod.rs"]
mod real_git;

pub struct ProductionDeliveryFixture {
    _git: real_git::Fixture,
    pub store: Store,
    pub repository: coding_agent_domain::Repository,
    pub coordinator: Arc<RepositoryControlCoordinator>,
    pub manager: DeliveryManagerHandle,
    pub task: Task,
    pub source_branch: String,
    pub source_worktree: std::path::PathBuf,
    pub target_head: String,
}

impl ProductionDeliveryFixture {
    pub async fn new(name: &str) -> Self {
        let git = real_git::Fixture::new(name).await;
        real_git::git_ok(&git.repository, &["branch", "-M", "main"]);
        let store = Store::open(git.root.join("delivery.sqlite3"))
            .await
            .expect("open production delivery Store");
        store
            .migrate()
            .await
            .expect("migrate production delivery Store");
        let coordinator = Arc::new(RepositoryControlCoordinator::new());
        let instance_scope = support::instance_process_scope(&git.runtime_directory);
        // Build the exact factory-backed production registries while the Store
        // is empty. Opening the later repository proves lookup is live rather
        // than captured in a startup HashMap.
        let (runtime, live_runtime, cleanup_runtime) =
            production_delivery_dynamic_registries_for_test(
                store.clone(),
                Arc::clone(&git.delivery_git),
                git.toolchain.clone(),
                git.artifact_root().to_path_buf(),
                git.runtime_directory.clone(),
                Arc::clone(&coordinator),
                instance_scope.clone(),
            );
        let repository = match store
            .register_repository(NewRepository {
                selected_path: canonical(&git.repository),
                display_name: "production delivery fixture".to_owned(),
                git_root: canonical(&git.repository),
                cargo_workspace_root: canonical(&git.repository),
            })
            .await
            .expect("register production delivery repository")
        {
            RegisterRepositoryOutcome::Created(repository)
            | RegisterRepositoryOutcome::Existing(repository) => repository,
        };
        let lookup = store
            .repository_identity_lookup(repository.id)
            .await
            .expect("load late repository identity")
            .expect("late repository identity exists");
        coordinator
            .register_alias(
                lookup,
                &coding_agent_app::FilesystemRepositoryIdentityResolver,
            )
            .expect("register late repository coordination identity");
        let queued = match store
            .create_task(support::new_task(
                repository.id,
                "production delivery source drift",
            ))
            .await
            .expect("create production delivery task")
        {
            CreateTaskOutcome::Created { task, .. } => task,
            CreateTaskOutcome::Existing { .. } => panic!("fixture request is unique"),
        };
        let running = match store
            .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
            .await
            .expect("start production delivery task")
        {
            TransitionOutcome::Applied { task, .. } => task,
            TransitionOutcome::Conflict { .. } => panic!("fixture task starts once"),
        };

        let worktrees = Arc::new(git.worktree_provisioner_for_repository_with_scope(
            &repository.id.to_string(),
            git.task_process_scope(),
        ));
        let identity = WorktreeIdentity::try_new(
            repository.id.to_string(),
            running.id.to_string(),
            running.attempt,
        )
        .expect("valid production delivery identity");
        let reservation = worktrees
            .prepare(identity, CancellationToken::new())
            .await
            .expect("prepare real linked worktree");
        let provisioned = worktrees
            .provision_reserved(reservation.clone(), CancellationToken::new())
            .await
            .expect("provision real linked worktree");
        std::fs::write(
            provisioned.worktree_path().join("tracked.txt"),
            b"approved production change\n",
        )
        .expect("write approved tracked change");
        std::fs::write(
            provisioned.worktree_path().join("approved-note.txt"),
            b"approved untracked change\n",
        )
        .expect("write approved untracked change");
        let fingerprint = git.fingerprint_for_provisioned(&provisioned).await;
        let artifact_identity = AttemptArtifactIdentity {
            task_id: running.id,
            repository_id: running.repository_id,
            attempt: running.attempt,
        };
        store
            .reserve_attempt_artifact(ReserveAttemptArtifact {
                identity: artifact_identity,
                base_commit: reservation.base_commit().to_owned(),
                branch_name: reservation.branch_name().to_owned(),
                worktree_path: canonical(reservation.worktree_path()),
            })
            .await
            .expect("persist real worktree reservation");
        store
            .mark_attempt_artifact_ready(artifact_identity)
            .await
            .expect("mark real worktree ready");
        store
            .append_running_event(
                running.id,
                TaskEventPayload::PlanUpdated {
                    plan: support::fixture_review_plan(),
                },
            )
            .await
            .expect("persist production review plan");
        let task = match store
            .finalize_reviewed_task(
                running.id,
                running.repository_id,
                running.attempt,
                approved_review(fingerprint),
            )
            .await
            .expect("finalize production reviewed task")
        {
            FinalizeReviewedTaskOutcome::Applied { task, .. }
            | FinalizeReviewedTaskOutcome::Existing { task, .. } => task,
        };

        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 128)
            .await
            .expect("spawn production delivery dispatcher");
        let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher.clone()), 32);
        let launch_resources = TaskManagerLaunchResources::new_for_test(
            SchedulerConcurrencyLimits::try_new(2, 1).expect("valid production fixture limits"),
            Arc::clone(&coordinator),
            instance_scope,
        );
        let task_manager = TaskManagerHandle::spawn(
            store.clone(),
            writer.clone(),
            dispatcher,
            ServiceStateController::new(ServiceState::StoreDegraded),
            Arc::new(support::ControlledRunner::default()),
            launch_resources,
            32,
        );
        let dependencies =
            DeliveryManagerLiveDependencies::new_with_store_operation_query_for_test(
                store.clone(),
                writer,
                task_manager,
                Arc::clone(&coordinator),
                Arc::clone(&runtime),
                Arc::new(CleanProcessProofs),
            )
            .with_live_runtime_registry_for_test(live_runtime)
            .with_cleanup_runtime_registry_for_test(cleanup_runtime);
        let manager = DeliveryManagerHandle::spawn_live_for_test(
            dependencies,
            ServiceStateController::new(ServiceState::Ready),
            16,
        );
        let target_head = real_git::git_line(&git.repository, &["rev-parse", "refs/heads/main"]);

        Self {
            source_branch: reservation.branch_name().to_owned(),
            source_worktree: reservation.worktree_path().to_path_buf(),
            _git: git,
            store,
            repository,
            coordinator,
            manager,
            task,
            target_head,
        }
    }

    pub async fn prepare_accept(&self) -> (DeliveryOperationId, AcceptMergeCommandRequest) {
        let preflight = self
            .manager
            .preflight(DeliveryPreflightRequest::new(
                PreflightCommandRequest::try_new(
                    ClientRequestId::new(),
                    self.task.id,
                    GitBranchRef::from_str("refs/heads/main").expect("valid main branch"),
                    GitCommitOid::from_str(&self.target_head).expect("valid target head"),
                )
                .expect("valid production preflight"),
            ))
            .await
            .expect("production delivery manager remains open");
        let operation_id = match preflight {
            DeliveryPreflightOutcome::Durable(operation) => {
                if operation.state() != DeliveryPreflightState::PreflightReady {
                    let persisted = self.operation(operation.operation_id()).await;
                    panic!(
                        "expected real production ready preflight, got {operation:?}; persisted failure={:?}",
                        persisted.failure_code
                    );
                }
                operation.operation_id()
            }
            other => panic!("expected real production ready preflight, got {other:?}"),
        };
        let operation = self.operation(operation_id).await;
        let evidence = self
            .store
            .delivery_eligibility_snapshot(self.task.id)
            .await
            .expect("load production eligibility")
            .expect("production task exists")
            .evidence_identity
            .expect("approved evidence exists");
        let command = AcceptMergeCommandRequest::try_new(
            ClientRequestId::new(),
            self.task.id,
            operation_id,
            operation.version,
            evidence.workspace_generation(),
            evidence.workspace_fingerprint().clone(),
            operation.target_branch,
            operation.expected_target_head,
        )
        .expect("valid production accept command");
        (operation_id, command)
    }

    pub async fn accept(
        &self,
        command: AcceptMergeCommandRequest,
    ) -> coding_agent_app::DeliveryMergeAcceptanceOutcome {
        self.manager
            .accept_merge(DeliveryAcceptRequest::new(command))
            .await
            .expect("production delivery manager remains open")
    }

    pub async fn operation(&self, operation_id: DeliveryOperationId) -> MergeOperationRecord {
        match self
            .store
            .delivery_operation_snapshot(operation_id)
            .await
            .expect("load production operation")
            .expect("production operation exists")
        {
            coding_agent_store::DeliveryOperationSnapshot::Merge(operation) => *operation,
            coding_agent_store::DeliveryOperationSnapshot::Cleanup(_) => {
                panic!("expected production merge operation")
            }
        }
    }

    pub fn source_ref_oid(&self) -> String {
        real_git::git_line(
            &self._git.repository,
            &["rev-parse", &format!("refs/heads/{}", self.source_branch)],
        )
    }

    pub fn switch_target_to_new_branch(&self, branch: &str) {
        real_git::git_ok(&self._git.repository, &["switch", "-c", branch]);
    }

    pub async fn accept_receipt_count(&self) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_delivery_command_receipts \
             WHERE task_id = ? AND command_kind = 'accept_merge'",
        )
        .bind(self.task.id.to_string())
        .fetch_one(self.store.pool())
        .await
        .expect("count production accept receipts")
    }
}

struct CleanProcessProofs;

impl DeliveryProcessProofProviderTestSeam for CleanProcessProofs {}

#[async_trait::async_trait]
impl DeliveryProcessProofProvider for CleanProcessProofs {
    async fn observe(
        &self,
        _task_id: coding_agent_domain::TaskId,
    ) -> Result<DeliveryProcessProof, DeliveryProcessProofError> {
        Ok(DeliveryProcessProof::Clean)
    }
}

fn approved_review(fingerprint: WorkspaceFingerprint) -> NewReviewEvidence {
    let digest = WorkspaceDigest::try_new(
        fingerprint
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
    )
    .expect("real fingerprint is a workspace digest");
    let check = RequiredCheck::try_cargo_test("fixture-cargo-test", None, None)
        .expect("construct fixture required check");
    let evidence = CheckEvidence::try_for_check(
        &check,
        CheckActor::Executor,
        1,
        1,
        digest.clone(),
        CheckEvidenceStatus::Passed,
        1,
        "real Git fixture check passed",
        false,
    )
    .expect("construct real Git check evidence");
    let coverage = ReviewCoverageEvidence::try_new(1, digest.clone(), "f".repeat(64), vec![0], 1)
        .expect("construct real Git coverage");
    NewReviewEvidence::try_new(
        1,
        ReviewDecisionSource::Reviewer,
        1,
        digest,
        ReviewVerdict::Approved,
        "real Git production delivery approved",
        Vec::new(),
        Vec::new(),
        vec![check],
        vec![evidence],
        Some(coverage),
    )
    .expect("construct real Git approved review")
}

fn canonical(path: &std::path::Path) -> CanonicalPath {
    CanonicalPath::try_from_canonical(path.canonicalize().expect("canonicalize fixture path"))
        .expect("fixture path is canonical")
}
