mod cleanup;
mod live;
mod preflight;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coding_agent_core::WorkspaceFingerprint;
use coding_agent_domain::{Repository, RepositoryId};
use coding_agent_runtime::{
    DeliverySourceLimits, DeliverySourceProvisioner, DeliveryTargetProvisioner,
    DeliveryTargetRequest, DeliveryWorktreeCleanupProvisioner, FingerprintLimits,
    ProbedDeliveryGit, ProcessLimits, ProcessLivenessScope, WorktreeIdentity, WorktreeProvisioner,
    WorktreeReservation,
};
use coding_agent_store::{
    AttemptArtifactState, DeliveryEligibilitySnapshot, MergeOperationState, Store,
};

use super::super::ProductionWorktreeProvisioners;
#[cfg(feature = "test-support")]
use super::super::{TestDeliveryProcessFaultBoundary, TestDeliveryTargetBoundary};
use super::PreparedDeliveryRuntime;
use crate::delivery_manager::delivery_process_scope_id;
use crate::{RepositoryControlCoordinator, RepositoryCoordinationKey};

const DELIVERY_GIT_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const DELIVERY_GIT_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

pub(in crate::runner_factory) fn production_delivery_runtime(
    store: Store,
    probe: Arc<ProbedDeliveryGit>,
    provisioners: Arc<ProductionWorktreeProvisioners>,
    repository_control: Arc<RepositoryControlCoordinator>,
) -> PreparedDeliveryRuntime {
    let registry = Arc::new(ProductionDeliveryRegistry {
        store,
        probe,
        instance_process_scope: provisioners.instance_process_scope.clone(),
        worktrees: ProductionDeliveryWorktrees::Factory(provisioners),
        repository_control,
        #[cfg(feature = "test-support")]
        test_target_boundary: None,
        #[cfg(feature = "test-support")]
        test_process_fault: None,
    });
    PreparedDeliveryRuntime::new(registry.clone(), registry.clone(), registry)
}

#[cfg(feature = "test-support")]
pub(in crate::runner_factory) fn production_delivery_runtime_with_test_support(
    store: Store,
    probe: Arc<ProbedDeliveryGit>,
    provisioners: Arc<ProductionWorktreeProvisioners>,
    repository_control: Arc<RepositoryControlCoordinator>,
    target_boundary: Option<TestDeliveryTargetBoundary>,
    process_fault: Option<TestDeliveryProcessFaultBoundary>,
) -> PreparedDeliveryRuntime {
    let registry = Arc::new(ProductionDeliveryRegistry {
        store,
        probe,
        instance_process_scope: provisioners.instance_process_scope.clone(),
        worktrees: ProductionDeliveryWorktrees::Factory(provisioners),
        repository_control,
        test_target_boundary: target_boundary,
        test_process_fault: process_fault,
    });
    PreparedDeliveryRuntime::new(registry.clone(), registry.clone(), registry)
}

pub(super) struct ProductionDeliveryRegistry {
    store: Store,
    probe: Arc<ProbedDeliveryGit>,
    worktrees: ProductionDeliveryWorktrees,
    repository_control: Arc<RepositoryControlCoordinator>,
    instance_process_scope: ProcessLivenessScope,
    #[cfg(feature = "test-support")]
    test_target_boundary: Option<TestDeliveryTargetBoundary>,
    #[cfg(feature = "test-support")]
    test_process_fault: Option<TestDeliveryProcessFaultBoundary>,
}

enum ProductionDeliveryWorktrees {
    Factory(Arc<ProductionWorktreeProvisioners>),
    #[cfg(feature = "test-support")]
    Fixed {
        repository_id: RepositoryId,
        provisioner: Arc<WorktreeProvisioner>,
        temporary_directory: PathBuf,
    },
}

impl ProductionDeliveryWorktrees {
    fn build(
        &self,
        repository: &Repository,
        process_scope: ProcessLivenessScope,
    ) -> Result<Arc<WorktreeProvisioner>, ProductionBindingError> {
        match self {
            Self::Factory(factory) => factory
                .build_provisioner(repository, process_scope)
                .map_err(|_| ProductionBindingError),
            #[cfg(feature = "test-support")]
            Self::Fixed {
                repository_id,
                provisioner,
                ..
            } if *repository_id == repository.id => Ok(Arc::clone(provisioner)),
            #[cfg(feature = "test-support")]
            Self::Fixed { .. } => Err(ProductionBindingError),
        }
    }

    fn temporary_directory(&self) -> &std::path::Path {
        match self {
            Self::Factory(factory) => &factory.temporary_directory,
            #[cfg(feature = "test-support")]
            Self::Fixed {
                temporary_directory,
                ..
            } => temporary_directory,
        }
    }
}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn production_delivery_registries_for_test(
    store: Store,
    probe: Arc<ProbedDeliveryGit>,
    provisioner: Arc<WorktreeProvisioner>,
    temporary_directory: PathBuf,
    repository: Repository,
    repository_control: Arc<RepositoryControlCoordinator>,
    instance_process_scope: ProcessLivenessScope,
) -> (
    Arc<dyn crate::DeliveryRuntimeRegistry>,
    Arc<dyn crate::DeliveryLiveRuntimeRegistry>,
    Arc<dyn crate::DeliveryCleanupRuntimeRegistry>,
) {
    let repository_id = repository.id;
    let registry = Arc::new(ProductionDeliveryRegistry {
        store,
        probe,
        worktrees: ProductionDeliveryWorktrees::Fixed {
            repository_id,
            provisioner,
            temporary_directory,
        },
        repository_control,
        instance_process_scope,
        test_target_boundary: None,
        test_process_fault: None,
    });
    (registry.clone(), registry.clone(), registry)
}

pub(super) struct ProductionDeliverySession {
    store: Store,
    snapshot: DeliveryEligibilitySnapshot,
    coordination_key: RepositoryCoordinationKey,
    source: Arc<DeliverySourceProvisioner>,
    target: Arc<DeliveryTargetProvisioner>,
    cleanup: Arc<DeliveryWorktreeCleanupProvisioner>,
    reservation: WorktreeReservation,
    worker_process_scope: ProcessLivenessScope,
    target_request: Mutex<Option<DeliveryTargetRequest>>,
    #[cfg(feature = "test-support")]
    test_process_fault: Option<TestDeliveryProcessFaultBoundary>,
}

impl ProductionDeliveryRegistry {
    async fn open(
        &self,
        snapshot: &DeliveryEligibilitySnapshot,
    ) -> Result<ProductionDeliverySession, ProductionBindingError> {
        let artifact = snapshot
            .ownership
            .artifact
            .as_ref()
            .ok_or(ProductionBindingError)?;
        let evidence = snapshot
            .evidence_identity
            .as_ref()
            .ok_or(ProductionBindingError)?;
        if artifact.state != AttemptArtifactState::Ready
            || artifact.identity.task_id != snapshot.task.id
            || artifact.identity.repository_id != snapshot.task.repository_id
            || artifact.identity.attempt != snapshot.task.attempt
            || evidence.identity().task_id() != snapshot.task.id
            || evidence.identity().repository_id() != snapshot.task.repository_id
            || evidence.identity().attempt() != snapshot.task.attempt
        {
            return Err(ProductionBindingError);
        }
        let repository = self
            .store
            .list_repositories()
            .await
            .map_err(|_| ProductionBindingError)?
            .into_iter()
            .find(|repository| repository.id == snapshot.task.repository_id)
            .ok_or(ProductionBindingError)?;
        #[cfg(feature = "test-support")]
        let test_process_fault = self
            .test_process_fault
            .as_ref()
            .filter(|fault| fault.matches(&repository))
            .cloned();
        let task_selector = *snapshot.task.id.as_uuid().as_bytes();
        let worker_process_scope = self
            .instance_process_scope
            .task_scope(task_selector)
            .map_err(|_| ProductionBindingError)?;
        let delivery_process_scope = self
            .instance_process_scope
            .task_scope(delivery_process_scope_id(snapshot.task.id))
            .map_err(|_| ProductionBindingError)?;
        let worktrees = self
            .worktrees
            .build(&repository, worker_process_scope.clone())?;
        let identity = WorktreeIdentity::try_new(
            snapshot.task.repository_id.to_string(),
            snapshot.task.id.to_string(),
            snapshot.task.attempt,
        )
        .map_err(|_| ProductionBindingError)?;
        let reservation = worktrees
            .restore_reservation(
                identity,
                artifact.base_commit.clone(),
                artifact.branch_name.clone(),
                artifact.worktree_path.as_path().to_path_buf(),
            )
            .map_err(|_| ProductionBindingError)?;
        let process_limits = production_delivery_process_limits();
        let source_limits = production_delivery_source_limits();
        let source = Arc::new(
            DeliverySourceProvisioner::from_worktree_provisioner(
                worktrees.as_ref(),
                Arc::clone(&self.probe),
                self.worktrees.temporary_directory(),
                delivery_process_scope.clone(),
                process_limits,
                source_limits,
                production_delivery_fingerprint_limits(),
            )
            .map_err(|_| ProductionBindingError)?,
        );
        let mut target = DeliveryTargetProvisioner::from_worktree_provisioner(
            worktrees.as_ref(),
            Arc::clone(&self.probe),
            self.worktrees.temporary_directory(),
            delivery_process_scope.clone(),
            process_limits,
            source_limits,
        )
        .map_err(|_| ProductionBindingError)?;
        #[cfg(feature = "test-support")]
        if let Some(boundary) = self
            .test_target_boundary
            .as_ref()
            .filter(|boundary| boundary.matches(&repository))
        {
            let hook = boundary.hook();
            target.set_actual_merge_boundary_hook_for_tests(move |phase| hook(phase));
        }
        let target = Arc::new(target);
        let cleanup = Arc::new(
            DeliveryWorktreeCleanupProvisioner::from_worktree_provisioner(
                worktrees.as_ref(),
                Arc::clone(&self.probe),
                self.worktrees.temporary_directory(),
                delivery_process_scope,
                process_limits,
                source_limits,
            )
            .map_err(|_| ProductionBindingError)?,
        );
        let target_request = snapshot
            .ownership
            .merge_operations
            .iter()
            .find(|operation| operation.state == MergeOperationState::PreflightPending)
            .map(target_request_from_operation)
            .transpose()?;
        Ok(ProductionDeliverySession {
            store: self.store.clone(),
            snapshot: snapshot.clone(),
            coordination_key: self
                .repository_control
                .delivery_coordination_key(snapshot.task.repository_id)
                .map_err(|_| ProductionBindingError)?,
            source,
            target,
            cleanup,
            reservation,
            worker_process_scope,
            target_request: Mutex::new(target_request),
            #[cfg(feature = "test-support")]
            test_process_fault,
        })
    }
}

fn target_request_from_operation(
    operation: &coding_agent_store::MergeOperationRecord,
) -> Result<DeliveryTargetRequest, ProductionBindingError> {
    DeliveryTargetRequest::try_new(
        local_branch_name(operation.target_branch.as_str())?,
        operation.expected_target_head.as_str(),
    )
    .map_err(|_| ProductionBindingError)
}

fn local_branch_name(reference: &str) -> Result<&str, ProductionBindingError> {
    reference
        .strip_prefix("refs/heads/")
        .filter(|branch| !branch.is_empty())
        .ok_or(ProductionBindingError)
}

fn approved_fingerprint(
    snapshot: &DeliveryEligibilitySnapshot,
) -> Result<WorkspaceFingerprint, ProductionBindingError> {
    let value = snapshot
        .evidence_identity
        .as_ref()
        .ok_or(ProductionBindingError)?
        .workspace_fingerprint()
        .as_str();
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(pair).map_err(|_| ProductionBindingError)?;
        bytes[index] = u8::from_str_radix(pair, 16).map_err(|_| ProductionBindingError)?;
    }
    Ok(WorkspaceFingerprint::from_bytes(bytes))
}

fn production_delivery_process_limits() -> ProcessLimits {
    ProcessLimits::try_new(
        512 * 1024,
        512 * 1024,
        DELIVERY_GIT_TIMEOUT,
        DELIVERY_GIT_CLEANUP_TIMEOUT,
    )
    .expect("constant delivery process limits are valid")
}

fn production_delivery_source_limits() -> DeliverySourceLimits {
    DeliverySourceLimits::try_new(
        DELIVERY_GIT_TIMEOUT,
        512 * 1024,
        64 * 1024,
        64 * 1024,
        4_096,
    )
    .expect("constant delivery source limits are valid")
}

fn production_delivery_fingerprint_limits() -> FingerprintLimits {
    FingerprintLimits::try_new(
        DELIVERY_GIT_TIMEOUT,
        4_096,
        2 * 1024 * 1024,
        32 * 1024 * 1024,
    )
    .expect("constant delivery fingerprint limits are valid")
}

#[derive(Debug, Clone, Copy)]
struct ProductionBindingError;
