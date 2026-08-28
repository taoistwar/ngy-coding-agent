use std::str::FromStr;
use std::sync::Arc;

use coding_agent_app::{
    DeliveryPreparedPreflight, DeliveryRuntimeAuthentication, DeliveryRuntimeAuthenticationOutcome,
    DeliveryRuntimeFailure, DeliveryRuntimeObservation, DeliveryRuntimeRegistry,
    DeliveryRuntimeRegistryTestSeam, DeliveryRuntimeSession, DeliveryRuntimeSessionTestSeam,
    RepositoryControlCoordinator,
};
use coding_agent_store::{
    DeliveryEligibilitySnapshot, DeliveryIdentity, GitBranchRef, GitCommitOid, GitObjectAlgorithm,
    GitTreeOid, MergePreflightResult, PreflightCommandRequest, Sha256Digest,
};

use super::{
    ADMIN_IDENTITY, CANDIDATE_TREE, COMMON_IDENTITY, MERGE_BASE, MERGE_TREE, PREFLIGHT_SOURCE,
    SOURCE_CONFIG_DIGEST, TARGET_CONFIG_DIGEST, TARGET_HEAD, TARGET_SECURITY_DIGEST,
};

pub struct PreflightRuntime {
    coordinator: Arc<RepositoryControlCoordinator>,
}

impl PreflightRuntime {
    pub fn new(coordinator: Arc<RepositoryControlCoordinator>) -> Arc<Self> {
        Arc::new(Self { coordinator })
    }
}

impl DeliveryRuntimeRegistryTestSeam for PreflightRuntime {}

#[async_trait::async_trait]
impl DeliveryRuntimeRegistry for PreflightRuntime {
    async fn open_session(
        &self,
        snapshot: &DeliveryEligibilitySnapshot,
    ) -> Result<Arc<dyn DeliveryRuntimeSession>, DeliveryRuntimeFailure> {
        let evidence = snapshot
            .evidence_identity
            .as_ref()
            .ok_or(DeliveryRuntimeFailure::Unavailable)?;
        let artifact = snapshot
            .ownership
            .artifact
            .as_ref()
            .ok_or(DeliveryRuntimeFailure::Unavailable)?;
        Ok(Arc::new(PreflightSession {
            coordination_key: self
                .coordinator
                .coordination_key(snapshot.task.repository_id)
                .map_err(|_| DeliveryRuntimeFailure::Unavailable)?,
            source_identity: evidence.identity(),
            source_base_commit: GitCommitOid::from_str(&artifact.base_commit)
                .map_err(|_| DeliveryRuntimeFailure::Unavailable)?,
            source_branch: GitBranchRef::from_str(&format!("refs/heads/{}", artifact.branch_name))
                .map_err(|_| DeliveryRuntimeFailure::Unavailable)?,
            approved_workspace_fingerprint: evidence.workspace_fingerprint().clone(),
        }))
    }
}

struct PreflightSession {
    coordination_key: coding_agent_app::RepositoryCoordinationKey,
    source_identity: DeliveryIdentity,
    source_base_commit: GitCommitOid,
    source_branch: GitBranchRef,
    approved_workspace_fingerprint: Sha256Digest,
}

impl DeliveryRuntimeSessionTestSeam for PreflightSession {}

#[async_trait::async_trait]
impl DeliveryRuntimeSession for PreflightSession {
    async fn observe(&self) -> Result<DeliveryRuntimeObservation, DeliveryRuntimeFailure> {
        Ok(DeliveryRuntimeObservation::available_for_test(
            GitBranchRef::from_str("refs/heads/main").expect("valid target branch"),
            GitCommitOid::from_str(TARGET_HEAD).expect("valid target head"),
        ))
    }

    async fn authenticate_preflight(
        &self,
        command: &PreflightCommandRequest,
    ) -> Result<DeliveryRuntimeAuthenticationOutcome, DeliveryRuntimeFailure> {
        let authentication = DeliveryRuntimeAuthentication::new_for_test(
            self.coordination_key,
            self.source_identity,
            self.source_base_commit.clone(),
            self.source_branch.clone(),
            self.approved_workspace_fingerprint.clone(),
            GitObjectAlgorithm::Sha1,
            coding_agent_store::DirectoryIdentity::try_new(
                "directory_identity_v1",
                COMMON_IDENTITY,
            )
            .expect("valid common identity"),
            coding_agent_store::DirectoryIdentity::try_new("directory_identity_v1", ADMIN_IDENTITY)
                .expect("valid admin identity"),
            Sha256Digest::from_str(SOURCE_CONFIG_DIGEST).expect("valid source config digest"),
            command.target_branch().clone(),
            command.expected_target_head().clone(),
            Sha256Digest::from_str(TARGET_CONFIG_DIGEST).expect("valid target config digest"),
            Sha256Digest::from_str(TARGET_SECURITY_DIGEST).expect("valid target security digest"),
        )?;
        Ok(DeliveryRuntimeAuthenticationOutcome::Ready(authentication))
    }

    async fn prepare_preflight(&self) -> Result<DeliveryPreparedPreflight, DeliveryRuntimeFailure> {
        Ok(DeliveryPreparedPreflight::new_for_test(
            GitTreeOid::from_str(CANDIDATE_TREE).expect("valid candidate tree"),
            GitCommitOid::from_str(PREFLIGHT_SOURCE).expect("valid preflight source"),
            (),
        ))
    }

    async fn run_preflight(
        &self,
        _prepared: &DeliveryPreparedPreflight,
    ) -> Result<MergePreflightResult, DeliveryRuntimeFailure> {
        Ok(MergePreflightResult::ready(
            GitCommitOid::from_str(MERGE_BASE).expect("valid merge base"),
            GitTreeOid::from_str(MERGE_TREE).expect("valid merge tree"),
        )
        .expect("valid ready preflight"))
    }
}
