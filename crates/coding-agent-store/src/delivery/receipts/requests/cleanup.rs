use coding_agent_domain::{ClientRequestId, TaskId};
use serde::{Deserialize, Serialize};

use super::{domain_client_request_id, parse_task_id, validate_request_ids};
use crate::delivery::mutation::{
    DeliveryMutationEntity, DeliveryMutationEntityKind, DeliveryMutationKey, DeliveryMutationKind,
    DeliveryMutationReceiptIdentity, impl_delivery_mutation_request,
};
use crate::delivery::receipts::hash;
use crate::delivery::receipts::model::{
    CanonicalCommandRequest, CommandActionAnchor, CommandRequestKey, DeliveryCommandKind,
};
use crate::delivery::{
    DeliveryCommandId, DeliveryError, DeliveryOperationId, DeliveryVersion, GitBranchRef,
    GitCommitOid, Sha256Digest,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawRemoveWorktreeCommandRequest")]
pub struct RemoveWorktreeCommandRequest {
    client_request_id: DeliveryCommandId,
    task_id: TaskId,
    expected_disposition_version: DeliveryVersion,
    expected_merge_operation_id: DeliveryOperationId,
    expected_source_ref: GitBranchRef,
    expected_source_oid: GitCommitOid,
}

impl RemoveWorktreeCommandRequest {
    pub fn try_new(
        client_request_id: ClientRequestId,
        task_id: TaskId,
        expected_disposition_version: DeliveryVersion,
        expected_merge_operation_id: DeliveryOperationId,
        expected_source_ref: GitBranchRef,
        expected_source_oid: GitCommitOid,
    ) -> Result<Self, DeliveryError> {
        let client_request_id = validate_request_ids(client_request_id, task_id)?;
        Ok(Self {
            client_request_id,
            task_id,
            expected_disposition_version,
            expected_merge_operation_id,
            expected_source_ref,
            expected_source_oid,
        })
    }

    pub const fn client_request_id(&self) -> DeliveryCommandId {
        self.client_request_id
    }

    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub const fn expected_disposition_version(&self) -> DeliveryVersion {
        self.expected_disposition_version
    }

    pub const fn expected_merge_operation_id(&self) -> DeliveryOperationId {
        self.expected_merge_operation_id
    }

    pub const fn expected_source_ref(&self) -> &GitBranchRef {
        &self.expected_source_ref
    }

    pub const fn expected_source_oid(&self) -> &GitCommitOid {
        &self.expected_source_oid
    }

    pub fn canonical_request_hash(&self) -> Sha256Digest {
        hash::remove_worktree(self)
    }
}

impl_delivery_mutation_request!(RemoveWorktreeCommandRequest, |request| {
    cleanup_acceptance_key(
        DeliveryMutationKind::AcceptWorktreeCleanup,
        DeliveryMutationEntityKind::WorktreeDisposition,
        request.client_request_id,
        request.task_id,
        request.expected_disposition_version,
        request.expected_merge_operation_id,
        DeliveryCommandKind::RemoveWorktree,
        request.canonical_request_hash(),
    )
});

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRemoveWorktreeCommandRequest {
    client_request_id: DeliveryCommandId,
    task_id: String,
    expected_disposition_version: DeliveryVersion,
    expected_merge_operation_id: DeliveryOperationId,
    expected_source_ref: GitBranchRef,
    expected_source_oid: GitCommitOid,
}

impl TryFrom<RawRemoveWorktreeCommandRequest> for RemoveWorktreeCommandRequest {
    type Error = DeliveryError;

    fn try_from(raw: RawRemoveWorktreeCommandRequest) -> Result<Self, Self::Error> {
        Self::try_new(
            domain_client_request_id(raw.client_request_id),
            parse_task_id(&raw.task_id)?,
            raw.expected_disposition_version,
            raw.expected_merge_operation_id,
            raw.expected_source_ref,
            raw.expected_source_oid,
        )
    }
}

impl CanonicalCommandRequest for RemoveWorktreeCommandRequest {
    fn command_request_key(&self) -> CommandRequestKey {
        CommandRequestKey {
            client_request_id: self.client_request_id,
            task_id: self.task_id,
            command_kind: DeliveryCommandKind::RemoveWorktree,
            canonical_request_hash: self.canonical_request_hash(),
            expected_accepted_version: DeliveryVersion::initial(),
            action_anchor: CommandActionAnchor::CleanupFromMerge(self.expected_merge_operation_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawDeleteBranchCommandRequest")]
pub struct DeleteBranchCommandRequest {
    client_request_id: DeliveryCommandId,
    task_id: TaskId,
    expected_disposition_version: DeliveryVersion,
    expected_merge_operation_id: DeliveryOperationId,
    expected_source_ref: GitBranchRef,
    expected_source_oid: GitCommitOid,
    target_branch: GitBranchRef,
    target_head: GitCommitOid,
}

impl DeleteBranchCommandRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        client_request_id: ClientRequestId,
        task_id: TaskId,
        expected_disposition_version: DeliveryVersion,
        expected_merge_operation_id: DeliveryOperationId,
        expected_source_ref: GitBranchRef,
        expected_source_oid: GitCommitOid,
        target_branch: GitBranchRef,
        target_head: GitCommitOid,
    ) -> Result<Self, DeliveryError> {
        let client_request_id = validate_request_ids(client_request_id, task_id)?;
        if expected_source_oid.algorithm() != target_head.algorithm() {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        Ok(Self {
            client_request_id,
            task_id,
            expected_disposition_version,
            expected_merge_operation_id,
            expected_source_ref,
            expected_source_oid,
            target_branch,
            target_head,
        })
    }

    pub const fn client_request_id(&self) -> DeliveryCommandId {
        self.client_request_id
    }

    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub const fn expected_disposition_version(&self) -> DeliveryVersion {
        self.expected_disposition_version
    }

    pub const fn expected_merge_operation_id(&self) -> DeliveryOperationId {
        self.expected_merge_operation_id
    }

    pub const fn expected_source_ref(&self) -> &GitBranchRef {
        &self.expected_source_ref
    }

    pub const fn expected_source_oid(&self) -> &GitCommitOid {
        &self.expected_source_oid
    }

    pub const fn target_branch(&self) -> &GitBranchRef {
        &self.target_branch
    }

    pub const fn target_head(&self) -> &GitCommitOid {
        &self.target_head
    }

    pub fn canonical_request_hash(&self) -> Sha256Digest {
        hash::delete_branch(self)
    }
}

impl_delivery_mutation_request!(DeleteBranchCommandRequest, |request| {
    cleanup_acceptance_key(
        DeliveryMutationKind::AcceptBranchCleanup,
        DeliveryMutationEntityKind::BranchDisposition,
        request.client_request_id,
        request.task_id,
        request.expected_disposition_version,
        request.expected_merge_operation_id,
        DeliveryCommandKind::DeleteBranch,
        request.canonical_request_hash(),
    )
});

#[allow(clippy::too_many_arguments)]
fn cleanup_acceptance_key(
    kind: DeliveryMutationKind,
    disposition_kind: DeliveryMutationEntityKind,
    client_request_id: DeliveryCommandId,
    task_id: TaskId,
    expected_disposition_version: DeliveryVersion,
    expected_merge_operation_id: DeliveryOperationId,
    command_kind: DeliveryCommandKind,
    canonical_request_hash: Sha256Digest,
) -> DeliveryMutationKey {
    DeliveryMutationKey::new(
        kind,
        task_id,
        vec![
            DeliveryMutationEntity::pending(DeliveryMutationEntityKind::CleanupOperation),
            DeliveryMutationEntity::task(
                disposition_kind,
                task_id,
                Some(expected_disposition_version),
            ),
        ],
        Some(DeliveryMutationReceiptIdentity::new(
            client_request_id,
            command_kind,
            canonical_request_hash,
            DeliveryVersion::initial(),
            Some(expected_merge_operation_id),
        )),
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDeleteBranchCommandRequest {
    client_request_id: DeliveryCommandId,
    task_id: String,
    expected_disposition_version: DeliveryVersion,
    expected_merge_operation_id: DeliveryOperationId,
    expected_source_ref: GitBranchRef,
    expected_source_oid: GitCommitOid,
    target_branch: GitBranchRef,
    target_head: GitCommitOid,
}

impl TryFrom<RawDeleteBranchCommandRequest> for DeleteBranchCommandRequest {
    type Error = DeliveryError;

    fn try_from(raw: RawDeleteBranchCommandRequest) -> Result<Self, Self::Error> {
        Self::try_new(
            domain_client_request_id(raw.client_request_id),
            parse_task_id(&raw.task_id)?,
            raw.expected_disposition_version,
            raw.expected_merge_operation_id,
            raw.expected_source_ref,
            raw.expected_source_oid,
            raw.target_branch,
            raw.target_head,
        )
    }
}

impl CanonicalCommandRequest for DeleteBranchCommandRequest {
    fn command_request_key(&self) -> CommandRequestKey {
        CommandRequestKey {
            client_request_id: self.client_request_id,
            task_id: self.task_id,
            command_kind: DeliveryCommandKind::DeleteBranch,
            canonical_request_hash: self.canonical_request_hash(),
            expected_accepted_version: DeliveryVersion::initial(),
            action_anchor: CommandActionAnchor::CleanupFromMerge(self.expected_merge_operation_id),
        }
    }
}
