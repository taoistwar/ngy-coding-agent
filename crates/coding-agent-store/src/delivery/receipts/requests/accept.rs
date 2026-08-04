use coding_agent_domain::{ClientRequestId, TaskId};
use serde::{Deserialize, Serialize};

use super::{domain_client_request_id, parse_task_id, validate_request_ids};
use crate::delivery::receipts::hash;
use crate::delivery::receipts::model::{
    CanonicalCommandRequest, CommandActionAnchor, CommandRequestKey, DeliveryCommandKind,
};
use crate::delivery::{
    DeliveryCommandId, DeliveryError, DeliveryOperationId, DeliveryVersion, GitBranchRef,
    GitCommitOid, Sha256Digest,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawAcceptMergeCommandRequest")]
pub struct AcceptMergeCommandRequest {
    client_request_id: DeliveryCommandId,
    task_id: TaskId,
    preflight_operation_id: DeliveryOperationId,
    expected_operation_version: DeliveryVersion,
    expected_review_generation: u64,
    expected_workspace_fingerprint: Sha256Digest,
    target_branch: GitBranchRef,
    expected_target_head: GitCommitOid,
}

impl AcceptMergeCommandRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        client_request_id: ClientRequestId,
        task_id: TaskId,
        preflight_operation_id: DeliveryOperationId,
        expected_operation_version: DeliveryVersion,
        expected_review_generation: u64,
        expected_workspace_fingerprint: Sha256Digest,
        target_branch: GitBranchRef,
        expected_target_head: GitCommitOid,
    ) -> Result<Self, DeliveryError> {
        let client_request_id = validate_request_ids(client_request_id, task_id)?;
        if expected_review_generation > DeliveryVersion::MAX
            || expected_operation_version.next().is_err()
        {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        Ok(Self {
            client_request_id,
            task_id,
            preflight_operation_id,
            expected_operation_version,
            expected_review_generation,
            expected_workspace_fingerprint,
            target_branch,
            expected_target_head,
        })
    }

    pub const fn client_request_id(&self) -> DeliveryCommandId {
        self.client_request_id
    }

    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub const fn preflight_operation_id(&self) -> DeliveryOperationId {
        self.preflight_operation_id
    }

    pub const fn expected_operation_version(&self) -> DeliveryVersion {
        self.expected_operation_version
    }

    pub const fn expected_review_generation(&self) -> u64 {
        self.expected_review_generation
    }

    pub const fn expected_workspace_fingerprint(&self) -> &Sha256Digest {
        &self.expected_workspace_fingerprint
    }

    pub const fn target_branch(&self) -> &GitBranchRef {
        &self.target_branch
    }

    pub const fn expected_target_head(&self) -> &GitCommitOid {
        &self.expected_target_head
    }

    pub fn canonical_request_hash(&self) -> Sha256Digest {
        hash::accept_merge(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAcceptMergeCommandRequest {
    client_request_id: DeliveryCommandId,
    task_id: String,
    preflight_operation_id: DeliveryOperationId,
    expected_operation_version: DeliveryVersion,
    expected_review_generation: u64,
    expected_workspace_fingerprint: Sha256Digest,
    target_branch: GitBranchRef,
    expected_target_head: GitCommitOid,
}

impl TryFrom<RawAcceptMergeCommandRequest> for AcceptMergeCommandRequest {
    type Error = DeliveryError;

    fn try_from(raw: RawAcceptMergeCommandRequest) -> Result<Self, Self::Error> {
        Self::try_new(
            domain_client_request_id(raw.client_request_id),
            parse_task_id(&raw.task_id)?,
            raw.preflight_operation_id,
            raw.expected_operation_version,
            raw.expected_review_generation,
            raw.expected_workspace_fingerprint,
            raw.target_branch,
            raw.expected_target_head,
        )
    }
}

impl CanonicalCommandRequest for AcceptMergeCommandRequest {
    fn command_request_key(&self) -> CommandRequestKey {
        CommandRequestKey {
            client_request_id: self.client_request_id,
            task_id: self.task_id,
            command_kind: DeliveryCommandKind::AcceptMerge,
            canonical_request_hash: self.canonical_request_hash(),
            expected_accepted_version: self
                .expected_operation_version
                .next()
                .expect("accept requests validate their next operation version"),
            action_anchor: CommandActionAnchor::ExistingOperation(self.preflight_operation_id),
        }
    }
}
