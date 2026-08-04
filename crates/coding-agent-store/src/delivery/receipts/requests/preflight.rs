use coding_agent_domain::{ClientRequestId, TaskId};
use serde::{Deserialize, Serialize};

use super::{domain_client_request_id, parse_task_id, validate_request_ids};
use crate::delivery::receipts::hash;
use crate::delivery::receipts::model::{
    CanonicalCommandRequest, CommandActionAnchor, CommandRequestKey, DeliveryCommandKind,
};
use crate::delivery::{
    DeliveryCommandId, DeliveryError, DeliveryVersion, GitBranchRef, GitCommitOid, Sha256Digest,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawPreflightCommandRequest")]
pub struct PreflightCommandRequest {
    client_request_id: DeliveryCommandId,
    task_id: TaskId,
    target_branch: GitBranchRef,
    expected_target_head: GitCommitOid,
}

impl PreflightCommandRequest {
    pub fn try_new(
        client_request_id: ClientRequestId,
        task_id: TaskId,
        target_branch: GitBranchRef,
        expected_target_head: GitCommitOid,
    ) -> Result<Self, DeliveryError> {
        let client_request_id = validate_request_ids(client_request_id, task_id)?;
        Ok(Self {
            client_request_id,
            task_id,
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

    pub const fn target_branch(&self) -> &GitBranchRef {
        &self.target_branch
    }

    pub const fn expected_target_head(&self) -> &GitCommitOid {
        &self.expected_target_head
    }

    pub fn canonical_request_hash(&self) -> Sha256Digest {
        hash::preflight(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPreflightCommandRequest {
    client_request_id: DeliveryCommandId,
    task_id: String,
    target_branch: GitBranchRef,
    expected_target_head: GitCommitOid,
}

impl TryFrom<RawPreflightCommandRequest> for PreflightCommandRequest {
    type Error = DeliveryError;

    fn try_from(raw: RawPreflightCommandRequest) -> Result<Self, Self::Error> {
        Self::try_new(
            domain_client_request_id(raw.client_request_id),
            parse_task_id(&raw.task_id)?,
            raw.target_branch,
            raw.expected_target_head,
        )
    }
}

impl CanonicalCommandRequest for PreflightCommandRequest {
    fn command_request_key(&self) -> CommandRequestKey {
        CommandRequestKey {
            client_request_id: self.client_request_id,
            task_id: self.task_id,
            command_kind: DeliveryCommandKind::Preflight,
            canonical_request_hash: self.canonical_request_hash(),
            expected_accepted_version: DeliveryVersion::initial(),
            action_anchor: CommandActionAnchor::NewOperation,
        }
    }
}
