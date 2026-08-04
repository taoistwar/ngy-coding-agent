use std::fmt;

use coding_agent_domain::TaskId;
use serde::{Deserialize, Serialize};

use super::requests::{
    AcceptMergeCommandRequest, DeleteBranchCommandRequest, PreflightCommandRequest,
    RemoveWorktreeCommandRequest,
};
use crate::delivery::{
    DeliveryCommandId, DeliveryError, DeliveryIdentity, DeliveryOperationId, DeliveryTimestamp,
    DeliveryVersion, Sha256Digest,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryCommandKind {
    Preflight,
    AcceptMerge,
    RemoveWorktree,
    DeleteBranch,
}

impl DeliveryCommandKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Preflight => "preflight",
            Self::AcceptMerge => "accept_merge",
            Self::RemoveWorktree => "remove_worktree",
            Self::DeleteBranch => "delete_branch",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, DeliveryError> {
        match value {
            "preflight" => Ok(Self::Preflight),
            "accept_merge" => Ok(Self::AcceptMerge),
            "remove_worktree" => Ok(Self::RemoveWorktree),
            "delete_branch" => Ok(Self::DeleteBranch),
            _ => Err(DeliveryError::InvalidCommandRequest),
        }
    }

    pub(super) const fn operation_kind(self) -> DeliveryOperationKind {
        match self {
            Self::Preflight | Self::AcceptMerge => DeliveryOperationKind::MergeOperation,
            Self::RemoveWorktree | Self::DeleteBranch => DeliveryOperationKind::CleanupOperation,
        }
    }

    pub(super) const fn response_discriminator(self) -> DeliveryResponseDiscriminator {
        match self {
            Self::Preflight => DeliveryResponseDiscriminator::PreflightCreated,
            Self::AcceptMerge => DeliveryResponseDiscriminator::MergeAccepted,
            Self::RemoveWorktree => DeliveryResponseDiscriminator::WorktreeCleanupAccepted,
            Self::DeleteBranch => DeliveryResponseDiscriminator::BranchCleanupAccepted,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeliveryAcceptedOperationState {
    PreflightPending,
    Accepted,
    UnlockPending,
    RemovePending,
    DeletePending,
}

impl DeliveryAcceptedOperationState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PreflightPending => "preflight_pending",
            Self::Accepted => "accepted",
            Self::UnlockPending => "unlock_pending",
            Self::RemovePending => "remove_pending",
            Self::DeletePending => "delete_pending",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, DeliveryError> {
        match value {
            "preflight_pending" => Ok(Self::PreflightPending),
            "accepted" => Ok(Self::Accepted),
            "unlock_pending" => Ok(Self::UnlockPending),
            "remove_pending" => Ok(Self::RemovePending),
            "delete_pending" => Ok(Self::DeletePending),
            _ => Err(DeliveryError::InvalidCommandRequest),
        }
    }

    pub(super) const fn accepts(self, kind: DeliveryCommandKind) -> bool {
        matches!(
            (kind, self),
            (DeliveryCommandKind::Preflight, Self::PreflightPending)
                | (DeliveryCommandKind::AcceptMerge, Self::Accepted)
                | (
                    DeliveryCommandKind::RemoveWorktree,
                    Self::UnlockPending | Self::RemovePending
                )
                | (DeliveryCommandKind::DeleteBranch, Self::DeletePending)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeliveryResponseDiscriminator {
    PreflightCreated,
    MergeAccepted,
    WorktreeCleanupAccepted,
    BranchCleanupAccepted,
}

impl DeliveryResponseDiscriminator {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PreflightCreated => "preflight_created",
            Self::MergeAccepted => "merge_accepted",
            Self::WorktreeCleanupAccepted => "worktree_cleanup_accepted",
            Self::BranchCleanupAccepted => "branch_cleanup_accepted",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, DeliveryError> {
        match value {
            "preflight_created" => Ok(Self::PreflightCreated),
            "merge_accepted" => Ok(Self::MergeAccepted),
            "worktree_cleanup_accepted" => Ok(Self::WorktreeCleanupAccepted),
            "branch_cleanup_accepted" => Ok(Self::BranchCleanupAccepted),
            _ => Err(DeliveryError::InvalidCommandRequest),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryCommandReceipt {
    pub client_request_id: DeliveryCommandId,
    pub command_kind: DeliveryCommandKind,
    pub identity: DeliveryIdentity,
    pub canonical_request_hash: Sha256Digest,
    pub operation_id: DeliveryOperationId,
    pub accepted_operation_version: DeliveryVersion,
    pub accepted_operation_state: DeliveryAcceptedOperationState,
    pub response_discriminator: DeliveryResponseDiscriminator,
    pub created_at: DeliveryTimestamp,
}

impl fmt::Debug for DeliveryCommandReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryCommandReceipt")
            .field("client_request_id", &self.client_request_id)
            .field("command_kind", &self.command_kind)
            .field("identity", &self.identity)
            .field("canonical_request_hash", &"<redacted>")
            .field("operation_id", &self.operation_id)
            .field(
                "accepted_operation_version",
                &self.accepted_operation_version,
            )
            .field("accepted_operation_state", &self.accepted_operation_state)
            .field("response_discriminator", &self.response_discriminator)
            .field("created_at", &self.created_at)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryCommand {
    Preflight(PreflightCommandRequest),
    AcceptMerge(AcceptMergeCommandRequest),
    RemoveWorktree(RemoveWorktreeCommandRequest),
    DeleteBranch(DeleteBranchCommandRequest),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryCommandLookup {
    Missing,
    Existing(DeliveryCommandReceipt),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeliveryOperationKind {
    MergeOperation,
    CleanupOperation,
}

impl DeliveryOperationKind {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::MergeOperation => "merge_operation",
            Self::CleanupOperation => "cleanup_operation",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, DeliveryError> {
        match value {
            "merge_operation" => Ok(Self::MergeOperation),
            "cleanup_operation" => Ok(Self::CleanupOperation),
            _ => Err(DeliveryError::InvalidCommandRequest),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::delivery) struct CommandRequestKey {
    pub(super) client_request_id: DeliveryCommandId,
    pub(super) task_id: TaskId,
    pub(super) command_kind: DeliveryCommandKind,
    pub(super) canonical_request_hash: Sha256Digest,
    pub(super) expected_accepted_version: DeliveryVersion,
    pub(super) action_anchor: CommandActionAnchor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CommandActionAnchor {
    NewOperation,
    ExistingOperation(DeliveryOperationId),
    CleanupFromMerge(DeliveryOperationId),
}

pub(in crate::delivery) trait CanonicalCommandRequest {
    fn command_request_key(&self) -> CommandRequestKey;
}

impl CanonicalCommandRequest for DeliveryCommand {
    fn command_request_key(&self) -> CommandRequestKey {
        match self {
            Self::Preflight(request) => request.command_request_key(),
            Self::AcceptMerge(request) => request.command_request_key(),
            Self::RemoveWorktree(request) => request.command_request_key(),
            Self::DeleteBranch(request) => request.command_request_key(),
        }
    }
}
