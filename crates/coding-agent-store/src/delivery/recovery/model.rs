use std::fmt;

use super::audit::AuditedDeliveryOwnership;
use crate::delivery::{
    DeliveryIdentity, DeliveryOperationId, DeliveryOwnershipSnapshot, DeliveryVersion,
    DirectoryIdentity,
};

pub const MAX_DELIVERY_RECOVERY_BATCH: usize = 64;

/// Opaque, identity-bound continuation token produced by a recovery batch.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryRecoveryCursor {
    pub(super) authenticated_identity: DirectoryIdentity,
    pub(super) initial_transition_id: i64,
    pub(super) entity_rank: u8,
    pub(super) canonical_id: String,
}

impl fmt::Debug for DeliveryRecoveryCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("DeliveryRecoveryCursor")
            .field(&"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DeliveryRecoveryQueryError {
    #[error("delivery recovery cursor belongs to a different authenticated identity")]
    CursorIdentityMismatch,
}

/// A bounded recovery query for one identity authenticated by the caller.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryRecoveryQuery {
    pub(super) authenticated_identity: DirectoryIdentity,
    pub(super) after: Option<DeliveryRecoveryCursor>,
}

impl DeliveryRecoveryQuery {
    pub fn first(authenticated_identity: DirectoryIdentity) -> Self {
        Self {
            authenticated_identity,
            after: None,
        }
    }

    pub fn try_after(
        authenticated_identity: DirectoryIdentity,
        cursor: DeliveryRecoveryCursor,
    ) -> Result<Self, DeliveryRecoveryQueryError> {
        if cursor.authenticated_identity != authenticated_identity {
            return Err(DeliveryRecoveryQueryError::CursorIdentityMismatch);
        }
        Ok(Self {
            authenticated_identity,
            after: Some(cursor),
        })
    }
}

impl fmt::Debug for DeliveryRecoveryQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryRecoveryQuery")
            .field("authenticated_identity", &"<redacted>")
            .field("after", &self.after)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptedDeliverySourceState {
    Missing,
    ObjectPending { version: DeliveryVersion },
    CommitPending { version: DeliveryVersion },
    Committed { version: DeliveryVersion },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryRecoveryAction {
    PreflightPending {
        operation_id: DeliveryOperationId,
        version: DeliveryVersion,
    },
    Accepted {
        operation_id: DeliveryOperationId,
        version: DeliveryVersion,
        source: AcceptedDeliverySourceState,
    },
    MergePending {
        operation_id: DeliveryOperationId,
        version: DeliveryVersion,
    },
    AbortPending {
        operation_id: DeliveryOperationId,
        version: DeliveryVersion,
    },
    UnlockPending {
        operation_id: DeliveryOperationId,
        version: DeliveryVersion,
    },
    UnlockedPendingRemove {
        operation_id: DeliveryOperationId,
        version: DeliveryVersion,
    },
    RemovePending {
        operation_id: DeliveryOperationId,
        version: DeliveryVersion,
    },
    DeletePending {
        operation_id: DeliveryOperationId,
        version: DeliveryVersion,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryRecoveryDisposition {
    Recover(DeliveryRecoveryAction),
    ReconciliationRequired,
}

#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryRecoveryEntry {
    pub identity: DeliveryIdentity,
    pub expected_common_git_identity: DirectoryIdentity,
    pub disposition: DeliveryRecoveryDisposition,
    pub ownership: DeliveryOwnershipSnapshot,
}

impl fmt::Debug for DeliveryRecoveryEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryRecoveryEntry")
            .field("identity", &self.identity)
            .field(
                "expected_common_git_identity",
                &self.expected_common_git_identity,
            )
            .field("disposition", &self.disposition)
            .field("ownership", &self.ownership)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryRecoveryBatch {
    pub entries: Vec<DeliveryRecoveryEntry>,
    pub next_cursor: Option<DeliveryRecoveryCursor>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct StartupDeliveryOwnership {
    pub identity: DeliveryIdentity,
    pub expected_common_git_identity: DirectoryIdentity,
    pub reconciliation_required: bool,
}

impl StartupDeliveryOwnership {
    pub(super) fn from_audited(audited: &AuditedDeliveryOwnership) -> Self {
        Self {
            identity: audited.identity,
            expected_common_git_identity: audited.expected_common_git_identity.clone(),
            reconciliation_required: audited.ownership.requires_reconciliation(),
        }
    }
}

impl fmt::Debug for StartupDeliveryOwnership {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StartupDeliveryOwnership")
            .field("identity", &self.identity)
            .field(
                "expected_common_git_identity",
                &self.expected_common_git_identity,
            )
            .field("reconciliation_required", &self.reconciliation_required)
            .finish()
    }
}
