mod abort;
mod accept;
mod conflicts;
mod merged;
mod model;
mod pending;
mod preflight;
mod preflight_inputs;
mod preflight_result;
mod proof;
mod replay;
mod terminal;
mod unbound_failure;

pub use model::{
    AcceptMergeOutcome, BeginMergeAbortRequest, CompleteMergeAbortRequest, CompleteMergeRequest,
    EnterMergePendingRequest, MergeConflictPaths, MergeKnownNotAppliedReason, MergePreflightResult,
    MergeReconciliationReason, MergeTransitionOutcome, MergeTransitionReceipt,
    PreflightRejectedReason, ReconcileMergeRequest, RecordMergeKnownFailureRequest,
    RecordMergePreflightResultRequest,
};
pub(in crate::delivery) use model::{merge_failure_code_is_valid, raw_relative_path_is_canonical};
pub use preflight_inputs::{BindMergePreflightInputsOutcome, BindMergePreflightInputsRequest};
pub use proof::{
    MergeAbortAppliedProof, MergeAbortProof, MergeAppliedProof, MergeAutostashObservation,
    MergeCommitObjectProof, OtherGitOperationObservation,
};
pub(in crate::delivery) use replay::{
    OperationLookup, TransitionLookup, load_operation_for_caller, lookup_transition,
};
pub use unbound_failure::{
    FailUnboundMergePreflightOutcome, FailUnboundMergePreflightRequest,
    UnboundMergePreflightFailure,
};

use std::fmt;

use super::mutation::{
    DeliveryMutationEntity, DeliveryMutationEntityKind, DeliveryMutationKey, DeliveryMutationKind,
    DeliveryMutationReceiptIdentity, impl_delivery_mutation_request,
};
use super::{
    DeliveryError, DirectoryIdentity, GitObjectAlgorithm, PreflightCommandRequest, Sha256Digest,
};
use crate::StoreError;

#[derive(Clone, PartialEq, Eq)]
pub struct CreatePreflightRequest {
    command: PreflightCommandRequest,
    common_git_identity: DirectoryIdentity,
    worktree_admin_identity: DirectoryIdentity,
    source_config_attributes_digest: Sha256Digest,
    target_config_attributes_digest: Sha256Digest,
    target_security_digest: Sha256Digest,
}

impl fmt::Debug for CreatePreflightRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreatePreflightRequest")
            .field("task_id", &self.command.task_id())
            .field("request_and_repository_values", &"<redacted>")
            .finish()
    }
}

impl CreatePreflightRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        command: PreflightCommandRequest,
        common_git_identity: DirectoryIdentity,
        worktree_admin_identity: DirectoryIdentity,
        source_config_attributes_digest: Sha256Digest,
        target_config_attributes_digest: Sha256Digest,
        target_security_digest: Sha256Digest,
    ) -> Result<Self, DeliveryError> {
        Ok(Self {
            command,
            common_git_identity,
            worktree_admin_identity,
            source_config_attributes_digest,
            target_config_attributes_digest,
            target_security_digest,
        })
    }

    pub const fn command(&self) -> &PreflightCommandRequest {
        &self.command
    }

    pub const fn common_git_identity(&self) -> &DirectoryIdentity {
        &self.common_git_identity
    }

    pub const fn worktree_admin_identity(&self) -> &DirectoryIdentity {
        &self.worktree_admin_identity
    }

    pub const fn source_config_attributes_digest(&self) -> &Sha256Digest {
        &self.source_config_attributes_digest
    }

    pub const fn target_config_attributes_digest(&self) -> &Sha256Digest {
        &self.target_config_attributes_digest
    }

    pub const fn target_security_digest(&self) -> &Sha256Digest {
        &self.target_security_digest
    }

    pub const fn object_algorithm(&self) -> GitObjectAlgorithm {
        self.command.expected_target_head().algorithm()
    }
}

impl_delivery_mutation_request!(CreatePreflightRequest, |request| {
    let command = request.command();
    DeliveryMutationKey::new(
        DeliveryMutationKind::CreateMergePreflight,
        command.task_id(),
        vec![DeliveryMutationEntity::pending(
            DeliveryMutationEntityKind::MergeOperation,
        )],
        Some(DeliveryMutationReceiptIdentity::new(
            command.client_request_id(),
            super::DeliveryCommandKind::Preflight,
            command.canonical_request_hash(),
            super::DeliveryVersion::initial(),
            None,
        )),
    )
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreatePreflightOutcome {
    Created(super::DeliveryCommandReceipt),
    Existing(super::DeliveryCommandReceipt),
}

fn invalid_preflight_request() -> StoreError {
    StoreError::Delivery(DeliveryError::InvalidCommandRequest)
}

const MERGE_INVARIANT: &str = "delivery merge operation is inconsistent";

fn merge_invariant() -> StoreError {
    StoreError::InvariantViolation(MERGE_INVARIANT)
}
