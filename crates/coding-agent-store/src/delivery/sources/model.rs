use std::fmt;

use coding_agent_domain::TaskId;

use crate::delivery::mutation::{
    DeliveryMutationEntity, DeliveryMutationEntityKind, DeliveryMutationKey, DeliveryMutationKind,
    DeliveryMutationReceiptIdentity, impl_delivery_mutation_request,
};
use crate::delivery::{
    AcceptMergeCommandRequest, DeliveryError, DeliveryOperationId, DeliverySourceRecord,
    DeliverySourceState, DeliveryTimestamp, DeliveryVersion, FailureCode,
};

use super::proof::{DeliverySourceAppliedProof, DeliverySourceObjectProof};

#[derive(Clone, PartialEq, Eq)]
pub struct CreateDeliverySourceRequest {
    accept_command: AcceptMergeCommandRequest,
}

impl CreateDeliverySourceRequest {
    pub fn try_new(accept_command: AcceptMergeCommandRequest) -> Result<Self, DeliveryError> {
        if accept_command.task_id().as_uuid().is_nil()
            || accept_command.preflight_operation_id().as_uuid().is_nil()
        {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        Ok(Self { accept_command })
    }

    pub const fn accept_command(&self) -> &AcceptMergeCommandRequest {
        &self.accept_command
    }
}

impl fmt::Debug for CreateDeliverySourceRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateDeliverySourceRequest")
            .field("task_id", &self.accept_command.task_id())
            .field("accept_command", &"<redacted>")
            .finish()
    }
}

impl_delivery_mutation_request!(CreateDeliverySourceRequest, |request| {
    let command = request.accept_command();
    let accepted_version = command
        .expected_operation_version()
        .next()
        .expect("accept requests validate their next operation version");
    DeliveryMutationKey::new(
        DeliveryMutationKind::CreateDeliverySource,
        command.task_id(),
        vec![
            DeliveryMutationEntity::task(
                DeliveryMutationEntityKind::DeliverySource,
                command.task_id(),
                None,
            ),
            DeliveryMutationEntity::operation(
                DeliveryMutationEntityKind::MergeOperation,
                command.preflight_operation_id(),
                accepted_version,
            ),
        ],
        Some(DeliveryMutationReceiptIdentity::new(
            command.client_request_id(),
            super::super::DeliveryCommandKind::AcceptMerge,
            command.canonical_request_hash(),
            accepted_version,
            Some(command.preflight_operation_id()),
        )),
    )
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateDeliverySourceOutcome {
    Created(DeliverySourceRecord),
    Existing(DeliverySourceRecord),
    Conflict,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct DeliverySourceAnchor {
    pub(super) task_id: TaskId,
    pub(super) accepted_operation_id: DeliveryOperationId,
    pub(super) accepted_receipt_version: DeliveryVersion,
}

impl DeliverySourceAnchor {
    pub fn try_new(
        task_id: TaskId,
        accepted_operation_id: DeliveryOperationId,
        accepted_receipt_version: DeliveryVersion,
    ) -> Result<Self, DeliveryError> {
        if task_id.as_uuid().is_nil()
            || accepted_operation_id.as_uuid().is_nil()
            || accepted_receipt_version == DeliveryVersion::initial()
        {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        Ok(Self {
            task_id,
            accepted_operation_id,
            accepted_receipt_version,
        })
    }

    pub const fn task_id(self) -> TaskId {
        self.task_id
    }

    pub const fn accepted_operation_id(self) -> DeliveryOperationId {
        self.accepted_operation_id
    }

    pub const fn accepted_receipt_version(self) -> DeliveryVersion {
        self.accepted_receipt_version
    }
}

impl fmt::Debug for DeliverySourceAnchor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliverySourceAnchor")
            .field("task_id", &self.task_id)
            .field("accepted_operation_id", &self.accepted_operation_id)
            .field("accepted_receipt_version", &self.accepted_receipt_version)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct AdvanceDeliverySourceObjectRequest {
    pub(super) anchor: DeliverySourceAnchor,
    pub(super) expected_source_version: DeliveryVersion,
    pub(super) proof: DeliverySourceObjectProof,
}

impl AdvanceDeliverySourceObjectRequest {
    pub fn try_new(
        anchor: DeliverySourceAnchor,
        expected_source_version: DeliveryVersion,
        proof: DeliverySourceObjectProof,
    ) -> Result<Self, DeliveryError> {
        expected_source_version.next()?;
        Ok(Self {
            anchor,
            expected_source_version,
            proof,
        })
    }
}

impl fmt::Debug for AdvanceDeliverySourceObjectRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdvanceDeliverySourceObjectRequest")
            .field("anchor", &self.anchor)
            .field("expected_source_version", &self.expected_source_version)
            .field("proof", &"<redacted>")
            .finish()
    }
}

impl_delivery_mutation_request!(AdvanceDeliverySourceObjectRequest, |request| {
    source_transition_key(
        DeliveryMutationKind::AdvanceDeliverySourceObject,
        request.anchor,
        request.expected_source_version,
    )
});

#[derive(Clone, PartialEq, Eq)]
pub struct CommitDeliverySourceRequest {
    pub(super) anchor: DeliverySourceAnchor,
    pub(super) expected_source_version: DeliveryVersion,
    pub(super) proof: DeliverySourceAppliedProof,
}

impl CommitDeliverySourceRequest {
    pub fn try_new(
        anchor: DeliverySourceAnchor,
        expected_source_version: DeliveryVersion,
        proof: DeliverySourceAppliedProof,
    ) -> Result<Self, DeliveryError> {
        expected_source_version.next()?;
        Ok(Self {
            anchor,
            expected_source_version,
            proof,
        })
    }
}

impl fmt::Debug for CommitDeliverySourceRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommitDeliverySourceRequest")
            .field("anchor", &self.anchor)
            .field("expected_source_version", &self.expected_source_version)
            .field("proof", &"<redacted>")
            .finish()
    }
}

impl_delivery_mutation_request!(CommitDeliverySourceRequest, |request| {
    source_transition_key(
        DeliveryMutationKind::CommitDeliverySource,
        request.anchor,
        request.expected_source_version,
    )
});

#[derive(Clone, PartialEq, Eq)]
pub struct RecordDeliverySourceRetryRequest {
    pub(super) anchor: DeliverySourceAnchor,
    pub(super) expected_state: DeliverySourceState,
    pub(super) expected_source_version: DeliveryVersion,
    pub(super) reason: DeliverySourceRetryReason,
}

impl RecordDeliverySourceRetryRequest {
    pub fn try_new(
        anchor: DeliverySourceAnchor,
        expected_state: DeliverySourceState,
        expected_source_version: DeliveryVersion,
        reason: DeliverySourceRetryReason,
    ) -> Result<Self, DeliveryError> {
        if !expected_state.is_side_effect_active() {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        expected_source_version.next()?;
        Ok(Self {
            anchor,
            expected_state,
            expected_source_version,
            reason,
        })
    }
}

impl fmt::Debug for RecordDeliverySourceRetryRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecordDeliverySourceRetryRequest")
            .field("task_id", &self.anchor.task_id)
            .field("expected_state", &self.expected_state)
            .field("expected_source_version", &self.expected_source_version)
            .field("diagnostic", &"<redacted>")
            .finish()
    }
}

impl_delivery_mutation_request!(RecordDeliverySourceRetryRequest, |request| {
    source_transition_key(
        DeliveryMutationKind::RecordDeliverySourceRetry,
        request.anchor,
        request.expected_source_version,
    )
});

#[derive(Clone, PartialEq, Eq)]
pub struct ReconcileDeliverySourceRequest {
    pub(super) anchor: DeliverySourceAnchor,
    pub(super) expected_state: DeliverySourceState,
    pub(super) expected_source_version: DeliveryVersion,
    pub(super) expected_current_merge_version: DeliveryVersion,
    pub(super) reason: DeliverySourceReconciliationReason,
}

impl ReconcileDeliverySourceRequest {
    pub fn try_new(
        anchor: DeliverySourceAnchor,
        expected_state: DeliverySourceState,
        expected_source_version: DeliveryVersion,
        expected_current_merge_version: DeliveryVersion,
        reason: DeliverySourceReconciliationReason,
    ) -> Result<Self, DeliveryError> {
        if !matches!(
            expected_state,
            DeliverySourceState::ObjectPending
                | DeliverySourceState::CommitPending
                | DeliverySourceState::Committed
        ) {
            return Err(DeliveryError::InvalidCommandRequest);
        }
        expected_source_version.next()?;
        expected_current_merge_version.next()?;
        Ok(Self {
            anchor,
            expected_state,
            expected_source_version,
            expected_current_merge_version,
            reason,
        })
    }
}

impl fmt::Debug for ReconcileDeliverySourceRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReconcileDeliverySourceRequest")
            .field("task_id", &self.anchor.task_id)
            .field("expected_state", &self.expected_state)
            .field("expected_source_version", &self.expected_source_version)
            .field(
                "expected_current_merge_version",
                &self.expected_current_merge_version,
            )
            .field("reason", &"<redacted>")
            .finish()
    }
}

impl_delivery_mutation_request!(ReconcileDeliverySourceRequest, |request| {
    DeliveryMutationKey::new(
        DeliveryMutationKind::ReconcileDeliverySource,
        request.anchor.task_id,
        vec![
            DeliveryMutationEntity::task(
                DeliveryMutationEntityKind::DeliverySource,
                request.anchor.task_id,
                Some(request.expected_source_version),
            ),
            DeliveryMutationEntity::operation(
                DeliveryMutationEntityKind::MergeOperation,
                request.anchor.accepted_operation_id,
                request.expected_current_merge_version,
            ),
        ],
        None,
    )
});

fn source_transition_key(
    kind: DeliveryMutationKind,
    anchor: DeliverySourceAnchor,
    expected_source_version: DeliveryVersion,
) -> DeliveryMutationKey {
    DeliveryMutationKey::new(
        kind,
        anchor.task_id,
        vec![
            DeliveryMutationEntity::task(
                DeliveryMutationEntityKind::DeliverySource,
                anchor.task_id,
                Some(expected_source_version),
            ),
            DeliveryMutationEntity::operation(
                DeliveryMutationEntityKind::MergeOperation,
                anchor.accepted_operation_id,
                anchor.accepted_receipt_version,
            ),
        ],
        None,
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverySourceTransitionReceipt {
    pub task_id: TaskId,
    pub version: DeliveryVersion,
    pub state: DeliverySourceState,
    pub failure_code: Option<FailureCode>,
    pub transitioned_at: DeliveryTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliverySourceTransitionOutcome {
    Applied(DeliverySourceTransitionReceipt),
    Existing(DeliverySourceTransitionReceipt),
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileDeliverySourceReceipt {
    pub source: DeliverySourceTransitionReceipt,
    pub merge_operation_id: DeliveryOperationId,
    pub merge_version: DeliveryVersion,
    pub failure_code: FailureCode,
    pub transitioned_at: DeliveryTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileDeliverySourceOutcome {
    Applied(ReconcileDeliverySourceReceipt),
    Existing(ReconcileDeliverySourceReceipt),
    Conflict,
}

/// Allowlisted diagnostics for a source command proven not to have applied.
///
/// An arbitrary failure code cannot be supplied to the durable source API:
///
/// ```compile_fail
/// use coding_agent_store::{DeliverySourceRetryReason, FailureCode};
/// let arbitrary: FailureCode = "ARBITRARY_FAILURE".parse()?;
/// let _: DeliverySourceRetryReason = arbitrary;
/// # Ok::<(), coding_agent_store::DeliveryError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliverySourceRetryReason {
    CommandTimedOut,
}

impl DeliverySourceRetryReason {
    pub const fn as_failure_code(self) -> &'static str {
        match self {
            Self::CommandTimedOut => "COMMAND_TIMED_OUT",
        }
    }
}

/// Allowlisted reasons whose outcome cannot be safely inferred from Git state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliverySourceReconciliationReason {
    SourceInconsistent,
    ProcessTreeCleanupFailed,
}

impl DeliverySourceReconciliationReason {
    pub const fn as_failure_code(self) -> &'static str {
        match self {
            Self::SourceInconsistent => "DELIVERY_SOURCE_INCONSISTENT",
            Self::ProcessTreeCleanupFailed => "PROCESS_TREE_CLEANUP_FAILED",
        }
    }
}
