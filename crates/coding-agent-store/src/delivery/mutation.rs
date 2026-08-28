use std::fmt;

use coding_agent_domain::TaskId;

use super::{
    DeliveryCommandId, DeliveryCommandKind, DeliveryOperationId, DeliveryVersion, Sha256Digest,
};

/// Identifies one of the Store's typed delivery mutations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeliveryMutationKind {
    CreateDeliverySource,
    AdvanceDeliverySourceObject,
    CommitDeliverySource,
    RecordDeliverySourceRetry,
    ReconcileDeliverySource,
    CreateMergePreflight,
    BindMergePreflightInputs,
    FailUnboundMergePreflight,
    MarkMergePreflightStale,
    RecordMergePreflightResult,
    AcceptMerge,
    EnterMergePending,
    CompleteMerge,
    BeginMergeAbort,
    CompleteMergeAbort,
    RecordMergeKnownFailure,
    ReconcileMerge,
    AcceptWorktreeCleanup,
    RecordWorktreeUnlocked,
    EnterWorktreeRemovePending,
    CompleteWorktreeCleanup,
    RecordWorktreeCleanupFailure,
    ReconcileWorktreeCleanup,
    AcceptBranchCleanup,
    RefreshBranchCleanupTarget,
    CompleteBranchCleanup,
    RecordBranchCleanupFailure,
    ReconcileBranchCleanup,
}

/// The durable aggregate addressed by a typed delivery mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeliveryMutationEntityKind {
    DeliverySource,
    MergeOperation,
    CleanupOperation,
    WorktreeDisposition,
    BranchDisposition,
}

/// A durable entity identifier, interpreted together with its entity kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeliveryMutationEntityId {
    Task(TaskId),
    Operation(DeliveryOperationId),
}

/// Read-only entity identity carried by a [`DeliveryMutationKey`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeliveryMutationEntity {
    kind: DeliveryMutationEntityKind,
    id: Option<DeliveryMutationEntityId>,
    expected_version: Option<DeliveryVersion>,
}

impl DeliveryMutationEntity {
    pub const fn kind(&self) -> DeliveryMutationEntityKind {
        self.kind
    }

    /// Returns `None` for a new operation whose ID is allocated by the Store.
    pub const fn id(&self) -> Option<DeliveryMutationEntityId> {
        self.id
    }

    /// Returns `None` when the request expects the entity to be absent.
    pub const fn expected_version(&self) -> Option<DeliveryVersion> {
        self.expected_version
    }

    pub(in crate::delivery) const fn pending(kind: DeliveryMutationEntityKind) -> Self {
        Self {
            kind,
            id: None,
            expected_version: None,
        }
    }

    pub(in crate::delivery) const fn task(
        kind: DeliveryMutationEntityKind,
        task_id: TaskId,
        expected_version: Option<DeliveryVersion>,
    ) -> Self {
        Self {
            kind,
            id: Some(DeliveryMutationEntityId::Task(task_id)),
            expected_version,
        }
    }

    pub(in crate::delivery) const fn operation(
        kind: DeliveryMutationEntityKind,
        operation_id: DeliveryOperationId,
        expected_version: DeliveryVersion,
    ) -> Self {
        Self {
            kind,
            id: Some(DeliveryMutationEntityId::Operation(operation_id)),
            expected_version: Some(expected_version),
        }
    }
}

/// Immutable receipt identity used for query-first delivery command handling.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DeliveryMutationReceiptIdentity {
    client_request_id: DeliveryCommandId,
    command_kind: DeliveryCommandKind,
    canonical_request_hash: Sha256Digest,
    expected_accepted_version: DeliveryVersion,
    operation_anchor: Option<DeliveryOperationId>,
}

impl fmt::Debug for DeliveryMutationReceiptIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryMutationReceiptIdentity")
            .field("client_request_id", &self.client_request_id)
            .field("command_kind", &self.command_kind)
            .field("canonical_request_hash", &"<redacted>")
            .field("expected_accepted_version", &self.expected_accepted_version)
            .field("operation_anchor", &self.operation_anchor)
            .finish()
    }
}

impl DeliveryMutationReceiptIdentity {
    pub const fn client_request_id(&self) -> DeliveryCommandId {
        self.client_request_id
    }

    pub const fn command_kind(&self) -> DeliveryCommandKind {
        self.command_kind
    }

    pub const fn canonical_request_hash(&self) -> &Sha256Digest {
        &self.canonical_request_hash
    }

    pub const fn expected_accepted_version(&self) -> DeliveryVersion {
        self.expected_accepted_version
    }

    pub const fn operation_anchor(&self) -> Option<DeliveryOperationId> {
        self.operation_anchor
    }

    pub(in crate::delivery) fn new(
        client_request_id: DeliveryCommandId,
        command_kind: DeliveryCommandKind,
        canonical_request_hash: Sha256Digest,
        expected_accepted_version: DeliveryVersion,
        operation_anchor: Option<DeliveryOperationId>,
    ) -> Self {
        Self {
            client_request_id,
            command_kind,
            canonical_request_hash,
            expected_accepted_version,
            operation_anchor,
        }
    }
}

/// A sealed, request-derived identity for one typed Store delivery mutation.
///
/// The key cannot be constructed directly outside this crate. Call
/// [`DeliveryMutationRequest::delivery_mutation_key`] on one of the Store's typed
/// request values instead.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeliveryMutationKey {
    kind: DeliveryMutationKind,
    task_id: TaskId,
    entities: Vec<DeliveryMutationEntity>,
    receipt: Option<DeliveryMutationReceiptIdentity>,
}

impl DeliveryMutationKey {
    pub const fn kind(&self) -> DeliveryMutationKind {
        self.kind
    }

    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn entities(&self) -> &[DeliveryMutationEntity] {
        &self.entities
    }

    pub const fn receipt(&self) -> Option<&DeliveryMutationReceiptIdentity> {
        self.receipt.as_ref()
    }

    pub(in crate::delivery) fn new(
        kind: DeliveryMutationKind,
        task_id: TaskId,
        entities: Vec<DeliveryMutationEntity>,
        receipt: Option<DeliveryMutationReceiptIdentity>,
    ) -> Self {
        debug_assert!(!entities.is_empty());
        Self {
            kind,
            task_id,
            entities,
            receipt,
        }
    }
}

/// Implemented only by the 28 typed request values accepted by Store delivery mutations.
pub trait DeliveryMutationRequest: sealed::Sealed {
    fn delivery_mutation_key(&self) -> DeliveryMutationKey;
}

pub(in crate::delivery) mod sealed {
    pub trait Sealed {}
}

macro_rules! impl_delivery_mutation_request {
    ($request:ty, |$value:ident| $key:expr) => {
        impl $crate::delivery::mutation::sealed::Sealed for $request {}

        impl $crate::delivery::mutation::DeliveryMutationRequest for $request {
            fn delivery_mutation_key(&self) -> $crate::delivery::mutation::DeliveryMutationKey {
                let $value = self;
                $key
            }
        }
    };
}

pub(in crate::delivery) use impl_delivery_mutation_request;
