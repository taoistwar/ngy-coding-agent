use coding_agent_store::{
    AdvanceDeliverySourceObjectRequest, CommitDeliverySourceRequest, CreateDeliverySourceOutcome,
    CreateDeliverySourceRequest, DeliveryMutationKey, DeliveryMutationKind,
    DeliveryMutationRequest, DeliverySourceTransitionOutcome, ReconcileDeliverySourceOutcome,
    ReconcileDeliverySourceRequest, RecordDeliverySourceRetryRequest, Store, StoreError,
};

use super::DeliveryDisposition;
use crate::pending_durable::KnownNotAppliedReason;
#[cfg(feature = "test-support")]
use crate::store_writer::StoreWriterOperationKind;

#[derive(Debug, Clone, PartialEq, Eq)]
// Boxing individual requests would make callers reconstruct an otherwise exact
// Store request merely to cross the writer boundary.
#[allow(clippy::large_enum_variant)]
pub enum DeliverySourceWriteCommand {
    Create(CreateDeliverySourceRequest),
    AdvanceObject(AdvanceDeliverySourceObjectRequest),
    Commit(CommitDeliverySourceRequest),
    RecordRetry(RecordDeliverySourceRetryRequest),
    Reconcile(ReconcileDeliverySourceRequest),
}

impl DeliverySourceWriteCommand {
    pub fn mutation_key(&self) -> DeliveryMutationKey {
        match self {
            Self::Create(request) => request.delivery_mutation_key(),
            Self::AdvanceObject(request) => request.delivery_mutation_key(),
            Self::Commit(request) => request.delivery_mutation_key(),
            Self::RecordRetry(request) => request.delivery_mutation_key(),
            Self::Reconcile(request) => request.delivery_mutation_key(),
        }
    }

    pub const fn kind(&self) -> DeliveryMutationKind {
        match self {
            Self::Create(_) => DeliveryMutationKind::CreateDeliverySource,
            Self::AdvanceObject(_) => DeliveryMutationKind::AdvanceDeliverySourceObject,
            Self::Commit(_) => DeliveryMutationKind::CommitDeliverySource,
            Self::RecordRetry(_) => DeliveryMutationKind::RecordDeliverySourceRetry,
            Self::Reconcile(_) => DeliveryMutationKind::ReconcileDeliverySource,
        }
    }

    #[cfg(feature = "test-support")]
    pub(super) const fn test_kind(&self) -> StoreWriterOperationKind {
        match self {
            Self::Create(_) => StoreWriterOperationKind::CreateDeliverySource,
            Self::AdvanceObject(_) => StoreWriterOperationKind::AdvanceDeliverySourceObject,
            Self::Commit(_) => StoreWriterOperationKind::CommitDeliverySource,
            Self::RecordRetry(_) => StoreWriterOperationKind::RecordDeliverySourceRetry,
            Self::Reconcile(_) => StoreWriterOperationKind::ReconcileDeliverySource,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
// The reconciliation result deliberately carries the complete canonical source
// graph so callers can validate reply-lost recovery without a second read.
#[allow(clippy::large_enum_variant)]
pub enum DeliverySourceWriteOutcome {
    Create(CreateDeliverySourceOutcome),
    AdvanceObject(DeliverySourceTransitionOutcome),
    Commit(DeliverySourceTransitionOutcome),
    RecordRetry(DeliverySourceTransitionOutcome),
    Reconcile(ReconcileDeliverySourceOutcome),
}

impl DeliverySourceWriteOutcome {
    pub const fn kind(&self) -> DeliveryMutationKind {
        match self {
            Self::Create(_) => DeliveryMutationKind::CreateDeliverySource,
            Self::AdvanceObject(_) => DeliveryMutationKind::AdvanceDeliverySourceObject,
            Self::Commit(_) => DeliveryMutationKind::CommitDeliverySource,
            Self::RecordRetry(_) => DeliveryMutationKind::RecordDeliverySourceRetry,
            Self::Reconcile(_) => DeliveryMutationKind::ReconcileDeliverySource,
        }
    }

    pub(super) const fn committed_durable_state(&self) -> bool {
        match self {
            Self::Create(CreateDeliverySourceOutcome::Created(_))
            | Self::AdvanceObject(DeliverySourceTransitionOutcome::Applied(_))
            | Self::Commit(DeliverySourceTransitionOutcome::Applied(_))
            | Self::RecordRetry(DeliverySourceTransitionOutcome::Applied(_))
            | Self::Reconcile(ReconcileDeliverySourceOutcome::Applied(_)) => true,
            Self::Create(
                CreateDeliverySourceOutcome::Existing(_) | CreateDeliverySourceOutcome::Conflict,
            )
            | Self::AdvanceObject(
                DeliverySourceTransitionOutcome::Existing(_)
                | DeliverySourceTransitionOutcome::Conflict,
            )
            | Self::Commit(
                DeliverySourceTransitionOutcome::Existing(_)
                | DeliverySourceTransitionOutcome::Conflict,
            )
            | Self::RecordRetry(
                DeliverySourceTransitionOutcome::Existing(_)
                | DeliverySourceTransitionOutcome::Conflict,
            )
            | Self::Reconcile(
                ReconcileDeliverySourceOutcome::Existing(_)
                | ReconcileDeliverySourceOutcome::Conflict,
            ) => false,
        }
    }
}

pub(super) async fn execute_store(
    store: &Store,
    command: DeliverySourceWriteCommand,
) -> Result<DeliverySourceWriteOutcome, StoreError> {
    match command {
        DeliverySourceWriteCommand::Create(request) => store
            .create_delivery_source(request)
            .await
            .map(DeliverySourceWriteOutcome::Create),
        DeliverySourceWriteCommand::AdvanceObject(request) => store
            .advance_delivery_source_object(request)
            .await
            .map(DeliverySourceWriteOutcome::AdvanceObject),
        DeliverySourceWriteCommand::Commit(request) => store
            .commit_delivery_source(request)
            .await
            .map(DeliverySourceWriteOutcome::Commit),
        DeliverySourceWriteCommand::RecordRetry(request) => store
            .record_delivery_source_retry(request)
            .await
            .map(DeliverySourceWriteOutcome::RecordRetry),
        DeliverySourceWriteCommand::Reconcile(request) => store
            .reconcile_delivery_source(request)
            .await
            .map(DeliverySourceWriteOutcome::Reconcile),
    }
}

pub(super) fn classify_outcome(outcome: DeliverySourceWriteOutcome) -> DeliveryDisposition {
    match outcome {
        confirmed @ DeliverySourceWriteOutcome::Create(
            CreateDeliverySourceOutcome::Created(_) | CreateDeliverySourceOutcome::Existing(_),
        )
        | confirmed @ DeliverySourceWriteOutcome::AdvanceObject(
            DeliverySourceTransitionOutcome::Applied(_)
            | DeliverySourceTransitionOutcome::Existing(_),
        )
        | confirmed @ DeliverySourceWriteOutcome::Commit(
            DeliverySourceTransitionOutcome::Applied(_)
            | DeliverySourceTransitionOutcome::Existing(_),
        )
        | confirmed @ DeliverySourceWriteOutcome::RecordRetry(
            DeliverySourceTransitionOutcome::Applied(_)
            | DeliverySourceTransitionOutcome::Existing(_),
        )
        | confirmed @ DeliverySourceWriteOutcome::Reconcile(
            ReconcileDeliverySourceOutcome::Applied(_)
            | ReconcileDeliverySourceOutcome::Existing(_),
        ) => DeliveryDisposition::Confirmed(super::DeliveryWriteOutcome::Source(confirmed)),
        conflict @ DeliverySourceWriteOutcome::Create(CreateDeliverySourceOutcome::Conflict)
        | conflict @ DeliverySourceWriteOutcome::AdvanceObject(
            DeliverySourceTransitionOutcome::Conflict,
        )
        | conflict
        @ DeliverySourceWriteOutcome::Commit(DeliverySourceTransitionOutcome::Conflict)
        | conflict @ DeliverySourceWriteOutcome::RecordRetry(
            DeliverySourceTransitionOutcome::Conflict,
        )
        | conflict @ DeliverySourceWriteOutcome::Reconcile(
            ReconcileDeliverySourceOutcome::Conflict,
        ) => DeliveryDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::ExactReconciliation,
            outcome: Some(super::DeliveryWriteOutcome::Source(conflict)),
            error: None,
        },
    }
}
