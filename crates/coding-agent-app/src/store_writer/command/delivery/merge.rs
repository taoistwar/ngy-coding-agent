use coding_agent_store::{
    AcceptMergeCommandRequest, AcceptMergeOutcome, BeginMergeAbortRequest,
    BindMergePreflightInputsOutcome, BindMergePreflightInputsRequest, CompleteMergeAbortRequest,
    CompleteMergeRequest, CreatePreflightOutcome, CreatePreflightRequest, DeliveryMutationKey,
    DeliveryMutationKind, DeliveryMutationRequest, EnterMergePendingRequest,
    FailUnboundMergePreflightOutcome, FailUnboundMergePreflightRequest, MarkPreflightStaleOutcome,
    MarkPreflightStaleRequest, MergeTransitionOutcome, ReconcileMergeRequest,
    RecordMergeKnownFailureRequest, RecordMergePreflightResultRequest, Store, StoreError,
};

use super::DeliveryDisposition;
use crate::pending_durable::KnownNotAppliedReason;
#[cfg(feature = "test-support")]
use crate::store_writer::StoreWriterOperationKind;

#[derive(Debug, Clone, PartialEq, Eq)]
// These variants intentionally retain the Store's exact validated requests;
// boxing selected requests would make the public replay shape inconsistent.
#[allow(clippy::large_enum_variant)]
pub enum DeliveryMergeWriteCommand {
    CreatePreflight(CreatePreflightRequest),
    BindPreflightInputs(BindMergePreflightInputsRequest),
    FailUnboundPreflight(FailUnboundMergePreflightRequest),
    MarkPreflightStale(MarkPreflightStaleRequest),
    RecordPreflightResult(RecordMergePreflightResultRequest),
    Accept(AcceptMergeCommandRequest),
    EnterPending(EnterMergePendingRequest),
    Complete(CompleteMergeRequest),
    BeginAbort(BeginMergeAbortRequest),
    CompleteAbort(CompleteMergeAbortRequest),
    RecordKnownFailure(RecordMergeKnownFailureRequest),
    Reconcile(ReconcileMergeRequest),
}

impl DeliveryMergeWriteCommand {
    pub fn mutation_key(&self) -> DeliveryMutationKey {
        match self {
            Self::CreatePreflight(request) => request.delivery_mutation_key(),
            Self::BindPreflightInputs(request) => request.delivery_mutation_key(),
            Self::FailUnboundPreflight(request) => request.delivery_mutation_key(),
            Self::MarkPreflightStale(request) => request.delivery_mutation_key(),
            Self::RecordPreflightResult(request) => request.delivery_mutation_key(),
            Self::Accept(request) => request.delivery_mutation_key(),
            Self::EnterPending(request) => request.delivery_mutation_key(),
            Self::Complete(request) => request.delivery_mutation_key(),
            Self::BeginAbort(request) => request.delivery_mutation_key(),
            Self::CompleteAbort(request) => request.delivery_mutation_key(),
            Self::RecordKnownFailure(request) => request.delivery_mutation_key(),
            Self::Reconcile(request) => request.delivery_mutation_key(),
        }
    }

    pub const fn kind(&self) -> DeliveryMutationKind {
        match self {
            Self::CreatePreflight(_) => DeliveryMutationKind::CreateMergePreflight,
            Self::BindPreflightInputs(_) => DeliveryMutationKind::BindMergePreflightInputs,
            Self::FailUnboundPreflight(_) => DeliveryMutationKind::FailUnboundMergePreflight,
            Self::MarkPreflightStale(_) => DeliveryMutationKind::MarkMergePreflightStale,
            Self::RecordPreflightResult(_) => DeliveryMutationKind::RecordMergePreflightResult,
            Self::Accept(_) => DeliveryMutationKind::AcceptMerge,
            Self::EnterPending(_) => DeliveryMutationKind::EnterMergePending,
            Self::Complete(_) => DeliveryMutationKind::CompleteMerge,
            Self::BeginAbort(_) => DeliveryMutationKind::BeginMergeAbort,
            Self::CompleteAbort(_) => DeliveryMutationKind::CompleteMergeAbort,
            Self::RecordKnownFailure(_) => DeliveryMutationKind::RecordMergeKnownFailure,
            Self::Reconcile(_) => DeliveryMutationKind::ReconcileMerge,
        }
    }

    #[cfg(feature = "test-support")]
    pub(super) const fn test_kind(&self) -> StoreWriterOperationKind {
        match self {
            Self::CreatePreflight(_) => StoreWriterOperationKind::CreateMergePreflight,
            Self::BindPreflightInputs(_) => StoreWriterOperationKind::BindMergePreflightInputs,
            Self::FailUnboundPreflight(_) => StoreWriterOperationKind::FailUnboundMergePreflight,
            Self::MarkPreflightStale(_) => StoreWriterOperationKind::MarkMergePreflightStale,
            Self::RecordPreflightResult(_) => StoreWriterOperationKind::RecordMergePreflightResult,
            Self::Accept(_) => StoreWriterOperationKind::AcceptMerge,
            Self::EnterPending(_) => StoreWriterOperationKind::EnterMergePending,
            Self::Complete(_) => StoreWriterOperationKind::CompleteMerge,
            Self::BeginAbort(_) => StoreWriterOperationKind::BeginMergeAbort,
            Self::CompleteAbort(_) => StoreWriterOperationKind::CompleteMergeAbort,
            Self::RecordKnownFailure(_) => StoreWriterOperationKind::RecordMergeKnownFailure,
            Self::Reconcile(_) => StoreWriterOperationKind::ReconcileMerge,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum DeliveryMergeWriteOutcome {
    CreatePreflight(CreatePreflightOutcome),
    BindPreflightInputs(BindMergePreflightInputsOutcome),
    FailUnboundPreflight(FailUnboundMergePreflightOutcome),
    MarkPreflightStale(MarkPreflightStaleOutcome),
    RecordPreflightResult(MergeTransitionOutcome),
    Accept(AcceptMergeOutcome),
    EnterPending(MergeTransitionOutcome),
    Complete(MergeTransitionOutcome),
    BeginAbort(MergeTransitionOutcome),
    CompleteAbort(MergeTransitionOutcome),
    RecordKnownFailure(MergeTransitionOutcome),
    Reconcile(MergeTransitionOutcome),
}

impl DeliveryMergeWriteOutcome {
    pub const fn kind(&self) -> DeliveryMutationKind {
        match self {
            Self::CreatePreflight(_) => DeliveryMutationKind::CreateMergePreflight,
            Self::BindPreflightInputs(_) => DeliveryMutationKind::BindMergePreflightInputs,
            Self::FailUnboundPreflight(_) => DeliveryMutationKind::FailUnboundMergePreflight,
            Self::MarkPreflightStale(_) => DeliveryMutationKind::MarkMergePreflightStale,
            Self::RecordPreflightResult(_) => DeliveryMutationKind::RecordMergePreflightResult,
            Self::Accept(_) => DeliveryMutationKind::AcceptMerge,
            Self::EnterPending(_) => DeliveryMutationKind::EnterMergePending,
            Self::Complete(_) => DeliveryMutationKind::CompleteMerge,
            Self::BeginAbort(_) => DeliveryMutationKind::BeginMergeAbort,
            Self::CompleteAbort(_) => DeliveryMutationKind::CompleteMergeAbort,
            Self::RecordKnownFailure(_) => DeliveryMutationKind::RecordMergeKnownFailure,
            Self::Reconcile(_) => DeliveryMutationKind::ReconcileMerge,
        }
    }

    pub(super) const fn committed_durable_state(&self) -> bool {
        match self {
            Self::CreatePreflight(CreatePreflightOutcome::Created(_))
            | Self::BindPreflightInputs(MergeTransitionOutcome::Applied(_))
            | Self::FailUnboundPreflight(MergeTransitionOutcome::Applied(_))
            | Self::MarkPreflightStale(MarkPreflightStaleOutcome::Applied { .. })
            | Self::RecordPreflightResult(MergeTransitionOutcome::Applied(_))
            | Self::Accept(AcceptMergeOutcome::Accepted(_))
            | Self::EnterPending(MergeTransitionOutcome::Applied(_))
            | Self::Complete(MergeTransitionOutcome::Applied(_))
            | Self::BeginAbort(MergeTransitionOutcome::Applied(_))
            | Self::CompleteAbort(MergeTransitionOutcome::Applied(_))
            | Self::RecordKnownFailure(MergeTransitionOutcome::Applied(_))
            | Self::Reconcile(MergeTransitionOutcome::Applied(_)) => true,
            Self::CreatePreflight(CreatePreflightOutcome::Existing(_))
            | Self::BindPreflightInputs(
                MergeTransitionOutcome::Existing(_) | MergeTransitionOutcome::Conflict,
            )
            | Self::FailUnboundPreflight(
                MergeTransitionOutcome::Existing(_) | MergeTransitionOutcome::Conflict,
            )
            | Self::MarkPreflightStale(
                MarkPreflightStaleOutcome::Existing { .. } | MarkPreflightStaleOutcome::Conflict,
            )
            | Self::RecordPreflightResult(
                MergeTransitionOutcome::Existing(_) | MergeTransitionOutcome::Conflict,
            )
            | Self::Accept(AcceptMergeOutcome::Existing(_) | AcceptMergeOutcome::Conflict)
            | Self::EnterPending(
                MergeTransitionOutcome::Existing(_) | MergeTransitionOutcome::Conflict,
            )
            | Self::Complete(
                MergeTransitionOutcome::Existing(_) | MergeTransitionOutcome::Conflict,
            )
            | Self::BeginAbort(
                MergeTransitionOutcome::Existing(_) | MergeTransitionOutcome::Conflict,
            )
            | Self::CompleteAbort(
                MergeTransitionOutcome::Existing(_) | MergeTransitionOutcome::Conflict,
            )
            | Self::RecordKnownFailure(
                MergeTransitionOutcome::Existing(_) | MergeTransitionOutcome::Conflict,
            )
            | Self::Reconcile(
                MergeTransitionOutcome::Existing(_) | MergeTransitionOutcome::Conflict,
            ) => false,
        }
    }
}

pub(super) async fn execute_store(
    store: &Store,
    command: DeliveryMergeWriteCommand,
) -> Result<DeliveryMergeWriteOutcome, StoreError> {
    match command {
        DeliveryMergeWriteCommand::CreatePreflight(request) => store
            .create_merge_preflight(request)
            .await
            .map(DeliveryMergeWriteOutcome::CreatePreflight),
        DeliveryMergeWriteCommand::BindPreflightInputs(request) => store
            .bind_merge_preflight_inputs(request)
            .await
            .map(DeliveryMergeWriteOutcome::BindPreflightInputs),
        DeliveryMergeWriteCommand::FailUnboundPreflight(request) => store
            .fail_unbound_merge_preflight(request)
            .await
            .map(DeliveryMergeWriteOutcome::FailUnboundPreflight),
        DeliveryMergeWriteCommand::MarkPreflightStale(request) => store
            .mark_merge_preflight_stale(request)
            .await
            .map(DeliveryMergeWriteOutcome::MarkPreflightStale),
        DeliveryMergeWriteCommand::RecordPreflightResult(request) => store
            .record_merge_preflight_result(request)
            .await
            .map(DeliveryMergeWriteOutcome::RecordPreflightResult),
        DeliveryMergeWriteCommand::Accept(request) => store
            .accept_merge(request)
            .await
            .map(DeliveryMergeWriteOutcome::Accept),
        DeliveryMergeWriteCommand::EnterPending(request) => store
            .enter_merge_pending(request)
            .await
            .map(DeliveryMergeWriteOutcome::EnterPending),
        DeliveryMergeWriteCommand::Complete(request) => store
            .complete_merge(request)
            .await
            .map(DeliveryMergeWriteOutcome::Complete),
        DeliveryMergeWriteCommand::BeginAbort(request) => store
            .begin_merge_abort(request)
            .await
            .map(DeliveryMergeWriteOutcome::BeginAbort),
        DeliveryMergeWriteCommand::CompleteAbort(request) => store
            .complete_merge_abort(request)
            .await
            .map(DeliveryMergeWriteOutcome::CompleteAbort),
        DeliveryMergeWriteCommand::RecordKnownFailure(request) => store
            .record_merge_known_failure(request)
            .await
            .map(DeliveryMergeWriteOutcome::RecordKnownFailure),
        DeliveryMergeWriteCommand::Reconcile(request) => store
            .reconcile_merge(request)
            .await
            .map(DeliveryMergeWriteOutcome::Reconcile),
    }
}

pub(super) fn classify_outcome(outcome: DeliveryMergeWriteOutcome) -> DeliveryDisposition {
    match outcome {
        confirmed @ DeliveryMergeWriteOutcome::CreatePreflight(
            CreatePreflightOutcome::Created(_) | CreatePreflightOutcome::Existing(_),
        )
        | confirmed @ DeliveryMergeWriteOutcome::BindPreflightInputs(
            MergeTransitionOutcome::Applied(_) | MergeTransitionOutcome::Existing(_),
        )
        | confirmed @ DeliveryMergeWriteOutcome::FailUnboundPreflight(
            MergeTransitionOutcome::Applied(_) | MergeTransitionOutcome::Existing(_),
        )
        | confirmed @ DeliveryMergeWriteOutcome::MarkPreflightStale(
            MarkPreflightStaleOutcome::Applied { .. } | MarkPreflightStaleOutcome::Existing { .. },
        )
        | confirmed @ DeliveryMergeWriteOutcome::RecordPreflightResult(
            MergeTransitionOutcome::Applied(_) | MergeTransitionOutcome::Existing(_),
        )
        | confirmed @ DeliveryMergeWriteOutcome::Accept(
            AcceptMergeOutcome::Accepted(_) | AcceptMergeOutcome::Existing(_),
        )
        | confirmed @ DeliveryMergeWriteOutcome::EnterPending(
            MergeTransitionOutcome::Applied(_) | MergeTransitionOutcome::Existing(_),
        )
        | confirmed @ DeliveryMergeWriteOutcome::Complete(
            MergeTransitionOutcome::Applied(_) | MergeTransitionOutcome::Existing(_),
        )
        | confirmed @ DeliveryMergeWriteOutcome::BeginAbort(
            MergeTransitionOutcome::Applied(_) | MergeTransitionOutcome::Existing(_),
        )
        | confirmed @ DeliveryMergeWriteOutcome::CompleteAbort(
            MergeTransitionOutcome::Applied(_) | MergeTransitionOutcome::Existing(_),
        )
        | confirmed @ DeliveryMergeWriteOutcome::RecordKnownFailure(
            MergeTransitionOutcome::Applied(_) | MergeTransitionOutcome::Existing(_),
        )
        | confirmed @ DeliveryMergeWriteOutcome::Reconcile(
            MergeTransitionOutcome::Applied(_) | MergeTransitionOutcome::Existing(_),
        ) => DeliveryDisposition::Confirmed(super::DeliveryWriteOutcome::Merge(confirmed)),
        conflict @ DeliveryMergeWriteOutcome::BindPreflightInputs(
            MergeTransitionOutcome::Conflict,
        )
        | conflict @ DeliveryMergeWriteOutcome::FailUnboundPreflight(
            MergeTransitionOutcome::Conflict,
        )
        | conflict @ DeliveryMergeWriteOutcome::MarkPreflightStale(
            MarkPreflightStaleOutcome::Conflict,
        )
        | conflict @ DeliveryMergeWriteOutcome::RecordPreflightResult(
            MergeTransitionOutcome::Conflict,
        )
        | conflict @ DeliveryMergeWriteOutcome::Accept(AcceptMergeOutcome::Conflict)
        | conflict @ DeliveryMergeWriteOutcome::EnterPending(MergeTransitionOutcome::Conflict)
        | conflict @ DeliveryMergeWriteOutcome::Complete(MergeTransitionOutcome::Conflict)
        | conflict @ DeliveryMergeWriteOutcome::BeginAbort(MergeTransitionOutcome::Conflict)
        | conflict @ DeliveryMergeWriteOutcome::CompleteAbort(MergeTransitionOutcome::Conflict)
        | conflict @ DeliveryMergeWriteOutcome::RecordKnownFailure(
            MergeTransitionOutcome::Conflict,
        )
        | conflict @ DeliveryMergeWriteOutcome::Reconcile(MergeTransitionOutcome::Conflict) => {
            DeliveryDisposition::KnownNotApplied {
                reason: KnownNotAppliedReason::ExactReconciliation,
                outcome: Some(super::DeliveryWriteOutcome::Merge(conflict)),
                error: None,
            }
        }
    }
}
