use coding_agent_store::{
    CleanupAcceptanceOutcome, CleanupTransitionOutcome, CompleteBranchCleanupRequest,
    CompleteWorktreeCleanupRequest, DeleteBranchCommandRequest, DeliveryMutationKey,
    DeliveryMutationKind, DeliveryMutationRequest, EnterWorktreeRemovePendingRequest,
    ReconcileBranchCleanupRequest, ReconcileWorktreeCleanupRequest,
    RecordBranchCleanupFailureRequest, RecordWorktreeCleanupFailureRequest,
    RecordWorktreeUnlockedRequest, RefreshBranchCleanupTargetRequest, RemoveWorktreeCommandRequest,
    Store, StoreError,
};

use super::DeliveryDisposition;
use crate::pending_durable::KnownNotAppliedReason;
#[cfg(feature = "test-support")]
use crate::store_writer::StoreWriterOperationKind;

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum DeliveryCleanupWriteCommand {
    AcceptWorktree(RemoveWorktreeCommandRequest),
    RecordWorktreeUnlocked(RecordWorktreeUnlockedRequest),
    EnterWorktreeRemovePending(EnterWorktreeRemovePendingRequest),
    CompleteWorktree(CompleteWorktreeCleanupRequest),
    RecordWorktreeFailure(RecordWorktreeCleanupFailureRequest),
    ReconcileWorktree(ReconcileWorktreeCleanupRequest),
    AcceptBranch(DeleteBranchCommandRequest),
    RefreshBranchTarget(RefreshBranchCleanupTargetRequest),
    CompleteBranch(CompleteBranchCleanupRequest),
    RecordBranchFailure(RecordBranchCleanupFailureRequest),
    ReconcileBranch(ReconcileBranchCleanupRequest),
}

impl DeliveryCleanupWriteCommand {
    pub fn mutation_key(&self) -> DeliveryMutationKey {
        match self {
            Self::AcceptWorktree(request) => request.delivery_mutation_key(),
            Self::RecordWorktreeUnlocked(request) => request.delivery_mutation_key(),
            Self::EnterWorktreeRemovePending(request) => request.delivery_mutation_key(),
            Self::CompleteWorktree(request) => request.delivery_mutation_key(),
            Self::RecordWorktreeFailure(request) => request.delivery_mutation_key(),
            Self::ReconcileWorktree(request) => request.delivery_mutation_key(),
            Self::AcceptBranch(request) => request.delivery_mutation_key(),
            Self::RefreshBranchTarget(request) => request.delivery_mutation_key(),
            Self::CompleteBranch(request) => request.delivery_mutation_key(),
            Self::RecordBranchFailure(request) => request.delivery_mutation_key(),
            Self::ReconcileBranch(request) => request.delivery_mutation_key(),
        }
    }

    pub const fn kind(&self) -> DeliveryMutationKind {
        match self {
            Self::AcceptWorktree(_) => DeliveryMutationKind::AcceptWorktreeCleanup,
            Self::RecordWorktreeUnlocked(_) => DeliveryMutationKind::RecordWorktreeUnlocked,
            Self::EnterWorktreeRemovePending(_) => DeliveryMutationKind::EnterWorktreeRemovePending,
            Self::CompleteWorktree(_) => DeliveryMutationKind::CompleteWorktreeCleanup,
            Self::RecordWorktreeFailure(_) => DeliveryMutationKind::RecordWorktreeCleanupFailure,
            Self::ReconcileWorktree(_) => DeliveryMutationKind::ReconcileWorktreeCleanup,
            Self::AcceptBranch(_) => DeliveryMutationKind::AcceptBranchCleanup,
            Self::RefreshBranchTarget(_) => DeliveryMutationKind::RefreshBranchCleanupTarget,
            Self::CompleteBranch(_) => DeliveryMutationKind::CompleteBranchCleanup,
            Self::RecordBranchFailure(_) => DeliveryMutationKind::RecordBranchCleanupFailure,
            Self::ReconcileBranch(_) => DeliveryMutationKind::ReconcileBranchCleanup,
        }
    }

    #[cfg(feature = "test-support")]
    pub(super) const fn test_kind(&self) -> StoreWriterOperationKind {
        match self {
            Self::AcceptWorktree(_) => StoreWriterOperationKind::AcceptWorktreeCleanup,
            Self::RecordWorktreeUnlocked(_) => StoreWriterOperationKind::RecordWorktreeUnlocked,
            Self::EnterWorktreeRemovePending(_) => {
                StoreWriterOperationKind::EnterWorktreeRemovePending
            }
            Self::CompleteWorktree(_) => StoreWriterOperationKind::CompleteWorktreeCleanup,
            Self::RecordWorktreeFailure(_) => {
                StoreWriterOperationKind::RecordWorktreeCleanupFailure
            }
            Self::ReconcileWorktree(_) => StoreWriterOperationKind::ReconcileWorktreeCleanup,
            Self::AcceptBranch(_) => StoreWriterOperationKind::AcceptBranchCleanup,
            Self::RefreshBranchTarget(_) => StoreWriterOperationKind::RefreshBranchCleanupTarget,
            Self::CompleteBranch(_) => StoreWriterOperationKind::CompleteBranchCleanup,
            Self::RecordBranchFailure(_) => StoreWriterOperationKind::RecordBranchCleanupFailure,
            Self::ReconcileBranch(_) => StoreWriterOperationKind::ReconcileBranchCleanup,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryCleanupWriteOutcome {
    AcceptWorktree(CleanupAcceptanceOutcome),
    RecordWorktreeUnlocked(CleanupTransitionOutcome),
    EnterWorktreeRemovePending(CleanupTransitionOutcome),
    CompleteWorktree(CleanupTransitionOutcome),
    RecordWorktreeFailure(CleanupTransitionOutcome),
    ReconcileWorktree(CleanupTransitionOutcome),
    AcceptBranch(CleanupAcceptanceOutcome),
    RefreshBranchTarget(CleanupTransitionOutcome),
    CompleteBranch(CleanupTransitionOutcome),
    RecordBranchFailure(CleanupTransitionOutcome),
    ReconcileBranch(CleanupTransitionOutcome),
}

impl DeliveryCleanupWriteOutcome {
    pub const fn kind(&self) -> DeliveryMutationKind {
        match self {
            Self::AcceptWorktree(_) => DeliveryMutationKind::AcceptWorktreeCleanup,
            Self::RecordWorktreeUnlocked(_) => DeliveryMutationKind::RecordWorktreeUnlocked,
            Self::EnterWorktreeRemovePending(_) => DeliveryMutationKind::EnterWorktreeRemovePending,
            Self::CompleteWorktree(_) => DeliveryMutationKind::CompleteWorktreeCleanup,
            Self::RecordWorktreeFailure(_) => DeliveryMutationKind::RecordWorktreeCleanupFailure,
            Self::ReconcileWorktree(_) => DeliveryMutationKind::ReconcileWorktreeCleanup,
            Self::AcceptBranch(_) => DeliveryMutationKind::AcceptBranchCleanup,
            Self::RefreshBranchTarget(_) => DeliveryMutationKind::RefreshBranchCleanupTarget,
            Self::CompleteBranch(_) => DeliveryMutationKind::CompleteBranchCleanup,
            Self::RecordBranchFailure(_) => DeliveryMutationKind::RecordBranchCleanupFailure,
            Self::ReconcileBranch(_) => DeliveryMutationKind::ReconcileBranchCleanup,
        }
    }

    pub(super) const fn committed_durable_state(&self) -> bool {
        match self {
            Self::AcceptWorktree(CleanupAcceptanceOutcome::Accepted(_))
            | Self::RecordWorktreeUnlocked(CleanupTransitionOutcome::Applied(_))
            | Self::EnterWorktreeRemovePending(CleanupTransitionOutcome::Applied(_))
            | Self::CompleteWorktree(CleanupTransitionOutcome::Applied(_))
            | Self::RecordWorktreeFailure(CleanupTransitionOutcome::Applied(_))
            | Self::ReconcileWorktree(CleanupTransitionOutcome::Applied(_))
            | Self::AcceptBranch(CleanupAcceptanceOutcome::Accepted(_))
            | Self::RefreshBranchTarget(CleanupTransitionOutcome::Applied(_))
            | Self::CompleteBranch(CleanupTransitionOutcome::Applied(_))
            | Self::RecordBranchFailure(CleanupTransitionOutcome::Applied(_))
            | Self::ReconcileBranch(CleanupTransitionOutcome::Applied(_)) => true,
            Self::AcceptWorktree(
                CleanupAcceptanceOutcome::Existing(_) | CleanupAcceptanceOutcome::Conflict,
            )
            | Self::RecordWorktreeUnlocked(
                CleanupTransitionOutcome::Existing(_) | CleanupTransitionOutcome::Conflict,
            )
            | Self::EnterWorktreeRemovePending(
                CleanupTransitionOutcome::Existing(_) | CleanupTransitionOutcome::Conflict,
            )
            | Self::CompleteWorktree(
                CleanupTransitionOutcome::Existing(_) | CleanupTransitionOutcome::Conflict,
            )
            | Self::RecordWorktreeFailure(
                CleanupTransitionOutcome::Existing(_) | CleanupTransitionOutcome::Conflict,
            )
            | Self::ReconcileWorktree(
                CleanupTransitionOutcome::Existing(_) | CleanupTransitionOutcome::Conflict,
            )
            | Self::AcceptBranch(
                CleanupAcceptanceOutcome::Existing(_) | CleanupAcceptanceOutcome::Conflict,
            )
            | Self::RefreshBranchTarget(
                CleanupTransitionOutcome::Existing(_) | CleanupTransitionOutcome::Conflict,
            )
            | Self::CompleteBranch(
                CleanupTransitionOutcome::Existing(_) | CleanupTransitionOutcome::Conflict,
            )
            | Self::RecordBranchFailure(
                CleanupTransitionOutcome::Existing(_) | CleanupTransitionOutcome::Conflict,
            )
            | Self::ReconcileBranch(
                CleanupTransitionOutcome::Existing(_) | CleanupTransitionOutcome::Conflict,
            ) => false,
        }
    }
}

pub(super) async fn execute_store(
    store: &Store,
    command: DeliveryCleanupWriteCommand,
) -> Result<DeliveryCleanupWriteOutcome, StoreError> {
    match command {
        DeliveryCleanupWriteCommand::AcceptWorktree(request) => store
            .accept_worktree_cleanup(request)
            .await
            .map(DeliveryCleanupWriteOutcome::AcceptWorktree),
        DeliveryCleanupWriteCommand::RecordWorktreeUnlocked(request) => store
            .record_worktree_unlocked(request)
            .await
            .map(DeliveryCleanupWriteOutcome::RecordWorktreeUnlocked),
        DeliveryCleanupWriteCommand::EnterWorktreeRemovePending(request) => store
            .enter_worktree_remove_pending(request)
            .await
            .map(DeliveryCleanupWriteOutcome::EnterWorktreeRemovePending),
        DeliveryCleanupWriteCommand::CompleteWorktree(request) => store
            .complete_worktree_cleanup(request)
            .await
            .map(DeliveryCleanupWriteOutcome::CompleteWorktree),
        DeliveryCleanupWriteCommand::RecordWorktreeFailure(request) => store
            .record_worktree_cleanup_failure(request)
            .await
            .map(DeliveryCleanupWriteOutcome::RecordWorktreeFailure),
        DeliveryCleanupWriteCommand::ReconcileWorktree(request) => store
            .reconcile_worktree_cleanup(request)
            .await
            .map(DeliveryCleanupWriteOutcome::ReconcileWorktree),
        DeliveryCleanupWriteCommand::AcceptBranch(request) => store
            .accept_branch_cleanup(request)
            .await
            .map(DeliveryCleanupWriteOutcome::AcceptBranch),
        DeliveryCleanupWriteCommand::RefreshBranchTarget(request) => store
            .refresh_branch_cleanup_target(request)
            .await
            .map(DeliveryCleanupWriteOutcome::RefreshBranchTarget),
        DeliveryCleanupWriteCommand::CompleteBranch(request) => store
            .complete_branch_cleanup(request)
            .await
            .map(DeliveryCleanupWriteOutcome::CompleteBranch),
        DeliveryCleanupWriteCommand::RecordBranchFailure(request) => store
            .record_branch_cleanup_failure(request)
            .await
            .map(DeliveryCleanupWriteOutcome::RecordBranchFailure),
        DeliveryCleanupWriteCommand::ReconcileBranch(request) => store
            .reconcile_branch_cleanup(request)
            .await
            .map(DeliveryCleanupWriteOutcome::ReconcileBranch),
    }
}

pub(super) fn classify_outcome(outcome: DeliveryCleanupWriteOutcome) -> DeliveryDisposition {
    match outcome {
        confirmed @ DeliveryCleanupWriteOutcome::AcceptWorktree(
            CleanupAcceptanceOutcome::Accepted(_) | CleanupAcceptanceOutcome::Existing(_),
        )
        | confirmed @ DeliveryCleanupWriteOutcome::RecordWorktreeUnlocked(
            CleanupTransitionOutcome::Applied(_) | CleanupTransitionOutcome::Existing(_),
        )
        | confirmed @ DeliveryCleanupWriteOutcome::EnterWorktreeRemovePending(
            CleanupTransitionOutcome::Applied(_) | CleanupTransitionOutcome::Existing(_),
        )
        | confirmed @ DeliveryCleanupWriteOutcome::CompleteWorktree(
            CleanupTransitionOutcome::Applied(_) | CleanupTransitionOutcome::Existing(_),
        )
        | confirmed @ DeliveryCleanupWriteOutcome::RecordWorktreeFailure(
            CleanupTransitionOutcome::Applied(_) | CleanupTransitionOutcome::Existing(_),
        )
        | confirmed @ DeliveryCleanupWriteOutcome::ReconcileWorktree(
            CleanupTransitionOutcome::Applied(_) | CleanupTransitionOutcome::Existing(_),
        )
        | confirmed @ DeliveryCleanupWriteOutcome::AcceptBranch(
            CleanupAcceptanceOutcome::Accepted(_) | CleanupAcceptanceOutcome::Existing(_),
        )
        | confirmed @ DeliveryCleanupWriteOutcome::RefreshBranchTarget(
            CleanupTransitionOutcome::Applied(_) | CleanupTransitionOutcome::Existing(_),
        )
        | confirmed @ DeliveryCleanupWriteOutcome::CompleteBranch(
            CleanupTransitionOutcome::Applied(_) | CleanupTransitionOutcome::Existing(_),
        )
        | confirmed @ DeliveryCleanupWriteOutcome::RecordBranchFailure(
            CleanupTransitionOutcome::Applied(_) | CleanupTransitionOutcome::Existing(_),
        )
        | confirmed @ DeliveryCleanupWriteOutcome::ReconcileBranch(
            CleanupTransitionOutcome::Applied(_) | CleanupTransitionOutcome::Existing(_),
        ) => DeliveryDisposition::Confirmed(super::DeliveryWriteOutcome::Cleanup(confirmed)),
        conflict @ DeliveryCleanupWriteOutcome::AcceptWorktree(
            CleanupAcceptanceOutcome::Conflict,
        )
        | conflict @ DeliveryCleanupWriteOutcome::RecordWorktreeUnlocked(
            CleanupTransitionOutcome::Conflict,
        )
        | conflict @ DeliveryCleanupWriteOutcome::EnterWorktreeRemovePending(
            CleanupTransitionOutcome::Conflict,
        )
        | conflict @ DeliveryCleanupWriteOutcome::CompleteWorktree(
            CleanupTransitionOutcome::Conflict,
        )
        | conflict @ DeliveryCleanupWriteOutcome::RecordWorktreeFailure(
            CleanupTransitionOutcome::Conflict,
        )
        | conflict @ DeliveryCleanupWriteOutcome::ReconcileWorktree(
            CleanupTransitionOutcome::Conflict,
        )
        | conflict
        @ DeliveryCleanupWriteOutcome::AcceptBranch(CleanupAcceptanceOutcome::Conflict)
        | conflict @ DeliveryCleanupWriteOutcome::RefreshBranchTarget(
            CleanupTransitionOutcome::Conflict,
        )
        | conflict @ DeliveryCleanupWriteOutcome::CompleteBranch(
            CleanupTransitionOutcome::Conflict,
        )
        | conflict @ DeliveryCleanupWriteOutcome::RecordBranchFailure(
            CleanupTransitionOutcome::Conflict,
        )
        | conflict @ DeliveryCleanupWriteOutcome::ReconcileBranch(
            CleanupTransitionOutcome::Conflict,
        ) => DeliveryDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::ExactReconciliation,
            outcome: Some(super::DeliveryWriteOutcome::Cleanup(conflict)),
            error: None,
        },
    }
}
