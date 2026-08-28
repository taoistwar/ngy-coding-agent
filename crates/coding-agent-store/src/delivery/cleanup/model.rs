mod anchor;
mod branch;
mod outcomes;
mod reasons;
mod worktree;

pub use anchor::CleanupOperationAnchor;
pub use branch::{
    CompleteBranchCleanupRequest, ReconcileBranchCleanupRequest, RecordBranchCleanupFailureRequest,
    RefreshBranchCleanupTargetRequest,
};
pub use outcomes::{CleanupAcceptanceOutcome, CleanupTransitionOutcome, CleanupTransitionReceipt};
pub use reasons::{
    BranchCleanupKnownNotAppliedReason, CleanupReconciliationReason,
    WorktreeCleanupKnownNotAppliedReason,
};
pub use worktree::{
    CompleteWorktreeCleanupRequest, EnterWorktreeRemovePendingRequest,
    ReconcileWorktreeCleanupRequest, RecordWorktreeCleanupFailureRequest,
    RecordWorktreeUnlockedRequest,
};

use crate::delivery::mutation::{
    DeliveryMutationEntity, DeliveryMutationEntityKind, DeliveryMutationKey, DeliveryMutationKind,
    impl_delivery_mutation_request,
};

macro_rules! cleanup_mutation_request {
    ($request:ty, $kind:expr) => {
        impl_delivery_mutation_request!($request, |request| {
            cleanup_mutation_key($kind, request.anchor)
        });
    };
}

cleanup_mutation_request!(
    RecordWorktreeUnlockedRequest,
    DeliveryMutationKind::RecordWorktreeUnlocked
);
cleanup_mutation_request!(
    EnterWorktreeRemovePendingRequest,
    DeliveryMutationKind::EnterWorktreeRemovePending
);
cleanup_mutation_request!(
    CompleteWorktreeCleanupRequest,
    DeliveryMutationKind::CompleteWorktreeCleanup
);
cleanup_mutation_request!(
    RecordWorktreeCleanupFailureRequest,
    DeliveryMutationKind::RecordWorktreeCleanupFailure
);
cleanup_mutation_request!(
    ReconcileWorktreeCleanupRequest,
    DeliveryMutationKind::ReconcileWorktreeCleanup
);
cleanup_mutation_request!(
    RefreshBranchCleanupTargetRequest,
    DeliveryMutationKind::RefreshBranchCleanupTarget
);
cleanup_mutation_request!(
    CompleteBranchCleanupRequest,
    DeliveryMutationKind::CompleteBranchCleanup
);
cleanup_mutation_request!(
    RecordBranchCleanupFailureRequest,
    DeliveryMutationKind::RecordBranchCleanupFailure
);
cleanup_mutation_request!(
    ReconcileBranchCleanupRequest,
    DeliveryMutationKind::ReconcileBranchCleanup
);

fn cleanup_mutation_key(
    kind: DeliveryMutationKind,
    anchor: CleanupOperationAnchor,
) -> DeliveryMutationKey {
    DeliveryMutationKey::new(
        kind,
        anchor.task_id,
        vec![DeliveryMutationEntity::operation(
            DeliveryMutationEntityKind::CleanupOperation,
            anchor.operation_id,
            anchor.expected_version,
        )],
        None,
    )
}
