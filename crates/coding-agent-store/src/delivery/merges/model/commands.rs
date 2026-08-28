mod abort;
mod merged;
mod pending;
mod terminal;

pub use abort::{BeginMergeAbortRequest, CompleteMergeAbortRequest};
pub use merged::CompleteMergeRequest;
pub use pending::EnterMergePendingRequest;
pub use terminal::{ReconcileMergeRequest, RecordMergeKnownFailureRequest};

use crate::delivery::mutation::{
    DeliveryMutationEntity, DeliveryMutationEntityKind, DeliveryMutationKey, DeliveryMutationKind,
    impl_delivery_mutation_request,
};

macro_rules! merge_mutation_request {
    ($request:ty, $kind:expr) => {
        impl_delivery_mutation_request!($request, |request| {
            DeliveryMutationKey::new(
                $kind,
                request.task_id,
                vec![DeliveryMutationEntity::operation(
                    DeliveryMutationEntityKind::MergeOperation,
                    request.operation_id,
                    request.expected_version,
                )],
                None,
            )
        });
    };
}

merge_mutation_request!(
    EnterMergePendingRequest,
    DeliveryMutationKind::EnterMergePending
);
merge_mutation_request!(CompleteMergeRequest, DeliveryMutationKind::CompleteMerge);
merge_mutation_request!(
    BeginMergeAbortRequest,
    DeliveryMutationKind::BeginMergeAbort
);
merge_mutation_request!(
    CompleteMergeAbortRequest,
    DeliveryMutationKind::CompleteMergeAbort
);
merge_mutation_request!(
    RecordMergeKnownFailureRequest,
    DeliveryMutationKind::RecordMergeKnownFailure
);
merge_mutation_request!(ReconcileMergeRequest, DeliveryMutationKind::ReconcileMerge);
