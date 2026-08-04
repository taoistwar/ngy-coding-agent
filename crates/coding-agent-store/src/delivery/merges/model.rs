mod commands;
mod outcomes;
mod paths;
mod preflight;
mod reasons;

pub use outcomes::{AcceptMergeOutcome, MergeTransitionOutcome, MergeTransitionReceipt};
pub use paths::MergeConflictPaths;
pub use preflight::{MergePreflightResult, RecordMergePreflightResultRequest};
pub use reasons::{MergeKnownNotAppliedReason, MergeReconciliationReason, PreflightRejectedReason};

pub use commands::{
    BeginMergeAbortRequest, CompleteMergeAbortRequest, CompleteMergeRequest,
    EnterMergePendingRequest, ReconcileMergeRequest, RecordMergeKnownFailureRequest,
};
pub(in crate::delivery) use paths::raw_relative_path_is_canonical;
pub(in crate::delivery) use reasons::merge_failure_code_is_valid;
