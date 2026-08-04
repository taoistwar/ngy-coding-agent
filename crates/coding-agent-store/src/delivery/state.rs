#[macro_use]
mod wire;

mod cleanup;
mod merge;
mod source;
mod transition;

pub use cleanup::{
    BranchDisposition, CleanupKind, CleanupOperationState, CleanupState, WorktreeDisposition,
    validate_cleanup_state, validate_cleanup_transition,
};
pub use merge::{MergeOperationState, validate_merge_source_state};
pub use source::DeliverySourceState;
pub use transition::{CleanupTransition, DeliveryState, StateTransition};
