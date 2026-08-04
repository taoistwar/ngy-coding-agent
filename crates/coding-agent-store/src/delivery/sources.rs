mod advance;
mod create;
mod load;
mod model;
mod proof;
mod reconcile;
mod validate;

pub use model::{
    AdvanceDeliverySourceObjectRequest, CommitDeliverySourceRequest, CreateDeliverySourceOutcome,
    CreateDeliverySourceRequest, DeliverySourceAnchor, DeliverySourceReconciliationReason,
    DeliverySourceRetryReason, DeliverySourceTransitionOutcome, DeliverySourceTransitionReceipt,
    ReconcileDeliverySourceOutcome, ReconcileDeliverySourceReceipt, ReconcileDeliverySourceRequest,
    RecordDeliverySourceRetryRequest,
};
pub use proof::{DeliverySourceAppliedProof, DeliverySourceObjectProof, SourceWorktreeProof};

use crate::StoreError;

const SOURCE_INVARIANT: &str = "delivery source transaction is inconsistent";
const SOURCE_AUTHOR_NAME: &str = "Coding Agent";
const SOURCE_AUTHOR_EMAIL: &str = "coding-agent@localhost";
const SOURCE_MESSAGE_TEMPLATE_VERSION: u32 = 1;

fn source_invariant() -> StoreError {
    StoreError::InvariantViolation(SOURCE_INVARIANT)
}
