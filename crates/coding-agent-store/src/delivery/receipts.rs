mod codec;
mod hash;
mod model;
mod requests;
mod store;

pub use model::{
    DeliveryAcceptedOperationState, DeliveryCommand, DeliveryCommandKind, DeliveryCommandLookup,
    DeliveryCommandReceipt, DeliveryResponseDiscriminator,
};
pub use requests::{
    AcceptMergeCommandRequest, DeleteBranchCommandRequest, PreflightCommandRequest,
    RemoveWorktreeCommandRequest,
};
pub(super) use store::{ReceiptWrite, insert_receipt, lookup_receipt};

pub const DELIVERY_COMMAND_REQUEST_HASH_DOMAIN: &str = "coding-agent-delivery-command-request";
pub const DELIVERY_COMMAND_REQUEST_HASH_VERSION: u16 = 1;
pub const DELIVERY_COMMAND_REQUEST_HASH_ALGORITHM: &str = "sha256";
