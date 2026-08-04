mod decode;
mod lookup;
mod write;

#[cfg(test)]
mod tests;

pub(crate) use lookup::lookup_receipt;
pub(crate) use write::{ReceiptWrite, insert_receipt};

use crate::StoreError;

const RECEIPT_INVARIANT: &str = "delivery command receipt is inconsistent";

fn receipt_invariant() -> StoreError {
    StoreError::InvariantViolation(RECEIPT_INVARIANT)
}
