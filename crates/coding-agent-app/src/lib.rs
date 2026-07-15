//! Application composition responsibilities for the coding agent.

mod service_state;
mod store_writer;

pub use service_state::{
    InvalidServiceTransition, ServiceState, ServiceStateController, ServiceStateSnapshot,
};
pub use store_writer::{
    EventWake, StoreWriterBackend, StoreWriterBackendFuture, StoreWriterError, StoreWriterHandle,
    StoreWriterOperation, StoreWriterOperationOutcome, WriteReceipt,
};
