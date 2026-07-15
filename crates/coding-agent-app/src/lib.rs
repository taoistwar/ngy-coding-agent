//! Application composition responsibilities for the coding agent.
//!
//! Store mutation internals are deliberately unavailable to application consumers:
//!
//! ```compile_fail
//! use coding_agent_app::{
//!     StoreWriterBackend, StoreWriterBackendFuture, StoreWriterOperation,
//!     StoreWriterOperationOutcome,
//! };
//! ```

mod event_dispatcher;
mod service_state;
mod store_writer;
mod task_manager;

pub use event_dispatcher::{EventDispatcherError, EventDispatcherHandle};
pub use service_state::{
    InvalidServiceTransition, ServiceState, ServiceStateController, ServiceStateSnapshot,
};
pub use store_writer::{EventWake, StoreWriterError, StoreWriterHandle, WriteReceipt};
pub use task_manager::{
    CancelOutcome, QuiesceResult, RunContext, RunnerEvent, RunnerEventError, RunnerEventSink,
    RunnerOutcome, RunnerShutdownHandle, TaskManagerError, TaskManagerHandle, TaskRunner,
};
