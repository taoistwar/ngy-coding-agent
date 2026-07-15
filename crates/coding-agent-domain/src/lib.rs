//! Pure domain types and invariants for the coding agent.

mod event;
mod ids;
mod repository;
mod task;
mod value;

pub use event::{
    ActivityEntry, ActivityLevel, DiffFile, DiffFileStatus, DiffSnapshot, PlanItem, PlanItemStatus,
    PlanSnapshot, TaskEvent, TaskEventKind, TaskEventPayload, TestCase, TestSnapshot, TestStatus,
    TimelineEntry,
};
pub use ids::{ClientRequestId, RepositoryId, TaskId};
pub use repository::{NewRepository, Repository};
pub use task::{NewTask, Task, TaskFailure, TaskStatus};
pub use value::{CanonicalPath, DomainError, EventCursor, EventId, UtcTimestamp};
