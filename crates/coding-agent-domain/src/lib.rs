//! Pure domain types and invariants for the coding agent.

mod event;
mod ids;
mod quality;
mod repository;
mod task;
mod value;

pub use event::{
    ActivityActor, ActivityEntry, ActivityLevel, DiffFile, DiffFileStatus, DiffSnapshot, PlanItem,
    PlanItemStatus, PlanSnapshot, TaskEvent, TaskEventKind, TaskEventPayload, TestCase,
    TestSnapshot, TestStatus, TimelineEntry,
};
pub use ids::{ClientRequestId, RepositoryId, TaskId};
pub use quality::{
    CheckActor, CheckEvidence, CheckEvidenceStatus, FindingSeverity, MAX_CARGO_SELECTOR_BYTES,
    MAX_WORKSPACE_GENERATION, NewReviewEvidence, RequiredCheck, RequiredCheckKind,
    RequiredCheckSelector, ReviewCoverageEvidence, ReviewDecisionSource, ReviewEvidence,
    ReviewFinding, ReviewVerdict, WorkspaceDigest, WorkspaceDigestAlgorithm,
    is_valid_cargo_selector,
};
pub use repository::{NewRepository, Repository};
pub use task::{DeliveryReadiness, NewTask, Task, TaskFailure, TaskStatus};
pub use value::{CanonicalPath, DomainError, EventCursor, EventId, UtcTimestamp};
