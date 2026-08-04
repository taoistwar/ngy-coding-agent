mod admission;
mod generation_publisher;
mod permit_release;

pub(crate) use crate::scheduler_api_projection::SchedulerStoreState;

pub use admission::{
    CandidateEvaluation, QueueReason, QueueReasonSignals, QueuedTaskCandidate,
    RepositoryCoordinationKey, SchedulerAdmissionGates, SchedulerConcurrencyLimits,
    SchedulerLimitError, SchedulerRepositoryStorageState, SchedulerScan, SchedulerScanError,
    SchedulerStorageNotification, SchedulerStorageNotificationSink, project_queue_reason,
    scan_queued_candidates,
};
pub use generation_publisher::{
    SchedulerProjectionCandidate, SchedulerProjectionSnapshot, SchedulerPublishOutcome,
    SchedulerPublisherError, SchedulerStatePublisher,
};
pub(crate) use generation_publisher::{SchedulerStateReader, SchedulerStateWatch};
pub(crate) use permit_release::PreparedTerminalPermitRelease;
pub use permit_release::{
    PermitLedger, PermitLedgerError, PermitLedgerSnapshot, PermitOwnershipState,
    PermitOwnershipWitness, PermitToken, SharedPermitOwnership, TerminalProcessCleanReleaseProof,
    TerminalReleaseProofError, advance_membership_watermark, is_membership_lifecycle_event,
    is_terminal_membership_event,
};
