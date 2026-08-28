use std::fmt;

use coding_agent_domain::TaskId;
use coding_agent_store::{
    AcceptMergeCommandRequest, DeleteBranchCommandRequest, PreflightCommandRequest,
    RemoveWorktreeCommandRequest,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DeliveryManagerError {
    #[error("delivery manager is closed")]
    Closed,
}

#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryPreflightRequest {
    pub(super) command: PreflightCommandRequest,
}

#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryAcceptRequest {
    pub(super) command: AcceptMergeCommandRequest,
}

#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryRemoveWorktreeRequest {
    pub(super) command: RemoveWorktreeCommandRequest,
}

#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryDeleteBranchRequest {
    pub(super) command: DeleteBranchCommandRequest,
}

impl DeliveryAcceptRequest {
    pub const fn new(command: AcceptMergeCommandRequest) -> Self {
        Self { command }
    }

    pub const fn task_id(&self) -> TaskId {
        self.command.task_id()
    }

    pub const fn command(&self) -> &AcceptMergeCommandRequest {
        &self.command
    }

    pub fn into_command(self) -> AcceptMergeCommandRequest {
        self.command
    }
}

impl fmt::Debug for DeliveryAcceptRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryAcceptRequest")
            .field("task_id", &self.command.task_id())
            .field("request_values", &"<redacted>")
            .finish()
    }
}

impl DeliveryRemoveWorktreeRequest {
    pub const fn new(command: RemoveWorktreeCommandRequest) -> Self {
        Self { command }
    }

    pub const fn task_id(&self) -> TaskId {
        self.command.task_id()
    }

    pub const fn command(&self) -> &RemoveWorktreeCommandRequest {
        &self.command
    }

    pub fn into_command(self) -> RemoveWorktreeCommandRequest {
        self.command
    }
}

impl fmt::Debug for DeliveryRemoveWorktreeRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryRemoveWorktreeRequest")
            .field("task_id", &self.command.task_id())
            .field("request_values", &"<redacted>")
            .finish()
    }
}

impl DeliveryDeleteBranchRequest {
    pub const fn new(command: DeleteBranchCommandRequest) -> Self {
        Self { command }
    }

    pub const fn task_id(&self) -> TaskId {
        self.command.task_id()
    }

    pub const fn command(&self) -> &DeleteBranchCommandRequest {
        &self.command
    }

    pub fn into_command(self) -> DeleteBranchCommandRequest {
        self.command
    }
}

impl fmt::Debug for DeliveryDeleteBranchRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryDeleteBranchRequest")
            .field("task_id", &self.command.task_id())
            .field("request_values", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryOperationRecoveryOutcome {
    Converged,
    Pending,
    NotFound,
    ReconciliationRequired,
    RetainedFailClosed,
    Unavailable,
}

impl DeliveryPreflightRequest {
    pub const fn new(command: PreflightCommandRequest) -> Self {
        Self { command }
    }

    pub const fn task_id(&self) -> TaskId {
        self.command.task_id()
    }

    pub const fn command(&self) -> &PreflightCommandRequest {
        &self.command
    }

    pub fn into_command(self) -> PreflightCommandRequest {
        self.command
    }
}

impl fmt::Debug for DeliveryPreflightRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryPreflightRequest")
            .field("task_id", &self.command.task_id())
            .field("request_values", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryManagerQuiesceSnapshot {
    pub(super) in_flight_workers: usize,
    pub(super) queued_workers: usize,
}

impl DeliveryManagerQuiesceSnapshot {
    pub const fn in_flight_workers(self) -> usize {
        self.in_flight_workers
    }

    pub const fn queued_workers(self) -> usize {
        self.queued_workers
    }
}
