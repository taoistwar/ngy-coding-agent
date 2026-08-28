mod cleanup;
mod merge;
mod source;

use coding_agent_store::{DeliveryMutationKey, DeliveryMutationKind, Store, StoreError};
use tokio::sync::oneshot;

pub use cleanup::{DeliveryCleanupWriteCommand, DeliveryCleanupWriteOutcome};
pub use merge::{DeliveryMergeWriteCommand, DeliveryMergeWriteOutcome};
pub use source::{DeliverySourceWriteCommand, DeliverySourceWriteOutcome};

use super::execution::{TypedExecutionError, execute_typed};
use super::{StoreWriterBackend, StoreWriterOperation, StoreWriterOperationOutcome, WriteCommand};
use crate::pending_durable::{KnownNotAppliedReason, OutcomeUnknownReason};

const DELIVERY_OUTCOME_MISMATCH: &str =
    "store writer backend returned a mismatched delivery outcome";

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
pub enum DeliveryWriteCommand {
    Cleanup(DeliveryCleanupWriteCommand),
    Merge(DeliveryMergeWriteCommand),
    Source(DeliverySourceWriteCommand),
}

impl DeliveryWriteCommand {
    pub fn mutation_key(&self) -> DeliveryMutationKey {
        match self {
            Self::Cleanup(command) => command.mutation_key(),
            Self::Merge(command) => command.mutation_key(),
            Self::Source(command) => command.mutation_key(),
        }
    }

    pub const fn kind(&self) -> DeliveryMutationKind {
        match self {
            Self::Cleanup(command) => command.kind(),
            Self::Merge(command) => command.kind(),
            Self::Source(command) => command.kind(),
        }
    }

    #[cfg(feature = "test-support")]
    pub(in crate::store_writer) const fn test_kind(
        &self,
    ) -> super::super::StoreWriterOperationKind {
        match self {
            Self::Cleanup(command) => command.test_kind(),
            Self::Merge(command) => command.test_kind(),
            Self::Source(command) => command.test_kind(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
pub enum DeliveryWriteOutcome {
    Cleanup(DeliveryCleanupWriteOutcome),
    Merge(DeliveryMergeWriteOutcome),
    Source(DeliverySourceWriteOutcome),
}

impl DeliveryWriteOutcome {
    pub const fn kind(&self) -> DeliveryMutationKind {
        match self {
            Self::Cleanup(outcome) => outcome.kind(),
            Self::Merge(outcome) => outcome.kind(),
            Self::Source(outcome) => outcome.kind(),
        }
    }

    pub(in crate::store_writer) const fn committed_durable_state(&self) -> bool {
        match self {
            Self::Cleanup(outcome) => outcome.committed_durable_state(),
            Self::Merge(outcome) => outcome.committed_durable_state(),
            Self::Source(outcome) => outcome.committed_durable_state(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeliverySubmissionIdentity {
    mutation_key: DeliveryMutationKey,
}

impl DeliverySubmissionIdentity {
    pub const fn mutation_key(&self) -> &DeliveryMutationKey {
        &self.mutation_key
    }

    pub const fn kind(&self) -> DeliveryMutationKind {
        self.mutation_key.kind()
    }

    pub(in crate::store_writer) fn for_command(command: &DeliveryWriteCommand) -> Self {
        Self {
            mutation_key: command.mutation_key(),
        }
    }
}

#[derive(Debug)]
// Keeping the exact typed command in the unknown branch lets the caller replay
// the original mutation without reconstructing safety-critical fields.
#[allow(clippy::large_enum_variant)]
pub enum DeliveryDisposition {
    Confirmed(DeliveryWriteOutcome),
    KnownNotApplied {
        reason: KnownNotAppliedReason,
        outcome: Option<DeliveryWriteOutcome>,
        error: Option<StoreError>,
    },
    OutcomeUnknown {
        reason: OutcomeUnknownReason,
        command: DeliveryWriteCommand,
    },
    InvariantConflict {
        message: &'static str,
        outcome: Option<DeliveryWriteOutcome>,
    },
}

#[derive(Debug)]
pub struct DeliveryCompletion {
    pub identity: DeliverySubmissionIdentity,
    pub disposition: DeliveryDisposition,
}

pub struct DeliverySubmission {
    pub(in crate::store_writer) identity: DeliverySubmissionIdentity,
    pub(in crate::store_writer) pending_command: DeliveryWriteCommand,
    pub(in crate::store_writer) completion_channel_closed_reason: OutcomeUnknownReason,
    pub(in crate::store_writer) receiver: oneshot::Receiver<DeliveryCompletion>,
}

impl DeliverySubmission {
    pub const fn identity(&self) -> &DeliverySubmissionIdentity {
        &self.identity
    }

    pub async fn completion(self) -> DeliveryCompletion {
        match self.receiver.await {
            Ok(completion) => completion,
            Err(_) => DeliveryCompletion {
                identity: self.identity,
                disposition: DeliveryDisposition::OutcomeUnknown {
                    reason: self.completion_channel_closed_reason,
                    command: self.pending_command,
                },
            },
        }
    }
}

pub(in crate::store_writer) async fn execute_store(
    store: &Store,
    command: DeliveryWriteCommand,
) -> Result<DeliveryWriteOutcome, StoreError> {
    match command {
        DeliveryWriteCommand::Cleanup(command) => cleanup::execute_store(store, command)
            .await
            .map(DeliveryWriteOutcome::Cleanup),
        DeliveryWriteCommand::Merge(command) => merge::execute_store(store, command)
            .await
            .map(DeliveryWriteOutcome::Merge),
        DeliveryWriteCommand::Source(command) => source::execute_store(store, command)
            .await
            .map(DeliveryWriteOutcome::Source),
    }
}

pub(super) async fn process_write_command(
    command: WriteCommand,
    backend: &dyn StoreWriterBackend,
) -> bool {
    let WriteCommand::Delivery {
        identity,
        command,
        deadline,
        reconciliation_lane,
        response,
    } = command
    else {
        return false;
    };
    if identity.mutation_key() != &command.mutation_key() {
        let _ = response.send(DeliveryCompletion {
            identity,
            disposition: DeliveryDisposition::InvariantConflict {
                message: "delivery submission identity does not match its typed command",
                outcome: None,
            },
        });
        return true;
    }

    let operation = StoreWriterOperation::Delivery(Box::new(command.clone()));
    let execution = execute_typed(
        backend,
        operation.clone(),
        Some(operation),
        deadline,
        reconciliation_lane,
    )
    .await;
    let disposition = match execution.result {
        Ok(StoreWriterOperationOutcome::Delivery(outcome)) if outcome.kind() == command.kind() => {
            classify_outcome(*outcome)
        }
        Ok(StoreWriterOperationOutcome::Delivery(outcome)) => {
            DeliveryDisposition::InvariantConflict {
                message: DELIVERY_OUTCOME_MISMATCH,
                outcome: Some(*outcome),
            }
        }
        Ok(_) => DeliveryDisposition::InvariantConflict {
            message: DELIVERY_OUTCOME_MISMATCH,
            outcome: None,
        },
        Err(error) => classify_execution_error(error, command),
    };
    let _ = response.send(DeliveryCompletion {
        identity,
        disposition,
    });
    true
}

fn classify_outcome(outcome: DeliveryWriteOutcome) -> DeliveryDisposition {
    match outcome {
        DeliveryWriteOutcome::Cleanup(outcome) => cleanup::classify_outcome(outcome),
        DeliveryWriteOutcome::Merge(outcome) => merge::classify_outcome(outcome),
        DeliveryWriteOutcome::Source(outcome) => source::classify_outcome(outcome),
    }
}

fn classify_execution_error(
    error: TypedExecutionError,
    command: DeliveryWriteCommand,
) -> DeliveryDisposition {
    match error {
        TypedExecutionError::DeadlineBeforeStart => DeliveryDisposition::KnownNotApplied {
            reason: KnownNotAppliedReason::DeadlineBeforeStart,
            outcome: None,
            error: None,
        },
        TypedExecutionError::OutcomeUnknown(reason) => {
            DeliveryDisposition::OutcomeUnknown { reason, command }
        }
        TypedExecutionError::Known(super::super::StoreWriterError::Busy) => {
            DeliveryDisposition::KnownNotApplied {
                reason: KnownNotAppliedReason::BusyRolledBack,
                outcome: None,
                error: None,
            }
        }
        TypedExecutionError::Known(super::super::StoreWriterError::DeadlineElapsed) => {
            DeliveryDisposition::KnownNotApplied {
                reason: KnownNotAppliedReason::DeadlineBeforeStart,
                outcome: None,
                error: None,
            }
        }
        TypedExecutionError::Known(super::super::StoreWriterError::Closed) => {
            DeliveryDisposition::OutcomeUnknown {
                reason: OutcomeUnknownReason::CommitStatusUnknown,
                command,
            }
        }
        TypedExecutionError::Known(super::super::StoreWriterError::Store(
            StoreError::Database(_),
        )) => DeliveryDisposition::OutcomeUnknown {
            reason: OutcomeUnknownReason::CommitStatusUnknown,
            command,
        },
        TypedExecutionError::Known(super::super::StoreWriterError::Store(
            StoreError::InvariantViolation(message),
        )) => DeliveryDisposition::InvariantConflict {
            message,
            outcome: None,
        },
        TypedExecutionError::Known(super::super::StoreWriterError::Store(error)) => {
            DeliveryDisposition::KnownNotApplied {
                reason: KnownNotAppliedReason::KnownRollback,
                outcome: None,
                error: Some(error),
            }
        }
    }
}
