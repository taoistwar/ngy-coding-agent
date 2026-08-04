use coding_agent_domain::{EventCursor, TaskEventKind, TaskId};

use crate::event_dispatcher::EventDispatcherError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TerminalProjectionAttempt {
    task_id: TaskId,
    operation_nonce: u64,
    attempt_id: u64,
    target: EventCursor,
    event_kind: TaskEventKind,
}

impl TerminalProjectionAttempt {
    pub(super) fn try_new(
        task_id: TaskId,
        operation_nonce: u64,
        attempt_id: u64,
        target: EventCursor,
        event_kind: TaskEventKind,
    ) -> Result<Self, TerminalProjectionAttemptError> {
        if operation_nonce == 0 {
            return Err(TerminalProjectionAttemptError::ZeroOperationNonce);
        }
        if attempt_id == 0 {
            return Err(TerminalProjectionAttemptError::ZeroAttemptId);
        }
        if target == EventCursor::ZERO {
            return Err(TerminalProjectionAttemptError::ZeroTarget);
        }
        if !is_terminal_event_kind(event_kind) {
            return Err(TerminalProjectionAttemptError::NonTerminalEventKind);
        }

        Ok(Self {
            task_id,
            operation_nonce,
            attempt_id,
            target,
            event_kind,
        })
    }

    pub(super) const fn task_id(self) -> TaskId {
        self.task_id
    }

    pub(super) const fn operation_nonce(self) -> u64 {
        self.operation_nonce
    }

    pub(super) const fn attempt_id(self) -> u64 {
        self.attempt_id
    }

    pub(super) const fn target(self) -> EventCursor {
        self.target
    }

    pub(super) const fn event_kind(self) -> TaskEventKind {
        self.event_kind
    }

    fn with_attempt_id(self, attempt_id: u64) -> Result<Self, TerminalProjectionAttemptError> {
        if attempt_id <= self.attempt_id {
            return Err(TerminalProjectionAttemptError::NonMonotonicRetry {
                current: self.attempt_id,
                requested: attempt_id,
            });
        }

        Ok(Self { attempt_id, ..self })
    }

    fn has_same_scope(self, other: Self) -> bool {
        self.task_id == other.task_id && self.operation_nonce == other.operation_nonce
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(super) enum TerminalProjectionAttemptError {
    #[error("terminal projection operation nonce must be positive")]
    ZeroOperationNonce,
    #[error("terminal projection attempt ID must be positive")]
    ZeroAttemptId,
    #[error("terminal projection target must be positive")]
    ZeroTarget,
    #[error("terminal projection event kind must be terminal")]
    NonTerminalEventKind,
    #[error(
        "terminal projection retry attempt must advance beyond {current}, requested {requested}"
    )]
    NonMonotonicRetry { current: u64, requested: u64 },
}

#[derive(Debug, Clone)]
pub(crate) struct TerminalProjectionCompletion {
    attempt: TerminalProjectionAttempt,
    result: Result<(), EventDispatcherError>,
}

impl TerminalProjectionCompletion {
    pub(super) fn new(
        attempt: TerminalProjectionAttempt,
        result: Result<(), EventDispatcherError>,
    ) -> Self {
        Self { attempt, result }
    }

    pub(super) const fn attempt(&self) -> TerminalProjectionAttempt {
        self.attempt
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TerminalProjectionCompletionDisposition {
    IgnoreStale,
    Conflict,
    Projected {
        target: EventCursor,
        event_kind: TaskEventKind,
    },
    RetrySameTarget {
        target: EventCursor,
        event_kind: TaskEventKind,
    },
    FreezeRetainingBarrier,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TerminalProjectionBarrier {
    current: TerminalProjectionAttempt,
}

impl TerminalProjectionBarrier {
    pub(super) const fn new(initial: TerminalProjectionAttempt) -> Self {
        Self { current: initial }
    }

    pub(super) const fn current(&self) -> TerminalProjectionAttempt {
        self.current
    }

    pub(super) fn classify_completion(
        &self,
        completion: &TerminalProjectionCompletion,
    ) -> TerminalProjectionCompletionDisposition {
        match self.relation_to(completion.attempt()) {
            TerminalProjectionAttemptRelation::StaleSameScope => {
                TerminalProjectionCompletionDisposition::IgnoreStale
            }
            TerminalProjectionAttemptRelation::Conflict => {
                TerminalProjectionCompletionDisposition::Conflict
            }
            TerminalProjectionAttemptRelation::Exact => match &completion.result {
                Ok(()) => TerminalProjectionCompletionDisposition::Projected {
                    target: self.current.target(),
                    event_kind: self.current.event_kind(),
                },
                Err(EventDispatcherError::Store(_)) => {
                    TerminalProjectionCompletionDisposition::RetrySameTarget {
                        target: self.current.target(),
                        event_kind: self.current.event_kind(),
                    }
                }
                Err(EventDispatcherError::Closed | EventDispatcherError::StartupCursorMismatch) => {
                    TerminalProjectionCompletionDisposition::FreezeRetainingBarrier
                }
            },
        }
    }

    pub(super) fn advance_retry(
        &mut self,
        next_attempt_id: u64,
    ) -> Result<TerminalProjectionAttempt, TerminalProjectionAttemptError> {
        let retry = self.current.with_attempt_id(next_attempt_id)?;
        self.current = retry;
        Ok(retry)
    }

    fn relation_to(
        &self,
        completion: TerminalProjectionAttempt,
    ) -> TerminalProjectionAttemptRelation {
        if self.current.has_same_scope(completion)
            && completion.attempt_id() < self.current.attempt_id()
            && completion.target() == self.current.target()
            && completion.event_kind() == self.current.event_kind()
        {
            return TerminalProjectionAttemptRelation::StaleSameScope;
        }
        if completion == self.current {
            TerminalProjectionAttemptRelation::Exact
        } else {
            TerminalProjectionAttemptRelation::Conflict
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalProjectionAttemptRelation {
    Exact,
    StaleSameScope,
    Conflict,
}

const fn is_terminal_event_kind(event_kind: TaskEventKind) -> bool {
    matches!(
        event_kind,
        TaskEventKind::TaskCompleted
            | TaskEventKind::TaskFailed
            | TaskEventKind::TaskCancelled
            | TaskEventKind::TaskInterrupted
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use coding_agent_store::StoreError;

    use super::*;

    #[test]
    fn attempt_requires_positive_terminal_identity() {
        let task_id = TaskId::new();
        let target = cursor(11);

        assert_eq!(
            TerminalProjectionAttempt::try_new(task_id, 0, 1, target, TaskEventKind::TaskCompleted,),
            Err(TerminalProjectionAttemptError::ZeroOperationNonce)
        );
        assert_eq!(
            TerminalProjectionAttempt::try_new(task_id, 7, 0, target, TaskEventKind::TaskCompleted,),
            Err(TerminalProjectionAttemptError::ZeroAttemptId)
        );
        assert_eq!(
            TerminalProjectionAttempt::try_new(
                task_id,
                7,
                1,
                EventCursor::ZERO,
                TaskEventKind::TaskCompleted,
            ),
            Err(TerminalProjectionAttemptError::ZeroTarget)
        );
        assert_eq!(
            TerminalProjectionAttempt::try_new(
                task_id,
                7,
                1,
                target,
                TaskEventKind::ActivityAppended,
            ),
            Err(TerminalProjectionAttemptError::NonTerminalEventKind)
        );
    }

    #[test]
    fn exact_success_projects_the_actor_owned_target_and_kind() {
        let attempt = attempt(TaskId::new(), 7, 11, 19, TaskEventKind::TaskFailed);
        let barrier = TerminalProjectionBarrier::new(attempt);
        let completion = TerminalProjectionCompletion::new(attempt, Ok(()));

        assert_eq!(
            barrier.classify_completion(&completion),
            TerminalProjectionCompletionDisposition::Projected {
                target: cursor(19),
                event_kind: TaskEventKind::TaskFailed,
            }
        );
        assert_eq!(barrier.current(), attempt);
    }

    #[test]
    fn old_attempt_from_the_same_scope_is_ignored_after_retry_advances() {
        let task_id = TaskId::new();
        let original = attempt(task_id, 7, 11, 19, TaskEventKind::TaskCompleted);
        let mut barrier = TerminalProjectionBarrier::new(original);

        let retry = barrier.advance_retry(13).expect("advance retry attempt");
        assert_eq!(retry.task_id(), original.task_id());
        assert_eq!(retry.operation_nonce(), original.operation_nonce());
        assert_eq!(retry.target(), original.target());
        assert_eq!(retry.event_kind(), original.event_kind());

        let stale = TerminalProjectionCompletion::new(original, Ok(()));
        assert_eq!(
            barrier.classify_completion(&stale),
            TerminalProjectionCompletionDisposition::IgnoreStale
        );

        let stale_with_conflicting_metadata = TerminalProjectionCompletion::new(
            attempt(task_id, 7, 12, 18, TaskEventKind::TaskCancelled),
            Ok(()),
        );
        assert_eq!(
            barrier.classify_completion(&stale_with_conflicting_metadata),
            TerminalProjectionCompletionDisposition::Conflict
        );
    }

    #[test]
    fn current_attempt_metadata_mismatch_is_a_conflict() {
        let task_id = TaskId::new();
        let current = attempt(task_id, 7, 11, 19, TaskEventKind::TaskCompleted);
        let barrier = TerminalProjectionBarrier::new(current);
        let mismatches = [
            attempt(TaskId::new(), 7, 11, 19, TaskEventKind::TaskCompleted),
            attempt(task_id, 8, 11, 19, TaskEventKind::TaskCompleted),
            attempt(task_id, 7, 12, 19, TaskEventKind::TaskCompleted),
            attempt(task_id, 7, 11, 20, TaskEventKind::TaskCompleted),
            attempt(task_id, 7, 11, 19, TaskEventKind::TaskFailed),
        ];

        for mismatch in mismatches {
            let completion = TerminalProjectionCompletion::new(mismatch, Ok(()));
            assert_eq!(
                barrier.classify_completion(&completion),
                TerminalProjectionCompletionDisposition::Conflict
            );
        }
    }

    #[test]
    fn store_error_retries_without_changing_target_or_event_kind() {
        let original = attempt(TaskId::new(), 7, 11, 19, TaskEventKind::TaskInterrupted);
        let mut barrier = TerminalProjectionBarrier::new(original);
        let completion = TerminalProjectionCompletion::new(
            original,
            Err(EventDispatcherError::Store(Arc::new(
                StoreError::InvariantViolation("terminal projection test failure"),
            ))),
        );

        assert_eq!(
            barrier.classify_completion(&completion),
            TerminalProjectionCompletionDisposition::RetrySameTarget {
                target: original.target(),
                event_kind: original.event_kind(),
            }
        );

        let retry = barrier.advance_retry(17).expect("advance retry attempt");
        assert_eq!(retry.task_id(), original.task_id());
        assert_eq!(retry.operation_nonce(), original.operation_nonce());
        assert_eq!(retry.target(), original.target());
        assert_eq!(retry.event_kind(), original.event_kind());
        assert_eq!(retry.attempt_id(), 17);
    }

    #[test]
    fn closed_dispatcher_freezes_without_consuming_the_barrier() {
        let attempt = attempt(TaskId::new(), 7, 11, 19, TaskEventKind::TaskCancelled);
        let barrier = TerminalProjectionBarrier::new(attempt);
        let completion =
            TerminalProjectionCompletion::new(attempt, Err(EventDispatcherError::Closed));

        assert_eq!(
            barrier.classify_completion(&completion),
            TerminalProjectionCompletionDisposition::FreezeRetainingBarrier
        );
        assert_eq!(barrier.current(), attempt);
    }

    #[test]
    fn retry_attempt_must_advance_and_failure_keeps_current_attempt() {
        let original = attempt(TaskId::new(), 7, 11, 19, TaskEventKind::TaskCompleted);
        let mut barrier = TerminalProjectionBarrier::new(original);

        assert_eq!(
            barrier.advance_retry(11),
            Err(TerminalProjectionAttemptError::NonMonotonicRetry {
                current: 11,
                requested: 11,
            })
        );
        assert_eq!(
            barrier.advance_retry(10),
            Err(TerminalProjectionAttemptError::NonMonotonicRetry {
                current: 11,
                requested: 10,
            })
        );
        assert_eq!(barrier.current(), original);
    }

    fn attempt(
        task_id: TaskId,
        operation_nonce: u64,
        attempt_id: u64,
        target: i64,
        event_kind: TaskEventKind,
    ) -> TerminalProjectionAttempt {
        TerminalProjectionAttempt::try_new(
            task_id,
            operation_nonce,
            attempt_id,
            cursor(target),
            event_kind,
        )
        .expect("valid terminal projection attempt")
    }

    fn cursor(value: i64) -> EventCursor {
        EventCursor::new(value).expect("valid event cursor")
    }
}
