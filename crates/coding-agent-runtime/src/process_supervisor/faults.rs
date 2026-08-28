//! Test-only, task-scoped process fault injection.
//!
//! The controller deliberately selects children only by their sanitized,
//! one-based admission ordinal. It cannot inspect or replace argv, cwd, the
//! executable, environment, or command-policy bindings. Admission happens in
//! `ProcessSupervisor` only after every retained command capability has been
//! revalidated and immediately before the real spawn boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::{self, Instant};

use crate::{ProcessCleanupProof, ProcessLivenessScope};

tokio::task_local! {
    static CURRENT_PROCESS_FAULT_CONTROLLER: ProcessFaultController;
}

/// One deterministic process-supervisor fault available to integration tests.
///
/// These variants describe only supervisor boundaries. They intentionally do
/// not identify the child command which reached that boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProcessFault {
    BeforeSpawn,
    AfterSpawnUnknown,
    StdoutOverflow,
    Deadline,
    WaitUnknown,
    ChannelUnknown,
    KillFailure,
    CleanupFailure,
}

/// Sanitized lifecycle events recorded by a [`ProcessFaultController`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessFaultEventKind {
    Admitted,
    Injected(ProcessFault),
    Returned,
}

/// One sanitized process lifecycle event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessFaultEvent {
    child_ordinal: u64,
    kind: ProcessFaultEventKind,
}

impl ProcessFaultEvent {
    pub const fn child_ordinal(self) -> u64 {
        self.child_ordinal
    }

    pub const fn kind(self) -> ProcessFaultEventKind {
        self.kind
    }
}

/// Redacted construction or proof failure from a test fault controller.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ProcessFaultControllerError {
    InvalidSchedule,
    NoChildObserved,
    FaultNotInjected,
    InvalidTimeout,
    ZeroLiveProofTimedOut,
}

impl ProcessFaultControllerError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidSchedule => "PROCESS_FAULT_INVALID_SCHEDULE",
            Self::NoChildObserved => "PROCESS_FAULT_NO_CHILD_OBSERVED",
            Self::FaultNotInjected => "PROCESS_FAULT_NOT_INJECTED",
            Self::InvalidTimeout => "PROCESS_FAULT_INVALID_TIMEOUT",
            Self::ZeroLiveProofTimedOut => "PROCESS_FAULT_ZERO_LIVE_UNPROVEN",
        }
    }
}

impl fmt::Debug for ProcessFaultControllerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProcessFaultControllerError(<redacted>)")
    }
}

impl fmt::Display for ProcessFaultControllerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("process fault controller failed")
    }
}

impl std::error::Error for ProcessFaultControllerError {}

/// Opaque proof that every liveness scope which admitted a controlled child
/// had no live or unproven process tree at the proof boundary.
pub struct ProcessFaultZeroLiveProof {
    observed_children: u64,
    checked_scopes: usize,
}

impl ProcessFaultZeroLiveProof {
    pub const fn observed_children(&self) -> u64 {
        self.observed_children
    }

    pub const fn checked_scopes(&self) -> usize {
        self.checked_scopes
    }
}

impl fmt::Debug for ProcessFaultZeroLiveProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProcessFaultZeroLiveProof(<opaque>)")
    }
}

/// Parallel-safe fault schedule scoped to one Tokio task tree by
/// [`Self::scope`].
///
/// A controller is inert outside that async scope. Nested or concurrent tests
/// therefore restore their previous task-local state automatically when the
/// scoped future completes or is dropped.
#[derive(Clone)]
pub struct ProcessFaultController {
    inner: Arc<ProcessFaultControllerInner>,
}

struct ProcessFaultControllerInner {
    schedule: BTreeMap<u64, ProcessFault>,
    next_ordinal: AtomicU64,
    events: Mutex<Vec<ProcessFaultEvent>>,
    scopes: Mutex<Vec<ProcessLivenessScope>>,
}

impl ProcessFaultController {
    /// Schedules one fault for one one-based admitted-child ordinal.
    pub fn for_child(
        child_ordinal: u64,
        fault: ProcessFault,
    ) -> Result<Self, ProcessFaultControllerError> {
        Self::from_schedule([(child_ordinal, fault)])
    }

    /// Builds an immutable one-based fault schedule.
    ///
    /// Duplicate or zero ordinals are rejected. A schedule may contain more
    /// than one child, but at most one fault can be assigned to each child.
    pub fn from_schedule(
        schedule: impl IntoIterator<Item = (u64, ProcessFault)>,
    ) -> Result<Self, ProcessFaultControllerError> {
        let mut collected = BTreeMap::new();
        for (ordinal, fault) in schedule {
            if ordinal == 0 || collected.insert(ordinal, fault).is_some() {
                return Err(ProcessFaultControllerError::InvalidSchedule);
            }
        }
        if collected.is_empty() {
            return Err(ProcessFaultControllerError::InvalidSchedule);
        }
        Ok(Self {
            inner: Arc::new(ProcessFaultControllerInner {
                schedule: collected,
                next_ordinal: AtomicU64::new(0),
                events: Mutex::new(Vec::new()),
                scopes: Mutex::new(Vec::new()),
            }),
        })
    }

    /// Runs `future` with this controller installed only in the current Tokio
    /// task-local scope. The previous controller is restored by Tokio's scope
    /// guard even when the future is cancelled or panics.
    pub async fn scope<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        CURRENT_PROCESS_FAULT_CONTROLLER
            .scope(self.clone(), future)
            .await
    }

    /// Returns a snapshot containing only child ordinals and sanitized events.
    pub fn events(&self) -> Vec<ProcessFaultEvent> {
        lock(&self.inner.events).clone()
    }

    /// Waits for the real process-liveness scopes used by admitted children to
    /// prove that no process tree remains live or unproven.
    ///
    /// The proof also requires every scheduled fault to have reached its
    /// injection boundary, preventing a wrong ordinal from passing vacuously.
    pub async fn prove_zero_live(
        &self,
        timeout: Duration,
    ) -> Result<ProcessFaultZeroLiveProof, ProcessFaultControllerError> {
        if timeout.is_zero() {
            return Err(ProcessFaultControllerError::InvalidTimeout);
        }
        let observed_children = self.inner.next_ordinal.load(Ordering::SeqCst);
        if observed_children == 0 {
            return Err(ProcessFaultControllerError::NoChildObserved);
        }
        let injected = lock(&self.inner.events)
            .iter()
            .filter_map(|event| match event.kind {
                ProcessFaultEventKind::Injected(fault) => Some((event.child_ordinal, fault)),
                ProcessFaultEventKind::Admitted | ProcessFaultEventKind::Returned => None,
            })
            .collect::<BTreeSet<_>>();
        if self
            .inner
            .schedule
            .iter()
            .any(|(ordinal, fault)| !injected.contains(&(*ordinal, *fault)))
        {
            return Err(ProcessFaultControllerError::FaultNotInjected);
        }

        let deadline = Instant::now() + timeout;
        loop {
            let scopes = lock(&self.inner.scopes).clone();
            if scopes.is_empty() {
                return Err(ProcessFaultControllerError::NoChildObserved);
            }
            let all_confirmed = scopes.iter().all(|scope| {
                scope.active_tree_count() == 0
                    && matches!(scope.cleanup_proof(), Ok(ProcessCleanupProof::Confirmed))
            });
            if all_confirmed {
                return Ok(ProcessFaultZeroLiveProof {
                    observed_children,
                    checked_scopes: scopes.len(),
                });
            }
            if Instant::now() >= deadline {
                return Err(ProcessFaultControllerError::ZeroLiveProofTimedOut);
            }
            time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn admit(&self, scope: &ProcessLivenessScope) -> ProcessFaultInvocation {
        let ordinal = self.inner.next_ordinal.fetch_add(1, Ordering::SeqCst) + 1;
        lock(&self.inner.scopes).push(scope.clone());
        self.record(ordinal, ProcessFaultEventKind::Admitted);
        ProcessFaultInvocation {
            controller: self.clone(),
            child_ordinal: ordinal,
            fault: self.inner.schedule.get(&ordinal).copied(),
            lifecycle: Arc::new(ProcessFaultInvocationLifecycle::default()),
            records_return: true,
        }
    }

    fn record(&self, child_ordinal: u64, kind: ProcessFaultEventKind) {
        lock(&self.inner.events).push(ProcessFaultEvent {
            child_ordinal,
            kind,
        });
    }
}

impl fmt::Debug for ProcessFaultController {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProcessFaultController(<opaque>)")
    }
}

pub(super) struct ProcessFaultInvocation {
    controller: ProcessFaultController,
    child_ordinal: u64,
    fault: Option<ProcessFault>,
    lifecycle: Arc<ProcessFaultInvocationLifecycle>,
    records_return: bool,
}

#[derive(Default)]
struct ProcessFaultInvocationLifecycle {
    injected: AtomicBool,
    returned: AtomicBool,
}

impl ProcessFaultInvocation {
    pub(super) const fn fault(&self) -> Option<ProcessFault> {
        self.fault
    }

    pub(super) fn injected(&self) {
        if let Some(fault) = self.fault
            && !self.lifecycle.injected.swap(true, Ordering::SeqCst)
        {
            self.controller
                .record(self.child_ordinal, ProcessFaultEventKind::Injected(fault));
        }
    }
}

impl Clone for ProcessFaultInvocation {
    fn clone(&self) -> Self {
        Self {
            controller: self.controller.clone(),
            child_ordinal: self.child_ordinal,
            fault: self.fault,
            lifecycle: Arc::clone(&self.lifecycle),
            // The admitted invocation remains in `ProcessExecution` (or the
            // pre-spawn call frame) and alone records the externally visible
            // return boundary. Worker clones must not race that event ahead of
            // faults injected while `ProcessExecution::wait` reconciles the
            // completed worker result.
            records_return: false,
        }
    }
}

impl Drop for ProcessFaultInvocation {
    fn drop(&mut self) {
        if self.records_return && !self.lifecycle.returned.swap(true, Ordering::SeqCst) {
            self.controller
                .record(self.child_ordinal, ProcessFaultEventKind::Returned);
        }
    }
}

pub(super) fn admit_current(scope: &ProcessLivenessScope) -> Option<ProcessFaultInvocation> {
    CURRENT_PROCESS_FAULT_CONTROLLER
        .try_with(|controller| controller.admit(scope))
        .ok()
}

pub(super) fn injected_error(fault: ProcessFault) -> std::io::Error {
    std::io::Error::other(format!("injected process fault: {fault:?}"))
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedules_are_one_based_unique_and_controller_diagnostics_are_redacted() {
        let empty = ProcessFaultController::from_schedule(std::iter::empty()).unwrap_err();
        let zero = ProcessFaultController::for_child(0, ProcessFault::BeforeSpawn).unwrap_err();
        let duplicate = ProcessFaultController::from_schedule([
            (1, ProcessFault::BeforeSpawn),
            (1, ProcessFault::Deadline),
        ])
        .unwrap_err();

        for error in [empty, zero, duplicate] {
            assert_eq!(error.code(), "PROCESS_FAULT_INVALID_SCHEDULE");
            assert_eq!(
                format!("{error:?}"),
                "ProcessFaultControllerError(<redacted>)"
            );
            assert_eq!(format!("{error}"), "process fault controller failed");
        }

        let controller = ProcessFaultController::for_child(1, ProcessFault::BeforeSpawn).unwrap();
        assert_eq!(
            format!("{controller:?}"),
            "ProcessFaultController(<opaque>)"
        );
    }
}
