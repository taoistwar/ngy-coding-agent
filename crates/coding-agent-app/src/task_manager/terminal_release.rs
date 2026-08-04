use std::collections::HashMap;

use coding_agent_domain::{EventCursor, EventId, Task, TaskEventKind, TaskId};

use crate::scheduler::{
    PermitLedger, PermitLedgerError, PreparedTerminalPermitRelease,
    TerminalProcessCleanReleaseProof, TerminalReleaseProofError,
};

use super::terminal_projection::TerminalProjectionBarrier;
use super::{
    ActiveRunner, ActiveSafetyEntry, ActiveSafetyRegistryState, ActiveStopState, AdmissionPhase,
    CriticalStopFact, RunnerShutdownHandle, TaskProcessCleanupConfirmation, terminal_event_kind,
    terminal_receipt_is_exact,
};

const MAX_TERMINAL_RELEASE_BATCH: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(super) enum TerminalReleaseError {
    #[error("terminal release batch cannot contain more than four tasks")]
    InvalidBatchSize,
    #[error("terminal release batch contains a duplicate task")]
    DuplicateTask,
    #[error("terminal release batch does not exactly match active ownership")]
    ActiveSetMismatch,
    #[error("terminal release active ownership is inconsistent")]
    ActiveOwnershipMismatch,
    #[error("terminal release task is not an exact terminal receipt")]
    TerminalReceiptMismatch,
    #[error("terminal release projection barrier is inconsistent")]
    ProjectionBarrierMismatch,
    #[error("terminal recovery release is not ready")]
    RecoveryNotReady,
    #[error("terminal release safety registry is inconsistent")]
    SafetyRegistryMismatch,
    #[error("terminal recovery release still has a critical safety fact")]
    CriticalSafetyPending,
    #[error("terminal recovery safety generation changed")]
    SafetyGenerationChanged,
    #[error("terminal release process cleanup confirmation is missing or inconsistent")]
    CleanupMismatch,
    #[error("terminal recovery shutdown receiver is missing")]
    ShutdownReceiverMissing,
    #[error("terminal release detached ownership changed after preflight")]
    DetachConflict,
    #[error(transparent)]
    Proof(#[from] TerminalReleaseProofError),
    #[error(transparent)]
    Permit(#[from] PermitLedgerError),
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ProjectedTerminalReleaseRequest {
    task_id: TaskId,
    operation_nonce: u64,
    barrier: TerminalProjectionBarrier,
    published_membership: EventCursor,
}

impl ProjectedTerminalReleaseRequest {
    pub(super) const fn new(
        task_id: TaskId,
        operation_nonce: u64,
        barrier: TerminalProjectionBarrier,
        published_membership: EventCursor,
    ) -> Self {
        Self {
            task_id,
            operation_nonce,
            barrier,
            published_membership,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryReleaseMode {
    QuiesceWithShutdownHandles,
    Degraded,
}

pub(super) struct RecoveryTerminalReleaseRequest<'a> {
    terminal_tasks: &'a [Task],
    projection: EventCursor,
    published_membership: EventCursor,
    expected_safety_generation: u64,
    mode: RecoveryReleaseMode,
}

impl<'a> RecoveryTerminalReleaseRequest<'a> {
    pub(super) const fn for_quiesce(
        terminal_tasks: &'a [Task],
        projection: EventCursor,
        published_membership: EventCursor,
        expected_safety_generation: u64,
    ) -> Self {
        Self {
            terminal_tasks,
            projection,
            published_membership,
            expected_safety_generation,
            mode: RecoveryReleaseMode::QuiesceWithShutdownHandles,
        }
    }

    pub(super) const fn for_degraded(
        terminal_tasks: &'a [Task],
        projection: EventCursor,
        published_membership: EventCursor,
        expected_safety_generation: u64,
    ) -> Self {
        Self {
            terminal_tasks,
            projection,
            published_membership,
            expected_safety_generation,
            mode: RecoveryReleaseMode::Degraded,
        }
    }

    const fn requires_shutdown_handles(&self) -> bool {
        matches!(self.mode, RecoveryReleaseMode::QuiesceWithShutdownHandles)
    }

    const fn requires_recovery_ready(&self) -> bool {
        matches!(self.mode, RecoveryReleaseMode::Degraded)
    }
}

pub(super) struct CommittedActiveRunner {
    task_id: TaskId,
    active: ActiveRunner,
}

impl CommittedActiveRunner {
    pub(super) const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub(super) fn into_active(self) -> ActiveRunner {
        self.active
    }
}

pub(super) struct TerminalReleaseCommit {
    committed: Vec<CommittedActiveRunner>,
    shutdown_handles: Vec<RunnerShutdownHandle>,
}

impl TerminalReleaseCommit {
    pub(super) fn released_count(&self) -> usize {
        self.committed.len()
    }

    pub(super) fn into_parts(self) -> (Vec<CommittedActiveRunner>, Vec<RunnerShutdownHandle>) {
        (self.committed, self.shutdown_handles)
    }
}

pub(super) fn commit_projected_terminal_release(
    ledger: &PermitLedger,
    active: &mut HashMap<TaskId, ActiveRunner>,
    registry: &mut ActiveSafetyRegistryState,
    request: ProjectedTerminalReleaseRequest,
) -> Result<TerminalReleaseCommit, TerminalReleaseError> {
    // Concurrent claims may be actor-owned before their safety entry is
    // published. A single projected release therefore proves only its target;
    // recovery batches still require the registry to equal the full active set.
    let spec = preflight_projected(active, registry, request)?;
    commit_preflighted(ledger, active, registry, vec![spec], false)
}

pub(super) fn commit_recovery_terminal_release(
    ledger: &PermitLedger,
    active: &mut HashMap<TaskId, ActiveRunner>,
    registry: &mut ActiveSafetyRegistryState,
    request: RecoveryTerminalReleaseRequest<'_>,
) -> Result<TerminalReleaseCommit, TerminalReleaseError> {
    preflight_registry_exact(active, registry)?;
    preflight_recovery_registry(registry, request.expected_safety_generation)?;
    let specs = preflight_recovery(active, &request)?;
    commit_preflighted(
        ledger,
        active,
        registry,
        specs,
        request.requires_shutdown_handles(),
    )
}

#[derive(Debug, Clone, Copy)]
struct TerminalReleaseSpec {
    task_id: TaskId,
    operation_nonce: u64,
    event_kind: TaskEventKind,
    event_id: EventId,
    projection: EventCursor,
    published_membership: EventCursor,
}

fn preflight_projected(
    active: &HashMap<TaskId, ActiveRunner>,
    registry: &ActiveSafetyRegistryState,
    request: ProjectedTerminalReleaseRequest,
) -> Result<TerminalReleaseSpec, TerminalReleaseError> {
    let active = active
        .get(&request.task_id)
        .ok_or(TerminalReleaseError::ActiveOwnershipMismatch)?;
    if request.operation_nonce == 0
        || active.operation_nonce != request.operation_nonce
        || active.phase != AdmissionPhase::ProjectionPending
    {
        return Err(TerminalReleaseError::ActiveOwnershipMismatch);
    }
    preflight_common_active(active, request.task_id)?;
    preflight_registry_entry(registry, request.task_id, active)?;

    let task = active
        .terminal_task
        .as_ref()
        .ok_or(TerminalReleaseError::TerminalReceiptMismatch)?;
    let (event_kind, event_id) = active
        .terminal_event
        .ok_or(TerminalReleaseError::TerminalReceiptMismatch)?;
    if !terminal_receipt_is_exact(Some(active), task, event_kind, event_id) {
        return Err(TerminalReleaseError::TerminalReceiptMismatch);
    }

    if active.terminal_projection_barrier != Some(request.barrier) {
        return Err(TerminalReleaseError::ProjectionBarrierMismatch);
    }
    let attempt = request.barrier.current();
    let target = EventCursor::new(event_id.get())
        .expect("a positive terminal event ID is a valid projection cursor");
    if attempt.task_id() != request.task_id
        || attempt.operation_nonce() != request.operation_nonce
        || attempt.target() != target
        || attempt.event_kind() != event_kind
    {
        return Err(TerminalReleaseError::ProjectionBarrierMismatch);
    }
    if request.published_membership.get() < event_id.get() {
        return Err(TerminalReleaseError::ProjectionBarrierMismatch);
    }
    if registry
        .pending_critical
        .get(&request.task_id)
        .is_some_and(|fact| fact.operation_nonce != request.operation_nonce)
    {
        return Err(TerminalReleaseError::SafetyRegistryMismatch);
    }

    Ok(TerminalReleaseSpec {
        task_id: request.task_id,
        operation_nonce: request.operation_nonce,
        event_kind,
        event_id,
        projection: target,
        published_membership: request.published_membership,
    })
}

fn preflight_recovery(
    active: &HashMap<TaskId, ActiveRunner>,
    request: &RecoveryTerminalReleaseRequest<'_>,
) -> Result<Vec<TerminalReleaseSpec>, TerminalReleaseError> {
    if active.len() > MAX_TERMINAL_RELEASE_BATCH || request.terminal_tasks.len() != active.len() {
        return Err(TerminalReleaseError::InvalidBatchSize);
    }

    let mut terminal_by_id = HashMap::with_capacity(request.terminal_tasks.len());
    for task in request.terminal_tasks {
        if terminal_by_id.insert(task.id, task).is_some() {
            return Err(TerminalReleaseError::DuplicateTask);
        }
    }
    if terminal_by_id.len() != active.len()
        || active
            .keys()
            .any(|task_id| !terminal_by_id.contains_key(task_id))
    {
        return Err(TerminalReleaseError::ActiveSetMismatch);
    }

    let mut task_ids = active.keys().copied().collect::<Vec<_>>();
    task_ids.sort_unstable_by_key(|task_id| task_id.as_uuid());
    let mut specs = Vec::with_capacity(task_ids.len());
    for task_id in task_ids {
        let active = active
            .get(&task_id)
            .ok_or(TerminalReleaseError::ActiveSetMismatch)?;
        let task = terminal_by_id
            .get(&task_id)
            .copied()
            .ok_or(TerminalReleaseError::ActiveSetMismatch)?;
        if active.phase != AdmissionPhase::RunnerReturned
            || !matches!(&active.stop_state, ActiveStopState::NoWinner)
            || (request.requires_recovery_ready() && !active.recovery_release_ready)
            || active.terminal_event.is_some()
            || active.terminal_task.is_some()
            || active.terminal_projection_barrier.is_some()
        {
            return Err(TerminalReleaseError::RecoveryNotReady);
        }
        preflight_common_active(active, task_id)?;
        if request.requires_shutdown_handles() && active.done_receiver.is_none() {
            return Err(TerminalReleaseError::ShutdownReceiverMissing);
        }

        let event_kind = terminal_event_kind(task.status)
            .ok_or(TerminalReleaseError::TerminalReceiptMismatch)?;
        if !terminal_receipt_is_exact(Some(active), task, event_kind, task.last_event_id)
            || request.projection.get() < task.last_event_id.get()
            || request.published_membership.get() < task.last_event_id.get()
        {
            return Err(TerminalReleaseError::TerminalReceiptMismatch);
        }
        specs.push(TerminalReleaseSpec {
            task_id,
            operation_nonce: active.operation_nonce,
            event_kind,
            event_id: task.last_event_id,
            projection: request.projection,
            published_membership: request.published_membership,
        });
    }
    Ok(specs)
}

fn preflight_common_active(
    active: &ActiveRunner,
    task_id: TaskId,
) -> Result<(), TerminalReleaseError> {
    let returned = active
        .runner_returned
        .ok_or(TerminalReleaseError::CleanupMismatch)?;
    let cleanup = active
        .cleanup_confirmation
        .as_ref()
        .ok_or(TerminalReleaseError::CleanupMismatch)?;
    if active.operation_nonce == 0
        || active.permit.task_id() != task_id
        || active.permit.admission_nonce() != active.operation_nonce
        || returned.task_id != task_id
        || returned.operation_nonce != active.operation_nonce
        || returned.process_owner_id != active.permit.process_owner_id()
        || cleanup.task_id() != task_id
        || cleanup.operation_nonce() != active.operation_nonce
        || cleanup.process_owner_id() != active.permit.process_owner_id()
        || !cleanup.is_available_for_terminal_release()
    {
        return Err(TerminalReleaseError::CleanupMismatch);
    }
    // `durable_sequence_blocked` deliberately stays latched after a durable
    // terminal mutation (and while recovery owns the task) so late mutations
    // remain rejected. It is not an outstanding-write witness. Exact terminal
    // receipts/projection or recovery receipts establish settlement; the
    // actor-owned maps and counter below prove that no write is still pending.
    if active.in_flight_mutations != 0
        || active.pending_terminal_write.is_some()
        || !active.pending_runner_event_writes.is_empty()
        || !active.pending_record_review_writes.is_empty()
        || !active.pending_record_review_replays.is_empty()
        || active.pending_runner_outcome.is_some()
        || active.cleanup_retry_scheduled
        || active.accepted_stop_task_load_in_flight
        || !active.user_cancel_waiters.is_empty()
        || !active.terminal_cancel_waiters.is_empty()
        || active.control_lease.is_some()
        || active.control_recovery.is_some()
        || active.done_sender.is_none()
    {
        return Err(TerminalReleaseError::ActiveOwnershipMismatch);
    }
    Ok(())
}

fn preflight_registry_exact(
    active: &HashMap<TaskId, ActiveRunner>,
    registry: &ActiveSafetyRegistryState,
) -> Result<(), TerminalReleaseError> {
    if registry.entries.len() != active.len() {
        return Err(TerminalReleaseError::SafetyRegistryMismatch);
    }
    for (task_id, active) in active {
        preflight_registry_entry(registry, *task_id, active)?;
    }
    if registry.pending_critical.iter().any(|(task_id, fact)| {
        !registry
            .entries
            .get(task_id)
            .is_some_and(|entry| entry.operation_nonce == fact.operation_nonce)
    }) {
        return Err(TerminalReleaseError::SafetyRegistryMismatch);
    }
    Ok(())
}

fn preflight_registry_entry(
    registry: &ActiveSafetyRegistryState,
    task_id: TaskId,
    active: &ActiveRunner,
) -> Result<(), TerminalReleaseError> {
    if !registry.entries.get(&task_id).is_some_and(|entry| {
        entry.operation_nonce == active.operation_nonce
            && entry.repository_id == active.repository_id
            && entry.coordination_key == active.permit.coordination_key()
    }) {
        return Err(TerminalReleaseError::SafetyRegistryMismatch);
    }
    Ok(())
}

fn preflight_recovery_registry(
    registry: &ActiveSafetyRegistryState,
    expected_safety_generation: u64,
) -> Result<(), TerminalReleaseError> {
    if registry.safety_generation_overflowed
        || registry.safety_generation != expected_safety_generation
    {
        return Err(TerminalReleaseError::SafetyGenerationChanged);
    }
    if !registry.pending_critical.is_empty()
        || registry
            .entries
            .values()
            .any(|entry| entry.stop.is_latched())
    {
        return Err(TerminalReleaseError::CriticalSafetyPending);
    }
    Ok(())
}

fn commit_preflighted(
    ledger: &PermitLedger,
    active: &mut HashMap<TaskId, ActiveRunner>,
    registry: &mut ActiveSafetyRegistryState,
    specs: Vec<TerminalReleaseSpec>,
    return_shutdown_handles: bool,
) -> Result<TerminalReleaseCommit, TerminalReleaseError> {
    let proofs = prepare_release_proofs(active, &specs)?;
    let mut detached = detach_terminal_ownership(active, registry, &specs)?;
    let batch = detached
        .iter()
        .zip(&proofs)
        .map(|(detached, proof)| {
            PreparedTerminalPermitRelease::new(
                &detached.active.permit,
                proof,
                detached
                    .active
                    .cleanup_confirmation
                    .as_ref()
                    .expect("a preflighted cleanup confirmation remains detached"),
            )
        })
        .collect::<Vec<_>>();

    if let Err(error) = ledger.release_terminal_batch(&batch) {
        drop(batch);
        rollback_detached(active, registry, &mut detached);
        return Err(TerminalReleaseError::Permit(error));
    }
    drop(batch);

    let mut committed = Vec::with_capacity(detached.len());
    let mut shutdown_handles = Vec::with_capacity(if return_shutdown_handles {
        detached.len()
    } else {
        0
    });
    for mut detached in detached {
        consume_preflighted_cleanup(
            detached
                .active
                .cleanup_confirmation
                .as_mut()
                .expect("a committed cleanup confirmation remains detached"),
        );
        if return_shutdown_handles {
            shutdown_handles.push(RunnerShutdownHandle {
                task_id: detached.task_id,
                cancellation: detached.active.cancellation.clone(),
                done: detached
                    .active
                    .done_receiver
                    .take()
                    .expect("a preflighted shutdown receiver remains detached"),
            });
        }
        if let Some(done) = detached.active.done_sender.take() {
            let _ = done.send(());
        }
        committed.push(CommittedActiveRunner {
            task_id: detached.task_id,
            active: detached.active,
        });
    }

    Ok(TerminalReleaseCommit {
        committed,
        shutdown_handles,
    })
}

fn prepare_release_proofs(
    active: &HashMap<TaskId, ActiveRunner>,
    specs: &[TerminalReleaseSpec],
) -> Result<Vec<TerminalProcessCleanReleaseProof>, TerminalReleaseError> {
    let mut proofs = Vec::with_capacity(specs.len());
    for spec in specs {
        let active = active
            .get(&spec.task_id)
            .ok_or(TerminalReleaseError::DetachConflict)?;
        let cleanup = active
            .cleanup_confirmation
            .as_ref()
            .ok_or(TerminalReleaseError::CleanupMismatch)?;
        proofs.push(
            TerminalProcessCleanReleaseProof::prepare_for_atomic_release(
                spec.task_id,
                spec.event_kind,
                spec.event_id,
                spec.projection,
                spec.published_membership,
                &active.permit,
                cleanup,
            )?,
        );
    }
    Ok(proofs)
}

struct DetachedTerminalOwnership {
    task_id: TaskId,
    active: ActiveRunner,
    registry_entry: ActiveSafetyEntry,
    critical_fact: Option<CriticalStopFact>,
}

fn detach_terminal_ownership(
    active: &mut HashMap<TaskId, ActiveRunner>,
    registry: &mut ActiveSafetyRegistryState,
    specs: &[TerminalReleaseSpec],
) -> Result<Vec<DetachedTerminalOwnership>, TerminalReleaseError> {
    let mut detached = Vec::with_capacity(specs.len());
    for spec in specs {
        let Some(active_runner) = active.remove(&spec.task_id) else {
            rollback_detached(active, registry, &mut detached);
            return Err(TerminalReleaseError::DetachConflict);
        };
        let Some(registry_entry) = registry.entries.remove(&spec.task_id) else {
            active.insert(spec.task_id, active_runner);
            rollback_detached(active, registry, &mut detached);
            return Err(TerminalReleaseError::DetachConflict);
        };
        if active_runner.operation_nonce != spec.operation_nonce
            || registry_entry.operation_nonce != spec.operation_nonce
        {
            active.insert(spec.task_id, active_runner);
            registry.entries.insert(spec.task_id, registry_entry);
            rollback_detached(active, registry, &mut detached);
            return Err(TerminalReleaseError::DetachConflict);
        }
        let critical_fact = registry.pending_critical.remove(&spec.task_id);
        detached.push(DetachedTerminalOwnership {
            task_id: spec.task_id,
            active: active_runner,
            registry_entry,
            critical_fact,
        });
    }
    Ok(detached)
}

fn rollback_detached(
    active: &mut HashMap<TaskId, ActiveRunner>,
    registry: &mut ActiveSafetyRegistryState,
    detached: &mut Vec<DetachedTerminalOwnership>,
) {
    for detached in detached.drain(..).rev() {
        let active_replaced = active.insert(detached.task_id, detached.active);
        let registry_replaced = registry
            .entries
            .insert(detached.task_id, detached.registry_entry);
        debug_assert!(active_replaced.is_none());
        debug_assert!(registry_replaced.is_none());
        if let Some(critical_fact) = detached.critical_fact {
            let critical_replaced = registry
                .pending_critical
                .insert(detached.task_id, critical_fact);
            debug_assert!(critical_replaced.is_none());
        }
    }
}

fn consume_preflighted_cleanup(cleanup: &mut TaskProcessCleanupConfirmation) {
    let consumed = cleanup.consumed.get_mut();
    debug_assert!(!*consumed);
    *consumed = true;
}

#[cfg(test)]
impl TaskProcessCleanupConfirmation {
    pub(crate) fn confirmed_for_atomic_release_test(
        task_id: TaskId,
        operation_nonce: u64,
        process_owner_id: u64,
    ) -> Self {
        Self {
            task_id,
            operation_nonce,
            process_owner_id,
            consumed: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_request_modes_keep_shutdown_and_readiness_requirements_distinct() {
        let tasks = Vec::new();
        let quiesce = RecoveryTerminalReleaseRequest::for_quiesce(
            &tasks,
            EventCursor::ZERO,
            EventCursor::ZERO,
            7,
        );
        let degraded = RecoveryTerminalReleaseRequest::for_degraded(
            &tasks,
            EventCursor::ZERO,
            EventCursor::ZERO,
            7,
        );

        assert!(quiesce.requires_shutdown_handles());
        assert!(!quiesce.requires_recovery_ready());
        assert!(!degraded.requires_shutdown_handles());
        assert!(degraded.requires_recovery_ready());
    }
}
