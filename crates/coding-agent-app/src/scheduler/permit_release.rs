use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use coding_agent_domain::{EventCursor, EventId, TaskEventKind, TaskId};

use super::{RepositoryCoordinationKey, SchedulerConcurrencyLimits};
use crate::task_manager::TaskProcessCleanupConfirmation;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermitOwnershipState {
    Provisional,
    Submitted,
    OutcomeUnknown,
    Active,
    Abandoned,
    Released,
}

impl PermitOwnershipState {
    const fn owns_capacity(self) -> bool {
        !matches!(self, Self::Released)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermitLedgerSnapshot {
    limits: SchedulerConcurrencyLimits,
    global_owned: u32,
    global_active: u32,
    repository_owned: HashMap<RepositoryCoordinationKey, u32>,
    repository_active: HashMap<RepositoryCoordinationKey, u32>,
    active_tasks: HashMap<TaskId, RepositoryCoordinationKey>,
    abandoned_tasks: Vec<TaskId>,
    has_abandoned: bool,
}

impl PermitLedgerSnapshot {
    pub const fn limits(&self) -> SchedulerConcurrencyLimits {
        self.limits
    }

    pub const fn global_owned(&self) -> u32 {
        self.global_owned
    }

    pub const fn global_active(&self) -> u32 {
        self.global_active
    }

    pub fn repository_owned(&self, key: RepositoryCoordinationKey) -> u32 {
        self.repository_owned.get(&key).copied().unwrap_or(0)
    }

    pub fn repository_active(&self, key: RepositoryCoordinationKey) -> u32 {
        self.repository_active.get(&key).copied().unwrap_or(0)
    }

    pub(crate) fn active_tasks(
        &self,
    ) -> impl Iterator<Item = (TaskId, RepositoryCoordinationKey)> + '_ {
        self.active_tasks
            .iter()
            .map(|(task_id, key)| (*task_id, *key))
    }

    pub fn abandoned_tasks(&self) -> &[TaskId] {
        &self.abandoned_tasks
    }

    pub const fn has_abandoned(&self) -> bool {
        self.has_abandoned
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PermitLedgerError {
    #[error("global scheduler capacity is exhausted")]
    GlobalCapacity,
    #[error("repository scheduler capacity is exhausted")]
    RepositoryCapacity,
    #[error("the task already owns scheduler capacity")]
    TaskAlreadyOwned,
    #[error("permit token does not belong to this ledger")]
    ForeignToken,
    #[error("permit token is no longer registered")]
    UnknownToken,
    #[error("permit token identity is inconsistent")]
    TokenIdentityMismatch,
    #[error("permit transition is invalid from {from:?} to {to:?}")]
    InvalidTransition {
        from: PermitOwnershipState,
        to: PermitOwnershipState,
    },
    #[error("permit capacity has already been released")]
    AlreadyReleased,
    #[error("permit token space is exhausted")]
    TokenSpaceExhausted,
}

#[derive(Debug)]
struct PermitEntry {
    task_id: TaskId,
    coordination_key: RepositoryCoordinationKey,
    state: PermitOwnershipState,
    durable_adopted: bool,
}

#[derive(Debug)]
struct PermitLedgerState {
    ledger_id: u64,
    limits: SchedulerConcurrencyLimits,
    next_token_id: u64,
    entries: HashMap<u64, PermitEntry>,
}

#[derive(Clone, Debug)]
pub struct PermitLedger {
    inner: Arc<Mutex<PermitLedgerState>>,
}

static NEXT_LEDGER_ID: AtomicU64 = AtomicU64::new(1);

impl PermitLedger {
    pub fn new(limits: SchedulerConcurrencyLimits) -> Self {
        let ledger_id = NEXT_LEDGER_ID.fetch_add(1, Ordering::Relaxed);
        assert_ne!(ledger_id, 0, "permit ledger identity space exhausted");
        Self {
            inner: Arc::new(Mutex::new(PermitLedgerState {
                ledger_id,
                limits,
                next_token_id: 1,
                entries: HashMap::new(),
            })),
        }
    }

    pub fn reserve(
        &self,
        task_id: TaskId,
        coordination_key: RepositoryCoordinationKey,
    ) -> Result<PermitToken, PermitLedgerError> {
        let mut state = lock_ledger(&self.inner);
        let snapshot = snapshot_from_state(&state);
        if snapshot.global_owned >= state.limits.global().get() {
            return Err(PermitLedgerError::GlobalCapacity);
        }
        if snapshot.repository_owned(coordination_key) >= state.limits.per_repository().get() {
            return Err(PermitLedgerError::RepositoryCapacity);
        }
        if state
            .entries
            .values()
            .any(|entry| entry.task_id == task_id && entry.state.owns_capacity())
        {
            return Err(PermitLedgerError::TaskAlreadyOwned);
        }

        let token_id = state.next_token_id;
        state.next_token_id = token_id
            .checked_add(1)
            .ok_or(PermitLedgerError::TokenSpaceExhausted)?;
        let ledger_id = state.ledger_id;
        state.entries.insert(
            token_id,
            PermitEntry {
                task_id,
                coordination_key,
                state: PermitOwnershipState::Provisional,
                durable_adopted: false,
            },
        );

        Ok(PermitToken {
            ledger_id,
            token_id,
            task_id,
            coordination_key,
            ledger: Arc::downgrade(&self.inner),
        })
    }

    pub fn adopt(&self, token: &PermitToken) -> Result<(), PermitLedgerError> {
        self.transition(
            token,
            &[
                PermitOwnershipState::Submitted,
                PermitOwnershipState::OutcomeUnknown,
            ],
            PermitOwnershipState::Active,
            true,
        )
    }

    pub fn mark_submitted(&self, token: &PermitToken) -> Result<(), PermitLedgerError> {
        self.transition(
            token,
            &[PermitOwnershipState::Provisional],
            PermitOwnershipState::Submitted,
            true,
        )
    }

    pub fn retain_outcome_unknown(&self, token: &PermitToken) -> Result<(), PermitLedgerError> {
        self.transition(
            token,
            &[PermitOwnershipState::Submitted],
            PermitOwnershipState::OutcomeUnknown,
            true,
        )
    }

    pub fn release_unsubmitted(&self, token: &PermitToken) -> Result<(), PermitLedgerError> {
        self.transition(
            token,
            &[PermitOwnershipState::Provisional],
            PermitOwnershipState::Released,
            false,
        )
    }

    pub fn release_known_not_applied(&self, token: &PermitToken) -> Result<(), PermitLedgerError> {
        self.transition(
            token,
            &[
                PermitOwnershipState::Submitted,
                PermitOwnershipState::OutcomeUnknown,
            ],
            PermitOwnershipState::Released,
            false,
        )
    }

    pub fn release_after_terminal_and_process_clean(
        &self,
        token: &PermitToken,
        proof: &TerminalProcessCleanReleaseProof,
    ) -> Result<(), PermitLedgerError> {
        if proof.task_id != token.task_id
            || proof.ledger_id != token.ledger_id
            || proof.token_id != token.token_id
        {
            return Err(PermitLedgerError::TokenIdentityMismatch);
        }
        self.transition(
            token,
            &[PermitOwnershipState::Active],
            PermitOwnershipState::Released,
            false,
        )
    }

    pub(crate) fn release_terminal_batch(
        &self,
        releases: &[PreparedTerminalPermitRelease<'_>],
    ) -> Result<(), PermitLedgerError> {
        let mut state = lock_ledger(&self.inner);
        let mut task_ids = HashSet::with_capacity(releases.len());
        let mut token_ids = HashSet::with_capacity(releases.len());

        for release in releases {
            if !Arc::ptr_eq(&self.inner, &release.permit.ledger.inner)
                || release.proof.task_id != release.permit.task_id()
                || release.proof.ledger_id != release.permit.token.ledger_id
                || release.proof.token_id != release.permit.token.token_id
                || release.proof.admission_nonce != release.permit.admission_nonce
                || release.proof.process_owner_id != release.permit.process_owner_id
                || release.cleanup.task_id() != release.proof.task_id
                || release.cleanup.operation_nonce() != release.proof.admission_nonce
                || release.cleanup.process_owner_id() != release.proof.process_owner_id
                || !release.cleanup.is_available_for_terminal_release()
                || release.proof.projection_as_of_event_id.get()
                    < release.proof.terminal_event_id.get()
                || release.proof.membership_watermark.get() < release.proof.terminal_event_id.get()
                || !task_ids.insert(release.proof.task_id)
                || !token_ids.insert(release.proof.token_id)
            {
                return Err(PermitLedgerError::TokenIdentityMismatch);
            }

            let entry = validate_token(&state, &release.permit.token)?;
            if entry.state == PermitOwnershipState::Released {
                return Err(PermitLedgerError::AlreadyReleased);
            }
            if entry.state != PermitOwnershipState::Active {
                return Err(PermitLedgerError::InvalidTransition {
                    from: entry.state,
                    to: PermitOwnershipState::Released,
                });
            }
        }

        for release in releases {
            let entry = state
                .entries
                .get_mut(&release.proof.token_id)
                .expect("a preflighted terminal permit remains ledger-owned");
            entry.state = PermitOwnershipState::Released;
            entry.durable_adopted = false;
        }
        Ok(())
    }

    pub fn state(&self, token: &PermitToken) -> Result<PermitOwnershipState, PermitLedgerError> {
        let state = lock_ledger(&self.inner);
        validate_token(&state, token).map(|entry| entry.state)
    }

    pub fn snapshot(&self) -> PermitLedgerSnapshot {
        snapshot_from_state(&lock_ledger(&self.inner))
    }

    fn transition(
        &self,
        token: &PermitToken,
        allowed: &[PermitOwnershipState],
        next: PermitOwnershipState,
        idempotent: bool,
    ) -> Result<(), PermitLedgerError> {
        let mut state = lock_ledger(&self.inner);
        let entry = validate_token_mut(&mut state, token)?;
        if entry.state == next && idempotent {
            return Ok(());
        }
        if entry.state == PermitOwnershipState::Released {
            return Err(PermitLedgerError::AlreadyReleased);
        }
        if !allowed.contains(&entry.state) {
            return Err(PermitLedgerError::InvalidTransition {
                from: entry.state,
                to: next,
            });
        }
        entry.state = next;
        if next == PermitOwnershipState::Active {
            entry.durable_adopted = true;
        }
        Ok(())
    }
}

fn lock_ledger(inner: &Mutex<PermitLedgerState>) -> std::sync::MutexGuard<'_, PermitLedgerState> {
    inner
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn validate_token<'a>(
    state: &'a PermitLedgerState,
    token: &PermitToken,
) -> Result<&'a PermitEntry, PermitLedgerError> {
    if state.ledger_id != token.ledger_id {
        return Err(PermitLedgerError::ForeignToken);
    }
    let entry = state
        .entries
        .get(&token.token_id)
        .ok_or(PermitLedgerError::UnknownToken)?;
    if entry.task_id != token.task_id || entry.coordination_key != token.coordination_key {
        return Err(PermitLedgerError::TokenIdentityMismatch);
    }
    Ok(entry)
}

fn validate_token_mut<'a>(
    state: &'a mut PermitLedgerState,
    token: &PermitToken,
) -> Result<&'a mut PermitEntry, PermitLedgerError> {
    if state.ledger_id != token.ledger_id {
        return Err(PermitLedgerError::ForeignToken);
    }
    let entry = state
        .entries
        .get_mut(&token.token_id)
        .ok_or(PermitLedgerError::UnknownToken)?;
    if entry.task_id != token.task_id || entry.coordination_key != token.coordination_key {
        return Err(PermitLedgerError::TokenIdentityMismatch);
    }
    Ok(entry)
}

fn snapshot_from_state(state: &PermitLedgerState) -> PermitLedgerSnapshot {
    let mut global_owned = 0_u32;
    let mut global_active = 0_u32;
    let mut repository_owned = HashMap::new();
    let mut repository_active = HashMap::new();
    let mut active_tasks = HashMap::new();
    let mut abandoned_tasks = Vec::new();

    for entry in state.entries.values() {
        if entry.state.owns_capacity() {
            global_owned = global_owned
                .checked_add(1)
                .expect("permit ledger cannot own more entries than its configured limit");
            *repository_owned.entry(entry.coordination_key).or_insert(0) += 1;
        }
        if entry.state == PermitOwnershipState::Active
            || (entry.state == PermitOwnershipState::Abandoned && entry.durable_adopted)
        {
            global_active = global_active
                .checked_add(1)
                .expect("active permit count cannot overflow");
            let count = repository_active
                .entry(entry.coordination_key)
                .or_insert(0_u32);
            *count = count
                .checked_add(1)
                .expect("active repository permit count cannot overflow");
            let previous = active_tasks.insert(entry.task_id, entry.coordination_key);
            debug_assert!(
                previous.is_none(),
                "one task cannot hold more than one active permit"
            );
        }
        if entry.state == PermitOwnershipState::Abandoned {
            abandoned_tasks.push(entry.task_id);
        }
    }
    abandoned_tasks.sort_unstable_by_key(|task_id| task_id.as_uuid());

    PermitLedgerSnapshot {
        limits: state.limits,
        global_owned,
        global_active,
        repository_owned,
        repository_active,
        active_tasks,
        has_abandoned: !abandoned_tasks.is_empty(),
        abandoned_tasks,
    }
}

/// Opaque ownership token for one global and one repository permit.
///
/// Dropping an unreleased token fail-closes the entry as abandoned. Capacity
/// remains owned so an actor panic cannot silently over-admit replacement work.
pub struct PermitToken {
    ledger_id: u64,
    token_id: u64,
    task_id: TaskId,
    coordination_key: RepositoryCoordinationKey,
    ledger: Weak<Mutex<PermitLedgerState>>,
}

impl PermitToken {
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub const fn coordination_key(&self) -> RepositoryCoordinationKey {
        self.coordination_key
    }
}

impl fmt::Debug for PermitToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PermitToken")
            .field("task_id", &self.task_id)
            .field("coordination_key", &self.coordination_key)
            .field("ownership", &"<opaque>")
            .finish()
    }
}

impl Drop for PermitToken {
    fn drop(&mut self) {
        let Some(ledger) = self.ledger.upgrade() else {
            return;
        };
        let mut state = lock_ledger(&ledger);
        if state.ledger_id != self.ledger_id {
            return;
        }
        let should_remove = match state.entries.get_mut(&self.token_id) {
            Some(entry)
                if entry.task_id == self.task_id
                    && entry.coordination_key == self.coordination_key =>
            {
                if entry.state == PermitOwnershipState::Released {
                    true
                } else {
                    entry.state = PermitOwnershipState::Abandoned;
                    false
                }
            }
            _ => false,
        };
        if should_remove {
            state.entries.remove(&self.token_id);
        }
    }
}

/// Actor-only transition authority for one move-only scheduler permit.
///
/// Runners receive only [`PermitOwnershipWitness`], which keeps the exact
/// token alive and exposes identity/state without any transition capability.
pub struct SharedPermitOwnership {
    ledger: PermitLedger,
    token: Arc<PermitToken>,
    admission_nonce: u64,
    process_owner_id: u64,
}

impl SharedPermitOwnership {
    pub fn new(
        ledger: PermitLedger,
        token: PermitToken,
        admission_nonce: u64,
        process_owner_id: u64,
    ) -> Result<Self, PermitLedgerError> {
        if admission_nonce == 0 || process_owner_id == 0 {
            return Err(PermitLedgerError::TokenIdentityMismatch);
        }
        ledger.state(&token)?;
        Ok(Self {
            ledger,
            token: Arc::new(token),
            admission_nonce,
            process_owner_id,
        })
    }

    pub fn task_id(&self) -> TaskId {
        self.token.task_id()
    }

    pub fn coordination_key(&self) -> RepositoryCoordinationKey {
        self.token.coordination_key()
    }

    pub const fn admission_nonce(&self) -> u64 {
        self.admission_nonce
    }

    pub const fn process_owner_id(&self) -> u64 {
        self.process_owner_id
    }

    pub fn witness(&self) -> PermitOwnershipWitness {
        PermitOwnershipWitness {
            ledger: self.ledger.clone(),
            token: Arc::clone(&self.token),
            admission_nonce: self.admission_nonce,
            process_owner_id: self.process_owner_id,
        }
    }

    pub fn state(&self) -> Result<PermitOwnershipState, PermitLedgerError> {
        self.ledger.state(&self.token)
    }

    pub fn adopt(&self) -> Result<(), PermitLedgerError> {
        self.ledger.adopt(&self.token)
    }

    pub fn mark_submitted(&self) -> Result<(), PermitLedgerError> {
        self.ledger.mark_submitted(&self.token)
    }

    pub fn retain_outcome_unknown(&self) -> Result<(), PermitLedgerError> {
        self.ledger.retain_outcome_unknown(&self.token)
    }

    pub fn release_unsubmitted(&self) -> Result<(), PermitLedgerError> {
        self.ledger.release_unsubmitted(&self.token)
    }

    pub fn release_known_not_applied(&self) -> Result<(), PermitLedgerError> {
        self.ledger.release_known_not_applied(&self.token)
    }

    pub fn release_after_terminal_and_process_clean(
        &self,
        proof: &TerminalProcessCleanReleaseProof,
    ) -> Result<(), PermitLedgerError> {
        if proof.admission_nonce != self.admission_nonce
            || proof.process_owner_id != self.process_owner_id
        {
            return Err(PermitLedgerError::TokenIdentityMismatch);
        }
        self.ledger
            .release_after_terminal_and_process_clean(&self.token, proof)
    }
}

impl fmt::Debug for SharedPermitOwnership {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SharedPermitOwnership")
            .field("task_id", &self.task_id())
            .field("coordination_key", &self.coordination_key())
            .field("ownership", &"<shared opaque>")
            .finish()
    }
}

/// Cloneable, transition-free evidence carried by a RunContext.
#[derive(Clone)]
pub struct PermitOwnershipWitness {
    ledger: PermitLedger,
    token: Arc<PermitToken>,
    admission_nonce: u64,
    process_owner_id: u64,
}

impl PermitOwnershipWitness {
    pub fn task_id(&self) -> TaskId {
        self.token.task_id()
    }

    pub fn coordination_key(&self) -> RepositoryCoordinationKey {
        self.token.coordination_key()
    }

    pub const fn admission_nonce(&self) -> u64 {
        self.admission_nonce
    }

    pub const fn process_owner_id(&self) -> u64 {
        self.process_owner_id
    }

    pub fn state(&self) -> Result<PermitOwnershipState, PermitLedgerError> {
        self.ledger.state(&self.token)
    }
}

impl fmt::Debug for PermitOwnershipWitness {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PermitOwnershipWitness")
            .field("task_id", &self.task_id())
            .field("coordination_key", &self.coordination_key())
            .field("ownership", &"<witness only>")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TerminalReleaseProofError {
    #[error("permit release requires a terminal lifecycle event")]
    NotTerminalEvent,
    #[error("permit release requires confirmed process-tree cleanup")]
    ProcessTreeNotClean,
    #[error("process-tree cleanup confirmation belongs to a different task")]
    CleanupTaskMismatch,
    #[error("process-tree cleanup confirmation belongs to a different process owner")]
    ProcessOwnerMismatch,
    #[error("process-tree cleanup confirmation was already consumed")]
    CleanupAlreadyConsumed,
    #[error("permit release requires an active adopted permit")]
    PermitNotActive,
    #[error("scheduler projection has not observed the terminal event")]
    ProjectionBehindTerminal,
    #[error("scheduler membership watermark has not recorded the terminal event")]
    MembershipWatermarkBehindTerminal,
}

#[derive(Debug, PartialEq, Eq)]
pub struct TerminalProcessCleanReleaseProof {
    task_id: TaskId,
    ledger_id: u64,
    token_id: u64,
    admission_nonce: u64,
    process_owner_id: u64,
    terminal_event_id: EventId,
    projection_as_of_event_id: EventCursor,
    membership_watermark: EventCursor,
}

pub(crate) struct PreparedTerminalPermitRelease<'a> {
    permit: &'a SharedPermitOwnership,
    proof: &'a TerminalProcessCleanReleaseProof,
    cleanup: &'a TaskProcessCleanupConfirmation,
}

impl<'a> PreparedTerminalPermitRelease<'a> {
    pub(crate) const fn new(
        permit: &'a SharedPermitOwnership,
        proof: &'a TerminalProcessCleanReleaseProof,
        cleanup: &'a TaskProcessCleanupConfirmation,
    ) -> Self {
        Self {
            permit,
            proof,
            cleanup,
        }
    }
}

impl TerminalProcessCleanReleaseProof {
    pub(crate) fn preflight(
        task_id: TaskId,
        terminal_event_kind: TaskEventKind,
        terminal_event_id: EventId,
        projection_as_of_event_id: EventCursor,
        membership_watermark: EventCursor,
        permit: &SharedPermitOwnership,
        process_cleanup: &TaskProcessCleanupConfirmation,
    ) -> Result<(), TerminalReleaseProofError> {
        if !is_terminal_membership_event(terminal_event_kind) {
            return Err(TerminalReleaseProofError::NotTerminalEvent);
        }
        if process_cleanup.task_id() != task_id {
            return Err(TerminalReleaseProofError::CleanupTaskMismatch);
        }
        if permit.task_id() != task_id {
            return Err(TerminalReleaseProofError::CleanupTaskMismatch);
        }
        if permit.admission_nonce() != process_cleanup.operation_nonce() {
            return Err(TerminalReleaseProofError::CleanupTaskMismatch);
        }
        if permit.process_owner_id() != process_cleanup.process_owner_id() {
            return Err(TerminalReleaseProofError::ProcessOwnerMismatch);
        }
        if !process_cleanup.is_available_for_terminal_release() {
            return Err(TerminalReleaseProofError::CleanupAlreadyConsumed);
        }
        if permit.state() != Ok(PermitOwnershipState::Active) {
            return Err(TerminalReleaseProofError::PermitNotActive);
        }
        if projection_as_of_event_id.get() < terminal_event_id.get() {
            return Err(TerminalReleaseProofError::ProjectionBehindTerminal);
        }
        if membership_watermark.get() < terminal_event_id.get() {
            return Err(TerminalReleaseProofError::MembershipWatermarkBehindTerminal);
        }
        Ok(())
    }

    pub(crate) fn prepare_for_atomic_release(
        task_id: TaskId,
        terminal_event_kind: TaskEventKind,
        terminal_event_id: EventId,
        projection_as_of_event_id: EventCursor,
        membership_watermark: EventCursor,
        permit: &SharedPermitOwnership,
        process_cleanup: &TaskProcessCleanupConfirmation,
    ) -> Result<Self, TerminalReleaseProofError> {
        Self::preflight(
            task_id,
            terminal_event_kind,
            terminal_event_id,
            projection_as_of_event_id,
            membership_watermark,
            permit,
            process_cleanup,
        )?;
        Ok(Self {
            task_id,
            ledger_id: permit.token.ledger_id,
            token_id: permit.token.token_id,
            admission_nonce: permit.admission_nonce,
            process_owner_id: permit.process_owner_id,
            terminal_event_id,
            projection_as_of_event_id,
            membership_watermark,
        })
    }

    #[cfg(test)]
    pub(crate) fn try_new(
        task_id: TaskId,
        terminal_event_kind: TaskEventKind,
        terminal_event_id: EventId,
        projection_as_of_event_id: EventCursor,
        membership_watermark: EventCursor,
        permit: &SharedPermitOwnership,
        process_cleanup: &TaskProcessCleanupConfirmation,
    ) -> Result<Self, TerminalReleaseProofError> {
        let proof = Self::prepare_for_atomic_release(
            task_id,
            terminal_event_kind,
            terminal_event_id,
            projection_as_of_event_id,
            membership_watermark,
            permit,
            process_cleanup,
        )?;
        process_cleanup.consume_for_terminal_release()?;
        Ok(proof)
    }

    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub const fn terminal_event_id(&self) -> EventId {
        self.terminal_event_id
    }

    pub const fn projection_as_of_event_id(&self) -> EventCursor {
        self.projection_as_of_event_id
    }

    pub const fn membership_watermark(&self) -> EventCursor {
        self.membership_watermark
    }
}

pub const fn is_membership_lifecycle_event(kind: TaskEventKind) -> bool {
    matches!(
        kind,
        TaskEventKind::TaskQueued
            | TaskEventKind::TaskStarted
            | TaskEventKind::TaskCompleted
            | TaskEventKind::TaskFailed
            | TaskEventKind::TaskCancelled
            | TaskEventKind::TaskInterrupted
    )
}

pub const fn is_terminal_membership_event(kind: TaskEventKind) -> bool {
    matches!(
        kind,
        TaskEventKind::TaskCompleted
            | TaskEventKind::TaskFailed
            | TaskEventKind::TaskCancelled
            | TaskEventKind::TaskInterrupted
    )
}

/// Advances the scheduler membership watermark only for the six lifecycle kinds.
pub fn advance_membership_watermark(
    current: EventCursor,
    kind: TaskEventKind,
    event_id: EventId,
) -> EventCursor {
    if is_membership_lifecycle_event(kind) && event_id.get() > current.get() {
        EventCursor::new(event_id.get()).expect("a positive event ID is a valid event cursor")
    } else {
        current
    }
}

#[cfg(test)]
mod tests;
