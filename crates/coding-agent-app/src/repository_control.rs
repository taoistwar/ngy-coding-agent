use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError, Weak};

use coding_agent_domain::RepositoryId;
use coding_agent_runtime::DirectoryIdentityMarker;
use coding_agent_store::{AttemptArtifactIdentity, AttemptArtifactState, RepositoryIdentityLookup};

use crate::RepositoryCoordinationKey;
use crate::artifact_reconciliation::VerifiedArtifactReconciliationEvidence;

mod recovery;

use recovery::RepositoryControlRecoveryLifecycle;
pub(crate) use recovery::{RepositoryControlRecoveryState, RepositoryControlRecoveryWitness};

/// Resolves one exact durable Git-root lookup to the authenticated common-Git
/// directory object observed by the current process.
///
/// Implementations must perform all filesystem/runtime work before returning;
/// the coordinator never invokes this callback while holding its state lock.
pub trait RepositoryIdentityResolver: Send + Sync {
    fn resolve(
        &self,
        identity: &RepositoryIdentityLookup,
    ) -> Result<DirectoryIdentityMarker, RepositoryIdentityResolutionError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RepositoryIdentityResolutionError {
    #[error("the registered repository identity is unavailable")]
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepositoryControlState {
    Available,
    Busy,
    Poisoned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepositoryControlPoisonReason {
    AbnormalLeaseDrop,
    GitChildOutcomeUnknown,
    ReservationWriteFailed,
    ReadyWriteFailed,
    InconsistentWriteFailed,
    IdentityUnavailable,
    IdentityDrift,
    SideEffectIdentityMismatch,
    AliasConflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RepositoryControlError {
    #[error("the repository identity is unavailable")]
    IdentityUnavailable,
    #[error("the repository identity changed")]
    IdentityDrift,
    #[error("the repository identity aliases are inconsistent")]
    AliasConflict,
    #[error("the repository is not registered with the coordinator")]
    UnknownRepository,
    #[error("the repository coordination key is not registered")]
    UnknownCoordinationKey,
    #[error("the repository coordinator is busy")]
    Busy,
    #[error("the repository coordination key is poisoned")]
    Poisoned,
    #[error("the repository coordination key is not poisoned")]
    NotPoisoned,
    #[error("the repository control lease is no longer current")]
    StaleLease,
    #[error("the repository control lease identity space is exhausted")]
    LeaseSpaceExhausted,
    #[error("the reconciliation proof does not match the owned lease")]
    InvalidReconciliationProof,
}

#[derive(Clone)]
pub struct RepositoryControlCoordinator {
    inner: Arc<CoordinatorInner>,
}

struct CoordinatorInner {
    state: Mutex<CoordinatorState>,
}

#[derive(Clone, Default)]
struct CoordinatorState {
    next_lease_id: u64,
    repositories: HashMap<RepositoryId, RepositoryAlias>,
    seeds: HashMap<String, RepositoryCoordinationKey>,
    groups: HashMap<RepositoryCoordinationKey, CoordinationGroup>,
}

#[derive(Clone)]
struct RepositoryAlias {
    identity: RepositoryIdentityLookup,
    key: RepositoryCoordinationKey,
}

#[derive(Clone)]
struct CoordinationGroup {
    repositories: HashSet<RepositoryId>,
    seeds: HashSet<String>,
    ownership: CoordinationOwnership,
    poison: Option<PoisonRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CoordinationOwnership {
    Available,
    Held { lease_id: u64, kind: LeaseKind },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PoisonRecord {
    reason: RepositoryControlPoisonReason,
    generation: u64,
    generation_exhausted: bool,
    observed_reasons: u16,
}

impl Default for RepositoryControlCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl RepositoryControlCoordinator {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CoordinatorInner {
                state: Mutex::new(CoordinatorState {
                    next_lease_id: 1,
                    ..CoordinatorState::default()
                }),
            }),
        }
    }

    /// Resolves and registers one Store-projected alias.
    ///
    /// Resolution deliberately happens before the coordinator lock is taken.
    pub fn register_alias(
        &self,
        lookup: RepositoryIdentityLookup,
        resolver: &dyn RepositoryIdentityResolver,
    ) -> Result<RepositoryCoordinationKey, RepositoryControlError> {
        self.register_aliases([lookup], resolver)
            .map(|mut registered| {
                registered
                    .pop()
                    .expect("one successful lookup produces one registered alias")
                    .1
            })
    }

    /// Registers one durable alias from an already authenticated and retained
    /// common-Git directory capability.
    ///
    /// Dynamic runtime attachment uses this path so the coordinator, worktree
    /// provisioner, and storage monitor all bind to the same observed object
    /// rather than reopening a string path independently.
    pub fn register_authenticated_alias(
        &self,
        lookup: RepositoryIdentityLookup,
        marker: DirectoryIdentityMarker,
    ) -> Result<RepositoryCoordinationKey, RepositoryControlError> {
        let repository_id = lookup.repository_id;
        let mut state = lock_state(&self.inner.state);
        let mut candidate = state.clone();
        let mut poison_events = Vec::new();
        match register_resolved_alias(&mut candidate, lookup, marker, &mut poison_events) {
            Ok(key) => {
                *state = candidate;
                Ok(key)
            }
            Err(error) => {
                apply_poison_events(&mut state, poison_events);
                debug_assert!(
                    state.repositories.contains_key(&repository_id)
                        || matches!(
                            error,
                            RepositoryControlError::AliasConflict
                                | RepositoryControlError::IdentityDrift
                        ),
                    "a failed first authenticated registration cannot create an alias"
                );
                Err(error)
            }
        }
    }

    /// Records that one durable common-Git seed could not be authenticated.
    ///
    /// This observation is deliberately non-constructive: it may poison an
    /// already registered seed group and/or repository group, but it never
    /// creates an alias or changes either mapping. That distinction lets a
    /// newly durable alias fail closed when it shares a seed with an existing
    /// coordination group without guessing a path-derived identity.
    pub fn observe_identity_unavailable(&self, lookup: &RepositoryIdentityLookup) {
        let mut state = lock_state(&self.inner.state);
        let mut poison_events = Vec::new();
        record_identity_unavailable(&mut state, &mut poison_events, lookup);
    }

    /// Resolves every callback before taking the coordinator lock, then
    /// validates and commits the whole alias batch atomically.
    ///
    /// A failed batch never leaves newly registered aliases behind. Security
    /// evidence discovered while validating the batch remains sticky on any
    /// pre-existing coordination groups it implicates.
    pub fn register_aliases(
        &self,
        lookups: impl IntoIterator<Item = RepositoryIdentityLookup>,
        resolver: &dyn RepositoryIdentityResolver,
    ) -> Result<Vec<(RepositoryId, RepositoryCoordinationKey)>, RepositoryControlError> {
        let observations = lookups
            .into_iter()
            .map(|lookup| {
                let marker = resolver.resolve(&lookup);
                (lookup, marker)
            })
            .collect::<Vec<_>>();
        let mut state = lock_state(&self.inner.state);
        let mut candidate = state.clone();
        let mut poison_events = Vec::new();
        let mut registered = Vec::with_capacity(observations.len());
        let mut first_error = None;

        for (lookup, marker) in observations {
            let repository_id = lookup.repository_id;
            let marker = match marker {
                Ok(marker) => marker,
                Err(RepositoryIdentityResolutionError::Unavailable) => {
                    record_identity_unavailable(&mut candidate, &mut poison_events, &lookup);
                    first_error.get_or_insert(RepositoryControlError::IdentityUnavailable);
                    continue;
                }
            };
            let key =
                match register_resolved_alias(&mut candidate, lookup, marker, &mut poison_events) {
                    Ok(key) => key,
                    Err(error) => {
                        first_error.get_or_insert(error);
                        continue;
                    }
                };
            registered.push((repository_id, key));
        }

        if let Some(error) = first_error {
            apply_poison_events(&mut state, poison_events);
            return Err(error);
        }
        *state = candidate;
        registered.sort_unstable_by_key(|(repository_id, _)| repository_id.as_uuid());
        Ok(registered)
    }

    pub fn coordination_key(
        &self,
        repository_id: RepositoryId,
    ) -> Result<RepositoryCoordinationKey, RepositoryControlError> {
        lock_state(&self.inner.state)
            .repositories
            .get(&repository_id)
            .map(|alias| alias.key)
            .ok_or(RepositoryControlError::UnknownRepository)
    }

    pub fn control_state(
        &self,
        repository_id: RepositoryId,
    ) -> Result<RepositoryControlState, RepositoryControlError> {
        let state = lock_state(&self.inner.state);
        let key = state
            .repositories
            .get(&repository_id)
            .map(|alias| alias.key)
            .ok_or(RepositoryControlError::UnknownRepository)?;
        group_public_state(&state, key)
    }

    pub fn poison_reason(
        &self,
        repository_id: RepositoryId,
    ) -> Result<Option<RepositoryControlPoisonReason>, RepositoryControlError> {
        let state = lock_state(&self.inner.state);
        let key = state
            .repositories
            .get(&repository_id)
            .map(|alias| alias.key)
            .ok_or(RepositoryControlError::UnknownRepository)?;
        let group = state
            .groups
            .get(&key)
            .ok_or(RepositoryControlError::UnknownCoordinationKey)?;
        Ok(group.poison.map(|poison| poison.reason))
    }

    /// Ensures a coordination group is sticky-poisoned for evidence-based
    /// reconciliation without changing its current owner.
    ///
    /// Replaying an already-observed reason is idempotent. A newly observed
    /// reason advances the poison generation so any proof minted before that
    /// evidence fails closed. If no owner exists, callers may immediately use
    /// `try_acquire_reconciliation`; a live owner remains exclusive.
    pub fn require_reconciliation(
        &self,
        key: RepositoryCoordinationKey,
        reason: RepositoryControlPoisonReason,
    ) -> Result<(), RepositoryControlError> {
        let mut state = lock_state(&self.inner.state);
        let group = state
            .groups
            .get_mut(&key)
            .ok_or(RepositoryControlError::UnknownCoordinationKey)?;
        require_poison(group, reason);
        Ok(())
    }

    /// Re-resolves one registered durable seed outside the coordinator lock.
    ///
    /// A missing or changed object poisons the whole coordination group.
    pub fn revalidate_repository(
        &self,
        repository_id: RepositoryId,
        resolver: &dyn RepositoryIdentityResolver,
    ) -> Result<RepositoryCoordinationKey, RepositoryControlError> {
        let alias = {
            let state = lock_state(&self.inner.state);
            state
                .repositories
                .get(&repository_id)
                .cloned()
                .ok_or(RepositoryControlError::UnknownRepository)?
        };
        let marker = match resolver.resolve(&alias.identity) {
            Ok(marker) => marker,
            Err(RepositoryIdentityResolutionError::Unavailable) => {
                self.poison_key(
                    alias.key,
                    RepositoryControlPoisonReason::IdentityUnavailable,
                );
                return Err(RepositoryControlError::IdentityUnavailable);
            }
        };
        let observed = RepositoryCoordinationKey::from_authenticated_marker(marker);
        let mut state = lock_state(&self.inner.state);
        let still_registered = state
            .repositories
            .get(&repository_id)
            .is_some_and(|current| current.identity == alias.identity && current.key == alias.key);
        if !still_registered {
            poison_group(
                &mut state,
                alias.key,
                RepositoryControlPoisonReason::AliasConflict,
            );
            return Err(RepositoryControlError::AliasConflict);
        }
        if observed != alias.key {
            poison_group(
                &mut state,
                alias.key,
                RepositoryControlPoisonReason::IdentityDrift,
            );
            return Err(RepositoryControlError::IdentityDrift);
        }
        Ok(alias.key)
    }

    /// Scheduler-facing non-blocking acquisition.
    ///
    /// This method never waits for either the coordinator state mutex or an
    /// existing logical lease.
    pub fn try_acquire(
        &self,
        key: RepositoryCoordinationKey,
    ) -> Result<RepositoryControlLease, RepositoryControlError> {
        let mut state = match self.inner.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => return Err(RepositoryControlError::Busy),
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
        };
        let group = state
            .groups
            .get(&key)
            .ok_or(RepositoryControlError::UnknownCoordinationKey)?;
        if group.poison.is_some() {
            return Err(RepositoryControlError::Poisoned);
        }
        match group.ownership {
            CoordinationOwnership::Available => {}
            CoordinationOwnership::Held { .. } => {
                return Err(RepositoryControlError::Busy);
            }
        }
        let lease_id = allocate_lease_id(&mut state)?;
        state
            .groups
            .get_mut(&key)
            .expect("validated coordination group remains present")
            .ownership = CoordinationOwnership::Held {
            lease_id,
            kind: LeaseKind::Operation,
        };
        Ok(RepositoryControlLease {
            inner: Arc::clone(&self.inner),
            key,
            lease_id,
            kind: LeaseKind::Operation,
            poison_generation_at_acquire: None,
            recovery_lifecycle: Arc::new(RepositoryControlRecoveryLifecycle::new()),
            completed: false,
        })
    }

    pub(crate) fn try_acquire_with_recovery_witness(
        &self,
        key: RepositoryCoordinationKey,
    ) -> Result<(RepositoryControlLease, RepositoryControlRecoveryWitness), RepositoryControlError>
    {
        let lease = self.try_acquire(key)?;
        let witness = RepositoryControlRecoveryWitness::new(&lease);
        Ok((lease, witness))
    }

    /// Acquires exclusive ownership of a poisoned group for evidence-based
    /// reconciliation. It never silently turns a matching marker into a clean
    /// state.
    pub fn try_acquire_reconciliation(
        &self,
        key: RepositoryCoordinationKey,
    ) -> Result<RepositoryControlLease, RepositoryControlError> {
        let mut state = match self.inner.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => return Err(RepositoryControlError::Busy),
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
        };
        let group = state
            .groups
            .get(&key)
            .ok_or(RepositoryControlError::UnknownCoordinationKey)?;
        let poison = group.poison.ok_or(RepositoryControlError::NotPoisoned)?;
        if poison.generation_exhausted {
            return Err(RepositoryControlError::Poisoned);
        }
        if matches!(group.ownership, CoordinationOwnership::Held { .. }) {
            return Err(RepositoryControlError::Busy);
        }
        let lease_id = allocate_lease_id(&mut state)?;
        state
            .groups
            .get_mut(&key)
            .expect("validated coordination group remains present")
            .ownership = CoordinationOwnership::Held {
            lease_id,
            kind: LeaseKind::Reconciliation,
        };
        Ok(RepositoryControlLease {
            inner: Arc::clone(&self.inner),
            key,
            lease_id,
            kind: LeaseKind::Reconciliation,
            poison_generation_at_acquire: Some(poison.generation),
            recovery_lifecycle: Arc::new(RepositoryControlRecoveryLifecycle::new()),
            completed: false,
        })
    }

    fn poison_key(&self, key: RepositoryCoordinationKey, reason: RepositoryControlPoisonReason) {
        poison_group(&mut lock_state(&self.inner.state), key, reason);
    }
}

impl fmt::Debug for RepositoryControlCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = lock_state(&self.inner.state);
        formatter
            .debug_struct("RepositoryControlCoordinator")
            .field("repository_count", &state.repositories.len())
            .field("coordination_group_count", &state.groups.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseKind {
    Operation,
    Reconciliation,
}

/// Owned logical repository-control lease.
///
/// The guard intentionally does not wrap a mutex/semaphore guard: an
/// uncompleted Drop poisons the group and never makes it available.
#[must_use = "dropping an unfinished repository control lease poisons its alias group"]
pub struct RepositoryControlLease {
    inner: Arc<CoordinatorInner>,
    key: RepositoryCoordinationKey,
    lease_id: u64,
    kind: LeaseKind,
    poison_generation_at_acquire: Option<u64>,
    recovery_lifecycle: Arc<RepositoryControlRecoveryLifecycle>,
    completed: bool,
}

impl RepositoryControlLease {
    pub const fn coordination_key(&self) -> RepositoryCoordinationKey {
        self.key
    }

    pub fn clean_release(mut self) -> Result<(), RepositoryControlError> {
        if self.kind != LeaseKind::Operation {
            return Err(RepositoryControlError::InvalidReconciliationProof);
        }
        let poisoned = release_operation_lease(&self.inner, self.key, self.lease_id)?;
        self.recovery_lifecycle.mark_released();
        self.completed = true;
        if poisoned {
            Err(RepositoryControlError::Poisoned)
        } else {
            Ok(())
        }
    }

    pub fn poison(
        mut self,
        reason: RepositoryControlPoisonReason,
    ) -> Result<(), RepositoryControlError> {
        poison_owned_lease(&self.inner, self.key, self.lease_id, self.kind, reason)?;
        self.recovery_lifecycle.mark_released();
        self.completed = true;
        Ok(())
    }

    /// Consumes this guard after recording sticky poison while deliberately
    /// retaining its logical ownership for the rest of this process.
    ///
    /// This is the fail-closed terminal path when a child process may still be
    /// alive and no process-clean proof is available yet. It prevents `Drop`
    /// from releasing the owner, so neither a normal operation nor
    /// reconciliation can acquire the group. Task 15 may later replace this
    /// process-lifetime retention with an explicit process-proof handoff.
    pub fn retain_fail_closed(
        mut self,
        reason: RepositoryControlPoisonReason,
    ) -> Result<(), RepositoryControlError> {
        mark_owned_lease_poisoned(&self.inner, self.key, self.lease_id, self.kind, reason)?;
        self.recovery_lifecycle.mark_retained(reason);
        self.completed = true;
        Ok(())
    }

    /// Records sticky poison without relinquishing the current owner.
    ///
    /// This is the only safe first step when a Git child or durable write may
    /// have completed but its outcome is not yet known. Reconciliation remains
    /// `Busy` until this exact guard is either proven process-clean and
    /// promoted, or deliberately retained fail-closed.
    pub fn mark_poisoned(
        &mut self,
        reason: RepositoryControlPoisonReason,
    ) -> Result<(), RepositoryControlError> {
        mark_owned_lease_poisoned(&self.inner, self.key, self.lease_id, self.kind, reason)
    }

    /// Converts this still-owned operation lease into reconciliation ownership
    /// without an owner-free handoff window.
    ///
    /// Callers may only do this after proving any child process tree has
    /// stopped. The resulting guard still requires artifact-bound evidence
    /// before it can clear sticky poison.
    pub fn promote_to_reconciliation(&mut self) -> Result<(), RepositoryControlError> {
        if self.kind != LeaseKind::Operation {
            return Err(RepositoryControlError::InvalidReconciliationProof);
        }
        let generation = promote_operation_lease(&self.inner, self.key, self.lease_id)?;
        self.kind = LeaseKind::Reconciliation;
        self.poison_generation_at_acquire = Some(generation);
        Ok(())
    }

    /// Binds authoritative artifact evidence to this exact reconciliation
    /// owner and poison generation.
    ///
    /// `VerifiedArtifactReconciliationEvidence` can only be created by the
    /// artifact mutation adapters after an exact Store query. The caller must
    /// additionally provide the attempt identity and durable state expected by
    /// the preparation state machine. The repository identity is checked
    /// against this lease's coordination group before an opaque release proof
    /// is minted.
    pub fn verify_artifact_reconciliation(
        &self,
        expected_identity: AttemptArtifactIdentity,
        expected_state: AttemptArtifactState,
        evidence: &VerifiedArtifactReconciliationEvidence,
    ) -> Result<VerifiedRepositoryControlState, RepositoryControlError> {
        if evidence.identity() != expected_identity || evidence.state() != expected_state {
            return Err(RepositoryControlError::InvalidReconciliationProof);
        }
        let poison_generation =
            verify_current_reconciliation_owner(self, expected_identity.repository_id)?;
        Ok(VerifiedRepositoryControlState {
            coordinator: Arc::downgrade(&self.inner),
            key: self.key,
            lease_id: self.lease_id,
            poison_generation,
            artifact_identity: expected_identity,
            artifact_state: expected_state,
        })
    }

    pub fn clean_release_after_reconciliation(
        mut self,
        proof: VerifiedRepositoryControlState,
    ) -> Result<(), RepositoryControlError> {
        let same_coordinator = proof
            .coordinator
            .upgrade()
            .is_some_and(|inner| Arc::ptr_eq(&inner, &self.inner));
        if self.kind != LeaseKind::Reconciliation
            || !same_coordinator
            || proof.key != self.key
            || proof.lease_id != self.lease_id
            || Some(proof.poison_generation) != self.poison_generation_at_acquire
            || !repository_is_bound_to_key(
                &self.inner,
                proof.artifact_identity.repository_id,
                self.key,
            )
            || proof.artifact_state == AttemptArtifactState::Reserved
        {
            return Err(RepositoryControlError::InvalidReconciliationProof);
        }
        release_reconciliation_lease(
            &self.inner,
            self.key,
            self.lease_id,
            proof.poison_generation,
        )?;
        self.recovery_lifecycle.mark_released();
        self.completed = true;
        Ok(())
    }

    /// Clears current-generation poison but keeps this exact owner as an
    /// operation lease. This is used when a reservation write was reconciled
    /// before any Git side effect and preparation can safely continue without
    /// opening an ownership transfer window.
    pub fn resume_operation_after_reconciliation(
        &mut self,
        proof: VerifiedRepositoryControlState,
    ) -> Result<(), RepositoryControlError> {
        let same_coordinator = proof
            .coordinator
            .upgrade()
            .is_some_and(|inner| Arc::ptr_eq(&inner, &self.inner));
        if self.kind != LeaseKind::Reconciliation
            || !same_coordinator
            || proof.key != self.key
            || proof.lease_id != self.lease_id
            || Some(proof.poison_generation) != self.poison_generation_at_acquire
            || !repository_is_bound_to_key(
                &self.inner,
                proof.artifact_identity.repository_id,
                self.key,
            )
            || proof.artifact_state != AttemptArtifactState::Reserved
        {
            return Err(RepositoryControlError::InvalidReconciliationProof);
        }
        resume_operation_lease(
            &self.inner,
            self.key,
            self.lease_id,
            proof.poison_generation,
        )?;
        self.kind = LeaseKind::Operation;
        self.poison_generation_at_acquire = None;
        Ok(())
    }
}

impl fmt::Debug for RepositoryControlLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RepositoryControlLease")
            .field("coordination_key", &self.key)
            .field("ownership", &"<opaque>")
            .finish()
    }
}

impl Drop for RepositoryControlLease {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut state = lock_state(&self.inner.state);
        let Some(group) = state.groups.get_mut(&self.key) else {
            return;
        };
        if matches!(
            group.ownership,
            CoordinationOwnership::Held { lease_id, kind }
                if lease_id == self.lease_id && kind == self.kind
        ) {
            note_poison(group, RepositoryControlPoisonReason::AbnormalLeaseDrop);
            self.recovery_lifecycle.mark_abnormal_retained();
        }
    }
}

/// Opaque proof that Task 11's identity-bound reconciliation reached a
/// durable, verified-safe state while owning this exact reconciliation lease.
pub struct VerifiedRepositoryControlState {
    coordinator: Weak<CoordinatorInner>,
    key: RepositoryCoordinationKey,
    lease_id: u64,
    poison_generation: u64,
    artifact_identity: AttemptArtifactIdentity,
    artifact_state: AttemptArtifactState,
}

impl fmt::Debug for VerifiedRepositoryControlState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("VerifiedRepositoryControlState(<opaque>)")
    }
}

fn verify_current_reconciliation_owner(
    lease: &RepositoryControlLease,
    repository_id: RepositoryId,
) -> Result<u64, RepositoryControlError> {
    if lease.kind != LeaseKind::Reconciliation {
        return Err(RepositoryControlError::InvalidReconciliationProof);
    }
    let Some(expected_generation) = lease.poison_generation_at_acquire else {
        return Err(RepositoryControlError::InvalidReconciliationProof);
    };
    let state = lock_state(&lease.inner.state);
    let repository_matches = state
        .repositories
        .get(&repository_id)
        .is_some_and(|alias| alias.key == lease.key);
    let group = state
        .groups
        .get(&lease.key)
        .ok_or(RepositoryControlError::InvalidReconciliationProof)?;
    let owns_current = matches!(
        group.ownership,
        CoordinationOwnership::Held {
            lease_id,
            kind: LeaseKind::Reconciliation,
        } if lease_id == lease.lease_id
    );
    let generation_is_current = group.poison.is_some_and(|poison| {
        !poison.generation_exhausted && poison.generation == expected_generation
    });
    if !repository_matches || !owns_current || !generation_is_current {
        return Err(RepositoryControlError::InvalidReconciliationProof);
    }
    Ok(expected_generation)
}

fn repository_is_bound_to_key(
    inner: &CoordinatorInner,
    repository_id: RepositoryId,
    key: RepositoryCoordinationKey,
) -> bool {
    lock_state(&inner.state)
        .repositories
        .get(&repository_id)
        .is_some_and(|alias| alias.key == key)
}

fn release_operation_lease(
    inner: &CoordinatorInner,
    key: RepositoryCoordinationKey,
    lease_id: u64,
) -> Result<bool, RepositoryControlError> {
    let mut state = lock_state(&inner.state);
    let group = state
        .groups
        .get_mut(&key)
        .ok_or(RepositoryControlError::UnknownCoordinationKey)?;
    if !matches!(
        group.ownership,
        CoordinationOwnership::Held {
            lease_id: current,
            kind: LeaseKind::Operation,
        } if current == lease_id
    ) {
        return Err(RepositoryControlError::StaleLease);
    }
    group.ownership = CoordinationOwnership::Available;
    Ok(group.poison.is_some())
}

fn release_reconciliation_lease(
    inner: &CoordinatorInner,
    key: RepositoryCoordinationKey,
    lease_id: u64,
    poison_generation: u64,
) -> Result<(), RepositoryControlError> {
    let mut state = lock_state(&inner.state);
    let group = state
        .groups
        .get_mut(&key)
        .ok_or(RepositoryControlError::UnknownCoordinationKey)?;
    let owns_current = matches!(
        group.ownership,
        CoordinationOwnership::Held {
            lease_id: current,
            kind: LeaseKind::Reconciliation,
        } if current == lease_id
    );
    let proof_is_current = group.poison.is_some_and(|poison| {
        !poison.generation_exhausted && poison.generation == poison_generation
    });
    if !owns_current || !proof_is_current {
        return Err(RepositoryControlError::InvalidReconciliationProof);
    }
    group.ownership = CoordinationOwnership::Available;
    group.poison = None;
    Ok(())
}

fn resume_operation_lease(
    inner: &CoordinatorInner,
    key: RepositoryCoordinationKey,
    lease_id: u64,
    poison_generation: u64,
) -> Result<(), RepositoryControlError> {
    let mut state = lock_state(&inner.state);
    let group = state
        .groups
        .get_mut(&key)
        .ok_or(RepositoryControlError::UnknownCoordinationKey)?;
    let owns_current = matches!(
        group.ownership,
        CoordinationOwnership::Held {
            lease_id: current,
            kind: LeaseKind::Reconciliation,
        } if current == lease_id
    );
    let proof_is_current = group.poison.is_some_and(|poison| {
        !poison.generation_exhausted && poison.generation == poison_generation
    });
    if !owns_current || !proof_is_current {
        return Err(RepositoryControlError::InvalidReconciliationProof);
    }
    group.ownership = CoordinationOwnership::Held {
        lease_id,
        kind: LeaseKind::Operation,
    };
    group.poison = None;
    Ok(())
}

fn poison_owned_lease(
    inner: &CoordinatorInner,
    key: RepositoryCoordinationKey,
    lease_id: u64,
    kind: LeaseKind,
    reason: RepositoryControlPoisonReason,
) -> Result<(), RepositoryControlError> {
    let mut state = lock_state(&inner.state);
    let group = state
        .groups
        .get_mut(&key)
        .ok_or(RepositoryControlError::UnknownCoordinationKey)?;
    if !matches!(
        group.ownership,
        CoordinationOwnership::Held {
            lease_id: current,
            kind: current_kind,
        } if current == lease_id && current_kind == kind
    ) {
        return Err(RepositoryControlError::StaleLease);
    }
    note_poison(group, reason);
    group.ownership = CoordinationOwnership::Available;
    Ok(())
}

fn mark_owned_lease_poisoned(
    inner: &CoordinatorInner,
    key: RepositoryCoordinationKey,
    lease_id: u64,
    kind: LeaseKind,
    reason: RepositoryControlPoisonReason,
) -> Result<(), RepositoryControlError> {
    let mut state = lock_state(&inner.state);
    let group = state
        .groups
        .get_mut(&key)
        .ok_or(RepositoryControlError::UnknownCoordinationKey)?;
    if !matches!(
        group.ownership,
        CoordinationOwnership::Held {
            lease_id: current,
            kind: current_kind,
        } if current == lease_id && current_kind == kind
    ) {
        return Err(RepositoryControlError::StaleLease);
    }
    note_poison(group, reason);
    Ok(())
}

fn promote_operation_lease(
    inner: &CoordinatorInner,
    key: RepositoryCoordinationKey,
    lease_id: u64,
) -> Result<u64, RepositoryControlError> {
    let mut state = lock_state(&inner.state);
    let group = state
        .groups
        .get_mut(&key)
        .ok_or(RepositoryControlError::UnknownCoordinationKey)?;
    if !matches!(
        group.ownership,
        CoordinationOwnership::Held {
            lease_id: current,
            kind: LeaseKind::Operation,
        } if current == lease_id
    ) {
        return Err(RepositoryControlError::StaleLease);
    }
    let poison = group.poison.ok_or(RepositoryControlError::NotPoisoned)?;
    if poison.generation_exhausted {
        return Err(RepositoryControlError::Poisoned);
    }
    group.ownership = CoordinationOwnership::Held {
        lease_id,
        kind: LeaseKind::Reconciliation,
    };
    Ok(poison.generation)
}

fn group_public_state(
    state: &CoordinatorState,
    key: RepositoryCoordinationKey,
) -> Result<RepositoryControlState, RepositoryControlError> {
    let group = state
        .groups
        .get(&key)
        .ok_or(RepositoryControlError::UnknownCoordinationKey)?;
    if group.poison.is_some() {
        Ok(RepositoryControlState::Poisoned)
    } else if matches!(group.ownership, CoordinationOwnership::Held { .. }) {
        Ok(RepositoryControlState::Busy)
    } else {
        Ok(RepositoryControlState::Available)
    }
}

fn allocate_lease_id(state: &mut CoordinatorState) -> Result<u64, RepositoryControlError> {
    let lease_id = state.next_lease_id;
    state.next_lease_id = lease_id
        .checked_add(1)
        .ok_or(RepositoryControlError::LeaseSpaceExhausted)?;
    Ok(lease_id)
}

fn register_resolved_alias(
    state: &mut CoordinatorState,
    lookup: RepositoryIdentityLookup,
    marker: DirectoryIdentityMarker,
    poison_events: &mut Vec<(RepositoryCoordinationKey, RepositoryControlPoisonReason)>,
) -> Result<RepositoryCoordinationKey, RepositoryControlError> {
    let observed_key = RepositoryCoordinationKey::from_authenticated_marker(marker);
    let seed_key = state.seeds.get(&lookup.git_identity_key).copied();
    let existing = state.repositories.get(&lookup.repository_id).cloned();
    let mut implicated = HashSet::new();
    let mut first_error = None;

    if let Some(expected_key) = seed_key
        && expected_key != observed_key
    {
        record_poison_once(
            state,
            poison_events,
            &mut implicated,
            expected_key,
            RepositoryControlPoisonReason::IdentityDrift,
        );
        first_error.get_or_insert(RepositoryControlError::IdentityDrift);
    }

    if let Some(existing) = &existing
        && (existing.identity != lookup || existing.key != observed_key)
    {
        record_poison_once(
            state,
            poison_events,
            &mut implicated,
            existing.key,
            RepositoryControlPoisonReason::AliasConflict,
        );
        if state.groups.contains_key(&observed_key) {
            record_poison_once(
                state,
                poison_events,
                &mut implicated,
                observed_key,
                RepositoryControlPoisonReason::AliasConflict,
            );
        }
        first_error.get_or_insert(RepositoryControlError::AliasConflict);
    }
    if let Some(error) = first_error {
        return Err(error);
    }

    if let Some(existing) = existing {
        return Ok(existing.key);
    }

    state
        .seeds
        .entry(lookup.git_identity_key.clone())
        .or_insert(observed_key);
    let group = state
        .groups
        .entry(observed_key)
        .or_insert_with(|| CoordinationGroup {
            repositories: HashSet::new(),
            seeds: HashSet::new(),
            ownership: CoordinationOwnership::Available,
            poison: None,
        });
    group.repositories.insert(lookup.repository_id);
    group.seeds.insert(lookup.git_identity_key.clone());
    state.repositories.insert(
        lookup.repository_id,
        RepositoryAlias {
            identity: lookup,
            key: observed_key,
        },
    );
    Ok(observed_key)
}

fn record_poison(
    state: &mut CoordinatorState,
    poison_events: &mut Vec<(RepositoryCoordinationKey, RepositoryControlPoisonReason)>,
    key: RepositoryCoordinationKey,
    reason: RepositoryControlPoisonReason,
) {
    poison_events.push((key, reason));
    poison_group(state, key, reason);
}

fn record_identity_unavailable(
    state: &mut CoordinatorState,
    poison_events: &mut Vec<(RepositoryCoordinationKey, RepositoryControlPoisonReason)>,
    lookup: &RepositoryIdentityLookup,
) {
    let seed_key = state.seeds.get(&lookup.git_identity_key).copied();
    let repository_key = state
        .repositories
        .get(&lookup.repository_id)
        .map(|alias| alias.key);
    if let Some(key) = seed_key {
        record_poison(
            state,
            poison_events,
            key,
            RepositoryControlPoisonReason::IdentityUnavailable,
        );
    }
    if let Some(key) = repository_key
        && Some(key) != seed_key
    {
        record_poison(
            state,
            poison_events,
            key,
            RepositoryControlPoisonReason::IdentityUnavailable,
        );
    }
}

fn record_poison_once(
    state: &mut CoordinatorState,
    poison_events: &mut Vec<(RepositoryCoordinationKey, RepositoryControlPoisonReason)>,
    implicated: &mut HashSet<RepositoryCoordinationKey>,
    key: RepositoryCoordinationKey,
    reason: RepositoryControlPoisonReason,
) {
    if implicated.insert(key) {
        record_poison(state, poison_events, key, reason);
    }
}

fn apply_poison_events(
    state: &mut CoordinatorState,
    poison_events: Vec<(RepositoryCoordinationKey, RepositoryControlPoisonReason)>,
) {
    for (key, reason) in poison_events {
        poison_group(state, key, reason);
    }
}

fn poison_group(
    state: &mut CoordinatorState,
    key: RepositoryCoordinationKey,
    reason: RepositoryControlPoisonReason,
) {
    if let Some(group) = state.groups.get_mut(&key) {
        note_poison(group, reason);
    }
}

fn note_poison(group: &mut CoordinationGroup, reason: RepositoryControlPoisonReason) {
    match &mut group.poison {
        Some(poison) => {
            poison.observed_reasons |= poison_reason_bit(reason);
            if let Some(generation) = poison.generation.checked_add(1) {
                poison.generation = generation;
            } else {
                poison.generation_exhausted = true;
            }
        }
        None => {
            group.poison = Some(PoisonRecord {
                reason,
                generation: 1,
                generation_exhausted: false,
                observed_reasons: poison_reason_bit(reason),
            });
        }
    }
}

fn require_poison(group: &mut CoordinationGroup, reason: RepositoryControlPoisonReason) {
    if group
        .poison
        .is_some_and(|poison| poison.observed_reasons & poison_reason_bit(reason) != 0)
    {
        return;
    }
    note_poison(group, reason);
}

const fn poison_reason_bit(reason: RepositoryControlPoisonReason) -> u16 {
    match reason {
        RepositoryControlPoisonReason::AbnormalLeaseDrop => 1 << 0,
        RepositoryControlPoisonReason::GitChildOutcomeUnknown => 1 << 1,
        RepositoryControlPoisonReason::ReservationWriteFailed => 1 << 2,
        RepositoryControlPoisonReason::ReadyWriteFailed => 1 << 3,
        RepositoryControlPoisonReason::InconsistentWriteFailed => 1 << 4,
        RepositoryControlPoisonReason::IdentityUnavailable => 1 << 5,
        RepositoryControlPoisonReason::IdentityDrift => 1 << 6,
        RepositoryControlPoisonReason::SideEffectIdentityMismatch => 1 << 7,
        RepositoryControlPoisonReason::AliasConflict => 1 << 8,
    }
}

fn lock_state(state: &Mutex<CoordinatorState>) -> MutexGuard<'_, CoordinatorState> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
