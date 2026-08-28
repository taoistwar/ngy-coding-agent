use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use super::{
    CoordinationOwnership, CoordinatorInner, LeaseKind, RepositoryControlLease,
    RepositoryControlPoisonReason, lock_state, poison_reason_bit,
};

const LIVE: u8 = 0;
const CLEANUP_RETAINED: u8 = 1;
const ABNORMAL_RETAINED: u8 = 2;
const RECONCILIATION_RETAINED: u8 = 3;
const RELEASED: u8 = 4;

pub(super) struct RepositoryControlRecoveryLifecycle {
    state: AtomicU8,
}

impl RepositoryControlRecoveryLifecycle {
    pub(super) fn new() -> Self {
        Self {
            state: AtomicU8::new(LIVE),
        }
    }

    pub(super) fn mark_released(&self) {
        self.state.store(RELEASED, Ordering::Release);
    }

    pub(super) fn mark_retained(&self, reason: RepositoryControlPoisonReason) {
        let retained = match reason {
            RepositoryControlPoisonReason::GitChildOutcomeUnknown => CLEANUP_RETAINED,
            _ => RECONCILIATION_RETAINED,
        };
        self.state.store(retained, Ordering::Release);
    }

    pub(super) fn mark_abnormal_retained(&self) {
        self.state.store(ABNORMAL_RETAINED, Ordering::Release);
    }

    fn state(&self) -> RepositoryControlRecoveryState {
        match self.state.load(Ordering::Acquire) {
            LIVE => RepositoryControlRecoveryState::Live,
            CLEANUP_RETAINED => RepositoryControlRecoveryState::CleanupRetained,
            ABNORMAL_RETAINED => RepositoryControlRecoveryState::AbnormalRetained,
            RECONCILIATION_RETAINED => RepositoryControlRecoveryState::ReconciliationRetained,
            RELEASED => RepositoryControlRecoveryState::Released,
            _ => RepositoryControlRecoveryState::Invalid,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepositoryControlRecoveryState {
    Live,
    CleanupRetained,
    AbnormalRetained,
    ReconciliationRetained,
    Released,
    Invalid,
}

impl RepositoryControlRecoveryState {
    pub(crate) const fn pauses_for_process_cleanup(self) -> bool {
        matches!(
            self,
            Self::CleanupRetained | Self::AbnormalRetained | Self::ReconciliationRetained
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum RepositoryControlRecoveryError {
    #[error("the runner still owns its repository control lease")]
    LeaseStillLive,
    #[error("the repository control recovery lifecycle is invalid")]
    InvalidLifecycle,
    #[error("the retained repository control owner is no longer exact")]
    ExactOwnerMismatch,
    #[error("the retained repository control owner has no matching sticky poison")]
    PoisonMismatch,
}

/// Move-only authority retained by the TaskManager for one exact operation
/// lease. The shared lifecycle distinguishes a normal release from a retained
/// fail-closed owner even if the runner unwinds and drops the lease.
pub(crate) struct RepositoryControlRecoveryWitness {
    inner: Arc<CoordinatorInner>,
    key: crate::RepositoryCoordinationKey,
    lease_id: u64,
    lifecycle: Arc<RepositoryControlRecoveryLifecycle>,
}

impl RepositoryControlRecoveryWitness {
    pub(super) fn new(lease: &RepositoryControlLease) -> Self {
        Self {
            inner: Arc::clone(&lease.inner),
            key: lease.key,
            lease_id: lease.lease_id,
            lifecycle: Arc::clone(&lease.recovery_lifecycle),
        }
    }

    pub(crate) fn state(&self) -> RepositoryControlRecoveryState {
        self.lifecycle.state()
    }

    /// Settles the exact run owner only after TaskManager has separately minted
    /// its task-bound process-clean confirmation. Cleanup uncertainty becomes
    /// an unowned sticky poison eligible only for reconciliation. It never
    /// clears the poison record or its generation.
    pub(crate) fn settle_after_process_cleanup(self) -> Result<(), RepositoryControlRecoveryError> {
        match self.state() {
            RepositoryControlRecoveryState::Released => Ok(()),
            RepositoryControlRecoveryState::CleanupRetained => self.release_retained_owner(
                CLEANUP_RETAINED,
                Some(RepositoryControlPoisonReason::GitChildOutcomeUnknown),
            ),
            RepositoryControlRecoveryState::AbnormalRetained => self.release_retained_owner(
                ABNORMAL_RETAINED,
                Some(RepositoryControlPoisonReason::AbnormalLeaseDrop),
            ),
            RepositoryControlRecoveryState::ReconciliationRetained => {
                self.release_retained_owner(RECONCILIATION_RETAINED, None)
            }
            RepositoryControlRecoveryState::Live => {
                Err(RepositoryControlRecoveryError::LeaseStillLive)
            }
            RepositoryControlRecoveryState::Invalid => {
                Err(RepositoryControlRecoveryError::InvalidLifecycle)
            }
        }
    }

    fn release_retained_owner(
        self,
        expected_state: u8,
        expected_reason: Option<RepositoryControlPoisonReason>,
    ) -> Result<(), RepositoryControlRecoveryError> {
        let mut state = lock_state(&self.inner.state);
        let group = state
            .groups
            .get_mut(&self.key)
            .ok_or(RepositoryControlRecoveryError::ExactOwnerMismatch)?;
        if !matches!(
            group.ownership,
            CoordinationOwnership::Held {
                lease_id,
                kind: LeaseKind::Operation | LeaseKind::Reconciliation,
            } if lease_id == self.lease_id
        ) {
            return Err(RepositoryControlRecoveryError::ExactOwnerMismatch);
        }
        if !group.poison.is_some_and(|poison| {
            expected_reason
                .is_none_or(|reason| poison.observed_reasons & poison_reason_bit(reason) != 0)
        }) {
            return Err(RepositoryControlRecoveryError::PoisonMismatch);
        }
        if self.lifecycle.state.load(Ordering::Acquire) != expected_state {
            return Err(RepositoryControlRecoveryError::InvalidLifecycle);
        }
        group.ownership = CoordinationOwnership::Available;
        self.lifecycle.state.store(RELEASED, Ordering::Release);
        Ok(())
    }
}

impl std::fmt::Debug for RepositoryControlRecoveryWitness {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RepositoryControlRecoveryWitness")
            .field("coordination_key", &self.key)
            .field("state", &self.state())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use coding_agent_runtime::RootCapability;

    use super::*;
    use crate::repository_control::{CoordinationGroup, RepositoryControlCoordinator};

    fn fixture() -> (
        tempfile::TempDir,
        RepositoryControlCoordinator,
        crate::RepositoryCoordinationKey,
    ) {
        let directory = tempfile::tempdir().expect("create recovery witness identity");
        let marker = RootCapability::open(directory.path().canonicalize().unwrap())
            .expect("open recovery witness identity")
            .identity_marker()
            .expect("observe recovery witness identity");
        let key = crate::RepositoryCoordinationKey::from_authenticated_marker(marker);
        let coordinator = RepositoryControlCoordinator::new();
        let mut state = lock_state(&coordinator.inner.state);
        state.groups.insert(
            key,
            CoordinationGroup {
                repositories: HashSet::new(),
                seeds: HashSet::new(),
                ownership: CoordinationOwnership::Available,
                poison: None,
            },
        );
        drop(state);
        (directory, coordinator, key)
    }

    fn poison_generation(
        coordinator: &RepositoryControlCoordinator,
        key: crate::RepositoryCoordinationKey,
    ) -> u64 {
        lock_state(&coordinator.inner.state)
            .groups
            .get(&key)
            .and_then(|group| group.poison)
            .expect("the retained owner has sticky poison")
            .generation
    }

    #[test]
    fn cleanup_retention_releases_exact_owner_without_changing_sticky_poison() {
        let (_directory, coordinator, key) = fixture();
        let (lease, witness) = coordinator
            .try_acquire_with_recovery_witness(key)
            .expect("acquire exact recovery lease");
        lease
            .retain_fail_closed(RepositoryControlPoisonReason::GitChildOutcomeUnknown)
            .expect("retain unknown child owner");
        assert_eq!(
            witness.state(),
            RepositoryControlRecoveryState::CleanupRetained
        );
        let generation = poison_generation(&coordinator, key);

        witness
            .settle_after_process_cleanup()
            .expect("exact cleanup proof releases retained owner");

        assert_eq!(poison_generation(&coordinator, key), generation);
        let reconciliation = coordinator
            .try_acquire_reconciliation(key)
            .expect("sticky poison is now eligible for reconciliation");
        reconciliation
            .poison(RepositoryControlPoisonReason::GitChildOutcomeUnknown)
            .expect("release reconciliation while preserving poison");
    }

    #[test]
    fn clean_release_witness_never_disturbs_a_new_owner() {
        let (_directory, coordinator, key) = fixture();
        let (lease, witness) = coordinator
            .try_acquire_with_recovery_witness(key)
            .expect("acquire exact recovery lease");
        lease.clean_release().expect("release first owner");
        let next = coordinator.try_acquire(key).expect("acquire next owner");

        witness
            .settle_after_process_cleanup()
            .expect("released witness settles without consulting the current owner");

        assert!(matches!(
            coordinator.try_acquire(key),
            Err(super::super::RepositoryControlError::Busy)
        ));
        next.clean_release().expect("release next owner");
    }

    #[test]
    fn abnormal_drop_and_store_ambiguity_become_unowned_sticky_poison_after_proof() {
        let (_directory, coordinator, key) = fixture();
        let (lease, witness) = coordinator
            .try_acquire_with_recovery_witness(key)
            .expect("acquire abnormal-drop lease");
        drop(lease);
        assert_eq!(
            witness.state(),
            RepositoryControlRecoveryState::AbnormalRetained
        );
        witness
            .settle_after_process_cleanup()
            .expect("process proof releases abnormal drop owner");
        let reconciliation = coordinator
            .try_acquire_reconciliation(key)
            .expect("abnormal drop remains sticky but unowned");
        reconciliation
            .poison(RepositoryControlPoisonReason::AbnormalLeaseDrop)
            .expect("release abnormal-drop reconciliation owner");

        let (_directory, coordinator, key) = fixture();
        let (lease, witness) = coordinator
            .try_acquire_with_recovery_witness(key)
            .expect("acquire Store-ambiguity lease");
        lease
            .retain_fail_closed(RepositoryControlPoisonReason::ReservationWriteFailed)
            .expect("retain Store-ambiguity owner");
        assert_eq!(
            witness.state(),
            RepositoryControlRecoveryState::ReconciliationRetained
        );
        let generation = poison_generation(&coordinator, key);
        witness
            .settle_after_process_cleanup()
            .expect("process proof releases Store-ambiguity owner to reconciliation");
        assert_eq!(poison_generation(&coordinator, key), generation);
        let reconciliation = coordinator
            .try_acquire_reconciliation(key)
            .expect("Store ambiguity remains sticky but no longer has a live owner");
        reconciliation
            .poison(RepositoryControlPoisonReason::ReservationWriteFailed)
            .expect("release Store-ambiguity reconciliation owner");
    }
}
