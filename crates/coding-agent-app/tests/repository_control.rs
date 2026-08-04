#![cfg(feature = "test-support")]

mod support;

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::str::FromStr;
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use coding_agent_app::{
    LiveStoreWriterArtifactAdapter, PermitLedger, RepositoryControlCoordinator,
    RepositoryControlError, RepositoryControlPoisonReason, RepositoryControlState,
    RepositoryCoordinationKey, RepositoryIdentityResolutionError, RepositoryIdentityResolver,
    SchedulerConcurrencyLimits,
};
use coding_agent_domain::{CanonicalPath, RepositoryId, TaskId};
use coding_agent_runtime::{DirectoryIdentityMarker, RootCapability};
use coding_agent_store::{
    AttemptArtifactIdentity, AttemptArtifactState, RepositoryIdentityLookup, ReserveAttemptArtifact,
};

fn repository_id(suffix: u32) -> RepositoryId {
    RepositoryId::from_str(&format!("10000000-0000-4000-8000-{suffix:012x}"))
        .expect("canonical repository ID")
}

fn task_id(suffix: u32) -> TaskId {
    TaskId::from_str(&format!("20000000-0000-4000-8000-{suffix:012x}")).expect("canonical task ID")
}

fn marker(directory: &tempfile::TempDir) -> DirectoryIdentityMarker {
    RootCapability::open(directory.path())
        .expect("open authenticated identity fixture")
        .identity_marker()
        .expect("observe fixture identity")
}

fn lookup(repository: u32, seed: &str) -> RepositoryIdentityLookup {
    lookup_for(repository_id(repository), seed)
}

fn lookup_for(repository_id: RepositoryId, seed: &str) -> RepositoryIdentityLookup {
    let git_root = std::env::current_dir()
        .expect("resolve repository-control fixture root")
        .join("target")
        .join("repository-control-identities")
        .join(repository_id.to_string());
    RepositoryIdentityLookup {
        repository_id,
        git_root: CanonicalPath::try_from_canonical(git_root)
            .expect("construct repository-control fixture Git root"),
        git_identity_key: seed.to_owned(),
    }
}

#[derive(Default)]
struct FakeResolver {
    observations:
        Mutex<HashMap<String, Result<DirectoryIdentityMarker, RepositoryIdentityResolutionError>>>,
}

impl FakeResolver {
    fn set_marker(&self, seed: &str, marker: DirectoryIdentityMarker) {
        self.observations
            .lock()
            .expect("lock fake resolver")
            .insert(seed.to_owned(), Ok(marker));
    }

    fn set_unavailable(&self, seed: &str) {
        self.observations
            .lock()
            .expect("lock fake resolver")
            .insert(
                seed.to_owned(),
                Err(RepositoryIdentityResolutionError::Unavailable),
            );
    }
}

impl RepositoryIdentityResolver for FakeResolver {
    fn resolve(
        &self,
        identity: &RepositoryIdentityLookup,
    ) -> Result<DirectoryIdentityMarker, RepositoryIdentityResolutionError> {
        self.observations
            .lock()
            .expect("lock fake resolver")
            .get(&identity.git_identity_key)
            .copied()
            .unwrap_or(Err(RepositoryIdentityResolutionError::Unavailable))
    }
}

#[test]
fn unavailable_first_resolution_has_no_fallback_key_and_repository_rebinding_fails_closed() {
    let identity_a = tempfile::tempdir().expect("identity A");
    let identity_b = tempfile::tempdir().expect("identity B");
    let resolver = FakeResolver::default();
    resolver.set_unavailable("missing-seed");
    let coordinator = RepositoryControlCoordinator::new();

    assert_eq!(
        coordinator.register_alias(lookup(1, "missing-seed"), &resolver),
        Err(RepositoryControlError::IdentityUnavailable)
    );
    assert_eq!(
        coordinator.coordination_key(repository_id(1)),
        Err(RepositoryControlError::UnknownRepository),
        "an unavailable seed must not create a path-based placeholder key"
    );

    resolver.set_marker("seed-a", marker(&identity_a));
    resolver.set_marker("seed-b", marker(&identity_b));
    let original_key = coordinator
        .register_alias(lookup(1, "seed-a"), &resolver)
        .expect("register original repository identity");
    assert_eq!(
        coordinator
            .register_alias(lookup(1, "seed-a"), &resolver)
            .expect("same repository and seed are idempotent"),
        original_key
    );
    assert_eq!(
        coordinator.register_alias(lookup(1, "seed-b"), &resolver),
        Err(RepositoryControlError::AliasConflict)
    );
    assert_eq!(
        coordinator.coordination_key(repository_id(1)),
        Ok(original_key),
        "the durable mapping is never rebound to the replacement"
    );
    assert_eq!(
        coordinator.poison_reason(repository_id(1)),
        Ok(Some(RepositoryControlPoisonReason::AliasConflict))
    );
}

#[test]
fn authenticated_dynamic_alias_registration_is_idempotent_and_never_rebinds() {
    let identity_a = tempfile::tempdir().expect("identity A");
    let identity_b = tempfile::tempdir().expect("identity B");
    let coordinator = RepositoryControlCoordinator::new();
    let durable_lookup = lookup(77, "durable-seed");
    let key = coordinator
        .register_authenticated_alias(durable_lookup.clone(), marker(&identity_a))
        .expect("register exact retained capability");

    assert_eq!(
        coordinator.register_authenticated_alias(durable_lookup.clone(), marker(&identity_a)),
        Ok(key),
        "an apply-before-reply retry is idempotent"
    );
    assert_eq!(
        coordinator.register_authenticated_alias(durable_lookup, marker(&identity_b)),
        Err(RepositoryControlError::IdentityDrift),
        "the same durable seed resolving to a replacement object is identity drift"
    );
    assert_eq!(
        coordinator.coordination_key(repository_id(77)),
        Ok(key),
        "the original authenticated mapping remains installed"
    );
    assert_eq!(
        coordinator.poison_reason(repository_id(77)),
        Ok(Some(RepositoryControlPoisonReason::IdentityDrift)),
        "the contradictory authenticated observation remains sticky"
    );
    assert_eq!(
        coordinator.try_acquire(key).unwrap_err(),
        RepositoryControlError::Poisoned,
        "the retained original mapping remains fail-closed after drift"
    );
}

#[test]
fn unavailable_dynamic_alias_observation_poisons_seed_group_without_creating_alias() {
    let identity = tempfile::tempdir().expect("identity");
    let coordinator = RepositoryControlCoordinator::new();
    let established = lookup(77, "shared-durable-seed");
    let key = coordinator
        .register_authenticated_alias(established, marker(&identity))
        .expect("register established seed group");
    let unavailable_alias = lookup(88, "shared-durable-seed");

    coordinator.observe_identity_unavailable(&unavailable_alias);

    assert_eq!(
        coordinator.coordination_key(repository_id(88)),
        Err(RepositoryControlError::UnknownRepository),
        "an unavailable observation never guesses or creates an alias"
    );
    assert_eq!(
        coordinator.coordination_key(repository_id(77)),
        Ok(key),
        "the established seed mapping is not rebound"
    );
    assert_eq!(
        coordinator.poison_reason(repository_id(77)),
        Ok(Some(RepositoryControlPoisonReason::IdentityUnavailable)),
        "the already known seed group fails closed"
    );
}

#[test]
fn unavailable_conflicting_alias_poisons_both_existing_identity_groups() {
    let identity_a = tempfile::tempdir().expect("identity A");
    let identity_b = tempfile::tempdir().expect("identity B");
    let resolver = FakeResolver::default();
    resolver.set_marker("seed-a", marker(&identity_a));
    resolver.set_marker("seed-b", marker(&identity_b));
    let coordinator = RepositoryControlCoordinator::new();
    let key_a = coordinator
        .register_alias(lookup(1, "seed-a"), &resolver)
        .expect("register repository A");
    let key_b = coordinator
        .register_alias(lookup(2, "seed-b"), &resolver)
        .expect("register repository B");

    resolver.set_unavailable("seed-b");
    assert_eq!(
        coordinator.register_alias(lookup(1, "seed-b"), &resolver),
        Err(RepositoryControlError::IdentityUnavailable)
    );
    assert_eq!(
        coordinator.coordination_key(repository_id(1)),
        Ok(key_a),
        "the unavailable rebinding never replaces the durable repository key"
    );
    assert_eq!(coordinator.coordination_key(repository_id(2)), Ok(key_b));
    for repository in [repository_id(1), repository_id(2)] {
        assert_eq!(
            coordinator.poison_reason(repository),
            Ok(Some(RepositoryControlPoisonReason::IdentityUnavailable)),
            "both sides of an unobservable alias contradiction fail closed"
        );
    }
}

#[test]
fn resolved_conflicting_alias_poisons_seed_repository_and_observed_groups() {
    let identity_a = tempfile::tempdir().expect("identity A");
    let identity_b = tempfile::tempdir().expect("identity B");
    let identity_c = tempfile::tempdir().expect("identity C");
    let resolver = FakeResolver::default();
    resolver.set_marker("seed-a", marker(&identity_a));
    resolver.set_marker("seed-b", marker(&identity_b));
    resolver.set_marker("seed-c", marker(&identity_c));
    let coordinator = RepositoryControlCoordinator::new();
    let key_a = coordinator
        .register_alias(lookup(1, "seed-a"), &resolver)
        .expect("register repository A");
    let key_b = coordinator
        .register_alias(lookup(2, "seed-b"), &resolver)
        .expect("register repository B");
    let key_c = coordinator
        .register_alias(lookup(3, "seed-c"), &resolver)
        .expect("register repository C");

    resolver.set_marker("seed-b", marker(&identity_c));
    assert_eq!(
        coordinator.register_alias(lookup(1, "seed-b"), &resolver),
        Err(RepositoryControlError::IdentityDrift),
        "the first input-order contradiction determines the typed error"
    );
    assert_eq!(
        coordinator.coordination_key(repository_id(1)),
        Ok(key_a),
        "repository A is never rebound"
    );
    assert_eq!(coordinator.coordination_key(repository_id(2)), Ok(key_b));
    assert_eq!(coordinator.coordination_key(repository_id(3)), Ok(key_c));
    assert_eq!(
        coordinator.poison_reason(repository_id(1)),
        Ok(Some(RepositoryControlPoisonReason::AliasConflict))
    );
    assert_eq!(
        coordinator.poison_reason(repository_id(2)),
        Ok(Some(RepositoryControlPoisonReason::IdentityDrift))
    );
    assert_eq!(
        coordinator.poison_reason(repository_id(3)),
        Ok(Some(RepositoryControlPoisonReason::AliasConflict))
    );
}

#[test]
fn aliases_share_one_key_and_lease_while_distinct_markers_progress_independently() {
    let identity_a = tempfile::tempdir().expect("identity A");
    let identity_b = tempfile::tempdir().expect("identity B");
    let resolver = FakeResolver::default();
    resolver.set_marker("seed-a-1", marker(&identity_a));
    resolver.set_marker("seed-a-2", marker(&identity_a));
    resolver.set_marker("seed-b", marker(&identity_b));
    let coordinator = RepositoryControlCoordinator::new();

    let registered = coordinator
        .register_aliases(
            [
                lookup(3, "seed-b"),
                lookup(4, "seed-a-1"),
                lookup(2, "seed-a-2"),
                lookup(1, "seed-a-1"),
            ],
            &resolver,
        )
        .expect("register aliases");
    assert_eq!(
        registered
            .iter()
            .map(|(repository, _)| *repository)
            .collect::<Vec<_>>(),
        vec![
            repository_id(1),
            repository_id(2),
            repository_id(3),
            repository_id(4)
        ]
    );
    let key_a = coordinator
        .coordination_key(repository_id(1))
        .expect("key A1");
    assert_eq!(
        key_a,
        coordinator
            .coordination_key(repository_id(2))
            .expect("key A2")
    );
    assert_eq!(
        key_a,
        coordinator
            .coordination_key(repository_id(4))
            .expect("same durable seed alias")
    );
    let key_b = coordinator
        .coordination_key(repository_id(3))
        .expect("key B");
    assert_ne!(key_a, key_b);

    let lease_a = coordinator.try_acquire(key_a).expect("acquire A");
    assert_eq!(
        coordinator.try_acquire(key_a).unwrap_err(),
        RepositoryControlError::Busy
    );
    assert_eq!(
        coordinator.try_acquire_reconciliation(key_a).unwrap_err(),
        RepositoryControlError::NotPoisoned
    );
    let lease_b = coordinator
        .try_acquire(key_b)
        .expect("different identity advances independently");
    assert_eq!(
        coordinator.control_state(repository_id(1)),
        Ok(RepositoryControlState::Busy)
    );
    assert_eq!(
        coordinator.control_state(repository_id(2)),
        Ok(RepositoryControlState::Busy)
    );

    lease_a.clean_release().expect("clean release A");
    lease_b.clean_release().expect("clean release B");
    assert_eq!(
        coordinator.control_state(repository_id(1)),
        Ok(RepositoryControlState::Available)
    );

    let rendered = format!("{coordinator:?}");
    assert!(!rendered.contains("seed-a"));
    let debug_lease = coordinator.try_acquire(key_a).expect("debug lease");
    assert!(!format!("{debug_lease:?}").contains("seed-a"));
    debug_lease.clean_release().expect("release debug lease");
}

#[test]
fn concurrent_alias_install_converges_and_same_key_try_acquire_has_one_winner() {
    let identity = tempfile::tempdir().expect("shared identity");
    let resolver = Arc::new(FakeResolver::default());
    resolver.set_marker("seed-a", marker(&identity));
    resolver.set_marker("seed-b", marker(&identity));
    let coordinator = Arc::new(RepositoryControlCoordinator::new());

    let registrations = (1..=8)
        .map(|suffix| {
            let coordinator = Arc::clone(&coordinator);
            let resolver = Arc::clone(&resolver);
            std::thread::spawn(move || {
                let seed = if suffix % 2 == 0 { "seed-a" } else { "seed-b" };
                coordinator.register_alias(lookup(suffix, seed), resolver.as_ref())
            })
        })
        .collect::<Vec<_>>();
    let keys = registrations
        .into_iter()
        .map(|registration| registration.join().expect("registration thread").unwrap())
        .collect::<Vec<_>>();
    assert!(
        keys.iter().all(|key| *key == keys[0]),
        "concurrent aliases for one marker must converge on one key"
    );

    let start = Arc::new(Barrier::new(9));
    let finish = Arc::new(Barrier::new(9));
    let attempts = (0..8)
        .map(|_| {
            let coordinator = Arc::clone(&coordinator);
            let start = Arc::clone(&start);
            let finish = Arc::clone(&finish);
            let key = keys[0];
            std::thread::spawn(move || {
                start.wait();
                let result = coordinator.try_acquire(key);
                finish.wait();
                match result {
                    Ok(lease) => {
                        lease.clean_release().expect("winner clean release");
                        true
                    }
                    Err(RepositoryControlError::Busy) => false,
                    Err(error) => panic!("unexpected concurrent acquisition result: {error:?}"),
                }
            })
        })
        .collect::<Vec<_>>();
    start.wait();
    finish.wait();
    let winners = attempts
        .into_iter()
        .map(|attempt| attempt.join().expect("acquisition thread"))
        .filter(|won| *won)
        .count();
    assert_eq!(winners, 1);
}

#[test]
fn conflicting_concurrent_aliases_never_rebind_one_repository() {
    let identity_a = tempfile::tempdir().expect("identity A");
    let identity_b = tempfile::tempdir().expect("identity B");
    let resolver = Arc::new(FakeResolver::default());
    resolver.set_marker("seed-a", marker(&identity_a));
    resolver.set_marker("seed-b", marker(&identity_b));
    let coordinator = Arc::new(RepositoryControlCoordinator::new());
    let start = Arc::new(Barrier::new(3));

    let attempts = ["seed-a", "seed-b"]
        .into_iter()
        .map(|seed| {
            let coordinator = Arc::clone(&coordinator);
            let resolver = Arc::clone(&resolver);
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                coordinator.register_alias(lookup(1, seed), resolver.as_ref())
            })
        })
        .collect::<Vec<_>>();
    start.wait();
    let outcomes = attempts
        .into_iter()
        .map(|attempt| attempt.join().expect("alias registration thread"))
        .collect::<Vec<_>>();
    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == Err(RepositoryControlError::AliasConflict))
            .count(),
        1
    );

    let durable_key = coordinator
        .coordination_key(repository_id(1))
        .expect("one durable mapping wins");
    assert!(
        outcomes.contains(&Ok(durable_key)),
        "the losing registration cannot replace the winning mapping"
    );
    assert_eq!(
        coordinator.control_state(repository_id(1)),
        Ok(RepositoryControlState::Poisoned),
        "the contradictory observation remains fail-closed"
    );
}

#[test]
fn failed_alias_batch_rolls_back_new_aliases_but_keeps_security_poison() {
    let identity_a = tempfile::tempdir().expect("identity A");
    let identity_b = tempfile::tempdir().expect("identity B");
    let identity_c = tempfile::tempdir().expect("identity C");
    let resolver = FakeResolver::default();
    resolver.set_marker("seed-existing-a", marker(&identity_a));
    resolver.set_marker("seed-existing-b", marker(&identity_b));
    resolver.set_marker("seed-new", marker(&identity_c));
    let coordinator = RepositoryControlCoordinator::new();
    let existing_key_a = coordinator
        .register_alias(lookup(1, "seed-existing-a"), &resolver)
        .expect("register pre-existing alias A");
    coordinator
        .register_alias(lookup(9, "seed-existing-b"), &resolver)
        .expect("register pre-existing alias B");

    resolver.set_unavailable("seed-unknown");
    resolver.set_marker("seed-existing-a", marker(&identity_c));
    resolver.set_unavailable("seed-existing-b");
    assert_eq!(
        coordinator.register_aliases(
            [
                lookup(2, "seed-unknown"),
                lookup(3, "seed-new"),
                lookup(4, "seed-existing-a"),
                lookup(5, "seed-existing-b"),
            ],
            &resolver,
        ),
        Err(RepositoryControlError::IdentityUnavailable)
    );
    for repository in 2..=5 {
        assert_eq!(
            coordinator.coordination_key(repository_id(repository)),
            Err(RepositoryControlError::UnknownRepository),
            "no item in a failed batch is partially committed"
        );
    }
    assert_eq!(
        coordinator.poison_reason(repository_id(1)),
        Ok(Some(RepositoryControlPoisonReason::IdentityDrift)),
        "known drift after the first batch error is still retained"
    );
    assert_eq!(
        coordinator.poison_reason(repository_id(9)),
        Ok(Some(RepositoryControlPoisonReason::IdentityUnavailable)),
        "known unavailability after the first batch error is still retained"
    );

    resolver.set_marker("seed-new", marker(&identity_a));
    assert_eq!(
        coordinator
            .register_alias(lookup(3, "seed-new"), &resolver)
            .expect("failed batch left no seed mapping behind"),
        existing_key_a
    );
}

#[test]
fn drift_or_unavailable_identity_poisons_every_alias_and_never_auto_recovers() {
    let original = tempfile::tempdir().expect("original identity");
    let replacement = tempfile::tempdir().expect("replacement identity");
    let resolver = FakeResolver::default();
    resolver.set_marker("seed-a", marker(&original));
    resolver.set_marker("seed-alias", marker(&original));
    let coordinator = RepositoryControlCoordinator::new();
    coordinator
        .register_aliases([lookup(1, "seed-a"), lookup(2, "seed-alias")], &resolver)
        .expect("register alias group");
    let key = coordinator
        .coordination_key(repository_id(1))
        .expect("coordination key");
    let held = coordinator.try_acquire(key).expect("hold operation lease");

    resolver.set_marker("seed-a", marker(&replacement));
    assert_eq!(
        coordinator.revalidate_repository(repository_id(1), &resolver),
        Err(RepositoryControlError::IdentityDrift)
    );
    for repository in [repository_id(1), repository_id(2)] {
        assert_eq!(
            coordinator.control_state(repository),
            Ok(RepositoryControlState::Poisoned)
        );
        assert_eq!(
            coordinator.poison_reason(repository),
            Ok(Some(RepositoryControlPoisonReason::IdentityDrift))
        );
    }
    assert_eq!(
        coordinator.try_acquire_reconciliation(key).unwrap_err(),
        RepositoryControlError::Busy,
        "external poison cannot erase a live operation owner"
    );
    assert_eq!(
        held.clean_release(),
        Err(RepositoryControlError::Poisoned),
        "an in-flight holder cannot clean-release after external drift"
    );
    assert_eq!(
        coordinator
            .register_alias(lookup(3, "seed-alias"), &resolver)
            .expect("a later alias still resolves to the original marker"),
        key
    );
    assert_eq!(
        coordinator.control_state(repository_id(3)),
        Ok(RepositoryControlState::Poisoned),
        "a later alias cannot bypass sticky group poison"
    );
    resolver.set_unavailable("seed-alias");
    assert_eq!(
        coordinator.revalidate_repository(repository_id(2), &resolver),
        Err(RepositoryControlError::IdentityUnavailable)
    );
    assert_eq!(
        coordinator.poison_reason(repository_id(1)),
        Ok(Some(RepositoryControlPoisonReason::IdentityDrift)),
        "later failures cannot overwrite the first poison reason"
    );
    resolver.set_marker("seed-alias", marker(&original));

    resolver.set_marker("seed-a", marker(&original));
    assert_eq!(
        coordinator.revalidate_repository(repository_id(1), &resolver),
        Ok(key),
        "identity can be observed again"
    );
    assert_eq!(
        coordinator.try_acquire(key).unwrap_err(),
        RepositoryControlError::Poisoned,
        "matching identity alone must not clear poison"
    );

    coordinator
        .try_acquire_reconciliation(key)
        .expect("first reconciliation lease")
        .poison(RepositoryControlPoisonReason::IdentityUnavailable)
        .expect("explicitly finish failed reconciliation while retaining poison");
    assert_eq!(
        coordinator.poison_reason(repository_id(1)),
        Ok(Some(RepositoryControlPoisonReason::IdentityDrift)),
        "abandoning reconciliation preserves the original sticky reason"
    );
    let reconciliation = coordinator
        .try_acquire_reconciliation(key)
        .expect("own poisoned group for evidence-based reconciliation");
    assert_eq!(
        coordinator.poison_reason(repository_id(1)),
        Ok(Some(RepositoryControlPoisonReason::IdentityDrift)),
        "reconciliation ownership never hides the sticky first reason"
    );
    assert_eq!(
        coordinator.try_acquire_reconciliation(key).unwrap_err(),
        RepositoryControlError::Busy
    );
    reconciliation
        .poison(RepositoryControlPoisonReason::IdentityUnavailable)
        .expect("failed reconciliation keeps the group poisoned");
    assert_eq!(
        coordinator.control_state(repository_id(2)),
        Ok(RepositoryControlState::Poisoned)
    );
    assert_eq!(
        coordinator.poison_reason(repository_id(1)),
        Ok(Some(RepositoryControlPoisonReason::IdentityDrift)),
        "failed reconciliation cannot overwrite or clear the first sticky reason"
    );
}

#[tokio::test]
async fn authoritative_artifact_evidence_is_bound_to_exact_identity_state_and_current_lease() {
    let fixture = support::writer_fixture().await;
    let task = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "artifact-bound proof"),
            support::deadline(),
        )
        .await
        .expect("create proof task")
        .value
        .task()
        .clone();
    let identity = AttemptArtifactIdentity {
        task_id: task.id,
        repository_id: fixture.repository.id,
        attempt: task.attempt,
    };
    let reservation = ReserveAttemptArtifact {
        identity,
        base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
        branch_name: format!("codex/artifact-proof-{}", task.id),
        worktree_path: CanonicalPath::try_from_canonical(
            fixture
                .repository
                .git_root
                .as_path()
                .join("artifact-proof")
                .join(task.id.to_string()),
        )
        .expect("construct canonical proof path"),
    };
    fixture
        .store
        .reserve_attempt_artifact(reservation)
        .await
        .expect("seed reserved artifact");
    fixture
        .store
        .mark_attempt_artifact_ready(identity)
        .await
        .expect("seed ready artifact");
    let adapter =
        LiveStoreWriterArtifactAdapter::new(fixture.store.clone(), fixture.writer.clone());
    let disposition = adapter
        .mark_attempt_artifact_ready(identity, tokio::time::Instant::now())
        .await;
    let evidence = disposition
        .reconciliation_evidence()
        .expect("expired writer request is reconciled by an exact Store query");

    let marker_directory = tempfile::tempdir().expect("coordinator marker");
    let resolver = FakeResolver::default();
    resolver.set_marker("artifact-seed", marker(&marker_directory));
    let coordinator = RepositoryControlCoordinator::new();
    let key = coordinator
        .register_alias(
            lookup_for(fixture.repository.id, "artifact-seed"),
            &resolver,
        )
        .expect("register artifact repository");
    coordinator
        .try_acquire(key)
        .expect("acquire operation")
        .poison(RepositoryControlPoisonReason::ReadyWriteFailed)
        .expect("poison ambiguous ready write");
    let reconciliation = coordinator
        .try_acquire_reconciliation(key)
        .expect("acquire reconciliation");

    let wrong_identity = AttemptArtifactIdentity {
        task_id: task_id(999),
        ..identity
    };
    assert_eq!(
        reconciliation
            .verify_artifact_reconciliation(wrong_identity, AttemptArtifactState::Ready, evidence)
            .unwrap_err(),
        RepositoryControlError::InvalidReconciliationProof
    );
    assert_eq!(
        reconciliation
            .verify_artifact_reconciliation(identity, AttemptArtifactState::Reserved, evidence)
            .unwrap_err(),
        RepositoryControlError::InvalidReconciliationProof
    );

    let proof = reconciliation
        .verify_artifact_reconciliation(identity, AttemptArtifactState::Ready, evidence)
        .expect("exact authoritative evidence mints a current-lease proof");
    coordinator
        .require_reconciliation(key, RepositoryControlPoisonReason::IdentityDrift)
        .expect("new evidence advances the poison generation");
    assert_eq!(
        reconciliation.clean_release_after_reconciliation(proof),
        Err(RepositoryControlError::InvalidReconciliationProof),
        "authoritative evidence predating a new poison generation is stale"
    );
    assert_eq!(
        coordinator.try_acquire_reconciliation(key).unwrap_err(),
        RepositoryControlError::Busy,
        "a rejected proof cannot abandon its logical owner"
    );

    let other_marker_directory = tempfile::tempdir().expect("other coordinator marker");
    resolver.set_marker("artifact-other-seed", marker(&other_marker_directory));
    let key_bound_coordinator = RepositoryControlCoordinator::new();
    let artifact_key = key_bound_coordinator
        .register_alias(
            lookup_for(fixture.repository.id, "artifact-seed"),
            &resolver,
        )
        .expect("register artifact key");
    let other_key = key_bound_coordinator
        .register_alias(lookup(2, "artifact-other-seed"), &resolver)
        .expect("register distinct key");
    for owned_key in [artifact_key, other_key] {
        key_bound_coordinator
            .try_acquire(owned_key)
            .expect("acquire key-bound operation")
            .poison(RepositoryControlPoisonReason::ReadyWriteFailed)
            .expect("poison key-bound operation");
    }
    let artifact_reconciliation = key_bound_coordinator
        .try_acquire_reconciliation(artifact_key)
        .expect("acquire artifact-key reconciliation");
    let artifact_proof = artifact_reconciliation
        .verify_artifact_reconciliation(identity, AttemptArtifactState::Ready, evidence)
        .expect("mint proof bound to artifact key");
    let other_reconciliation = key_bound_coordinator
        .try_acquire_reconciliation(other_key)
        .expect("acquire other-key reconciliation");
    assert_eq!(
        other_reconciliation.clean_release_after_reconciliation(artifact_proof),
        Err(RepositoryControlError::InvalidReconciliationProof),
        "a proof cannot cross coordination keys"
    );
    drop(artifact_reconciliation);

    let coordinator_a = RepositoryControlCoordinator::new();
    let coordinator_b = RepositoryControlCoordinator::new();
    let key_a = coordinator_a
        .register_alias(
            lookup_for(fixture.repository.id, "artifact-seed"),
            &resolver,
        )
        .expect("register coordinator A");
    let key_b = coordinator_b
        .register_alias(
            lookup_for(fixture.repository.id, "artifact-seed"),
            &resolver,
        )
        .expect("register coordinator B");
    for (owner, owned_key) in [(&coordinator_a, key_a), (&coordinator_b, key_b)] {
        owner
            .try_acquire(owned_key)
            .expect("acquire coordinator-bound operation")
            .poison(RepositoryControlPoisonReason::ReadyWriteFailed)
            .expect("poison coordinator-bound operation");
    }
    let reconciliation_a = coordinator_a
        .try_acquire_reconciliation(key_a)
        .expect("acquire coordinator A reconciliation");
    let foreign_proof = reconciliation_a
        .verify_artifact_reconciliation(identity, AttemptArtifactState::Ready, evidence)
        .expect("mint coordinator A proof");
    let reconciliation_b = coordinator_b
        .try_acquire_reconciliation(key_b)
        .expect("acquire coordinator B reconciliation");
    assert_eq!(
        reconciliation_b.clean_release_after_reconciliation(foreign_proof),
        Err(RepositoryControlError::InvalidReconciliationProof),
        "a proof cannot cross coordinator instances"
    );
    drop(reconciliation_a);

    let success_coordinator = RepositoryControlCoordinator::new();
    let success_key = success_coordinator
        .register_alias(
            lookup_for(fixture.repository.id, "artifact-seed"),
            &resolver,
        )
        .expect("register successful proof repository");
    success_coordinator
        .try_acquire(success_key)
        .expect("acquire successful operation")
        .poison(RepositoryControlPoisonReason::ReadyWriteFailed)
        .expect("poison successful operation");
    let success_reconciliation = success_coordinator
        .try_acquire_reconciliation(success_key)
        .expect("acquire successful reconciliation");
    let success_proof = success_reconciliation
        .verify_artifact_reconciliation(identity, AttemptArtifactState::Ready, evidence)
        .expect("mint exact current proof");
    success_reconciliation
        .clean_release_after_reconciliation(success_proof)
        .expect("artifact-bound proof clears current poison");
    assert_eq!(
        success_coordinator.control_state(fixture.repository.id),
        Ok(RepositoryControlState::Available)
    );
}

#[test]
fn poisoned_owner_promotion_has_no_handoff_window_and_can_remain_fail_closed() {
    let identity = tempfile::tempdir().expect("identity");
    let resolver = FakeResolver::default();
    resolver.set_marker("seed", marker(&identity));
    let coordinator = RepositoryControlCoordinator::new();
    let key = coordinator
        .register_alias(lookup(1, "seed"), &resolver)
        .expect("register identity");
    let mut lease = coordinator.try_acquire(key).expect("acquire operation");

    lease
        .mark_poisoned(RepositoryControlPoisonReason::GitChildOutcomeUnknown)
        .expect("record unknown child outcome without releasing owner");
    assert_eq!(
        coordinator.try_acquire(key).unwrap_err(),
        RepositoryControlError::Poisoned
    );
    assert_eq!(
        coordinator.try_acquire_reconciliation(key).unwrap_err(),
        RepositoryControlError::Busy,
        "sticky poison must not erase a potentially live child owner"
    );

    lease
        .promote_to_reconciliation()
        .expect("process-clean owner changes kind in place");
    assert_eq!(
        coordinator.try_acquire_reconciliation(key).unwrap_err(),
        RepositoryControlError::Busy,
        "promotion has no owner-free transfer window"
    );
    lease
        .retain_fail_closed(RepositoryControlPoisonReason::GitChildOutcomeUnknown)
        .expect("missing process-clean proof retains the promoted owner");
    assert_eq!(
        coordinator.try_acquire_reconciliation(key).unwrap_err(),
        RepositoryControlError::Busy,
        "fail-closed completion never creates an ownership handoff window"
    );
}

#[test]
fn retain_fail_closed_keeps_poisoned_owner_busy_without_a_forgotten_guard() {
    let identity = tempfile::tempdir().expect("identity");
    let resolver = FakeResolver::default();
    resolver.set_marker("seed", marker(&identity));
    let coordinator = RepositoryControlCoordinator::new();
    let key = coordinator
        .register_alias(lookup(1, "seed"), &resolver)
        .expect("register identity");

    coordinator
        .try_acquire(key)
        .expect("acquire operation")
        .retain_fail_closed(RepositoryControlPoisonReason::GitChildOutcomeUnknown)
        .expect("retain unknown child ownership fail closed");
    assert_eq!(
        coordinator.poison_reason(repository_id(1)),
        Ok(Some(RepositoryControlPoisonReason::GitChildOutcomeUnknown))
    );
    assert_eq!(
        coordinator.try_acquire(key).unwrap_err(),
        RepositoryControlError::Poisoned
    );
    assert_eq!(
        coordinator.try_acquire_reconciliation(key).unwrap_err(),
        RepositoryControlError::Busy,
        "retained logical ownership blocks reconciliation"
    );
    std::thread::sleep(Duration::from_millis(5));
    assert_eq!(
        coordinator.try_acquire_reconciliation(key).unwrap_err(),
        RepositoryControlError::Busy,
        "elapsed time cannot release a retained fail-closed owner"
    );
}

#[test]
fn require_reconciliation_is_idempotent_and_makes_an_unowned_group_acquirable() {
    let identity = tempfile::tempdir().expect("identity");
    let resolver = FakeResolver::default();
    resolver.set_marker("seed", marker(&identity));
    let coordinator = RepositoryControlCoordinator::new();
    let key = coordinator
        .register_alias(lookup(1, "seed"), &resolver)
        .expect("register identity");

    coordinator
        .require_reconciliation(key, RepositoryControlPoisonReason::ReservationWriteFailed)
        .expect("require startup reconciliation");
    coordinator
        .require_reconciliation(key, RepositoryControlPoisonReason::ReservationWriteFailed)
        .expect("replay identical requirement");
    let reconciliation = coordinator
        .try_acquire_reconciliation(key)
        .expect("unowned poisoned group is available for reconciliation");
    coordinator
        .require_reconciliation(key, RepositoryControlPoisonReason::ReservationWriteFailed)
        .expect("identical in-flight requirement remains idempotent");
    reconciliation
        .poison(RepositoryControlPoisonReason::ReservationWriteFailed)
        .expect("failed reconciliation releases ownership but preserves poison");
    assert_eq!(
        coordinator.control_state(repository_id(1)),
        Ok(RepositoryControlState::Poisoned)
    );
}

#[test]
fn require_reconciliation_never_transfers_an_existing_operation_owner() {
    let identity = tempfile::tempdir().expect("identity");
    let resolver = FakeResolver::default();
    resolver.set_marker("seed", marker(&identity));
    let coordinator = RepositoryControlCoordinator::new();
    let key = coordinator
        .register_alias(lookup(1, "seed"), &resolver)
        .expect("register identity");
    let held = coordinator.try_acquire(key).expect("hold operation owner");

    coordinator
        .require_reconciliation(key, RepositoryControlPoisonReason::GitChildOutcomeUnknown)
        .expect("record required reconciliation");
    assert_eq!(
        coordinator.try_acquire(key).unwrap_err(),
        RepositoryControlError::Poisoned
    );
    assert_eq!(
        coordinator.try_acquire_reconciliation(key).unwrap_err(),
        RepositoryControlError::Busy,
        "required reconciliation cannot erase a live operation owner"
    );
    assert_eq!(
        held.clean_release(),
        Err(RepositoryControlError::Poisoned),
        "externally poisoned owner cannot claim a clean release"
    );
    let reconciliation = coordinator
        .try_acquire_reconciliation(key)
        .expect("reconciliation becomes acquirable only after owner completion");
    reconciliation
        .poison(RepositoryControlPoisonReason::GitChildOutcomeUnknown)
        .expect("unverified reconciliation remains fail closed");
    assert_eq!(
        coordinator.control_state(repository_id(1)),
        Ok(RepositoryControlState::Poisoned)
    );
}

#[test]
fn reconciliation_cannot_use_normal_release_or_abandon_its_owner() {
    let identity = tempfile::tempdir().expect("identity");
    let resolver = FakeResolver::default();
    resolver.set_marker("seed", marker(&identity));
    let coordinator = RepositoryControlCoordinator::new();
    let key = coordinator
        .register_alias(lookup(1, "seed"), &resolver)
        .expect("register identity");
    coordinator
        .try_acquire(key)
        .expect("acquire operation")
        .poison(RepositoryControlPoisonReason::ReadyWriteFailed)
        .expect("poison operation");
    let reconciliation = coordinator
        .try_acquire_reconciliation(key)
        .expect("acquire reconciliation");
    assert_eq!(
        reconciliation.clean_release(),
        Err(RepositoryControlError::InvalidReconciliationProof)
    );
    assert_eq!(
        coordinator.poison_reason(repository_id(1)),
        Ok(Some(RepositoryControlPoisonReason::ReadyWriteFailed))
    );
    assert_eq!(
        coordinator.try_acquire_reconciliation(key).unwrap_err(),
        RepositoryControlError::Busy,
        "invalid completion cannot transfer a consumed reconciliation owner"
    );
}

#[test]
fn every_ambiguous_completion_and_abnormal_drop_remains_fail_closed() {
    let identity = tempfile::tempdir().expect("identity");
    let resolver = FakeResolver::default();
    resolver.set_marker("seed", marker(&identity));

    for reason in [
        RepositoryControlPoisonReason::GitChildOutcomeUnknown,
        RepositoryControlPoisonReason::ReservationWriteFailed,
        RepositoryControlPoisonReason::ReadyWriteFailed,
        RepositoryControlPoisonReason::InconsistentWriteFailed,
        RepositoryControlPoisonReason::IdentityDrift,
        RepositoryControlPoisonReason::SideEffectIdentityMismatch,
    ] {
        let coordinator = RepositoryControlCoordinator::new();
        let key = coordinator
            .register_alias(lookup(1, "seed"), &resolver)
            .expect("register identity");
        coordinator
            .try_acquire(key)
            .expect("acquire operation")
            .poison(reason)
            .expect("explicit poison");
        assert_eq!(
            coordinator.poison_reason(repository_id(1)),
            Ok(Some(reason))
        );
        let reconciliation = coordinator
            .try_acquire_reconciliation(key)
            .expect("acquire reconciliation");
        reconciliation
            .poison(reason)
            .expect("unverified reconciliation preserves fail-closed state");
        assert_eq!(
            coordinator.control_state(repository_id(1)),
            Ok(RepositoryControlState::Poisoned)
        );
    }

    let coordinator = RepositoryControlCoordinator::new();
    let key = coordinator
        .register_alias(lookup(1, "seed"), &resolver)
        .expect("register abandoned identity");
    drop(coordinator.try_acquire(key).expect("acquire then abandon"));
    assert_eq!(
        coordinator.poison_reason(repository_id(1)),
        Ok(Some(RepositoryControlPoisonReason::AbnormalLeaseDrop))
    );
    assert_eq!(
        coordinator.try_acquire(key).unwrap_err(),
        RepositoryControlError::Poisoned
    );
    assert_eq!(
        coordinator.try_acquire_reconciliation(key).unwrap_err(),
        RepositoryControlError::Busy,
        "abnormal drop never transfers an owner without process-clean proof"
    );

    let panic_coordinator = RepositoryControlCoordinator::new();
    let panic_key = panic_coordinator
        .register_alias(lookup(2, "seed"), &resolver)
        .expect("register panic identity");
    let panic_lease = panic_coordinator
        .try_acquire(panic_key)
        .expect("panic-owned lease");
    let panic = catch_unwind(AssertUnwindSafe(move || {
        let _owned = panic_lease;
        panic!("simulated repository-control owner panic");
    }));
    assert!(panic.is_err());
    assert_eq!(
        panic_coordinator.poison_reason(repository_id(2)),
        Ok(Some(RepositoryControlPoisonReason::AbnormalLeaseDrop))
    );
    assert_eq!(
        panic_coordinator
            .try_acquire_reconciliation(panic_key)
            .unwrap_err(),
        RepositoryControlError::Busy,
        "panic cannot make a possibly live Git-child owner transferable"
    );
}

#[test]
fn acquisition_is_nonblocking_has_no_forced_timeout_and_busy_callers_release_provisional_permits() {
    let identity = tempfile::tempdir().expect("identity");
    let resolver = FakeResolver::default();
    resolver.set_marker("seed", marker(&identity));
    let coordinator = RepositoryControlCoordinator::new();
    let key = coordinator
        .register_alias(lookup(1, "seed"), &resolver)
        .expect("register identity");
    let held = coordinator.try_acquire(key).expect("acquire lease");

    let started = Instant::now();
    assert_eq!(
        coordinator.try_acquire(key).unwrap_err(),
        RepositoryControlError::Busy
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "try_acquire must not await the existing lease"
    );
    std::thread::sleep(Duration::from_millis(5));
    assert_eq!(
        coordinator.try_acquire(key).unwrap_err(),
        RepositoryControlError::Busy,
        "elapsed wall time cannot force-transfer a lease"
    );

    let ledger = PermitLedger::new(SchedulerConcurrencyLimits::try_new(1, 1).unwrap());
    let provisional = ledger
        .reserve(task_id(1), key)
        .expect("reserve provisional permits");
    assert_eq!(
        coordinator.try_acquire(key).unwrap_err(),
        RepositoryControlError::Busy
    );
    ledger
        .release_unsubmitted(&provisional)
        .expect("caller immediately releases provisional permits");
    assert_eq!(ledger.snapshot().global_owned(), 0);

    held.clean_release().expect("clean release");
}

struct ReentrantResolver {
    coordinator: RepositoryControlCoordinator,
    unrelated_key: RepositoryCoordinationKey,
    marker: DirectoryIdentityMarker,
    callbacks: Arc<Mutex<usize>>,
}

impl RepositoryIdentityResolver for ReentrantResolver {
    fn resolve(
        &self,
        _identity: &RepositoryIdentityLookup,
    ) -> Result<DirectoryIdentityMarker, RepositoryIdentityResolutionError> {
        assert_eq!(
            self.coordinator
                .try_acquire(self.unrelated_key)
                .unwrap_err(),
            RepositoryControlError::UnknownCoordinationKey,
            "resolver callback observed the coordinator lock held"
        );
        *self.callbacks.lock().expect("lock callback count") += 1;
        Ok(self.marker)
    }
}

#[test]
fn resolver_callbacks_never_run_while_the_coordinator_lock_is_held() {
    let identity = tempfile::tempdir().expect("identity");
    let unrelated = tempfile::tempdir().expect("unrelated identity");
    let coordinator = RepositoryControlCoordinator::new();
    let callbacks = Arc::new(Mutex::new(0));
    let resolver = ReentrantResolver {
        coordinator: coordinator.clone(),
        unrelated_key: RepositoryCoordinationKey::from_authenticated_marker(marker(&unrelated)),
        marker: marker(&identity),
        callbacks: Arc::clone(&callbacks),
    };

    coordinator
        .register_alias(lookup(1, "seed"), &resolver)
        .expect("register without lock inversion");
    coordinator
        .revalidate_repository(repository_id(1), &resolver)
        .expect("revalidate without lock inversion");
    assert_eq!(*callbacks.lock().expect("lock callback count"), 2);
}
