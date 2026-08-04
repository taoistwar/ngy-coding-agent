#![cfg(feature = "test-support")]

mod support;

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coding_agent_app::{
    ArtifactMutationDisposition, ArtifactReconciliationDecision, ArtifactReconciliationError,
    AttemptArtifactObserver, LiveStoreWriterArtifactAdapter, RepositoryControlCoordinator,
    RepositoryControlError, RepositoryControlPoisonReason, RepositoryControlState,
    RepositoryIdentityResolutionError, RepositoryIdentityResolver, RestartArtifactObservation,
    StartupDirectStoreArtifactAdapter, StoreWriterHandle, WorktreeArtifactObserver,
    decide_restart_artifact, reconcile_restart_artifacts, reconcile_startup_artifacts_grouped,
};
use coding_agent_domain::{CanonicalPath, NewRepository, Repository, TaskId};
use coding_agent_runtime::{
    DirectoryIdentityMarker, ProcessLimits, RootCapability, WorktreeIdentity, WorktreeLimits,
    WorktreeProvisioner, discover_toolchain,
};
use coding_agent_store::{
    AttemptArtifactIdentity, AttemptArtifactState, RegisterRepositoryOutcome,
    RepositoryIdentityLookup, ReserveAttemptArtifact, ReserveAttemptArtifactOutcome, Store,
    TaskAttemptArtifact,
};
use tokio::sync::Barrier;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

struct ScriptedObserver {
    states: HashMap<String, RestartArtifactObservation>,
}

#[async_trait::async_trait]
impl AttemptArtifactObserver for ScriptedObserver {
    async fn observe(&self, artifact: &TaskAttemptArtifact) -> RestartArtifactObservation {
        self.states[&artifact.branch_name]
    }
}

struct DelayedObserver {
    delay: Duration,
    observation: RestartArtifactObservation,
}

#[async_trait::async_trait]
impl AttemptArtifactObserver for DelayedObserver {
    async fn observe(&self, _: &TaskAttemptArtifact) -> RestartArtifactObservation {
        tokio::time::sleep(self.delay).await;
        self.observation
    }
}

struct ConcurrencyObserver {
    active: AtomicUsize,
    max_active: AtomicUsize,
    calls: Mutex<Vec<TaskId>>,
    first_two_rendezvous: Option<Arc<Barrier>>,
    delay: Duration,
}

impl ConcurrencyObserver {
    fn new(first_two_rendezvous: Option<Arc<Barrier>>) -> Self {
        Self {
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            calls: Mutex::new(Vec::new()),
            first_two_rendezvous,
            delay: Duration::from_millis(30),
        }
    }

    fn max_active(&self) -> usize {
        self.max_active.load(Ordering::SeqCst)
    }

    fn calls(&self) -> Vec<TaskId> {
        self.calls.lock().expect("lock observer calls").clone()
    }
}

#[async_trait::async_trait]
impl AttemptArtifactObserver for ConcurrencyObserver {
    async fn observe(&self, artifact: &TaskAttemptArtifact) -> RestartArtifactObservation {
        let call_index = {
            let mut calls = self.calls.lock().expect("lock observer calls");
            let call_index = calls.len();
            calls.push(artifact.identity.task_id);
            call_index
        };
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        record_maximum(&self.max_active, active);
        if call_index < 2
            && let Some(rendezvous) = &self.first_two_rendezvous
        {
            rendezvous.wait().await;
        }
        tokio::time::sleep(self.delay).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        RestartArtifactObservation::Ready
    }
}

#[derive(Default)]
struct FreshInvocationObserver {
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl AttemptArtifactObserver for FreshInvocationObserver {
    async fn observe(&self, _: &TaskAttemptArtifact) -> RestartArtifactObservation {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            RestartArtifactObservation::Unavailable
        } else {
            RestartArtifactObservation::Ready
        }
    }
}

struct DriftingIdentityResolver {
    stable: DirectoryIdentityMarker,
    drifted: DirectoryIdentityMarker,
    calls: AtomicUsize,
}

impl RepositoryIdentityResolver for DriftingIdentityResolver {
    fn resolve(
        &self,
        _: &RepositoryIdentityLookup,
    ) -> Result<DirectoryIdentityMarker, RepositoryIdentityResolutionError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) < 2 {
            Ok(self.stable)
        } else {
            Ok(self.drifted)
        }
    }
}

fn record_maximum(maximum: &AtomicUsize, candidate: usize) {
    let mut current = maximum.load(Ordering::SeqCst);
    while candidate > current {
        match maximum.compare_exchange(current, candidate, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

#[test]
fn restart_observation_decision_table_is_fixed() {
    let cases = [
        (
            RestartArtifactObservation::Absent,
            ArtifactReconciliationDecision::Inconsistent("WORKTREE_RESERVATION_ABANDONED"),
        ),
        (
            RestartArtifactObservation::Ready,
            ArtifactReconciliationDecision::Ready,
        ),
        (
            RestartArtifactObservation::Partial,
            ArtifactReconciliationDecision::Inconsistent("WORKTREE_STATE_INCONSISTENT"),
        ),
        (
            RestartArtifactObservation::Inconsistent,
            ArtifactReconciliationDecision::Inconsistent("WORKTREE_STATE_INCONSISTENT"),
        ),
        (
            RestartArtifactObservation::Unavailable,
            ArtifactReconciliationDecision::RetainReserved,
        ),
        (
            RestartArtifactObservation::ProcessCleanupUnproven,
            ArtifactReconciliationDecision::RetainReserved,
        ),
        (
            RestartArtifactObservation::RepositoryMismatch,
            ArtifactReconciliationDecision::Inconsistent("WORKTREE_STATE_INCONSISTENT"),
        ),
    ];

    for (observation, expected) in cases {
        assert_eq!(decide_restart_artifact(observation), expected);
    }
}

#[tokio::test]
async fn grouped_startup_serializes_aliases_by_coordination_key_and_artifact_order() {
    let fixture = support::writer_fixture().await;
    let alias =
        register_alias_repository(&fixture.store, &fixture.repository, "startup-alias").await;
    create_reserved_artifact(&fixture, &fixture.repository, "startup-alias-first").await;
    create_reserved_artifact(&fixture, &alias, "startup-alias-second").await;
    let expected_order = fixture
        .store
        .list_reserved_attempt_artifacts()
        .await
        .unwrap()
        .into_iter()
        .map(|artifact| artifact.identity.task_id)
        .collect::<Vec<_>>();
    let (coordinator, resolver) = support::repository_control_fixture(&fixture.store).await;
    assert_eq!(
        coordinator.coordination_key(fixture.repository.id).unwrap(),
        coordinator.coordination_key(alias.id).unwrap()
    );
    let observer = ConcurrencyObserver::new(None);
    let adapter = StartupDirectStoreArtifactAdapter::new(fixture.store.clone());

    let summary = tokio::time::timeout(
        Duration::from_secs(2),
        reconcile_startup_artifacts_grouped(
            &adapter,
            coordinator.as_ref(),
            resolver.as_ref(),
            &observer,
            NonZeroUsize::new(2).unwrap(),
        ),
    )
    .await
    .expect("same-key startup reconciliation must make progress")
    .unwrap();

    assert_eq!(summary.examined, 2);
    assert_eq!(summary.marked_ready, 2);
    assert_eq!(observer.max_active(), 1);
    assert_eq!(observer.calls(), expected_order);
    assert_eq!(
        coordinator.control_state(fixture.repository.id).unwrap(),
        RepositoryControlState::Available
    );
    assert_eq!(
        coordinator.control_state(alias.id).unwrap(),
        RepositoryControlState::Available
    );
}

#[tokio::test]
async fn grouped_startup_overlaps_distinct_keys_without_exceeding_bound() {
    let fixture = support::writer_fixture().await;
    let second =
        register_distinct_repository(&fixture.store, &fixture.repository, "startup-second").await;
    let third =
        register_distinct_repository(&fixture.store, &fixture.repository, "startup-third").await;
    create_reserved_artifact(&fixture, &fixture.repository, "startup-first").await;
    create_reserved_artifact(&fixture, &second, "startup-second").await;
    create_reserved_artifact(&fixture, &third, "startup-third").await;
    let (coordinator, resolver) = support::repository_control_fixture(&fixture.store).await;
    let observer = ConcurrencyObserver::new(Some(Arc::new(Barrier::new(2))));
    let adapter = StartupDirectStoreArtifactAdapter::new(fixture.store.clone());

    let summary = tokio::time::timeout(
        Duration::from_secs(2),
        reconcile_startup_artifacts_grouped(
            &adapter,
            coordinator.as_ref(),
            resolver.as_ref(),
            &observer,
            NonZeroUsize::new(2).unwrap(),
        ),
    )
    .await
    .expect("two independent startup groups must overlap")
    .unwrap();

    assert_eq!(summary.examined, 3);
    assert_eq!(summary.marked_ready, 3);
    assert_eq!(observer.calls().len(), 3);
    assert_eq!(observer.max_active(), 2);
}

#[tokio::test]
async fn grouped_startup_invokes_a_fresh_observation_on_each_attempt() {
    let fixture = support::writer_fixture().await;
    let reservation =
        create_reserved_artifact(&fixture, &fixture.repository, "startup-fresh").await;
    let (coordinator, resolver) = support::repository_control_fixture(&fixture.store).await;
    let observer = FreshInvocationObserver::default();
    let adapter = StartupDirectStoreArtifactAdapter::new(fixture.store.clone());

    let first = reconcile_startup_artifacts_grouped(
        &adapter,
        coordinator.as_ref(),
        resolver.as_ref(),
        &observer,
        NonZeroUsize::new(1).unwrap(),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        first,
        ArtifactReconciliationError::ObservationUnavailable { identity }
            if identity == reservation.identity
    ));
    assert_eq!(
        fixture
            .store
            .load_attempt_artifact(reservation.identity.task_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        AttemptArtifactState::Reserved
    );

    let second = reconcile_startup_artifacts_grouped(
        &adapter,
        coordinator.as_ref(),
        resolver.as_ref(),
        &observer,
        NonZeroUsize::new(1).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(second.examined, 1);
    assert_eq!(second.marked_ready, 1);
    assert_eq!(observer.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        fixture
            .store
            .load_attempt_artifact(reservation.identity.task_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        AttemptArtifactState::Ready
    );
}

#[tokio::test]
async fn grouped_startup_unavailable_preserves_reserved_without_cleanup_or_repair() {
    let fixture = support::writer_fixture().await;
    let reservation =
        create_reserved_artifact(&fixture, &fixture.repository, "startup-unavailable").await;
    assert!(!reservation.worktree_path.as_path().exists());
    let (coordinator, resolver) = support::repository_control_fixture(&fixture.store).await;
    let adapter = StartupDirectStoreArtifactAdapter::new(fixture.store.clone());
    let error = reconcile_startup_artifacts_grouped(
        &adapter,
        coordinator.as_ref(),
        resolver.as_ref(),
        &ScriptedObserver {
            states: HashMap::from([(
                reservation.branch_name.clone(),
                RestartArtifactObservation::Unavailable,
            )]),
        },
        NonZeroUsize::new(2).unwrap(),
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        ArtifactReconciliationError::ObservationUnavailable { identity }
            if identity == reservation.identity
    ));
    let retained = fixture
        .store
        .load_attempt_artifact(reservation.identity.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.state, AttemptArtifactState::Reserved);
    assert!(retained.failure_code.is_none());
    assert!(!reservation.worktree_path.as_path().exists());
    assert_eq!(
        coordinator.control_state(fixture.repository.id).unwrap(),
        RepositoryControlState::Poisoned
    );
}

#[tokio::test]
async fn grouped_startup_unknown_observation_process_retains_owner_and_reserved_artifact() {
    let fixture = support::writer_fixture().await;
    let reservation =
        create_reserved_artifact(&fixture, &fixture.repository, "startup-process-unknown").await;
    let (coordinator, resolver) = support::repository_control_fixture(&fixture.store).await;
    let key = coordinator
        .coordination_key(fixture.repository.id)
        .expect("registered coordination key");
    let adapter = StartupDirectStoreArtifactAdapter::new(fixture.store.clone());

    let error = reconcile_startup_artifacts_grouped(
        &adapter,
        coordinator.as_ref(),
        resolver.as_ref(),
        &ScriptedObserver {
            states: HashMap::from([(
                reservation.branch_name.clone(),
                RestartArtifactObservation::ProcessCleanupUnproven,
            )]),
        },
        NonZeroUsize::new(1).unwrap(),
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        ArtifactReconciliationError::ObservationProcessCleanupUnproven { identity }
            if identity == reservation.identity
    ));
    let retained = fixture
        .store
        .load_attempt_artifact(reservation.identity.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.state, AttemptArtifactState::Reserved);
    assert!(retained.failure_code.is_none());
    assert_eq!(
        coordinator.try_acquire_reconciliation(key).unwrap_err(),
        RepositoryControlError::Busy,
        "unknown observation child keeps the original reconciliation owner"
    );
}

#[tokio::test]
async fn grouped_startup_repository_mismatch_is_inconsistent_and_sticky_poisoned() {
    let fixture = support::writer_fixture().await;
    let reservation =
        create_reserved_artifact(&fixture, &fixture.repository, "startup-repository-mismatch")
            .await;
    let (coordinator, resolver) = support::repository_control_fixture(&fixture.store).await;
    let adapter = StartupDirectStoreArtifactAdapter::new(fixture.store.clone());

    let error = reconcile_startup_artifacts_grouped(
        &adapter,
        coordinator.as_ref(),
        resolver.as_ref(),
        &ScriptedObserver {
            states: HashMap::from([(
                reservation.branch_name.clone(),
                RestartArtifactObservation::RepositoryMismatch,
            )]),
        },
        NonZeroUsize::new(1).unwrap(),
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        ArtifactReconciliationError::ObservationRepositoryMismatch { identity }
            if identity == reservation.identity
    ));
    let artifact = fixture
        .store
        .load_attempt_artifact(reservation.identity.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(artifact.state, AttemptArtifactState::Inconsistent);
    assert_eq!(
        artifact.failure_code.as_deref(),
        Some("WORKTREE_STATE_INCONSISTENT")
    );
    assert_eq!(
        coordinator.control_state(fixture.repository.id).unwrap(),
        RepositoryControlState::Poisoned
    );
}

#[tokio::test]
async fn grouped_startup_identity_drift_is_durable_inconsistent_and_keeps_poison() {
    let fixture = support::writer_fixture().await;
    let reservation =
        create_reserved_artifact(&fixture, &fixture.repository, "startup-identity-drift").await;
    let stable_directory = tempfile::tempdir().unwrap();
    let drifted_directory = tempfile::tempdir().unwrap();
    let resolver = DriftingIdentityResolver {
        stable: RootCapability::open(stable_directory.path())
            .unwrap()
            .identity_marker()
            .unwrap(),
        drifted: RootCapability::open(drifted_directory.path())
            .unwrap()
            .identity_marker()
            .unwrap(),
        calls: AtomicUsize::new(0),
    };
    let coordinator = RepositoryControlCoordinator::new();
    coordinator
        .register_aliases(
            fixture
                .store
                .list_repository_identity_lookups()
                .await
                .unwrap(),
            &resolver,
        )
        .unwrap();
    let adapter = StartupDirectStoreArtifactAdapter::new(fixture.store.clone());

    let error = reconcile_startup_artifacts_grouped(
        &adapter,
        &coordinator,
        &resolver,
        &ScriptedObserver {
            states: HashMap::from([(
                reservation.branch_name.clone(),
                RestartArtifactObservation::Ready,
            )]),
        },
        NonZeroUsize::new(1).unwrap(),
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        ArtifactReconciliationError::RepositoryControl(RepositoryControlError::IdentityDrift)
    ));
    assert_eq!(resolver.calls.load(Ordering::SeqCst), 3);
    let retained = fixture
        .store
        .load_attempt_artifact(reservation.identity.task_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.state, AttemptArtifactState::Inconsistent);
    assert_eq!(
        retained.failure_code.as_deref(),
        Some("WORKTREE_STATE_INCONSISTENT")
    );
    assert_eq!(
        coordinator.poison_reason(fixture.repository.id).unwrap(),
        Some(RepositoryControlPoisonReason::AbnormalLeaseDrop)
    );
    assert_eq!(
        coordinator.control_state(fixture.repository.id).unwrap(),
        RepositoryControlState::Poisoned
    );
}

#[tokio::test]
async fn unavailable_observation_retains_reserved_artifact() {
    let fixture = support::writer_fixture().await;
    let task = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "unavailable"),
            support::deadline(),
        )
        .await
        .unwrap()
        .value
        .task()
        .clone();
    let input = reservation(&fixture.repository, &task, "unavailable");
    fixture
        .writer
        .reserve_attempt_artifact(input.clone(), support::deadline())
        .await
        .unwrap();

    let error = reconcile_restart_artifacts(
        &fixture.store,
        &fixture.writer,
        &ScriptedObserver {
            states: HashMap::from([(
                input.branch_name.clone(),
                RestartArtifactObservation::Unavailable,
            )]),
        },
        Duration::from_secs(2),
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error,
        ArtifactReconciliationError::ObservationUnavailable { identity }
            if identity == input.identity
    ));
    let artifact = fixture
        .store
        .load_attempt_artifact(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(artifact.state, AttemptArtifactState::Reserved);
    assert!(artifact.failure_code.is_none());
}

#[tokio::test]
async fn startup_direct_adapter_mutates_store_without_a_writer_handle() {
    let fixture = support::writer_fixture().await;
    let task = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "startup direct"),
            support::deadline(),
        )
        .await
        .unwrap()
        .value
        .task()
        .clone();
    let input = reservation(&fixture.repository, &task, "startup-direct");
    let adapter = StartupDirectStoreArtifactAdapter::new(fixture.store.clone());

    assert!(matches!(
        adapter.reserve_attempt_artifact(input.clone()).await,
        ArtifactMutationDisposition::Confirmed(ref artifact)
            if artifact.identity == input.identity
                && artifact.state == AttemptArtifactState::Reserved
    ));
    assert!(matches!(
        adapter.mark_attempt_artifact_ready(input.identity).await,
        ArtifactMutationDisposition::Confirmed(ref artifact)
            if artifact.identity == input.identity
                && artifact.state == AttemptArtifactState::Ready
    ));
}

#[tokio::test]
async fn live_adapter_reconciles_writer_error_only_after_exact_store_query() {
    let fixture = support::writer_fixture().await;
    let task = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "live exact query"),
            support::deadline(),
        )
        .await
        .unwrap()
        .value
        .task()
        .clone();
    let input = reservation(&fixture.repository, &task, "live-exact-query");
    fixture
        .store
        .reserve_attempt_artifact(input.clone())
        .await
        .unwrap();
    fixture
        .store
        .mark_attempt_artifact_ready(input.identity)
        .await
        .unwrap();
    let adapter =
        LiveStoreWriterArtifactAdapter::new(fixture.store.clone(), fixture.writer.clone());

    let disposition = adapter
        .mark_attempt_artifact_ready(input.identity, Instant::now())
        .await;
    let evidence = disposition
        .reconciliation_evidence()
        .expect("expired writer deadline must be reconciled by exact durable state");
    assert_eq!(evidence.identity(), input.identity);
    assert_eq!(evidence.state(), AttemptArtifactState::Ready);
    assert!(evidence.failure_code().is_none());
}

#[tokio::test]
async fn restart_marks_absent_partial_and_mismatched_inconsistent_but_valid_ready() {
    let fixture = support::writer_fixture().await;
    let cases = [
        ("absent", RestartArtifactObservation::Absent),
        ("valid", RestartArtifactObservation::Ready),
        ("partial", RestartArtifactObservation::Partial),
        ("mismatched", RestartArtifactObservation::Inconsistent),
    ];
    let mut states = HashMap::new();
    let mut task_ids = HashMap::new();
    for (name, observation) in cases {
        let task = fixture
            .writer
            .create_task(
                support::new_task(fixture.repository.id, name),
                support::deadline(),
            )
            .await
            .unwrap()
            .value
            .task()
            .clone();
        let reservation = reservation(&fixture.repository, &task, name);
        states.insert(reservation.branch_name.clone(), observation);
        task_ids.insert(name, task.id);
        fixture
            .writer
            .reserve_attempt_artifact(reservation, support::deadline())
            .await
            .unwrap();
    }

    let summary = reconcile_restart_artifacts(
        &fixture.store,
        &fixture.writer,
        &ScriptedObserver { states },
        Duration::from_secs(2),
    )
    .await
    .unwrap();

    assert_eq!(summary.examined, 4);
    assert_eq!(summary.marked_ready, 1);
    assert_eq!(summary.marked_inconsistent, 3);
    let valid = fixture
        .store
        .load_attempt_artifact(task_ids["valid"])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(valid.state, AttemptArtifactState::Ready);
    assert!(valid.failure_code.is_none());
    let absent = fixture
        .store
        .load_attempt_artifact(task_ids["absent"])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(absent.state, AttemptArtifactState::Inconsistent);
    assert_eq!(
        absent.failure_code.as_deref(),
        Some("WORKTREE_RESERVATION_ABANDONED")
    );
    for name in ["partial", "mismatched"] {
        let artifact = fixture
            .store
            .load_attempt_artifact(task_ids[name])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(artifact.state, AttemptArtifactState::Inconsistent);
        assert_eq!(
            artifact.failure_code.as_deref(),
            Some("WORKTREE_STATE_INCONSISTENT")
        );
    }
    assert!(
        fixture
            .store
            .list_reserved_attempt_artifacts()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn store_writer_deadline_starts_after_slow_artifact_observation() {
    let fixture = support::writer_fixture().await;
    let task = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "slow observation"),
            support::deadline(),
        )
        .await
        .unwrap()
        .value
        .task()
        .clone();
    fixture
        .writer
        .reserve_attempt_artifact(
            reservation(&fixture.repository, &task, "slow-observation"),
            support::deadline(),
        )
        .await
        .unwrap();

    let summary = reconcile_restart_artifacts(
        &fixture.store,
        &fixture.writer,
        &DelayedObserver {
            delay: Duration::from_secs(2),
            observation: RestartArtifactObservation::Ready,
        },
        Duration::from_secs(1),
    )
    .await
    .expect("observation time must not consume the StoreWriter budget");

    assert_eq!(summary.examined, 1);
    assert_eq!(summary.marked_ready, 1);
    assert_eq!(summary.marked_inconsistent, 0);
    assert_eq!(
        fixture
            .store
            .load_attempt_artifact(task.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        AttemptArtifactState::Ready
    );
}

#[tokio::test]
async fn same_run_identical_reservation_remains_reserved_for_safe_reentry() {
    let fixture = support::writer_fixture().await;
    let task = fixture
        .writer
        .create_task(
            support::new_task(fixture.repository.id, "same run"),
            support::deadline(),
        )
        .await
        .unwrap()
        .value
        .task()
        .clone();
    let input = reservation(&fixture.repository, &task, "same-run");
    fixture
        .writer
        .reserve_attempt_artifact(input.clone(), support::deadline())
        .await
        .unwrap();
    let replay = fixture
        .writer
        .reserve_attempt_artifact(input, support::deadline())
        .await
        .unwrap();

    assert!(matches!(
        replay.value,
        ReserveAttemptArtifactOutcome::Existing(ref artifact)
            if artifact.state == AttemptArtifactState::Reserved
    ));
    assert_eq!(
        fixture
            .store
            .list_reserved_attempt_artifacts()
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn restart_reconciliation_observes_real_git_and_disk_state() {
    let test_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/reconcile-tests");
    std::fs::create_dir_all(&test_root).unwrap();
    let temporary = tempfile::Builder::new()
        .prefix("restart-real-")
        .tempdir_in(test_root)
        .unwrap();
    let root = temporary.path().canonicalize().unwrap();
    let repository_path = root.join("repository");
    let cargo_workspace = repository_path.join("nested/rust");
    let artifact_root = root.join("artifacts");
    let runtime_directory = root.join("runtime");
    for directory in [
        cargo_workspace.join("src"),
        artifact_root.clone(),
        runtime_directory.clone(),
    ] {
        std::fs::create_dir_all(directory).unwrap();
    }
    git(&repository_path, &["init", "--quiet"]);
    git(&repository_path, &["config", "user.name", "Reconcile Test"]);
    git(
        &repository_path,
        &["config", "user.email", "reconcile@example.invalid"],
    );
    std::fs::write(repository_path.join("tracked.txt"), b"first\n").unwrap();
    std::fs::write(
        cargo_workspace.join("Cargo.toml"),
        b"[workspace]\n\n[package]\nname = \"reconcile_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .unwrap();
    std::fs::write(
        cargo_workspace.join("Cargo.lock"),
        b"# This file is automatically @generated by Cargo.\nversion = 4\n\n[[package]]\nname = \"reconcile_fixture\"\nversion = \"0.1.0\"\n",
    )
    .unwrap();
    std::fs::write(cargo_workspace.join("src/lib.rs"), b"pub fn first() {}\n").unwrap();
    git(&repository_path, &["add", "--all"]);
    git(
        &repository_path,
        &["commit", "--quiet", "--no-gpg-sign", "-m", "first"],
    );
    std::fs::write(repository_path.join("tracked.txt"), b"second\n").unwrap();
    git(&repository_path, &["add", "--all"]);
    git(
        &repository_path,
        &["commit", "--quiet", "--no-gpg-sign", "-m", "second"],
    );

    let store = Store::open(":memory:").await.unwrap();
    store.migrate().await.unwrap();
    let repository = match store
        .register_repository(NewRepository {
            selected_path: canonical(&repository_path),
            display_name: "restart-real".to_owned(),
            git_root: canonical(&repository_path),
            cargo_workspace_root: canonical(&cargo_workspace),
        })
        .await
        .unwrap()
    {
        RegisterRepositoryOutcome::Created(repository)
        | RegisterRepositoryOutcome::Existing(repository) => repository,
    };
    let writer = StoreWriterHandle::spawn(
        store.clone(),
        Arc::new(support::CountingWake::default()),
        16,
    );
    let toolchain = discover_toolchain(
        &runtime_directory,
        support::instance_process_scope(&runtime_directory),
        Some(&concrete_rustc()),
        Some(&path_executable(if cfg!(windows) {
            "git.exe"
        } else {
            "git"
        })),
    )
    .await
    .unwrap();
    let provisioner = Arc::new(
        WorktreeProvisioner::from_trusted_paths(
            &toolchain,
            repository.id.to_string(),
            &repository_path,
            &cargo_workspace,
            &artifact_root,
            &runtime_directory,
            support::task_process_scope(&runtime_directory),
            process_limits(),
            WorktreeLimits::try_new(Duration::from_secs(15)).unwrap(),
        )
        .unwrap(),
    );

    let mut task_ids = HashMap::new();
    let mut reservations = HashMap::new();
    for name in ["absent", "ready", "partial", "mismatched"] {
        let task = writer
            .create_task(support::new_task(repository.id, name), support::deadline())
            .await
            .unwrap()
            .value
            .task()
            .clone();
        let identity =
            WorktreeIdentity::try_new(repository.id.to_string(), task.id.to_string(), task.attempt)
                .unwrap();
        let reserved = provisioner
            .prepare(identity, CancellationToken::new())
            .await
            .unwrap();
        writer
            .reserve_attempt_artifact(
                ReserveAttemptArtifact {
                    identity: AttemptArtifactIdentity {
                        task_id: task.id,
                        repository_id: repository.id,
                        attempt: task.attempt,
                    },
                    base_commit: reserved.base_commit().to_owned(),
                    branch_name: reserved.branch_name().to_owned(),
                    worktree_path: CanonicalPath::try_from_canonical(
                        reserved.worktree_path().to_owned(),
                    )
                    .unwrap(),
                },
                support::deadline(),
            )
            .await
            .unwrap();
        task_ids.insert(name, task.id);
        reservations.insert(name, reserved);
    }
    provisioner
        .provision_reserved(
            reservations.remove("ready").unwrap(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let partial = &reservations["partial"];
    git(
        &repository_path,
        &["branch", partial.branch_name(), partial.base_commit()],
    );
    let mismatched = &reservations["mismatched"];
    git(
        &repository_path,
        &["branch", mismatched.branch_name(), "HEAD^"],
    );

    let observer = WorktreeArtifactObserver::new([(repository.id, provisioner)]);
    let error = reconcile_restart_artifacts(&store, &writer, &observer, Duration::from_secs(5))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ArtifactReconciliationError::ObservationRepositoryMismatch { identity }
            if identity.task_id == task_ids["mismatched"]
    ));
    assert_eq!(
        store
            .load_attempt_artifact(task_ids["ready"])
            .await
            .unwrap()
            .unwrap()
            .state,
        AttemptArtifactState::Ready
    );
    for name in ["absent", "partial", "mismatched"] {
        assert_eq!(
            store
                .load_attempt_artifact(task_ids[name])
                .await
                .unwrap()
                .unwrap()
                .state,
            AttemptArtifactState::Inconsistent
        );
    }
}

fn reservation(
    repository: &coding_agent_domain::Repository,
    task: &coding_agent_domain::Task,
    name: &str,
) -> ReserveAttemptArtifact {
    ReserveAttemptArtifact {
        identity: AttemptArtifactIdentity {
            task_id: task.id,
            repository_id: repository.id,
            attempt: task.attempt,
        },
        base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
        branch_name: format!("codex/{name}-{}", task.id),
        worktree_path: CanonicalPath::try_from_canonical(
            repository
                .git_root
                .as_path()
                .join("restart-artifacts")
                .join(task.id.to_string()),
        )
        .unwrap(),
    }
}

async fn create_reserved_artifact(
    fixture: &support::WriterFixture,
    repository: &Repository,
    name: &str,
) -> ReserveAttemptArtifact {
    let task = fixture
        .writer
        .create_task(support::new_task(repository.id, name), support::deadline())
        .await
        .unwrap()
        .value
        .task()
        .clone();
    let reservation = reservation(repository, &task, name);
    fixture
        .writer
        .reserve_attempt_artifact(reservation.clone(), support::deadline())
        .await
        .unwrap();
    reservation
}

async fn register_alias_repository(store: &Store, existing: &Repository, name: &str) -> Repository {
    let root = existing
        .git_root
        .as_path()
        .parent()
        .expect("fixture Git root has a parent");
    let selected_path = root.join(format!("{name}-selected"));
    let cargo_workspace_root = root.join(format!("{name}-workspace"));
    std::fs::create_dir_all(&selected_path).unwrap();
    std::fs::create_dir_all(&cargo_workspace_root).unwrap();
    register_test_repository(
        store,
        NewRepository {
            selected_path: canonical(&selected_path),
            display_name: name.to_owned(),
            git_root: existing.git_root.clone(),
            cargo_workspace_root: canonical(&cargo_workspace_root),
        },
    )
    .await
}

async fn register_distinct_repository(
    store: &Store,
    existing: &Repository,
    name: &str,
) -> Repository {
    let root = existing
        .git_root
        .as_path()
        .parent()
        .expect("fixture Git root has a parent");
    let selected_path = root.join(format!("{name}-selected"));
    let git_root = root.join(format!("{name}-git"));
    let cargo_workspace_root = root.join(format!("{name}-workspace"));
    for path in [&selected_path, &git_root, &cargo_workspace_root] {
        std::fs::create_dir_all(path).unwrap();
    }
    register_test_repository(
        store,
        NewRepository {
            selected_path: canonical(&selected_path),
            display_name: name.to_owned(),
            git_root: canonical(&git_root),
            cargo_workspace_root: canonical(&cargo_workspace_root),
        },
    )
    .await
}

async fn register_test_repository(store: &Store, input: NewRepository) -> Repository {
    match store.register_repository(input).await.unwrap() {
        RegisterRepositoryOutcome::Created(repository)
        | RegisterRepositoryOutcome::Existing(repository) => repository,
    }
}

fn canonical(path: &Path) -> CanonicalPath {
    CanonicalPath::try_from_canonical(path.canonicalize().unwrap()).unwrap()
}

fn process_limits() -> ProcessLimits {
    ProcessLimits::try_new(
        512 * 1024,
        512 * 1024,
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .unwrap()
}

fn git(repository: &Path, arguments: &[&str]) {
    let output = support::command_output(
        Command::new(path_executable(if cfg!(windows) {
            "git.exe"
        } else {
            "git"
        }))
        .arg("-C")
        .arg(repository)
        .args(arguments),
    )
    .unwrap();
    assert!(
        output.status.success(),
        "git {} failed: {}",
        arguments.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn concrete_rustc() -> PathBuf {
    let output =
        support::command_output(Command::new("rustc").args(["--print", "sysroot"])).unwrap();
    assert!(output.status.success());
    PathBuf::from(String::from_utf8(output.stdout).unwrap().trim())
        .join("bin")
        .join(if cfg!(windows) { "rustc.exe" } else { "rustc" })
        .canonicalize()
        .unwrap()
}

fn path_executable(name: &str) -> PathBuf {
    std::env::split_paths(&std::env::var_os("PATH").unwrap())
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .unwrap()
        .canonicalize()
        .unwrap()
}
