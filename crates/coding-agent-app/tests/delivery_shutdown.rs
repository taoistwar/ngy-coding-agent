#![cfg(feature = "test-support")]

mod support;

use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

use coding_agent_app::{
    DeliveryManagerError, DeliveryManagerHandle, InstanceLock, RepositoryControlCoordinator,
    RepositoryControlError, RepositoryControlPoisonReason, RepositoryControlState, ServiceState,
    ServiceStateController,
};
use coding_agent_domain::{CanonicalPath, RepositoryId, TaskId};
use coding_agent_runtime::RootCapability;
use coding_agent_store::RepositoryIdentityLookup;

#[tokio::test]
async fn top_level_shutdown_closes_http_but_keeps_primary_lock_for_unknown_delivery_owner() {
    let fixture = support::shutdown_fixture([]).await;
    fixture
        .handles
        .delivery_manager
        .retain_fail_closed_for_shutdown_test(fixture.repository.id)
        .await
        .expect("retain exact delivery worker ownership");
    let port = fixture.primary.port();
    let descriptor = fixture.startup.paths.instance_descriptor.clone();
    let lock_path = fixture.startup.paths.instance_lock.clone();

    tokio::time::pause();
    let shutdown = tokio::spawn({
        let coordinator = fixture.primary.shutdown_coordinator();
        async move { coordinator.shutdown().await }
    });
    settle().await;
    assert!(
        fixture.handles.task_manager.shutdown_latched_for_test(),
        "TaskManager cleanup starts without waiting back on DeliveryManager"
    );

    tokio::time::advance(Duration::from_secs(10)).await;
    for _ in 0..10_000 {
        if !descriptor.exists() {
            break;
        }
        tokio::time::advance(Duration::ZERO).await;
        tokio::task::yield_now().await;
    }
    assert!(
        !shutdown.is_finished(),
        "unknown delivery ownership has no finite safe process exit"
    );
    assert!(!descriptor.exists(), "the HTTP descriptor is unpublished");
    assert!(
        tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port))
            .await
            .is_err(),
        "the HTTP listener closes at the outer shutdown budget"
    );
    assert!(
        InstanceLock::try_acquire(&lock_path)
            .expect("probe primary lock while delivery ownership is unknown")
            .is_none(),
        "the primary lock remains the process-exit safety fence"
    );
    tokio::time::resume();

    shutdown.abort();
    let _ = shutdown.await;
}

#[tokio::test]
async fn hard_shutdown_closes_intake_and_joins_when_ownership_is_empty() {
    let manager = DeliveryManagerHandle::spawn_unavailable(
        Arc::new(RepositoryControlCoordinator::new()),
        ServiceStateController::new(ServiceState::Ready),
        2,
    );

    let proof = manager
        .shutdown_and_join()
        .await
        .expect("an idle delivery manager has an exact shutdown proof");

    assert_eq!(proof.in_flight_workers(), 0);
    assert_eq!(proof.queued_workers(), 0);
    assert_eq!(proof.retained_workers(), 0);
    assert_eq!(
        manager.query(TaskId::new()).await,
        Err(DeliveryManagerError::Closed)
    );
}

#[tokio::test]
async fn unknown_worker_retains_repository_global_slot_and_shutdown_join() {
    let first_identity = tempfile::tempdir().expect("first common identity");
    let second_identity = tempfile::tempdir().expect("second common identity");
    let coordinator = Arc::new(RepositoryControlCoordinator::new());
    register(&coordinator, 1, "first", &first_identity);
    let second_key = register(&coordinator, 2, "second", &second_identity);
    let manager = DeliveryManagerHandle::spawn_unavailable(
        Arc::clone(&coordinator),
        ServiceStateController::new(ServiceState::Ready),
        2,
    );
    manager
        .retain_fail_closed_for_shutdown_test(repository_id(1))
        .await
        .expect("inject an exact unknown child owner");

    let shutdown = tokio::spawn({
        let manager = manager.clone();
        async move { manager.shutdown_and_join().await }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!shutdown.is_finished());
    assert_eq!(manager.available_git_permits_for_test().await, 1);
    assert_eq!(
        coordinator.control_state(repository_id(1)),
        Ok(RepositoryControlState::Poisoned),
        "the exact common identity is sticky-poisoned while its owner remains retained"
    );
    assert_eq!(
        coordinator.poison_reason(repository_id(1)),
        Ok(Some(RepositoryControlPoisonReason::GitChildOutcomeUnknown))
    );
    let unrelated = coordinator
        .try_acquire_delivery(second_key)
        .expect("an unrelated authenticated common identity remains operable");
    unrelated
        .clean_release()
        .expect("release unrelated delivery owner");

    shutdown.abort();
    let _ = shutdown.await;
}

#[test]
fn exact_common_identity_poison_does_not_freeze_other_delivery_mutation() {
    let first_identity = tempfile::tempdir().expect("first common identity");
    let second_identity = tempfile::tempdir().expect("second common identity");
    let coordinator = RepositoryControlCoordinator::new();
    let first_key = register(&coordinator, 1, "first", &first_identity);
    let second_key = register(&coordinator, 2, "second", &second_identity);

    coordinator
        .try_acquire_delivery(first_key)
        .expect("acquire first delivery identity")
        .poison(RepositoryControlPoisonReason::DeliveryReconciliationRequired)
        .expect("poison only the authenticated first identity");

    assert!(!coordinator.delivery_mutations_frozen());
    assert_eq!(
        coordinator.try_acquire_delivery(first_key).unwrap_err(),
        RepositoryControlError::Poisoned
    );
    assert_eq!(
        coordinator.coordination_key(repository_id(2)),
        Ok(second_key),
        "read-only routing for the unrelated identity remains available"
    );
    coordinator
        .try_acquire_delivery(second_key)
        .expect("second identity remains operable")
        .clean_release()
        .expect("release second identity");
}

#[test]
fn unbounded_identity_failure_freezes_only_delivery_mutation() {
    let identity = tempfile::tempdir().expect("known common identity");
    let coordinator = RepositoryControlCoordinator::new();
    let known_key = register(&coordinator, 1, "known", &identity);

    coordinator.observe_identity_unavailable(&lookup(99, "unbounded"));

    assert!(coordinator.delivery_mutations_frozen());
    assert_eq!(
        coordinator.try_acquire_delivery(known_key).unwrap_err(),
        RepositoryControlError::DeliveryMutationsFrozen
    );
    assert_eq!(
        coordinator.coordination_key(repository_id(1)),
        Ok(known_key),
        "global delivery freeze does not block read-only identity routing"
    );
    coordinator
        .try_acquire(known_key)
        .expect("TaskManager safety work does not use the delivery-only freeze")
        .clean_release()
        .expect("release TaskManager repository owner");
    assert_eq!(
        coordinator.control_state(repository_id(1)),
        Ok(RepositoryControlState::Available)
    );
}

fn register(
    coordinator: &RepositoryControlCoordinator,
    suffix: u32,
    seed: &str,
    identity: &tempfile::TempDir,
) -> coding_agent_app::RepositoryCoordinationKey {
    let marker = RootCapability::open(identity.path().canonicalize().unwrap())
        .expect("open authenticated common identity")
        .identity_marker()
        .expect("observe authenticated common identity");
    coordinator
        .register_authenticated_alias(lookup(suffix, seed), marker)
        .expect("register authenticated repository alias")
}

fn repository_id(suffix: u32) -> RepositoryId {
    RepositoryId::from_str(&format!("10000000-0000-4000-8000-{suffix:012x}"))
        .expect("canonical repository ID")
}

fn lookup(suffix: u32, seed: &str) -> RepositoryIdentityLookup {
    let repository_id = repository_id(suffix);
    let git_root = std::env::current_dir()
        .expect("resolve delivery-shutdown fixture root")
        .join("target")
        .join("delivery-shutdown-identities")
        .join(repository_id.to_string());
    RepositoryIdentityLookup {
        repository_id,
        git_root: CanonicalPath::try_from_canonical(git_root)
            .expect("construct delivery-shutdown Git root"),
        git_identity_key: seed.to_owned(),
    }
}

async fn settle() {
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
}
