// The startup matrix intentionally uses only selected controls from these
// shared integration fixtures.
#[allow(dead_code)]
mod delivery_cleanup_support;
#[allow(dead_code, unused_imports)]
mod delivery_merge_support;
mod delivery_production_support;
mod support;

use coding_agent_app::{
    DeliveryCommandConflict, DeliveryMergeAcceptanceOutcome,
    startup_artifact_is_delivery_owned_for_test,
};
use coding_agent_domain::{CanonicalPath, TaskStatus};
use coding_agent_runtime::DeliveryRemovePendingDisposition;
use coding_agent_store::{
    AttemptArtifactIdentity, AttemptArtifactState, CleanupOperationState, CreateTaskOutcome,
    DeliverySourceState, MergeOperationState, ReserveAttemptArtifact, TaskAttemptArtifact,
    TaskTransition, TransitionOutcome, WorktreeDisposition,
};

use delivery_cleanup_support::DeliveryCleanupFixture;
use delivery_merge_support::{DeliveryMergeFixture, LiveFault, LiveStage};
use delivery_production_support::ProductionDeliveryFixture;

#[tokio::test]
async fn production_registry_resolves_late_repository_and_fresh_accept_rejects_real_source_drift() {
    let fixture = ProductionDeliveryFixture::new("fresh-accept-source-drift").await;
    let (operation_id, command) = fixture.prepare_accept().await;
    let source_before = fixture.source_ref_oid();
    assert_eq!(fixture.accept_receipt_count().await, 0);

    std::fs::write(
        fixture.source_worktree.join("tracked.txt"),
        b"changed after durable ready preflight\n",
    )
    .expect("drift the real reviewed source after preflight");
    let outcome = fixture.accept(command).await;

    assert_eq!(
        outcome,
        DeliveryMergeAcceptanceOutcome::Conflict(DeliveryCommandConflict::SourceChanged)
    );
    assert_eq!(fixture.accept_receipt_count().await, 0);
    assert_eq!(fixture.source_ref_oid(), source_before);
    let operation = fixture.operation(operation_id).await;
    assert_eq!(operation.state, MergeOperationState::Stale);
    assert_eq!(
        operation.failure_code.as_ref().map(|code| code.as_str()),
        Some("DELIVERY_SOURCE_CHANGED")
    );
    assert!(operation.accept_receipt_id.is_none());
}

#[tokio::test]
async fn production_fresh_accept_target_branch_drift_is_durable_stale_without_acceptance() {
    let fixture = ProductionDeliveryFixture::new("fresh-accept-target-branch-drift").await;
    let (operation_id, command) = fixture.prepare_accept().await;
    let source_before = fixture.source_ref_oid();
    assert_eq!(fixture.accept_receipt_count().await, 0);

    fixture.switch_target_to_new_branch("drifted-target");
    let outcome = fixture.accept(command).await;

    assert_eq!(
        outcome,
        DeliveryMergeAcceptanceOutcome::Conflict(DeliveryCommandConflict::TargetBranchMismatch)
    );
    assert_eq!(fixture.accept_receipt_count().await, 0);
    assert_eq!(fixture.source_ref_oid(), source_before);
    let ownership = fixture
        .store
        .delivery_ownership_snapshot(fixture.task.id)
        .await
        .expect("load production delivery ownership")
        .expect("production delivery ownership exists");
    assert!(ownership.source.is_none());
    let operation = fixture.operation(operation_id).await;
    assert_eq!(operation.state, MergeOperationState::Stale);
    assert_eq!(
        operation.failure_code.as_ref().map(|code| code.as_str()),
        Some("TARGET_BRANCH_MISMATCH")
    );
    assert!(operation.accept_receipt_id.is_none());
}

#[tokio::test]
async fn ownership_overlay_excludes_every_durable_delivery_lifecycle_from_base_routing() {
    let ready_fixture = DeliveryMergeFixture::new(None).await;
    let ready = ready_fixture.prepare_accept().await;
    let ready_artifact = artifact(&ready_fixture, ready.task.id).await;
    assert_owned(&ready_fixture, &ready_artifact).await;
    ready_fixture.finish().await;

    let committed_fixture = DeliveryMergeFixture::new(None).await;
    committed_fixture
        .live_runtime
        .fail_once(LiveStage::ActualMerge, LiveFault::Unavailable);
    let committed = committed_fixture.prepare_accept().await;
    committed_fixture.accept(&committed).await;
    committed_fixture
        .wait_source_state(committed.task.id, DeliverySourceState::Committed)
        .await;
    committed_fixture
        .wait_operation_state(committed.operation_id, MergeOperationState::MergePending)
        .await;
    assert_owned(
        &committed_fixture,
        &artifact(&committed_fixture, committed.task.id).await,
    )
    .await;
    committed_fixture.finish().await;

    let cleanup = DeliveryCleanupFixture::new(None).await;
    let cleanup_artifact = artifact(&cleanup.merge, cleanup.task.id).await;
    assert_owned(&cleanup.merge, &cleanup_artifact).await;

    let removed = cleanup.remove().await;
    cleanup
        .wait_operation_state(removed.operation_id(), CleanupOperationState::Completed)
        .await;
    assert_owned(&cleanup.merge, &cleanup_artifact).await;

    let deleted = cleanup.delete().await;
    cleanup
        .wait_operation_state(deleted.operation_id(), CleanupOperationState::Completed)
        .await;
    assert_owned(&cleanup.merge, &cleanup_artifact).await;
    cleanup.finish().await;

    let retained = DeliveryCleanupFixture::new(None).await;
    retained
        .runtime
        .push_remove_step(DeliveryRemovePendingDisposition::KnownNotAppliedDirty);
    let failed = retained.remove().await;
    retained
        .wait_operation_state(failed.operation_id(), CleanupOperationState::Failed)
        .await;
    let disposition = retained
        .merge
        .base
        .store
        .delivery_ownership_snapshot(retained.task.id)
        .await
        .expect("load retained ownership")
        .expect("retained task exists")
        .disposition
        .expect("retained disposition exists");
    assert_eq!(
        disposition.worktree_state,
        WorktreeDisposition::RetainedUnlocked
    );
    assert_owned(
        &retained.merge,
        &artifact(&retained.merge, retained.task.id).await,
    )
    .await;
    retained.finish().await;
}

#[tokio::test]
async fn ownership_overlay_routes_real_non_delivery_artifacts_to_p4a() {
    let fixture = DeliveryMergeFixture::new(None).await;
    let artifact = create_non_delivery_artifact(&fixture).await;

    assert!(
        !startup_artifact_is_delivery_owned_for_test(&fixture.base.store, &artifact)
            .await
            .expect("non-delivery artifact has an unambiguous base route")
    );
    fixture.finish().await;
}

#[tokio::test]
async fn ownership_overlay_fails_closed_on_attempt_or_state_mismatch() {
    let fixture = DeliveryMergeFixture::new(None).await;
    let prepared = fixture.prepare_accept().await;
    let owned = artifact(&fixture, prepared.task.id).await;

    let mut wrong_attempt = owned.clone();
    wrong_attempt.identity.attempt = wrong_attempt
        .identity
        .attempt
        .checked_add(1)
        .expect("fixture attempt increments");
    assert!(
        startup_artifact_is_delivery_owned_for_test(&fixture.base.store, &wrong_attempt)
            .await
            .is_err()
    );

    let mut wrong_state = owned;
    wrong_state.state = AttemptArtifactState::Reserved;
    assert!(
        startup_artifact_is_delivery_owned_for_test(&fixture.base.store, &wrong_state)
            .await
            .is_err()
    );
    fixture.finish().await;
}

async fn artifact(
    fixture: &DeliveryMergeFixture,
    task_id: coding_agent_domain::TaskId,
) -> TaskAttemptArtifact {
    fixture
        .base
        .store
        .load_attempt_artifact(task_id)
        .await
        .expect("load attempt artifact")
        .expect("attempt artifact exists")
}

async fn assert_owned(fixture: &DeliveryMergeFixture, artifact: &TaskAttemptArtifact) {
    assert!(
        startup_artifact_is_delivery_owned_for_test(&fixture.base.store, artifact)
            .await
            .expect("delivery ownership graph is valid")
    );
}

async fn create_non_delivery_artifact(fixture: &DeliveryMergeFixture) -> TaskAttemptArtifact {
    let queued = match fixture
        .base
        .store
        .create_task(support::new_task(
            fixture.base.repository.id,
            "base lifecycle artifact",
        ))
        .await
        .expect("create non-delivery task")
    {
        CreateTaskOutcome::Created { task, .. } => task,
        CreateTaskOutcome::Existing { .. } => panic!("fixture request is unique"),
    };
    let running = match fixture
        .base
        .store
        .transition_with_event(queued.id, TaskStatus::Queued, TaskTransition::Running)
        .await
        .expect("start non-delivery task")
    {
        TransitionOutcome::Applied { task, .. } => task,
        TransitionOutcome::Conflict { .. } => panic!("fixture task starts once"),
    };
    let identity = AttemptArtifactIdentity {
        task_id: running.id,
        repository_id: running.repository_id,
        attempt: running.attempt,
    };
    let path = fixture
        .base
        .repository
        .git_root
        .as_path()
        .join("artifacts")
        .join(running.id.to_string());
    fixture
        .base
        .store
        .reserve_attempt_artifact(ReserveAttemptArtifact {
            identity,
            base_commit: delivery_merge_support::BASE_COMMIT.to_owned(),
            branch_name: format!("codex/{}", running.id),
            worktree_path: CanonicalPath::try_from_canonical(path)
                .expect("canonical fixture artifact path"),
        })
        .await
        .expect("reserve non-delivery artifact");
    fixture
        .base
        .store
        .mark_attempt_artifact_ready(identity)
        .await
        .expect("mark non-delivery artifact ready");
    artifact(fixture, running.id).await
}
