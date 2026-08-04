use std::str::FromStr;

use coding_agent_domain::Task;
use coding_agent_store::{
    AdvanceDeliverySourceObjectRequest, CommitDeliverySourceRequest, CreateDeliverySourceOutcome,
    CreateDeliverySourceRequest, DeliverySourceAnchor, DeliverySourceAppliedProof,
    DeliverySourceObjectProof, DeliverySourceReconciliationReason, DeliverySourceRetryReason,
    DeliverySourceState, DeliveryVersion, GitCommitOid, ReconcileDeliverySourceOutcome,
    ReconcileDeliverySourceRequest, RecordDeliverySourceRetryRequest, SourceWorktreeProof, Store,
};

use crate::snapshot::CompatibilitySnapshot;
use crate::support::delivery::eligibility::SOURCE_COMMIT;

use super::helpers::{applied_source, ownership};
use super::preflight::AcceptedPreflight;
use super::scenario;

struct SourceStage {
    anchor: DeliverySourceAnchor,
    object: DeliverySourceObjectProof,
    version: DeliveryVersion,
}

pub async fn commit_delivery_source(
    store: &Store,
    task: &Task,
    accepted: &AcceptedPreflight,
    baseline: &CompatibilitySnapshot,
) {
    let mut stage = create_object_pending(store, task, accepted, baseline).await;
    advance_to_commit_pending(store, &mut stage, baseline).await;
    commit_source(store, task, &stage, baseline).await;
}

pub async fn exercise_retry_and_reconcile_transitions() {
    retry_from(DeliverySourceState::ObjectPending).await;
    retry_from(DeliverySourceState::CommitPending).await;
    reconcile_from(DeliverySourceState::ObjectPending).await;
    reconcile_from(DeliverySourceState::CommitPending).await;
    reconcile_from(DeliverySourceState::Committed).await;
}

async fn retry_from(state: DeliverySourceState) {
    let (fixture, baseline, accepted) = scenario::accepted().await;
    let mut stage =
        create_object_pending(&fixture.store, &fixture.delivery_task, &accepted, &baseline).await;
    if state == DeliverySourceState::CommitPending {
        advance_to_commit_pending(&fixture.store, &mut stage, &baseline).await;
    }
    applied_source(
        fixture
            .store
            .record_delivery_source_retry(
                RecordDeliverySourceRetryRequest::try_new(
                    stage.anchor,
                    state,
                    stage.version,
                    DeliverySourceRetryReason::CommandTimedOut,
                )
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    baseline
        .assert_unchanged(&fixture.store, retry_label(state))
        .await;
}

async fn reconcile_from(state: DeliverySourceState) {
    let (fixture, baseline, accepted) = scenario::accepted().await;
    let mut stage =
        create_object_pending(&fixture.store, &fixture.delivery_task, &accepted, &baseline).await;
    if matches!(
        state,
        DeliverySourceState::CommitPending | DeliverySourceState::Committed
    ) {
        advance_to_commit_pending(&fixture.store, &mut stage, &baseline).await;
    }
    if state == DeliverySourceState::Committed {
        commit_source(&fixture.store, &fixture.delivery_task, &stage, &baseline).await;
        stage.version = stage.version.next().unwrap();
    }
    let outcome = fixture
        .store
        .reconcile_delivery_source(
            ReconcileDeliverySourceRequest::try_new(
                stage.anchor,
                state,
                stage.version,
                accepted.version,
                DeliverySourceReconciliationReason::SourceInconsistent,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        ReconcileDeliverySourceOutcome::Applied(_)
    ));
    baseline
        .assert_unchanged(&fixture.store, reconcile_label(state))
        .await;
}

async fn create_object_pending(
    store: &Store,
    task: &Task,
    accepted: &AcceptedPreflight,
    baseline: &CompatibilitySnapshot,
) -> SourceStage {
    let source = match store
        .create_delivery_source(
            CreateDeliverySourceRequest::try_new(accepted.command.clone()).unwrap(),
        )
        .await
        .unwrap()
    {
        CreateDeliverySourceOutcome::Created(source) => source,
        other => panic!("expected object-pending source, got {other:?}"),
    };
    baseline
        .assert_unchanged(store, "accepted to source object pending")
        .await;
    let anchor =
        DeliverySourceAnchor::try_new(task.id, accepted.operation_id, accepted.version).unwrap();
    let object = DeliverySourceObjectProof::try_new(
        GitCommitOid::from_str(SOURCE_COMMIT).unwrap(),
        source.candidate_tree.clone(),
        vec![source.expected_parent.clone()],
        source.commit_metadata.clone(),
    )
    .unwrap();
    let version = source.version;
    SourceStage {
        anchor,
        object,
        version,
    }
}

async fn advance_to_commit_pending(
    store: &Store,
    stage: &mut SourceStage,
    baseline: &CompatibilitySnapshot,
) {
    let receipt = applied_source(
        store
            .advance_delivery_source_object(
                AdvanceDeliverySourceObjectRequest::try_new(
                    stage.anchor,
                    stage.version,
                    stage.object.clone(),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
    );
    stage.version = receipt.version;
    baseline
        .assert_unchanged(store, "source object pending to commit pending")
        .await;
}

async fn commit_source(
    store: &Store,
    task: &Task,
    stage: &SourceStage,
    baseline: &CompatibilitySnapshot,
) {
    let current = ownership(store, task.id).await.source.unwrap();
    let worktree = SourceWorktreeProof::try_new(
        current.candidate_tree.clone(),
        current.candidate_tree.clone(),
        0,
        0,
        0,
        0,
    )
    .unwrap();
    let source_oid = GitCommitOid::from_str(SOURCE_COMMIT).unwrap();
    let applied = DeliverySourceAppliedProof::try_new(
        stage.object.clone(),
        current.provenance.source_branch.clone(),
        source_oid.clone(),
        source_oid,
        worktree,
        current.provenance.common_git_identity.clone(),
        current.provenance.worktree_admin_identity.clone(),
        current.provenance.fixed_lock_reason.clone(),
        current.provenance.config_attributes_digest.clone(),
    )
    .unwrap();
    applied_source(
        store
            .commit_delivery_source(
                CommitDeliverySourceRequest::try_new(stage.anchor, stage.version, applied).unwrap(),
            )
            .await
            .unwrap(),
    );
    baseline
        .assert_unchanged(store, "source commit pending to committed")
        .await;
}

const fn retry_label(state: DeliverySourceState) -> &'static str {
    match state {
        DeliverySourceState::ObjectPending => "source object pending retry",
        DeliverySourceState::CommitPending => "source commit pending retry",
        _ => unreachable!(),
    }
}

const fn reconcile_label(state: DeliverySourceState) -> &'static str {
    match state {
        DeliverySourceState::ObjectPending => "source object pending paired reconcile",
        DeliverySourceState::CommitPending => "source commit pending paired reconcile",
        DeliverySourceState::Committed => "source committed paired reconcile",
        _ => unreachable!(),
    }
}
