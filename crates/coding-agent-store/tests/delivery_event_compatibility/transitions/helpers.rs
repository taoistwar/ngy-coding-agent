use coding_agent_domain::TaskId;
use coding_agent_store::{
    CleanupTransitionOutcome, DeliverySourceTransitionOutcome, MergeTransitionOutcome, Store,
};

pub async fn ownership(
    store: &Store,
    task_id: TaskId,
) -> coding_agent_store::DeliveryOwnershipSnapshot {
    store
        .delivery_ownership_snapshot(task_id)
        .await
        .unwrap()
        .unwrap()
}

pub fn applied_source(
    outcome: DeliverySourceTransitionOutcome,
) -> coding_agent_store::DeliverySourceTransitionReceipt {
    match outcome {
        DeliverySourceTransitionOutcome::Applied(receipt) => receipt,
        other => panic!("expected applied source transition, got {other:?}"),
    }
}

pub fn applied_merge(
    outcome: MergeTransitionOutcome,
) -> coding_agent_store::MergeTransitionReceipt {
    match outcome {
        MergeTransitionOutcome::Applied(receipt) => receipt,
        other => panic!("expected applied merge transition, got {other:?}"),
    }
}

pub fn applied_cleanup(
    outcome: CleanupTransitionOutcome,
) -> coding_agent_store::CleanupTransitionReceipt {
    match outcome {
        CleanupTransitionOutcome::Applied(receipt) => receipt,
        other => panic!("expected applied cleanup transition, got {other:?}"),
    }
}
