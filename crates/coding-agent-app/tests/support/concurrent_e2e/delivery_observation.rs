use std::future::Future;
use std::time::Duration;

use coding_agent_domain::TaskId;
use coding_agent_store::{
    CleanupOperationRecord, CleanupOperationState, DeliveryOperationId, DeliveryOperationSnapshot,
    DeliverySourceRecord, DeliverySourceState, DeliveryVersion, MergeOperationRecord,
    MergeOperationState, Store,
};

use super::{DELIVERY_NO_PROGRESS_TIMEOUT, DELIVERY_OBSERVATION_POLL_INTERVAL};

// Normal fixture paths use only a handful of states. This deliberately loose
// test safety cap catches pathological durable churn across workers; it is not
// a production operation-lifecycle bound.
const TEST_DELIVERY_PROGRESS_STATE_LIMIT: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeliveryProgress<P> {
    Pending(P),
    Complete,
    Terminal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeliveryProgressError<T> {
    Terminal(T),
    NoProgress { last: Option<T> },
    ProgressStateLimit { observed_states: usize, last: T },
}

pub(crate) async fn wait_for_delivery_progress<T, P, Observe, ObserveFuture, Classify>(
    no_progress_timeout: Duration,
    poll_interval: Duration,
    max_progress_states: usize,
    mut observe: Observe,
    mut classify: Classify,
) -> Result<T, DeliveryProgressError<T>>
where
    P: Eq,
    T: Clone,
    Observe: FnMut() -> ObserveFuture,
    ObserveFuture: Future<Output = T>,
    Classify: FnMut(&T) -> DeliveryProgress<P>,
{
    assert!(
        max_progress_states > 0,
        "delivery progress cap must be positive"
    );

    let mut last_progress = None;
    let mut last_observation = None;
    let mut observed_states = 0usize;
    let mut deadline = tokio::time::Instant::now() + no_progress_timeout;
    loop {
        let observation = match tokio::time::timeout_at(deadline, observe()).await {
            Ok(observation) => observation,
            Err(_) => {
                return Err(DeliveryProgressError::NoProgress {
                    last: last_observation,
                });
            }
        };
        let progress = match classify(&observation) {
            DeliveryProgress::Complete => return Ok(observation),
            DeliveryProgress::Terminal => {
                return Err(DeliveryProgressError::Terminal(observation));
            }
            DeliveryProgress::Pending(progress) => progress,
        };

        if last_progress.as_ref() != Some(&progress) {
            observed_states += 1;
            if observed_states > max_progress_states {
                return Err(DeliveryProgressError::ProgressStateLimit {
                    observed_states,
                    last: observation,
                });
            }
            last_progress = Some(progress);
            last_observation = Some(observation.clone());
            deadline = tokio::time::Instant::now() + no_progress_timeout;
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(DeliveryProgressError::NoProgress {
                last: Some(observation),
            });
        }
        tokio::time::sleep_until(std::cmp::min(deadline, now + poll_interval)).await;
    }
}

pub(super) async fn wait_for_merge(
    store: &Store,
    operation_id: DeliveryOperationId,
) -> MergeOperationRecord {
    let task_id = merge_operation(store, operation_id)
        .await
        .provenance
        .identity
        .task_id();
    match wait_for_delivery_progress(
        DELIVERY_NO_PROGRESS_TIMEOUT,
        DELIVERY_OBSERVATION_POLL_INTERVAL,
        TEST_DELIVERY_PROGRESS_STATE_LIMIT,
        || merge_observation(store, task_id, operation_id),
        classify_merge_observation,
    )
    .await
    {
        Ok(observation) => observation.operation,
        Err(DeliveryProgressError::Terminal(observation)) => panic!(
            "concurrent delivery merge terminated in {:?}: {observation:?}",
            observation.operation.state
        ),
        Err(DeliveryProgressError::NoProgress { last }) => panic!(
            "concurrent delivery merge {operation_id} made no durable progress for \
             {DELIVERY_NO_PROGRESS_TIMEOUT:?}; last observation: {last:?}"
        ),
        Err(DeliveryProgressError::ProgressStateLimit {
            observed_states,
            last,
        }) => panic!(
            "concurrent delivery merge {operation_id} exceeded the \
             {TEST_DELIVERY_PROGRESS_STATE_LIMIT}-state test safety cap after \
             {observed_states} states; last observation: {last:?}"
        ),
    }
}

pub(super) async fn wait_for_cleanup(
    store: &Store,
    operation_id: DeliveryOperationId,
) -> CleanupOperationRecord {
    match wait_for_delivery_progress(
        DELIVERY_NO_PROGRESS_TIMEOUT,
        DELIVERY_OBSERVATION_POLL_INTERVAL,
        TEST_DELIVERY_PROGRESS_STATE_LIMIT,
        || cleanup_operation(store, operation_id),
        classify_cleanup_observation,
    )
    .await
    {
        Ok(operation) => operation,
        Err(DeliveryProgressError::Terminal(operation)) => panic!(
            "concurrent delivery cleanup terminated in {:?}: {operation:?}",
            operation.state
        ),
        Err(DeliveryProgressError::NoProgress { last }) => panic!(
            "concurrent delivery cleanup {operation_id} made no durable progress for \
             {DELIVERY_NO_PROGRESS_TIMEOUT:?}; last observation: {last:?}"
        ),
        Err(DeliveryProgressError::ProgressStateLimit {
            observed_states,
            last,
        }) => panic!(
            "concurrent delivery cleanup {operation_id} exceeded the \
             {TEST_DELIVERY_PROGRESS_STATE_LIMIT}-state test safety cap after \
             {observed_states} states; last observation: {last:?}"
        ),
    }
}

#[derive(Debug, Clone)]
struct MergeObservation {
    operation: MergeOperationRecord,
    source: Option<DeliverySourceRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MergeProgress {
    operation: (MergeOperationState, DeliveryVersion),
    source: Option<(DeliverySourceState, DeliveryVersion)>,
}

async fn merge_observation(
    store: &Store,
    task_id: TaskId,
    operation_id: DeliveryOperationId,
) -> MergeObservation {
    let ownership = store
        .delivery_ownership_snapshot(task_id)
        .await
        .expect("load concurrent delivery ownership")
        .expect("concurrent delivery ownership exists");
    let operation = ownership
        .merge_operations
        .into_iter()
        .find(|operation| operation.operation_id == operation_id)
        .expect("concurrent delivery merge operation exists in ownership");
    MergeObservation {
        operation,
        source: ownership.source,
    }
}

fn classify_merge_observation(observation: &MergeObservation) -> DeliveryProgress<MergeProgress> {
    match observation.operation.state {
        MergeOperationState::Merged => DeliveryProgress::Complete,
        MergeOperationState::Conflict
        | MergeOperationState::Rejected
        | MergeOperationState::Stale
        | MergeOperationState::Superseded
        | MergeOperationState::Failed
        | MergeOperationState::ReconciliationRequired => DeliveryProgress::Terminal,
        _ => DeliveryProgress::Pending(MergeProgress {
            operation: (observation.operation.state, observation.operation.version),
            source: observation
                .source
                .as_ref()
                .map(|source| (source.state, source.version)),
        }),
    }
}

fn classify_cleanup_observation(
    operation: &CleanupOperationRecord,
) -> DeliveryProgress<(CleanupOperationState, DeliveryVersion)> {
    match operation.state {
        CleanupOperationState::Completed => DeliveryProgress::Complete,
        CleanupOperationState::Failed | CleanupOperationState::ReconciliationRequired => {
            DeliveryProgress::Terminal
        }
        _ => DeliveryProgress::Pending((operation.state, operation.version)),
    }
}

async fn merge_operation(store: &Store, operation_id: DeliveryOperationId) -> MergeOperationRecord {
    match store
        .delivery_operation_snapshot(operation_id)
        .await
        .expect("load concurrent delivery merge operation")
        .expect("concurrent delivery merge operation exists")
    {
        DeliveryOperationSnapshot::Merge(operation) => *operation,
        DeliveryOperationSnapshot::Cleanup(_) => panic!("expected concurrent merge operation"),
    }
}

async fn cleanup_operation(
    store: &Store,
    operation_id: DeliveryOperationId,
) -> CleanupOperationRecord {
    match store
        .delivery_operation_snapshot(operation_id)
        .await
        .expect("load concurrent delivery cleanup operation")
        .expect("concurrent delivery cleanup operation exists")
    {
        DeliveryOperationSnapshot::Cleanup(operation) => *operation,
        DeliveryOperationSnapshot::Merge(_) => panic!("expected concurrent cleanup operation"),
    }
}
