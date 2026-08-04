use super::*;

#[test]
fn activity_updates_are_serialized_and_end_with_idle() {
    let mut synchronizer = StorageActivitySynchronizer::new();
    let queued = StorageActivity::new(1, 0);
    let active = StorageActivity::new(0, 1);
    let idle = StorageActivity::default();

    let first = synchronizer.request(queued).unwrap().unwrap();
    assert_eq!(first.sequence, 1);
    assert_eq!(first.activity, queued);
    assert_eq!(synchronizer.request(active).unwrap(), None);

    let second = synchronizer.complete(first, Ok(())).unwrap().unwrap();
    assert_eq!(second.sequence, 2);
    assert_eq!(second.activity, active);
    assert_eq!(synchronizer.request(idle).unwrap(), None);

    let third = synchronizer.complete(second, Ok(())).unwrap().unwrap();
    assert_eq!(third.sequence, 3);
    assert_eq!(third.activity, idle);
    assert_eq!(synchronizer.complete(third, Ok(())).unwrap(), None);
    assert!(!synchronizer.has_in_flight());
    assert_eq!(synchronizer.applied, idle);
}

#[test]
fn stale_completion_is_rejected_without_reordering_the_owner() {
    let mut synchronizer = StorageActivitySynchronizer::new();
    let first = synchronizer
        .request(StorageActivity::new(1, 0))
        .unwrap()
        .unwrap();
    let stale = StorageActivitySubmission {
        sequence: first.sequence + 1,
        activity: first.activity,
    };

    assert_eq!(
        synchronizer.complete(stale, Ok(())),
        Err(StorageActivitySyncError::CompletionMismatch)
    );
    assert!(synchronizer.has_in_flight());
    assert_eq!(synchronizer.complete(first, Ok(())).unwrap(), None);
}

#[test]
fn monitor_failure_clears_the_pending_pipeline_for_fail_closed_handling() {
    let mut synchronizer = StorageActivitySynchronizer::new();
    let first = synchronizer
        .request(StorageActivity::new(1, 0))
        .unwrap()
        .unwrap();
    synchronizer.request(StorageActivity::new(0, 1)).unwrap();

    assert_eq!(
        synchronizer.complete(first, Err(StorageMonitorError::Unavailable)),
        Err(StorageActivitySyncError::Monitor(
            StorageMonitorError::Unavailable
        ))
    );
    assert!(!synchronizer.has_in_flight());
    assert_eq!(synchronizer.pending, None);
}
