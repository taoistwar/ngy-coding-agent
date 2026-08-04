use super::*;

#[test]
fn scheduler_storage_notification_is_retained_before_actor_bind() {
    let signals = TaskManagerStorageSignals::new();
    let notification = SchedulerStorageNotification::new(
        StorageState::Pressure,
        StorageState::Pressure,
        StorageState::Normal,
        Vec::new(),
    );

    signals.notify_storage_classification(notification.clone());

    assert_eq!(signals.latest_scheduler_storage(), (1, Some(notification)));
}
