use std::sync::Arc;

use coding_agent_domain::EventCursor;
use coding_agent_store::SchedulerBootstrapSnapshot;
use uuid::Uuid;

use crate::scheduler::{
    SchedulerProjectionCandidate, SchedulerProjectionSnapshot, SchedulerPublisherError,
    SchedulerStatePublisher, SchedulerStateReader, SchedulerStoreState,
};
use crate::scheduler_api_projection::{
    SchedulerProjectionBuildError, SchedulerPublicLimits, SchedulerRuntimeProjection,
};

/// Actor-owned bridge from one consistent Store read to one scheduler
/// generation.
///
/// The bridge intentionally accepts no TaskManager-local membership. A caller
/// must supply the complete Store snapshot, so provisional permits and active
/// ownership can never be combined with a newer durable watermark.
pub(super) struct SchedulerProjectionBridge {
    server_instance_id: Uuid,
    publisher: SchedulerStatePublisher<SchedulerStoreState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(super) enum SchedulerProjectionPublishError {
    #[error("scheduler Store snapshot is older than the published membership watermark")]
    StaleSnapshot,
    #[error("scheduler Store snapshot has an invalid event watermark")]
    InvalidSnapshot,
    #[error(transparent)]
    Publisher(#[from] SchedulerPublisherError),
    #[error("scheduler Store projection did not publish the exact supplied snapshot")]
    InexactPublication,
    #[error("scheduler storage activity synchronization could not be scheduled")]
    StorageActivitySync,
    #[error(transparent)]
    Build(#[from] SchedulerProjectionBuildError),
}

impl SchedulerProjectionBridge {
    #[cfg(test)]
    pub(super) fn new(server_instance_id: Uuid, service_state_generation: u64) -> Self {
        let limits = SchedulerPublicLimits::compatibility_defaults(
            crate::SchedulerConcurrencyLimits::try_new(4, 4)
                .expect("fixed scheduler projection test limits are valid"),
        );
        Self::new_complete(server_instance_id, service_state_generation, limits, false)
    }

    pub(super) fn new_complete(
        server_instance_id: Uuid,
        service_state_generation: u64,
        limits: SchedulerPublicLimits,
        service_paused: bool,
    ) -> Self {
        Self {
            server_instance_id,
            publisher: SchedulerStatePublisher::new(SchedulerProjectionCandidate::new(
                SchedulerStoreState::empty(server_instance_id, limits, service_paused),
                EventCursor::ZERO,
                service_state_generation,
            )),
        }
    }

    pub(super) fn reader(&self) -> SchedulerStateReader<SchedulerStoreState> {
        self.publisher.reader()
    }

    pub(super) fn current(&self) -> Arc<SchedulerProjectionSnapshot<SchedulerStoreState>> {
        self.publisher.current()
    }

    #[cfg(test)]
    pub(super) fn publish(
        &mut self,
        snapshot: &SchedulerBootstrapSnapshot,
        service_state_generation: u64,
    ) -> Result<
        Arc<SchedulerProjectionSnapshot<SchedulerStoreState>>,
        SchedulerProjectionPublishError,
    > {
        self.publish_state(
            snapshot,
            service_state_generation,
            SchedulerStoreState::from_store_snapshot(self.server_instance_id, snapshot),
        )
    }

    pub(super) fn publish_complete(
        &mut self,
        snapshot: &SchedulerBootstrapSnapshot,
        service_state_generation: u64,
        limits: SchedulerPublicLimits,
        runtime: SchedulerRuntimeProjection<'_>,
    ) -> Result<
        Arc<SchedulerProjectionSnapshot<SchedulerStoreState>>,
        SchedulerProjectionPublishError,
    > {
        let state = SchedulerStoreState::from_complete_snapshot(
            self.server_instance_id,
            limits,
            snapshot,
            runtime,
        )?;
        self.publish_state(snapshot, service_state_generation, state)
    }

    fn publish_state(
        &mut self,
        snapshot: &SchedulerBootstrapSnapshot,
        service_state_generation: u64,
        state: SchedulerStoreState,
    ) -> Result<
        Arc<SchedulerProjectionSnapshot<SchedulerStoreState>>,
        SchedulerProjectionPublishError,
    > {
        if snapshot.membership_event_id > snapshot.latest_event_id {
            return Err(SchedulerProjectionPublishError::InvalidSnapshot);
        }
        if snapshot.membership_event_id < self.publisher.current().as_of_event_id() {
            return Err(SchedulerProjectionPublishError::StaleSnapshot);
        }

        self.publisher.stage(SchedulerProjectionCandidate::new(
            state,
            snapshot.membership_event_id,
            service_state_generation,
        ))?;
        let published = self.publisher.flush()?.snapshot().clone();
        if published.as_of_event_id() != snapshot.membership_event_id
            || published.service_state_generation() != service_state_generation
            || published.public_state().server_instance_id() != self.server_instance_id
            || !published.public_state().exactly_matches(snapshot)
        {
            return Err(SchedulerProjectionPublishError::InexactPublication);
        }
        Ok(published)
    }
}

#[cfg(test)]
mod tests {
    use coding_agent_domain::{
        CanonicalPath, ClientRequestId, DeliveryReadiness, EventId, Repository, RepositoryId, Task,
        TaskId, TaskStatus, UtcTimestamp,
    };
    use coding_agent_store::{SchedulerBootstrapSnapshot, StopIntentKind, StopIntentReceipt};
    use time::OffsetDateTime;

    use super::*;

    #[test]
    fn bridge_preserves_the_primary_server_instance_id() {
        let instance_id = Uuid::new_v4();
        let bridge = SchedulerProjectionBridge::new(instance_id, 7);

        assert_eq!(
            bridge.current().public_state().server_instance_id(),
            instance_id
        );
        assert_eq!(bridge.reader().current().service_state_generation(), 7);
    }

    #[test]
    fn first_store_scan_publishes_one_complete_exact_projection() {
        let instance_id = Uuid::new_v4();
        let mut bridge = SchedulerProjectionBridge::new(instance_id, 3);
        let queued = queued_snapshot();

        let published = bridge
            .publish(&queued, 3)
            .expect("publish the first exact Store scan");

        assert_eq!(published.generation(), 1);
        assert_eq!(published.as_of_event_id(), queued.membership_event_id);
        assert_eq!(published.service_state_generation(), 3);
        assert!(published.public_state().exactly_matches(&queued));
    }

    #[test]
    fn synthetic_initial_state_never_matches_an_empty_store_snapshot() {
        let instance_id = Uuid::new_v4();
        let mut bridge = SchedulerProjectionBridge::new(instance_id, 0);
        let empty = SchedulerBootstrapSnapshot {
            repositories: Vec::new(),
            tasks: Vec::new(),
            running_stop_intents: Vec::new(),
            latest_event_id: EventCursor::ZERO,
            membership_event_id: EventCursor::ZERO,
        };

        assert!(!bridge.current().public_state().exactly_matches(&empty));

        let published = bridge
            .publish(&empty, 0)
            .expect("publish the first exact empty Store snapshot");
        assert_eq!(published.generation(), 1);
        assert!(published.public_state().exactly_matches(&empty));
    }

    #[test]
    fn same_membership_watermark_publishes_an_exact_set_only_change() {
        let instance_id = Uuid::new_v4();
        let mut bridge = SchedulerProjectionBridge::new(instance_id, 1);
        let running = running_snapshot();
        let first = bridge.publish(&running, 1).expect("publish Running set");
        let task = &running.tasks[0];
        let with_intent = SchedulerBootstrapSnapshot {
            running_stop_intents: vec![StopIntentReceipt {
                task_id: task.id,
                repository_id: task.repository_id,
                attempt: task.attempt,
                kind: StopIntentKind::UserCancelled,
                requested_at: timestamp(2),
            }],
            ..running.clone()
        };

        let second = bridge
            .publish(&with_intent, 1)
            .expect("publish exact stop-intent set");

        assert_eq!(second.as_of_event_id(), first.as_of_event_id());
        assert_eq!(second.generation(), first.generation() + 1);
        assert!(second.public_state().exactly_matches(&with_intent));
    }

    #[test]
    fn provisional_claim_cannot_change_the_store_projection() {
        let instance_id = Uuid::new_v4();
        let mut bridge = SchedulerProjectionBridge::new(instance_id, 1);
        let queued = queued_snapshot();
        let first = bridge
            .publish(&queued, 1)
            .expect("publish queued Store state");

        // A provisional claim exists only in TaskManager ownership. Replaying
        // the unchanged Store snapshot therefore cannot manufacture Running
        // membership or consume a scheduler generation.
        let second = bridge
            .publish(&queued, 1)
            .expect("re-publish unchanged Store state");

        assert_eq!(second.generation(), first.generation());
        assert!(second.public_state().exactly_matches(&queued));
    }

    #[test]
    fn terminal_watermark_is_published_only_with_the_terminal_store_set() {
        let instance_id = Uuid::new_v4();
        let mut bridge = SchedulerProjectionBridge::new(instance_id, 1);
        let running = running_snapshot();
        bridge.publish(&running, 1).expect("publish Running set");
        let terminal = terminal_snapshot(&running);

        let published = bridge
            .publish(&terminal, 1)
            .expect("publish terminal Store set");

        assert_eq!(published.as_of_event_id(), terminal.membership_event_id);
        assert!(published.public_state().exactly_matches(&terminal));
    }

    fn queued_snapshot() -> SchedulerBootstrapSnapshot {
        let repository = repository();
        let task = task(&repository, TaskStatus::Queued, 1);
        SchedulerBootstrapSnapshot {
            repositories: vec![repository],
            tasks: vec![task],
            running_stop_intents: Vec::new(),
            latest_event_id: cursor(1),
            membership_event_id: cursor(1),
        }
    }

    fn running_snapshot() -> SchedulerBootstrapSnapshot {
        let repository = repository();
        let task = task(&repository, TaskStatus::Running, 1);
        SchedulerBootstrapSnapshot {
            repositories: vec![repository],
            tasks: vec![task],
            running_stop_intents: Vec::new(),
            latest_event_id: cursor(1),
            membership_event_id: cursor(1),
        }
    }

    fn terminal_snapshot(running: &SchedulerBootstrapSnapshot) -> SchedulerBootstrapSnapshot {
        let mut task = running.tasks[0].clone();
        task.status = TaskStatus::Completed;
        task.delivery_readiness = DeliveryReadiness::Unreviewed;
        task.finished_at = Some(timestamp(3));
        task.last_event_id = EventId::new(2).expect("positive terminal event ID");
        SchedulerBootstrapSnapshot {
            repositories: running.repositories.clone(),
            tasks: vec![task],
            running_stop_intents: Vec::new(),
            latest_event_id: cursor(2),
            membership_event_id: cursor(2),
        }
    }

    fn repository() -> Repository {
        let root = std::env::current_dir()
            .expect("read current directory")
            .canonicalize()
            .expect("canonicalize current directory");
        let root = CanonicalPath::try_from_canonical(root).expect("canonical test path");
        Repository {
            id: RepositoryId::new(),
            selected_path: root.clone(),
            display_name: "scheduler projection".to_owned(),
            git_root: root.clone(),
            cargo_workspace_root: root,
            created_at: timestamp(0),
            last_opened_at: timestamp(0),
        }
    }

    fn task(repository: &Repository, status: TaskStatus, event_id: i64) -> Task {
        let started_at = (status != TaskStatus::Queued).then_some(timestamp(1));
        Task {
            id: TaskId::new(),
            client_request_id: ClientRequestId::new(),
            repository_id: repository.id,
            prompt: "scheduler projection".to_owned(),
            status,
            delivery_readiness: DeliveryReadiness::Unreviewed,
            attempt: 1,
            retry_of: None,
            created_at: timestamp(0),
            started_at,
            finished_at: None,
            last_event_id: EventId::new(event_id).expect("positive event ID"),
            failure: None,
        }
    }

    fn timestamp(seconds: i64) -> UtcTimestamp {
        UtcTimestamp::new(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(seconds))
            .expect("valid test timestamp")
    }

    fn cursor(value: i64) -> EventCursor {
        EventCursor::new(value).expect("nonnegative event cursor")
    }
}
