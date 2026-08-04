use std::sync::Arc;

use coding_agent_domain::EventCursor;
use tokio::sync::watch;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerProjectionCandidate<S> {
    public_state: S,
    as_of_event_id: EventCursor,
    service_state_generation: u64,
}

impl<S> SchedulerProjectionCandidate<S> {
    pub const fn new(
        public_state: S,
        as_of_event_id: EventCursor,
        service_state_generation: u64,
    ) -> Self {
        Self {
            public_state,
            as_of_event_id,
            service_state_generation,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerProjectionSnapshot<S> {
    generation: u64,
    as_of_event_id: EventCursor,
    service_state_generation: u64,
    public_state: S,
}

impl<S> SchedulerProjectionSnapshot<S> {
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn as_of_event_id(&self) -> EventCursor {
        self.as_of_event_id
    }

    pub const fn service_state_generation(&self) -> u64 {
        self.service_state_generation
    }

    pub const fn public_state(&self) -> &S {
        &self.public_state
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerPublishOutcome<S> {
    Unchanged(Arc<SchedulerProjectionSnapshot<S>>),
    Published(Arc<SchedulerProjectionSnapshot<S>>),
}

impl<S> SchedulerPublishOutcome<S> {
    pub fn snapshot(&self) -> &Arc<SchedulerProjectionSnapshot<S>> {
        match self {
            Self::Unchanged(snapshot) | Self::Published(snapshot) => snapshot,
        }
    }

    pub const fn changed(&self) -> bool {
        matches!(self, Self::Published(_))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SchedulerPublisherError {
    #[error("scheduler membership watermark cannot move backwards")]
    MembershipWatermarkRegression,
    #[error("scheduler service-state generation cannot move backwards")]
    ServiceGenerationRegression,
    #[error("scheduler generation is exhausted")]
    GenerationExhausted,
}

/// Latest-only immutable scheduler projection publisher.
///
/// Callers may stage several recomputations and flush once. Only the final
/// staged public value is compared with the published snapshot, so coalesced
/// intermediate states do not consume generations.
pub struct SchedulerStatePublisher<S> {
    sender: watch::Sender<Arc<SchedulerProjectionSnapshot<S>>>,
    pending: Option<SchedulerProjectionCandidate<S>>,
}

#[derive(Clone)]
pub(crate) struct SchedulerStateReader<S> {
    receiver: watch::Receiver<Arc<SchedulerProjectionSnapshot<S>>>,
}

impl<S> SchedulerStateReader<S> {
    pub(crate) fn current(&self) -> Arc<SchedulerProjectionSnapshot<S>> {
        self.receiver.borrow().clone()
    }

    pub(crate) fn watch(&self) -> SchedulerStateWatch<S> {
        SchedulerStateWatch {
            receiver: self.receiver.clone(),
        }
    }
}

pub(crate) struct SchedulerStateWatch<S> {
    receiver: watch::Receiver<Arc<SchedulerProjectionSnapshot<S>>>,
}

impl<S> SchedulerStateWatch<S> {
    pub(crate) fn current(&self) -> Arc<SchedulerProjectionSnapshot<S>> {
        self.receiver.borrow().clone()
    }

    pub(crate) async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.receiver.changed().await
    }
}

impl<S> SchedulerStatePublisher<S>
where
    S: PartialEq,
{
    pub fn new(initial: SchedulerProjectionCandidate<S>) -> Self {
        let snapshot = Arc::new(SchedulerProjectionSnapshot {
            generation: 0,
            as_of_event_id: initial.as_of_event_id,
            service_state_generation: initial.service_state_generation,
            public_state: initial.public_state,
        });
        let (sender, _) = watch::channel(snapshot);
        Self {
            sender,
            pending: None,
        }
    }

    pub fn current(&self) -> Arc<SchedulerProjectionSnapshot<S>> {
        self.sender.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<Arc<SchedulerProjectionSnapshot<S>>> {
        self.sender.subscribe()
    }

    pub(crate) fn reader(&self) -> SchedulerStateReader<S> {
        SchedulerStateReader {
            receiver: self.sender.subscribe(),
        }
    }

    pub fn stage(
        &mut self,
        candidate: SchedulerProjectionCandidate<S>,
    ) -> Result<(), SchedulerPublisherError> {
        let current = self.sender.borrow();
        let minimum_membership = self
            .pending
            .as_ref()
            .map_or(current.as_of_event_id, |pending| pending.as_of_event_id);
        let minimum_service = self
            .pending
            .as_ref()
            .map_or(current.service_state_generation, |pending| {
                pending.service_state_generation
            });
        if candidate.as_of_event_id < minimum_membership {
            return Err(SchedulerPublisherError::MembershipWatermarkRegression);
        }
        if candidate.service_state_generation < minimum_service {
            return Err(SchedulerPublisherError::ServiceGenerationRegression);
        }
        drop(current);
        self.pending = Some(candidate);
        Ok(())
    }

    pub fn flush(&mut self) -> Result<SchedulerPublishOutcome<S>, SchedulerPublisherError> {
        let current = self.sender.borrow().clone();
        let Some(candidate) = self.pending.take() else {
            return Ok(SchedulerPublishOutcome::Unchanged(current));
        };
        if candidate.as_of_event_id == current.as_of_event_id
            && candidate.service_state_generation == current.service_state_generation
            && candidate.public_state == current.public_state
        {
            return Ok(SchedulerPublishOutcome::Unchanged(current));
        }

        let generation = current
            .generation
            .checked_add(1)
            .ok_or(SchedulerPublisherError::GenerationExhausted)?;
        let next = Arc::new(SchedulerProjectionSnapshot {
            generation,
            as_of_event_id: candidate.as_of_event_id,
            service_state_generation: candidate.service_state_generation,
            public_state: candidate.public_state,
        });
        self.sender.send_replace(next.clone());
        Ok(SchedulerPublishOutcome::Published(next))
    }
}
