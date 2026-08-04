use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::Arc;

use crate::{
    ApiResult, SchedulerStateChunkControl, SchedulerStateControl, SchedulerStateDto,
    SchedulerStateFrames,
};

pub(super) enum SchedulerFrame {
    Manifest(SchedulerStateControl),
    Chunk(SchedulerStateChunkControl),
}

pub(super) struct SchedulerDelivery {
    highest_seen: Option<Arc<SchedulerStateDto>>,
    candidate: Option<Arc<SchedulerStateDto>>,
    active: VecDeque<SchedulerFrame>,
    incomplete: bool,
}

impl SchedulerDelivery {
    pub(super) const fn new() -> Self {
        Self {
            highest_seen: None,
            candidate: None,
            active: VecDeque::new(),
            incomplete: false,
        }
    }

    pub(super) fn observe(
        &mut self,
        observed: ApiResult<Arc<SchedulerStateDto>>,
    ) -> Result<(), SchedulerDeliveryError> {
        let snapshot = observed.map_err(|_| SchedulerDeliveryError)?;
        let Some(highest) = self.highest_seen.as_ref() else {
            self.accept(snapshot);
            return Ok(());
        };

        if snapshot.server_instance_id != highest.server_instance_id {
            return Err(SchedulerDeliveryError);
        }

        match snapshot.generation.cmp(&highest.generation) {
            Ordering::Less => Ok(()),
            Ordering::Equal if snapshot == *highest => Ok(()),
            Ordering::Equal => Err(SchedulerDeliveryError),
            Ordering::Greater => {
                self.accept(snapshot);
                Ok(())
            }
        }
    }

    pub(super) fn abort_active(&mut self) {
        self.active.clear();
    }

    pub(super) fn acknowledge_frame_yielded(&mut self) {
        if self.active.is_empty() {
            self.incomplete = false;
        }
    }

    pub(super) fn needs_reset(&self) -> bool {
        self.incomplete && self.active.is_empty() && self.candidate.is_none()
    }

    pub(super) fn has_active(&self) -> bool {
        !self.active.is_empty()
    }

    pub(super) fn has_pending(&self) -> bool {
        self.candidate.is_some() || self.has_active() || self.incomplete
    }

    pub(super) fn next_frame(
        &mut self,
        applied_membership: i64,
        applied_service: u64,
    ) -> Result<Option<SchedulerFrame>, SchedulerDeliveryError> {
        if let Some(frame) = self.active.pop_front() {
            self.incomplete = true;
            return Ok(Some(frame));
        }

        let Some(candidate) = self.candidate.as_ref() else {
            return Ok(None);
        };
        let candidate_membership =
            i64::try_from(candidate.as_of_event_id).map_err(|_| SchedulerDeliveryError)?;

        if candidate_membership < applied_membership
            || candidate.service_state_generation < applied_service
        {
            self.candidate = None;
            return Ok(None);
        }
        if candidate_membership != applied_membership
            || candidate.service_state_generation != applied_service
        {
            return Ok(None);
        }

        let candidate = self.candidate.take().expect("checked candidate");
        let (manifest, chunks) = SchedulerStateFrames::try_from_snapshot(&candidate)
            .map_err(|_| SchedulerDeliveryError)?
            .into_parts();
        self.active.push_back(SchedulerFrame::Manifest(manifest));
        self.active
            .extend(chunks.into_iter().map(SchedulerFrame::Chunk));
        let frame = self.active.pop_front();
        self.incomplete = frame.is_some();
        Ok(frame)
    }

    fn accept(&mut self, snapshot: Arc<SchedulerStateDto>) {
        self.highest_seen = Some(snapshot.clone());
        self.candidate = Some(snapshot);
        self.active.clear();
        // Merely observing a higher generation does not repair a partial group already visible
        // to the client. Keep that marker until the replacement manifest is actually yielded.
    }
}

#[derive(Clone, Copy)]
pub(super) struct SchedulerDeliveryError;
