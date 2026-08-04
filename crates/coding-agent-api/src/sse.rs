#[path = "sse/scheduler.rs"]
mod scheduler;

use std::collections::{BTreeMap, VecDeque};
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::response::sse::Event;
use futures_util::{FutureExt as _, Stream, StreamExt as _};
use tokio::time::{Instant, Interval, MissedTickBehavior, interval_at};

use self::scheduler::{SchedulerDelivery, SchedulerFrame};
use crate::{
    ApiResult, LiveEventItem, LiveEventStream, SchedulerStateDto, SchedulerStateStream,
    ServiceStateControl, ServiceStateDto, ServiceStateStream, SseBackend, StreamResetControl,
    TaskEventDto,
};

const PAGE_SIZE: usize = 256;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const MAX_READY_SERVICE_UPDATES: usize = 64;
const MAX_READY_SCHEDULER_UPDATES: usize = 64;
const READY_LIVE_BATCH_SIZE: usize = 256;
const MAX_BUFFERED_LIVE_EVENTS: usize = 4_096;

pub(crate) type SseEventStream =
    Pin<Box<dyn Stream<Item = Result<Event, Infallible>> + Send + 'static>>;

pub(crate) fn connect(backend: Arc<dyn SseBackend>, after: i64) -> SseEventStream {
    // All three subscriptions are eager. Their buffers therefore cover every transition that
    // races the lazy current-state and Store reads performed after the response starts polling.
    let live = backend.subscribe_live();
    let service = backend.subscribe_service_state();
    let scheduler = backend.subscribe_scheduler_state();
    let connection = Connection::new(backend, live, service, scheduler, after);

    Box::pin(futures_util::stream::unfold(
        connection,
        |mut connection| async move {
            let event = connection.next_event().await?;
            Some((Ok(event), connection))
        },
    ))
}

struct Connection {
    backend: Arc<dyn SseBackend>,
    live: LiveEventStream,
    service: ServiceStateStream,
    scheduler: SchedulerStateStream,
    heartbeat: Interval,
    phase: Phase,
    after: i64,
    last: i64,
    latest_known: i64,
    applied_membership: i64,
    applied_service: u64,
    latest_service: Option<u64>,
    deferred: Option<DeferredAfterYield>,
    pending_read: Option<PendingRead>,
    reset_requested: Option<i64>,
    replay_high: i64,
    replay_destination: ReplayDestination,
    page: VecDeque<TaskEventDto>,
    buffered: BTreeMap<i64, TaskEventDto>,
    pending_refills: usize,
    live_closed: bool,
    service_closed: bool,
    scheduler_closed: bool,
    scheduler_saturated: bool,
    scheduler_gate_ready: bool,
    scheduler_yielded: bool,
    service_yielded: bool,
    phase_was_preferred: bool,
    scheduler_delivery: SchedulerDelivery,
}

impl Connection {
    fn new(
        backend: Arc<dyn SseBackend>,
        live: LiveEventStream,
        service: ServiceStateStream,
        scheduler: SchedulerStateStream,
        after: i64,
    ) -> Self {
        let mut heartbeat = interval_at(Instant::now() + HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        Self {
            backend,
            live,
            service,
            scheduler,
            heartbeat,
            phase: Phase::CurrentService,
            after,
            last: after,
            latest_known: after,
            applied_membership: 0,
            applied_service: 0,
            latest_service: None,
            deferred: None,
            pending_read: None,
            reset_requested: None,
            replay_high: after,
            replay_destination: ReplayDestination::InitialJoin,
            page: VecDeque::new(),
            buffered: BTreeMap::new(),
            pending_refills: 0,
            live_closed: false,
            service_closed: false,
            scheduler_closed: false,
            scheduler_saturated: false,
            scheduler_gate_ready: false,
            scheduler_yielded: false,
            service_yielded: false,
            phase_was_preferred: false,
            scheduler_delivery: SchedulerDelivery::new(),
        }
    }

    async fn next_event(&mut self) -> Option<Event> {
        self.apply_deferred();
        loop {
            if self.phase == Phase::CurrentService {
                return self.read_current_service().await;
            }
            match self.poll_immediate().await {
                Immediate::Event(event) => return Some(event),
                Immediate::Closed => return None,
                Immediate::Retry => continue,
                Immediate::Proceed => {}
            }
            if let Some(event) = self.advance_phase().await {
                return Some(event);
            }
        }
    }

    async fn poll_immediate(&mut self) -> Immediate {
        if let Some(latest) = self.reset_requested.take() {
            self.phase = Phase::Closed;
            return reset_event(latest).map_or(Immediate::Closed, Immediate::Event);
        }
        if self.phase == Phase::Closed {
            return Immediate::Closed;
        }
        if self.scheduler_yielded {
            self.scheduler_yielded = false;
            self.scheduler_delivery.acknowledge_frame_yielded();
            tokio::task::yield_now().await;
        }
        let service_was_yielded = std::mem::take(&mut self.service_yielded);
        if service_was_yielded {
            tokio::task::yield_now().await;
        }
        if self.scheduler_saturated {
            tokio::task::yield_now().await;
            if self.heartbeat.tick().now_or_never().is_some() {
                return Immediate::Event(heartbeat_event());
            }
            if !service_was_yielded && let Some(event) = self.take_ready_service() {
                return Immediate::Event(event);
            }
            if self.drain_ready_scheduler().is_err() {
                self.request_reset();
                return Immediate::Retry;
            }
        }

        if !service_was_yielded && let Some(event) = self.take_ready_service() {
            return Immediate::Event(event);
        }
        if !self.scheduler_saturated && self.drain_ready_scheduler().is_err() {
            self.request_reset();
            return Immediate::Retry;
        }
        if self.scheduler_delivery.needs_reset() {
            self.request_reset();
            return Immediate::Retry;
        }
        if self.heartbeat.tick().now_or_never().is_some() {
            return Immediate::Event(heartbeat_event());
        }

        self.ensure_pending_read();
        if let Some(read) = self.take_ready_read() {
            self.handle_read(read);
            return Immediate::Retry;
        }
        if self.scheduler_delivery.has_active()
            && let Some(event) = self.take_phase_event_before_scheduler()
        {
            return Immediate::Event(event);
        }
        if self.phase == Phase::Join && self.pending_refills > 0 {
            return Immediate::Proceed;
        }
        if let Some(event) = self.take_scheduler_frame() {
            return Immediate::Event(event);
        }
        if self.scheduler_delivery.needs_reset() {
            self.request_reset();
            return Immediate::Retry;
        }
        Immediate::Proceed
    }

    async fn advance_phase(&mut self) -> Option<Event> {
        match self.phase {
            Phase::CurrentService => unreachable!("handled before control polling"),
            Phase::CurrentScheduler
            | Phase::InitialMembership
            | Phase::InitialHigh
            | Phase::LagHigh => {
                let activity = self.wait_for_activity().await;
                self.handle_activity(activity)
            }
            Phase::Replay => self.advance_replay().await,
            Phase::Join => self.step_join(),
            Phase::Live => {
                let activity = self.wait_for_live_activity().await;
                self.handle_activity(activity)
            }
            Phase::Closed => None,
        }
    }

    async fn advance_replay(&mut self) -> Option<Event> {
        if let Some(event) = self.page.pop_front() {
            return self.prepare_persisted(event);
        }
        if self.last >= self.replay_high {
            self.finish_replay();
            return None;
        }
        let activity = self.wait_for_activity().await;
        self.handle_activity(activity)
    }

    fn handle_activity(&mut self, activity: Activity) -> Option<Event> {
        match activity {
            Activity::Read(read) => self.handle_read(read),
            Activity::Service(first) => return self.handle_service_input(first),
            Activity::Scheduler(first) => {
                if self.handle_scheduler_batch(first).is_err() {
                    self.request_reset();
                }
            }
            Activity::Heartbeat => return Some(heartbeat_event()),
            Activity::Live(next) => {
                return self.handle_live_input(next.map(|item| *item));
            }
        }
        None
    }

    fn apply_deferred(&mut self) {
        let Some(deferred) = self.deferred.take() else {
            return;
        };
        match deferred {
            DeferredAfterYield::Service(generation) => {
                self.applied_service = generation;
                self.scheduler_delivery.abort_active();
                self.phase_was_preferred = false;
            }
            DeferredAfterYield::Persisted { id, membership } => {
                self.last = id;
                self.latest_known = self.latest_known.max(id);
                if membership {
                    self.applied_membership = id;
                    self.scheduler_delivery.abort_active();
                    self.phase_was_preferred = false;
                }
            }
        }
    }

    async fn read_current_service(&mut self) -> Option<Event> {
        let current = match self.backend.current_service_state().await {
            Ok(current) => current,
            Err(_) => {
                log_backend_failure("current_service_state");
                self.phase = Phase::Closed;
                return None;
            }
        };
        self.latest_service = Some(current.generation);
        self.deferred = Some(DeferredAfterYield::Service(current.generation));
        self.phase = if current.state == ServiceStateDto::Quiescing {
            Phase::Closed
        } else {
            Phase::CurrentScheduler
        };
        match control_event("service.state", &current) {
            Ok(event) => {
                self.service_yielded = true;
                Some(event)
            }
            Err(()) => {
                self.request_reset();
                self.next_reset_event()
            }
        }
    }

    fn take_ready_service(&mut self) -> Option<Event> {
        if self.service_closed {
            return None;
        }
        let first = self.service.next().now_or_never()?;
        self.handle_service_input(first)
    }

    fn handle_service_input(&mut self, first: Option<ServiceStateControl>) -> Option<Event> {
        let newest = coalesce_service(first, &mut self.service, &mut self.service_closed)?;
        if self
            .latest_service
            .is_some_and(|generation| newest.generation <= generation)
        {
            return None;
        }

        self.latest_service = Some(newest.generation);
        self.scheduler_delivery.abort_active();
        self.phase_was_preferred = false;
        self.deferred = Some(DeferredAfterYield::Service(newest.generation));
        if newest.state == ServiceStateDto::Quiescing {
            self.phase = Phase::Closed;
            self.pending_read = None;
        }
        match control_event("service.state", &newest) {
            Ok(event) => {
                self.service_yielded = true;
                Some(event)
            }
            Err(()) => {
                self.request_reset();
                self.next_reset_event()
            }
        }
    }

    fn drain_ready_scheduler(&mut self) -> Result<(), ()> {
        if self.scheduler_closed {
            self.scheduler_saturated = false;
            return Ok(());
        }
        let Some(first) = self.scheduler.next().now_or_never() else {
            self.scheduler_saturated = false;
            return Ok(());
        };
        self.handle_scheduler_batch(first)
    }

    fn handle_scheduler_batch(
        &mut self,
        first: Option<ApiResult<Arc<SchedulerStateDto>>>,
    ) -> Result<(), ()> {
        let Some(first) = first else {
            self.scheduler_closed = true;
            self.scheduler_saturated = false;
            return Ok(());
        };
        self.scheduler_delivery.observe(first).map_err(|_| ())?;
        let mut observed = 1;
        while observed < MAX_READY_SCHEDULER_UPDATES {
            match self.scheduler.next().now_or_never() {
                Some(Some(next)) => {
                    self.scheduler_delivery.observe(next).map_err(|_| ())?;
                    observed += 1;
                }
                Some(None) => {
                    self.scheduler_closed = true;
                    self.scheduler_saturated = false;
                    return Ok(());
                }
                None => {
                    self.scheduler_saturated = false;
                    return Ok(());
                }
            }
        }
        self.scheduler_saturated = true;
        Ok(())
    }

    fn take_scheduler_frame(&mut self) -> Option<Event> {
        if !self.scheduler_gate_ready || self.scheduler_saturated {
            return None;
        }
        let frame = match self
            .scheduler_delivery
            .next_frame(self.applied_membership, self.applied_service)
        {
            Ok(frame) => frame?,
            Err(_) => {
                self.request_reset();
                return self.next_reset_event();
            }
        };
        let event = match frame {
            SchedulerFrame::Manifest(manifest) => control_event(manifest.event_name(), &manifest),
            SchedulerFrame::Chunk(chunk) => control_event(chunk.event_name(), &chunk),
        };
        match event {
            Ok(event) => {
                self.scheduler_yielded = true;
                self.phase_was_preferred = false;
                Some(event)
            }
            Err(()) => {
                self.request_reset();
                self.next_reset_event()
            }
        }
    }

    fn take_phase_event_before_scheduler(&mut self) -> Option<Event> {
        if self.phase_was_preferred {
            self.phase_was_preferred = false;
            return None;
        }
        let event = match self.phase {
            Phase::Replay => self.page.pop_front(),
            Phase::Join => {
                if self.pending_refills > 0 {
                    None
                } else {
                    self.drain_join_live();
                    if self.pending_refills > 0 {
                        None
                    } else {
                        self.buffered.pop_first().map(|(_, event)| event)
                    }
                }
            }
            Phase::Live => self.take_ready_live_event(),
            _ => None,
        }?;
        let membership = is_membership_event(&event);
        let event = self.prepare_persisted(event)?;
        if !membership && self.scheduler_delivery.has_active() {
            self.phase_was_preferred = true;
        }
        Some(event)
    }

    fn take_ready_live_event(&mut self) -> Option<TaskEventDto> {
        if self.live_closed {
            return None;
        }
        match self.live.next().now_or_never()? {
            Some(LiveEventItem::Event(event)) if event.id() > self.last => Some(event),
            Some(LiveEventItem::Event(_)) => None,
            Some(LiveEventItem::Lagged) => {
                self.begin_lag_recovery();
                None
            }
            None => {
                self.live_closed = true;
                None
            }
        }
    }

    fn prepare_persisted(&mut self, event: TaskEventDto) -> Option<Event> {
        let id = event.id();
        if id <= self.last {
            return None;
        }
        let membership = is_membership_event(&event);
        if membership {
            self.scheduler_delivery.abort_active();
            self.phase_was_preferred = false;
        }
        match persisted_event(&event) {
            Ok(frame) => {
                self.deferred = Some(DeferredAfterYield::Persisted { id, membership });
                Some(frame)
            }
            Err(()) => {
                self.request_reset();
                self.next_reset_event()
            }
        }
    }

    fn ensure_pending_read(&mut self) {
        if self.pending_read.is_some() {
            return;
        }
        let backend = self.backend.clone();
        self.pending_read = match self.phase {
            Phase::CurrentScheduler => Some(Box::pin(async move {
                BackendRead::CurrentScheduler(backend.current_scheduler_state().await)
            })),
            Phase::InitialMembership => {
                let after = self.after;
                Some(Box::pin(async move {
                    BackendRead::Membership(backend.membership_watermark_through(after).await)
                }))
            }
            Phase::InitialHigh | Phase::LagHigh => Some(Box::pin(async move {
                BackendRead::High(backend.latest_event_id().await)
            })),
            Phase::Replay if self.page.is_empty() && self.last < self.replay_high => {
                let after = self.last;
                let through = self.replay_high;
                Some(Box::pin(async move {
                    BackendRead::Page(backend.events_between(after, through, PAGE_SIZE).await)
                }))
            }
            _ => None,
        };
    }

    fn take_ready_read(&mut self) -> Option<BackendRead> {
        let read = self.pending_read.as_mut()?;
        let result = read.as_mut().now_or_never()?;
        self.pending_read = None;
        Some(result)
    }

    async fn wait_for_activity(&mut self) -> Activity {
        let read = self
            .pending_read
            .as_mut()
            .expect("phase requiring a backend read");
        tokio::select! {
            output = read.as_mut() => {
                self.pending_read = None;
                Activity::Read(output)
            }
            next = self.service.next(), if !self.service_closed => Activity::Service(next),
            next = self.scheduler.next(), if !self.scheduler_closed => Activity::Scheduler(next),
            _ = self.heartbeat.tick() => Activity::Heartbeat,
        }
    }

    fn handle_read(&mut self, read: BackendRead) {
        match read {
            BackendRead::CurrentScheduler(result) => {
                if self.scheduler_delivery.observe(result).is_err() {
                    self.request_reset();
                } else {
                    self.phase = Phase::InitialMembership;
                }
            }
            BackendRead::Membership(result) => match result {
                Ok(watermark) if (0..=self.after).contains(&watermark) => {
                    self.applied_membership = watermark;
                    self.phase = Phase::InitialHigh;
                }
                Ok(_) => self.request_reset(),
                Err(_) => {
                    log_backend_failure("membership_watermark_through");
                    self.phase = Phase::Closed;
                }
            },
            BackendRead::High(result) => self.handle_high(result),
            BackendRead::Page(result) => self.handle_page(result),
        }
    }

    fn handle_high(&mut self, result: ApiResult<i64>) {
        let initial = self.phase == Phase::InitialHigh;
        let operation = if initial {
            "latest_event_id"
        } else {
            "latest_event_id_after_lag"
        };
        let high = match result {
            Ok(high) => high,
            Err(_) => {
                log_backend_failure(operation);
                self.phase = Phase::Closed;
                return;
            }
        };
        self.latest_known = high;
        if self.last > high {
            self.request_reset_at(high);
            return;
        }

        if initial {
            self.scheduler_gate_ready = true;
        }
        if self.last < high {
            self.replay_high = high;
            self.replay_destination = if initial {
                ReplayDestination::InitialJoin
            } else {
                ReplayDestination::LagJoin
            };
            self.phase = Phase::Replay;
        } else {
            self.phase = Phase::Join;
        }
    }

    fn handle_page(&mut self, result: ApiResult<Vec<TaskEventDto>>) {
        let operation = match self.replay_destination {
            ReplayDestination::InitialJoin => "events_between",
            ReplayDestination::LagJoin => "events_between_after_lag",
        };
        let Some(page) = normalize_page(result, operation) else {
            self.phase = Phase::Closed;
            return;
        };
        self.page = page
            .into_iter()
            .filter(|event| event.id() > self.last && event.id() <= self.replay_high)
            .collect();
        if self.page.is_empty() {
            tracing::error!(code = "SSE_REPLAY_NO_PROGRESS", "SSE replay terminated");
            self.phase = Phase::Closed;
        }
    }

    fn finish_replay(&mut self) {
        match self.replay_destination {
            ReplayDestination::InitialJoin | ReplayDestination::LagJoin => {
                self.buffered.retain(|id, _| *id > self.last);
                self.phase = Phase::Join;
            }
        }
    }

    fn step_join(&mut self) -> Option<Event> {
        if self.pending_refills > 0 {
            self.pending_refills -= 1;
            self.phase = Phase::LagHigh;
            return None;
        }
        self.drain_join_live();
        if self.pending_refills > 0 {
            return None;
        }
        if let Some((_, event)) = self.buffered.pop_first() {
            return self.prepare_persisted(event);
        }
        self.phase = Phase::Live;
        None
    }

    fn drain_join_live(&mut self) {
        let Some(drain) = drain_ready_live(
            &mut self.live,
            &mut self.live_closed,
            self.last,
            &mut self.buffered,
        ) else {
            self.request_reset();
            return;
        };
        self.pending_refills += drain.lagged + usize::from(drain.exhausted);
    }

    async fn wait_for_live_activity(&mut self) -> Activity {
        if self.live_closed && self.service_closed && self.scheduler_closed {
            if self.scheduler_delivery.has_pending() {
                self.request_reset();
            } else {
                self.phase = Phase::Closed;
            }
            return Activity::Live(None);
        }
        tokio::select! {
            next = self.service.next(), if !self.service_closed => Activity::Service(next),
            next = self.scheduler.next(), if !self.scheduler_closed => Activity::Scheduler(next),
            _ = self.heartbeat.tick() => Activity::Heartbeat,
            next = self.live.next(), if !self.live_closed => {
                Activity::Live(next.map(Box::new))
            },
        }
    }

    fn handle_live_input(&mut self, next: Option<LiveEventItem>) -> Option<Event> {
        match next {
            Some(LiveEventItem::Event(event)) => self.prepare_persisted(event),
            Some(LiveEventItem::Lagged) => {
                self.begin_lag_recovery();
                None
            }
            None => {
                self.live_closed = true;
                None
            }
        }
    }

    fn begin_lag_recovery(&mut self) {
        self.buffered.clear();
        self.pending_refills = 1;
        self.phase = Phase::Join;
    }

    fn request_reset(&mut self) {
        self.request_reset_at(self.latest_known.max(self.last));
    }

    fn request_reset_at(&mut self, latest: i64) {
        self.scheduler_delivery.abort_active();
        self.pending_read = None;
        self.reset_requested = Some(latest);
    }

    fn next_reset_event(&mut self) -> Option<Event> {
        let latest = self.reset_requested.take()?;
        self.phase = Phase::Closed;
        reset_event(latest)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    CurrentService,
    CurrentScheduler,
    InitialMembership,
    InitialHigh,
    Replay,
    Join,
    LagHigh,
    Live,
    Closed,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReplayDestination {
    InitialJoin,
    LagJoin,
}

enum DeferredAfterYield {
    Service(u64),
    Persisted { id: i64, membership: bool },
}

type PendingRead = Pin<Box<dyn Future<Output = BackendRead> + Send + 'static>>;

enum BackendRead {
    CurrentScheduler(ApiResult<Arc<SchedulerStateDto>>),
    Membership(ApiResult<i64>),
    High(ApiResult<i64>),
    Page(ApiResult<Vec<TaskEventDto>>),
}

enum Immediate {
    Proceed,
    Retry,
    Event(Event),
    Closed,
}

enum Activity {
    Read(BackendRead),
    Service(Option<ServiceStateControl>),
    Scheduler(Option<ApiResult<Arc<SchedulerStateDto>>>),
    Heartbeat,
    Live(Option<Box<LiveEventItem>>),
}

fn coalesce_service(
    first: Option<ServiceStateControl>,
    service: &mut ServiceStateStream,
    service_closed: &mut bool,
) -> Option<ServiceStateControl> {
    let mut newest = match first {
        Some(first) => first,
        None => {
            *service_closed = true;
            return None;
        }
    };
    for _ in 1..MAX_READY_SERVICE_UPDATES {
        match service.next().now_or_never() {
            Some(Some(candidate)) if candidate.generation > newest.generation => {
                newest = candidate;
            }
            Some(Some(_)) => {}
            Some(None) => {
                *service_closed = true;
                break;
            }
            None => break,
        }
    }
    Some(newest)
}

fn normalize_page(
    result: ApiResult<Vec<TaskEventDto>>,
    operation: &'static str,
) -> Option<Vec<TaskEventDto>> {
    let mut page = match result {
        Ok(page) => page,
        Err(_) => {
            log_backend_failure(operation);
            return None;
        }
    };
    page.sort_unstable_by_key(TaskEventDto::id);
    page.dedup_by_key(|event| event.id());
    Some(page)
}

struct LiveDrain {
    lagged: usize,
    exhausted: bool,
}

fn drain_ready_live(
    live: &mut LiveEventStream,
    live_closed: &mut bool,
    last: i64,
    buffered: &mut BTreeMap<i64, TaskEventDto>,
) -> Option<LiveDrain> {
    let mut lagged = 0;
    for _ in 0..READY_LIVE_BATCH_SIZE {
        if *live_closed {
            return Some(LiveDrain {
                lagged,
                exhausted: false,
            });
        }
        match live.next().now_or_never() {
            Some(Some(LiveEventItem::Event(event))) => {
                if event.id() > last {
                    if !buffered.contains_key(&event.id())
                        && buffered.len() >= MAX_BUFFERED_LIVE_EVENTS
                    {
                        tracing::error!(code = "SSE_LIVE_BUFFER_LIMIT", "SSE stream reset");
                        return None;
                    }
                    buffered.insert(event.id(), event);
                }
            }
            Some(Some(LiveEventItem::Lagged)) => lagged += 1,
            Some(None) => {
                *live_closed = true;
                return Some(LiveDrain {
                    lagged,
                    exhausted: false,
                });
            }
            None => {
                return Some(LiveDrain {
                    lagged,
                    exhausted: false,
                });
            }
        }
    }
    Some(LiveDrain {
        lagged,
        exhausted: true,
    })
}

fn is_membership_event(event: &TaskEventDto) -> bool {
    matches!(
        event,
        TaskEventDto::TaskQueued(_)
            | TaskEventDto::TaskStarted(_)
            | TaskEventDto::TaskCompleted(_)
            | TaskEventDto::TaskFailed(_)
            | TaskEventDto::TaskCancelled(_)
            | TaskEventDto::TaskInterrupted(_)
    )
}

fn persisted_event(event: &TaskEventDto) -> Result<Event, ()> {
    Ok(Event::default()
        .id(event.id().to_string())
        .event(event.event_name())
        .data(serialize(event)?))
}

fn heartbeat_event() -> Event {
    Event::default().comment("heartbeat")
}

fn control_event(name: &'static str, value: &impl serde::Serialize) -> Result<Event, ()> {
    Ok(Event::default().event(name).data(serialize(value)?))
}

fn reset_event(latest: i64) -> Option<Event> {
    match control_event("stream.reset", &StreamResetControl::new(latest)) {
        Ok(event) => Some(event),
        Err(()) => {
            tracing::error!(
                code = "SSE_RESET_SERIALIZATION_FAILED",
                "SSE stream terminated"
            );
            None
        }
    }
}

fn serialize(value: &impl serde::Serialize) -> Result<String, ()> {
    serde_json::to_string(value).map_err(|_| {
        tracing::error!(
            code = "SSE_SERIALIZATION_FAILED",
            "SSE stream reset requested"
        );
    })
}

fn log_backend_failure(operation: &'static str) {
    tracing::error!(
        code = "SSE_BACKEND_READ_FAILED",
        operation,
        "SSE stream terminated"
    );
}
