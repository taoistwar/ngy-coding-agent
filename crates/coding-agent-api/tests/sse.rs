mod support;

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coding_agent_api::{
    LiveEventItem, LiveEventStream, MAX_SCHEDULER_FRAME_BYTES, MAX_SCHEDULER_ITEMS_PER_CHUNK,
    SchedulerAdmissionStateDto, SchedulerLimitsDto, SchedulerQueueReasonDto,
    SchedulerQueuedTaskDto, SchedulerRepositoryStorageDto, SchedulerStateDto, SchedulerStateFrames,
    SchedulerStateItemDto, SchedulerStopIntentDto, SchedulerStoppingTaskDto, SchedulerStorageDto,
    SchedulerStorageScopeDto, SchedulerStorageStateDto, SchedulerWireError, ServiceStateControl,
    ServiceStateDto, ServiceStateStream, SseBackend, SseMessage, TaskEventDto, build_api_router,
    canonical_scheduler_state_bytes, ensure_scheduler_state_frame_size, scheduler_snapshot_digest,
    scheduler_state_chunk_frame_len, scheduler_state_frame_len,
};
use coding_agent_domain::{
    ActivityEntry, ActivityLevel, ClientRequestId, DeliveryReadiness, DiffSnapshot, EventId,
    NewReviewEvidence, PlanSnapshot, RepositoryId, RequiredCheck, ReviewDecisionSource,
    ReviewEvidence, ReviewFinding, ReviewVerdict, Task, TaskEvent, TaskEventPayload, TaskFailure,
    TaskId, TaskStatus, TestSnapshot, TestStatus, UtcTimestamp, WorkspaceDigest,
};
use futures_util::{StreamExt as _, poll, stream};
use http::StatusCode;
use http_body_util::BodyExt as _;
use tokio::sync::{Semaphore, mpsc};

use support::{FakeBackend, FakeSecurity, read_request, send};

type TestSchedulerStateStream = Pin<
    Box<
        dyn futures_util::Stream<Item = coding_agent_api::ApiResult<Arc<SchedulerStateDto>>>
            + Send
            + 'static,
    >,
>;

struct ScriptedSse {
    calls: Mutex<Vec<&'static str>>,
    highs: Mutex<VecDeque<i64>>,
    persisted: Vec<TaskEventDto>,
    live: Mutex<Option<LiveEventStream>>,
    service: Mutex<Option<ServiceStateStream>>,
    scheduler: Mutex<Option<TestSchedulerStateStream>>,
    current: ServiceStateControl,
    current_scheduler: Arc<SchedulerStateDto>,
}

struct ScriptedSseConfig {
    highs: Vec<i64>,
    persisted: Vec<TaskEventDto>,
    live: Vec<LiveEventItem>,
    service: Vec<ServiceStateControl>,
    current: ServiceStateControl,
    current_scheduler: Arc<SchedulerStateDto>,
    scheduler: Vec<coding_agent_api::ApiResult<Arc<SchedulerStateDto>>>,
}

impl ScriptedSseConfig {
    fn standard(highs: Vec<i64>, persisted: Vec<TaskEventDto>) -> Self {
        Self {
            highs,
            persisted,
            live: Vec::new(),
            service: Vec::new(),
            current: ServiceStateControl::new(ServiceStateDto::Ready, 1),
            current_scheduler: scheduler_at(0, 1, 0),
            scheduler: Vec::new(),
        }
    }
}

impl ScriptedSse {
    fn finite(
        highs: impl IntoIterator<Item = i64>,
        persisted: Vec<TaskEventDto>,
        live: Vec<LiveEventItem>,
        service: Vec<ServiceStateControl>,
    ) -> Arc<Self> {
        let mut config = ScriptedSseConfig::standard(highs.into_iter().collect(), persisted);
        config.live = live;
        config.service = service;
        Self::configured(config)
    }

    fn configured(config: ScriptedSseConfig) -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            highs: Mutex::new(config.highs.into()),
            persisted: config.persisted,
            live: Mutex::new(Some(Box::pin(stream::iter(config.live)))),
            service: Mutex::new(Some(Box::pin(stream::iter(config.service)))),
            scheduler: Mutex::new(Some(Box::pin(stream::iter(config.scheduler)))),
            current: config.current,
            current_scheduler: config.current_scheduler,
        })
    }

    fn pending() -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            highs: Mutex::new(VecDeque::from([0])),
            persisted: Vec::new(),
            live: Mutex::new(Some(Box::pin(stream::pending()))),
            service: Mutex::new(Some(Box::pin(stream::pending()))),
            scheduler: Mutex::new(Some(Box::pin(stream::pending()))),
            current: ServiceStateControl::new(ServiceStateDto::Ready, 1),
            current_scheduler: scheduler_at(0, 1, 0),
        })
    }

    fn record(&self, call: &'static str) {
        self.calls.lock().expect("calls lock").push(call);
    }

    fn calls(&self) -> Vec<&'static str> {
        self.calls.lock().expect("calls lock").clone()
    }
}

#[async_trait::async_trait]
impl SseBackend for ScriptedSse {
    fn subscribe_live(&self) -> LiveEventStream {
        self.record("subscribe_live");
        self.live
            .lock()
            .expect("live lock")
            .take()
            .unwrap_or_else(|| Box::pin(stream::empty()))
    }

    fn subscribe_service_state(&self) -> ServiceStateStream {
        self.record("subscribe_service_state");
        self.service
            .lock()
            .expect("service lock")
            .take()
            .unwrap_or_else(|| Box::pin(stream::empty()))
    }

    fn subscribe_scheduler_state(&self) -> TestSchedulerStateStream {
        self.record("subscribe_scheduler_state");
        self.scheduler
            .lock()
            .expect("scheduler lock")
            .take()
            .unwrap_or_else(|| Box::pin(stream::empty()))
    }

    async fn current_service_state(&self) -> coding_agent_api::ApiResult<ServiceStateControl> {
        self.record("current_service_state");
        Ok(self.current.clone())
    }

    async fn current_scheduler_state(&self) -> coding_agent_api::ApiResult<Arc<SchedulerStateDto>> {
        self.record("current_scheduler_state");
        Ok(self.current_scheduler.clone())
    }

    async fn membership_watermark_through(
        &self,
        after_cursor: i64,
    ) -> coding_agent_api::ApiResult<i64> {
        self.record("membership_watermark_through");
        Ok(self
            .persisted
            .iter()
            .filter(|event| dto_id(event) <= after_cursor && is_membership_event(event))
            .map(dto_id)
            .max()
            .unwrap_or(0))
    }

    async fn latest_event_id(&self) -> coding_agent_api::ApiResult<i64> {
        self.record("latest_event_id");
        let mut highs = self.highs.lock().expect("highs lock");
        Ok(match highs.len() {
            0 => 0,
            1 => *highs.front().expect("one high"),
            _ => highs.pop_front().expect("scripted high"),
        })
    }

    async fn events_between(
        &self,
        after: i64,
        through: i64,
        limit: usize,
    ) -> coding_agent_api::ApiResult<Vec<TaskEventDto>> {
        self.record("events_between");
        Ok(self
            .persisted
            .iter()
            .filter(|event| {
                let id = dto_id(event);
                id > after && id <= through
            })
            .take(limit)
            .cloned()
            .collect())
    }
}

struct JoinSse {
    high: AtomicI64,
    persisted: Mutex<Vec<TaskEventDto>>,
    live_sender: Mutex<Option<mpsc::UnboundedSender<LiveEventItem>>>,
    live_receiver: Mutex<Option<mpsc::UnboundedReceiver<LiveEventItem>>>,
    pause_latest: AtomicBool,
    latest_entered: Semaphore,
    resume_latest: Semaphore,
    page_calls: AtomicUsize,
    first_page_entered: Semaphore,
    resume_first_page: Semaphore,
    second_page_entered: Semaphore,
    resume_second_page: Semaphore,
    live_poll_entered: Arc<Semaphore>,
    resume_live_poll: Arc<Semaphore>,
}

impl JoinSse {
    fn new(high: i64) -> Arc<Self> {
        let (live_sender, live_receiver) = mpsc::unbounded_channel();
        Arc::new(Self {
            high: AtomicI64::new(high),
            persisted: Mutex::new(Vec::new()),
            live_sender: Mutex::new(Some(live_sender)),
            live_receiver: Mutex::new(Some(live_receiver)),
            pause_latest: AtomicBool::new(true),
            latest_entered: Semaphore::new(0),
            resume_latest: Semaphore::new(0),
            page_calls: AtomicUsize::new(0),
            first_page_entered: Semaphore::new(0),
            resume_first_page: Semaphore::new(0),
            second_page_entered: Semaphore::new(0),
            resume_second_page: Semaphore::new(0),
            live_poll_entered: Arc::new(Semaphore::new(0)),
            resume_live_poll: Arc::new(Semaphore::new(0)),
        })
    }

    fn seed(&self, ids: impl IntoIterator<Item = i64>) {
        self.persisted
            .lock()
            .expect("persisted lock")
            .extend(ids.into_iter().map(queued));
    }

    fn commit(&self, id: i64) {
        let event = queued(id);
        self.persisted
            .lock()
            .expect("persisted lock")
            .push(event.clone());
        self.high.store(id, Ordering::SeqCst);
        self.live_sender
            .lock()
            .expect("live sender lock")
            .as_mut()
            .expect("open live sender")
            .send(LiveEventItem::Event(event))
            .expect("send live event");
    }

    fn close_live(&self) {
        self.live_sender.lock().expect("live sender lock").take();
    }
}

#[async_trait::async_trait]
impl SseBackend for JoinSse {
    fn subscribe_live(&self) -> LiveEventStream {
        let receiver = self
            .live_receiver
            .lock()
            .expect("live receiver lock")
            .take()
            .expect("one live subscription");
        let entered = self.live_poll_entered.clone();
        let resume = self.resume_live_poll.clone();
        Box::pin(async_stream::stream! {
            entered.add_permits(1);
            resume.acquire().await.unwrap().forget();
            let mut receiver = receiver;
            while let Some(item) = receiver.recv().await {
                yield item;
            }
        })
    }

    fn subscribe_service_state(&self) -> ServiceStateStream {
        Box::pin(stream::empty())
    }

    fn subscribe_scheduler_state(&self) -> TestSchedulerStateStream {
        Box::pin(stream::empty())
    }

    async fn current_service_state(&self) -> coding_agent_api::ApiResult<ServiceStateControl> {
        Ok(ServiceStateControl::new(ServiceStateDto::Ready, 0))
    }

    async fn current_scheduler_state(&self) -> coding_agent_api::ApiResult<Arc<SchedulerStateDto>> {
        Ok(scheduler_at(0, 0, 0))
    }

    async fn membership_watermark_through(
        &self,
        after_cursor: i64,
    ) -> coding_agent_api::ApiResult<i64> {
        Ok(membership_watermark(
            &self.persisted.lock().expect("persisted lock"),
            after_cursor,
        ))
    }

    async fn latest_event_id(&self) -> coding_agent_api::ApiResult<i64> {
        if self.pause_latest.swap(false, Ordering::SeqCst) {
            self.latest_entered.add_permits(1);
            self.resume_latest.acquire().await.unwrap().forget();
        }
        Ok(self.high.load(Ordering::SeqCst))
    }

    async fn events_between(
        &self,
        after: i64,
        through: i64,
        limit: usize,
    ) -> coding_agent_api::ApiResult<Vec<TaskEventDto>> {
        match self.page_calls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                self.first_page_entered.add_permits(1);
                self.resume_first_page.acquire().await.unwrap().forget();
            }
            1 => {
                self.second_page_entered.add_permits(1);
                self.resume_second_page.acquire().await.unwrap().forget();
            }
            _ => {}
        }
        let page = self
            .persisted
            .lock()
            .expect("persisted lock")
            .iter()
            .filter(|event| dto_id(event) > after && dto_id(event) <= through)
            .take(limit)
            .cloned()
            .collect();
        Ok(page)
    }
}

struct ServiceRaceSse {
    current: Mutex<ServiceStateControl>,
    service_sender: Mutex<Option<mpsc::UnboundedSender<ServiceStateControl>>>,
    service_receiver: Mutex<Option<mpsc::UnboundedReceiver<ServiceStateControl>>>,
    current_entered: Semaphore,
    resume_current: Semaphore,
    current_captured: Semaphore,
}

impl ServiceRaceSse {
    fn new() -> Arc<Self> {
        let (service_sender, service_receiver) = mpsc::unbounded_channel();
        Arc::new(Self {
            current: Mutex::new(ServiceStateControl::new(ServiceStateDto::Ready, 0)),
            service_sender: Mutex::new(Some(service_sender)),
            service_receiver: Mutex::new(Some(service_receiver)),
            current_entered: Semaphore::new(0),
            resume_current: Semaphore::new(0),
            current_captured: Semaphore::new(0),
        })
    }

    fn publish(&self, state: ServiceStateDto, generation: u64) {
        let control = ServiceStateControl::new(state, generation);
        *self.current.lock().expect("current service lock") = control.clone();
        self.service_sender
            .lock()
            .expect("service sender lock")
            .as_ref()
            .expect("open service sender")
            .send(control)
            .expect("publish service state");
    }

    fn close_service(&self) {
        self.service_sender
            .lock()
            .expect("service sender lock")
            .take();
    }
}

#[async_trait::async_trait]
impl SseBackend for ServiceRaceSse {
    fn subscribe_live(&self) -> LiveEventStream {
        Box::pin(stream::empty())
    }

    fn subscribe_service_state(&self) -> ServiceStateStream {
        let receiver = self
            .service_receiver
            .lock()
            .expect("service receiver lock")
            .take()
            .expect("one service subscription");
        Box::pin(stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        }))
    }

    fn subscribe_scheduler_state(&self) -> TestSchedulerStateStream {
        Box::pin(stream::empty())
    }

    async fn current_service_state(&self) -> coding_agent_api::ApiResult<ServiceStateControl> {
        self.current_entered.add_permits(1);
        self.resume_current.acquire().await.unwrap().forget();
        let captured = self.current.lock().expect("current service lock").clone();
        self.current_captured.add_permits(1);
        Ok(captured)
    }

    async fn current_scheduler_state(&self) -> coding_agent_api::ApiResult<Arc<SchedulerStateDto>> {
        let service_generation = self
            .current
            .lock()
            .expect("current service lock")
            .generation;
        Ok(scheduler_at(0, service_generation, 0))
    }

    async fn membership_watermark_through(&self, _: i64) -> coding_agent_api::ApiResult<i64> {
        Ok(0)
    }

    async fn latest_event_id(&self) -> coding_agent_api::ApiResult<i64> {
        Ok(0)
    }

    async fn events_between(
        &self,
        _: i64,
        _: i64,
        _: usize,
    ) -> coding_agent_api::ApiResult<Vec<TaskEventDto>> {
        Ok(Vec::new())
    }
}

struct ControlRaceSse {
    current_scheduler: Arc<SchedulerStateDto>,
    live_sender: Mutex<Option<mpsc::UnboundedSender<LiveEventItem>>>,
    live_receiver: Mutex<Option<mpsc::UnboundedReceiver<LiveEventItem>>>,
    service_sender: Mutex<Option<mpsc::UnboundedSender<ServiceStateControl>>>,
    service_receiver: Mutex<Option<mpsc::UnboundedReceiver<ServiceStateControl>>>,
    scheduler_sender:
        Mutex<Option<mpsc::UnboundedSender<coding_agent_api::ApiResult<Arc<SchedulerStateDto>>>>>,
    scheduler_receiver:
        Mutex<Option<mpsc::UnboundedReceiver<coding_agent_api::ApiResult<Arc<SchedulerStateDto>>>>>,
    latest_calls: AtomicUsize,
    pause_recovery: AtomicBool,
    recovery_entered: Semaphore,
    resume_recovery: Semaphore,
}

impl ControlRaceSse {
    fn new(current_scheduler: Arc<SchedulerStateDto>) -> Arc<Self> {
        let (live_sender, live_receiver) = mpsc::unbounded_channel();
        let (service_sender, service_receiver) = mpsc::unbounded_channel();
        let (scheduler_sender, scheduler_receiver) = mpsc::unbounded_channel();
        Arc::new(Self {
            current_scheduler,
            live_sender: Mutex::new(Some(live_sender)),
            live_receiver: Mutex::new(Some(live_receiver)),
            service_sender: Mutex::new(Some(service_sender)),
            service_receiver: Mutex::new(Some(service_receiver)),
            scheduler_sender: Mutex::new(Some(scheduler_sender)),
            scheduler_receiver: Mutex::new(Some(scheduler_receiver)),
            latest_calls: AtomicUsize::new(0),
            pause_recovery: AtomicBool::new(false),
            recovery_entered: Semaphore::new(0),
            resume_recovery: Semaphore::new(0),
        })
    }

    fn publish_live(&self, event: TaskEventDto) {
        self.live_sender
            .lock()
            .expect("live sender lock")
            .as_ref()
            .expect("open live sender")
            .send(LiveEventItem::Event(event))
            .expect("publish live event");
    }

    fn publish_lag(&self) {
        self.live_sender
            .lock()
            .expect("live sender lock")
            .as_ref()
            .expect("open live sender")
            .send(LiveEventItem::Lagged)
            .expect("publish live lag");
    }

    fn publish_service(&self, state: ServiceStateDto, generation: u64) {
        self.service_sender
            .lock()
            .expect("service sender lock")
            .as_ref()
            .expect("open service sender")
            .send(ServiceStateControl::new(state, generation))
            .expect("publish service state");
    }

    fn publish_scheduler(&self, snapshot: Arc<SchedulerStateDto>) {
        self.scheduler_sender
            .lock()
            .expect("scheduler sender lock")
            .as_ref()
            .expect("open scheduler sender")
            .send(Ok(snapshot))
            .expect("publish scheduler state");
    }

    fn pause_recovery(&self) {
        self.pause_recovery.store(true, Ordering::SeqCst);
    }

    fn close(&self) {
        self.live_sender.lock().expect("live sender lock").take();
        self.service_sender
            .lock()
            .expect("service sender lock")
            .take();
        self.scheduler_sender
            .lock()
            .expect("scheduler sender lock")
            .take();
    }
}

#[async_trait::async_trait]
impl SseBackend for ControlRaceSse {
    fn subscribe_live(&self) -> LiveEventStream {
        let receiver = self
            .live_receiver
            .lock()
            .expect("live receiver lock")
            .take()
            .expect("one live subscription");
        Box::pin(stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        }))
    }

    fn subscribe_service_state(&self) -> ServiceStateStream {
        let receiver = self
            .service_receiver
            .lock()
            .expect("service receiver lock")
            .take()
            .expect("one service subscription");
        Box::pin(stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        }))
    }

    fn subscribe_scheduler_state(&self) -> TestSchedulerStateStream {
        let receiver = self
            .scheduler_receiver
            .lock()
            .expect("scheduler receiver lock")
            .take()
            .expect("one scheduler subscription");
        Box::pin(stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        }))
    }

    async fn current_service_state(&self) -> coding_agent_api::ApiResult<ServiceStateControl> {
        Ok(ServiceStateControl::new(ServiceStateDto::Ready, 0))
    }

    async fn current_scheduler_state(&self) -> coding_agent_api::ApiResult<Arc<SchedulerStateDto>> {
        Ok(self.current_scheduler.clone())
    }

    async fn membership_watermark_through(&self, _: i64) -> coding_agent_api::ApiResult<i64> {
        Ok(0)
    }

    async fn latest_event_id(&self) -> coding_agent_api::ApiResult<i64> {
        let call = self.latest_calls.fetch_add(1, Ordering::SeqCst);
        if call == 1 && self.pause_recovery.swap(false, Ordering::SeqCst) {
            self.recovery_entered.add_permits(1);
            self.resume_recovery.acquire().await.unwrap().forget();
        }
        Ok(0)
    }

    async fn events_between(
        &self,
        _: i64,
        _: i64,
        _: usize,
    ) -> coding_agent_api::ApiResult<Vec<TaskEventDto>> {
        Ok(Vec::new())
    }
}

struct LagPauseSse {
    latest_calls: AtomicUsize,
    high: AtomicI64,
    persisted: Mutex<Vec<TaskEventDto>>,
    recovery_entered: Semaphore,
    resume_recovery: Semaphore,
    live_sender: Mutex<Option<mpsc::UnboundedSender<LiveEventItem>>>,
    live_receiver: Mutex<Option<mpsc::UnboundedReceiver<LiveEventItem>>>,
    service_sender: mpsc::UnboundedSender<ServiceStateControl>,
    service_receiver: Mutex<Option<mpsc::UnboundedReceiver<ServiceStateControl>>>,
}

struct HotLiveSse;

struct HotSchedulerSse;

struct HotServiceSse;

#[async_trait::async_trait]
impl SseBackend for HotLiveSse {
    fn subscribe_live(&self) -> LiveEventStream {
        Box::pin(stream::repeat_with(|| LiveEventItem::Event(queued(1))))
    }

    fn subscribe_service_state(&self) -> ServiceStateStream {
        Box::pin(stream::pending())
    }

    fn subscribe_scheduler_state(&self) -> TestSchedulerStateStream {
        Box::pin(stream::pending())
    }

    async fn current_service_state(&self) -> coding_agent_api::ApiResult<ServiceStateControl> {
        Ok(ServiceStateControl::new(ServiceStateDto::Ready, 0))
    }

    async fn current_scheduler_state(&self) -> coding_agent_api::ApiResult<Arc<SchedulerStateDto>> {
        Ok(scheduler_at(0, 0, 0))
    }

    async fn membership_watermark_through(&self, _: i64) -> coding_agent_api::ApiResult<i64> {
        Ok(0)
    }

    async fn latest_event_id(&self) -> coding_agent_api::ApiResult<i64> {
        Ok(0)
    }

    async fn events_between(
        &self,
        _: i64,
        _: i64,
        _: usize,
    ) -> coding_agent_api::ApiResult<Vec<TaskEventDto>> {
        Ok(Vec::new())
    }
}

#[async_trait::async_trait]
impl SseBackend for HotSchedulerSse {
    fn subscribe_live(&self) -> LiveEventStream {
        Box::pin(stream::pending())
    }

    fn subscribe_service_state(&self) -> ServiceStateStream {
        Box::pin(stream::pending())
    }

    fn subscribe_scheduler_state(&self) -> TestSchedulerStateStream {
        Box::pin(stream::unfold(1_u64, |generation| async move {
            Some((Ok(scheduler_at(0, 0, generation)), generation + 1))
        }))
    }

    async fn current_service_state(&self) -> coding_agent_api::ApiResult<ServiceStateControl> {
        Ok(ServiceStateControl::new(ServiceStateDto::Ready, 0))
    }

    async fn current_scheduler_state(&self) -> coding_agent_api::ApiResult<Arc<SchedulerStateDto>> {
        Ok(scheduler_at(0, 0, 0))
    }

    async fn membership_watermark_through(&self, _: i64) -> coding_agent_api::ApiResult<i64> {
        Ok(0)
    }

    async fn latest_event_id(&self) -> coding_agent_api::ApiResult<i64> {
        Ok(0)
    }

    async fn events_between(
        &self,
        _: i64,
        _: i64,
        _: usize,
    ) -> coding_agent_api::ApiResult<Vec<TaskEventDto>> {
        Ok(Vec::new())
    }
}

#[async_trait::async_trait]
impl SseBackend for HotServiceSse {
    fn subscribe_live(&self) -> LiveEventStream {
        Box::pin(stream::pending())
    }

    fn subscribe_service_state(&self) -> ServiceStateStream {
        Box::pin(stream::unfold(1_u64, |generation| async move {
            Some((
                ServiceStateControl::new(ServiceStateDto::Ready, generation),
                generation + 1,
            ))
        }))
    }

    fn subscribe_scheduler_state(&self) -> TestSchedulerStateStream {
        Box::pin(stream::pending())
    }

    async fn current_service_state(&self) -> coding_agent_api::ApiResult<ServiceStateControl> {
        Ok(ServiceStateControl::new(ServiceStateDto::Ready, 0))
    }

    async fn current_scheduler_state(&self) -> coding_agent_api::ApiResult<Arc<SchedulerStateDto>> {
        Ok(scheduler_at(0, 0, 0))
    }

    async fn membership_watermark_through(&self, _: i64) -> coding_agent_api::ApiResult<i64> {
        Ok(0)
    }

    async fn latest_event_id(&self) -> coding_agent_api::ApiResult<i64> {
        Ok(1)
    }

    async fn events_between(
        &self,
        after: i64,
        through: i64,
        _: usize,
    ) -> coding_agent_api::ApiResult<Vec<TaskEventDto>> {
        Ok((after < 1 && through >= 1)
            .then(|| queued(1))
            .into_iter()
            .collect())
    }
}

impl LagPauseSse {
    fn new() -> Arc<Self> {
        let (live_sender, live_receiver) = mpsc::unbounded_channel();
        live_sender
            .send(LiveEventItem::Lagged)
            .expect("seed first lag marker");
        let (service_sender, service_receiver) = mpsc::unbounded_channel();
        Arc::new(Self {
            latest_calls: AtomicUsize::new(0),
            high: AtomicI64::new(0),
            persisted: Mutex::new(Vec::new()),
            recovery_entered: Semaphore::new(0),
            resume_recovery: Semaphore::new(0),
            live_sender: Mutex::new(Some(live_sender)),
            live_receiver: Mutex::new(Some(live_receiver)),
            service_sender,
            service_receiver: Mutex::new(Some(service_receiver)),
        })
    }

    fn publish_service(&self, state: ServiceStateDto, generation: u64) {
        self.service_sender
            .send(ServiceStateControl::new(state, generation))
            .expect("publish service state during lag recovery");
    }

    fn persist(&self, id: i64) {
        self.persisted
            .lock()
            .expect("persisted lock")
            .push(queued(id));
        self.high.store(id, Ordering::SeqCst);
    }

    fn push_lag(&self) {
        self.live_sender
            .lock()
            .expect("live sender lock")
            .as_ref()
            .expect("open live sender")
            .send(LiveEventItem::Lagged)
            .expect("send repeated lag marker");
    }

    fn close_live(&self) {
        self.live_sender.lock().expect("live sender lock").take();
    }
}

#[async_trait::async_trait]
impl SseBackend for LagPauseSse {
    fn subscribe_live(&self) -> LiveEventStream {
        let receiver = self
            .live_receiver
            .lock()
            .expect("live receiver lock")
            .take()
            .expect("one live subscription");
        Box::pin(stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        }))
    }

    fn subscribe_service_state(&self) -> ServiceStateStream {
        let receiver = self
            .service_receiver
            .lock()
            .expect("service receiver lock")
            .take()
            .expect("one service subscription");
        Box::pin(stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        }))
    }

    fn subscribe_scheduler_state(&self) -> TestSchedulerStateStream {
        Box::pin(stream::empty())
    }

    async fn current_service_state(&self) -> coding_agent_api::ApiResult<ServiceStateControl> {
        Ok(ServiceStateControl::new(ServiceStateDto::Ready, 0))
    }

    async fn current_scheduler_state(&self) -> coding_agent_api::ApiResult<Arc<SchedulerStateDto>> {
        Ok(scheduler_at(0, 0, 0))
    }

    async fn membership_watermark_through(
        &self,
        after_cursor: i64,
    ) -> coding_agent_api::ApiResult<i64> {
        Ok(membership_watermark(
            &self.persisted.lock().expect("persisted lock"),
            after_cursor,
        ))
    }

    async fn latest_event_id(&self) -> coding_agent_api::ApiResult<i64> {
        if self.latest_calls.fetch_add(1, Ordering::SeqCst) == 1 {
            self.recovery_entered.add_permits(1);
            self.resume_recovery.acquire().await.unwrap().forget();
        }
        Ok(self.high.load(Ordering::SeqCst))
    }

    async fn events_between(
        &self,
        after: i64,
        through: i64,
        limit: usize,
    ) -> coding_agent_api::ApiResult<Vec<TaskEventDto>> {
        Ok(self
            .persisted
            .lock()
            .expect("persisted lock")
            .iter()
            .filter(|event| dto_id(event) > after && dto_id(event) <= through)
            .take(limit)
            .cloned()
            .collect())
    }
}

#[tokio::test]
async fn service_change_between_watch_subscription_and_current_read_cannot_regress() {
    let sse = ServiceRaceSse::new();
    let reading_sse = sse.clone();
    let reading = tokio::spawn(async move { finite_body(connect(reading_sse, 0).await).await });

    sse.current_entered.acquire().await.unwrap().forget();
    sse.publish(ServiceStateDto::StoreDegraded, 1);
    sse.resume_current.add_permits(1);
    sse.current_captured.acquire().await.unwrap().forget();
    sse.publish(ServiceStateDto::Quiescing, 2);
    sse.close_service();

    let (_, body) = reading.await.unwrap();
    assert_eq!(service_generations(&body), vec![1, 2]);
}

#[tokio::test(start_paused = true)]
async fn heartbeat_remains_fair_while_lag_recovery_database_read_is_pending() {
    let sse = LagPauseSse::new();
    let response = connect(sse.clone(), 0).await;
    let mut body = response.into_body().into_data_stream();
    let initial = body.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&initial).contains("service.state"));
    let scheduler = body.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&scheduler).contains("scheduler.state"));

    let next = body.next();
    futures_util::pin_mut!(next);
    for _ in 0..4 {
        assert!(poll!(next.as_mut()).is_pending());
        if sse.recovery_entered.available_permits() == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(sse.recovery_entered.available_permits(), 1);

    tokio::time::advance(Duration::from_secs(14)).await;
    assert!(poll!(next.as_mut()).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    let heartbeat = match poll!(next.as_mut()) {
        std::task::Poll::Ready(Some(Ok(bytes))) => String::from_utf8(bytes.to_vec()).unwrap(),
        other => panic!("heartbeat must be ready at 15 seconds: {other:?}"),
    };
    assert_eq!(heartbeat, ": heartbeat\n\n");
    sse.resume_recovery.add_permits(1);
}

#[tokio::test]
async fn service_control_remains_fair_while_lag_recovery_database_read_is_pending() {
    let sse = LagPauseSse::new();
    let response = connect(sse.clone(), 0).await;
    let mut body = response.into_body().into_data_stream();
    let initial = body.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&initial).contains("service.state"));
    let scheduler = body.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&scheduler).contains("scheduler.state"));

    let next = body.next();
    futures_util::pin_mut!(next);
    for _ in 0..4 {
        assert!(poll!(next.as_mut()).is_pending());
        if sse.recovery_entered.available_permits() == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(sse.recovery_entered.available_permits(), 1);
    sse.publish_service(ServiceStateDto::StoreDegraded, 1);
    let control = match poll!(next.as_mut()) {
        std::task::Poll::Ready(Some(Ok(bytes))) => String::from_utf8(bytes.to_vec()).unwrap(),
        other => panic!("service control must not wait for lag recovery: {other:?}"),
    };
    assert!(control.contains("event: service.state\n"));
    assert!(control.contains("\"generation\":1"));
    sse.resume_recovery.add_permits(1);
}

#[tokio::test]
async fn lag_injected_during_recovery_forces_another_fresh_high_watermark_read() {
    let sse = LagPauseSse::new();
    let response = connect(sse.clone(), 0).await;
    let mut body = response.into_body().into_data_stream();
    body.next().await.unwrap().unwrap();
    let scheduler = body.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&scheduler).contains("scheduler.state"));

    let next = body.next();
    futures_util::pin_mut!(next);
    for _ in 0..4 {
        assert!(poll!(next.as_mut()).is_pending());
        if sse.recovery_entered.available_permits() == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(sse.recovery_entered.available_permits(), 1);
    sse.persist(1);
    sse.push_lag();
    sse.close_live();
    sse.resume_recovery.add_permits(1);

    let event = next.await.expect("recovered event frame").expect("body");
    assert!(String::from_utf8_lossy(&event).contains("id: 1\n"));
    let tail = body.next();
    futures_util::pin_mut!(tail);
    assert!(poll!(tail.as_mut()).is_pending());
    assert_eq!(sse.latest_calls.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn continuously_ready_live_batches_still_yield_the_heartbeat_without_unbounded_drain() {
    let response = connect(Arc::new(HotLiveSse), 0).await;
    let mut body = response.into_body().into_data_stream();
    body.next().await.unwrap().unwrap();
    let scheduler = body.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&scheduler).contains("scheduler.state"));

    let next = body.next();
    futures_util::pin_mut!(next);
    assert!(poll!(next.as_mut()).is_pending());
    tokio::time::advance(Duration::from_secs(15)).await;

    let mut heartbeat = None;
    for _ in 0..64 {
        match poll!(next.as_mut()) {
            std::task::Poll::Ready(Some(Ok(bytes))) => {
                heartbeat = Some(String::from_utf8(bytes.to_vec()).unwrap());
                break;
            }
            std::task::Poll::Pending => tokio::task::yield_now().await,
            other => panic!("hot live stream closed unexpectedly: {other:?}"),
        }
    }
    assert_eq!(heartbeat.as_deref(), Some(": heartbeat\n\n"));
}

#[tokio::test(start_paused = true)]
async fn continuously_ready_scheduler_updates_do_not_starve_the_heartbeat() {
    let response = connect(Arc::new(HotSchedulerSse), 0).await;
    let mut body = response.into_body().into_data_stream();
    let service = body.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&service).contains("service.state"));

    let next = body.next();
    futures_util::pin_mut!(next);
    assert!(poll!(next.as_mut()).is_pending());
    tokio::time::advance(Duration::from_secs(15)).await;

    let mut heartbeat = None;
    for _ in 0..128 {
        match poll!(next.as_mut()) {
            std::task::Poll::Ready(Some(Ok(bytes))) => {
                heartbeat = Some(String::from_utf8(bytes.to_vec()).unwrap());
                break;
            }
            std::task::Poll::Pending => tokio::task::yield_now().await,
            other => panic!("hot scheduler stream closed unexpectedly: {other:?}"),
        }
    }
    assert_eq!(heartbeat.as_deref(), Some(": heartbeat\n\n"));
}

#[tokio::test(start_paused = true)]
async fn continuously_ready_service_updates_do_not_starve_heartbeat_or_replay() {
    let response = connect(Arc::new(HotServiceSse), 0).await;
    let mut body = response.into_body().into_data_stream();
    let service = body.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&service).contains("service.state"));

    tokio::time::advance(Duration::from_secs(15)).await;
    let heartbeat = body.next().await.unwrap().unwrap();
    assert_eq!(String::from_utf8_lossy(&heartbeat), ": heartbeat\n\n");

    let mut replayed = false;
    for _ in 0..32 {
        let frame = body.next().await.unwrap().unwrap();
        if String::from_utf8_lossy(&frame).contains("id: 1\n") {
            replayed = true;
            break;
        }
    }
    assert!(replayed, "hot service updates must not starve Store replay");
}

#[tokio::test]
async fn commits_at_every_join_pause_are_gap_free_and_deduplicated() {
    let sse = JoinSse::new(300);
    sse.seed(1..=300);
    let reading_sse = sse.clone();
    let reading = tokio::spawn(async move { finite_body(connect(reading_sse, 0).await).await });

    sse.latest_entered.acquire().await.unwrap().forget();
    sse.commit(301);
    sse.resume_latest.add_permits(1);

    sse.first_page_entered.acquire().await.unwrap().forget();
    sse.commit(302);
    sse.resume_first_page.add_permits(1);

    sse.second_page_entered.acquire().await.unwrap().forget();
    sse.commit(303);
    sse.resume_second_page.add_permits(1);

    sse.live_poll_entered.acquire().await.unwrap().forget();
    sse.commit(304);
    sse.close_live();
    sse.resume_live_poll.add_permits(1);

    let (_, body) = reading.await.unwrap();
    assert_eq!(persisted_ids(&body), (1..=304).collect::<Vec<_>>());
}

#[tokio::test]
async fn authentication_and_query_validation_happen_before_any_subscription() {
    let sse = ScriptedSse::finite([0], Vec::new(), Vec::new(), Vec::new());
    let router = build_api_router(
        Arc::new(FakeBackend::new()),
        Arc::new(FakeSecurity),
        sse.clone(),
    );
    let unauthorized = support::request(http::Method::GET, "/api/events?after=0")
        .body(axum::body::Body::empty())
        .unwrap();
    assert_eq!(
        send(router.clone(), unauthorized).await.status(),
        StatusCode::UNAUTHORIZED
    );
    assert!(sse.calls().is_empty());

    assert_eq!(
        send(
            router,
            read_request(http::Method::GET, "/api/events?after=-1")
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST
    );
    assert!(sse.calls().is_empty());
}

#[tokio::test]
async fn subscriptions_precede_snapshot_and_join_deduplicates_sorted_live_overlap() {
    let persisted = vec![queued(2), queued(1)];
    let live = vec![
        LiveEventItem::Event(queued(2)),
        LiveEventItem::Event(queued(5)),
        LiveEventItem::Event(queued(4)),
        LiveEventItem::Event(queued(4)),
    ];
    let sse = ScriptedSse::finite([2], persisted, live, Vec::new());
    let response = connect(sse.clone(), 0).await;

    let (status, body) = finite_body(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(persisted_ids(&body), vec![1, 2, 4, 5]);
    assert!(body.starts_with("event: service.state\n"));
    assert_eq!(
        &sse.calls()[..7],
        &[
            "subscribe_live",
            "subscribe_service_state",
            "subscribe_scheduler_state",
            "current_service_state",
            "current_scheduler_state",
            "membership_watermark_through",
            "latest_event_id",
        ]
    );
}

#[tokio::test]
async fn current_quiescing_service_is_the_only_frame_and_skips_store_reads() {
    let sse = ScriptedSse::configured(ScriptedSseConfig {
        current: ServiceStateControl::new(ServiceStateDto::Quiescing, 8),
        current_scheduler: scheduler_at(0, 8, 1),
        ..ScriptedSseConfig::standard(vec![0], Vec::new())
    });
    let (_, body) = finite_body(connect(sse.clone(), 0).await).await;

    assert_eq!(service_generations(&body), vec![8]);
    assert!(!body.contains("event: scheduler.state\n"));
    assert!(!body.contains("event: stream.reset\n"));
    assert_eq!(
        sse.calls(),
        vec![
            "subscribe_live",
            "subscribe_service_state",
            "subscribe_scheduler_state",
            "current_service_state",
        ]
    );
}

#[tokio::test]
async fn future_scheduler_snapshot_waits_until_its_membership_event_was_yielded() {
    let persisted = vec![
        queued(1),
        event(
            2,
            TaskEventPayload::PlanUpdated {
                plan: PlanSnapshot::legacy(1, Vec::new()),
            },
        ),
        event(
            3,
            TaskEventPayload::TaskStarted {
                task: task_with_status(3, TaskStatus::Running),
            },
        ),
    ];
    let sse = ScriptedSse::configured(ScriptedSseConfig {
        scheduler: vec![Ok(scheduler_at(3, 1, 1))],
        ..ScriptedSseConfig::standard(vec![3], persisted)
    });
    let (_, body) = finite_body(connect(sse, 0).await).await;

    assert_eq!(scheduler_generations(&body), vec![1]);
    let started = body.find("id: 3\n").expect("started lifecycle frame");
    let scheduler = body
        .find("event: scheduler.state\n")
        .expect("scheduler manifest");
    assert!(
        started < scheduler,
        "scheduler state must follow the lifecycle frame that unlocks its watermark: {body}"
    );
}

#[tokio::test]
async fn ready_scheduler_updates_coalesce_to_only_the_latest_generation() {
    let sse = ScriptedSse::configured(ScriptedSseConfig {
        current_scheduler: scheduler_at(0, 1, 1),
        scheduler: vec![Ok(scheduler_at(0, 1, 2)), Ok(scheduler_at(0, 1, 3))],
        ..ScriptedSseConfig::standard(vec![0], Vec::new())
    });
    let (_, body) = finite_body(connect(sse, 0).await).await;

    assert_eq!(scheduler_generations(&body), vec![3]);
    let scheduler = body
        .split("\n\n")
        .find(|frame| frame.contains("event: scheduler.state\n"))
        .expect("scheduler manifest");
    assert!(!scheduler.lines().any(|line| line.starts_with("id:")));
}

#[tokio::test]
async fn scheduler_burst_larger_than_the_drain_bound_still_emits_only_latest() {
    let scheduler = (1..=130)
        .map(|generation| Ok(scheduler_at(0, 1, generation)))
        .collect();
    let sse = ScriptedSse::configured(ScriptedSseConfig {
        scheduler,
        ..ScriptedSseConfig::standard(vec![0], Vec::new())
    });
    let (_, body) = finite_body(connect(sse, 0).await).await;

    assert_eq!(scheduler_generations(&body), vec![130]);
}

#[tokio::test]
async fn stale_scheduler_snapshot_is_dropped_without_resetting_the_stream() {
    let sse = ScriptedSse::configured(ScriptedSseConfig {
        current_scheduler: scheduler_at(0, 1, 1),
        ..ScriptedSseConfig::standard(vec![1], vec![queued(1)])
    });
    let (_, body) = finite_body(connect(sse, 1).await).await;

    assert!(scheduler_generations(&body).is_empty());
    assert!(!body.contains("event: stream.reset\n"));
}

#[tokio::test]
async fn conflicting_payload_for_the_same_scheduler_generation_forces_reset() {
    let current = scheduler_at(0, 1, 1);
    let mut conflicting = (*current).clone();
    conflicting.admission_state = SchedulerAdmissionStateDto::Paused;
    let sse = ScriptedSse::configured(ScriptedSseConfig {
        current_scheduler: current,
        scheduler: vec![Ok(Arc::new(conflicting))],
        ..ScriptedSseConfig::standard(vec![0], Vec::new())
    });
    let (_, body) = finite_body(connect(sse, 0).await).await;

    assert!(scheduler_generations(&body).is_empty());
    assert!(body.contains("event: stream.reset\n"));
}

#[tokio::test]
async fn invalid_scheduler_segmentation_emits_idless_reset_and_closes() {
    let mut invalid = scheduler_snapshot(1, 0, 0);
    invalid.as_of_event_id = 0;
    invalid.service_state_generation = 1;
    invalid.generation = 1;
    invalid.queued_task_count = 0;
    let sse = ScriptedSse::configured(ScriptedSseConfig {
        current_scheduler: Arc::new(invalid),
        ..ScriptedSseConfig::standard(vec![0], Vec::new())
    });
    let (_, body) = finite_body(connect(sse, 0).await).await;

    assert!(!body.contains("event: scheduler.state\n"));
    let reset = body
        .split("\n\n")
        .find(|frame| frame.contains("event: stream.reset"))
        .expect("stream reset");
    assert!(!reset.lines().any(|line| line.starts_with("id:")));
}

#[tokio::test]
async fn service_generation_aborts_old_scheduler_chunks_before_the_replacement() {
    let current = scheduler_with_items(129, 0, 0, 1);
    let sse = ControlRaceSse::new(current);
    let response = connect(sse.clone(), 0).await;
    let mut body = response.into_body().into_data_stream();

    let service = body.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&service).contains("event: service.state\n"));
    let old_manifest = body.next().await.unwrap().unwrap();
    let old_manifest = String::from_utf8(old_manifest.to_vec()).unwrap();
    assert!(old_manifest.contains("event: scheduler.state\n"));
    assert!(old_manifest.contains("\"generation\":1"));

    sse.publish_scheduler(scheduler_at(0, 1, 2));
    sse.publish_service(ServiceStateDto::Ready, 1);
    sse.close();

    let service = body.next().await.unwrap().unwrap();
    let service = String::from_utf8(service.to_vec()).unwrap();
    assert!(service.contains("event: service.state\n"));
    assert!(service.contains("\"generation\":1"));
    let replacement = body.next().await.unwrap().unwrap();
    let replacement = String::from_utf8(replacement.to_vec()).unwrap();
    assert!(replacement.contains("event: scheduler.state\n"));
    assert!(replacement.contains("\"generation\":2"));
    let tail = tokio::time::timeout(
        Duration::from_secs(2),
        futures_util::StreamExt::collect::<Vec<_>>(body),
    )
    .await
    .expect("finite replacement stream");
    let tail = tail
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("body");
    let tail = tail
        .into_iter()
        .map(|bytes| String::from_utf8(bytes.to_vec()).unwrap())
        .collect::<String>();
    assert!(!tail.contains("\"generation\":1"));
}

#[tokio::test]
async fn lag_recovery_starts_before_an_active_scheduler_segment_can_continue() {
    let current = scheduler_with_items(129, 0, 0, 1);
    let sse = ControlRaceSse::new(current);
    sse.pause_recovery();
    let response = connect(sse.clone(), 0).await;
    let mut body = response.into_body().into_data_stream();

    body.next().await.unwrap().unwrap();
    let manifest = body.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&manifest).contains("scheduler.state"));
    sse.publish_lag();

    let next = body.next();
    futures_util::pin_mut!(next);
    for _ in 0..6 {
        match poll!(next.as_mut()) {
            std::task::Poll::Pending => {
                if sse.recovery_entered.available_permits() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            std::task::Poll::Ready(Some(Ok(_))) => {
                assert_eq!(sse.recovery_entered.available_permits(), 1);
                break;
            }
            other => panic!("scheduler stream closed during lag recovery: {other:?}"),
        }
    }
    assert_eq!(sse.recovery_entered.available_permits(), 1);

    sse.resume_recovery.add_permits(1);
    sse.close();
}

#[tokio::test]
async fn interrupted_scheduler_group_without_replacement_forces_reset() {
    let current = scheduler_with_items(129, 0, 0, 1);
    let sse = ControlRaceSse::new(current);
    let response = connect(sse.clone(), 0).await;
    let mut body = response.into_body().into_data_stream();

    body.next().await.unwrap().unwrap();
    let manifest = body.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&manifest).contains("scheduler.state"));
    sse.publish_service(ServiceStateDto::StoreDegraded, 1);
    sse.close();

    let service = body.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&service).contains("service.state"));
    let reset = body.next().await.unwrap().unwrap();
    let reset = String::from_utf8(reset.to_vec()).unwrap();
    assert!(reset.contains("event: stream.reset\n"));
    assert!(!reset.lines().any(|line| line.starts_with("id:")));
}

#[tokio::test]
async fn stale_higher_candidate_cannot_hide_an_interrupted_scheduler_group() {
    let current = scheduler_with_items(129, 0, 0, 1);
    let sse = ControlRaceSse::new(current);
    let response = connect(sse.clone(), 0).await;
    let mut body = response.into_body().into_data_stream();

    body.next().await.unwrap().unwrap();
    let manifest = body.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&manifest).contains("\"generation\":1"));
    sse.publish_scheduler(scheduler_at(1, 0, 2));
    sse.publish_live(queued(2));
    sse.close();

    let membership = body.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&membership).contains("id: 2\n"));
    let reset = body.next().await.unwrap().unwrap();
    let reset = String::from_utf8(reset.to_vec()).unwrap();
    assert!(reset.contains("event: stream.reset\n"));
    assert!(!reset.contains("\"generation\":2"));
}

#[tokio::test]
async fn quiescing_during_scheduler_segmentation_emits_service_then_closes() {
    let current = scheduler_with_items(129, 0, 0, 1);
    let sse = ControlRaceSse::new(current);
    let response = connect(sse.clone(), 0).await;
    let mut body = response.into_body().into_data_stream();

    body.next().await.unwrap().unwrap();
    let old_manifest = body.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&old_manifest).contains("\"generation\":1"));

    sse.publish_service(ServiceStateDto::Quiescing, 1);
    sse.close();

    let quiescing = body.next().await.unwrap().unwrap();
    let quiescing = String::from_utf8(quiescing.to_vec()).unwrap();
    assert!(quiescing.contains("event: service.state\n"));
    assert!(quiescing.contains("\"state\":\"quiescing\""));
    assert!(
        tokio::time::timeout(Duration::from_secs(2), body.next())
            .await
            .expect("quiescing closes immediately")
            .is_none()
    );
}

#[tokio::test]
async fn yielded_membership_event_aborts_old_chunks_and_unlocks_the_replacement() {
    let current = scheduler_with_items(129, 0, 0, 1);
    let sse = ControlRaceSse::new(current);
    let response = connect(sse.clone(), 0).await;
    let mut body = response.into_body().into_data_stream();

    body.next().await.unwrap().unwrap();
    let old_manifest = body.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&old_manifest).contains("\"generation\":1"));

    sse.publish_scheduler(scheduler_at(1, 0, 2));
    sse.publish_live(queued(1));
    sse.close();

    let membership = body.next().await.unwrap().unwrap();
    let membership = String::from_utf8(membership.to_vec()).unwrap();
    assert!(membership.contains("id: 1\n"));
    let replacement = body.next().await.unwrap().unwrap();
    let replacement = String::from_utf8(replacement.to_vec()).unwrap();
    assert!(replacement.contains("event: scheduler.state\n"));
    assert!(replacement.contains("\"generation\":2"));
    let tail = tokio::time::timeout(
        Duration::from_secs(2),
        futures_util::StreamExt::collect::<Vec<_>>(body),
    )
    .await
    .expect("finite replacement stream");
    let tail = tail
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("body");
    let tail = tail
        .into_iter()
        .map(|bytes| String::from_utf8(bytes.to_vec()).unwrap())
        .collect::<String>();
    assert!(!tail.contains("\"generation\":1"));
}

#[tokio::test]
async fn lag_refills_from_sqlite_without_serializing_a_lag_event() {
    let sse = ScriptedSse::finite(
        [2, 3],
        vec![queued(1), queued(2), queued(3)],
        vec![LiveEventItem::Lagged],
        Vec::new(),
    );
    let (_, body) = finite_body(connect(sse, 0).await).await;

    assert_eq!(persisted_ids(&body), vec![1, 2, 3]);
    assert!(!body.contains("lagged"));
}

#[tokio::test]
async fn repeated_ready_lag_markers_each_refresh_the_database_high_watermark() {
    let sse = ScriptedSse::finite(
        [1, 2, 3],
        vec![queued(1), queued(2), queued(3)],
        vec![LiveEventItem::Lagged, LiveEventItem::Lagged],
        Vec::new(),
    );
    let (_, body) = finite_body(connect(sse.clone(), 0).await).await;

    assert_eq!(persisted_ids(&body), vec![1, 2, 3]);
    assert_eq!(
        sse.calls()
            .into_iter()
            .filter(|call| *call == "latest_event_id")
            .count(),
        3
    );
}

#[tokio::test]
async fn backlog_is_paged_through_a_fixed_high_watermark_in_chunks_of_256() {
    let persisted = (1..=600).map(queued).collect();
    let sse = ScriptedSse::finite([600], persisted, Vec::new(), Vec::new());
    let (_, body) = finite_body(connect(sse.clone(), 0).await).await;

    let ids = persisted_ids(&body);
    assert_eq!(ids.len(), 600);
    assert_eq!(ids.first(), Some(&1));
    assert_eq!(ids.last(), Some(&600));
    assert_eq!(
        sse.calls()
            .into_iter()
            .filter(|call| *call == "events_between")
            .count(),
        3
    );
}

#[tokio::test]
async fn cursor_ahead_of_high_watermark_emits_idless_reset_and_closes() {
    let sse = ScriptedSse::finite([3], Vec::new(), Vec::new(), Vec::new());
    let (_, body) = finite_body(connect(sse, 9).await).await;

    assert!(body.contains("event: stream.reset\n"));
    assert!(body.contains("\"latest_event_id\":3"));
    assert_eq!(persisted_ids(&body), Vec::<i64>::new());
    let reset = body
        .split("\n\n")
        .find(|frame| frame.contains("stream.reset"))
        .unwrap();
    assert!(!reset.lines().any(|line| line.starts_with("id:")));
}

#[tokio::test]
async fn service_generations_only_advance_and_never_change_the_persisted_cursor() {
    let service = vec![
        ServiceStateControl::new(ServiceStateDto::StoreDegraded, 3),
        ServiceStateControl::new(ServiceStateDto::Ready, 2),
        ServiceStateControl::new(ServiceStateDto::Quiescing, 4),
    ];
    let sse = ScriptedSse::finite([0], Vec::new(), Vec::new(), service);
    let (_, body) = finite_body(connect(sse, 0).await).await;

    assert_eq!(service_generations(&body), vec![1, 4]);
    assert_eq!(persisted_ids(&body), Vec::<i64>::new());
}

#[tokio::test(start_paused = true)]
async fn heartbeat_arrives_at_fifteen_seconds_and_has_no_id() {
    let response = connect(ScriptedSse::pending(), 0).await;
    let mut body = response.into_body().into_data_stream();
    let first = body
        .next()
        .await
        .expect("initial service frame")
        .expect("body");
    assert!(String::from_utf8_lossy(&first).contains("service.state"));
    let scheduler = body
        .next()
        .await
        .expect("initial scheduler frame")
        .expect("body");
    assert!(String::from_utf8_lossy(&scheduler).contains("scheduler.state"));

    let heartbeat = body.next();
    futures_util::pin_mut!(heartbeat);
    assert!(poll!(heartbeat.as_mut()).is_pending());
    tokio::time::advance(Duration::from_secs(14)).await;
    assert!(poll!(heartbeat.as_mut()).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    let heartbeat = match poll!(heartbeat.as_mut()) {
        std::task::Poll::Ready(Some(Ok(bytes))) => {
            String::from_utf8(bytes.to_vec()).expect("UTF-8 heartbeat")
        }
        other => panic!("heartbeat must be ready at 15 seconds: {other:?}"),
    };
    assert_eq!(heartbeat, ": heartbeat\n\n");
    assert!(!heartbeat.contains("id:"));
}

#[tokio::test]
async fn all_persisted_variants_use_the_exact_named_event_and_matching_json_id() {
    let events = all_wire_events();
    let sse = ScriptedSse::finite([11], events, Vec::new(), Vec::new());
    let (_, body) = finite_body(connect(sse, 0).await).await;
    let expected = [
        "task.queued",
        "task.started",
        "plan.updated",
        "activity.appended",
        "diff.updated",
        "test.updated",
        "review.updated",
        "task.completed",
        "task.failed",
        "task.cancelled",
        "task.interrupted",
    ];

    let frames: Vec<_> = body
        .split("\n\n")
        .filter(|frame| frame.lines().any(|line| line.starts_with("id:")))
        .collect();
    assert_eq!(frames.len(), expected.len());
    for (index, (frame, name)) in frames.iter().zip(expected).enumerate() {
        assert!(frame.contains(&format!("event: {name}\n")), "{frame}");
        let id = (index + 1) as i64;
        assert!(frame.contains(&format!("id: {id}\n")), "{frame}");
        assert!(frame.contains(&format!("\"id\":{id}")), "{frame}");
        if name == "review.updated" {
            let data = frame
                .lines()
                .find_map(|line| line.strip_prefix("data:").map(str::trim))
                .expect("review frame data");
            let data: serde_json::Value = serde_json::from_str(data).unwrap();
            let review = &data["payload"]["review"];
            assert_eq!(review["decision_source"], "system");
            assert_eq!(review["verdict"], "changes_requested");
            assert_eq!(review["coverage"], serde_json::Value::Null);
            assert_eq!(review["required_checks"][0]["kind"], "cargo_test");
            assert_eq!(
                review["required_checks"][0]["package"],
                serde_json::Value::Null
            );
            assert_eq!(
                review["required_checks"][0]["integration_test"],
                serde_json::Value::Null
            );
            assert_eq!(review.as_object().unwrap().len(), 12);
        }
    }
}

async fn connect(sse: Arc<dyn SseBackend>, after: i64) -> axum::response::Response {
    let router = build_api_router(Arc::new(FakeBackend::new()), Arc::new(FakeSecurity), sse);
    send(
        router,
        read_request(http::Method::GET, &format!("/api/events?after={after}")),
    )
    .await
}

async fn finite_body(response: axum::response::Response) -> (StatusCode, String) {
    let status = response.status();
    let bytes = tokio::time::timeout(Duration::from_secs(2), response.into_body().collect())
        .await
        .expect("finite SSE stream")
        .expect("collect body")
        .to_bytes();
    (
        status,
        String::from_utf8(bytes.to_vec()).expect("UTF-8 SSE"),
    )
}

fn persisted_ids(body: &str) -> Vec<i64> {
    body.lines()
        .filter_map(|line| line.strip_prefix("id:").map(str::trim))
        .map(|id| id.parse().expect("numeric SSE id"))
        .collect()
}

fn service_generations(body: &str) -> Vec<u64> {
    body.split("\n\n")
        .filter(|frame| frame.contains("event: service.state"))
        .map(|frame| {
            let data = frame
                .lines()
                .find_map(|line| line.strip_prefix("data:").map(str::trim))
                .expect("service data");
            serde_json::from_str::<serde_json::Value>(data).unwrap()["generation"]
                .as_u64()
                .unwrap()
        })
        .collect()
}

fn scheduler_generations(body: &str) -> Vec<u64> {
    body.split("\n\n")
        .filter(|frame| frame.contains("event: scheduler.state\n"))
        .map(|frame| {
            let data = frame
                .lines()
                .find_map(|line| line.strip_prefix("data:").map(str::trim))
                .expect("scheduler data");
            serde_json::from_str::<serde_json::Value>(data).unwrap()["generation"]
                .as_u64()
                .unwrap()
        })
        .collect()
}

fn dto_id(event: &TaskEventDto) -> i64 {
    serde_json::to_value(event).unwrap()["id"].as_i64().unwrap()
}

fn membership_watermark(events: &[TaskEventDto], after_cursor: i64) -> i64 {
    events
        .iter()
        .filter(|event| dto_id(event) <= after_cursor && is_membership_event(event))
        .map(dto_id)
        .max()
        .unwrap_or(0)
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

fn queued(id: i64) -> TaskEventDto {
    event(id, TaskEventPayload::TaskQueued { task: task(id) })
}

fn event(id: i64, payload: TaskEventPayload) -> TaskEventDto {
    TaskEventDto::from(TaskEvent::new(
        EventId::new(id).unwrap(),
        TaskId::new(),
        payload,
        timestamp(),
    ))
}

fn all_wire_events() -> Vec<TaskEventDto> {
    let payloads = vec![
        TaskEventPayload::TaskQueued {
            task: task_with_status(1, TaskStatus::Queued),
        },
        TaskEventPayload::TaskStarted {
            task: task_with_status(2, TaskStatus::Running),
        },
        TaskEventPayload::PlanUpdated {
            plan: PlanSnapshot::legacy(1, Vec::new()),
        },
        TaskEventPayload::ActivityAppended {
            entry: ActivityEntry::legacy("activity", ActivityLevel::Info, "safe", timestamp()),
        },
        TaskEventPayload::DiffUpdated {
            diff: DiffSnapshot {
                revision: 1,
                files: Vec::new(),
            },
        },
        TaskEventPayload::TestUpdated {
            tests: TestSnapshot {
                revision: 1,
                status: TestStatus::Passed,
                cases: Vec::new(),
            },
        },
        TaskEventPayload::ReviewUpdated {
            review: system_review(),
        },
        TaskEventPayload::TaskCompleted {
            task: task_with_status(8, TaskStatus::Completed),
        },
        TaskEventPayload::TaskFailed {
            task: task_with_status(9, TaskStatus::Failed),
        },
        TaskEventPayload::TaskCancelled {
            task: task_with_status(10, TaskStatus::Cancelled),
        },
        TaskEventPayload::TaskInterrupted {
            task: task_with_status(11, TaskStatus::Interrupted),
        },
    ];
    payloads
        .into_iter()
        .enumerate()
        .map(|(index, payload)| {
            TaskEventDto::from(TaskEvent::new(
                EventId::new((index + 1) as i64).unwrap(),
                TaskId::new(),
                payload,
                timestamp(),
            ))
        })
        .collect()
}

fn system_review() -> ReviewEvidence {
    let digest = WorkspaceDigest::try_new("a".repeat(64)).unwrap();
    let required_checks = vec![RequiredCheck::try_cargo_test("tests", None, None).unwrap()];
    let evidence = NewReviewEvidence::try_new(
        1,
        ReviewDecisionSource::System,
        0,
        digest,
        ReviewVerdict::ChangesRequested,
        "workspace changed",
        vec![ReviewFinding::system_workspace_changed(1).unwrap()],
        Vec::new(),
        required_checks,
        Vec::new(),
        None,
    )
    .unwrap();
    ReviewEvidence::try_from_new(evidence, timestamp()).unwrap()
}

fn task(id: i64) -> Task {
    task_with_status(id, TaskStatus::Queued)
}

fn task_with_status(id: i64, status: TaskStatus) -> Task {
    let started_at = matches!(
        status,
        TaskStatus::Running | TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Interrupted
    )
    .then(timestamp);
    let finished_at = matches!(
        status,
        TaskStatus::Completed
            | TaskStatus::Failed
            | TaskStatus::Cancelled
            | TaskStatus::Interrupted
    )
    .then(timestamp);
    let failure =
        matches!(status, TaskStatus::Failed | TaskStatus::Interrupted).then(|| TaskFailure {
            code: "SAFE".to_owned(),
            message: "safe".to_owned(),
            retryable: false,
        });
    Task {
        id: TaskId::new(),
        client_request_id: ClientRequestId::new(),
        repository_id: RepositoryId::new(),
        prompt: "safe fixture".to_owned(),
        status,
        delivery_readiness: DeliveryReadiness::Unreviewed,
        attempt: 1,
        retry_of: None,
        created_at: timestamp(),
        started_at,
        finished_at,
        last_event_id: EventId::new(id.max(1)).unwrap(),
        failure,
    }
}

#[test]
fn scheduler_wire_empty_snapshot_emits_one_idless_manifest_and_no_chunks() {
    let snapshot = scheduler_snapshot(0, 0, 0);
    let frames = SchedulerStateFrames::try_from_snapshot(&snapshot).unwrap();

    assert!(frames.chunks().is_empty());
    assert_eq!(frames.manifest().item_count, 0);
    assert_eq!(frames.manifest().chunk_count, 0);
    assert_eq!(frames.manifest().queued_task_count, 0);
    assert_eq!(frames.manifest().stopping_task_count, 0);
    assert_eq!(frames.manifest().repository_storage_count, 0);
    let value =
        serde_json::to_value(SseMessage::SchedulerState(frames.manifest().clone())).unwrap();
    assert_eq!(value["kind"], "scheduler.state");
    assert!(value.get("id").is_none());
    assert_eq!(
        scheduler_state_frame_len(frames.manifest()).unwrap(),
        format!(
            "event: scheduler.state\ndata: {}\n\n",
            serde_json::to_string(frames.manifest()).unwrap()
        )
        .len()
    );
    assert!(scheduler_state_frame_len(frames.manifest()).unwrap() <= MAX_SCHEDULER_FRAME_BYTES);
}

#[test]
fn scheduler_wire_chunks_canonical_group_order_at_the_128_item_boundary() {
    let snapshot = scheduler_snapshot(129, 2, 2);
    let frames = SchedulerStateFrames::try_from_snapshot(&snapshot).unwrap();

    assert_eq!(frames.manifest().item_count, 133);
    assert_eq!(frames.manifest().chunk_count, 2);
    assert_eq!(frames.chunks().len(), 2);
    assert_eq!(
        frames.chunks()[0].items.len(),
        MAX_SCHEDULER_ITEMS_PER_CHUNK
    );
    assert_eq!(frames.chunks()[1].items.len(), 5);

    let items = frames
        .chunks()
        .iter()
        .flat_map(|chunk| chunk.items.iter())
        .collect::<Vec<_>>();
    assert!(
        items[..129]
            .iter()
            .all(|item| matches!(item, SchedulerStateItemDto::QueuedTask(_)))
    );
    assert!(
        items[129..131]
            .iter()
            .all(|item| matches!(item, SchedulerStateItemDto::StoppingTask(_)))
    );
    assert!(
        items[131..]
            .iter()
            .all(|item| matches!(item, SchedulerStateItemDto::RepositoryStorage(_)))
    );

    for (index, chunk) in frames.chunks().iter().enumerate() {
        assert_eq!(chunk.chunk_index, u32::try_from(index).unwrap());
        assert_eq!(chunk.chunk_count, frames.manifest().chunk_count);
        assert_eq!(
            chunk.server_instance_id,
            frames.manifest().server_instance_id
        );
        assert_eq!(chunk.generation, frames.manifest().generation);
        assert_eq!(chunk.snapshot_digest, frames.manifest().snapshot_digest);
        let value = serde_json::to_value(SseMessage::SchedulerStateChunk(chunk.clone())).unwrap();
        assert_eq!(value["kind"], "scheduler.state.chunk");
        assert!(value.get("id").is_none());
        assert_eq!(
            scheduler_state_chunk_frame_len(chunk).unwrap(),
            format!(
                "event: scheduler.state.chunk\ndata: {}\n\n",
                serde_json::to_string(chunk).unwrap()
            )
            .len()
        );
        assert!(scheduler_state_chunk_frame_len(chunk).unwrap() <= MAX_SCHEDULER_FRAME_BYTES);
    }
}

#[test]
fn scheduler_wire_rejects_a_snapshot_whose_declared_count_is_not_exact() {
    let mut snapshot = scheduler_snapshot(1, 0, 0);
    snapshot.queued_task_count = 2;

    assert!(matches!(
        SchedulerStateFrames::try_from_snapshot(&snapshot),
        Err(SchedulerWireError::QueuedTaskCountMismatch {
            declared: 2,
            actual: 1
        })
    ));
}

#[test]
fn scheduler_wire_rejects_unsafe_integers_duplicates_and_noncanonical_repositories() {
    let mut unsafe_integer = scheduler_snapshot(0, 0, 0);
    unsafe_integer.generation = 9_007_199_254_740_992;
    assert!(matches!(
        SchedulerStateFrames::try_from_snapshot(&unsafe_integer),
        Err(SchedulerWireError::UnsafeInteger {
            field: "generation",
            ..
        })
    ));

    let mut duplicate_task = scheduler_snapshot(1, 1, 0);
    duplicate_task.stopping_tasks[0].task_id = duplicate_task.queued_tasks[0].task_id;
    assert!(matches!(
        SchedulerStateFrames::try_from_snapshot(&duplicate_task),
        Err(SchedulerWireError::DuplicateTaskId)
    ));

    let mut repositories = scheduler_snapshot(0, 0, 2);
    repositories.storage.repositories.swap(0, 1);
    assert!(matches!(
        SchedulerStateFrames::try_from_snapshot(&repositories),
        Err(SchedulerWireError::RepositoryStorageNotCanonical)
    ));
}

#[test]
fn scheduler_wire_frame_guard_counts_the_complete_sse_envelope() {
    let frames = SchedulerStateFrames::try_from_snapshot(&scheduler_snapshot(0, 0, 0)).unwrap();
    let mut oversized = frames.manifest().clone();
    oversized.snapshot_digest = "a".repeat(MAX_SCHEDULER_FRAME_BYTES);

    assert!(matches!(
        ensure_scheduler_state_frame_size(&oversized),
        Err(SchedulerWireError::FrameTooLarge {
            kind: "scheduler.state",
            encoded_bytes,
        }) if encoded_bytes > MAX_SCHEDULER_FRAME_BYTES
    ));
}

#[test]
fn scheduler_wire_digest_uses_exact_rfc8785_bytes_and_lowercase_sha256() {
    let snapshot = scheduler_snapshot(1, 1, 1);
    let canonical = canonical_scheduler_state_bytes(&snapshot).unwrap();
    let digest = scheduler_snapshot_digest(&snapshot).unwrap();
    let frames = SchedulerStateFrames::try_from_snapshot(&snapshot).unwrap();
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../testdata/scheduler-state-rfc8785.json"
    ))
    .unwrap();

    assert_eq!(
        serde_json::to_value(&snapshot).unwrap(),
        fixture["snapshot"]
    );
    assert_eq!(
        String::from_utf8(canonical).unwrap(),
        fixture["canonical_json"].as_str().unwrap()
    );
    assert_eq!(digest, fixture["sha256"].as_str().unwrap());
    assert_eq!(frames.manifest().snapshot_digest, digest);
    assert_eq!(digest.len(), 64);
    assert!(
        digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
}

fn scheduler_snapshot(
    queued_task_count: usize,
    stopping_task_count: usize,
    repository_storage_count: usize,
) -> SchedulerStateDto {
    let queued_tasks = (0..queued_task_count)
        .map(|index| SchedulerQueuedTaskDto {
            task_id: indexed_uuid(0x1000 + index),
            reason: SchedulerQueueReasonDto::RepositoryCapacity,
        })
        .collect::<Vec<_>>();
    let stopping_tasks = (0..stopping_task_count)
        .map(|index| SchedulerStoppingTaskDto {
            task_id: indexed_uuid(0x2000 + index),
            intent: SchedulerStopIntentDto::UserCancelled,
        })
        .collect::<Vec<_>>();
    let repositories = (0..repository_storage_count)
        .map(|index| SchedulerRepositoryStorageDto {
            repository_id: indexed_uuid(0x3000 + index),
            state: SchedulerStorageStateDto::Pressure,
        })
        .collect::<Vec<_>>();

    SchedulerStateDto {
        schema_version: 1,
        server_instance_id: uuid::Uuid::parse_str("123e4567-e89b-42d3-a456-426614174000").unwrap(),
        server_started_at: timestamp().into(),
        generation: 9_007_199_254_740_991,
        as_of_event_id: 41,
        service_state_generation: 7,
        admission_state: SchedulerAdmissionStateDto::Running,
        limits: SchedulerLimitsDto {
            global: 4,
            per_repository: 2,
            queued: 256,
            cargo_jobs_per_task: 8,
        },
        active_task_count: u32::try_from(stopping_task_count).unwrap(),
        queued_task_count: u32::try_from(queued_task_count).unwrap(),
        queued_tasks,
        stopping_tasks,
        storage: SchedulerStorageDto {
            state: SchedulerStorageStateDto::Unavailable,
            data: SchedulerStorageScopeDto {
                state: SchedulerStorageStateDto::Normal,
            },
            runtime: SchedulerStorageScopeDto {
                state: SchedulerStorageStateDto::Unavailable,
            },
            repositories,
        },
    }
}

fn scheduler_at(
    as_of_event_id: u64,
    service_state_generation: u64,
    generation: u64,
) -> Arc<SchedulerStateDto> {
    let mut snapshot = scheduler_snapshot(0, 0, 0);
    snapshot.as_of_event_id = as_of_event_id;
    snapshot.service_state_generation = service_state_generation;
    snapshot.generation = generation;
    Arc::new(snapshot)
}

fn scheduler_with_items(
    queued_task_count: usize,
    as_of_event_id: u64,
    service_state_generation: u64,
    generation: u64,
) -> Arc<SchedulerStateDto> {
    let mut snapshot = scheduler_snapshot(queued_task_count, 0, 0);
    snapshot.as_of_event_id = as_of_event_id;
    snapshot.service_state_generation = service_state_generation;
    snapshot.generation = generation;
    Arc::new(snapshot)
}

fn indexed_uuid(index: usize) -> uuid::Uuid {
    uuid::Uuid::from_u128(0x123e_4567_e89b_42d3_a456_0000_0000_0000 + index as u128)
}

fn timestamp() -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339("2026-07-15T00:00:00Z").unwrap()
}
