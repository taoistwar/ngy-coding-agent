mod support;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coding_agent_api::{
    LiveEventItem, LiveEventStream, ServiceStateControl, ServiceStateDto, ServiceStateStream,
    SseBackend, TaskEventDto, build_api_router,
};
use coding_agent_domain::{
    ActivityEntry, ActivityLevel, ClientRequestId, DiffSnapshot, EventId, PlanSnapshot,
    RepositoryId, Task, TaskEvent, TaskEventPayload, TaskFailure, TaskId, TaskStatus, TestSnapshot,
    TestStatus, UtcTimestamp,
};
use futures_util::{StreamExt as _, poll, stream};
use http::StatusCode;
use http_body_util::BodyExt as _;
use tokio::sync::{Semaphore, mpsc};

use support::{FakeBackend, FakeSecurity, read_request, send};

struct ScriptedSse {
    calls: Mutex<Vec<&'static str>>,
    highs: Mutex<VecDeque<i64>>,
    persisted: Vec<TaskEventDto>,
    live: Mutex<Option<LiveEventStream>>,
    service: Mutex<Option<ServiceStateStream>>,
    current: ServiceStateControl,
}

impl ScriptedSse {
    fn finite(
        highs: impl IntoIterator<Item = i64>,
        persisted: Vec<TaskEventDto>,
        live: Vec<LiveEventItem>,
        service: Vec<ServiceStateControl>,
    ) -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            highs: Mutex::new(highs.into_iter().collect()),
            persisted,
            live: Mutex::new(Some(Box::pin(stream::iter(live)))),
            service: Mutex::new(Some(Box::pin(stream::iter(service)))),
            current: ServiceStateControl::new(ServiceStateDto::Ready, 1),
        })
    }

    fn pending() -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            highs: Mutex::new(VecDeque::from([0])),
            persisted: Vec::new(),
            live: Mutex::new(Some(Box::pin(stream::pending()))),
            service: Mutex::new(Some(Box::pin(stream::pending()))),
            current: ServiceStateControl::new(ServiceStateDto::Ready, 1),
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

    async fn current_service_state(&self) -> coding_agent_api::ApiResult<ServiceStateControl> {
        self.record("current_service_state");
        Ok(self.current.clone())
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

    async fn current_service_state(&self) -> coding_agent_api::ApiResult<ServiceStateControl> {
        Ok(ServiceStateControl::new(ServiceStateDto::Ready, 0))
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

    async fn current_service_state(&self) -> coding_agent_api::ApiResult<ServiceStateControl> {
        self.current_entered.add_permits(1);
        self.resume_current.acquire().await.unwrap().forget();
        let captured = self.current.lock().expect("current service lock").clone();
        self.current_captured.add_permits(1);
        Ok(captured)
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

#[async_trait::async_trait]
impl SseBackend for HotLiveSse {
    fn subscribe_live(&self) -> LiveEventStream {
        Box::pin(stream::repeat_with(|| LiveEventItem::Event(queued(1))))
    }

    fn subscribe_service_state(&self) -> ServiceStateStream {
        Box::pin(stream::pending())
    }

    async fn current_service_state(&self) -> coding_agent_api::ApiResult<ServiceStateControl> {
        Ok(ServiceStateControl::new(ServiceStateDto::Ready, 0))
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

    async fn current_service_state(&self) -> coding_agent_api::ApiResult<ServiceStateControl> {
        Ok(ServiceStateControl::new(ServiceStateDto::Ready, 0))
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

    let next = body.next();
    futures_util::pin_mut!(next);
    assert!(poll!(next.as_mut()).is_pending());
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

    let next = body.next();
    futures_util::pin_mut!(next);
    assert!(poll!(next.as_mut()).is_pending());
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

    let next = body.next();
    futures_util::pin_mut!(next);
    assert!(poll!(next.as_mut()).is_pending());
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
        &sse.calls()[..4],
        &[
            "subscribe_live",
            "subscribe_service_state",
            "current_service_state",
            "latest_event_id",
        ]
    );
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
    let sse = ScriptedSse::finite([10], events, Vec::new(), Vec::new());
    let (_, body) = finite_body(connect(sse, 0).await).await;
    let expected = [
        "task.queued",
        "task.started",
        "plan.updated",
        "activity.appended",
        "diff.updated",
        "test.updated",
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

fn dto_id(event: &TaskEventDto) -> i64 {
    serde_json::to_value(event).unwrap()["id"].as_i64().unwrap()
}

fn queued(id: i64) -> TaskEventDto {
    TaskEventDto::from(TaskEvent::new(
        EventId::new(id).unwrap(),
        TaskId::new(),
        TaskEventPayload::TaskQueued { task: task(id) },
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
            plan: PlanSnapshot {
                revision: 1,
                items: Vec::new(),
            },
        },
        TaskEventPayload::ActivityAppended {
            entry: ActivityEntry {
                id: "activity".to_owned(),
                level: ActivityLevel::Info,
                message: "safe".to_owned(),
                created_at: timestamp(),
            },
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
        TaskEventPayload::TaskCompleted {
            task: task_with_status(7, TaskStatus::Completed),
        },
        TaskEventPayload::TaskFailed {
            task: task_with_status(8, TaskStatus::Failed),
        },
        TaskEventPayload::TaskCancelled {
            task: task_with_status(9, TaskStatus::Cancelled),
        },
        TaskEventPayload::TaskInterrupted {
            task: task_with_status(10, TaskStatus::Interrupted),
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
        attempt: 1,
        retry_of: None,
        created_at: timestamp(),
        started_at,
        finished_at,
        last_event_id: EventId::new(id.max(1)).unwrap(),
        failure,
    }
}

fn timestamp() -> UtcTimestamp {
    UtcTimestamp::parse_rfc3339("2026-07-15T00:00:00Z").unwrap()
}
