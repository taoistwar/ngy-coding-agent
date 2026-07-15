use std::sync::Arc;
use std::time::Duration;

use coding_agent_domain::{EventCursor, TaskEvent};
use coding_agent_store::{Store, StoreError};
use tokio::sync::{Notify, broadcast, mpsc, oneshot};
use tokio::time::{Instant, MissedTickBehavior, interval_at};

use crate::EventWake;

const EVENT_PAGE_SIZE: usize = 256;
const POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, thiserror::Error)]
pub enum EventDispatcherError {
    #[error("event store read failed: {0}")]
    Store(#[source] Arc<StoreError>),
    #[error("event dispatcher is closed")]
    Closed,
}

impl From<StoreError> for EventDispatcherError {
    fn from(error: StoreError) -> Self {
        Self::Store(Arc::new(error))
    }
}

#[derive(Clone)]
pub struct EventDispatcherHandle {
    flushes: mpsc::UnboundedSender<FlushCommand>,
    wake: Arc<Notify>,
    events: broadcast::Sender<TaskEvent>,
}

impl EventDispatcherHandle {
    pub async fn spawn(
        store: Store,
        broadcast_capacity: usize,
    ) -> Result<Self, EventDispatcherError> {
        assert!(
            broadcast_capacity > 0,
            "event-dispatcher broadcast capacity must be positive"
        );
        let cursor = store.latest_event_id().await?;
        let (flushes, receiver) = mpsc::unbounded_channel();
        let wake = Arc::new(Notify::new());
        let (events, _) = broadcast::channel(broadcast_capacity);
        tokio::spawn(run_dispatcher(
            store,
            cursor,
            receiver,
            wake.clone(),
            events.clone(),
        ));
        Ok(Self {
            flushes,
            wake,
            events,
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TaskEvent> {
        self.events.subscribe()
    }

    pub fn wake(&self) {
        self.wake.notify_one();
    }

    pub async fn flush_to(&self, target: EventCursor) -> Result<(), EventDispatcherError> {
        let (response, receiver) = oneshot::channel();
        self.flushes
            .send(FlushCommand { target, response })
            .map_err(|_| EventDispatcherError::Closed)?;
        receiver.await.map_err(|_| EventDispatcherError::Closed)?
    }
}

impl EventWake for EventDispatcherHandle {
    fn wake(&self) {
        EventDispatcherHandle::wake(self);
    }
}

struct FlushCommand {
    target: EventCursor,
    response: oneshot::Sender<Result<(), EventDispatcherError>>,
}

struct FlushWaiter {
    target: EventCursor,
    response: oneshot::Sender<Result<(), EventDispatcherError>>,
}

async fn run_dispatcher(
    store: Store,
    mut cursor: EventCursor,
    mut flushes: mpsc::UnboundedReceiver<FlushCommand>,
    wake: Arc<Notify>,
    events: broadcast::Sender<TaskEvent>,
) {
    let mut poll = interval_at(Instant::now() + POLL_INTERVAL, POLL_INTERVAL);
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut waiters = Vec::new();

    loop {
        let should_poll = tokio::select! {
            command = flushes.recv() => match command {
                Some(FlushCommand { target, response }) if cursor >= target => {
                    let _ = response.send(Ok(()));
                    false
                }
                Some(FlushCommand { target, response }) => {
                    waiters.push(FlushWaiter { target, response });
                    true
                }
                None => break,
            },
            () = wake.notified() => true,
            _ = poll.tick() => true,
        };

        if !should_poll {
            continue;
        }

        if let Err(error) = publish_available(&store, &events, &mut cursor, &mut waiters).await {
            let error = Arc::new(error);
            tracing::error!(error = %error, "event dispatcher failed to read persisted events");
            fail_waiters(&mut waiters, error);
        }
    }
}

async fn publish_available(
    store: &Store,
    events: &broadcast::Sender<TaskEvent>,
    cursor: &mut EventCursor,
    waiters: &mut Vec<FlushWaiter>,
) -> Result<(), StoreError> {
    loop {
        let mut page = store.events_after(*cursor, EVENT_PAGE_SIZE).await?;
        page.events.sort_unstable_by_key(|event| event.id);
        let cursor_before_page = *cursor;

        for event in page.events {
            if event.id.get() <= cursor.get() {
                continue;
            }
            let event_cursor = EventCursor::new(event.id.get())?;
            let _ = events.send(event);
            *cursor = event_cursor;
            complete_waiters(waiters, *cursor);
        }

        if *cursor >= page.high_watermark || *cursor == cursor_before_page {
            return Ok(());
        }
    }
}

fn complete_waiters(waiters: &mut Vec<FlushWaiter>, cursor: EventCursor) {
    let mut pending = Vec::with_capacity(waiters.len());
    for waiter in waiters.drain(..) {
        if cursor >= waiter.target {
            let _ = waiter.response.send(Ok(()));
        } else {
            pending.push(waiter);
        }
    }
    *waiters = pending;
}

fn fail_waiters(waiters: &mut Vec<FlushWaiter>, error: Arc<StoreError>) {
    for waiter in waiters.drain(..) {
        let _ = waiter
            .response
            .send(Err(EventDispatcherError::Store(error.clone())));
    }
}
