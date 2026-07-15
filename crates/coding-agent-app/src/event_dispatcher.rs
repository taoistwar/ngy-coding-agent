use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use coding_agent_domain::{EventCursor, TaskEvent};
use coding_agent_store::{Store, StoreError};
use tokio::sync::{Notify, broadcast, mpsc, oneshot};
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tokio_util::sync::CancellationToken;

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
    commands: mpsc::UnboundedSender<DispatcherCommand>,
    wake: Arc<Notify>,
    events: broadcast::Sender<TaskEvent>,
    lifecycle: Arc<DispatcherLifecycle>,
}

struct DispatcherLifecycle {
    close_requested: AtomicBool,
    closed: CancellationToken,
}

impl DispatcherLifecycle {
    fn new() -> Self {
        Self {
            close_requested: AtomicBool::new(false),
            closed: CancellationToken::new(),
        }
    }
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
        let (commands, receiver) = mpsc::unbounded_channel();
        let wake = Arc::new(Notify::new());
        let (events, _) = broadcast::channel(broadcast_capacity);
        let lifecycle = Arc::new(DispatcherLifecycle::new());
        let actor_lifecycle = lifecycle.clone();
        let actor_wake = wake.clone();
        let actor_events = events.clone();
        tokio::spawn(async move {
            run_dispatcher(store, cursor, receiver, actor_wake, actor_events).await;
            actor_lifecycle.closed.cancel();
        });
        Ok(Self {
            commands,
            wake,
            events,
            lifecycle,
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TaskEvent> {
        self.events.subscribe()
    }

    pub fn wake(&self) {
        self.wake.notify_one();
    }

    pub async fn flush_to(&self, target: EventCursor) -> Result<(), EventDispatcherError> {
        if self.lifecycle.close_requested.load(Ordering::Acquire)
            || self.lifecycle.closed.is_cancelled()
        {
            return Err(EventDispatcherError::Closed);
        }
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(DispatcherCommand::Flush { target, response })
            .map_err(|_| EventDispatcherError::Closed)?;
        receiver.await.map_err(|_| EventDispatcherError::Closed)?
    }

    pub async fn close(&self) -> Result<(), EventDispatcherError> {
        if self.lifecycle.closed.is_cancelled() {
            return Ok(());
        }

        if self
            .lifecycle
            .close_requested
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let (response, receiver) = oneshot::channel();
            if self
                .commands
                .send(DispatcherCommand::Close { response })
                .is_err()
            {
                self.lifecycle.closed.cancel();
                return Ok(());
            }
            let _ = receiver.await;
        }

        self.lifecycle.closed.cancelled().await;
        Ok(())
    }
}

impl EventWake for EventDispatcherHandle {
    fn wake(&self) {
        EventDispatcherHandle::wake(self);
    }
}

enum DispatcherCommand {
    Flush {
        target: EventCursor,
        response: oneshot::Sender<Result<(), EventDispatcherError>>,
    },
    Close {
        response: oneshot::Sender<()>,
    },
}

struct FlushWaiter {
    target: EventCursor,
    response: oneshot::Sender<Result<(), EventDispatcherError>>,
}

async fn run_dispatcher(
    store: Store,
    mut cursor: EventCursor,
    mut commands: mpsc::UnboundedReceiver<DispatcherCommand>,
    wake: Arc<Notify>,
    events: broadcast::Sender<TaskEvent>,
) {
    let mut poll = interval_at(Instant::now() + POLL_INTERVAL, POLL_INTERVAL);
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut waiters = Vec::new();

    loop {
        let should_poll = tokio::select! {
            command = commands.recv() => match command {
                Some(DispatcherCommand::Flush { target, response }) if cursor >= target => {
                    let _ = response.send(Ok(()));
                    false
                }
                Some(DispatcherCommand::Flush { target, response }) => {
                    waiters.push(FlushWaiter { target, response });
                    true
                }
                Some(DispatcherCommand::Close { response }) => {
                    fail_waiters(&mut waiters, EventDispatcherError::Closed);
                    let _ = response.send(());
                    return;
                }
                None => {
                    fail_waiters(&mut waiters, EventDispatcherError::Closed);
                    return;
                },
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
            fail_waiters(&mut waiters, EventDispatcherError::Store(error));
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

fn fail_waiters(waiters: &mut Vec<FlushWaiter>, error: EventDispatcherError) {
    for waiter in waiters.drain(..) {
        let _ = waiter.response.send(Err(error.clone()));
    }
}
