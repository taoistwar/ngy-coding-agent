use std::collections::BTreeMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::response::sse::Event;
use futures_util::{FutureExt as _, Stream, StreamExt as _};
use tokio::time::{Instant, MissedTickBehavior, interval_at};

use crate::{LiveEventItem, SseBackend, StreamResetControl, TaskEventDto};

const PAGE_SIZE: usize = 256;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

pub(crate) type SseEventStream =
    Pin<Box<dyn Stream<Item = Result<Event, Infallible>> + Send + 'static>>;

pub(crate) fn connect(backend: Arc<dyn SseBackend>, after: i64) -> SseEventStream {
    // These subscriptions deliberately happen before the first await and before the response
    // stream becomes lazy. They buffer every commit/state transition that races the snapshot.
    let mut live = backend.subscribe_live();
    let mut service = backend.subscribe_service_state();

    Box::pin(async_stream::stream! {
        let current = match backend.current_service_state().await {
            Ok(current) => current,
            Err(_) => {
                log_backend_failure("current_service_state");
                return;
            }
        };
        let mut service_generation = current.generation;
        let Some(frame) = control_event("service.state", &current) else {
            return;
        };
        yield Ok(frame);

        let initial_high = match backend.latest_event_id().await {
            Ok(high) => high,
            Err(_) => {
                log_backend_failure("latest_event_id");
                return;
            }
        };
        if after > initial_high {
            let reset = StreamResetControl::new(initial_high);
            if let Some(frame) = control_event("stream.reset", &reset) {
                yield Ok(frame);
            }
            return;
        }

        let mut last = after;
        while last < initial_high {
            let Some(page) = read_page(&backend, last, initial_high).await else {
                return;
            };
            let before = last;
            for event in page {
                let id = event.id();
                if id <= last || id > initial_high {
                    continue;
                }
                let Some(frame) = persisted_event(&event) else {
                    return;
                };
                yield Ok(frame);
                last = id;
            }
            if last == before {
                tracing::error!(code = "SSE_REPLAY_NO_PROGRESS", "SSE replay terminated");
                return;
            }
        }

        let mut live_closed = false;
        let mut service_closed = false;
        let mut buffered = BTreeMap::<i64, TaskEventDto>::new();
        let mut pending_refills = drain_ready_live(
            &mut live,
            &mut live_closed,
            last,
            &mut buffered,
        );

        loop {
            if pending_refills > 0 {
                pending_refills -= 1;
                let high = match backend.latest_event_id().await {
                    Ok(high) => high,
                    Err(_) => {
                        log_backend_failure("latest_event_id_after_lag");
                        return;
                    }
                };
                if last > high {
                    let reset = StreamResetControl::new(high);
                    if let Some(frame) = control_event("stream.reset", &reset) {
                        yield Ok(frame);
                    }
                    return;
                }
                while last < high {
                    let Some(page) = read_page(&backend, last, high).await else {
                        return;
                    };
                    let before = last;
                    for event in page {
                        let id = event.id();
                        if id <= last || id > high {
                            continue;
                        }
                        let Some(frame) = persisted_event(&event) else {
                            return;
                        };
                        yield Ok(frame);
                        last = id;
                    }
                    if last == before {
                        tracing::error!(code = "SSE_REPLAY_NO_PROGRESS", "SSE replay terminated");
                        return;
                    }
                }
                buffered.retain(|id, _| *id > last);
                pending_refills += drain_ready_live(
                    &mut live,
                    &mut live_closed,
                    last,
                    &mut buffered,
                );
                if pending_refills > 0 {
                    continue;
                }
            }

            for (_, event) in std::mem::take(&mut buffered) {
                let id = event.id();
                if id <= last {
                    continue;
                }
                let Some(frame) = persisted_event(&event) else {
                    return;
                };
                yield Ok(frame);
                last = id;
            }

            break;
        }

        let mut heartbeat = interval_at(
            Instant::now() + HEARTBEAT_INTERVAL,
            HEARTBEAT_INTERVAL,
        );
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            if live_closed && service_closed {
                return;
            }

            tokio::select! {
                biased;

                next = service.next(), if !service_closed => {
                    match next {
                        Some(first) => {
                            let mut newest = first;
                            loop {
                                match service.next().now_or_never() {
                                    Some(Some(candidate)) => {
                                        if candidate.generation > newest.generation {
                                            newest = candidate;
                                        }
                                    }
                                    Some(None) => {
                                        service_closed = true;
                                        break;
                                    }
                                    None => break,
                                }
                            }
                            if newest.generation > service_generation {
                                service_generation = newest.generation;
                                let Some(frame) = control_event("service.state", &newest) else {
                                    return;
                                };
                                yield Ok(frame);
                            }
                        }
                        None => service_closed = true,
                    }
                }

                _ = heartbeat.tick() => {
                    yield Ok(Event::default().comment("heartbeat"));
                }

                next = live.next(), if !live_closed => {
                    match next {
                        Some(LiveEventItem::Event(event)) if event.id() > last => {
                            let id = event.id();
                            let Some(frame) = persisted_event(&event) else {
                                return;
                            };
                            yield Ok(frame);
                            last = id;
                        }
                        Some(LiveEventItem::Event(_)) => {}
                        Some(LiveEventItem::Lagged) => {
                            buffered.clear();
                            pending_refills = 1 + drain_ready_live(
                                &mut live,
                                &mut live_closed,
                                last,
                                &mut buffered,
                            );

                            while pending_refills > 0 {
                                pending_refills -= 1;
                                let high = match backend.latest_event_id().await {
                                    Ok(high) => high,
                                    Err(_) => {
                                        log_backend_failure("latest_event_id_after_lag");
                                        return;
                                    }
                                };
                                if last > high {
                                    let reset = StreamResetControl::new(high);
                                    if let Some(frame) = control_event("stream.reset", &reset) {
                                        yield Ok(frame);
                                    }
                                    return;
                                }
                                while last < high {
                                    let Some(page) = read_page(&backend, last, high).await else {
                                        return;
                                    };
                                    let before = last;
                                    for event in page {
                                        let id = event.id();
                                        if id <= last || id > high {
                                            continue;
                                        }
                                        let Some(frame) = persisted_event(&event) else {
                                            return;
                                        };
                                        yield Ok(frame);
                                        last = id;
                                    }
                                    if last == before {
                                        tracing::error!(code = "SSE_REPLAY_NO_PROGRESS", "SSE replay terminated");
                                        return;
                                    }
                                }
                                buffered.retain(|id, _| *id > last);
                                pending_refills += drain_ready_live(
                                    &mut live,
                                    &mut live_closed,
                                    last,
                                    &mut buffered,
                                );
                            }

                            for (_, event) in std::mem::take(&mut buffered) {
                                let id = event.id();
                                if id <= last {
                                    continue;
                                }
                                let Some(frame) = persisted_event(&event) else {
                                    return;
                                };
                                yield Ok(frame);
                                last = id;
                            }
                        }
                        None => live_closed = true,
                    }
                }
            }
        }
    })
}

async fn read_page(
    backend: &Arc<dyn SseBackend>,
    after: i64,
    through: i64,
) -> Option<Vec<TaskEventDto>> {
    let mut page = match backend.events_between(after, through, PAGE_SIZE).await {
        Ok(page) => page,
        Err(_) => {
            log_backend_failure("events_between");
            return None;
        }
    };
    page.sort_unstable_by_key(TaskEventDto::id);
    page.dedup_by_key(|event| event.id());
    Some(page)
}

fn drain_ready_live(
    live: &mut crate::LiveEventStream,
    live_closed: &mut bool,
    last: i64,
    buffered: &mut BTreeMap<i64, TaskEventDto>,
) -> usize {
    let mut lagged = 0;
    while !*live_closed {
        match live.next().now_or_never() {
            Some(Some(LiveEventItem::Event(event))) => {
                if event.id() > last {
                    buffered.insert(event.id(), event);
                }
            }
            Some(Some(LiveEventItem::Lagged)) => lagged += 1,
            Some(None) => *live_closed = true,
            None => break,
        }
    }
    lagged
}

fn persisted_event(event: &TaskEventDto) -> Option<Event> {
    let data = serialize(event)?;
    Some(
        Event::default()
            .id(event.id().to_string())
            .event(event.event_name())
            .data(data),
    )
}

fn control_event(name: &'static str, value: &impl serde::Serialize) -> Option<Event> {
    Some(Event::default().event(name).data(serialize(value)?))
}

fn serialize(value: &impl serde::Serialize) -> Option<String> {
    match serde_json::to_string(value) {
        Ok(value) => Some(value),
        Err(_) => {
            tracing::error!(code = "SSE_SERIALIZATION_FAILED", "SSE stream terminated");
            None
        }
    }
}

fn log_backend_failure(operation: &'static str) {
    tracing::error!(
        code = "SSE_BACKEND_READ_FAILED",
        operation,
        "SSE stream terminated"
    );
}
