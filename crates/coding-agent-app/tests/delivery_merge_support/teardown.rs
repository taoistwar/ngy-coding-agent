use std::sync::{Arc, Weak};

use coding_agent_app::{
    DeliveryManagerHandle, DeliveryProcessProof, DeliveryProcessProofError,
    DeliveryProcessProofProvider, DeliveryProcessProofProviderTestSeam, EventDispatcherHandle,
    EventWake, QuiesceResult, TaskManagerHandle,
};
use coding_agent_domain::TaskId;
use tokio::time::{Duration, Instant, timeout};

const FIXTURE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const FIXTURE_SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(10);

pub struct TeardownFailures {
    fixture: &'static str,
    failures: Vec<String>,
}

impl TeardownFailures {
    pub fn new(fixture: &'static str) -> Self {
        Self {
            fixture,
            failures: Vec::new(),
        }
    }

    pub fn push(&mut self, failure: impl Into<String>) {
        self.failures.push(failure.into());
    }

    pub fn into_result(self) -> Result<(), String> {
        if self.failures.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "{} teardown failed after completing all cleanup steps: {}",
                self.fixture,
                self.failures.join("; ")
            ))
        }
    }
}

struct TrackedDispatcherWake {
    dispatcher: EventDispatcherHandle,
    _actor_lifetime: Arc<()>,
}

impl EventWake for TrackedDispatcherWake {
    fn wake(&self) {
        self.dispatcher.wake();
    }
}

pub fn tracked_dispatcher_wake(
    dispatcher: EventDispatcherHandle,
) -> (Arc<dyn EventWake>, Weak<()>) {
    let actor_lifetime = Arc::new(());
    let lifetime = Arc::downgrade(&actor_lifetime);
    (
        Arc::new(TrackedDispatcherWake {
            dispatcher,
            _actor_lifetime: actor_lifetime,
        }),
        lifetime,
    )
}

struct TrackedProcessProofProvider<T> {
    inner: Arc<T>,
    _actor_lifetime: Arc<()>,
}

impl<T> DeliveryProcessProofProviderTestSeam for TrackedProcessProofProvider<T> where
    T: DeliveryProcessProofProvider
{
}

#[async_trait::async_trait]
impl<T> DeliveryProcessProofProvider for TrackedProcessProofProvider<T>
where
    T: DeliveryProcessProofProvider,
{
    async fn observe(
        &self,
        task_id: TaskId,
    ) -> Result<DeliveryProcessProof, DeliveryProcessProofError> {
        self.inner.observe(task_id).await
    }
}

pub fn tracked_process_proofs<T>(inner: Arc<T>) -> (Arc<dyn DeliveryProcessProofProvider>, Weak<()>)
where
    T: DeliveryProcessProofProvider,
{
    let actor_lifetime = Arc::new(());
    let lifetime = Arc::downgrade(&actor_lifetime);
    (
        Arc::new(TrackedProcessProofProvider {
            inner,
            _actor_lifetime: actor_lifetime,
        }),
        lifetime,
    )
}

pub async fn stop_delivery_manager(
    manager: DeliveryManagerHandle,
    actor_lifetimes: &[Weak<()>],
    failures: &mut TeardownFailures,
) {
    // Some tests intentionally end with fail-closed retained ownership. Such
    // ownership cannot produce a graceful shutdown proof, so the bounded wait
    // falls back to closing the last ingress handle. The lifetime proof below
    // still requires every actor and worker dependency to be released.
    match timeout(FIXTURE_SHUTDOWN_TIMEOUT, manager.quiesce()).await {
        Ok(Ok(snapshot)) if snapshot.in_flight_workers() != 0 || snapshot.queued_workers() != 0 => {
            // Explicit non-empty ownership is the one supported fail-closed
            // fallback: closing ingress releases the actor and its retained
            // in-process test lease without claiming a graceful proof.
        }
        Ok(Ok(_)) => match timeout(FIXTURE_SHUTDOWN_TIMEOUT, manager.shutdown_and_join()).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                failures.push(format!("delivery manager shutdown failed: {error}"));
            }
            Err(_) => failures.push("delivery manager shutdown timed out"),
        },
        Ok(Err(error)) => failures.push(format!("delivery manager quiesce failed: {error}")),
        Err(_) => failures.push("delivery manager quiesce timed out"),
    }
    drop(manager);
    for (generation, lifetime) in actor_lifetimes.iter().enumerate() {
        if wait_for_actor_exit(lifetime).await.is_err() {
            failures.push(format!(
                "delivery manager generation {generation} did not release actor dependencies"
            ));
        }
    }
}

pub async fn stop_task_manager<T>(
    manager: TaskManagerHandle,
    actor_lifetime: &Weak<T>,
    failures: &mut TeardownFailures,
) {
    let quiesce = timeout(
        FIXTURE_SHUTDOWN_TIMEOUT,
        manager.quiesce_and_interrupt(Instant::now() + FIXTURE_SHUTDOWN_TIMEOUT),
    )
    .await;
    match quiesce {
        Ok(Ok(QuiesceResult::Durable { active, .. })) => {
            stop_active_runners(active, failures).await;
        }
        Ok(Ok(QuiesceResult::Frozen { active, error })) => {
            failures.push(format!("task manager quiesce froze: {error}"));
            stop_active_runners(active, failures).await;
        }
        Ok(Err(error)) => failures.push(format!("task manager could not quiesce: {error}")),
        Err(_) => failures.push("task manager quiesce timed out"),
    }
    drop(manager);
    if wait_for_actor_exit(actor_lifetime).await.is_err() {
        failures.push("task manager did not release actor dependencies");
    }
}

async fn stop_active_runners(
    active: Vec<coding_agent_app::RunnerShutdownHandle>,
    failures: &mut TeardownFailures,
) {
    for handle in active {
        let task_id = handle.task_id;
        handle.cancellation.cancel();
        match timeout(FIXTURE_SHUTDOWN_TIMEOUT, handle.done).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => failures.push(format!("runner {task_id} dropped its shutdown proof")),
            Err(_) => failures.push(format!("runner {task_id} did not stop")),
        }
    }
}

pub async fn stop_store_writer(
    writer: coding_agent_app::StoreWriterHandle,
    actor_lifetime: &Weak<()>,
    failures: &mut TeardownFailures,
) {
    drop(writer);
    if wait_for_actor_exit(actor_lifetime).await.is_err() {
        failures.push("store writer did not release actor dependencies");
    }
}

pub async fn close_dispatcher(dispatcher: EventDispatcherHandle, failures: &mut TeardownFailures) {
    match timeout(FIXTURE_SHUTDOWN_TIMEOUT, dispatcher.close()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => failures.push(format!("event dispatcher close failed: {error}")),
        Err(_) => failures.push("event dispatcher close timed out"),
    }
    drop(dispatcher);
}

async fn wait_for_actor_exit<T>(actor_lifetime: &Weak<T>) -> Result<(), ()> {
    timeout(FIXTURE_SHUTDOWN_TIMEOUT, async {
        while actor_lifetime.upgrade().is_some() {
            tokio::time::sleep(FIXTURE_SHUTDOWN_POLL_INTERVAL).await;
        }
    })
    .await
    .map_err(|_| ())
}
