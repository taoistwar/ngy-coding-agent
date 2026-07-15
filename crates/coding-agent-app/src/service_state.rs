use std::sync::{Arc, Mutex};

use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceState {
    Ready,
    StoreDegraded,
    Quiescing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServiceStateSnapshot {
    pub state: ServiceState,
    pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid service-state transition from {from:?} to {to:?}")]
pub struct InvalidServiceTransition {
    pub from: ServiceState,
    pub to: ServiceState,
}

#[derive(Clone)]
pub struct ServiceStateController {
    current: Arc<Mutex<ServiceStateSnapshot>>,
    sender: watch::Sender<ServiceStateSnapshot>,
}

impl ServiceStateController {
    pub fn new(initial: ServiceState) -> Self {
        let initial = ServiceStateSnapshot {
            state: initial,
            generation: 0,
        };
        let (sender, _) = watch::channel(initial);
        Self {
            current: Arc::new(Mutex::new(initial)),
            sender,
        }
    }

    pub fn current(&self) -> ServiceStateSnapshot {
        *self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn subscribe(&self) -> watch::Receiver<ServiceStateSnapshot> {
        self.sender.subscribe()
    }

    pub fn set(
        &self,
        next: ServiceState,
    ) -> Result<ServiceStateSnapshot, InvalidServiceTransition> {
        let mut current = self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if current.state == next {
            return Ok(*current);
        }
        if current.state == ServiceState::Quiescing {
            return Err(InvalidServiceTransition {
                from: current.state,
                to: next,
            });
        }

        let snapshot = ServiceStateSnapshot {
            state: next,
            generation: current
                .generation
                .checked_add(1)
                .expect("service-state generation overflow"),
        };
        *current = snapshot;
        self.sender.send_replace(snapshot);
        Ok(snapshot)
    }
}
