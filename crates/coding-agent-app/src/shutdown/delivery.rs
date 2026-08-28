use std::future;

use crate::{DeliveryManagerHandle, DeliveryManagerShutdownProof};

#[derive(Clone)]
pub(super) struct DeliveryShutdown {
    manager: DeliveryManagerHandle,
}

impl DeliveryShutdown {
    pub(super) const fn new(manager: DeliveryManagerHandle) -> Self {
        Self { manager }
    }

    /// Closes delivery intake synchronously before returning the join future.
    /// This phase never calls TaskManager and therefore cannot create a reverse
    /// shutdown dependency.
    pub(super) fn begin(&self) -> DeliveryShutdownJoin {
        self.manager.begin_shutdown();
        DeliveryShutdownJoin {
            manager: self.manager.clone(),
        }
    }

    pub(super) fn close_intake(&self) {
        self.manager.begin_shutdown();
    }
}

pub(super) struct DeliveryShutdownJoin {
    manager: DeliveryManagerHandle,
}

impl DeliveryShutdownJoin {
    pub(super) async fn wait(self) -> DeliveryManagerShutdownProof {
        match self.manager.shutdown_and_join().await {
            Ok(proof) => proof,
            Err(error) => {
                tracing::error!(
                    %error,
                    error_code = "DELIVERY_SHUTDOWN_OWNERSHIP_UNPROVEN",
                    "delivery manager closed before exact worker ownership discharge; retaining the primary safety boundary"
                );
                future::pending().await
            }
        }
    }
}
