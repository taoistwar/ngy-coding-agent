use std::sync::Arc;

use coding_agent_runtime::ProcessLivenessScope;
use coding_agent_store::{StartupDeliveryOwnership, Store};

use crate::delivery_manager::ProcessLivenessDeliveryProofProvider;
use crate::delivery_reconciliation::{
    DeliveryArtifactOwnershipRouter, DeliveryOwnershipRoutingError,
};
use crate::{
    DeliveryCleanupRuntimeRegistry, DeliveryLiveRuntimeRegistry, DeliveryProcessProofProvider,
    DeliveryRuntimeRegistry,
};

mod production;

#[cfg(feature = "test-support")]
pub use production::production_delivery_registries_for_test;
pub(super) use production::production_delivery_runtime;
#[cfg(feature = "test-support")]
pub(super) use production::production_delivery_runtime_with_test_support;

#[derive(Clone)]
pub(crate) struct PreparedDeliveryRuntime {
    runtime: Arc<dyn DeliveryRuntimeRegistry>,
    live_runtime: Arc<dyn DeliveryLiveRuntimeRegistry>,
    cleanup_runtime: Arc<dyn DeliveryCleanupRuntimeRegistry>,
}

impl PreparedDeliveryRuntime {
    pub(super) fn new(
        runtime: Arc<dyn DeliveryRuntimeRegistry>,
        live_runtime: Arc<dyn DeliveryLiveRuntimeRegistry>,
        cleanup_runtime: Arc<dyn DeliveryCleanupRuntimeRegistry>,
    ) -> Self {
        Self {
            runtime,
            live_runtime,
            cleanup_runtime,
        }
    }

    pub(crate) fn runtime(&self) -> Arc<dyn DeliveryRuntimeRegistry> {
        Arc::clone(&self.runtime)
    }

    pub(crate) fn live_runtime(&self) -> Arc<dyn DeliveryLiveRuntimeRegistry> {
        Arc::clone(&self.live_runtime)
    }

    pub(crate) fn cleanup_runtime(&self) -> Arc<dyn DeliveryCleanupRuntimeRegistry> {
        Arc::clone(&self.cleanup_runtime)
    }
}

/// Immutable delivery ownership and process-observation inputs established
/// before any actor exists. Runtime registry capabilities are attached by the
/// production repository bindings in the live runner stage.
#[derive(Clone)]
pub(crate) struct PreparedDeliveryStartup {
    ownership: DeliveryArtifactOwnershipRouter,
    runtime: Option<PreparedDeliveryRuntime>,
}

impl PreparedDeliveryStartup {
    pub(super) async fn load(store: &Store) -> Result<Self, DeliveryOwnershipRoutingError> {
        Ok(Self {
            ownership: DeliveryArtifactOwnershipRouter::load(store).await?,
            runtime: None,
        })
    }

    pub(super) fn with_runtime(mut self, runtime: PreparedDeliveryRuntime) -> Self {
        self.runtime = Some(runtime);
        self
    }

    pub(super) const fn ownership_router(&self) -> &DeliveryArtifactOwnershipRouter {
        &self.ownership
    }

    pub(crate) fn ownership(&self) -> &[StartupDeliveryOwnership] {
        self.ownership.ownership()
    }

    pub(crate) fn process_proofs(
        &self,
        instance_scope: ProcessLivenessScope,
    ) -> Arc<dyn DeliveryProcessProofProvider> {
        Arc::new(ProcessLivenessDeliveryProofProvider::new(instance_scope))
    }

    pub(crate) const fn runtime(&self) -> Option<&PreparedDeliveryRuntime> {
        self.runtime.as_ref()
    }
}

impl std::fmt::Debug for PreparedDeliveryStartup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedDeliveryStartup")
            .field("owned_task_count", &self.ownership().len())
            .finish()
    }
}
