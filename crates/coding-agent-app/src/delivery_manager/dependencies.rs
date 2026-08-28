use std::fmt;
use std::sync::{Arc, Mutex};

use coding_agent_domain::TaskId;
use coding_agent_store::Store;

use crate::{
    RepositoryControlCoordinator, StoreWriterHandle, TaskActiveOwnership, TaskManagerError,
    TaskManagerHandle,
};

use super::cleanup_runtime::DeliveryCleanupRuntimeRegistry;
use super::live_runtime::DeliveryLiveRuntimeRegistry;
use super::operation_query::{DeliveryOperationQuery, StoreDeliveryOperationQuery};
use super::runtime::{DeliveryProcessProofProvider, DeliveryRuntimeRegistry};

/// Direct production dependencies for the Task 20 live pipeline. Runtime and
/// process observation are the only injected seams; task/operation Store reads,
/// StoreWriter mutations, TaskManager ownership and repository coordination
/// remain in DeliveryManager.
pub struct DeliveryManagerLiveDependencies {
    pub(crate) store: Store,
    pub(crate) writer: StoreWriterHandle,
    pub(crate) task_ownership: DeliveryTaskOwnershipBinding,
    pub(crate) repository_control: Arc<RepositoryControlCoordinator>,
    pub(crate) runtime_registry: Arc<dyn DeliveryRuntimeRegistry>,
    pub(crate) process_proofs: Arc<dyn DeliveryProcessProofProvider>,
    pub(crate) operation_query: Arc<dyn DeliveryOperationQuery>,
    pub(crate) live_runtime_registry: Option<Arc<dyn DeliveryLiveRuntimeRegistry>>,
    pub(crate) cleanup_runtime_registry: Option<Arc<dyn DeliveryCleanupRuntimeRegistry>>,
}

/// Startup-safe ownership bridge used to preserve the P4-A/P4-B cold-start
/// order. Before TaskManager exists, the completed process-sentinel probe and
/// atomic P4-A recovery transaction prove that no task is active. The bridge
/// is switched exactly once to the live actor before bootstrap and before any
/// HTTP listener can be published.
#[derive(Clone)]
pub(crate) struct DeliveryTaskOwnershipBinding {
    state: Arc<Mutex<DeliveryTaskOwnershipState>>,
}

enum DeliveryTaskOwnershipState {
    StartupProvenInactive,
    Live(TaskManagerHandle),
}

impl DeliveryTaskOwnershipBinding {
    pub(crate) fn startup_proven_inactive() -> Self {
        Self {
            state: Arc::new(Mutex::new(
                DeliveryTaskOwnershipState::StartupProvenInactive,
            )),
        }
    }

    fn live(task_manager: TaskManagerHandle) -> Self {
        Self {
            state: Arc::new(Mutex::new(DeliveryTaskOwnershipState::Live(task_manager))),
        }
    }

    pub(crate) fn install_live(
        &self,
        task_manager: TaskManagerHandle,
    ) -> Result<(), DeliveryTaskOwnershipInstallError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DeliveryTaskOwnershipInstallError::Closed)?;
        match &*state {
            DeliveryTaskOwnershipState::StartupProvenInactive => {
                *state = DeliveryTaskOwnershipState::Live(task_manager);
                Ok(())
            }
            DeliveryTaskOwnershipState::Live(_) => {
                Err(DeliveryTaskOwnershipInstallError::AlreadyInstalled)
            }
        }
    }

    pub(crate) async fn active_ownership(
        &self,
        task_id: TaskId,
    ) -> Result<TaskActiveOwnership, TaskManagerError> {
        let task_manager = {
            let state = self.state.lock().map_err(|_| TaskManagerError::Closed)?;
            match &*state {
                DeliveryTaskOwnershipState::StartupProvenInactive => None,
                DeliveryTaskOwnershipState::Live(task_manager) => Some(task_manager.clone()),
            }
        };
        match task_manager {
            Some(task_manager) => task_manager.active_ownership(task_id).await,
            None => Ok(TaskActiveOwnership::Inactive),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeliveryTaskOwnershipInstallError {
    Closed,
    AlreadyInstalled,
}

impl DeliveryManagerLiveDependencies {
    #[allow(dead_code)]
    pub(crate) fn new(
        store: Store,
        writer: StoreWriterHandle,
        task_manager: TaskManagerHandle,
        repository_control: Arc<RepositoryControlCoordinator>,
        runtime_registry: Arc<dyn DeliveryRuntimeRegistry>,
        process_proofs: Arc<dyn DeliveryProcessProofProvider>,
    ) -> Self {
        Self::new_with_task_ownership(
            store,
            writer,
            DeliveryTaskOwnershipBinding::live(task_manager),
            repository_control,
            runtime_registry,
            process_proofs,
        )
    }

    pub(crate) fn new_for_startup(
        store: Store,
        writer: StoreWriterHandle,
        task_ownership: DeliveryTaskOwnershipBinding,
        repository_control: Arc<RepositoryControlCoordinator>,
        runtime_registry: Arc<dyn DeliveryRuntimeRegistry>,
        process_proofs: Arc<dyn DeliveryProcessProofProvider>,
    ) -> Self {
        Self::new_with_task_ownership(
            store,
            writer,
            task_ownership,
            repository_control,
            runtime_registry,
            process_proofs,
        )
    }

    fn new_with_task_ownership(
        store: Store,
        writer: StoreWriterHandle,
        task_ownership: DeliveryTaskOwnershipBinding,
        repository_control: Arc<RepositoryControlCoordinator>,
        runtime_registry: Arc<dyn DeliveryRuntimeRegistry>,
        process_proofs: Arc<dyn DeliveryProcessProofProvider>,
    ) -> Self {
        let operation_query = Arc::new(StoreDeliveryOperationQuery::new(store.clone()));
        Self {
            store,
            writer,
            task_ownership,
            repository_control,
            runtime_registry,
            process_proofs,
            operation_query,
            live_runtime_registry: None,
            cleanup_runtime_registry: None,
        }
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn new_for_test(
        store: Store,
        writer: StoreWriterHandle,
        task_manager: TaskManagerHandle,
        repository_control: Arc<RepositoryControlCoordinator>,
        runtime_registry: Arc<dyn DeliveryRuntimeRegistry>,
        process_proofs: Arc<dyn DeliveryProcessProofProvider>,
        operation_query: Arc<dyn DeliveryOperationQuery>,
    ) -> Self {
        Self {
            store,
            writer,
            task_ownership: DeliveryTaskOwnershipBinding::live(task_manager),
            repository_control,
            runtime_registry,
            process_proofs,
            operation_query,
            live_runtime_registry: None,
            cleanup_runtime_registry: None,
        }
    }

    /// Connects the independently sealed accepted-operation runtime. Task 23
    /// owns the production runner-factory wiring; keeping this explicit avoids
    /// silently treating the preflight-only session as mutation authority.
    #[allow(dead_code)]
    pub(crate) fn with_live_runtime_registry(
        mut self,
        registry: Arc<dyn DeliveryLiveRuntimeRegistry>,
    ) -> Self {
        self.live_runtime_registry = Some(registry);
        self
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn with_live_runtime_registry_for_test(
        self,
        registry: Arc<dyn DeliveryLiveRuntimeRegistry>,
    ) -> Self {
        self.with_live_runtime_registry(registry)
    }

    #[allow(dead_code)]
    pub(crate) fn with_cleanup_runtime_registry(
        mut self,
        registry: Arc<dyn DeliveryCleanupRuntimeRegistry>,
    ) -> Self {
        self.cleanup_runtime_registry = Some(registry);
        self
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn with_cleanup_runtime_registry_for_test(
        self,
        registry: Arc<dyn DeliveryCleanupRuntimeRegistry>,
    ) -> Self {
        self.with_cleanup_runtime_registry(registry)
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn new_with_store_operation_query_for_test(
        store: Store,
        writer: StoreWriterHandle,
        task_manager: TaskManagerHandle,
        repository_control: Arc<RepositoryControlCoordinator>,
        runtime_registry: Arc<dyn DeliveryRuntimeRegistry>,
        process_proofs: Arc<dyn DeliveryProcessProofProvider>,
    ) -> Self {
        Self::new(
            store,
            writer,
            task_manager,
            repository_control,
            runtime_registry,
            process_proofs,
        )
    }
}

impl fmt::Debug for DeliveryManagerLiveDependencies {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeliveryManagerLiveDependencies(<redacted>)")
    }
}

#[derive(Clone)]
pub(crate) enum DeliveryManagerBackend {
    Unavailable,
    Live(Arc<DeliveryManagerLiveDependencies>),
}
